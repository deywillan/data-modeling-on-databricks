"""*PassagemHub*: marketplace parceiro da ModelBricks Viação, entregue por API paginada em
JSON aninhado.

O marketplace vende passagens das linhas da Viação para os mesmos clientes do mundo
verdadeiro, com chave e grafia próprias: `id_cliente` com prefixo `PH`, nome em
maiúsculas, CPF com máscara e `cod_trecho` no lugar da rota. Cada evento de venda é um
documento aninhado (`pedido`, `comprador.contato`, `viagem.origem`/`destino`, `valores`):

| `tipo` | Quando |
|---|---|
| `venda` | na compra |
| `cancelamento` | entre a compra e a 0h do dia da viagem |
| `remarcacao` | idem, com `viagem.data` adiada |

A API devolve os eventos de cada dia, em ordem de `ocorrido_em`, em páginas de
`eventos_por_pagina`. Cada página é um arquivo com um único documento JSON (em uma linha):
`{"pagina", "por_pagina", "total_paginas", "total_eventos", "data_referencia", "eventos": [...]}`,
no caminho `passagemhub/eventos_venda/<AAAA-MM-DD>/eventos_venda_<AAAAMMDD>_<página>.json`.

*Schema evolution*: a partir da data do evento `coluna_nova_passagemhub` do roteiro, todo
evento traz o objeto novo `pagamento` (`forma`, `parcelas`); antes dela, nenhum traz.

Os pedidos de cada dia nascem do seed do lote daquele dia, e as mudanças são decididas na
venda; para gerar um intervalo, a projeção reconstrói as vendas desde o dia mais antigo que
ainda pode ter mudança nele. O aninhamento é montado no Spark (`struct` + `to_json`) sobre
colunas planas, sem UDF. Números no `roteiro.yaml`; o preço segue o reajuste de tarifas da
Viação (`reajuste_tarifas_embarque`), porque o marketplace vende a mesma tarifa.

Defeitos (`defeitos.py`), sorteados por pedido (todos os eventos do pedido saem iguais),
aplicados na grafia do sistema e registrados com o arquivo da página: nome abreviado ou
com erro e contato nulo, como no Estudo de Caso; o CPF fora do padrão também é chamado e
só age se o roteiro der taxa ao *PassagemHub*.
"""

from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo, Defeitos, registros, tabela_de_registros
from ..escritor import Arquivo, caminho_landing
from ..mundo import linhas as linhas_na_escala
from ..sementes import semente
from .base import Contexto, Entrega, Lote, calendario, materializar

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

SISTEMA = "passagemhub"
ENTIDADE = "eventos_venda"
EVENTO_COLUNA_NOVA = "coluna_nova_passagemhub"
EVENTO_REAJUSTE = "reajuste_tarifas_embarque"


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    lotes, defeitos = projecao.paginas(inicio)
    return Entrega(lotes=lotes, defeitos=defeitos, mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.passagemhub
        self.fim = fim
        self.empresa = contexto.roteiro.sistemas[SISTEMA].empresa
        self.coluna_nova = contexto.roteiro.eventos[EVENTO_COLUNA_NOVA].data
        self.reajuste = contexto.roteiro.eventos[EVENTO_REAJUSTE]
        self.defeitos = Defeitos(contexto.roteiro, SISTEMA)

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    # -- cadastros vindos do mundo verdadeiro, na grafia do sistema ---------------------------

    @cached_property
    def clientes(self) -> "DataFrame":
        mundo = self.contexto.mundo
        participacoes = mundo["participacoes"].where(
            f"tipo_entidade = 'pessoa' AND papel = 'cliente' AND empresa = '{self.empresa}'"
        ).selectExpr("id_entidade AS id_pessoa", "desde")
        return materializar(self.contexto, participacoes.join(mundo["pessoas"], "id_pessoa").selectExpr(
            "id_pessoa",
            f"concat('PH', cast({self.chave('clientes', 'id_pessoa', 8)} AS string)) AS id_cliente",
            "upper(nome) AS nome",
            "concat(substr(cpf, 1, 3), '.', substr(cpf, 4, 3), '.', substr(cpf, 7, 3), '-', substr(cpf, 10, 2)) AS cpf",
            "email", "telefone",
            "desde AS data_cadastro",
        ))

    @cached_property
    def trechos(self) -> "DataFrame":
        rotas = self.contexto.mundo["rotas"].where(f"empresa = '{self.empresa}'")
        percentual = self.reajuste.parametros["percentual"]
        return materializar(self.contexto, rotas.selectExpr(
            "id_rota",
            f"{self.chave('trechos', 'id_rota', 6)} AS cod_trecho",
            "cod_municipio_origem", "cod_municipio_destino", "tarifa_base",
            f"cast(round(tarifa_base * (1 + {percentual!r}), 2) AS decimal(10,2)) AS tarifa_reajustada",
        ))

    def mapa_chaves(self) -> "DataFrame":
        """Mapa acumulado: todos os clientes e trechos da Viação, qualquer que seja o
        intervalo pedido (o `avancar_tempo` regrava `gabarito.mapa_chaves` inteira)."""
        return mapa(self.clientes, SISTEMA, ENTIDADE, "id_cliente", "pessoas", "id_pessoa").unionByName(
            mapa(self.trechos, SISTEMA, ENTIDADE, "cod_trecho", "rotas", "id_rota")
        )

    # -- páginas da API -----------------------------------------------------------------------

    def paginas(self, inicio: date) -> tuple[list[Lote], list["DataFrame"]]:
        """Um lote por página de cada dia de `inicio` a `fim`, e o registro dos defeitos."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        por_pagina = self.p.eventos_por_pagina
        eventos = self.sujar(self.eventos(inicio))
        dia = F.to_date("ocorrido_em")
        no_dia = Window.partitionBy("data")
        numerados = (
            eventos.withColumn("data", dia)
            .where(F.col("data").between(F.lit(inicio), F.lit(self.fim)))
            .withColumn("_ordem", F.row_number().over(no_dia.orderBy("ocorrido_em", "id_evento")))
            .withColumn("total_eventos", F.count(F.lit(1)).over(no_dia))
            .withColumn("pagina", F.floor((F.col("_ordem") - 1) / por_pagina).cast("int") + 1)
            .withColumn("_evento", self.documento())
        )
        por_ordem = "(l, r) -> CASE WHEN l.o < r.o THEN -1 WHEN l.o > r.o THEN 1 ELSE 0 END"
        tabela = (
            numerados.groupBy("data", "pagina")
            .agg(
                F.max("total_eventos").alias("total_eventos"),
                F.count(F.lit(1)).alias("registros"),
                F.collect_list(F.struct(F.col("_ordem").alias("o"), F.col("_evento").alias("e"))).alias("_eventos"),
                F.collect_list(F.struct(F.col("_ordem").alias("o"), F.col("_defeitos").alias("d"))).alias("_anotacoes"),
            )
            .select(
                F.col("data").cast("string").alias("data"),
                F.lit(ENTIDADE).alias("entidade"),
                "pagina",
                "registros",
                F.to_json(F.struct(
                    F.col("pagina"),
                    F.lit(por_pagina).alias("por_pagina"),
                    F.ceil(F.col("total_eventos") / por_pagina).cast("int").alias("total_paginas"),
                    F.col("total_eventos"),
                    F.col("data").cast("string").alias("data_referencia"),
                    F.expr(f"transform(array_sort(_eventos, {por_ordem}), x -> x.e)").alias("eventos"),
                )).alias("linha"),
                F.expr(f"flatten(transform(array_sort(_anotacoes, {por_ordem}), x -> x.d))").alias("_defeitos"),
            )
            .orderBy("data", "pagina")
            .toArrow()
        )

        lotes, defeitos = [], []
        for i in range(tabela.num_rows):
            pagina = tabela.slice(i, 1)
            data_lote = date.fromisoformat(pagina.column("data")[0].as_py())
            caminho = caminho_landing(SISTEMA, ENTIDADE, data_lote, "json", pagina.column("pagina")[0].as_py())
            conteudo = f"{pagina.column('linha')[0].as_py()}\n".encode("utf-8")
            lotes.append(Lote(SISTEMA, ENTIDADE, data_lote, "json_aninhado", pagina.column("registros")[0].as_py(),
                              Arquivo(caminho, conteudo)))
            defeitos += [(*r[:-1], caminho) for r in registros(pagina, SISTEMA, seq_coluna=None)]
        registro = tabela_de_registros(self.contexto.spark, defeitos)
        return lotes, [registro] if registro is not None else []

    def documento(self) -> "Column":
        """O evento aninhado; `pagamento` só nos eventos a partir da data da coluna nova."""
        from pyspark.sql import functions as F

        def objeto(**campos):
            return F.struct(*[F.col(c).alias(nome) if isinstance(c, str) else c.alias(nome) for nome, c in campos.items()])

        return F.struct(
            F.col("id_evento"), F.col("tipo"), F.col("ocorrido_em"),
            objeto(codigo="codigo_pedido", canal="canal", criado_em="criado_em").alias("pedido"),
            objeto(
                id_cliente="id_cliente", nome="nome", cpf="cpf",
                contato=objeto(email="email", telefone="telefone"),
            ).alias("comprador"),
            objeto(
                cod_trecho="cod_trecho",
                origem=objeto(cod_municipio="cod_municipio_origem"),
                destino=objeto(cod_municipio="cod_municipio_destino"),
                data="data_viagem", poltrona="poltrona",
            ).alias("viagem"),
            objeto(tarifa="tarifa", taxa_servico="taxa_servico", total="total").alias("valores"),
            F.when(
                F.col("data") >= F.lit(self.coluna_nova),
                objeto(forma="forma_pagamento", parcelas="parcelas"),
            ).alias("pagamento"),
        )

    # -- defeitos ---------------------------------------------------------------------------

    def sujar(self, df: "DataFrame") -> "DataFrame":
        d = self.defeitos
        alvo = Alvo(ENTIDADE, ("codigo_pedido",), "pessoas", "_id_pessoa")
        df = d.cpf_fora_do_padrao(df, alvo, "cpf")
        df = d.nome_abreviado_ou_com_erro(df, alvo, "nome")
        df = d.contato_nulo(df, alvo, ("email", "telefone"))
        if "_defeitos" not in df.columns:
            from pyspark.sql import functions as F

            from ..defeitos import ANOTACAO

            df = df.withColumn("_defeitos", F.array().cast(f"array<{ANOTACAO}>"))
        return df

    # -- eventos e vendas ---------------------------------------------------------------------

    def eventos(self, inicio: date) -> "DataFrame":
        vendas = self.vendas(inicio)
        semente_evento = semente(self.roteiro.seed_base, SISTEMA, "id_evento")
        venda = "unix_seconds(criado_em)"
        # Mudança antes da viagem: entre a venda e a 0h do dia da viagem original.
        antes_da_viagem = (
            f"timestamp_seconds({venda} + 1 + cast(floor(_u_momento * "
            f"(unix_seconds(cast(data_viagem AS timestamp)) - {venda} - 1)) AS bigint))"
        )
        imagem = [
            "codigo_pedido", "canal", "criado_em", "id_cliente", "nome", "cpf", "email", "telefone",
            "cod_trecho", "cod_municipio_origem", "cod_municipio_destino", "poltrona",
            "tarifa", "taxa_servico", "total", "forma_pagamento", "parcelas", "_id_pessoa",
        ]

        def evento(df: "DataFrame", tipo: str, ocorrido: str, data_viagem: str) -> "DataFrame":
            h = f"sha2(concat_ws('|', '{semente_evento}', codigo_pedido, '{tipo}'), 256)"
            uuid = (
                f"concat_ws('-', substr({h}, 1, 8), substr({h}, 9, 4), substr({h}, 13, 4), "
                f"substr({h}, 17, 4), substr({h}, 21, 12))"
            )
            return df.selectExpr(
                f"{uuid} AS id_evento", f"'{tipo}' AS tipo", f"{ocorrido} AS ocorrido_em",
                f"{data_viagem} AS data_viagem", *imagem,
            )

        return (
            evento(vendas, "venda", "criado_em", "data_viagem")
            .unionByName(evento(vendas.where("_destino = 'cancelada'"), "cancelamento", antes_da_viagem, "data_viagem"))
            .unionByName(evento(
                vendas.where("_destino = 'remarcada'"), "remarcacao", antes_da_viagem,
                "date_add(data_viagem, _adiamento)",
            ))
        )

    def vendas(self, inicio: date) -> "DataFrame":
        """Pedidos vendidos desde o dia mais antigo que ainda pode ter mudança no intervalo."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        p, datas = self.p, self.roteiro.datas
        primeira_venda = max(datas.inicio, inicio - timedelta(days=p.antecedencia_compra_dias.maximo + 1))

        # Pedidos por dia: a parte do marketplace no volume do roteiro, dividida pelos dias.
        dias_historia = (datas.carga_inicial - datas.inicio).days + 1
        fracao = 1 - self.roteiro.parametros.embarque.fracao_passagens
        por_dia = linhas_na_escala(self.roteiro.volumes.passagens_embarque_passagemhub * fracao,
                                   self.contexto.escala) / dias_historia
        inteiro, resto = int(por_dia), por_dia - int(por_dia)

        # Clientes elegíveis num dia: os cadastrados até ele, numerados por ordem de cadastro.
        clientes = self.clientes.select(
            "*", (F.row_number().over(Window.orderBy("data_cadastro", "id_cliente")) - 1).alias("_indice_cliente"),
        )
        cadastros = clientes.groupBy(F.col("data_cadastro").alias("data")).agg(F.count(F.lit(1)).alias("_novos"))
        dias = (
            calendario(self.contexto, SISTEMA, self.fim)
            .join(cadastros, "data", "left")
            .withColumn("_clientes", F.sum(F.coalesce("_novos", F.lit(0))).over(
                Window.orderBy("data").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
            .where(F.col("data") >= F.lit(primeira_venda))
            .where("_clientes > 0")
            .withColumn("_n_dia", F.expr(
                f"{inteiro} + CASE WHEN pmod(xxhash64(semente, 0), 1000000) / 1000000.0 < {resto!r} THEN 1 ELSE 0 END"))
            .where("_n_dia > 0")
            .select("data", "semente", "_clientes", F.explode(F.sequence(F.lit(1), F.col("_n_dia"))).alias("n"))
        )
        trechos = self.trechos.select(
            "*", (F.row_number().over(Window.orderBy("cod_trecho")) - 1).alias("_indice_trecho"),
        )
        total_trechos = trechos.agg(F.count(F.lit(1)).alias("_trechos"))

        def u(k: int) -> str:
            return f"(pmod(xxhash64(semente, n, {k}), 1000000) / 1000000.0)"

        def sorteio(pesos: dict[str, float], k: int) -> str:
            nomes, total, acumulado, casos = sorted(pesos), sum(pesos.values()), 0.0, []
            for nome in nomes[:-1]:
                acumulado += pesos[nome] / total
                casos.append(f"WHEN {u(k)} < {acumulado!r} THEN '{nome}'")
            return f"CASE {' '.join(casos)} ELSE '{nomes[-1]}' END" if casos else f"'{nomes[-1]}'"

        horas, antecedencia, adiamento, parcelas = (
            p.horario_vendas, p.antecedencia_compra_dias, p.adiamento_remarcacao_dias, p.parcelas_cartao,
        )
        limite_cancelamento = p.cancelamento
        limite_remarcacao = limite_cancelamento + p.remarcacao
        destino = (
            f"CASE WHEN _antecedencia = 0 THEN 'normal' "
            f"WHEN {u(7)} < {limite_cancelamento!r} THEN 'cancelada' "
            f"WHEN {u(7)} < {limite_remarcacao!r} THEN 'remarcada' ELSE 'normal' END"
        )
        reajuste = self.reajuste.data.isoformat()

        return (
            dias.crossJoin(total_trechos)
            .selectExpr(
                "*",
                "concat('PH', date_format(data, 'yyyyMMdd'), lpad(cast(n AS string), 6, '0')) AS codigo_pedido",
                "pmod(xxhash64(semente, n, 1), _clientes) AS _indice_cliente",
                "pmod(xxhash64(semente, n, 2), _trechos) AS _indice_trecho",
                f"timestamp_seconds(unix_seconds(cast(data AS timestamp)) + {horas.minimo * 3600} "
                f"+ pmod(xxhash64(semente, n, 3), {(horas.maximo - horas.minimo) * 3600})) AS criado_em",
                f"{antecedencia.minimo} + pmod(xxhash64(semente, n, 4), "
                f"{antecedencia.maximo - antecedencia.minimo + 1}) AS _antecedencia",
                f"{sorteio(p.canais, 5)} AS canal",
                f"cast(1 + pmod(xxhash64(semente, n, 6), {p.poltronas}) AS int) AS poltrona",
                f"cast({adiamento.minimo} + pmod(xxhash64(semente, n, 8), "
                f"{adiamento.maximo - adiamento.minimo + 1}) AS int) AS _adiamento",
                f"{u(9)} AS _u_momento",
                f"{sorteio(p.formas_pagamento, 10)} AS forma_pagamento",
            )
            .withColumn("data_viagem", F.expr("date_add(data, cast(_antecedencia AS int))"))
            .withColumn("_destino", F.expr(destino))
            .withColumn("parcelas", F.expr(
                f"CASE WHEN forma_pagamento = 'cartao_credito' THEN cast({parcelas.minimo} + "
                f"pmod(xxhash64(semente, n, 11), {parcelas.maximo - parcelas.minimo + 1}) AS int) ELSE 1 END"))
            .join(clientes.selectExpr(
                "_indice_cliente", "id_pessoa AS _id_pessoa", "id_cliente", "nome", "cpf", "email", "telefone",
            ), "_indice_cliente")
            .join(trechos, "_indice_trecho")
            .withColumn("tarifa", F.expr(
                f"CASE WHEN data >= DATE'{reajuste}' THEN tarifa_reajustada ELSE tarifa_base END"))
            .withColumn("taxa_servico", F.expr(f"cast(round(tarifa * {p.taxa_servico!r}, 2) AS decimal(10,2))"))
            .withColumn("total", F.expr("cast(tarifa + taxa_servico AS decimal(10,2))"))
        )


__all__ = ["projetar"]
