"""*Pátio*: DMS da ModelBricks Veículos (venda de veículos e oficina), entregue como
snapshot completo em CSV.

Entidades (todas com chave própria do sistema, ver `chaves.py`):

| Entidade | Chave | Origem | Mudanças |
|---|---|---|---|
| `lojas` | `id_loja` | lojas da concessionária no mundo verdadeiro | nenhuma |
| `clientes` | `id_cliente` | clientes PF e PJ da ModelBricks Veículos e as empresas do grupo que compram a frota própria | cadastro; depois, atualizações de telefone, e-mail ou endereço |
| `veiculos` | `id_veiculo` | veículos vendidos pela concessionária (inclusive a frota própria) | entra no pátio (`em_estoque`); vira `vendido` na venda |
| `vendas` | `id_venda` | vendas de veículos do mundo verdadeiro | nenhuma |
| `ordens_servico` | `id_ordem_servico` | fato do sistema (oficina), não existe no mundo | abre (`aberta`); conclui (`concluida`) ou é cancelada e excluída |

O cliente é cadastrado no dia em que passa a ser cliente da concessionária ou, se antes,
no dia da primeira compra. As empresas do grupo são cadastradas no início da história.
As ordens de serviço de cada dia nascem do seed do lote daquele dia (seed base, sistema,
data) e atendem veículos já vendidos. As atualizações cadastrais são do DMS, não do mundo
verdadeiro: o gabarito guarda o cadastro verdadeiro, e o DMS, a versão que o cliente
informou depois.

O histórico de mudanças de cada entidade vai para o motor de snapshot (`snapshot.py`), que
tira as fotos nas datas da agenda do roteiro (`parametros.patio.snapshots`). Números
(volumes, taxas, datas, frequência) vêm do `roteiro.yaml`.

Defeitos (`defeitos.py`), aplicados só quando o roteiro dá taxa ao *Pátio*: município fora
do padrão IBGE e contato nulo nos `clientes`.
"""

from dataclasses import replace
from datetime import date
from functools import cached_property, reduce
from typing import TYPE_CHECKING

from ..cdc import Mudancas
from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo, Defeitos
from ..mundo import linhas as linhas_na_escala
from ..sementes import semente
from ..snapshot import datas_de_snapshot, entrega_snapshot
from .base import Contexto, Entrega, calendario, materializar

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "patio"
# Id único dos clientes antes da chave do sistema: pessoas, empresas e empresas do grupo
# em faixas separadas, para que as chaves nunca colidam.
_FAIXA_PJ = 10_000_000
_FAIXA_GRUPO = 20_000_000
_NUMERO_MAXIMO = 3999  # números de endereço, como no mundo verdadeiro
_CNPJ_PESOS = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    datas = datas_de_snapshot(projecao.p.snapshots, contexto.roteiro.datas.inicio, fim)
    if not any(inicio <= d <= fim for d in datas):
        return Entrega()
    historicos = [
        Mudancas("lojas", ("id_loja",), projecao.historico_lojas()),
        Mudancas("clientes", ("id_cliente",), projecao.sujar_clientes(projecao.historico_clientes())),
        Mudancas("veiculos", ("id_veiculo",), projecao.historico_veiculos()),
        Mudancas("vendas", ("id_venda",), projecao.historico_vendas()),
        Mudancas("ordens_servico", ("id_ordem_servico",), projecao.historico_ordens_servico()),
    ]
    formato = contexto.roteiro.sistemas[SISTEMA].formato
    entrega = entrega_snapshot(SISTEMA, historicos, datas, inicio, fim, formato)
    return replace(entrega, mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.patio
        self.fim = fim
        self.defeitos = Defeitos(contexto.roteiro, SISTEMA)

    # -- utilitários ------------------------------------------------------------------------

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    def hash(self, nome: str, *colunas: str) -> str:
        return f"xxhash64({semente(self.roteiro.seed_base, SISTEMA, nome)}, {', '.join(colunas)})"

    def uniforme(self, nome: str, *colunas: str) -> str:
        return f"(pmod({self.hash(nome, *colunas)}, 1000000) / 1000000.0)"

    def no_intervalo(self, nome: str, intervalo, *colunas: str) -> str:
        return f"({intervalo.minimo} + pmod({self.hash(nome, *colunas)}, {intervalo.maximo - intervalo.minimo + 1}))"

    def no_expediente(self, dia: str, nome: str, *colunas: str) -> str:
        """Instante dentro do horário de operação do dia."""
        h = self.p.horario_operacao
        return (
            f"timestamp_seconds(unix_seconds(cast({dia} AS timestamp)) + {h.minimo * 3600} "
            f"+ pmod({self.hash(nome, *colunas)}, {(h.maximo - h.minimo) * 3600}))"
        )

    def antes_do_expediente(self, dia: str, nome: str, *colunas: str) -> str:
        """Instante na hora anterior à abertura (cadastros e entradas antes de vendas do dia)."""
        inicio = (self.p.horario_operacao.minimo - 1) * 3600
        return f"timestamp_seconds(unix_seconds(cast({dia} AS timestamp)) + {inicio} + pmod({self.hash(nome, *colunas)}, 3600))"

    def telefone(self, ddd: str, nome: str, *colunas: str) -> str:
        return (
            f"concat('(', {ddd}, ') 9', lpad(cast(pmod({self.hash(nome, *colunas, '1')}, 10000) AS string), 4, '0'), "
            f"'-', lpad(cast(pmod({self.hash(nome, *colunas, '2')}, 10000) AS string), 4, '0'))"
        )

    def endereco(self, nome: str, *colunas: str) -> dict[str, str]:
        """Endereço sorteado (precisa das colunas `_municipios` e `_logradouros` dos pools)."""
        def h(k: str) -> str:
            return self.hash(nome, *colunas, k)

        return {
            "cod_municipio": f"element_at(_municipios, cast(pmod({h('1')}, size(_municipios)) AS int) + 1)",
            "logradouro": f"element_at(_logradouros, cast(pmod({h('2')}, size(_logradouros)) AS int) + 1)",
            "numero": f"cast(1 + pmod({h('3')}, {_NUMERO_MAXIMO}) AS int)",
            "cep": (
                f"concat(lpad(cast(pmod({h('4')}, 100000) AS string), 5, '0'), '-', "
                f"lpad(cast(pmod({h('5')}, 1000) AS string), 3, '0'))"
            ),
        }

    # -- cadastros vindos do mundo verdadeiro ---------------------------------------------

    @cached_property
    def pools(self) -> "DataFrame":
        """Uma linha com os municípios de atuação e os logradouros do mundo (para mudanças)."""
        from pyspark.sql import functions as F

        mundo = self.contexto.mundo
        municipios = sorted(
            r[0] for r in mundo["municipios"].where(F.col("sigla_uf").isin(self.roteiro.mundo.ufs_atuacao))
            .select("cod_municipio").collect()
        )
        logradouros = sorted(r[0] for r in mundo["pessoas"].select("logradouro").distinct().collect())
        return self.contexto.spark.createDataFrame(
            [(municipios, logradouros)], "_municipios array<int>, _logradouros array<string>"
        )

    @cached_property
    def vendas(self) -> "DataFrame":
        """Vendas do mundo com os dados do veículo, as chaves do DMS e os instantes."""
        mundo = self.contexto.mundo
        grupo = sorted(self.p.clientes_grupo)
        indice_grupo = "CASE empresa_compradora " + " ".join(f"WHEN '{e}' THEN {i}" for i, e in enumerate(grupo)) + " END"
        comprador = (
            "CASE tipo_comprador WHEN 'pf' THEN id_pessoa "
            f"WHEN 'pj' THEN {_FAIXA_PJ} + id_empresa ELSE {_FAIXA_GRUPO} + {indice_grupo} END"
        )
        veiculos = mundo["veiculos"].select("id_veiculo", "placa", "marca", "modelo", "tipo", "ano_fabricacao")
        vendas = mundo["vendas_veiculos"].join(veiculos, "id_veiculo").selectExpr(
            "id_venda AS _id_venda", "id_veiculo AS _id_veiculo", "id_loja AS _id_loja",
            f"cast({comprador} AS bigint) AS _id_cliente",
            "placa", "marca", "modelo", "tipo", "ano_fabricacao", "data_venda", "valor_venda",
        )
        estoque = self.no_intervalo("estoque", self.p.dias_em_estoque, "_id_venda")
        return materializar(self.contexto, vendas.selectExpr(
            "*",
            f"{self.chave('vendas', '_id_venda', 7)} AS id_venda",
            f"{self.chave('veiculos', '_id_veiculo', 7)} AS id_veiculo",
            f"CASE WHEN _id_loja IS NOT NULL THEN {self.chave('lojas', '_id_loja', 4)} END AS id_loja",
            f"{self.chave('clientes', '_id_cliente', 8)} AS id_cliente",
            f"{self.no_expediente('data_venda', 'venda', '_id_venda')} AS _ts_venda",
            f"greatest(date_sub(data_venda, cast({estoque} AS int)), DATE'{self.roteiro.datas.inicio.isoformat()}') "
            "AS _dia_entrada",
        ))

    @cached_property
    def clientes(self) -> "DataFrame":
        """Cadastro inicial de cada cliente do DMS, com o id verdadeiro (`_id_verdadeiro`,
        na tabela `_tabela`) quando vem do mundo."""
        from pyspark.sql import functions as F

        mundo = self.contexto.mundo
        participacoes = mundo["participacoes"].where("papel = 'cliente' AND empresa = 'mb_veiculos'")
        colunas = [
            "_id_verdadeiro", "_tabela", "_id_cliente", "tipo_pessoa", "nome", "documento", "data_nascimento",
            "email", "telefone", "cod_municipio", "logradouro", "numero", "cep", "_desde",
        ]
        pf = participacoes.where("tipo_entidade = 'pessoa'").selectExpr("id_entidade AS id_pessoa", "desde").join(
            mundo["pessoas"], "id_pessoa"
        ).selectExpr(
            "cast(id_pessoa AS bigint) AS _id_verdadeiro", "'pessoas' AS _tabela", "cast(id_pessoa AS bigint) AS _id_cliente",
            "'F' AS tipo_pessoa", "nome", "cpf AS documento", "data_nascimento", "email", "telefone",
            "cast(cod_municipio AS int) AS cod_municipio", "logradouro", "cast(numero_endereco AS int) AS numero", "cep",
            "desde AS _desde",
        )
        pj = participacoes.where("tipo_entidade = 'empresa_pj'").selectExpr("id_entidade AS id_empresa", "desde").join(
            mundo["empresas_pj"], "id_empresa"
        ).selectExpr(
            "cast(id_empresa AS bigint) AS _id_verdadeiro", "'empresas_pj' AS _tabela",
            f"cast({_FAIXA_PJ} + id_empresa AS bigint) AS _id_cliente",
            "'J' AS tipo_pessoa", "razao_social AS nome", "cnpj AS documento", "cast(NULL AS date) AS data_nascimento",
            "email", "telefone", "cast(cod_municipio AS int) AS cod_municipio", "logradouro",
            "cast(numero_endereco AS int) AS numero", "cep", "desde AS _desde",
        )
        grupo = self.contexto.spark.createDataFrame(
            [
                (_FAIXA_GRUPO + i, nome, _cnpj(semente(self.roteiro.seed_base, SISTEMA, "cnpj", empresa)),
                 f"frota@{empresa.removeprefix('mb_')}.modelbricks.com.br", self.roteiro.datas.inicio)
                for i, (empresa, nome) in enumerate(sorted(self.p.clientes_grupo.items()))
            ],
            "_id_cliente bigint, nome string, documento string, email string, _desde date",
        )
        ddds = ", ".join(f"'{d}'" for d in self.roteiro.catalogos.ddds)
        endereco = self.endereco("endereco_grupo", "_id_cliente")
        grupo = grupo.crossJoin(self.pools).selectExpr(
            "cast(NULL AS bigint) AS _id_verdadeiro", "cast(NULL AS string) AS _tabela", "_id_cliente",
            "'J' AS tipo_pessoa", "nome", "documento", "cast(NULL AS date) AS data_nascimento", "email",
            self.telefone(
                f"element_at(array({ddds}), cast(pmod({self.hash('ddd_grupo', '_id_cliente')}, "
                f"{len(self.roteiro.catalogos.ddds)}) AS int) + 1)",
                "telefone_grupo", "_id_cliente",
            ) + " AS telefone",
            *[f"{expressao} AS {coluna}" for coluna, expressao in endereco.items()],
            "_desde",
        )
        primeira_compra = self.vendas.groupBy("_id_cliente").agg(F.min("data_venda").alias("_primeira_compra"))
        todos = pf.select(*colunas).unionByName(pj.select(*colunas)).unionByName(grupo.select(*colunas))
        return materializar(self.contexto, todos.join(primeira_compra, "_id_cliente", "left").selectExpr(
            f"{self.chave('clientes', '_id_cliente', 8)} AS id_cliente",
            *colunas[:2], "_id_cliente", *colunas[3:-1],
            "least(_desde, coalesce(_primeira_compra, _desde)) AS data_cadastro",
        ))

    def mapa_chaves(self) -> "DataFrame":
        mundo = self.contexto.mundo
        clientes = self.clientes
        lojas = mundo["lojas"].where("empresa = 'mb_veiculos'").selectExpr(
            "id_loja AS _id_loja", f"{self.chave('lojas', 'id_loja', 4)} AS id_loja"
        )
        partes = [
            mapa(clientes.where("_tabela = 'pessoas'"), SISTEMA, "clientes", "id_cliente", "pessoas", "_id_verdadeiro"),
            mapa(clientes.where("_tabela = 'empresas_pj'"), SISTEMA, "clientes", "id_cliente", "empresas_pj", "_id_verdadeiro"),
            mapa(self.vendas, SISTEMA, "veiculos", "id_veiculo", "veiculos", "_id_veiculo"),
            mapa(self.vendas, SISTEMA, "vendas", "id_venda", "vendas_veiculos", "_id_venda"),
            mapa(lojas, SISTEMA, "lojas", "id_loja", "lojas", "_id_loja"),
        ]
        return reduce(lambda a, b: a.unionByName(b), partes)

    # -- defeitos ---------------------------------------------------------------------------

    def sujar_clientes(self, df: "DataFrame") -> "DataFrame":
        """Município fora do padrão e contato nulo, sorteados pela chave do cliente: a
        sujeira acompanha o cliente em todas as versões do cadastro."""
        from pyspark.sql import functions as F

        partes = []
        for tabela in ("pessoas", "empresas_pj", None):
            parte = df.where(F.col("_tabela") == tabela if tabela else F.col("_tabela").isNull())
            alvo = Alvo("clientes", ("id_cliente",), tabela, "_id_verdadeiro" if tabela else None)
            parte = self.defeitos.municipio_fora_do_padrao(parte, alvo, "cod_municipio")
            partes.append(self.defeitos.contato_nulo(parte, alvo, ("email", "telefone")))
        return reduce(lambda a, b: a.unionByName(b), partes)

    # -- histórico de mudanças por entidade -------------------------------------------------

    def historico_lojas(self) -> "DataFrame":
        inicio = self.roteiro.datas.inicio.isoformat()
        return self.contexto.mundo["lojas"].where("empresa = 'mb_veiculos'").selectExpr(
            f"{self.chave('lojas', 'id_loja', 4)} AS id_loja", "'I' AS op",
            f"cast(DATE'{inicio}' AS timestamp) AS ts_operacao", "nome", "cast(cod_municipio AS int) AS cod_municipio",
        )

    def historico_clientes(self) -> "DataFrame":
        """`I` no cadastro; depois, até `maximo_por_cliente` atualizações (telefone, e-mail ou
        endereço), espaçadas pelo intervalo do roteiro. As empresas do grupo não mudam."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        mc = self.p.mudancas_cadastro
        pesos = sorted(mc.pesos.items())
        total = sum(p for _, p in pesos)
        acumulado, casos = 0.0, []
        for tipo, peso in pesos[:-1]:
            acumulado += peso / total
            casos.append(f"WHEN {self.uniforme('tipo_mudanca', '_id_cliente', '_k')} < {acumulado!r} THEN '{tipo}'")
        tipo_mudanca = f"CASE {' '.join(casos)} ELSE '{pesos[-1][0]}' END" if casos else f"'{pesos[-1][0]}'"

        janela = Window.partitionBy("_id_cliente").orderBy("_k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        versoes = (
            self.clientes.crossJoin(self.pools)
            .withColumn("_k", F.explode(F.expr(
                f"sequence(0, CASE WHEN _tabela IS NULL THEN 0 ELSE {mc.maximo_por_cliente} END)")))
            .withColumn("_intervalo", F.expr(
                f"CASE WHEN _k = 0 THEN 0 ELSE {self.no_intervalo('intervalo_mudanca', mc.intervalo_dias, '_id_cliente', '_k')} END"))
            .withColumn("_dia", F.date_add("data_cadastro", F.sum("_intervalo").over(janela).cast("int")))
            .where(F.col("_dia") <= F.lit(self.fim))
            .withColumn("_tipo", F.expr(f"CASE WHEN _k = 0 THEN 'cadastro' ELSE {tipo_mudanca} END"))
        )
        for tipo in ("telefone", "email", "endereco"):
            versoes = versoes.withColumn(f"_k_{tipo}", F.max(F.when(F.col("_tipo") == tipo, F.col("_k"))).over(janela))

        dominios = ", ".join(f"'{d}'" for d in self.roteiro.catalogos.dominios_email)
        outros = f"array_remove(array({dominios}), substring_index(email, '@', -1))"
        novo_email = (
            f"concat(substring_index(email, '@', 1), '@', element_at({outros}, "
            f"cast(pmod({self.hash('email', '_id_cliente', '_k_email')}, greatest(size({outros}), 1)) AS int) + 1))"
        )
        novo_telefone = self.telefone("substr(telefone, 2, 2)", "telefone", "_id_cliente", "_k_telefone")
        novo_endereco = self.endereco("endereco", "_id_cliente", "_k_endereco")

        def mudou(tipo: str, coluna: str, expressao: str) -> str:
            return f"CASE WHEN _k_{tipo} IS NULL THEN {coluna} ELSE {expressao} END AS {coluna}"

        return versoes.selectExpr(
            "id_cliente",
            "CASE WHEN _k = 0 THEN 'I' ELSE 'U' END AS op",
            f"CASE WHEN _k = 0 THEN {self.antes_do_expediente('_dia', 'cadastro', '_id_cliente')} "
            f"ELSE {self.no_expediente('_dia', 'mudanca', '_id_cliente', '_k')} END AS ts_operacao",
            "tipo_pessoa", "nome", "documento", "data_nascimento",
            mudou("email", "email", f"coalesce({novo_email}, email)"),
            mudou("telefone", "telefone", novo_telefone),
            *[mudou("endereco", coluna, expressao) for coluna, expressao in novo_endereco.items()],
            "data_cadastro", "_id_verdadeiro", "_tabela",
        )

    def historico_veiculos(self) -> "DataFrame":
        """`I` na entrada no pátio (`em_estoque`); `U` na venda (`vendido`)."""
        imagem = ["placa", "marca", "modelo", "tipo", "ano_fabricacao"]
        entrada = self.antes_do_expediente("_dia_entrada", "entrada", "_id_veiculo")
        return self.vendas.selectExpr(
            "id_veiculo", "'I' AS op", f"{entrada} AS ts_operacao", *imagem,
            "'em_estoque' AS status", "_dia_entrada AS data_entrada",
        ).unionByName(self.vendas.selectExpr(
            "id_veiculo", "'U' AS op", "_ts_venda AS ts_operacao", *imagem,
            "'vendido' AS status", "_dia_entrada AS data_entrada",
        ))

    def historico_vendas(self) -> "DataFrame":
        return self.vendas.selectExpr(
            "id_venda", "'I' AS op", "_ts_venda AS ts_operacao", "id_veiculo", "id_cliente", "id_loja",
            "data_venda", "valor_venda",
        )

    def historico_ordens_servico(self) -> "DataFrame":
        """Ordens de serviço de cada dia para veículos já vendidos: `I` na abertura; `U` na
        conclusão ou `D` no cancelamento (a OS cancelada é excluída do DMS)."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        o, datas = self.p.ordens_servico, self.roteiro.datas
        dias_historia = (datas.carga_inicial - datas.inicio).days + 1
        por_dia = linhas_na_escala(self.roteiro.volumes.ordens_servico_patio, self.contexto.escala) / dias_historia
        inteiro, fracao = int(por_dia), por_dia - int(por_dia)

        # Veículos vendidos numerados pela ordem de venda: num dia, os elegíveis são os
        # vendidos nos dias anteriores.
        vendidos = self.vendas.select(
            "id_veiculo", "id_cliente", "data_venda",
            (F.row_number().over(Window.orderBy("data_venda", "_id_venda")) - 1).alias("_indice_veiculo"),
        )
        novos = vendidos.groupBy(F.col("data_venda").alias("data")).agg(F.count(F.lit(1)).alias("_novos"))
        dias = (
            calendario(self.contexto, SISTEMA, self.fim)
            .join(novos, "data", "left")
            .withColumn("_disponiveis", F.sum(F.coalesce("_novos", F.lit(0))).over(
                Window.orderBy("data").rowsBetween(Window.unboundedPreceding, -1)))
            .where("_disponiveis > 0")
            .withColumn("_n_dia", F.expr(
                f"{inteiro} + CASE WHEN pmod(xxhash64(semente, 0), 1000000) / 1000000.0 < {fracao!r} THEN 1 ELSE 0 END"))
            .where("_n_dia > 0")
            .select("data", "semente", "_disponiveis", F.explode(F.sequence(F.lit(1), F.col("_n_dia"))).alias("n"))
        )

        def u(k: int) -> str:
            return f"(pmod(xxhash64(semente, n, {k}), 1000000) / 1000000.0)"

        h, duracao, valor = self.p.horario_operacao, o.duracao_dias, o.valor
        tipos = ", ".join("'" + t.replace("'", "\\'") + "'" for t in o.tipos)
        ordens = dias.selectExpr(
            "*",
            "cast(date_format(data, 'yyyyMMdd') AS bigint) * 10000 + n AS id_ordem_servico",
            "pmod(xxhash64(semente, n, 1), _disponiveis) AS _indice_veiculo",
            f"timestamp_seconds(unix_seconds(cast(data AS timestamp)) + {h.minimo * 3600} "
            f"+ pmod(xxhash64(semente, n, 2), {(h.maximo - h.minimo) * 3600})) AS _ts_abertura",
            f"element_at(array({tipos}), cast(pmod(xxhash64(semente, n, 3), {len(o.tipos)}) AS int) + 1) AS tipo_servico",
            f"{duracao.minimo} + pmod(xxhash64(semente, n, 4), {duracao.maximo - duracao.minimo + 1}) AS _duracao",
            f"{u(5)} < {o.cancelamento!r} AS _cancelada",
            f"cast(round({valor.minimo} + {valor.maximo - valor.minimo} * {u(6)}, 2) AS decimal(12,2)) AS _valor",
        ).join(vendidos, "_indice_veiculo").selectExpr(
            "*",
            "timestamp_seconds(unix_seconds(_ts_abertura) + _duracao * 86400 + 3600 "
            "+ pmod(xxhash64(semente, n, 7), 10800)) AS _ts_fim",
        )
        imagem = ["id_veiculo", "id_cliente", "tipo_servico", "data AS data_abertura"]
        abertas = ordens.selectExpr(
            "id_ordem_servico", "'I' AS op", "_ts_abertura AS ts_operacao", *imagem, "'aberta' AS status",
            "cast(NULL AS date) AS data_conclusao", "cast(NULL AS decimal(12,2)) AS valor",
        )
        concluidas = ordens.where("NOT _cancelada").selectExpr(
            "id_ordem_servico", "'U' AS op", "_ts_fim AS ts_operacao", *imagem, "'concluida' AS status",
            "to_date(_ts_fim) AS data_conclusao", "_valor AS valor",
        )
        canceladas = ordens.where("_cancelada").selectExpr(
            "id_ordem_servico", "'D' AS op", "_ts_fim AS ts_operacao", *imagem, "'cancelada' AS status",
            "cast(NULL AS date) AS data_conclusao", "cast(NULL AS decimal(12,2)) AS valor",
        )
        return abertas.unionByName(concluidas).unionByName(canceladas)


def _cnpj(semente_: int) -> str:
    """CNPJ válido (matriz) derivado de um seed: raiz de 8 dígitos + 0001 + verificadores."""
    def verificador(digitos: str, pesos: list[int]) -> str:
        resto = sum(int(d) * p for d, p in zip(digitos, pesos)) % 11
        return "0" if resto < 2 else str(11 - resto)

    base = f"{semente_ % 10**8:08d}0001"
    dv1 = verificador(base, _CNPJ_PESOS)
    return base + dv1 + verificador(base + dv1, [6, *_CNPJ_PESOS])


__all__ = ["projetar"]
