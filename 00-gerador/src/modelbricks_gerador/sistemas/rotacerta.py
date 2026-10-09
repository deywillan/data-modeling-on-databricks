"""*RotaCerta*: TMS da ModelBricks Cargas (ordens de frete, coletas e entregas), entregue como
CDC em JSON, no mesmo formato do *Embarque* (motor `cdc.py`).

Entidades (todas com chave própria do sistema, ver `chaves.py`):

| Entidade | Chave | Origem no mundo verdadeiro | Operações |
|---|---|---|---|
| `clientes` | `id_cliente` | empresas PJ clientes da Cargas | `I` no dia em que a empresa vira cliente |
| `caminhoes` | `id_caminhao` | caminhões da frota própria da Cargas | `I` no dia da aquisição |
| `fretes` | `id_frete` | fato do sistema (não existe no mundo) | `I` na abertura; um `U` por mudança de status |

Cada frete percorre o ciclo de status do roteiro (`parametros.rotacerta.ciclo`) em
`parametros.eventos_por_frete` mudanças (a abertura inclusive): as etapas obrigatórias sempre
acontecem, e as opcionais sorteadas completam a quantidade. Cada mudança cai num dia
diferente, `dias_entre_eventos` depois da anterior; só o `status` muda na imagem da linha.
É o histórico que alimenta o *accumulating snapshot* do artigo de historização.

Os fretes de cada dia nascem do seed do lote daquele dia (seed base, sistema, data), e todo
o ciclo de um frete é decidido na abertura. Para gerar um intervalo qualquer, a projeção
reconstrói as aberturas desde o dia mais antigo que ainda pode ter mudança no intervalo e
entrega só as operações do intervalo. Números no `roteiro.yaml`.

Defeitos (`defeitos.py`), aplicados só quando o roteiro dá taxa ao *RotaCerta*: contato
nulo nos `clientes`; evento atrasado e CDC duplicado nos `fretes`. O roteiro do Estudo de
Caso só ativa o evento atrasado: a mudança de status chega dias depois, com `seq` maior que
a de mudanças posteriores do mesmo frete (*late-arriving*).
"""

from dataclasses import replace
from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from ..cdc import Mudancas, entrega_cdc
from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo, Defeitos
from ..mundo import linhas as linhas_na_escala
from .base import Contexto, Entrega, calendario, materializar

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "rotacerta"


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    mudancas = [
        Mudancas("clientes", ("id_cliente",), projecao.sujar_clientes(projecao.mudancas_clientes())),
        Mudancas("caminhoes", ("id_caminhao",), projecao.mudancas_caminhoes()),
        Mudancas("fretes", ("id_frete",), projecao.sujar_fretes(projecao.mudancas_fretes(inicio))),
    ]
    return replace(entrega_cdc(SISTEMA, mudancas, inicio, fim), mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.rotacerta
        self.fim = fim
        self.empresa = contexto.roteiro.sistemas[SISTEMA].empresa
        self.defeitos = Defeitos(contexto.roteiro, SISTEMA)

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    def calendario(self) -> "DataFrame":
        return calendario(self.contexto, SISTEMA, self.fim)

    # -- cadastros vindos do mundo verdadeiro ---------------------------------------------

    def participantes(self, tipo_entidade: str, papel: str) -> "DataFrame":
        return self.contexto.mundo["participacoes"].where(
            f"tipo_entidade = '{tipo_entidade}' AND papel = '{papel}' AND empresa = '{self.empresa}'"
        ).selectExpr("id_entidade AS _id_verdadeiro", "desde")

    @cached_property
    def clientes(self) -> "DataFrame":
        empresas = self.contexto.mundo["empresas_pj"].withColumnRenamed("id_empresa", "_id_verdadeiro")
        return materializar(self.contexto, self.participantes("empresa_pj", "cliente").join(
            empresas, "_id_verdadeiro"
        ).selectExpr(
            "_id_verdadeiro",
            f"{self.chave('clientes', '_id_verdadeiro', 6)} AS id_cliente",
            "cnpj", "razao_social", "email", "telefone", "cod_municipio",
            "desde AS data_cadastro",
        ))

    @cached_property
    def caminhoes(self) -> "DataFrame":
        veiculos = self.contexto.mundo["veiculos"].withColumnRenamed("id_veiculo", "_id_verdadeiro")
        return materializar(self.contexto, self.participantes("veiculo", "frota").join(
            veiculos, "_id_verdadeiro"
        ).selectExpr(
            "_id_verdadeiro",
            f"{self.chave('caminhoes', '_id_verdadeiro', 5)} AS id_caminhao",
            "placa", "marca", "modelo", "ano_fabricacao",
            "desde AS data_aquisicao",
        ))

    @cached_property
    def destinos(self) -> "DataFrame":
        """Municípios das UFs de atuação, numerados (destinos possíveis dos fretes)."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        ufs = ", ".join(f"'{uf}'" for uf in self.roteiro.mundo.ufs_atuacao)
        return materializar(self.contexto, self.contexto.mundo["municipios"].where(f"sigla_uf IN ({ufs})").select(
            "cod_municipio", (F.row_number().over(Window.orderBy("cod_municipio")) - 1).alias("_indice_destino"),
        ))

    def mapa_chaves(self) -> "DataFrame":
        """Mapa acumulado: todos os clientes e caminhões, qualquer que seja o intervalo."""
        return mapa(self.clientes, SISTEMA, "clientes", "id_cliente", "empresas_pj", "_id_verdadeiro").unionByName(
            mapa(self.caminhoes, SISTEMA, "caminhoes", "id_caminhao", "veiculos", "_id_verdadeiro")
        )

    # -- defeitos ---------------------------------------------------------------------------

    def sujar_clientes(self, df: "DataFrame") -> "DataFrame":
        alvo = Alvo("clientes", ("id_cliente",), "empresas_pj", "_id_verdadeiro")
        return self.defeitos.contato_nulo(df, alvo, ("email", "telefone"))

    def sujar_fretes(self, df: "DataFrame") -> "DataFrame":
        alvo = Alvo("fretes", ("id_frete",))
        return self.defeitos.cdc_duplicado(self.defeitos.evento_atrasado(df, alvo), alvo)

    # -- mudanças por entidade ------------------------------------------------------------

    def cadastro(self, df: "DataFrame", coluna_data: str, colunas: list[str]) -> "DataFrame":
        """`I` no dia do cadastro, de madrugada (antes de qualquer frete daquele dia)."""
        antes_da_operacao = self.p.horario_operacao.minimo * 3600
        return (
            df.join(self.calendario().withColumnRenamed("data", coluna_data), coluna_data)
            .selectExpr(
                colunas[0], "_id_verdadeiro", "'I' AS op",
                f"timestamp_seconds(unix_seconds(cast({coluna_data} AS timestamp)) "
                f"+ pmod(xxhash64(semente, _id_verdadeiro), {antes_da_operacao})) AS ts_operacao",
                *colunas[1:],
            )
        )

    def mudancas_clientes(self) -> "DataFrame":
        return self.cadastro(self.clientes, "data_cadastro", [
            "id_cliente", "cnpj", "razao_social", "email", "telefone", "cod_municipio", "data_cadastro",
        ])

    def mudancas_caminhoes(self) -> "DataFrame":
        return self.cadastro(self.caminhoes, "data_aquisicao", [
            "id_caminhao", "placa", "marca", "modelo", "ano_fabricacao", "data_aquisicao",
        ]).drop("_id_verdadeiro")

    def mudancas_fretes(self, inicio: date) -> "DataFrame":
        """`I` na abertura e um `U` por etapa seguinte do ciclo, cada uma num dia."""
        from pyspark.sql import functions as F

        p = self.p
        eventos = self.roteiro.parametros.eventos_por_frete
        horas = p.horario_operacao
        dias = p.dias_entre_eventos
        ciclo = p.ciclo
        status = f"array({', '.join(repr(e.status) for e in ciclo)})"
        opcionais = f"array({', '.join(str(i) for i, e in enumerate(ciclo) if e.opcional)})" if any(
            e.opcional for e in ciclo) else "cast(array() AS array<int>)"
        obrigatorias = sum(not e.opcional for e in ciclo)
        # Etapas opcionais sorteadas: as `n_eventos - obrigatorias` de menor hash.
        escolhidas = (
            f"filter({opcionais}, i -> size(filter({opcionais}, j -> "
            f"xxhash64(semente, n, 20, j) < xxhash64(semente, n, 20, i))) < _n_eventos - {obrigatorias})"
        )
        etapas = (
            f"filter(sequence(0, {len(ciclo) - 1}), i -> "
            f"NOT array_contains({opcionais}, i) OR array_contains({escolhidas}, i))"
        )
        # Dias desde a abertura até cada mudança (a abertura é a posição 0).
        intervalos = (
            f"transform(sequence(0, _n_eventos - 1), k -> CASE WHEN k = 0 THEN 0 ELSE "
            f"{dias.minimo} + pmod(xxhash64(semente, n, 30, k), {dias.maximo - dias.minimo + 1}) END)"
        )
        imagem = [
            "id_cliente", "id_caminhao", "cod_municipio_origem", "cod_municipio_destino", "peso_kg",
            "valor_frete", "data_hora_abertura", "data_prevista_entrega",
        ]
        no_dia = (
            f"timestamp_seconds(unix_seconds(cast(date_add(data, cast(_dias AS int)) AS timestamp)) + {horas.minimo * 3600} "
            f"+ pmod(xxhash64(semente, n, 40, _posicao), {(horas.maximo - horas.minimo) * 3600}))"
        )
        return (
            self.aberturas(inicio)
            .withColumn("_etapa", F.expr(
                f"transform({etapas}, (e, k) -> named_struct('posicao', k, 'status', element_at({status}, e + 1), "
                f"'dias', aggregate(slice({intervalos}, 1, k + 1), 0L, (a, x) -> a + x)))"))
            .withColumn("_etapa", F.explode("_etapa"))
            .selectExpr(
                "id_frete", "_etapa.posicao AS _posicao", "_etapa.dias AS _dias", "_etapa.status AS status",
                "data", "semente", "n", *imagem,
            )
            .selectExpr(
                "id_frete",
                "CASE WHEN _posicao = 0 THEN 'I' ELSE 'U' END AS op",
                f"CASE WHEN _posicao = 0 THEN data_hora_abertura ELSE {no_dia} END AS ts_operacao",
                *imagem, "status",
            )
        )

    # -- aberturas --------------------------------------------------------------------------

    def aberturas(self, inicio: date) -> "DataFrame":
        """Fretes abertos desde o dia mais antigo que ainda pode mudar no intervalo."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        p, datas = self.p, self.roteiro.datas
        eventos = self.roteiro.parametros.eventos_por_frete
        duracao = (eventos.maximo - 1) * p.dias_entre_eventos.maximo
        primeira = max(datas.inicio, inicio - timedelta(days=duracao + 1 + self.defeitos.folga_dias()))

        # Fretes por dia: o volume do roteiro dividido pelos dias da história.
        dias_historia = (datas.carga_inicial - datas.inicio).days + 1
        por_dia = linhas_na_escala(self.roteiro.volumes.fretes_rotacerta, self.contexto.escala) / dias_historia
        inteiro, fracao = int(por_dia), por_dia - int(por_dia)

        # Clientes e caminhões elegíveis num dia: os cadastrados até ele, numerados por ordem de cadastro.
        def numerados(df: "DataFrame", chave: str, data: str, indice: str) -> "DataFrame":
            return df.select(chave, data, (F.row_number().over(Window.orderBy(data, chave)) - 1).alias(indice))

        clientes = numerados(self.clientes, "id_cliente", "data_cadastro", "_indice_cliente")
        caminhoes = numerados(self.caminhoes, "id_caminhao", "data_aquisicao", "_indice_caminhao")
        acumulado = Window.orderBy("data").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        def novos(df: "DataFrame", data: str, saida: str) -> "DataFrame":
            return df.groupBy(F.col(data).alias("data")).agg(F.count(F.lit(1)).alias(saida))

        dias = (
            self.calendario()
            .join(novos(clientes, "data_cadastro", "_novos_clientes"), "data", "left")
            .join(novos(caminhoes, "data_aquisicao", "_novos_caminhoes"), "data", "left")
            .withColumn("_clientes", F.sum(F.coalesce("_novos_clientes", F.lit(0))).over(acumulado))
            .withColumn("_caminhoes", F.sum(F.coalesce("_novos_caminhoes", F.lit(0))).over(acumulado))
            .where(F.col("data") >= F.lit(primeira))
            .where("_clientes > 0 AND _caminhoes > 0")
            .withColumn("_n_dia", F.expr(
                f"{inteiro} + CASE WHEN pmod(xxhash64(semente, 0), 1000000) / 1000000.0 < {fracao!r} THEN 1 ELSE 0 END"))
            .where("_n_dia > 0")
            .select("data", "semente", "_clientes", "_caminhoes",
                    F.explode(F.sequence(F.lit(1), F.col("_n_dia"))).alias("n"))
        )
        total_destinos = self.destinos.agg(F.count(F.lit(1)).alias("_destinos"))

        horas, peso, prazo = p.horario_operacao, p.peso_kg, p.prazo_entrega_dias
        return (
            dias.crossJoin(total_destinos)
            .selectExpr(
                "*",
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 100000 + n AS id_frete",
                "pmod(xxhash64(semente, n, 1), _clientes) AS _indice_cliente",
                "pmod(xxhash64(semente, n, 2), _caminhoes) AS _indice_caminhao",
                "pmod(xxhash64(semente, n, 3), _destinos) AS _sorteio_destino",
                f"timestamp_seconds(unix_seconds(cast(data AS timestamp)) + {horas.minimo * 3600} "
                f"+ pmod(xxhash64(semente, n, 4), {(horas.maximo - horas.minimo) * 3600})) AS data_hora_abertura",
                f"cast({peso.minimo} + pmod(xxhash64(semente, n, 5), {peso.maximo - peso.minimo + 1}) AS int) AS peso_kg",
                f"date_add(data, cast({prazo.minimo} + pmod(xxhash64(semente, n, 6), "
                f"{prazo.maximo - prazo.minimo + 1}) AS int)) AS data_prevista_entrega",
                f"cast({eventos.minimo} + pmod(xxhash64(semente, n, 7), {eventos.maximo - eventos.minimo + 1}) AS int) "
                "AS _n_eventos",
            )
            .join(clientes.select("id_cliente", "_indice_cliente"), "_indice_cliente")
            .join(caminhoes.select("id_caminhao", "_indice_caminhao"), "_indice_caminhao")
            .join(self.clientes.selectExpr("id_cliente", "cod_municipio AS cod_municipio_origem"), "id_cliente")
            # O destino sorteado nunca é a origem: se coincidir, vale o município seguinte.
            .join(self.destinos.withColumnRenamed("cod_municipio", "_destino"),
                  F.col("_indice_destino") == F.col("_sorteio_destino"))
            .join(self.destinos.selectExpr("cod_municipio AS _seguinte", "_indice_destino AS _indice_seguinte"),
                  F.col("_indice_seguinte") == F.expr("pmod(_sorteio_destino + 1, _destinos)"))
            .withColumn("cod_municipio_destino", F.expr(
                "CASE WHEN _destino = cod_municipio_origem THEN _seguinte ELSE _destino END"))
            .withColumn("valor_frete", F.expr(f"cast(round(peso_kg * {p.frete_por_kg!r}, 2) AS decimal(12,2))"))
        )


__all__ = ["projetar"]
