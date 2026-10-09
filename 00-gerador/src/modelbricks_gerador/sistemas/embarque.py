"""*Embarque*: bilhetagem da ModelBricks Viação (guichê e site), entregue como CDC em JSON.

Entidades (todas com chave própria do sistema, ver `chaves.py`):

| Entidade | Chave | Origem no mundo verdadeiro | Operações |
|---|---|---|---|
| `clientes` | `id_cliente` | pessoas clientes da Viação | `I` no dia em que a pessoa vira cliente |
| `tarifas` | `id_tarifa` | uma por rota da Viação (`cod_linha`) | `I` no início da história; `U` em todas no reajuste roteirizado |
| `passagens` | `id_passagem` | fato do sistema (não existe no mundo) | `I` na venda; `U` na remarcação, no cancelamento e na utilização; `D` no estorno |

As passagens de cada dia nascem do seed do lote daquele dia (seed base, sistema, data); as
mudanças posteriores de uma passagem (remarcação, cancelamento, utilização) são decididas
na venda. Para gerar um intervalo qualquer, a projeção reconstrói as vendas desde o dia
mais antigo que ainda pode ter mudança no intervalo e entrega só as operações do intervalo.
Números (volumes, taxas, datas) vêm do `roteiro.yaml`.
"""

from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from ..cdc import Mudancas, lotes_cdc
from ..chaves import chave_do_sistema, mapa
from ..mundo import linhas as linhas_na_escala
from .base import Contexto, Entrega, calendario, materializar

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "embarque"
EVENTO_REAJUSTE = "reajuste_tarifas_embarque"
_SEGUNDOS_DIA = 86400


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    mudancas = [
        Mudancas("clientes", ("id_cliente",), projecao.mudancas_clientes()),
        Mudancas("tarifas", ("id_tarifa",), projecao.mudancas_tarifas()),
        Mudancas("passagens", ("id_passagem",), projecao.mudancas_passagens(inicio)),
    ]
    return Entrega(lotes=lotes_cdc(SISTEMA, mudancas, inicio, fim), mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.embarque
        self.fim = fim
        self.reajuste = contexto.roteiro.eventos[EVENTO_REAJUSTE]
        self.empresa = contexto.roteiro.sistemas[SISTEMA].empresa

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    # -- cadastros vindos do mundo verdadeiro ---------------------------------------------

    @cached_property
    def clientes(self) -> "DataFrame":
        mundo = self.contexto.mundo
        participacoes = mundo["participacoes"].where(
            f"tipo_entidade = 'pessoa' AND papel = 'cliente' AND empresa = '{self.empresa}'"
        ).selectExpr("id_entidade AS id_pessoa", "desde")
        return materializar(self.contexto, participacoes.join(mundo["pessoas"], "id_pessoa").selectExpr(
            "id_pessoa",
            f"{self.chave('clientes', 'id_pessoa', 8)} AS id_cliente",
            "nome", "cpf", "data_nascimento", "email", "telefone", "cod_municipio",
            "desde AS data_cadastro",
        ))

    @cached_property
    def linhas(self) -> "DataFrame":
        rotas = self.contexto.mundo["rotas"].where(f"empresa = '{self.empresa}'")
        return materializar(self.contexto, rotas.selectExpr(
            "id_rota",
            f"{self.chave('tarifas', 'id_rota', 6)} AS id_tarifa",
            f"{self.chave('linhas', 'id_rota', 5)} AS cod_linha",
            "cod_municipio_origem", "cod_municipio_destino", "tarifa_base",
            f"{self.valor_reajustado('tarifa_base')} AS tarifa_reajustada",
        ))

    def valor_reajustado(self, coluna: str) -> str:
        percentual = self.reajuste.parametros["percentual"]
        return f"cast(round({coluna} * (1 + {percentual!r}), 2) AS decimal(10,2))"

    def mapa_chaves(self) -> "DataFrame":
        linhas_ = self.linhas
        return (
            mapa(self.clientes, SISTEMA, "clientes", "id_cliente", "pessoas", "id_pessoa")
            .unionByName(mapa(linhas_, SISTEMA, "tarifas", "id_tarifa", "rotas", "id_rota"))
            .unionByName(mapa(linhas_, SISTEMA, "tarifas", "cod_linha", "rotas", "id_rota"))
        )

    # -- mudanças por entidade ------------------------------------------------------------

    def mudancas_clientes(self) -> "DataFrame":
        """`I` no dia do cadastro, de madrugada (antes de qualquer venda daquele dia)."""
        antes_das_vendas = self.p.horario_vendas.minimo * 3600
        return (
            self.clientes
            .join(calendario(self.contexto, SISTEMA, self.fim).withColumnRenamed("data", "data_cadastro"), "data_cadastro")
            .selectExpr(
                "id_cliente", "'I' AS op",
                f"timestamp_seconds(unix_seconds(cast(data_cadastro AS timestamp)) "
                f"+ pmod(xxhash64(semente, id_pessoa), {antes_das_vendas})) AS ts_operacao",
                "nome", "cpf", "data_nascimento", "email", "telefone", "cod_municipio", "data_cadastro",
            )
        )

    def mudancas_tarifas(self) -> "DataFrame":
        """`I` à 0h do primeiro dia da história; `U` de todas à 0h do dia do reajuste."""
        imagem = ("cod_linha", "cod_municipio_origem", "cod_municipio_destino")
        inicio = self.roteiro.datas.inicio.isoformat()
        reajuste = self.reajuste.data.isoformat()
        base = self.linhas
        criacao = base.selectExpr(
            "id_tarifa", "'I' AS op", f"cast(DATE'{inicio}' AS timestamp) AS ts_operacao", *imagem,
            "tarifa_base AS valor", f"DATE'{inicio}' AS vigente_desde",
        )
        reajustes = base.selectExpr(
            "id_tarifa", "'U' AS op", f"cast(DATE'{reajuste}' AS timestamp) AS ts_operacao", *imagem,
            "tarifa_reajustada AS valor", f"DATE'{reajuste}' AS vigente_desde",
        )
        return criacao.unionByName(reajustes)

    def mudancas_passagens(self, inicio: date) -> "DataFrame":
        vendas = self.vendas(inicio)
        imagem = ["id_cliente", "cod_linha", "data_hora_venda", "data_viagem", "poltrona", "canal", "valor"]

        def operacao(df: "DataFrame", op: str, ts: str, data_viagem: str, status: str) -> "DataFrame":
            return df.selectExpr(
                "id_passagem", f"'{op}' AS op", f"timestamp_seconds({ts}) AS ts_operacao",
                *[c if c != "data_viagem" else f"{data_viagem} AS data_viagem" for c in imagem],
                f"'{status}' AS status",
            )

        venda = "unix_seconds(data_hora_venda)"
        # Mudança antes da viagem: entre a venda e a 0h do dia da viagem original.
        antes_da_viagem = (
            f"{venda} + 1 + cast(floor(_u_momento * "
            f"(unix_seconds(cast(data_viagem AS timestamp)) - {venda} - 1)) AS bigint)"
        )
        viagem_final = "CASE WHEN _destino = 'remarcada' THEN date_add(data_viagem, _adiamento) ELSE data_viagem END"
        # A utilização é registrada no fim do dia da viagem (entre 23h e 23h30).
        utilizacao = f"unix_seconds(cast({viagem_final} AS timestamp)) + 82800 + pmod(xxhash64(semente, n, 11), 1800)"
        return (
            operacao(vendas, "I", venda, "data_viagem", "emitida")
            .unionByName(operacao(
                vendas.where("_destino = 'estornada'"), "D",
                f"{venda} + 60 + pmod(xxhash64(semente, n, 10), 3540)", "data_viagem", "estornada",
            ))
            .unionByName(operacao(vendas.where("_destino = 'cancelada'"), "U", antes_da_viagem, "data_viagem", "cancelada"))
            .unionByName(operacao(
                vendas.where("_destino = 'remarcada'"), "U", antes_da_viagem,
                "date_add(data_viagem, _adiamento)", "remarcada",
            ))
            .unionByName(operacao(
                vendas.where("_destino IN ('normal', 'remarcada')"), "U", utilizacao, viagem_final, "utilizada",
            ))
        )

    # -- vendas -----------------------------------------------------------------------------

    def vendas(self, inicio: date) -> "DataFrame":
        """Passagens vendidas desde o dia mais antigo que ainda pode mudar no intervalo."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        p, datas = self.p, self.roteiro.datas
        defasagem = p.antecedencia_compra_dias.maximo + p.adiamento_remarcacao_dias.maximo + 1
        primeira_venda = max(datas.inicio, inicio - timedelta(days=defasagem))

        # Passagens por dia: o volume do roteiro dividido pelos dias da história.
        dias_historia = (datas.carga_inicial - datas.inicio).days + 1
        por_dia = linhas_na_escala(self.roteiro.volumes.passagens_embarque_passagemhub * p.fracao_passagens,
                         self.contexto.escala) / dias_historia
        inteiro, fracao = int(por_dia), por_dia - int(por_dia)

        # Clientes elegíveis num dia: os cadastrados até ele, numerados por ordem de cadastro.
        clientes = self.clientes.select(
            "id_cliente", "data_cadastro",
            (F.row_number().over(Window.orderBy("data_cadastro", "id_cliente")) - 1).alias("_indice_cliente"),
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
                f"{inteiro} + CASE WHEN pmod(xxhash64(semente, 0), 1000000) / 1000000.0 < {fracao!r} THEN 1 ELSE 0 END"))
            .where("_n_dia > 0")
            .select("data", "semente", "_clientes", F.explode(F.sequence(F.lit(1), F.col("_n_dia"))).alias("n"))
        )

        linhas_ = self.linhas.select(
            "cod_linha", "tarifa_base", "tarifa_reajustada",
            (F.row_number().over(Window.orderBy("cod_linha")) - 1).alias("_indice_linha"),
        )
        total_linhas = linhas_.agg(F.count(F.lit(1)).alias("_linhas"))

        def u(k: int) -> str:
            return f"(pmod(xxhash64(semente, n, {k}), 1000000) / 1000000.0)"

        horas = p.horario_vendas
        antecedencia = p.antecedencia_compra_dias
        adiamento = p.adiamento_remarcacao_dias
        canais = sorted(p.canais)
        total_pesos = sum(p.canais.values())
        acumulado, casos = 0.0, []
        for canal in canais[:-1]:
            acumulado += p.canais[canal] / total_pesos
            casos.append(f"WHEN {u(5)} < {acumulado!r} THEN '{canal}'")
        canal = f"CASE {' '.join(casos)} ELSE '{canais[-1]}' END" if casos else f"'{canais[-1]}'"
        limite_estorno = p.estorno
        limite_cancelamento = limite_estorno + p.cancelamento
        limite_remarcacao = limite_cancelamento + p.remarcacao
        destino = (
            f"CASE WHEN {u(7)} < {limite_estorno!r} THEN 'estornada' "
            f"WHEN _antecedencia = 0 THEN 'normal' "
            f"WHEN {u(7)} < {limite_cancelamento!r} THEN 'cancelada' "
            f"WHEN {u(7)} < {limite_remarcacao!r} THEN 'remarcada' ELSE 'normal' END"
        )
        reajuste = self.reajuste.data.isoformat()

        return (
            dias.crossJoin(total_linhas)
            .selectExpr(
                "*",
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 100000 + n AS id_passagem",
                "pmod(xxhash64(semente, n, 1), _clientes) AS _indice_cliente",
                "pmod(xxhash64(semente, n, 2), _linhas) AS _indice_linha",
                f"timestamp_seconds(unix_seconds(cast(data AS timestamp)) + {horas.minimo * 3600} "
                f"+ pmod(xxhash64(semente, n, 3), {(horas.maximo - horas.minimo) * 3600})) AS data_hora_venda",
                f"{antecedencia.minimo} + pmod(xxhash64(semente, n, 4), "
                f"{antecedencia.maximo - antecedencia.minimo + 1}) AS _antecedencia",
                f"{canal} AS canal",
                f"1 + pmod(xxhash64(semente, n, 6), {p.poltronas}) AS poltrona",
                f"{adiamento.minimo} + pmod(xxhash64(semente, n, 8), "
                f"{adiamento.maximo - adiamento.minimo + 1}) AS _adiamento",
                f"{u(9)} AS _u_momento",
            )
            .withColumn("data_viagem", F.expr("date_add(data, cast(_antecedencia AS int))"))
            .withColumn("_destino", F.expr(destino))
            .join(clientes.select("id_cliente", "_indice_cliente"), "_indice_cliente")
            .join(linhas_, "_indice_linha")
            .withColumn("valor", F.expr(
                f"CASE WHEN data >= DATE'{reajuste}' THEN tarifa_reajustada ELSE tarifa_base END"))
            .withColumn("_adiamento", F.col("_adiamento").cast("int"))
        )


__all__ = ["projetar"]
