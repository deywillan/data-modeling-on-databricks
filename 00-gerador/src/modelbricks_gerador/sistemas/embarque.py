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

Defeitos (`defeitos.py`), aplicados só quando o roteiro dá taxa ao *Embarque*: pessoa
duplicada, CPF fora do padrão, nome abreviado e contato nulo nos `clientes`; FK órfã
(`cod_linha` inexistente) nas `passagens`; evento atrasado e CDC duplicado (reentrega) em
todas as entidades. O roteiro do Estudo de Caso só ativa o CDC duplicado.
"""

from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from dataclasses import replace

from ..cdc import Mudancas, entrega_cdc
from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo, Defeitos
from ..expressoes import instante, sorteio, uniforme_do_lote
from .base import Contexto, Entrega, calendario, linhas_por_dia, materializar, volume_por_dia

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "embarque"
EVENTO_REAJUSTE = "reajuste_tarifas_embarque"
_SEGUNDOS_DIA = 86400


# Margem de ids (acima de qualquer id verdadeiro) para chaves que não existem no mundo:
# cópias duplicadas de clientes e linhas inexistentes das FKs órfãs.
_FORA_DO_MUNDO_CLIENTES = 50_000_000
_FORA_DO_MUNDO_LINHAS = 50_000


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    mudancas = [
        Mudancas("clientes", ("id_cliente",), projecao.sujar_clientes(projecao.mudancas_clientes())),
        Mudancas("tarifas", ("id_tarifa",), projecao.sujar_entrega(projecao.mudancas_tarifas(), Alvo("tarifas", ("id_tarifa",)))),
        Mudancas("passagens", ("id_passagem",), projecao.sujar_passagens(projecao.mudancas_passagens(inicio))),
    ]
    return replace(entrega_cdc(SISTEMA, mudancas, inicio, fim), mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.embarque
        self.fim = fim
        self.reajuste = contexto.roteiro.eventos[EVENTO_REAJUSTE]
        self.empresa = contexto.roteiro.sistemas[SISTEMA].empresa
        self.defeitos = Defeitos(contexto.roteiro, SISTEMA)

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

    @property
    def alvo_clientes(self) -> Alvo:
        return Alvo("clientes", ("id_cliente",), "pessoas", "_id_verdadeiro")

    def chave_da_copia(self, coluna_id: str) -> str:
        """Chave do sistema da cópia duplicada de um cliente (id verdadeiro deslocado para fora do mundo)."""
        return self.chave("clientes", f"({coluna_id} + {_FORA_DO_MUNDO_CLIENTES})", 8)

    def mapa_chaves(self) -> "DataFrame":
        """Mapa acumulado: todos os clientes, as cópias duplicadas deles e as linhas/tarifas."""
        linhas = self.linhas
        copias = self.defeitos.copias_de_pessoas(
            self.clientes, self.alvo_clientes, {"id_cliente": self.chave_da_copia("id_pessoa")})
        return (
            mapa(self.clientes, SISTEMA, "clientes", "id_cliente", "pessoas", "id_pessoa")
            .unionByName(mapa(copias, SISTEMA, "clientes", "id_cliente", "pessoas", "id_pessoa"))
            .unionByName(mapa(linhas, SISTEMA, "tarifas", "id_tarifa", "rotas", "id_rota"))
            .unionByName(mapa(linhas, SISTEMA, "tarifas", "cod_linha", "rotas", "id_rota"))
        )

    # -- defeitos ---------------------------------------------------------------------------

    def sujar_clientes(self, df: "DataFrame") -> "DataFrame":
        d = self.defeitos
        alvo = self.alvo_clientes
        df = d.pessoa_duplicada_no_sistema(df, alvo, {"id_cliente": self.chave_da_copia("_id_verdadeiro")})
        df = d.cpf_fora_do_padrao(df, alvo, "cpf")
        df = d.nome_abreviado_ou_com_erro(df, alvo, "nome")
        df = d.contato_nulo(df, alvo, ("email", "telefone"))
        return self.sujar_entrega(df, alvo)

    def sujar_passagens(self, df: "DataFrame") -> "DataFrame":
        alvo = Alvo("passagens", ("id_passagem",))
        inexistente = self.chave(
            "linhas", f"({_FORA_DO_MUNDO_LINHAS} + pmod(xxhash64(id_passagem), {_FORA_DO_MUNDO_LINHAS - 1}))", 5)
        return self.sujar_entrega(self.defeitos.fk_orfa(df, alvo, "cod_linha", inexistente), alvo)

    def sujar_entrega(self, df: "DataFrame", alvo: Alvo) -> "DataFrame":
        return self.defeitos.cdc_duplicado(self.defeitos.evento_atrasado(df, alvo), alvo)

    # -- mudanças por entidade ------------------------------------------------------------

    def mudancas_clientes(self) -> "DataFrame":
        """`I` no dia do cadastro, de madrugada (antes de qualquer venda daquele dia)."""
        antes_das_vendas = self.p.horario_vendas.minimo * 3600
        return (
            self.clientes
            .join(calendario(self.contexto, SISTEMA, self.fim).withColumnRenamed("data", "data_cadastro"), "data_cadastro")
            .selectExpr(
                "id_cliente", "id_pessoa AS _id_verdadeiro", "'I' AS op",
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
        # Folga dos defeitos de entrega: linhas de dias anteriores que chegam no intervalo.
        defasagem = (
            p.antecedencia_compra_dias.maximo + p.adiamento_remarcacao_dias.maximo + 1 + self.defeitos.folga_dias()
        )
        primeira_venda = max(datas.inicio, inicio - timedelta(days=defasagem))

        # Passagens por dia: o volume do roteiro dividido pelos dias da história.
        inteiro, fracao = volume_por_dia(
            self.contexto, self.roteiro.volumes.passagens_embarque_passagemhub * p.fracao_passagens)

        # Clientes elegíveis num dia: os cadastrados até ele, numerados por ordem de cadastro.
        clientes = self.clientes.select(
            "id_cliente", "data_cadastro",
            (F.row_number().over(Window.orderBy("data_cadastro", "id_cliente")) - 1).alias("_indice_cliente"),
        )
        cadastros = clientes.groupBy(F.col("data_cadastro").alias("data")).agg(F.count(F.lit(1)).alias("_novos"))
        dias = linhas_por_dia(
            calendario(self.contexto, SISTEMA, self.fim)
            .join(cadastros, "data", "left")
            .withColumn("_clientes", F.sum(F.coalesce("_novos", F.lit(0))).over(
                Window.orderBy("data").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
            .where(F.col("data") >= F.lit(primeira_venda))
            .where("_clientes > 0"),
            inteiro, fracao, "_clientes",
        )

        linhas_numeradas = self.linhas.select(
            "cod_linha", "tarifa_base", "tarifa_reajustada",
            (F.row_number().over(Window.orderBy("cod_linha")) - 1).alias("_indice_linha"),
        )
        total_linhas = linhas_numeradas.agg(F.count(F.lit(1)).alias("_linhas"))

        horas = p.horario_vendas
        antecedencia = p.antecedencia_compra_dias
        adiamento = p.adiamento_remarcacao_dias
        canal = sorteio(p.canais, uniforme_do_lote(5))
        limite_estorno = p.estorno
        limite_cancelamento = limite_estorno + p.cancelamento
        limite_remarcacao = limite_cancelamento + p.remarcacao
        destino = (
            f"CASE WHEN {uniforme_do_lote(7)} < {limite_estorno!r} THEN 'estornada' "
            f"WHEN _antecedencia = 0 THEN 'normal' "
            f"WHEN {uniforme_do_lote(7)} < {limite_cancelamento!r} THEN 'cancelada' "
            f"WHEN {uniforme_do_lote(7)} < {limite_remarcacao!r} THEN 'remarcada' ELSE 'normal' END"
        )
        reajuste = self.reajuste.data.isoformat()

        return (
            dias.crossJoin(total_linhas)
            .selectExpr(
                "*",
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 100000 + n AS id_passagem",
                "pmod(xxhash64(semente, n, 1), _clientes) AS _indice_cliente",
                "pmod(xxhash64(semente, n, 2), _linhas) AS _indice_linha",
                f"{instante('data', horas, 'semente', 'n', 3)} AS data_hora_venda",
                f"{antecedencia.minimo} + pmod(xxhash64(semente, n, 4), "
                f"{antecedencia.maximo - antecedencia.minimo + 1}) AS _antecedencia",
                f"{canal} AS canal",
                f"1 + pmod(xxhash64(semente, n, 6), {p.poltronas}) AS poltrona",
                f"{adiamento.minimo} + pmod(xxhash64(semente, n, 8), "
                f"{adiamento.maximo - adiamento.minimo + 1}) AS _adiamento",
                f"{uniforme_do_lote(9)} AS _u_momento",
            )
            .withColumn("data_viagem", F.expr("date_add(data, cast(_antecedencia AS int))"))
            .withColumn("_destino", F.expr(destino))
            .join(clientes.select("id_cliente", "_indice_cliente"), "_indice_cliente")
            .join(linhas_numeradas, "_indice_linha")
            .withColumn("valor", F.expr(
                f"CASE WHEN data >= DATE'{reajuste}' THEN tarifa_reajustada ELSE tarifa_base END"))
            .withColumn("_adiamento", F.col("_adiamento").cast("int"))
        )


__all__ = ["projetar"]
