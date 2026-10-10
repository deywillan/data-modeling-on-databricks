"""*SolTicket*: bilhetagem legada do Expresso Vale do Sol, entregue como carga histórica
única no dia da aquisição e depois como snapshot semanal, em CSV `;` com `latin1` (ver
`legados.py`).

Entidades (todas com chave própria do sistema, ver `chaves.py`):

| Entidade | Chave | Origem | Mudanças |
|---|---|---|---|
| `clientes` | `cod_cliente` | pessoas clientes do Expresso Vale do Sol | cadastro |
| `linhas` | `cod_linha` | rotas do Expresso Vale do Sol | nenhuma |
| `passagens` | `num_bilhete` | fato do sistema (não existe no mundo) | venda (`emitida`); cancelamento antes da viagem (`cancelada`) ou utilização no dia da viagem (`utilizada`) |

As pessoas que também são clientes da ModelBricks Viação aparecem aqui com outra chave e,
muitas vezes, outra grafia (CPF e nome sujos): a sobreposição com o *Embarque* só se
resolve pelo `gabarito.mapa_chaves`. As passagens de cada dia nascem do seed do lote
daquele dia (seed base, sistema, data). Números em `parametros.solticket` e
`volumes.passagens_solticket` (a carga histórica, na escala 1).

Defeitos (`defeitos.py`), aplicados só quando o roteiro dá taxa ao *SolTicket*: pessoa
duplicada, CPF fora do padrão, nome abreviado e município fora do padrão nos `clientes`;
FK órfã (`cod_linha` inexistente) nas `passagens`; `latin1` em todos os arquivos.
"""

from datetime import date
from functools import cached_property
from typing import TYPE_CHECKING

from ..cdc import Mudancas
from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo
from ..expressoes import instante, sorteio, uniforme_do_lote
from . import legados
from .base import Contexto, Entrega, calendario, linhas_por_dia, materializar

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "solticket"
EMPRESA = "evs"
# Margem de ids (acima de qualquer id verdadeiro) para chaves que não existem no mundo:
# cópias duplicadas de clientes e linhas inexistentes das FKs órfãs.
_FORA_DO_MUNDO_CLIENTES = 50_000_000
_FORA_DO_MUNDO_LINHAS = 50_000


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)

    def historicos() -> list[Mudancas]:
        return [
            Mudancas("clientes", ("cod_cliente",), projecao.sujar_clientes(projecao.historico_clientes())),
            Mudancas("linhas", ("cod_linha",), projecao.historico_linhas()),
            Mudancas("passagens", ("num_bilhete",), projecao.sujar_passagens(projecao.historico_passagens())),
        ]

    return legados.entregar(contexto, SISTEMA, inicio, fim, historicos, projecao.mapa_chaves)


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.solticket
        self.fim = fim
        self.defeitos = contexto.defeitos(SISTEMA)

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    # -- cadastros vindos do mundo verdadeiro ---------------------------------------------

    @cached_property
    def clientes(self) -> "DataFrame":
        mundo = self.contexto.mundo
        participacoes = mundo["participacoes"].where(
            f"tipo_entidade = 'pessoa' AND papel = 'cliente' AND empresa = '{EMPRESA}'"
        ).selectExpr("id_entidade AS id_pessoa", "desde")
        return materializar(self.contexto, participacoes.join(mundo["pessoas"], "id_pessoa").selectExpr(
            "cast(id_pessoa AS bigint) AS _id_verdadeiro",
            f"{self.chave('clientes', 'id_pessoa', 8)} AS cod_cliente",
            "nome", "cpf", "data_nascimento", "telefone", "cast(cod_municipio AS int) AS cod_municipio",
            "desde AS data_cadastro",
        ))

    @cached_property
    def linhas(self) -> "DataFrame":
        mundo = self.contexto.mundo
        municipios = mundo["municipios"].selectExpr("cod_municipio", "concat(nome_municipio, '/', sigla_uf) AS _nome")
        rotas = mundo["rotas"].where(f"empresa = '{EMPRESA}'")
        origem = municipios.withColumnRenamed("cod_municipio", "cod_municipio_origem").withColumnRenamed("_nome", "_origem")
        destino = municipios.withColumnRenamed("cod_municipio", "cod_municipio_destino").withColumnRenamed("_nome", "_destino")
        return materializar(self.contexto, rotas.join(origem, "cod_municipio_origem").join(destino, "cod_municipio_destino").selectExpr(
            "cast(id_rota AS bigint) AS _id_verdadeiro",
            f"{self.chave('linhas', 'id_rota', 5)} AS cod_linha",
            "concat(_origem, ' x ', _destino) AS descricao",
            "cod_municipio_origem", "cod_municipio_destino", "distancia_km", "tarifa_base AS tarifa",
        ))

    @property
    def alvo_clientes(self) -> Alvo:
        return Alvo("clientes", ("cod_cliente",), "pessoas", "_id_verdadeiro")

    def chave_da_copia(self, coluna_id: str) -> str:
        """Chave do sistema da cópia duplicada de um cliente (id verdadeiro deslocado para fora do mundo)."""
        return self.chave("clientes", f"({coluna_id} + {_FORA_DO_MUNDO_CLIENTES})", 8)

    def mapa_chaves(self) -> "DataFrame":
        """Mapa acumulado: clientes cadastrados até `fim`, as cópias duplicadas deles e todas as linhas."""
        clientes = self.clientes.where(f"data_cadastro <= DATE'{self.fim.isoformat()}'")
        copias = self.defeitos.copias_de_pessoas(
            clientes, self.alvo_clientes, {"cod_cliente": self.chave_da_copia("_id_verdadeiro")})
        return (
            mapa(clientes, SISTEMA, "clientes", "cod_cliente", "pessoas", "_id_verdadeiro")
            .unionByName(mapa(copias, SISTEMA, "clientes", "cod_cliente", "pessoas", "_id_verdadeiro"))
            .unionByName(mapa(self.linhas, SISTEMA, "linhas", "cod_linha", "rotas", "_id_verdadeiro"))
        )

    # -- defeitos ---------------------------------------------------------------------------

    def sujar_clientes(self, df: "DataFrame") -> "DataFrame":
        d = self.defeitos
        alvo = self.alvo_clientes
        df = d.pessoa_duplicada_no_sistema(df, alvo, {"cod_cliente": self.chave_da_copia("_id_verdadeiro")})
        df = d.cpf_fora_do_padrao(df, alvo, "cpf")
        df = d.nome_abreviado_ou_com_erro(df, alvo, "nome")
        return d.municipio_fora_do_padrao(df, alvo, "cod_municipio")

    def sujar_passagens(self, df: "DataFrame") -> "DataFrame":
        alvo = Alvo("passagens", ("num_bilhete",))
        inexistente = self.chave(
            "linhas", f"({_FORA_DO_MUNDO_LINHAS} + pmod(xxhash64(num_bilhete), {_FORA_DO_MUNDO_LINHAS - 1}))", 5)
        return self.defeitos.fk_orfa(df, alvo, "cod_linha", inexistente)

    # -- histórico de mudanças por entidade -------------------------------------------------

    def historico_clientes(self) -> "DataFrame":
        """`I` no dia do cadastro, de madrugada (antes de qualquer venda daquele dia)."""
        antes_das_vendas = self.p.horario_vendas.minimo * 3600
        return (
            self.clientes
            .where(f"data_cadastro <= DATE'{self.fim.isoformat()}'")
            .join(calendario(self.contexto, SISTEMA, self.fim).withColumnRenamed("data", "data_cadastro"), "data_cadastro")
            .selectExpr(
                "cod_cliente", "_id_verdadeiro", "'I' AS op",
                f"timestamp_seconds(unix_seconds(cast(data_cadastro AS timestamp)) "
                f"+ pmod(xxhash64(semente, _id_verdadeiro), {antes_das_vendas})) AS ts_operacao",
                "nome", "cpf", "data_nascimento", "telefone", "cod_municipio", "data_cadastro",
            )
        )

    def historico_linhas(self) -> "DataFrame":
        inicio = self.roteiro.datas.inicio.isoformat()
        return self.linhas.selectExpr(
            "cod_linha", "'I' AS op", f"cast(DATE'{inicio}' AS timestamp) AS ts_operacao",
            "descricao", "cod_municipio_origem", "cod_municipio_destino", "distancia_km", "tarifa",
        )

    def historico_passagens(self) -> "DataFrame":
        """`I` na venda (`emitida`); `U` no cancelamento, entre a venda e a 0h do dia da
        viagem (`cancelada`), ou no fim do dia da viagem (`utilizada`)."""
        vendas = self.vendas()
        imagem = ["cod_cliente", "cod_linha", "data_hora_venda", "data_viagem", "poltrona", "canal", "valor"]

        def operacao(df: "DataFrame", op: str, ts: str, situacao: str) -> "DataFrame":
            return df.selectExpr(
                "num_bilhete", f"'{op}' AS op", f"timestamp_seconds({ts}) AS ts_operacao", *imagem,
                f"'{situacao}' AS situacao",
            )

        venda = "unix_seconds(data_hora_venda)"
        cancelamento = (
            f"{venda} + 1 + cast(floor(_u_momento * "
            f"(unix_seconds(cast(data_viagem AS timestamp)) - {venda} - 1)) AS bigint)"
        )
        utilizacao = "unix_seconds(cast(data_viagem AS timestamp)) + 82800 + pmod(xxhash64(semente, n, 11), 1800)"
        return (
            operacao(vendas, "I", venda, "emitida")
            .unionByName(operacao(vendas.where("_cancelada"), "U", cancelamento, "cancelada"))
            .unionByName(operacao(vendas.where("NOT _cancelada"), "U", utilizacao, "utilizada"))
        )

    # -- vendas -----------------------------------------------------------------------------

    def vendas(self) -> "DataFrame":
        """Passagens vendidas do início da história até `fim` (a foto precisa de todas)."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        p = self.p
        inteiro, fracao = legados.por_dia(self.contexto, self.roteiro.volumes.passagens_solticket)

        # Clientes elegíveis num dia: os cadastrados até ele, numerados por ordem de cadastro.
        clientes = self.clientes.select(
            "cod_cliente", "data_cadastro",
            (F.row_number().over(Window.orderBy("data_cadastro", "cod_cliente")) - 1).alias("_indice_cliente"),
        )
        cadastros = clientes.groupBy(F.col("data_cadastro").alias("data")).agg(F.count(F.lit(1)).alias("_novos"))
        dias = linhas_por_dia(
            calendario(self.contexto, SISTEMA, self.fim)
            .join(cadastros, "data", "left")
            .withColumn("_clientes", F.sum(F.coalesce("_novos", F.lit(0))).over(
                Window.orderBy("data").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
            .where("_clientes > 0"),
            inteiro, fracao, "_clientes",
        )
        linhas_numeradas = self.linhas.select(
            "cod_linha", "tarifa", (F.row_number().over(Window.orderBy("cod_linha")) - 1).alias("_indice_linha"),
        )
        total_linhas = linhas_numeradas.agg(F.count(F.lit(1)).alias("_linhas"))

        horas, antecedencia = p.horario_vendas, p.antecedencia_compra_dias
        return (
            dias.crossJoin(total_linhas)
            .where("_linhas > 0")
            .selectExpr(
                "*",
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 100000 + n AS num_bilhete",
                "pmod(xxhash64(semente, n, 1), _clientes) AS _indice_cliente",
                "pmod(xxhash64(semente, n, 2), _linhas) AS _indice_linha",
                f"{instante('data', horas, 'semente', 'n', 3)} AS data_hora_venda",
                f"cast({antecedencia.minimo} + pmod(xxhash64(semente, n, 4), "
                f"{antecedencia.maximo - antecedencia.minimo + 1}) AS int) AS _antecedencia",
                f"{sorteio(p.canais, uniforme_do_lote(5))} AS canal",
                f"cast(1 + pmod(xxhash64(semente, n, 6), {p.poltronas}) AS int) AS poltrona",
                f"{uniforme_do_lote(7)} AS _u_cancelamento",
                f"{uniforme_do_lote(9)} AS _u_momento",
            )
            .withColumn("data_viagem", F.expr("date_add(data, _antecedencia)"))
            .withColumn("_cancelada", F.expr(f"_antecedencia > 0 AND _u_cancelamento < {p.cancelamento!r}"))
            .join(clientes.select("cod_cliente", "_indice_cliente"), "_indice_cliente")
            .join(linhas_numeradas, "_indice_linha")
            .withColumnRenamed("tarifa", "valor")
        )


__all__ = ["projetar"]
