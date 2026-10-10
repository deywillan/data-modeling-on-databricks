"""*SolFin*: ERP legado do Expresso Vale do Sol (financeiro próprio, ainda não migrado para
o *Núcleo*), entregue como carga histórica única no dia da aquisição e depois como
snapshot semanal, em CSV `;` com `latin1` (ver `legados.py`).

Entidades (todas com chave própria do sistema, ver `chaves.py`):

| Entidade | Chave | Origem | Mudanças |
|---|---|---|---|
| `fornecedores` | `cod_fornecedor` | parte das empresas PJ do mundo (podem ser também fornecedoras ou clientes do grupo) | cadastro no início da história |
| `titulos_pagar` | `num_titulo` | fato do sistema (contas a pagar) | emissão (`aberto`); pagamento (`pago`) |

Os títulos de cada dia nascem do seed do lote daquele dia (seed base, sistema, data).
Números em `parametros.solfin` (`titulos_pagar` é a carga histórica, na escala 1).

Defeitos (`defeitos.py`): só o `latin1`, em todos os arquivos. As categorias de despesa e as
razões sociais têm acento, então os arquivos não são UTF-8 válido.
"""

from datetime import date
from functools import cached_property
from typing import TYPE_CHECKING

from ..cdc import Mudancas
from ..chaves import chave_do_sistema, mapa
from ..sementes import semente
from . import legados
from .base import Contexto, Entrega, calendario, materializar

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "solfin"


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)

    def historicos() -> list[Mudancas]:
        return [
            Mudancas("fornecedores", ("cod_fornecedor",), projecao.historico_fornecedores()),
            Mudancas("titulos_pagar", ("num_titulo",), projecao.historico_titulos()),
        ]

    return legados.entregar(contexto, SISTEMA, inicio, fim, historicos, projecao.mapa_chaves)


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.solfin
        self.fim = fim
        self.inicio = contexto.roteiro.datas.inicio

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    def instante(self, data: str, *colunas: str) -> str:
        """Timestamp no expediente do dia `data`, sorteado pelas colunas."""
        h = self.p.horario_expediente
        return (
            f"timestamp_seconds(unix_seconds(cast({data} AS timestamp)) + {h.minimo * 3600} "
            f"+ pmod(xxhash64({', '.join(colunas)}), {(h.maximo - h.minimo) * 3600}))"
        )

    @cached_property
    def fornecedores(self) -> "DataFrame":
        """Empresas PJ do mundo que fornecem ao Vale do Sol."""
        s = semente(self.roteiro.seed_base, SISTEMA, "fornecedores")
        return materializar(self.contexto, self.contexto.mundo["empresas_pj"].where(
            f"pmod(xxhash64({s}, id_empresa), 1000000) / 1000000.0 < {self.p.fornecedores!r}"
        ).selectExpr(
            "cast(id_empresa AS bigint) AS _id_verdadeiro",
            f"{self.chave('fornecedores', 'id_empresa', 5)} AS cod_fornecedor",
            "razao_social", "cnpj", "telefone", "cast(cod_municipio AS int) AS cod_municipio",
        ))

    def mapa_chaves(self) -> "DataFrame":
        return mapa(self.fornecedores, SISTEMA, "fornecedores", "cod_fornecedor", "empresas_pj", "_id_verdadeiro")

    def historico_fornecedores(self) -> "DataFrame":
        inicio = self.inicio.isoformat()
        return self.fornecedores.selectExpr(
            "cod_fornecedor", "'I' AS op", f"cast(DATE'{inicio}' AS timestamp) AS ts_operacao",
            "razao_social", "cnpj", "telefone", "cod_municipio", f"DATE'{inicio}' AS data_cadastro",
        )

    def historico_titulos(self) -> "DataFrame":
        """`I` na emissão (`aberto`); `U` no pagamento, dias depois do vencimento (`pago`)."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        p = self.p
        inteiro, fracao = legados.por_dia(self.contexto, p.titulos_pagar)
        fornecedores = self.fornecedores.select(
            "cod_fornecedor", (F.row_number().over(Window.orderBy("cod_fornecedor")) - 1).alias("_indice_fornecedor"),
        )
        total = fornecedores.agg(F.count(F.lit(1)).alias("_fornecedores"))

        def u(k: int) -> str:
            return f"(pmod(xxhash64(semente, n, {k}), 1000000) / 1000000.0)"

        prazo, pagamento, valor = p.prazo_vencimento_dias, p.dias_pagamento_apos_vencimento, p.valor_titulo
        titulos = (
            calendario(self.contexto, SISTEMA, self.fim)
            .withColumn("_n_dia", F.expr(
                f"{inteiro} + CASE WHEN pmod(xxhash64(semente, 0), 1000000) / 1000000.0 < {fracao!r} THEN 1 ELSE 0 END"))
            .where("_n_dia > 0")
            .select("data", "semente", F.explode(F.sequence(F.lit(1), F.col("_n_dia"))).alias("n"))
            .crossJoin(total)
            .where("_fornecedores > 0")
            .selectExpr(
                "*",
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 10000 + n AS num_titulo",
                "pmod(xxhash64(semente, n, 1), _fornecedores) AS _indice_fornecedor",
                f"{legados.sorteio(p.categorias_despesa, u(2))} AS categoria",
                f"cast(round({valor.minimo} + {valor.maximo - valor.minimo} * {u(3)}, 2) AS decimal(12,2)) AS valor",
                "data AS data_emissao",
                f"date_add(data, cast({prazo.minimo} + pmod(xxhash64(semente, n, 4), "
                f"{prazo.maximo - prazo.minimo + 1}) AS int)) AS data_vencimento",
                f"cast({pagamento.minimo} + pmod(xxhash64(semente, n, 5), "
                f"{pagamento.maximo - pagamento.minimo + 1}) AS int) AS _dias_pagamento",
            )
            .join(fornecedores, "_indice_fornecedor")
            .withColumn("_pago_em", F.expr("date_add(data_vencimento, _dias_pagamento)"))
        )
        imagem = ["cod_fornecedor", "categoria", "valor", "data_emissao", "data_vencimento"]
        aberto = titulos.selectExpr(
            "num_titulo", "'I' AS op", f"{self.instante('data', 'semente', 'n', '6')} AS ts_operacao", *imagem,
            "'aberto' AS situacao", "cast(NULL AS date) AS data_pagamento",
        )
        pago = titulos.selectExpr(
            "num_titulo", "'U' AS op", f"{self.instante('_pago_em', 'semente', 'n', '7')} AS ts_operacao", *imagem,
            "'pago' AS situacao", "_pago_em AS data_pagamento",
        )
        return aberto.unionByName(pago)


__all__ = ["projetar"]
