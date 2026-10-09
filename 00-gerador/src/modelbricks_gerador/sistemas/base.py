"""Contrato comum das projeções de sistema de origem.

Uma projeção é `projetar(contexto, inicio, fim) -> Entrega`: recebe o mundo verdadeiro e o
roteiro (no `Contexto`) e um intervalo de datas simuladas (inclusive) e devolve tudo o que
o sistema entregaria nesse intervalo. O intervalo é arbitrário: a carga inicial pede
01/01/2024 a 31/12/2025 e o `avancar_tempo` pede os dias seguintes ao relógio. Gerar um
intervalo em partes ou de uma vez produz os mesmos lotes.

Também ficam aqui os utilitários de semente por lote, comuns a todos os sistemas.
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from ..escritor import Arquivo
from ..mundo import Mundo, gerar_mundo
from ..roteiro import Fatia, Roteiro
from ..sementes import semente

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class Contexto:
    """O que toda projeção recebe além do intervalo de datas."""

    spark: "SparkSession"
    roteiro: Roteiro
    fatia: Fatia
    escala: float
    carga_inicial: bool  # True na carga inicial do gerar_fatia; False no avancar_tempo

    @cached_property
    def mundo(self) -> Mundo:
        """Mundo verdadeiro da escala, montado na primeira consulta (DataFrames lazy por nome:
        `contexto.mundo["pessoas"]`, `contexto.mundo["participacoes"]`; ver `mundo.TABELAS`)."""
        return gerar_mundo(self.spark, self.roteiro, self.escala)


@dataclass(frozen=True)
class Lote:
    """Um arquivo entregue na landing, com os metadados de entrega."""

    sistema: str
    entidade: str
    data_simulada: date  # dia em que o sistema entregou o lote
    formato: str  # json, json_aninhado, parquet, csv, csv_latin1
    registros: int  # linhas (ou operações) no arquivo
    arquivo: Arquivo


@dataclass(frozen=True)
class Entrega:
    """Resultado de uma projeção num intervalo de datas.

    - `lotes`: arquivos da landing, em ordem determinística;
    - `defeitos`: DataFrames com o registro dos defeitos injetados (gravados em
      `gabarito.defeitos`); vazio enquanto o sistema não injeta defeitos;
    - `mapa_chaves`: DataFrame no esquema de `chaves.COLUNAS_MAPA` (chave verdadeira →
      chave do sistema), gravado em `gabarito.mapa_chaves`; `None` se o sistema não tem
      entidades do mundo verdadeiro.
    """

    lotes: list[Lote] = field(default_factory=list)
    defeitos: list["DataFrame"] = field(default_factory=list)
    mapa_chaves: "DataFrame | None" = None


def semente_do_lote(roteiro: Roteiro, sistema: str, data_simulada: date) -> int:
    """Seed do lote de um sistema num dia: hash estável de (seed base, sistema, data)."""
    return semente(roteiro.seed_base, sistema, data_simulada.isoformat())


def calendario(contexto: Contexto, sistema: str, fim: date) -> "DataFrame":
    """Um dia por linha, do início da história até `fim`: colunas `data` (date) e `semente`
    (long, a do lote do sistema naquele dia). Base para gerar fatos diários sem depender do
    intervalo pedido nem da ordem de execução."""
    from pyspark.sql.types import DateType, LongType, StructField, StructType

    inicio = contexto.roteiro.datas.inicio
    dias = [inicio + timedelta(days=i) for i in range((fim - inicio).days + 1)]
    esquema = StructType([StructField("data", DateType(), False), StructField("semente", LongType(), False)])
    return contexto.spark.createDataFrame(
        [(d, semente_do_lote(contexto.roteiro, sistema, d)) for d in dias], esquema
    )


def materializar(contexto: Contexto, df: "DataFrame") -> "DataFrame":
    """Calcula `df` uma vez e o devolve como relação local (via Arrow).

    Para cadastros pequenos derivados do mundo verdadeiro que a projeção usa várias vezes:
    sem isso, cada uso recalcularia o plano inteiro do mundo no servidor.
    """
    tabela = df.toArrow()
    if tabela.num_rows == 0:  # o Spark não infere esquema de relação vazia
        return contexto.spark.createDataFrame([], df.schema)
    return contexto.spark.createDataFrame(tabela)


__all__ = ["Contexto", "Entrega", "Lote", "calendario", "materializar", "semente_do_lote"]
