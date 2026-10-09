"""Projeções do mundo verdadeiro em cada sistema de origem.

Cada sistema é um módulo com uma função `projetar(contexto, inicio, fim) -> list[Arquivo]`
que devolve os arquivos que o sistema entregaria na landing entre `inicio` e `fim`
(inclusive). Para adicionar um sistema, crie o módulo e registre-o em `PROJECOES`, com a
mesma chave usada em `sistemas:` no roteiro. Sistemas da fatia ainda sem projeção são
ignorados (e avisados) pela API.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from ..escritor import Arquivo
from ..roteiro import Fatia, Roteiro
from . import ibge

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class Contexto:
    """O que toda projeção recebe além do intervalo de datas."""

    spark: "SparkSession"
    roteiro: Roteiro
    fatia: Fatia
    escala: float
    carga_inicial: bool  # True na carga inicial do gerar_fatia; False no avancar_tempo


Projecao = Callable[[Contexto, date, date], list[Arquivo]]

PROJECOES: dict[str, Projecao] = {
    "ibge": ibge.projetar,
}

__all__ = ["Contexto", "Projecao", "PROJECOES"]
