"""Chaves próprias dos sistemas de origem e o mapa chave verdadeira → chave do sistema.

Cada sistema identifica as entidades do mundo verdadeiro com a própria chave. A chave do
sistema é uma expressão SQL bijetiva sobre o id verdadeiro: ids distintos geram chaves
distintas, sem tabela de tradução nem estado, e sistemas diferentes geram chaves
diferentes para a mesma entidade. O mapa vai para `gabarito.mapa_chaves`, para o leitor
medir a integração; nunca para a landing.

Interface: `chave_do_sistema(...)` (expressão SQL) e `mapa(...)` (DataFrame no esquema
`COLUNAS_MAPA`). Nada aqui conhece um sistema específico.
"""

from typing import TYPE_CHECKING

from .sementes import semente

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Multiplicador primo com 10: `id * A + B` módulo 10^d é uma bijeção.
_MULTIPLICADOR = 387420489  # 3^18

COLUNAS_MAPA = ("sistema", "entidade", "coluna", "tabela_verdadeira", "id_verdadeiro", "chave_sistema")


def chave_do_sistema(seed_base: int, sistema: str, entidade: str, coluna_id: str, digitos: int) -> str:
    """Expressão SQL (bigint com `digitos + 1` algarismos, começando por 1) da chave do
    sistema para o id verdadeiro em `coluna_id`. Única para ids de 1 a 10^digitos."""
    deslocamento = semente(seed_base, "chave", sistema, entidade)
    modulo = 10**digitos
    return f"cast(pmod({coluna_id} * {_MULTIPLICADOR} + {deslocamento}, {modulo}) + {modulo} AS bigint)"


def mapa(
    df: "DataFrame", sistema: str, entidade: str, coluna: str, tabela_verdadeira: str, coluna_id: str
) -> "DataFrame":
    """Linhas do mapa a partir de um DataFrame com o id verdadeiro (`coluna_id`) e a chave
    do sistema (`coluna`), como aparece na entidade `entidade` do sistema."""
    from pyspark.sql import functions as F

    return df.select(
        F.lit(sistema).alias("sistema"),
        F.lit(entidade).alias("entidade"),
        F.lit(coluna).alias("coluna"),
        F.lit(tabela_verdadeira).alias("tabela_verdadeira"),
        F.col(coluna_id).cast("bigint").alias("id_verdadeiro"),
        F.col(coluna).cast("string").alias("chave_sistema"),
    ).distinct()


__all__ = ["COLUNAS_MAPA", "chave_do_sistema", "mapa"]
