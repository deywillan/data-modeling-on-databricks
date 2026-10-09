"""Escritor único de bytes da landing.

Toda serialização passa por aqui, para que o destino local e o Unity Catalog recebam
exatamente os mesmos bytes. Regras:

- o lote é ordenado de forma total antes de coletar (nunca depender de partições);
- o que tem semântica de texto (JSON, datas, valores monetários) é formatado no Spark;
- nada de Python UDF (falha via Databricks Connect serverless).

Interface: `Arquivo`, `caminho_landing(...)` e os serializadores `json_linhas`, `csv` e
`parquet`, que recebem um DataFrame e devolvem `bytes`.
"""

import io
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class Arquivo:
    """Um arquivo pronto para a landing: caminho relativo ao Volume e conteúdo em bytes."""

    caminho: str
    conteudo: bytes


def caminho_landing(sistema: str, entidade: str, data_simulada: date, extensao: str, sequencial: int = 1) -> str:
    """Caminho padrão `<sistema>/<entidade>/<AAAA-MM-DD>/<entidade>_<AAAAMMDD>_<NNNN>.<ext>`."""
    return (
        f"{sistema}/{entidade}/{data_simulada.isoformat()}/"
        f"{entidade}_{data_simulada:%Y%m%d}_{sequencial:04d}.{extensao}"
    )


def json_linhas(df: "DataFrame", ordem: Sequence[str]) -> bytes:
    """JSON lines (uma linha por registro, UTF-8), serializado no Spark com `to_json`.

    Colunas `struct`/`array` viram JSON aninhado.
    """
    from pyspark.sql import functions as F

    linhas = (
        df.select(*ordem, F.to_json(F.struct(*df.columns)).alias("_linha"))
        .orderBy(*ordem)
        .select("_linha")
        .collect()
    )
    return "".join(f"{linha[0]}\n" for linha in linhas).encode("utf-8")


def csv(df: "DataFrame", ordem: Sequence[str], separador: str = ",", encoding: str = "utf-8") -> bytes:
    """CSV com cabeçalho. Formate números e datas no Spark antes (o pandas traz `Decimal`).

    Com `encoding="latin-1"`, caracteres fora do latin1 fazem a serialização falhar.
    """
    tabela = df.orderBy(*ordem).toPandas()
    texto = tabela.to_csv(sep=separador, index=False, lineterminator="\n")
    return texto.encode(encoding, errors="strict")


def parquet(df: "DataFrame", ordem: Sequence[str]) -> bytes:
    """Parquet (snappy, sem metadados do pandas), com os tipos preservados via Arrow."""
    import pyarrow.parquet as pq

    tabela = df.orderBy(*ordem).toArrow().replace_schema_metadata(None)
    buffer = io.BytesIO()
    pq.write_table(tabela, buffer, compression="snappy")
    return buffer.getvalue()
