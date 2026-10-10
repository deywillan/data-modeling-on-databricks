"""Escritor único de bytes da landing.

Toda serialização passa por aqui, para que o destino local e o Unity Catalog recebam
exatamente os mesmos bytes. Regras:

- o lote é ordenado de forma total antes de coletar (nunca depender de partições);
- o que tem semântica de texto (JSON, datas, valores monetários) é formatado no Spark;
- nada de Python UDF (falha via Databricks Connect serverless).

Interface: `Arquivo`, `caminho_landing(...)` e os serializadores `parquet_de_arrow` e
`csv_de_arrow`, que recebem uma `pyarrow.Table` já coletada e ordenada e devolvem `bytes`. O
JSON (CDC e APIs) é montado no Spark, com `to_json`, pelos motores que o entregam.
"""

import io
from dataclasses import dataclass
from datetime import date


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


def parquet_de_arrow(tabela) -> bytes:
    """Parquet (snappy, sem metadados de esquema) de uma `pyarrow.Table` já coletada e ordenada."""
    import pyarrow.parquet as pq

    buffer = io.BytesIO()
    pq.write_table(tabela.replace_schema_metadata(None), buffer, compression="snappy")
    return buffer.getvalue()


def csv_de_arrow(tabela) -> bytes:
    """CSV `,` UTF-8 com cabeçalho de uma `pyarrow.Table` já coletada e ordenada.

    Texto sempre entre aspas (inclusive o que já vem formatado no Spark, como "1.234,56"),
    nulo como campo vazio, data ISO e timestamp como `AAAA-MM-DD HH:MM:SS` no fuso da coluna
    (o da sessão, fixado pelo roteiro). Frações de segundo são descartadas.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pcsv

    for i, campo in enumerate(tabela.schema):
        if pa.types.is_timestamp(campo.type):
            segundos = tabela.column(i).cast(pa.timestamp("s", tz=campo.type.tz), safe=False)
            tabela = tabela.set_column(i, campo.name, pc.strftime(segundos, format="%Y-%m-%d %H:%M:%S"))
    buffer = io.BytesIO()
    pcsv.write_csv(tabela, buffer, pcsv.WriteOptions(quoting_style="needed"))
    return buffer.getvalue()
