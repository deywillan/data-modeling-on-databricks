"""Motor de mutação temporal incremental: transforma o histórico de versões de um sistema em
lotes diários "linhas alteradas no dia", como a extração por `updated_at` de um ERP.

O sistema descreve, para cada entidade, **todas** as versões de cada registro (um DataFrame
com a chave, `updated_at` e a imagem completa da linha depois da mudança). Exclusão lógica
(*soft delete*) é só mais uma versão, com a coluna de exclusão do sistema verdadeira: o
motor não conhece a coluna, e nada some do lote. O motor:

1. recorta as versões cujo dia de entrega cai no intervalo pedido (sem defeitos de entrega,
   o dia de `updated_at`);
2. coleta cada entidade numa ida ao Spark (`toArrow`, que preserva decimal, data e
   timestamp), em ordem de dia, chave e `updated_at`;
3. separa um arquivo por entidade e dia simulado, no caminho
   `<sistema>/<entidade>/<AAAA-MM-DD>/<entidade>_<AAAAMMDD>_0001.<extensão>`, serializado
   pelo `Formato` (Parquet no *Núcleo*; CSV no *Apólice+*), e registra os defeitos das
   linhas entregues com o arquivo em que chegaram (ver `defeitos.py`).

Defeitos de entrega (colunas internas criadas por `defeitos.py`, com `instante="updated_at"`):
`_entrega` adia a linha para um lote posterior (o `updated_at` continua o da mudança) e a
reentrega (`_reentrega`) é só mais uma linha num lote posterior. Colunas que começam com `_`
nunca vão para o arquivo.

O formato só serializa: quem precisa de texto com semântica própria (ex.: valor monetário
como "1.234,56" no CSV) formata a coluna no Spark, na projeção, antes de chamar o motor.

Interface: `Versoes`, `Formato`, `PARQUET` e `entrega_incremental(sistema, versoes, inicio,
fim, formato)`. Nada aqui conhece um sistema específico.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from .defeitos import ANOTACAO, registros, tabela_de_registros
from .escritor import Arquivo, caminho_landing, parquet_de_arrow
from .sistemas.base import Entrega, Lote

if TYPE_CHECKING:
    import pyarrow as pa
    from pyspark.sql import DataFrame

COLUNA_ATUALIZACAO = "updated_at"


@dataclass(frozen=True)
class Formato:
    """Como um lote vira bytes: nome do formato (o do `Lote`), extensão e serializador de uma
    `pyarrow.Table` já ordenada, só com as colunas do arquivo."""

    nome: str
    extensao: str
    serializar: Callable[["pa.Table"], bytes]


PARQUET = Formato("parquet", "parquet", parquet_de_arrow)


@dataclass(frozen=True)
class Versoes:
    """Histórico completo de versões de uma entidade do sistema.

    `df` tem as colunas da `chave`, `updated_at` (timestamp) e as colunas da imagem da linha,
    na ordem do arquivo (as que começam com `_` são internas). Contrato: uma chave não tem
    duas versões com o mesmo `updated_at`, e nada vem depois da versão de exclusão.
    """

    entidade: str
    chave: tuple[str, ...]
    df: "DataFrame"


def entrega_incremental(
    sistema: str, versoes: Sequence[Versoes], inicio: date, fim: date, formato: Formato = PARQUET
) -> Entrega:
    """Lotes incrementais (um arquivo por entidade e dia) com as linhas entregues de `inicio`
    a `fim` e o registro dos defeitos dessas linhas."""
    lotes, defeitos = [], []
    for v in versoes:
        tabela = _linhas(v, inicio, fim).toArrow()
        novos, registros_ = _separar(sistema, v.entidade, tabela, formato)
        lotes += novos
        defeitos += registros_
    registro = tabela_de_registros(versoes[0].df.sparkSession, defeitos) if versoes else None
    return Entrega(lotes=lotes, defeitos=[registro] if registro is not None else [])


def _linhas(versoes: Versoes, inicio: date, fim: date) -> "DataFrame":
    from pyspark.sql import functions as F

    df = versoes.df
    if COLUNA_ATUALIZACAO not in df.columns:
        raise ValueError(f"{versoes.entidade}: as versões precisam da coluna {COLUNA_ATUALIZACAO}.")
    if "_entrega" not in df.columns:
        df = df.withColumn("_entrega", F.col(COLUNA_ATUALIZACAO))
    if "_defeitos" not in df.columns:
        df = df.withColumn("_defeitos", F.array().cast(f"array<{ANOTACAO}>"))
    if "_reentrega" not in df.columns:
        df = df.withColumn("_reentrega", F.lit(False))
    visiveis = [c for c in df.columns if not c.startswith("_")]
    dia = F.to_date("_entrega")
    return (
        df.where(dia.between(F.lit(inicio), F.lit(fim)))
        .select(
            dia.cast("string").alias("_data"),
            F.lit(versoes.entidade).alias("_entidade"),
            *visiveis,
            "_defeitos",
            "_reentrega",
        )
        .orderBy("_data", *versoes.chave, COLUNA_ATUALIZACAO, "_reentrega")
    )


def _separar(sistema: str, entidade: str, tabela: "pa.Table", formato: Formato) -> tuple[list[Lote], list]:
    """Divide a tabela coletada (ordenada por dia) em um lote por dia e registra os defeitos."""
    datas = tabela.column("_data").to_pylist()
    visiveis = [c for c in tabela.column_names if not c.startswith("_")]
    lotes, defeitos = [], []
    inicio = 0
    for i in range(1, len(datas) + 1):
        if i < len(datas) and datas[i] == datas[inicio]:
            continue
        data_lote = date.fromisoformat(datas[inicio])
        fatia = tabela.slice(inicio, i - inicio)
        caminho = caminho_landing(sistema, entidade, data_lote, formato.extensao)
        conteudo = formato.serializar(fatia.select(visiveis))
        lotes.append(Lote(sistema, entidade, data_lote, formato.nome, fatia.num_rows, Arquivo(caminho, conteudo)))
        defeitos += [
            (*r[:-1], caminho)
            for r in registros(fatia, sistema, entidade_coluna="_entidade", data_coluna="_data", seq_coluna=None)
        ]
        inicio = i
    return lotes, defeitos


__all__ = ["COLUNA_ATUALIZACAO", "Formato", "PARQUET", "Versoes", "entrega_incremental"]
