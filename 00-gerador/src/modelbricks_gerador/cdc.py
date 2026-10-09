"""Motor de mutação temporal CDC: transforma o histórico de mudanças de um sistema em lotes
diários de operações I/U/D, como um log de CDC entregue em arquivos.

O sistema descreve, para cada entidade, **todas** as mudanças que ela sofre (um DataFrame
com a chave, `op` em I/U/D, `ts_operacao` e a imagem posterior da linha). O motor:

1. recorta as operações cujo dia (de `ts_operacao`) cai no intervalo pedido;
2. numera cada operação com `seq`, única e crescente no tempo **por sistema** (entre todas
   as entidades): `segundo_unix * 10^7 + índice_da_entidade * 10^6 + ordem_no_segundo`.
   A numeração depende só do instante e da chave, nunca do intervalo pedido, então gerar
   um dia isolado ou junto com os demais dá a mesma `seq`;
3. monta cada linha JSON `{"op", "seq", "ts_operacao", <chave>, <imagem>}`; no `D`, só a
   chave (as demais colunas ficam nulas e o `to_json` as omite);
4. coleta tudo numa ida ao Spark e separa em um arquivo por entidade e dia simulado, no
   caminho `<sistema>/<entidade>/<AAAA-MM-DD>/<entidade>_<AAAAMMDD>_0001.json`.

Contrato das mudanças (responsabilidade do sistema): a primeira operação de cada chave é
`I`; nada vem depois de um `D`; e uma chave não tem duas operações no mesmo segundo. Até
10 entidades por sistema e menos de 10^6 operações por entidade no mesmo segundo.

Interface: `Mudancas` e `lotes_cdc(sistema, mudancas, inicio, fim)`. Nada aqui conhece um
sistema específico; *Embarque* e *RotaCerta* usam o mesmo motor.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from functools import reduce
from typing import TYPE_CHECKING

from .escritor import Arquivo, caminho_landing
from .sistemas.base import Lote

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

COLUNAS_CONTROLE = ("op", "ts_operacao")
_SEQ_POR_SEGUNDO = 10**7
_SEQ_POR_ENTIDADE = 10**6
_MAX_ENTIDADES = _SEQ_POR_SEGUNDO // _SEQ_POR_ENTIDADE


@dataclass(frozen=True)
class Mudancas:
    """Histórico completo de mudanças de uma entidade do sistema.

    `df` tem as colunas da `chave`, `op` ('I', 'U' ou 'D'), `ts_operacao` (timestamp, em
    segundos inteiros) e as colunas da imagem posterior da linha, na ordem do arquivo.
    """

    entidade: str
    chave: tuple[str, ...]
    df: "DataFrame"


def lotes_cdc(sistema: str, mudancas: Sequence[Mudancas], inicio: date, fim: date) -> list[Lote]:
    """Lotes CDC (um JSON lines por entidade e dia) com as operações de `inicio` a `fim`."""
    if not mudancas:
        return []
    if len(mudancas) > _MAX_ENTIDADES:
        raise ValueError(f"{sistema}: o motor CDC aceita até {_MAX_ENTIDADES} entidades por sistema.")
    partes = [_linhas(m, indice, inicio, fim) for indice, m in enumerate(mudancas)]
    tabela = reduce(lambda a, b: a.unionByName(b), partes).orderBy("data", "entidade", "seq").toArrow()
    return _separar(sistema, tabela)


def _linhas(mudancas: Mudancas, indice: int, inicio: date, fim: date) -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    chave = list(mudancas.chave)
    imagem = [c for c in mudancas.df.columns if c not in (*chave, *COLUNAS_CONTROLE)]
    dia = F.to_date("ts_operacao")
    recorte = mudancas.df.where(dia.between(F.lit(inicio), F.lit(fim)))
    segundo = F.unix_seconds("ts_operacao")
    ordem_op = F.expr("CASE op WHEN 'I' THEN 0 WHEN 'U' THEN 1 ELSE 2 END")
    ordem = F.row_number().over(Window.partitionBy(segundo).orderBy(*chave, ordem_op))
    seq = segundo * _SEQ_POR_SEGUNDO + indice * _SEQ_POR_ENTIDADE + ordem
    numerado = recorte.withColumn("seq", seq.cast("bigint"))
    campos = [
        F.col("op"), F.col("seq"), F.col("ts_operacao"),
        *[F.col(c) for c in chave],
        *[F.when(F.col("op") != "D", F.col(c)).alias(c) for c in imagem],
    ]
    return numerado.select(
        dia.cast("string").alias("data"),
        F.lit(mudancas.entidade).alias("entidade"),
        F.col("seq"),
        F.to_json(F.struct(*campos)).alias("linha"),
    )


def _separar(sistema: str, tabela) -> list[Lote]:
    """Divide a tabela coletada (ordenada por data, entidade e seq) em um lote por grupo."""
    datas = tabela.column("data").to_pylist()
    entidades = tabela.column("entidade").to_pylist()
    linhas = tabela.column("linha")
    lotes = []
    inicio = 0
    for i in range(1, len(datas) + 1):
        if i < len(datas) and (datas[i], entidades[i]) == (datas[inicio], entidades[inicio]):
            continue
        data_lote = date.fromisoformat(datas[inicio])
        entidade = entidades[inicio]
        conteudo = "".join(f"{linha}\n" for linha in linhas.slice(inicio, i - inicio).to_pylist())
        lotes.append(
            Lote(
                sistema, entidade, data_lote, "json", i - inicio,
                Arquivo(caminho_landing(sistema, entidade, data_lote, "json"), conteudo.encode("utf-8")),
            )
        )
        inicio = i
    return lotes


__all__ = ["Mudancas", "lotes_cdc"]
