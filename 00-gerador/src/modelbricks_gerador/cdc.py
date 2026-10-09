"""Motor de mutação temporal CDC: transforma o histórico de mudanças de um sistema em lotes
diários de operações I/U/D, como um log de CDC entregue em arquivos.

O sistema descreve, para cada entidade, **todas** as mudanças que ela sofre (um DataFrame
com a chave, `op` em I/U/D, `ts_operacao` e a imagem posterior da linha). O motor:

1. numera cada operação com `seq`, única e crescente no instante da entrega **por
   sistema** (entre todas as entidades): `segundo_unix * 10^7 + índice_da_entidade * 10^6 +
   ordem_no_segundo`. A numeração depende só do instante e da chave, nunca do intervalo
   pedido, então gerar um dia isolado ou junto com os demais dá a mesma `seq`;
2. recorta as linhas cujo dia de entrega cai no intervalo pedido;
3. monta cada linha JSON `{"op", "seq", "ts_operacao", <chave>, <imagem>}`; no `D`, só a
   chave (as demais colunas ficam nulas e o `to_json` as omite);
4. coleta tudo numa ida ao Spark e separa em um arquivo por entidade e dia simulado, no
   caminho `<sistema>/<entidade>/<AAAA-MM-DD>/<entidade>_<AAAAMMDD>_0001.json`, junto com
   o registro dos defeitos das linhas entregues (ver `defeitos.py`).

Sem defeitos de entrega, o instante da entrega é o da operação. Com eles (colunas
internas criadas por `defeitos.py`): `_entrega` adia a linha (atraso: a `seq` é a do
instante da entrega, maior que a de operações posteriores da mesma chave) e `_reentrega`
marca a cópia de uma operação já entregue, que recebe a mesma `seq` da original. Colunas
que começam com `_` nunca vão para o arquivo.

Contrato das mudanças (responsabilidade do sistema): a primeira operação de cada chave é
`I`; nada vem depois de um `D`; e uma chave não tem duas operações no mesmo segundo. Até
10 entidades por sistema e menos de 10^6 operações por entidade no mesmo segundo. Para
que a `seq` independa do intervalo, o histórico de cada entidade precisa estar completo a
partir do dia `inicio - folga` (`Defeitos.folga_dias()`).

Interface: `Mudancas` e `entrega_cdc(sistema, mudancas, inicio, fim)`. Nada aqui conhece
um sistema específico; *Embarque* e *RotaCerta* usam o mesmo motor.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from functools import reduce
from typing import TYPE_CHECKING

from .defeitos import registros, tabela_de_registros
from .escritor import Arquivo, caminho_landing
from .sistemas.base import Entrega, Lote

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


def entrega_cdc(sistema: str, mudancas: Sequence[Mudancas], inicio: date, fim: date) -> Entrega:
    """Lotes CDC (um JSON lines por entidade e dia) com as linhas entregues de `inicio` a
    `fim` e o registro dos defeitos dessas linhas."""
    if not mudancas:
        return Entrega()
    if len(mudancas) > _MAX_ENTIDADES:
        raise ValueError(f"{sistema}: o motor CDC aceita até {_MAX_ENTIDADES} entidades por sistema.")
    partes = [_linhas(m, indice, inicio, fim) for indice, m in enumerate(mudancas)]
    tabela = reduce(lambda a, b: a.unionByName(b), partes).orderBy("data", "entidade", "seq", "_reentrega").toArrow()
    linhas = registros(tabela, sistema)
    defeitos = tabela_de_registros(mudancas[0].df.sparkSession, linhas)
    return Entrega(lotes=_separar(sistema, tabela), defeitos=[defeitos] if defeitos is not None else [])


def _linhas(mudancas: Mudancas, indice: int, inicio: date, fim: date) -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .defeitos import ANOTACAO

    chave = list(mudancas.chave)
    df = mudancas.df
    if "_entrega" not in df.columns:
        df = df.withColumn("_entrega", F.col("ts_operacao"))
    if "_reentrega" not in df.columns:
        df = df.withColumn("_reentrega", F.lit(False))
    if "_defeitos" not in df.columns:
        df = df.withColumn("_defeitos", F.array().cast(f"array<{ANOTACAO}>"))
    imagem = [c for c in df.columns if c not in (*chave, *COLUNAS_CONTROLE) and not c.startswith("_")]
    df = df.where(F.to_date("_entrega") <= F.lit(fim))

    # seq: numerada nas originais pelo instante da entrega; a reentrega herda a da original.
    originais = df.where(~F.col("_reentrega"))
    segundo = F.unix_seconds("_entrega")
    ordem_op = F.expr("CASE op WHEN 'I' THEN 0 WHEN 'U' THEN 1 ELSE 2 END")
    ordem = F.row_number().over(Window.partitionBy(segundo).orderBy(*chave, "ts_operacao", ordem_op))
    seq = segundo * _SEQ_POR_SEGUNDO + indice * _SEQ_POR_ENTIDADE + ordem
    originais = originais.withColumn("seq", seq.cast("bigint"))
    identidade = [*chave, "ts_operacao", "op"]
    reentregas = df.where(F.col("_reentrega")).join(originais.select(*identidade, "seq"), identidade)
    numerado = originais.unionByName(reentregas)

    dia = F.to_date("_entrega")
    recorte = numerado.where(dia.between(F.lit(inicio), F.lit(fim)))
    campos = [
        F.col("op"), F.col("seq"), F.col("ts_operacao"),
        *[F.col(c) for c in chave],
        *[F.when(F.col("op") != "D", F.col(c)).alias(c) for c in imagem],
    ]
    return recorte.select(
        dia.cast("string").alias("data"),
        F.lit(mudancas.entidade).alias("entidade"),
        F.col("seq"),
        F.col("_reentrega"),
        F.to_json(F.struct(*campos)).alias("linha"),
        F.col("_defeitos"),
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


__all__ = ["Mudancas", "entrega_cdc"]
