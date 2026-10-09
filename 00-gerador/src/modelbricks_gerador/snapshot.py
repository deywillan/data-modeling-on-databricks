"""Motor de snapshot completo: transforma o histórico de mudanças de um sistema em fotos
inteiras das suas tabelas, uma pasta por data de foto, em CSV.

O sistema descreve cada entidade como no motor CDC (`cdc.Mudancas`): a chave, `op` em
I/U/D, `ts_operacao` e a imagem posterior da linha. O motor:

1. calcula as datas das fotos pela agenda do roteiro (`datas_de_snapshot`): diária, semanal
   ou mensal (último dia do mês), por período. A agenda é fixa, então gerar um intervalo
   em partes ou de uma vez produz as mesmas fotos;
2. para cada data de foto, toma a última versão de cada chave com operação até o fim do
   dia; a chave cujo último evento é `D` fica fora da foto (é assim que a exclusão aparece);
3. escreve um CSV por entidade e data, com cabeçalho, no caminho
   `<sistema>/<entidade>/<AAAA-MM-DD>/<entidade>_<AAAAMMDD>_0001.csv`, linhas em ordem da
   chave. Toda entidade tem arquivo em toda data, mesmo vazia (só o cabeçalho): a foto de
   uma tabela vazia continua sendo uma foto. As linhas são serializadas no Spark
   (`to_csv`), com datas `AAAA-MM-DD`, instantes `AAAA-MM-DD HH:MM:SS` e nulos vazios.

Defeitos de linha (`defeitos.py`) viajam com a versão na coluna `_defeitos` e são
registrados uma vez por versão, na data da primeira foto que a contém. Versão que muda
antes de qualquer foto nunca é entregue e não tem registro. Defeitos de entrega (atraso,
reentrega) não se aplicam a fotos.

Formatos: `csv` (vírgula, UTF-8) e `csv_latin1` (ponto e vírgula, latin1). Com latin1, um
caractere fora dele faz a serialização falhar.

As linhas vêm do Spark em fluxo, ordenadas por data, e cada foto vira bytes assim que
termina, para que cargas grandes (a carga histórica do *SolTicket*) não precisem caber
inteiras na memória do driver como linhas.

Interface: `datas_de_snapshot(agenda, inicio_historia, fim)`,
`entrega_snapshot(sistema, historicos, datas, inicio, fim, formato)` e
`registrar_por_arquivo(entrega, defeitos, tipo, spark)` (defeito de arquivo inteiro, como o
`latin1`, registrado em cada arquivo). Nada aqui conhece um sistema específico; o *Pátio*
e os legados usam o mesmo motor.
"""

from collections.abc import Sequence
from dataclasses import replace
from datetime import date, timedelta
from functools import reduce
from typing import TYPE_CHECKING

from .cdc import Mudancas
from .defeitos import ANOTACAO, Defeitos, registros, tabela_de_registros
from .escritor import Arquivo, caminho_landing
from .roteiro import AgendaSnapshots
from .sistemas.base import Entrega, Lote

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

FORMATOS = {"csv": (",", "utf-8"), "csv_latin1": (";", "latin-1")}


def datas_de_snapshot(agenda: AgendaSnapshots, inicio_historia: date, fim: date) -> list[date]:
    """Datas das fotos, do início da história (ou de `agenda.desde`) até `fim`."""
    datas = []
    d = max(inicio_historia, agenda.desde or inicio_historia)
    while d <= fim:
        periodo = next(p for p in agenda.periodos if p.ate is None or d <= p.ate)
        if (
            periodo.frequencia == "diaria"
            or (periodo.frequencia == "semanal" and d.weekday() == periodo.dia_semana)
            or (periodo.frequencia == "mensal" and (d + timedelta(days=1)).month != d.month)
        ):
            datas.append(d)
        d += timedelta(days=1)
    return datas


def entrega_snapshot(
    sistema: str, historicos: Sequence[Mudancas], datas: Sequence[date], inicio: date, fim: date, formato: str = "csv"
) -> Entrega:
    """Fotos completas (um CSV por entidade e data) das datas de `datas` entre `inicio` e
    `fim`, e o registro dos defeitos entregues pela primeira vez nelas.

    `datas` são todas as datas de foto até `fim` (inclusive as anteriores a `inicio`, que
    decidem em que foto cada versão é entregue pela primeira vez).
    """
    separador, encoding = FORMATOS[formato]
    todas = sorted(d for d in datas if d <= fim)
    no_intervalo = [d for d in todas if d >= inicio]
    if not historicos or not no_intervalo:
        return Entrega()
    spark = historicos[0].df.sparkSession
    calendario = spark.createDataFrame([(d,) for d in todas], "_foto date")
    cabecalhos, partes = {}, []
    for h in historicos:
        cabecalhos[h.entidade], parte = _fotos(h, calendario, inicio, fim, separador)
        partes.append(parte)
    linhas = reduce(lambda a, b: a.unionByName(b), partes).orderBy("data", "entidade", "_n")
    lotes, anotadas = _lotes(sistema, linhas, no_intervalo, cabecalhos, separador, encoding, formato)
    defeitos = tabela_de_registros(spark, registros(_tabela_de_anotadas(anotadas), sistema, seq_coluna=None))
    return Entrega(lotes=lotes, defeitos=[defeitos] if defeitos is not None else [])


def registrar_por_arquivo(entrega: Entrega, defeitos: Defeitos, tipo: str, spark: "SparkSession") -> Entrega:
    """A entrega com o defeito de arquivo inteiro `tipo` (ex.: `encoding_latin1`) registrado
    em cada arquivo entregue, quando o roteiro dá ao sistema a taxa `arquivo`."""
    if defeitos.taxa(tipo) != "arquivo" or not entrega.lotes:
        return entrega
    linhas = [
        defeitos.linha_agregada(tipo, lote.entidade, lote.data_simulada, arquivo=lote.arquivo.caminho)
        for lote in entrega.lotes
    ]
    return replace(entrega, defeitos=[*entrega.defeitos, tabela_de_registros(spark, linhas)])


def _fotos(historico: Mudancas, calendario: "DataFrame", inicio: date, fim: date, separador: str):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    chave = list(historico.chave)
    df = historico.df
    if "_defeitos" not in df.columns:
        df = df.withColumn("_defeitos", F.array().cast(f"array<{ANOTACAO}>"))
    imagem = [c for c in df.columns if c not in (*chave, "op", "ts_operacao") and not c.startswith("_")]
    colunas = [*chave, *imagem]

    # Cada versão vale do dia da operação até o dia da próxima operação da mesma chave.
    ordem_op = F.expr("CASE op WHEN 'I' THEN 0 WHEN 'U' THEN 1 ELSE 2 END")
    versoes = (
        df.withColumn("_desde", F.to_date("ts_operacao"))
        .withColumn("_ate", F.lead("_desde").over(Window.partitionBy(*chave).orderBy("ts_operacao", ordem_op)))
        .where(F.col("op") != "D")
    )
    vigentes = versoes.join(
        calendario,
        (F.col("_foto") >= F.col("_desde")) & (F.col("_ate").isNull() | (F.col("_foto") < F.col("_ate"))),
    ).withColumn("_primeira", F.min("_foto").over(Window.partitionBy(*chave, "ts_operacao")))
    recorte = vigentes.where(F.col("_foto").between(F.lit(inicio), F.lit(fim)))

    def texto(coluna: str):
        if isinstance(df.schema[coluna].dataType, TimestampType):
            return F.date_format(coluna, "yyyy-MM-dd HH:mm:ss").alias(coluna)
        return F.col(coluna).cast("string").alias(coluna)

    opcoes = {
        "sep": separador, "quote": '"', "escape": '"', "nullValue": "", "emptyValue": '""',
        "ignoreLeadingWhiteSpace": "false", "ignoreTrailingWhiteSpace": "false",
    }
    linhas = recorte.select(
        F.col("_foto").cast("string").alias("data"),
        F.lit(historico.entidade).alias("entidade"),
        F.row_number().over(Window.partitionBy("_foto").orderBy(*chave)).alias("_n"),
        F.to_csv(F.struct(*[texto(c) for c in colunas]), opcoes).alias("linha"),
        F.when(F.col("_foto") == F.col("_primeira"), F.col("_defeitos"))
        .otherwise(F.array().cast(f"array<{ANOTACAO}>"))
        .alias("_defeitos"),
    )
    return colunas, linhas


def _lotes(sistema, linhas: "DataFrame", datas, cabecalhos, separador, encoding, formato):
    """Um lote por data e entidade (na ordem dos históricos), mesmo sem linhas.

    As linhas chegam em fluxo, ordenadas por data (`toLocalIterator`), e cada data vira bytes
    assim que termina: a memória guarda uma foto por vez além dos arquivos prontos, e não
    todas as fotos do intervalo. Devolve também as linhas com anotação de defeito.
    """
    lotes, anotadas = [], []
    pendentes = iter(datas)
    atual: str | None = None
    grupos: dict[str, list[str]] = {}

    def fechar(ate: str | None) -> None:
        """Gera os lotes das datas pendentes até `ate` (inclusive; `None` = todas)."""
        for data_foto in pendentes:
            for entidade, colunas in cabecalhos.items():
                linhas_ = grupos.get(entidade, []) if data_foto.isoformat() == atual else []
                conteudo = "".join(f"{linha}\n" for linha in [separador.join(colunas), *linhas_])
                arquivo = Arquivo(
                    caminho_landing(sistema, entidade, data_foto, "csv"), conteudo.encode(encoding, errors="strict")
                )
                lotes.append(Lote(sistema, entidade, data_foto, formato, len(linhas_), arquivo))
            if data_foto.isoformat() == ate:
                return

    for data, entidade, _, linha, anotacoes in linhas.toLocalIterator():
        if data != atual:
            if atual is not None:
                fechar(atual)
            atual, grupos = data, {}
        grupos.setdefault(entidade, []).append(linha)
        if anotacoes:
            anotadas.append({"data": data, "entidade": entidade, "_defeitos": [a.asDict() for a in anotacoes]})
    fechar(None)
    return lotes, anotadas


def _tabela_de_anotadas(anotadas: list[dict]):
    import pyarrow as pa

    if not anotadas:
        return pa.table({"data": pa.array([], pa.string()), "entidade": pa.array([], pa.string())})
    return pa.Table.from_pylist(anotadas)


__all__ = ["FORMATOS", "datas_de_snapshot", "entrega_snapshot", "registrar_por_arquivo"]
