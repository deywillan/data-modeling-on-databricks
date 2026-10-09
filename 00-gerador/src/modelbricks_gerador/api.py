"""API pública do gerador: o único seam que jobs e testes usam.

- `gerar_fatia(artigo, destino, ...)`: carga inicial da fatia do artigo.
- `avancar_tempo(artigo, destino, dias, ...)`: lotes dos dias seguintes ao relógio.

As duas devolvem um `Resultado` e imprimem uma mensagem curta em português.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING

from .chaves import COLUNAS_MAPA
from .defeitos import COLUNAS_DEFEITOS, ESQUEMA
from .destino import Destino
from .relogio import Relogio, gravar_relogio, ler_relogio
from .roteiro import Roteiro, carregar_roteiro
from .sistemas import PROJECOES, Contexto, Entrega

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SCHEMAS_MEDALHAO = ("landing", "bronze", "silver", "gold")
TABELA_MAPA_CHAVES = "mapa_chaves"  # gabarito: chave verdadeira → chave em cada sistema
TABELA_DEFEITOS = "defeitos"  # gabarito: cada defeito injetado nos dados da landing


@dataclass(frozen=True)
class Resultado:
    """Resumo de uma chamada da API."""

    artigo: str
    data_relogio: date  # data simulada do relógio ao final da chamada
    gerou: bool  # False quando nada foi gravado (ex.: carga inicial já feita)
    arquivos: int  # arquivos novos gravados na landing (sem contar o relógio)
    mensagem: str


def gerar_fatia(
    artigo: str,
    destino: Destino,
    escala: float = 1,
    ate: date | None = None,
    reset: bool = False,
    *,
    spark: "SparkSession | None" = None,
    roteiro: Roteiro | str | Path | None = None,
) -> Resultado:
    """Prepara o ambiente do artigo e grava a carga inicial da fatia até `ate`.

    Cria os schemas `landing` (com Volume), `bronze`, `silver`, `gold` e, se a fatia pedir,
    `gabarito`; só a landing (e o gabarito) recebem dados. Se o relógio já existir, não
    altera nada e informa a data atual; com `reset=True`, recria o ambiente do zero.
    `ate` padrão: `datas.carga_inicial` do roteiro. `roteiro` aceita um caminho alternativo.
    """
    roteiro = _roteiro(roteiro)
    fatia = roteiro.fatia(artigo)
    if escala <= 0:
        raise ValueError(f"escala deve ser positiva (recebido {escala}).")
    ate = ate or roteiro.datas.carga_inicial
    if ate < roteiro.datas.inicio:
        raise ValueError(f"ate ({ate}) é anterior ao início da história ({roteiro.datas.inicio}).")

    schemas = SCHEMAS_MEDALHAO + (("gabarito",) if fatia.gabarito else ())
    destino.preparar(schemas, reset=reset)

    relogio = ler_relogio(destino)
    if relogio is not None:
        return _concluir(
            Resultado(
                artigo, relogio.data_simulada, False, 0,
                f"{artigo}: carga inicial já feita; relógio em {relogio.data_simulada:%d/%m/%Y}. "
                "Nada foi alterado (use reset=True para recomeçar do zero).",
            )
        )

    contexto = Contexto(_sessao(spark, roteiro), roteiro, fatia, float(escala), carga_inicial=True)
    entregas = _projetar(contexto, roteiro.datas.inicio, ate, destino)
    arquivos = sum(len(e.lotes) for e in entregas)
    if fatia.gabarito:
        for tabela in contexto.mundo:
            destino.gravar_gabarito(tabela, contexto.mundo.ordenado(tabela))
        mapas = [e.mapa_chaves for e in entregas if e.mapa_chaves is not None]
        if mapas:
            mapa = reduce(lambda a, b: a.unionByName(b), mapas)
            destino.gravar_gabarito(TABELA_MAPA_CHAVES, mapa.orderBy(*COLUNAS_MAPA))
        _gravar_defeitos(contexto, entregas, destino, acrescentar=False)
    gravar_relogio(destino, Relogio(ate, artigo, float(escala), roteiro.versao))
    return _concluir(
        Resultado(
            artigo, ate, True, arquivos,
            f"{artigo}: carga inicial gravada ({arquivos} arquivos na landing); relógio em {ate:%d/%m/%Y}.",
        )
    )


def avancar_tempo(
    artigo: str,
    destino: Destino,
    dias: int,
    *,
    spark: "SparkSession | None" = None,
    roteiro: Roteiro | str | Path | None = None,
) -> Resultado:
    """Gera os lotes de `relógio+1` até `relógio+dias` da fatia, grava só arquivos novos e
    avança o relógio. Exige a carga inicial (`gerar_fatia`) feita no mesmo destino."""
    roteiro = _roteiro(roteiro)
    fatia = roteiro.fatia(artigo)
    if dias < 1:
        raise ValueError(f"dias deve ser pelo menos 1 (recebido {dias}).")
    relogio = ler_relogio(destino)
    if relogio is None:
        raise RuntimeError(f"{artigo}: o destino não tem relógio. Rode gerar_fatia antes de avancar_tempo.")
    if relogio.artigo != artigo:
        raise RuntimeError(f"O destino pertence ao artigo {relogio.artigo}, não a {artigo}.")

    inicio = relogio.data_simulada + timedelta(days=1)
    fim = relogio.data_simulada + timedelta(days=dias)
    contexto = Contexto(_sessao(spark, roteiro), roteiro, fatia, relogio.escala, carga_inicial=False)
    entregas = _projetar(contexto, inicio, fim, destino)
    arquivos = sum(len(e.lotes) for e in entregas)
    if fatia.gabarito:
        _gravar_defeitos(contexto, entregas, destino, acrescentar=True)
    gravar_relogio(destino, Relogio(fim, artigo, relogio.escala, roteiro.versao))
    return _concluir(
        Resultado(
            artigo, fim, True, arquivos,
            f"{artigo}: {dias} dia(s) gerado(s) ({arquivos} arquivos novos); relógio em {fim:%d/%m/%Y}.",
        )
    )


def _projetar(contexto: Contexto, inicio: date, fim: date, destino: Destino) -> list[Entrega]:
    """Projeta cada sistema da fatia no intervalo e grava os lotes na landing."""
    entregas = []
    for sistema in contexto.fatia.sistemas:
        projetar = PROJECOES.get(sistema)
        if projetar is None:
            print(f"Aviso: o sistema '{sistema}' ainda não tem projeção no gerador; ignorado.")
            continue
        entrega = projetar(contexto, inicio, fim)
        destino.gravar_arquivos(lote.arquivo for lote in entrega.lotes)
        entregas.append(entrega)
    return entregas


def _gravar_defeitos(contexto: Contexto, entregas: list[Entrega], destino: Destino, acrescentar: bool) -> None:
    """Grava os defeitos das entregas em `gabarito.defeitos`. Na carga inicial a tabela é
    criada mesmo sem defeitos; no `avancar_tempo`, só os dos dias novos são acrescentados."""
    partes = [df for e in entregas for df in e.defeitos]
    if not partes:
        if acrescentar:
            return
        partes = [contexto.spark.createDataFrame([], ESQUEMA)]
    defeitos = reduce(lambda a, b: a.unionByName(b), partes).select(*COLUNAS_DEFEITOS)
    ordem = ("sistema", "entidade", "data_entrega", "tipo", "chave_sistema", "seq", "coluna", "arquivo")
    destino.gravar_gabarito(TABELA_DEFEITOS, defeitos.orderBy(*ordem), acrescentar=acrescentar)


def _roteiro(roteiro: Roteiro | str | Path | None) -> Roteiro:
    return roteiro if isinstance(roteiro, Roteiro) else carregar_roteiro(roteiro)


def _sessao(spark: "SparkSession | None", roteiro: Roteiro) -> "SparkSession":
    """Sessão recebida ou a do ambiente (job ou Connect), com o fuso do roteiro fixado."""
    if spark is None:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", roteiro.fuso_horario)
    return spark


def _concluir(resultado: Resultado) -> Resultado:
    print(resultado.mensagem)
    return resultado


__all__ = ["Resultado", "avancar_tempo", "gerar_fatia"]
