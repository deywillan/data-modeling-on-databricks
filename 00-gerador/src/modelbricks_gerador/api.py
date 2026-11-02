"""API pública do gerador: o único seam que jobs e testes usam.

- `gerar_carga_inicial(destino, ...)`: carga inicial de todos os sistemas na Bronze da série.
- `avancar_tempo(destino, dias, ...)`: lotes dos dias seguintes ao relógio.

As duas devolvem um `Resultado` e imprimem uma mensagem curta em português.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import date, timedelta
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING

from .chaves import COLUNAS_MAPA
from .defeitos import COLUNAS_DEFEITOS, ESQUEMA
from .destino import Destino
from .relogio import Relogio, conferir_relogio, gravar_relogio, ler_relogio
from .roteiro import Roteiro, carregar_roteiro
from .sistemas import PROJECOES, Contexto, Entrega

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

TABELA_MAPA_CHAVES = "mapa_chaves"  # gabarito: chave verdadeira → chave em cada sistema
TABELA_DEFEITOS = "defeitos"  # gabarito: cada defeito injetado nos dados da landing


@dataclass(frozen=True)
class Resultado:
    """Resumo de uma chamada da API."""

    data_relogio: date  # data simulada do relógio ao final da chamada
    gerou: bool  # False quando nada foi gravado (ex.: carga inicial já feita)
    arquivos: int  # arquivos novos gravados na landing (sem contar o relógio)
    mensagem: str


def gerar_carga_inicial(
    destino: Destino,
    escala: float = 1,
    ate: date | None = None,
    reset: bool = False,
    *,
    spark: "SparkSession | None" = None,
    roteiro: Roteiro | str | Path | None = None,
) -> Resultado:
    """Prepara o lakehouse da série e grava a carga inicial de todos os sistemas até `ate`.

    Cria os três catálogos da série (`bronze_<amb>`, `silver_<amb>`, `gold_<amb>`), no Bronze
    um schema por sistema da carga com o Volume `landing` e o schema `_controle` (relógio), e o
    schema `gabarito` no Silver; só as landings, o relógio e o gabarito recebem dados. O
    `destino` já sabe o ambiente. Se o relógio já existir, não altera nada e informa a data
    atual (mas levanta `RelogioIncompativel` se ele foi gravado com outra versão do roteiro ou
    outra escala); com `reset=True`, apaga os três catálogos e recomeça do zero.
    `ate` padrão: `datas.carga_inicial` do roteiro. `roteiro` aceita um caminho alternativo.

    Se a chamada cair no meio, basta repeti-la: arquivo já gravado com os mesmos bytes conta
    como entregue (bytes diferentes continuam sendo erro) e o relógio só é gravado no fim.
    """
    roteiro = _roteiro(roteiro)
    if escala <= 0:
        raise ValueError(f"escala deve ser positiva (recebido {escala}).")
    ate = ate or roteiro.datas.carga_inicial
    if ate < roteiro.datas.inicio:
        raise ValueError(f"ate ({ate}) é anterior ao início da história ({roteiro.datas.inicio}).")

    destino.preparar(roteiro.carga.sistemas, reset=reset)

    relogio = ler_relogio(destino)
    if relogio is not None:
        conferir_relogio(relogio, roteiro.versao, float(escala))
        return _concluir(
            Resultado(
                relogio.data_simulada, False, 0,
                f"Carga inicial já feita; relógio em {relogio.data_simulada:%d/%m/%Y}. "
                "Nada foi alterado (use reset=True para recomeçar do zero).",
            )
        )

    contexto = Contexto(_sessao(spark, roteiro), roteiro, roteiro.carga, float(escala), carga_inicial=True)
    entregas, arquivos = _projetar(contexto, roteiro.datas.inicio, ate, destino)
    t0 = time.monotonic()
    for tabela in contexto.mundo:
        destino.gravar_gabarito(tabela, contexto.mundo.ordenado(tabela))
    _gravar_mapa_chaves(entregas, destino)
    _gravar_defeitos(contexto, entregas, destino, acrescentar=False)
    print(f"  gabarito: {time.monotonic() - t0:.0f} s")
    gravar_relogio(destino, Relogio(ate, float(escala), roteiro.versao))
    return _concluir(
        Resultado(
            ate, True, arquivos,
            f"Carga inicial gravada ({arquivos} arquivos na landing); relógio em {ate:%d/%m/%Y}.",
        )
    )


def avancar_tempo(
    destino: Destino,
    dias: int,
    *,
    spark: "SparkSession | None" = None,
    roteiro: Roteiro | str | Path | None = None,
) -> Resultado:
    """Gera os lotes de `relógio+1` até `relógio+dias` de todos os sistemas da carga, grava só
    arquivos novos e avança o relógio. Exige a carga inicial (`gerar_carga_inicial`) feita no
    mesmo destino, com a mesma versão do roteiro (senão levanta `RelogioIncompativel`).

    Se a chamada cair no meio, basta repeti-la: o relógio só avança no fim, os arquivos já
    gravados com os mesmos bytes contam como entregues e o gabarito do intervalo é regravado
    sem duplicar."""
    roteiro = _roteiro(roteiro)
    if dias < 1:
        raise ValueError(f"dias deve ser pelo menos 1 (recebido {dias}).")
    relogio = ler_relogio(destino)
    if relogio is None:
        raise RuntimeError("O destino não tem relógio. Rode gerar_carga_inicial antes de avancar_tempo.")
    conferir_relogio(relogio, roteiro.versao)

    inicio = relogio.data_simulada + timedelta(days=1)
    fim = relogio.data_simulada + timedelta(days=dias)
    contexto = Contexto(_sessao(spark, roteiro), roteiro, roteiro.carga, relogio.escala, carga_inicial=False)
    entregas, arquivos = _projetar(contexto, inicio, fim, destino)
    _gravar_mapa_chaves(entregas, destino)
    # Uma tentativa anterior pode ter acrescentado os defeitos deste intervalo e caído antes do
    # relógio: apaga as linhas do intervalo para a nova tentativa não duplicá-las.
    destino.apagar_do_gabarito(TABELA_DEFEITOS, "data_entrega", inicio)
    _gravar_defeitos(contexto, entregas, destino, acrescentar=True)
    gravar_relogio(destino, Relogio(fim, relogio.escala, roteiro.versao))
    return _concluir(
        Resultado(
            fim, True, arquivos,
            f"{dias} dia(s) gerado(s) ({arquivos} arquivos novos); relógio em {fim:%d/%m/%Y}.",
        )
    )


def _projetar(contexto: Contexto, inicio: date, fim: date, destino: Destino) -> tuple[list[Entrega], int]:
    """Projeta cada sistema da carga no intervalo e grava os lotes na landing.

    A gravação de um sistema roda numa thread enquanto o Spark gera o seguinte (a geração e o
    upload levam, cada um, metade do tempo do setup). No máximo uma gravação fica pendente,
    então a memória guarda os arquivos de no máximo dois sistemas. Devolve as entregas sem os
    bytes dos lotes (já gravados) e o número de arquivos gravados.
    """
    entregas, arquivos = [], 0
    with ThreadPoolExecutor(max_workers=1) as gravador:
        pendente = None
        for sistema in contexto.carga.sistemas:
            projetar = PROJECOES.get(sistema)
            if projetar is None:
                print(f"Aviso: o sistema '{sistema}' ainda não tem projeção no gerador; ignorado.")
                continue
            t0 = time.monotonic()
            entrega = projetar(contexto, inicio, fim)
            geracao = time.monotonic() - t0
            if pendente is not None:
                pendente.result()  # propaga a primeira exceção (ex.: ArquivoJaExiste)
            pendente = gravador.submit(_gravar_lotes, destino, sistema, entrega.lotes, geracao)
            arquivos += len(entrega.lotes)
            entregas.append(replace(entrega, lotes=[]))
        if pendente is not None:
            pendente.result()
    return entregas, arquivos


def _gravar_lotes(destino: Destino, sistema: str, lotes: list, geracao: float) -> None:
    t0 = time.monotonic()
    destino.gravar_arquivos(lote.arquivo for lote in lotes)
    megabytes = sum(len(lote.arquivo.conteudo) for lote in lotes) / 1e6
    print(
        f"  {sistema}: {len(lotes)} arquivos, {megabytes:.1f} MB "
        f"(geração {geracao:.0f} s, gravação na landing {time.monotonic() - t0:.0f} s)"
    )


def _gravar_mapa_chaves(entregas: list[Entrega], destino: Destino) -> None:
    """Regrava `gabarito.mapa_chaves`. O mapa de cada entrega é acumulado (todas as chaves
    que o sistema usou até o fim do intervalo), então substituir a tabela no `avancar_tempo`
    inclui as chaves que surgiram nos dias novos sem duplicar as antigas."""
    mapas = [e.mapa_chaves for e in entregas if e.mapa_chaves is not None]
    if mapas:
        mapa = reduce(lambda a, b: a.unionByName(b), mapas).distinct()
        destino.gravar_gabarito(TABELA_MAPA_CHAVES, mapa.orderBy(*COLUNAS_MAPA))


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


__all__ = ["Resultado", "avancar_tempo", "gerar_carga_inicial"]
