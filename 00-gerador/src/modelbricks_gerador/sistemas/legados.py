"""Regras comuns aos legados do Expresso Vale do Sol (*SolTicket* e *SolFin*).

A empresa foi adquirida em 01/07/2025 (`eventos.aquisicao_expresso_vale_do_sol`). Antes
disso os legados não entregam nada; no dia da aquisição chega a carga histórica inteira,
que é a primeira foto completa das tabelas; depois, uma foto por semana. As datas vêm da
agenda `parametros.legados.snapshots`, cujo `desde` é a data da aquisição, e as fotos são
tiradas pelo motor de snapshot (`snapshot.py`), em CSV `;` com encoding `latin1`.

O `latin1` é um defeito de arquivo inteiro (`defeitos.encoding_latin1`): cada arquivo
entregue tem um registro em `gabarito.defeitos`, com granularidade `arquivo`.

Os volumes dos legados contam a carga histórica (do início da história à véspera da
aquisição); depois dela, o sistema segue no mesmo ritmo diário.
"""

from collections.abc import Callable
from dataclasses import replace
from datetime import date
from typing import TYPE_CHECKING

from ..cdc import Mudancas
from ..defeitos import Defeitos
from ..mundo import linhas as linhas_na_escala
from ..snapshot import datas_de_snapshot, entrega_snapshot, registrar_por_arquivo
from .base import Contexto, Entrega

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

DEFEITO_DE_ARQUIVO = "encoding_latin1"


def entregar(
    contexto: Contexto,
    sistema: str,
    inicio: date,
    fim: date,
    historicos: Callable[[], list[Mudancas]],
    mapa_chaves: Callable[[], "DataFrame"],
) -> Entrega:
    """Fotos do legado no intervalo, com o `latin1` registrado por arquivo e o mapa de
    chaves acumulado. Sem nenhuma foto até `fim` (antes da aquisição), não há entrega nem
    mapa: o legado ainda não existe para o grupo."""
    datas = datas_de_snapshot(contexto.roteiro.parametros.legados.snapshots, contexto.roteiro.datas.inicio, fim)
    if not datas:
        return Entrega()
    if not any(inicio <= d <= fim for d in datas):
        return Entrega(mapa_chaves=mapa_chaves())
    formato = contexto.roteiro.sistemas[sistema].formato
    entrega = entrega_snapshot(sistema, historicos(), datas, inicio, fim, formato)
    entrega = registrar_por_arquivo(entrega, Defeitos(contexto.roteiro, sistema), DEFEITO_DE_ARQUIVO, contexto.spark)
    return replace(entrega, mapa_chaves=mapa_chaves())


def por_dia(contexto: Contexto, volume: int) -> tuple[int, float]:
    """Linhas por dia (parte inteira e fração) para que a carga histórica tenha `volume`
    linhas na escala 1."""
    datas = contexto.roteiro.datas
    dias = (contexto.roteiro.parametros.legados.snapshots.desde - datas.inicio).days
    taxa = linhas_na_escala(volume, contexto.escala) / dias
    return int(taxa), taxa - int(taxa)


def sorteio(pesos: dict[str, float], u: str) -> str:
    """Expressão SQL que escolhe um dos nomes de `pesos` pelo uniforme `u`."""
    nomes, total, acumulado, casos = sorted(pesos), sum(pesos.values()), 0.0, []
    for nome in nomes[:-1]:
        acumulado += pesos[nome] / total
        casos.append(f"WHEN {u} < {acumulado!r} THEN {_literal(nome)}")
    ultimo = _literal(nomes[-1])
    return f"CASE {' '.join(casos)} ELSE {ultimo} END" if casos else ultimo


def _literal(texto: str) -> str:
    return "'" + texto.replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = ["DEFEITO_DE_ARQUIVO", "entregar", "por_dia", "sorteio"]
