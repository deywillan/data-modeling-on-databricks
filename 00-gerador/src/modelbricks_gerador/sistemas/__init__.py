"""Projeções do mundo verdadeiro em cada sistema de origem.

Cada sistema é um módulo com uma função `projetar(contexto, inicio, fim) -> Entrega` (ver
`base.py`): os lotes que o sistema entregaria na landing entre `inicio` e `fim` (inclusive),
os registros de defeitos injetados e o mapa chave verdadeira → chave do sistema. Para
adicionar um sistema, crie o módulo e registre-o em `PROJECOES`, com a mesma chave usada em
`sistemas:` no roteiro. Sistemas da fatia ainda sem projeção são ignorados (e avisados)
pela API.
"""

from collections.abc import Callable
from datetime import date

from . import embarque, ibge
from .base import Contexto, Entrega, Lote

Projecao = Callable[[Contexto, date, date], Entrega]

PROJECOES: dict[str, Projecao] = {
    "embarque": embarque.projetar,
    "ibge": ibge.projetar,
}

__all__ = ["Contexto", "Entrega", "Lote", "Projecao", "PROJECOES"]
