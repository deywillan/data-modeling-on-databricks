"""Relógio simulado de um catálogo, guardado na pasta reservada `_controle/` da landing.

Cada avanço grava um arquivo novo `_controle/relogio/<AAAA-MM-DD>.json`; o relógio atual é
o de maior data. Assim a landing continua sem sobrescrita e guarda o histórico de avanços.
Os pipelines leem `landing/<sistema>/...`, nunca `_controle/`.

Interface: `ler_relogio(destino) -> Relogio | None` e `gravar_relogio(destino, relogio)`.
"""

import json
from dataclasses import asdict, dataclass
from datetime import date

from .destino import Destino
from .escritor import Arquivo

PASTA = "_controle/relogio"


@dataclass(frozen=True)
class Relogio:
    """Estado do relógio: data simulada atual e parâmetros com que o catálogo foi gerado."""

    data_simulada: date
    artigo: str
    escala: float
    versao_roteiro: int


def ler_relogio(destino: Destino) -> Relogio | None:
    """Relógio atual do destino, ou `None` se o catálogo ainda não recebeu a carga inicial."""
    arquivos = [c for c in destino.listar(PASTA) if c.endswith(".json")]
    if not arquivos:
        return None
    dados = json.loads(destino.ler(max(arquivos)))
    return Relogio(
        data_simulada=date.fromisoformat(dados["data_simulada"]),
        artigo=dados["artigo"],
        escala=float(dados["escala"]),
        versao_roteiro=int(dados["versao_roteiro"]),
    )


def gravar_relogio(destino: Destino, relogio: Relogio) -> None:
    """Registra uma nova data simulada (arquivo novo; nunca sobrescreve)."""
    dados = asdict(relogio) | {"data_simulada": relogio.data_simulada.isoformat()}
    conteudo = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    destino.gravar_arquivos([Arquivo(f"{PASTA}/{relogio.data_simulada.isoformat()}.json", conteudo)])
