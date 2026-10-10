"""Relógio simulado de um catálogo, guardado na pasta reservada `_controle/` da landing.

Cada avanço grava um arquivo novo `_controle/relogio/<AAAA-MM-DD>.json`; o relógio atual é
o de maior data. Assim a landing continua sem sobrescrita e guarda o histórico de avanços.
Os pipelines leem `landing/<sistema>/...`, nunca `_controle/`.

O relógio também guarda a versão do roteiro e a escala com que o catálogo foi gerado, e
`conferir_relogio` recusa continuar com outras (os dados novos não combinariam com os antigos).

Interface: `ler_relogio(destino) -> Relogio | None`, `gravar_relogio(destino, relogio)` e
`conferir_relogio(relogio, versao_roteiro, escala=None)`.
"""

import json
import math
from dataclasses import asdict, dataclass
from datetime import date

from .destino import Destino
from .escritor import Arquivo

PASTA = "_controle/relogio"


class RelogioIncompativel(RuntimeError):
    """O catálogo foi gerado com outra versão do roteiro ou outra escala que a pedida agora."""


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


def conferir_relogio(relogio: Relogio, versao_roteiro: int, escala: float | None = None) -> None:
    """Levanta `RelogioIncompativel` se o relógio foi gravado com outra versão do roteiro ou,
    quando `escala` é informada, com outra escala. Sem reset, misturar as duas gerações
    deixaria na landing dados que não pertencem à mesma história."""
    if relogio.versao_roteiro != versao_roteiro:
        raise RelogioIncompativel(
            f"O destino foi gerado com a versão {relogio.versao_roteiro} do roteiro, mas o roteiro atual é a "
            f"versão {versao_roteiro}. Os dados novos não combinariam com os que já estão na landing: "
            "recomece do zero com gerar_fatia(..., reset=True)."
        )
    if escala is not None and not math.isclose(relogio.escala, escala, rel_tol=1e-9):
        raise RelogioIncompativel(
            f"O destino foi gerado na escala {relogio.escala:g}, mas a escala pedida é {escala:g}. Os dados "
            "novos não combinariam com os que já estão na landing: recomece do zero com gerar_fatia(..., reset=True)."
        )
