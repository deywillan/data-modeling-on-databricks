"""Gerador de dados sintéticos do Grupo ModelBricks.

API pública: `gerar_fatia` e `avancar_tempo`, com um `Destino` (`DestinoLocal` nos testes;
Unity Catalog nos jobs). O resto do pacote é interno.
"""

from .api import Resultado, avancar_tempo, gerar_fatia
from .catalogos import catalogos_do_artigo, nome_catalogo
from .destino import ArquivoJaExiste, Destino, DestinoLocal
from .destino_uc import DestinoUC
from .relogio import RelogioIncompativel
from .roteiro import RoteiroInvalido, carregar_roteiro

__all__ = [
    "ArquivoJaExiste",
    "Destino",
    "DestinoLocal",
    "DestinoUC",
    "RelogioIncompativel",
    "Resultado",
    "RoteiroInvalido",
    "avancar_tempo",
    "carregar_roteiro",
    "catalogos_do_artigo",
    "gerar_fatia",
    "nome_catalogo",
]
