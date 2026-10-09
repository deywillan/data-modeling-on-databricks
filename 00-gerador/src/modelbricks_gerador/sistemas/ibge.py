"""IBGE: arquivo de referência (municípios, UFs e regiões), entregue uma vez na carga inicial.

O CSV vem versionado no pacote (`dados/ibge_municipios.csv`, extraído da API de
Localidades do IBGE pelo `scripts/extrair_ibge.py`) e é copiado byte a byte: o setup não
depende de rede.
"""

from datetime import date
from importlib import resources

from ..escritor import Arquivo, caminho_landing


def projetar(contexto, inicio: date, fim: date) -> list[Arquivo]:
    if not contexto.carga_inicial:
        return []
    conteudo = resources.files("modelbricks_gerador").joinpath("dados/ibge_municipios.csv").read_bytes()
    return [Arquivo(caminho_landing("ibge", "municipios", fim, "csv"), conteudo)]
