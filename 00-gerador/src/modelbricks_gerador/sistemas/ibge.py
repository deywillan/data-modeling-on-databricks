"""IBGE: arquivo de referência (municípios, UFs e regiões), entregue uma vez na carga inicial.

O CSV vem versionado no pacote (`dados/ibge_municipios.csv`, extraído da API de
Localidades do IBGE pelo `scripts/extrair_ibge.py`) e é copiado byte a byte: o setup não
depende de rede.
"""

from datetime import date
from importlib import resources

from ..escritor import Arquivo, caminho_landing
from .base import Contexto, Entrega, Lote


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    if not contexto.carga_inicial:
        return Entrega()
    conteudo = resources.files("modelbricks_gerador").joinpath("dados/ibge_municipios.csv").read_bytes()
    arquivo = Arquivo(caminho_landing("ibge", "municipios", fim, "csv"), conteudo)
    return Entrega(lotes=[Lote("ibge", "municipios", fim, "csv", conteudo.count(b"\n") - 1, arquivo)])
