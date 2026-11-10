"""IBGE: arquivo de referência (municípios, UFs e regiões), entregue uma vez por ano.

O grupo baixa a lista de municípios do IBGE no dia 2 de janeiro de cada ano, então a landing
recebe um arquivo completo por ano: 02/01/2024 e 02/01/2025 na carga inicial e 02/01/2026 no
`avancar_tempo`. Cada arquivo é a lista inteira, não só o que mudou, e é isso que a Bronze
espelha com overwrite: a tabela passa a ter só o conteúdo do arquivo mais recente.

As entregas não são idênticas. Boa Esperança do Norte (MT, código 5101837) foi instalado em
1º de janeiro de 2025 e só aparece a partir da lista de 2026: as de 2024 e 2025 têm 5.570
municípios, e a de 2026 tem os 5.571 do arquivo versionado. Nenhum sistema do grupo usa
municípios de MT (ver `mundo.ufs_atuacao` no roteiro), então a diferença não deixa nenhum
endereço sem município na Bronze.

O CSV vem versionado no pacote (`dados/ibge_municipios.csv`, extraído da API de
Localidades do IBGE pelo `scripts/extrair_ibge.py`): o setup não depende de rede.
"""

from datetime import date
from importlib import resources

from ..escritor import Arquivo, caminho_landing
from .base import Contexto, Entrega, Lote

# Município → ano da primeira lista anual que o traz. Os ausentes do dicionário estão em todas.
PRIMEIRA_LISTA = {5101837: 2026}  # Boa Esperança do Norte (MT), instalado em 01/01/2025


def datas_de_entrega(inicio: date, fim: date) -> list[date]:
    """Os dias 2 de janeiro entre `inicio` e `fim` (inclusive)."""
    return [d for d in (date(ano, 1, 2) for ano in range(inicio.year, fim.year + 1)) if inicio <= d <= fim]


def conteudo_da_lista(ano: int) -> bytes:
    """O CSV versionado sem os municípios que só entram em listas posteriores a `ano`."""
    linhas = (
        resources.files("modelbricks_gerador").joinpath("dados/ibge_municipios.csv").read_bytes()
    ).splitlines(keepends=True)
    ausentes = {str(codigo).encode() for codigo, primeira in PRIMEIRA_LISTA.items() if primeira > ano}
    return b"".join([linhas[0], *(linha for linha in linhas[1:] if linha.split(b",", 1)[0] not in ausentes)])


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    lotes = []
    for dia in datas_de_entrega(inicio, fim):
        conteudo = conteudo_da_lista(dia.year)
        arquivo = Arquivo(caminho_landing("ibge", "municipios", dia, "csv"), conteudo)
        lotes.append(Lote("ibge", "municipios", dia, "csv", conteudo.count(b"\n") - 1, arquivo))
    return Entrega(lotes=lotes)
