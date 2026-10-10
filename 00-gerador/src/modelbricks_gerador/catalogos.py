"""Nome dos catálogos do Unity Catalog: a única função que os produz.

Cada artigo tem três catálogos, um por camada, com o ambiente no fim do nome:
`aNN_bronze_<amb>`, `aNN_silver_<amb>` e `aNN_gold_<amb>`. O ambiente vem do target do
bundle (`dev` → `dev`, `prod` → `prd`); os testes usam um ambiente próprio (`t` + hexadecimais).
Como o nome entra em comandos SQL, artigo, camada e ambiente são validados aqui, e só aqui.
"""

import re

CAMADAS = ("bronze", "silver", "gold")
_ARTIGO = re.compile(r"(\d{2})(?:-[a-z0-9-]+)?")
_AMBIENTE = re.compile(r"[a-z][a-z0-9]{0,15}")


def numero_do_artigo(artigo: str) -> str:
    """Os dois dígitos do artigo (`03-3fn` → `03`)."""
    casamento = _ARTIGO.fullmatch(artigo)
    if not casamento:
        raise ValueError(f"artigo inválido: '{artigo}' (esperado 'NN' ou 'NN-tema').")
    return casamento[1]


def validar_ambiente(ambiente: str) -> str:
    """Devolve o ambiente se ele cabe no nome de um catálogo (minúsculas e dígitos, até 16)."""
    if not _AMBIENTE.fullmatch(ambiente):
        raise ValueError(
            f"ambiente inválido: '{ambiente}' (use letra minúscula seguida de até 15 letras minúsculas ou dígitos)."
        )
    return ambiente


def nome_catalogo(artigo: str, camada: str, ambiente: str) -> str:
    """Catálogo de uma camada do artigo no ambiente, ex.: `a03_bronze_dev`."""
    numero = numero_do_artigo(artigo)
    if camada not in CAMADAS:
        raise ValueError(f"camada inválida: '{camada}' (esperado uma de {', '.join(CAMADAS)}).")
    return f"a{numero}_{camada}_{validar_ambiente(ambiente)}"


def catalogos_do_artigo(artigo: str, ambiente: str) -> dict[str, str]:
    """Os três catálogos do artigo, por camada."""
    return {camada: nome_catalogo(artigo, camada, ambiente) for camada in CAMADAS}
