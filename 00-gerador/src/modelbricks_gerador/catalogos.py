"""Nome dos catálogos do Unity Catalog: a única função que os produz.

A série constrói um único lakehouse, com três catálogos, um por camada, e o ambiente no fim do
nome: `bronze_<amb>`, `silver_<amb>` e `gold_<amb>`. Cada artigo acrescenta a sua parte a esse
lakehouse (os artigos da Silver e da Gold, num schema próprio). O ambiente vem do target do
bundle (`dev` → `dev`, `prod` → `prd`); os testes usam um ambiente próprio (`t` + hexadecimais).
Como o nome entra em comandos SQL, camada e ambiente são validados aqui, e só aqui.
"""

import re

CAMADAS = ("bronze", "silver", "gold")
_AMBIENTE = re.compile(r"[a-z][a-z0-9]{0,15}")


def validar_ambiente(ambiente: str) -> str:
    """Devolve o ambiente se ele cabe no nome de um catálogo (minúsculas e dígitos, até 16)."""
    if not _AMBIENTE.fullmatch(ambiente):
        raise ValueError(
            f"ambiente inválido: '{ambiente}' (use letra minúscula seguida de até 15 letras minúsculas ou dígitos)."
        )
    return ambiente


def nome_catalogo(camada: str, ambiente: str) -> str:
    """Catálogo de uma camada no ambiente, ex.: `bronze_dev`."""
    if camada not in CAMADAS:
        raise ValueError(f"camada inválida: '{camada}' (esperado uma de {', '.join(CAMADAS)}).")
    return f"{camada}_{validar_ambiente(ambiente)}"


def catalogos_da_serie(ambiente: str) -> dict[str, str]:
    """Os três catálogos da série no ambiente, por camada."""
    return {camada: nome_catalogo(camada, ambiente) for camada in CAMADAS}
