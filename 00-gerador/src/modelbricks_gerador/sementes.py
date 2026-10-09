"""Seeds determinísticos derivados de hash estável.

Nunca use `hash()` do Python para seed: ele é aleatorizado por processo.
"""

import hashlib

_MASCARA_31_BITS = 0x7FFFFFFF


def semente(*partes: object) -> int:
    """Seed inteiro (0 a 2³¹−1) estável para as partes informadas.

    Exemplo: `semente(roteiro.seed_base, "embarque", date(2025, 12, 31))`. O mesmo conjunto
    de partes, na mesma ordem, gera sempre o mesmo seed, em qualquer máquina ou processo.
    """
    texto = "|".join(str(parte) for parte in partes)
    digest = hashlib.blake2b(texto.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & _MASCARA_31_BITS
