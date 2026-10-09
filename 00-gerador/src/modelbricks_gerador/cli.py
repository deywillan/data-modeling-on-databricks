"""Pontos de entrada dos jobs do bundle (`python_wheel_task`).

Os parâmetros do job chegam como texto, por exemplo:

    gerar_fatia --artigo 01-bronze --escala 1 --reset false
    avancar_tempo --artigo 01-bronze --dias 1

O catálogo vem da fatia do artigo no roteiro. `--sufixo-catalogo` existe só para os testes
de fumaça do autor (ex.: `_t02`), para não tocar no catálogo real do artigo.
"""

import argparse
import re

from . import api
from .destino_uc import DestinoUC
from .roteiro import carregar_roteiro


def _booleano(texto: str) -> bool:
    valor = texto.strip().lower()
    if valor not in ("true", "false"):
        raise argparse.ArgumentTypeError(f"use true ou false (recebido '{texto}')")
    return valor == "true"


def _sufixo(texto: str) -> str:
    if not re.fullmatch(r"[a-z0-9_]*", texto):
        raise argparse.ArgumentTypeError(f"sufixo de catálogo inválido: '{texto}'")
    return texto


def gerar_fatia(argv: list[str] | None = None) -> None:
    """Job `<NN>_setup`: prepara o catálogo do artigo e grava a carga inicial da fatia."""
    parser = argparse.ArgumentParser(prog="gerar_fatia", description=gerar_fatia.__doc__)
    parser.add_argument("--artigo", required=True, help="artigo da fatia, ex.: 01-bronze")
    parser.add_argument("--escala", type=float, default=1.0, help="multiplicador dos volumes (padrão 1)")
    parser.add_argument("--reset", type=_booleano, default=False, help="true apaga e recria o catálogo")
    parser.add_argument("--sufixo-catalogo", type=_sufixo, default="", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    from databricks.connect import DatabricksSession

    roteiro = carregar_roteiro()
    catalogo = roteiro.fatia(args.artigo).catalogo + args.sufixo_catalogo
    spark = DatabricksSession.builder.getOrCreate()
    destino = DestinoUC(catalogo, spark=spark)
    api.gerar_fatia(args.artigo, destino, escala=args.escala, reset=args.reset, spark=spark, roteiro=roteiro)
    print(f"Landing em {destino.volume}.")


def _dias(texto: str) -> int:
    try:
        valor = int(texto)
    except ValueError:
        raise argparse.ArgumentTypeError(f"dias deve ser um número inteiro (recebido '{texto}')") from None
    if valor < 1:
        raise argparse.ArgumentTypeError(f"dias deve ser pelo menos 1 (recebido {valor})")
    return valor


def avancar_tempo(argv: list[str] | None = None) -> None:
    """Job `avancar_tempo`: avança o relógio do catálogo do artigo e grava os lotes novos."""
    parser = argparse.ArgumentParser(prog="avancar_tempo", description=avancar_tempo.__doc__)
    parser.add_argument("--artigo", required=True, help="artigo da fatia, ex.: 01-bronze")
    parser.add_argument("--dias", type=_dias, default=1, help="dias simulados a avançar (padrão 1)")
    parser.add_argument("--sufixo-catalogo", type=_sufixo, default="", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    from databricks.connect import DatabricksSession

    roteiro = carregar_roteiro()
    catalogo = roteiro.fatia(args.artigo).catalogo + args.sufixo_catalogo
    spark = DatabricksSession.builder.getOrCreate()
    destino = DestinoUC(catalogo, spark=spark)
    api.avancar_tempo(args.artigo, destino, args.dias, spark=spark, roteiro=roteiro)
    print(f"Landing em {destino.volume}.")
