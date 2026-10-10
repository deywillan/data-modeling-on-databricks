"""Pontos de entrada dos jobs do bundle (`python_wheel_task`).

Os parâmetros do job chegam como texto, por exemplo:

    gerar_fatia --artigo 01-bronze --ambiente dev --escala 1 --reset false
    avancar_tempo --artigo 01-bronze --ambiente dev --dias 1

Os catálogos do artigo saem do artigo e do ambiente (`catalogos.nome_catalogo`). O ambiente é a
variável `ambiente` do bundle, que cada target define (`dev` → `dev`, `prod` → `prd`).
"""

import argparse

from . import api
from .catalogos import validar_ambiente
from .destino_uc import DestinoUC
from .roteiro import carregar_roteiro


def _booleano(texto: str) -> bool:
    valor = texto.strip().lower()
    if valor not in ("true", "false"):
        raise argparse.ArgumentTypeError(f"use true ou false (recebido '{texto}')")
    return valor == "true"


def _ambiente(texto: str) -> str:
    try:
        return validar_ambiente(texto)
    except ValueError as erro:
        raise argparse.ArgumentTypeError(str(erro)) from None


def _artigo(texto: str) -> str:
    try:
        carregar_roteiro().fatia(texto)
    except KeyError as erro:
        raise argparse.ArgumentTypeError(erro.args[0]) from None
    return texto


def _acrescentar_comuns(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--artigo", required=True, type=_artigo, help="artigo da fatia, ex.: 01-bronze")
    parser.add_argument(
        "--ambiente", required=True, type=_ambiente,
        help="ambiente dos catálogos (variável 'ambiente' do bundle: dev ou prd)",
    )


def _imprimir_catalogos(destino: DestinoUC) -> None:
    print(f"Catálogos: {', '.join(destino.catalogos.values())}. Landing de cada sistema em "
          f"/Volumes/{destino.catalogos['bronze']}/<sistema>/landing.")


def gerar_fatia(argv: list[str] | None = None) -> None:
    """Job `<NN>_setup`: prepara os catálogos do artigo e grava a carga inicial da fatia."""
    parser = argparse.ArgumentParser(prog="gerar_fatia", description=gerar_fatia.__doc__)
    _acrescentar_comuns(parser)
    parser.add_argument("--escala", type=float, default=1.0, help="multiplicador dos volumes (padrão 1)")
    parser.add_argument("--reset", type=_booleano, default=False, help="true apaga e recria os três catálogos")
    args = parser.parse_args(argv)

    from databricks.connect import DatabricksSession

    roteiro = carregar_roteiro()
    spark = DatabricksSession.builder.getOrCreate()
    destino = DestinoUC(args.artigo, args.ambiente, spark=spark)
    api.gerar_fatia(args.artigo, destino, escala=args.escala, reset=args.reset, spark=spark, roteiro=roteiro)
    _imprimir_catalogos(destino)


def _dias(texto: str) -> int:
    try:
        valor = int(texto)
    except ValueError:
        raise argparse.ArgumentTypeError(f"dias deve ser um número inteiro (recebido '{texto}')") from None
    if valor < 1:
        raise argparse.ArgumentTypeError(f"dias deve ser pelo menos 1 (recebido {valor})")
    return valor


def avancar_tempo(argv: list[str] | None = None) -> None:
    """Job `avancar_tempo`: avança o relógio dos catálogos do artigo e grava os lotes novos."""
    parser = argparse.ArgumentParser(prog="avancar_tempo", description=avancar_tempo.__doc__)
    _acrescentar_comuns(parser)
    parser.add_argument("--dias", type=_dias, default=1, help="dias simulados a avançar (padrão 1)")
    args = parser.parse_args(argv)

    from databricks.connect import DatabricksSession

    roteiro = carregar_roteiro()
    spark = DatabricksSession.builder.getOrCreate()
    destino = DestinoUC(args.artigo, args.ambiente, spark=spark)
    api.avancar_tempo(args.artigo, destino, args.dias, spark=spark, roteiro=roteiro)
    _imprimir_catalogos(destino)
