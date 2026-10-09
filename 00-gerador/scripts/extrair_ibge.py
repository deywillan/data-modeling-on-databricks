"""Extrai municípios, UFs e regiões da API de Localidades do IBGE para o CSV versionado.

Script avulso, fora do runtime do gerador: o setup nunca baixa nada da internet.
Fonte: IBGE, API de Localidades (https://servicodados.ibge.gov.br/api/v1/localidades/municipios).

Uso:
    python scripts/extrair_ibge.py                    # baixa da API
    python scripts/extrair_ibge.py --entrada m.json   # usa um JSON já baixado

UF e região são derivadas pelo caminho região imediata → intermediária → UF → região,
que é completo para todos os municípios (a microrregião vem nula em municípios novos).
"""

import argparse
import csv
import json
import urllib.request
from pathlib import Path

URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
SAIDA = Path(__file__).resolve().parents[1] / "src" / "modelbricks_gerador" / "dados" / "ibge_municipios.csv"
CABECALHO = [
    "cod_municipio", "nome_municipio", "cod_uf", "sigla_uf", "nome_uf",
    "cod_regiao", "sigla_regiao", "nome_regiao",
    "cod_regiao_imediata", "nome_regiao_imediata",
    "cod_regiao_intermediaria", "nome_regiao_intermediaria",
]


def _linha(m: dict) -> list:
    imediata = m["regiao-imediata"]
    intermediaria = imediata["regiao-intermediaria"]
    uf = intermediaria["UF"]
    regiao = uf["regiao"]
    return [
        m["id"], m["nome"], uf["id"], uf["sigla"], uf["nome"],
        regiao["id"], regiao["sigla"], regiao["nome"],
        imediata["id"], imediata["nome"],
        intermediaria["id"], intermediaria["nome"],
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entrada", type=Path, help="JSON da API já baixado")
    args = parser.parse_args()
    if args.entrada:
        municipios = json.loads(args.entrada.read_text(encoding="utf-8"))
    else:
        with urllib.request.urlopen(URL) as resposta:
            municipios = json.load(resposta)
    linhas = sorted((_linha(m) for m in municipios), key=lambda linha: linha[0])
    SAIDA.parent.mkdir(parents=True, exist_ok=True)
    with SAIDA.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo, lineterminator="\n")
        escritor.writerow(CABECALHO)
        escritor.writerows(linhas)
    print(f"{len(linhas)} municípios gravados em {SAIDA}")


if __name__ == "__main__":
    main()
