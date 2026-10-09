"""Comportamento de `gerar_fatia` observado pelo que fica gravado no destino local."""

from pathlib import Path

from modelbricks_gerador import gerar_fatia


def _arquivos(pasta: Path) -> list[Path]:
    return sorted(p for p in pasta.rglob("*") if p.is_file())


def test_prepara_landing_e_deixa_bronze_silver_gold_vazios(spark, destino):
    gerar_fatia("01-bronze", destino, spark=spark)

    raiz = destino.raiz
    for schema in ("landing", "bronze", "silver", "gold"):
        assert (raiz / schema).is_dir(), f"schema {schema} não foi criado"
    for schema in ("bronze", "silver", "gold"):
        assert _arquivos(raiz / schema) == [], f"o gerador escreveu em {schema}"
    assert _arquivos(raiz / "landing"), "a landing ficou vazia"


def test_grava_municipios_do_ibge_na_landing_pela_data_simulada(spark, destino):
    gerar_fatia("01-bronze", destino, spark=spark)

    pasta = destino.volume / "ibge" / "municipios" / "2025-12-31"
    csvs = list(pasta.glob("*.csv"))
    assert len(csvs) == 1
    linhas = csvs[0].read_text(encoding="utf-8").splitlines()
    assert linhas[0].startswith("cod_municipio,nome_municipio,cod_uf,sigla_uf")
    assert len(linhas) - 1 == 5571
    assert any(linha.startswith("3550308,São Paulo,35,SP,São Paulo,3,SE,Sudeste") for linha in linhas)
