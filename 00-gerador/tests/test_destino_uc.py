"""`gerar_fatia` gravando no Unity Catalog (`DestinoUC`), conferido por SQL no workspace.

Usa um catálogo de teste próprio, removido no início e no fim da sessão de testes.
"""

import pytest

from modelbricks_gerador import ArquivoJaExiste, DestinoUC, gerar_fatia
from modelbricks_gerador.escritor import Arquivo

CATALOGO = "medalhao_01_bronze_t02"


def _apagar_catalogo(spark):
    spark.sql(f"DROP CATALOG IF EXISTS {CATALOGO} CASCADE")


@pytest.fixture(scope="module")
def destino_uc(spark):
    _apagar_catalogo(spark)
    yield DestinoUC(CATALOGO, spark=spark)
    _apagar_catalogo(spark)


def _schemas(spark) -> set[str]:
    return {linha[0] for linha in spark.sql(f"SHOW SCHEMAS IN {CATALOGO}").collect()}


def _tabelas(spark, schema: str) -> list[str]:
    return [linha["tableName"] for linha in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect()]


def _arquivos_landing() -> list[tuple[str, int, int | None]]:
    """(caminho, tamanho, última modificação) de todos os arquivos do Volume landing.

    Lista pelo SDK, e não por `read_files`, porque o Spark ignora pastas iniciadas por `_`.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    arquivos, pendentes = [], [f"/Volumes/{CATALOGO}/landing/landing"]
    while pendentes:
        for item in w.files.list_directory_contents(pendentes.pop()):
            if item.is_directory:
                pendentes.append(item.path)
            else:
                arquivos.append((item.path, item.file_size, item.last_modified))
    return sorted(arquivos)


def test_carga_inicial_cria_catalogo_schemas_volume_ibge_e_relogio(spark, destino_uc):
    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01)

    assert resultado.gerou
    assert {"landing", "bronze", "silver", "gold"} <= _schemas(spark)
    volumes = [linha["volume_name"] for linha in spark.sql(f"SHOW VOLUMES IN {CATALOGO}.landing").collect()]
    assert volumes == ["landing"]
    for schema in ("bronze", "silver", "gold"):
        assert _tabelas(spark, schema) == [], f"o gerador escreveu em {schema}"

    caminhos = [arquivo[0] for arquivo in _arquivos_landing()]
    assert any(c.endswith("/ibge/municipios/2025-12-31/municipios_20251231_0001.csv") for c in caminhos)
    assert any(c.endswith("/_controle/relogio/2025-12-31.json") for c in caminhos)

    municipios = spark.sql(
        f"SELECT count(*) AS n, count_if(nome_municipio = 'São Paulo') AS sp FROM read_files("
        f"'/Volumes/{CATALOGO}/landing/landing/ibge/municipios/', format => 'csv', header => true)"
    ).first()
    assert municipios["n"] == 5571
    assert municipios["sp"] == 1


def test_reexecutar_sem_reset_nao_altera_nada(spark, destino_uc):
    antes = _arquivos_landing()

    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01)

    assert not resultado.gerou
    assert "31/12/2025" in resultado.mensagem
    assert _arquivos_landing() == antes


def test_landing_nunca_sobrescreve(destino_uc):
    caminho = "ibge/municipios/2025-12-31/municipios_20251231_0001.csv"

    with pytest.raises(ArquivoJaExiste):
        destino_uc.gravar_arquivos([Arquivo(caminho, b"outro conteudo")])

    assert destino_uc.ler(caminho).startswith(b"cod_municipio,nome_municipio")


def test_reset_recria_o_catalogo_do_zero(spark, destino_uc):
    antes = [arquivo[:2] for arquivo in _arquivos_landing()]
    spark.sql(f"CREATE TABLE {CATALOGO}.bronze.sobra_de_execucao_anterior (id INT)")

    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, reset=True)

    assert resultado.gerou
    assert _tabelas(spark, "bronze") == []
    assert [arquivo[:2] for arquivo in _arquivos_landing()] == antes
