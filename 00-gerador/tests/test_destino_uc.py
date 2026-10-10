"""`gerar_fatia` gravando no Unity Catalog (`DestinoUC`), conferido por SQL no workspace.

Cada execução da sessão usa um catálogo de teste com sufixo próprio (`_t` + 8 hexadecimais
aleatórios), para que duas execuções simultâneas (ou uma que morreu no meio) nunca disputem o
mesmo catálogo. Ele só é removido no fim, com `try/finally`; nada é apagado antes, porque o
nome é novo. A carga inicial roda uma vez, numa fixture do módulo, e cada teste parte dela:
uma falha num teste não derruba os outros em cascata. O que se testa é onde o destino grava,
não a geração dos sistemas: a fatia 01-bronze é reduzida ao IBGE, o que poupa a geração e o
envio de milhares de arquivos ao Volume. Marcado como `lento` porque cria e apaga um catálogo
no workspace.
"""

import re
import uuid

import geracoes
import pytest

from modelbricks_gerador import ArquivoJaExiste, DestinoUC, gerar_fatia
from modelbricks_gerador.escritor import Arquivo

pytestmark = pytest.mark.lento

SO_IBGE = geracoes.roteiro_so_ibge()
_SUFIXO_VALIDO = re.compile(r"[a-z0-9_]*")  # o mesmo que `cli._sufixo` aceita


@pytest.fixture(scope="session")
def sufixo_catalogo() -> str:
    """Sufixo único desta execução da sessão de testes."""
    sufixo = f"_t{uuid.uuid4().hex[:8]}"
    assert _SUFIXO_VALIDO.fullmatch(sufixo)
    return sufixo


@pytest.fixture(scope="module")
def catalogo(spark, sufixo_catalogo):
    """Catálogo de teste desta execução, apagado só no fim."""
    nome = f"medalhao_01_bronze{sufixo_catalogo}"
    try:
        yield nome
    finally:
        spark.sql(f"DROP CATALOG IF EXISTS {nome} CASCADE")


@pytest.fixture(scope="module")
def destino_uc(spark, catalogo):
    return DestinoUC(catalogo, spark=spark)


@pytest.fixture(scope="module")
def carga_inicial(spark, destino_uc):
    """A carga inicial da fatia, feita uma vez; os testes partem do que ela deixou."""
    return gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, roteiro=SO_IBGE)


def _schemas(spark, catalogo) -> set[str]:
    return {linha[0] for linha in spark.sql(f"SHOW SCHEMAS IN {catalogo}").collect()}


def _tabelas(spark, catalogo, schema: str) -> list[str]:
    return [linha["tableName"] for linha in spark.sql(f"SHOW TABLES IN {catalogo}.{schema}").collect()]


def _arquivos_landing(catalogo) -> list[tuple[str, int, int | None]]:
    """(caminho, tamanho, última modificação) de todos os arquivos do Volume landing.

    Lista pelo SDK, e não por `read_files`, porque o Spark ignora pastas iniciadas por `_`.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    arquivos, pendentes = [], [f"/Volumes/{catalogo}/landing/landing"]
    while pendentes:
        for item in w.files.list_directory_contents(pendentes.pop()):
            if item.is_directory:
                pendentes.append(item.path)
            else:
                arquivos.append((item.path, item.file_size, item.last_modified))
    return sorted(arquivos)


def test_carga_inicial_cria_catalogo_schemas_volume_ibge_e_relogio(spark, catalogo, carga_inicial):
    assert carga_inicial.gerou
    assert {"landing", "bronze", "silver", "gold"} <= _schemas(spark, catalogo)
    volumes = [linha["volume_name"] for linha in spark.sql(f"SHOW VOLUMES IN {catalogo}.landing").collect()]
    assert volumes == ["landing"]
    for schema in ("bronze", "silver", "gold"):
        assert _tabelas(spark, catalogo, schema) == [], f"o gerador escreveu em {schema}"

    caminhos = [arquivo[0] for arquivo in _arquivos_landing(catalogo)]
    assert any(c.endswith("/ibge/municipios/2025-12-31/municipios_20251231_0001.csv") for c in caminhos)
    assert any(c.endswith("/_controle/relogio/2025-12-31.json") for c in caminhos)

    municipios = spark.sql(
        f"SELECT count(*) AS n, count_if(nome_municipio = 'São Paulo') AS sp FROM read_files("
        f"'/Volumes/{catalogo}/landing/landing/ibge/municipios/', format => 'csv', header => true)"
    ).first()
    assert municipios["n"] == 5571
    assert municipios["sp"] == 1


def test_reexecutar_sem_reset_nao_altera_nada(spark, catalogo, destino_uc, carga_inicial):
    antes = _arquivos_landing(catalogo)

    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, roteiro=SO_IBGE)

    assert not resultado.gerou
    assert "31/12/2025" in resultado.mensagem
    assert _arquivos_landing(catalogo) == antes


def test_landing_nunca_sobrescreve(destino_uc, carga_inicial):
    caminho = "ibge/municipios/2025-12-31/municipios_20251231_0001.csv"

    with pytest.raises(ArquivoJaExiste):
        destino_uc.gravar_arquivos([Arquivo(caminho, b"outro conteudo")])

    assert destino_uc.ler(caminho).startswith(b"cod_municipio,nome_municipio")


def test_reset_recria_o_catalogo_do_zero(spark, catalogo, destino_uc, carga_inicial):
    antes = [arquivo[:2] for arquivo in _arquivos_landing(catalogo)]
    spark.sql(f"CREATE TABLE {catalogo}.bronze.sobra_de_execucao_anterior (id INT)")

    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, reset=True, roteiro=SO_IBGE)

    assert resultado.gerou
    assert _tabelas(spark, catalogo, "bronze") == []
    assert [arquivo[:2] for arquivo in _arquivos_landing(catalogo)] == antes
