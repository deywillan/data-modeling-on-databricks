"""`gerar_fatia` gravando no Unity Catalog (`DestinoUC`), conferido por SQL no workspace.

Cada execução da sessão usa um ambiente de teste próprio (`t` + 8 hexadecimais aleatórios, o que
dá catálogos como `a01_bronze_t3f9a2c1b`), para que duas execuções simultâneas (ou uma que
morreu no meio) nunca disputem os mesmos catálogos. Eles só são removidos no fim, com
`try/finally`; nada é apagado antes, porque o nome é novo. A carga inicial roda uma vez, numa
fixture do módulo, e cada teste parte dela: uma falha num teste não derruba os outros em cascata.
O que se testa é onde o destino grava, não a geração dos sistemas: a fatia 01-bronze é reduzida
ao IBGE, o que poupa a geração e o envio de milhares de arquivos ao Volume. Marcado como `lento`
porque cria e apaga catálogos no workspace.
"""

import uuid

import geracoes
import pytest

from modelbricks_gerador import ArquivoJaExiste, DestinoUC, catalogos_do_artigo, gerar_fatia
from modelbricks_gerador.catalogos import validar_ambiente
from modelbricks_gerador.escritor import Arquivo

pytestmark = pytest.mark.lento

SO_IBGE = geracoes.roteiro_so_ibge()


@pytest.fixture(scope="session")
def ambiente_de_teste() -> str:
    """Ambiente único desta execução da sessão de testes."""
    return validar_ambiente(f"t{uuid.uuid4().hex[:8]}")


@pytest.fixture(scope="module")
def catalogos(spark, ambiente_de_teste):
    """Os três catálogos de teste desta execução, apagados só no fim."""
    nomes = catalogos_do_artigo("01-bronze", ambiente_de_teste)
    try:
        yield nomes
    finally:
        for nome in nomes.values():
            spark.sql(f"DROP CATALOG IF EXISTS {nome} CASCADE")


@pytest.fixture(scope="module")
def destino_uc(spark, ambiente_de_teste, catalogos):
    return DestinoUC("01-bronze", ambiente_de_teste, spark=spark)


@pytest.fixture(scope="module")
def carga_inicial(spark, destino_uc):
    """A carga inicial da fatia, feita uma vez; os testes partem do que ela deixou."""
    return gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, roteiro=SO_IBGE)


def _schemas(spark, catalogo) -> set[str]:
    return {linha[0] for linha in spark.sql(f"SHOW SCHEMAS IN {catalogo}").collect()} - {"default", "information_schema"}


def _tabelas(spark, catalogo, schema: str) -> list[str]:
    return [linha["tableName"] for linha in spark.sql(f"SHOW TABLES IN {catalogo}.{schema}").collect()]


def _arquivos_landing(destino_uc) -> list[tuple[str, int, int | None]]:
    """(caminho, tamanho, última modificação) de todos os arquivos dos Volumes do Bronze.

    Lista pelo SDK, e não por `read_files`, porque o Spark ignora pastas iniciadas por `_`.
    """
    arquivos = []
    for schema in sorted(_schemas(destino_uc.spark, destino_uc.catalogos["bronze"])):
        pendentes = [destino_uc.raiz_do_volume(schema, "relogio" if schema == "_controle" else "landing")]
        while pendentes:
            for item in destino_uc.workspace.files.list_directory_contents(pendentes.pop()):
                if item.is_directory:
                    pendentes.append(item.path)
                else:
                    arquivos.append((item.path, item.file_size, item.last_modified))
    return sorted(arquivos)


def test_carga_inicial_cria_tres_catalogos_schema_por_sistema_ibge_e_relogio(spark, catalogos, destino_uc, carga_inicial):
    assert carga_inicial.gerou
    assert _schemas(spark, catalogos["bronze"]) == {"ibge", "_controle"}
    for schema, volume in (("ibge", "landing"), ("_controle", "relogio")):
        volumes = [linha["volume_name"] for linha in spark.sql(f"SHOW VOLUMES IN {catalogos['bronze']}.{schema}").collect()]
        assert volumes == [volume]
    assert _schemas(spark, catalogos["silver"]) == set(), "Silver vazio (a fatia 01 não tem gabarito)"
    assert _schemas(spark, catalogos["gold"]) == set()

    caminhos = [arquivo[0] for arquivo in _arquivos_landing(destino_uc)]
    bronze = catalogos["bronze"]
    assert f"/Volumes/{bronze}/ibge/landing/municipios/2025-12-31/municipios_20251231_0001.csv" in caminhos
    assert f"/Volumes/{bronze}/_controle/relogio/2025-12-31.json" in caminhos

    municipios = spark.sql(
        f"SELECT count(*) AS n, count_if(nome_municipio = 'São Paulo') AS sp FROM read_files("
        f"'/Volumes/{bronze}/ibge/landing/municipios/', format => 'csv', header => true)"
    ).first()
    assert municipios["n"] == 5571
    assert municipios["sp"] == 1


def test_reexecutar_sem_reset_nao_altera_nada(spark, destino_uc, carga_inicial):
    antes = _arquivos_landing(destino_uc)

    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, roteiro=SO_IBGE)

    assert not resultado.gerou
    assert "31/12/2025" in resultado.mensagem
    assert _arquivos_landing(destino_uc) == antes


def test_landing_nunca_sobrescreve(destino_uc, carga_inicial):
    caminho = "ibge/municipios/2025-12-31/municipios_20251231_0001.csv"

    with pytest.raises(ArquivoJaExiste):
        destino_uc.gravar_arquivos([Arquivo(caminho, b"outro conteudo")])

    assert destino_uc.ler(caminho).startswith(b"cod_municipio,nome_municipio")


def test_arquivo_ja_gravado_com_os_mesmos_bytes_conta_como_gravado(destino_uc, carga_inicial):
    caminho = "ibge/municipios/2025-12-31/municipios_20251231_0001.csv"
    conteudo = destino_uc.ler(caminho)

    destino_uc.gravar_arquivos([Arquivo(caminho, conteudo)])  # nova tentativa de uma chamada que caiu

    assert destino_uc.ler(caminho) == conteudo


def test_listar_devolve_os_caminhos_do_contrato(destino_uc, carga_inicial):
    assert destino_uc.listar("") == [
        "_controle/relogio/2025-12-31.json",
        "ibge/municipios/2025-12-31/municipios_20251231_0001.csv",
    ]
    assert destino_uc.listar("_controle/relogio") == ["_controle/relogio/2025-12-31.json"]
    assert destino_uc.listar("rastro") == []


def test_apagar_do_gabarito_remove_so_as_linhas_a_partir_da_data(spark, destino_uc, carga_inicial):
    from datetime import date

    destino_uc.preparar(["ibge"], gabarito=True)
    linhas = [(date(2026, 1, 1), "antes"), (date(2026, 1, 2), "no_dia"), (date(2026, 1, 3), "depois"), (None, "sem_data")]
    destino_uc.gravar_gabarito("defeitos_de_teste", spark.createDataFrame(linhas, "data_entrega date, rotulo string"))

    destino_uc.apagar_do_gabarito("defeitos_de_teste", "data_entrega", date(2026, 1, 2))
    destino_uc.apagar_do_gabarito("tabela_que_nao_existe", "data_entrega", date(2026, 1, 2))  # não faz nada

    restantes = spark.table(f"{destino_uc.catalogos['silver']}.gabarito.defeitos_de_teste").collect()
    assert sorted(r["rotulo"] for r in restantes) == ["antes", "sem_data"]


def test_reset_recria_os_tres_catalogos_do_zero(spark, catalogos, destino_uc, carga_inicial):
    antes = [arquivo[:2] for arquivo in _arquivos_landing(destino_uc)]
    spark.sql(f"CREATE TABLE {catalogos['bronze']}.ibge.sobra_de_execucao_anterior (id INT)")
    spark.sql(f"CREATE TABLE {catalogos['gold']}.default.sobra_de_execucao_anterior (id INT)")

    resultado = gerar_fatia("01-bronze", destino_uc, spark=spark, escala=0.01, reset=True, roteiro=SO_IBGE)

    assert resultado.gerou
    assert _tabelas(spark, catalogos["bronze"], "ibge") == []
    assert _tabelas(spark, catalogos["gold"], "default") == []
    assert [arquivo[:2] for arquivo in _arquivos_landing(destino_uc)] == antes
