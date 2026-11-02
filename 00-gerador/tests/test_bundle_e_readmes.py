"""Recursos do bundle e READMEs, lidos como arquivo: rodam sem Spark e sem cota.

A série constrói um único lakehouse, em ordem. Estes testes garantem que o bundle declara um
só setup (o `00_setup`, no gerador), que nenhum artigo tem setup próprio, e que o README de
cada artigo avisa que a construção é incremental, diz o que rodar antes e usa os catálogos da
série, sem os nomes antigos por artigo (`aNN_*`) e sem dados do workspace do autor.
"""

import re

import geracoes
import pytest
import yaml

from modelbricks_gerador import carregar_roteiro

RAIZ_REPOSITORIO = geracoes.RAIZ_GERADOR.parent
ARTIGOS = sorted(p.name for p in RAIZ_REPOSITORIO.iterdir() if re.fullmatch(r"\d{2}-[a-z0-9-]+", p.name) and p.name != "00-gerador")


def _jobs(caminho) -> dict:
    return yaml.safe_load(caminho.read_text(encoding="utf-8"))["resources"]["jobs"]


def _readme(artigo: str) -> str:
    return (RAIZ_REPOSITORIO / artigo / "README.md").read_text(encoding="utf-8")


def test_a_serie_tem_os_dez_artigos():
    assert [a[:2] for a in ARTIGOS] == [f"{n:02d}" for n in range(1, 11)]


def test_o_00_setup_chama_o_gerador_para_a_serie_e_roda_em_serverless():
    jobs = _jobs(geracoes.RAIZ_GERADOR / "resources" / "00_setup.job.yml")

    assert list(jobs) == ["00_setup"]
    job = jobs["00_setup"]
    (tarefa,) = job["tasks"]
    assert tarefa["python_wheel_task"]["entry_point"] == "gerar_carga_inicial"
    parametros = tarefa["python_wheel_task"]["parameters"]
    assert "--ambiente=${var.ambiente}" in parametros
    assert not [p for p in parametros if p.startswith("--artigo")], "o setup não depende do artigo"
    assert "job_cluster_key" not in tarefa and "new_cluster" not in job, "só compute serverless"
    assert {p["name"]: p["default"] for p in job["parameters"]} == {"escala": "1", "reset": "false"}


def test_o_avancar_tempo_avanca_a_serie_inteira():
    job = _jobs(geracoes.RAIZ_GERADOR / "resources" / "avancar_tempo.job.yml")["avancar_tempo"]

    assert {p["name"]: p["default"] for p in job["parameters"]} == {"dias": "1"}
    (tarefa,) = job["tasks"]
    assert tarefa["python_wheel_task"]["entry_point"] == "avancar_tempo"
    assert not [p for p in tarefa["python_wheel_task"]["parameters"] if p.startswith("--artigo")]


def test_os_pontos_de_entrada_dos_jobs_existem_no_pacote():
    texto = (geracoes.RAIZ_GERADOR / "pyproject.toml").read_text(encoding="utf-8")

    assert 'gerar_carga_inicial = "modelbricks_gerador.cli:gerar_carga_inicial"' in texto
    assert 'avancar_tempo = "modelbricks_gerador.cli:avancar_tempo"' in texto


@pytest.mark.parametrize("artigo", ARTIGOS)
def test_nenhum_artigo_declara_setup_proprio(artigo):
    recursos = RAIZ_REPOSITORIO / artigo / "resources"
    jobs = [nome for arquivo in recursos.glob("*.yml") for nome in _jobs(arquivo) or {}] if recursos.is_dir() else []

    assert not [j for j in jobs if j.endswith("_setup")], "os dados vêm só do 00_setup"


@pytest.mark.parametrize("artigo", ARTIGOS)
def test_readme_avisa_a_ordem_incremental_e_o_que_rodar_antes(artigo):
    numero = int(artigo[:2])
    texto = _readme(artigo)

    assert "Este artigo segue uma ordem incremental de construção." in texto
    if numero == 1:
        assert "parte da landing zone pronta, construída no artigo 00" in texto
        assert "camada Bronze **inteira**" in texto
        assert "databricks bundle run 00_setup -p <seu-perfil>\n" not in texto, "o 01 não constrói a landing"
    else:
        assert "construa a landing zone com o `00_setup` (artigo 00)" in texto
        assert "pipeline do artigo 01" in texto or "pipelines dos artigos 01 " in texto
        assert "não funciona sozinho" in texto


@pytest.mark.parametrize("artigo", ARTIGOS)
def test_readme_usa_os_catalogos_da_serie_e_os_jobs_atuais(artigo):
    texto = _readme(artigo)

    assert re.search(r"`(bronze|silver|gold)_dev`", texto), "o README cita os catálogos da série"
    assert not re.search(r"\ba\d{2}_(bronze|silver|gold)_", texto), "nomes antigos de catálogo por artigo"
    assert not re.search(r"\b(0[1-9]|10)_setup\b", texto), "os artigos não têm setup próprio"
    assert "avancar_tempo -p <seu-perfil> --params dias=1" in texto
    assert "artigo=" not in texto
    assert "`_prd`" in texto or "bronze_prd" in texto
    assert "--var ambiente=" not in texto, "o sufixo do ambiente vem do target do bundle"
    # Repositório público: nada de perfil, host ou credencial do autor.
    assert not re.search(r"databricks-free-edition|dbw-|adb-\d|dapi[0-9a-f]{8}|https://[\w.-]+\.cloud\.databricks\.com", texto)


def test_o_readme_da_bronze_lista_todos_os_sistemas():
    texto = _readme("01-bronze")

    for sistema in carregar_roteiro().carga.sistemas:
        assert f"| `{sistema}` |" in texto, f"{sistema} sem linha na tabela de sistemas da Bronze"


def test_o_readme_do_gerador_explica_como_gerar_os_dados():
    texto = (geracoes.RAIZ_GERADOR / "README.md").read_text(encoding="utf-8")

    assert "## Como gerar os dados" in texto
    assert "### 4. Construir a landing zone" in texto
    assert "conteúdo prático do artigo 00" in texto
    assert "databricks bundle run 00_setup -p <seu-perfil>" in texto
    assert "databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1" in texto
    assert not re.search(r"\ba\d{2}_(bronze|silver|gold)_", texto)
    assert not re.search(r"\b(0[1-9]|10)_setup\b", texto)


def test_o_readme_raiz_indexa_os_artigos_e_orienta_um_por_vez():
    texto = (RAIZ_REPOSITORIO / "README.md").read_text(encoding="utf-8")

    for artigo in ["00-gerador", *ARTIGOS]:
        assert f"]({artigo}/README.md)" in texto, f"{artigo} sem link no README raiz"
    assert "One active pipeline" not in texto and "um pipeline ativo por tipo" in texto
    assert "databricks bundle run 00_setup -p <seu-perfil>" in texto
    assert "DROP CATALOG bronze_dev CASCADE;" in texto
    assert not re.search(r"\ba\d{2}_(bronze|silver|gold)_", texto)
    assert not re.search(r"databricks-free-edition|dbw-|adb-\d|dapi[0-9a-f]{8}|https://[\w.-]+\.cloud\.databricks\.com", texto)
