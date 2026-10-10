"""Fatias 04-ods a 10-data-marts: o que o leitor encontra depois de rodar `04_setup` a `10_setup`.

Duas partes:

- o registro de fatias e os recursos do bundle (job de setup e README de cada artigo), lidos
  como arquivo: rodam sem Spark e sem cota, e garantem que cada artigo aponta para o próprio
  catálogo, grava só os próprios sistemas e tem gabarito só quando a fatia o declara;
- as fatias inteiras 05 (que a 06 repete) e 10, geradas uma vez cada nas gerações
  compartilhadas `fatia_05` e `fatia_10` (escala 0,03, carga inicial). A 10 tem todos os
  sistemas, então é nela que se confere a convergência de cliente, veículo e geografia entre
  as divisões. São testes de marco do ticket 16, por isso marcados como `lento`.
"""

import csv
import io
import itertools
import re

import geracoes
import pytest
import yaml

from modelbricks_gerador import carregar_roteiro

RAIZ_REPOSITORIO = geracoes.RAIZ_GERADOR.parent

# Registro artigo → (catálogo, sistemas, gabarito), conforme a spec.
FATIAS = {
    "04-ods": ("medalhao_04_ods", {"embarque", "passagemhub", "rotacerta"}, False),
    "05-raw-vault": ("medalhao_05_raw_vault", {"embarque", "passagemhub", "solticket", "solfin"}, True),
    "06-business-vault": ("medalhao_06_business_vault", {"embarque", "passagemhub", "solticket", "solfin"}, True),
    "07-star-schema": ("medalhao_07_star_schema", {"embarque", "passagemhub"}, False),
    "08-snowflake": ("medalhao_08_snowflake", {"patio", "ibge"}, False),
    "09-obt": ("medalhao_09_obt", {"rotacerta", "rastro"}, False),
    "10-data-marts": (
        "medalhao_10_data_marts",
        {"nucleo", "rastro", "embarque", "passagemhub", "rotacerta", "patio", "apolice", "solticket", "solfin", "ibge"},
        True,
    ),
}
TODOS_OS_SISTEMAS = FATIAS["10-data-marts"][1]

# Divisão (empresa) dona de cada sistema, como o gabarito a chama.
DIVISAO = {
    "embarque": "mb_viacao", "passagemhub": "mb_viacao", "rotacerta": "mb_cargas", "patio": "mb_veiculos",
    "apolice": "mb_seguros", "solticket": "evs", "solfin": "evs", "nucleo": "mb", "rastro": "mb",
}


def _job_de_setup(artigo: str) -> dict:
    numero = artigo.split("-")[0]
    caminho = RAIZ_REPOSITORIO / artigo / "resources" / f"{numero}_setup.job.yml"
    jobs = yaml.safe_load(caminho.read_text(encoding="utf-8"))["resources"]["jobs"]
    assert list(jobs) == [f"{numero}_setup"], "cada pasta de artigo declara só o próprio job de setup"
    return jobs[f"{numero}_setup"]


@pytest.mark.parametrize("artigo", FATIAS)
def test_job_de_setup_chama_o_gerador_para_o_proprio_artigo_e_roda_em_serverless(artigo):
    job = _job_de_setup(artigo)

    assert job["name"] == f"{artigo.split('-')[0]}_setup"
    (tarefa,) = job["tasks"]
    assert tarefa["python_wheel_task"]["entry_point"] == "gerar_fatia"
    parametros = tarefa["python_wheel_task"]["parameters"]
    assert f"--artigo={artigo}" in parametros
    assert "--sufixo-catalogo=${var.sufixo_catalogo}" in parametros
    assert "job_cluster_key" not in tarefa and "new_cluster" not in job, "só compute serverless"
    assert {p["name"]: p["default"] for p in job["parameters"]} == {"escala": "1", "reset": "false"}


@pytest.mark.parametrize("artigo", FATIAS)
def test_cada_fatia_tem_o_proprio_catalogo_so_os_proprios_sistemas_e_gabarito_so_quando_declarado(artigo):
    catalogo, sistemas, gabarito = FATIAS[artigo]
    fatia = carregar_roteiro().fatia(artigo)

    assert fatia.catalogo == catalogo
    assert set(fatia.sistemas) == sistemas
    assert fatia.gabarito == gabarito


def test_a_fatia_10_e_a_uniao_de_todas_as_outras_e_nenhum_catalogo_se_repete():
    fatias = carregar_roteiro().fatias

    assert set(fatias["10-data-marts"].sistemas) == {s for f in fatias.values() for s in f.sistemas}
    catalogos = [f.catalogo for f in fatias.values()]
    assert len(set(catalogos)) == len(catalogos), "um catálogo por artigo"


@pytest.mark.parametrize("artigo", FATIAS)
def test_readme_do_artigo_explica_o_setup_o_catalogo_os_sistemas_e_o_gabarito(artigo):
    catalogo, sistemas, gabarito = FATIAS[artigo]
    numero = artigo.split("-")[0]
    texto = (RAIZ_REPOSITORIO / artigo / "README.md").read_text(encoding="utf-8")

    assert f"databricks bundle run {numero}_setup" in texto
    assert f"avancar_tempo -p <seu-perfil> --params artigo={artigo},dias=1" in texto
    assert f"DROP CATALOG {catalogo} CASCADE" in texto
    assert f"a{numero}_landing" in texto, "a alternativa de catálogo único está documentada"
    for sistema in sistemas:
        assert f"| `{sistema}/` |" in texto, f"{sistema} sem linha na tabela de sistemas"
    outros = {f"| `{s}/` |" for s in TODOS_OS_SISTEMAS - sistemas}
    assert not [o for o in outros if o in texto], "o README lista só os sistemas do próprio artigo"
    assert (f"`{catalogo}.gabarito`" in texto) == gabarito
    assert ("Este artigo não usa o schema `gabarito`." in texto) == (not gabarito)
    # Repositório público: nada de perfil, host ou credencial do autor.
    assert not re.search(r"databricks-free-edition|dbw-|adb-\d|dapi[0-9a-f]{8}|https://[\w.-]+\.cloud\.databricks\.com", texto)


# Fatias inteiras (geradas nas gerações compartilhadas `fatia_05` e `fatia_10`).


def _raizes_da_landing(destino) -> set[str]:
    return {p.name for p in destino.volume.iterdir()}


def _tabelas_do_gabarito(destino) -> dict[str, list[dict]]:
    return geracoes.gabarito(destino)


@pytest.mark.lento
def test_05_grava_so_os_sistemas_da_fatia_e_o_gabarito_fica_fora_da_landing(fatia_05):
    destino = fatia_05.final
    sistemas = FATIAS["05-raw-vault"][1]

    assert fatia_05.resultados[0].gerou
    assert _raizes_da_landing(destino) == sistemas | {"_controle"}
    for sistema in sistemas:
        assert geracoes.arquivos(destino, sistema), f"{sistema} sem arquivos na landing"
    assert not any("gabarito" in p.as_posix() for p in destino.volume.rglob("*"))
    tabelas = _tabelas_do_gabarito(destino)
    assert {"mapa_chaves", "defeitos", "pessoas"} <= set(tabelas)
    # O gabarito só descreve o que foi entregue: os outros sistemas não aparecem.
    assert {r["sistema"] for r in tabelas["mapa_chaves"]} <= sistemas
    assert {r["sistema"] for r in tabelas["defeitos"]} <= sistemas


@pytest.mark.lento
def test_10_grava_todos_os_sistemas_e_o_gabarito_cobre_todos_os_que_tem_chaves(fatia_10):
    destino = fatia_10.final

    assert fatia_10.resultados[0].gerou
    assert _raizes_da_landing(destino) == TODOS_OS_SISTEMAS | {"_controle"}
    for sistema in TODOS_OS_SISTEMAS:
        assert geracoes.arquivos(destino, sistema), f"{sistema} sem arquivos na landing"
    assert not any("gabarito" in p.as_posix() for p in destino.volume.rglob("*"))
    tabelas = _tabelas_do_gabarito(destino)
    assert {r["sistema"] for r in tabelas["mapa_chaves"]} <= TODOS_OS_SISTEMAS
    # O SolFin e o IBGE não têm chave de entidade do mundo verdadeiro no mapa; os demais têm.
    assert {r["sistema"] for r in tabelas["mapa_chaves"]} >= TODOS_OS_SISTEMAS - {"solfin", "ibge"}


@pytest.mark.lento
def test_o_mesmo_cliente_tem_os_mesmos_dados_nos_artigos_05_e_10(fatia_05, fatia_10):
    for sistema in FATIAS["05-raw-vault"][1]:
        assert geracoes.arquivos(fatia_05.final, sistema) == geracoes.arquivos(fatia_10.final, sistema), (
            f"{sistema} difere entre o 05 e o 10"
        )


def _ids_por_sistema(mapa_chaves: list[dict], tabela: str) -> dict[str, set]:
    ids: dict[str, set] = {}
    for r in mapa_chaves:
        if r["tabela_verdadeira"] == tabela:
            ids.setdefault(r["sistema"], set()).add(r["id_verdadeiro"])
    return ids


def _ids_por_divisao(mapa_chaves: list[dict], tabela: str) -> dict[str, set]:
    por_divisao: dict[str, set] = {}
    for sistema, ids in _ids_por_sistema(mapa_chaves, tabela).items():
        por_divisao.setdefault(DIVISAO.get(sistema, sistema), set()).update(ids)
    return por_divisao


@pytest.mark.lento
@pytest.mark.parametrize(
    "tabela, divisoes",
    [
        ("pessoas", {"mb_viacao", "mb_veiculos", "mb_seguros", "evs"}),  # a mesma pessoa compra passagem e seguro
        ("empresas_pj", {"mb_cargas", "mb_veiculos", "mb_seguros", "mb"}),  # frete, caminhão, seguro, fornecedor
        ("veiculos", {"mb_veiculos", "mb_cargas", "mb"}),  # DMS, TMS, telemetria e ERP como ativo
    ],
)
def test_cliente_e_veiculo_convergem_entre_todas_as_divisoes_da_fatia_10(fatia_10, tabela, divisoes):
    mapa = _tabelas_do_gabarito(fatia_10.final)["mapa_chaves"]
    por_divisao = _ids_por_divisao(mapa, tabela)

    assert set(por_divisao) >= divisoes
    for a, b in itertools.combinations(sorted(divisoes), 2):
        assert por_divisao[a] & por_divisao[b], f"nenhum registro de {tabela} em comum entre {a} e {b}"


@pytest.mark.lento
def test_veiculos_da_frota_reaparecem_na_telemetria_e_no_erp_como_ativo(fatia_10):
    tabelas = _tabelas_do_gabarito(fatia_10.final)
    por_sistema = _ids_por_sistema(tabelas["mapa_chaves"], "veiculos")
    frota = {v["id_veiculo"] for v in tabelas["veiculos"] if v["frota_propria"]}

    assert frota
    assert frota <= por_sistema["patio"] and frota <= por_sistema["rastro"] and frota <= por_sistema["nucleo"]


@pytest.mark.lento
def test_geografia_toda_localizacao_das_divisoes_existe_nos_municipios_do_ibge(fatia_10):
    destino = fatia_10.final
    pasta = destino.volume / "ibge" / "municipios" / geracoes.CARGA_INICIAL.isoformat()
    codigos = {
        int(linha["cod_municipio"])
        for arquivo in pasta.glob("*.csv")
        for linha in csv.DictReader(io.StringIO(arquivo.read_text(encoding="utf-8")))
    }
    tabelas = _tabelas_do_gabarito(destino)
    usados = {
        "pessoas": {r["cod_municipio"] for r in tabelas["pessoas"]},
        "empresas_pj": {r["cod_municipio"] for r in tabelas["empresas_pj"]},
        "rotas": {r[c] for r in tabelas["rotas"] for c in ("cod_municipio_origem", "cod_municipio_destino")},
        "bases": {r["cod_municipio"] for r in tabelas["bases"]},
        "lojas": {r["cod_municipio"] for r in tabelas["lojas"]},
    }

    assert len(codigos) == len(tabelas["municipios"]) > 5000
    for origem, municipios in usados.items():
        assert municipios, f"{origem} sem localização"
        assert municipios <= codigos, f"{origem} referencia município que não está no IBGE"
