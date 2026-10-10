"""Fatias 02-historizacao e 03-3fn: o que o leitor encontra depois de rodar `02_setup` e `03_setup`.

Duas partes:

- os recursos do bundle (job de setup de cada artigo), lidos como arquivo: rodam sem Spark e
  sem cota, e garantem que cada job aponta para o próprio artigo e para o próprio catálogo;
- as fatias inteiras, geradas uma vez cada nas gerações compartilhadas `fatia_02` e `fatia_03`
  (escala 0,03, carga inicial) e comparadas entre si. São testes de marco do ticket 14, por
  isso marcados como `lento`.
"""

import json

import geracoes
import pytest
import yaml

from modelbricks_gerador import carregar_roteiro

RAIZ_REPOSITORIO = geracoes.RAIZ_GERADOR.parent
SISTEMAS_02 = {"patio", "embarque", "nucleo", "rotacerta"}
SISTEMAS_03 = {"nucleo", "embarque", "patio", "apolice"}
EM_COMUM = ("nucleo", "embarque", "patio")


def _job_de_setup(artigo: str) -> dict:
    numero = artigo.split("-")[0]
    caminho = RAIZ_REPOSITORIO / artigo / "resources" / f"{numero}_setup.job.yml"
    jobs = yaml.safe_load(caminho.read_text(encoding="utf-8"))["resources"]["jobs"]
    assert list(jobs) == [f"{numero}_setup"], "cada pasta de artigo declara só o próprio job de setup"
    return jobs[f"{numero}_setup"]


@pytest.mark.parametrize("artigo", ["01-bronze", "02-historizacao", "03-3fn"])
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


def test_catalogos_dos_artigos_seguem_a_convencao_de_nomes():
    fatias = carregar_roteiro().fatias

    assert fatias["02-historizacao"].catalogo == "medalhao_02_historizacao"
    assert fatias["03-3fn"].catalogo == "medalhao_03_3fn"
    assert set(fatias["02-historizacao"].sistemas) == SISTEMAS_02
    assert set(fatias["03-3fn"].sistemas) == SISTEMAS_03
    assert not fatias["02-historizacao"].gabarito and fatias["03-3fn"].gabarito


# Fatias inteiras (geradas nas gerações compartilhadas `fatia_02` e `fatia_03`).


def _raizes_da_landing(destino) -> set[str]:
    return {p.name for p in destino.volume.iterdir()}


def _conteudo_do_sistema(destino, sistema: str) -> dict[str, bytes]:
    return geracoes.arquivos(destino, sistema)


@pytest.mark.lento
def test_02_grava_so_os_proprios_sistemas_e_nao_tem_gabarito(fatia_02):
    destino = fatia_02.final

    assert fatia_02.resultados[0].gerou
    assert _raizes_da_landing(destino) == SISTEMAS_02 | {"_controle"}
    assert not (destino.raiz / "gabarito").exists()
    for sistema in SISTEMAS_02:
        assert _conteudo_do_sistema(destino, sistema), f"{sistema} sem arquivos na landing"


@pytest.mark.lento
def test_03_grava_so_os_proprios_sistemas_e_o_gabarito_fica_fora_da_landing(fatia_03):
    destino = fatia_03.final

    assert fatia_03.resultados[0].gerou
    assert _raizes_da_landing(destino) == SISTEMAS_03 | {"_controle"}
    for sistema in SISTEMAS_03:
        assert _conteudo_do_sistema(destino, sistema), f"{sistema} sem arquivos na landing"
    assert not any("gabarito" in p.as_posix() for p in destino.volume.rglob("*"))
    tabelas = geracoes.gabarito(destino)
    assert {"mapa_chaves", "defeitos", "pessoas"} <= set(tabelas)
    assert all(tabelas[nome] for nome in ("mapa_chaves", "pessoas"))
    # O gabarito só descreve o que foi entregue: os sistemas de fora da fatia não aparecem.
    assert {r["sistema"] for r in tabelas["mapa_chaves"]} <= SISTEMAS_03
    assert {r["sistema"] for r in tabelas["defeitos"]} <= SISTEMAS_03


@pytest.mark.lento
def test_o_mesmo_cliente_tem_os_mesmos_dados_nos_artigos_02_e_03(fatia_02, fatia_03):
    for sistema in EM_COMUM:
        no_02 = _conteudo_do_sistema(fatia_02.final, sistema)
        no_03 = _conteudo_do_sistema(fatia_03.final, sistema)
        assert no_02 == no_03, f"{sistema} difere entre o 02 e o 03"


@pytest.mark.lento
def test_cada_artigo_foi_gerado_sozinho_e_o_relogio_e_o_da_carga_inicial(fatia_02, fatia_03):
    # As duas gerações partem de destinos vazios e independentes: nenhum artigo precisa do outro.
    assert fatia_02.final.raiz != fatia_03.final.raiz
    for gerada, artigo in ((fatia_02, "02-historizacao"), (fatia_03, "03-3fn")):
        assert gerada.resultados[0].data_relogio == geracoes.CARGA_INICIAL
        relogios = sorted((gerada.final.volume / "_controle" / "relogio").glob("*.json"))
        assert [p.stem for p in relogios] == [geracoes.CARGA_INICIAL.isoformat()]
        assert json.loads(relogios[0].read_text(encoding="utf-8")), artigo
