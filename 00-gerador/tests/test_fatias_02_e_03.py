"""Fatias 02-historizacao e 03-3fn: o que o leitor encontra depois de rodar `02_setup` e `03_setup`.

Duas partes:

- os recursos do bundle (job de setup de cada artigo), lidos como arquivo: rodam sem Spark e
  sem cota, e garantem que cada job aponta para o próprio artigo e para o próprio catálogo;
- as fatias inteiras, geradas uma vez cada nas gerações compartilhadas `fatia_02` e `fatia_03`
  (escala 0,03, carga inicial) e comparadas entre si. São testes de marco do ticket 14, por
  isso marcados como `lento`.
"""

import csv
import io
import json

import geracoes
import pyarrow.parquet as pq
import pytest
import yaml

from modelbricks_gerador import carregar_roteiro, catalogos_do_artigo

RAIZ_REPOSITORIO = geracoes.RAIZ_GERADOR.parent
SISTEMAS_02 = {"patio", "embarque", "nucleo", "rotacerta"}
SISTEMAS_03 = {"nucleo", "embarque", "patio", "apolice"}


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
    assert "--ambiente=${var.ambiente}" in parametros
    assert "job_cluster_key" not in tarefa and "new_cluster" not in job, "só compute serverless"
    assert {p["name"]: p["default"] for p in job["parameters"]} == {"escala": "1", "reset": "false"}


@pytest.mark.parametrize(("artigo", "gabarito"), [("01-bronze", False), ("02-historizacao", False), ("03-3fn", True)])
def test_readme_diz_se_os_arquivos_saem_com_ou_sem_defeitos(artigo, gabarito):
    texto = (RAIZ_REPOSITORIO / artigo / "README.md").read_text(encoding="utf-8")

    assert carregar_roteiro().fatia(artigo).gabarito == gabarito
    assert ("saem sem defeitos" in texto) == (not gabarito)


def test_catalogos_dos_artigos_seguem_a_convencao_de_nomes_e_as_fatias_o_roteiro():
    fatias = carregar_roteiro().fatias

    assert catalogos_do_artigo("02-historizacao", "dev") == {
        "bronze": "a02_bronze_dev", "silver": "a02_silver_dev", "gold": "a02_gold_dev",
    }
    assert catalogos_do_artigo("03-3fn", "prd")["silver"] == "a03_silver_prd"
    assert set(fatias["02-historizacao"].sistemas) == SISTEMAS_02
    assert set(fatias["03-3fn"].sistemas) == SISTEMAS_03
    assert not fatias["02-historizacao"].gabarito and fatias["03-3fn"].gabarito


# Fatias inteiras (geradas nas gerações compartilhadas `fatia_02` e `fatia_03`).


def _raizes_da_landing(destino) -> set[str]:
    return {p.name for p in destino.pasta("bronze").iterdir()}


def _conteudo_do_sistema(destino, sistema: str) -> dict[str, bytes]:
    return geracoes.arquivos(destino, sistema)


@pytest.mark.lento
def test_02_grava_so_os_proprios_sistemas_e_nao_tem_gabarito(fatia_02):
    destino = fatia_02.final

    assert fatia_02.resultados[0].gerou
    assert _raizes_da_landing(destino) == SISTEMAS_02 | {"_controle"}
    assert not destino.pasta_gabarito().exists()
    for sistema in SISTEMAS_02:
        assert _conteudo_do_sistema(destino, sistema), f"{sistema} sem arquivos na landing"


@pytest.mark.lento
def test_03_grava_so_os_proprios_sistemas_e_o_gabarito_fica_fora_da_landing(fatia_03):
    destino = fatia_03.final

    assert fatia_03.resultados[0].gerou
    assert _raizes_da_landing(destino) == SISTEMAS_03 | {"_controle"}
    for sistema in SISTEMAS_03:
        assert _conteudo_do_sistema(destino, sistema), f"{sistema} sem arquivos na landing"
    assert not any("gabarito" in c for c in destino.listar(""))
    tabelas = geracoes.gabarito(destino)
    assert {"mapa_chaves", "defeitos", "pessoas"} <= set(tabelas)
    assert all(tabelas[nome] for nome in ("mapa_chaves", "pessoas"))
    # O gabarito só descreve o que foi entregue: os sistemas de fora da fatia não aparecem.
    assert {r["sistema"] for r in tabelas["mapa_chaves"]} <= SISTEMAS_03
    assert {r["sistema"] for r in tabelas["defeitos"]} <= SISTEMAS_03


def _linhas_csv(conteudo: bytes) -> dict[str, dict]:
    linhas = list(csv.DictReader(io.StringIO(conteudo.decode("utf-8"))))
    return {linha[next(iter(linha))]: linha for linha in linhas}  # a chave é a primeira coluna


def _linhas_json(destino, sistema: str) -> set[tuple[str, str]]:
    """(entidade, linha) de cada linha entregue, sem a data do lote: a reentrega do CDC repete a linha."""
    return {
        (caminho.split("/")[1], linha)
        for caminho in destino.listar(sistema)
        for linha in destino.ler(caminho).decode("utf-8").splitlines()
    }


def _linhas_parquet(destino, sistema: str) -> set[tuple[str, tuple]]:
    """(entidade, linha) de cada versão entregue, sem a data do lote: o evento atrasado só troca o lote."""
    return {
        (caminho.split("/")[1], tuple(sorted((k, str(v)) for k, v in linha.items())))
        for caminho in destino.listar(sistema)
        for linha in pq.read_table(io.BytesIO(destino.ler(caminho))).to_pylist()
    }


@pytest.mark.lento
def test_o_mesmo_cliente_tem_os_mesmos_dados_nos_artigos_02_e_03(fatia_02, fatia_03):
    """A 02 não tem gabarito e sai limpa; a 03 tem os defeitos do catálogo. O mesmo cliente tem os
    mesmos dados nas duas, exceto onde a 03 registra um defeito (e ali a 02 tem o valor original)."""
    limpa, suja = fatia_02.final, fatia_03.final
    assert _linhas_json(limpa, "embarque") == _linhas_json(suja, "embarque")
    assert _linhas_parquet(limpa, "nucleo") == _linhas_parquet(suja, "nucleo")

    caminhos = limpa.listar("patio")
    assert caminhos == suja.listar("patio")
    registrados = {
        (r["data_entrega"].isoformat(), r["entidade"], r["chave_sistema"], r["coluna"]): r["valor_original"]
        for r in geracoes.gabarito(suja, "patio")["defeitos"]
    }
    assert registrados, "a 03 registra defeitos do Pátio"
    for caminho in caminhos:
        _, entidade, data_foto, _ = caminho.split("/")
        no_02, no_03 = _linhas_csv(limpa.ler(caminho)), _linhas_csv(suja.ler(caminho))
        assert no_02.keys() == no_03.keys(), caminho
        for chave, linha in no_03.items():
            for coluna, valor in linha.items():
                if valor != no_02[chave][coluna]:
                    assert registrados[(data_foto, entidade, chave, coluna)] == no_02[chave][coluna], (caminho, chave, coluna)


@pytest.mark.lento
def test_cada_artigo_foi_gerado_sozinho_e_o_relogio_e_o_da_carga_inicial(fatia_02, fatia_03):
    # As duas gerações partem de destinos vazios e independentes: nenhum artigo precisa do outro.
    assert fatia_02.final.raiz != fatia_03.final.raiz
    for gerada, artigo in ((fatia_02, "02-historizacao"), (fatia_03, "03-3fn")):
        assert gerada.resultados[0].data_relogio == geracoes.CARGA_INICIAL
        relogios = gerada.final.listar("_controle/relogio")
        assert relogios == [f"_controle/relogio/{geracoes.CARGA_INICIAL.isoformat()}.json"]
        assert json.loads(gerada.final.ler(relogios[0])), artigo
