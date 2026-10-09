"""Fatia 01-bronze completa, observada pelo que `gerar_fatia`/`avancar_tempo` gravam no
destino local: sistemas da fatia, formatos, determinismo, idempotência, `reset` e a
fronteira do primeiro destino (nada em `bronze`, `silver` ou `gold`).

Para poupar a cota, a fatia é gerada só duas vezes: na geração compartilhada `fatia_01`
(carga inicial e avanço de 1 dia, pré-aquecida em paralelo e com cache) e no destino B, cópia
da carga recriada com `reset=True`, o que também compara duas gerações independentes. A
segunda chamada sem reset volta antes de abrir a sessão. É o teste de marco do ticket 10
(fatia oficial inteira), por isso marcado como `lento`.
"""

import csv
import io
import json
import shutil
from datetime import date
from pathlib import Path

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal, carregar_roteiro, gerar_fatia

pytestmark = pytest.mark.lento

ARTIGO = "01-bronze"
ESCALA = geracoes.ESCALA_FATIA_INTEIRA  # menor escala com rota da Viação (a 0,01 não tem passagens)
CARGA_INICIAL = date(2025, 12, 31)
DIA_NOVO = "2026-01-01"
SISTEMAS = {"embarque", "patio", "passagemhub", "rastro", "ibge"}
EXTENSOES = {"embarque": ".json", "patio": ".csv", "passagemhub": ".json", "rastro": ".json", "ibge": ".csv"}


def _arquivos(pasta: Path) -> list[Path]:
    return sorted(p for p in pasta.rglob("*") if p.is_file())


def _conteudo(pasta: Path) -> dict[str, bytes]:
    return {p.relative_to(pasta).as_posix(): p.read_bytes() for p in _arquivos(pasta)}


def _sem_relogio(conteudo: dict[str, bytes]) -> dict[str, bytes]:
    return {c: v for c, v in conteudo.items() if not c.startswith("_controle/")}


@pytest.fixture(scope="module")
def fatia(fatia_01, spark, tmp_path_factory):
    """A geração `fatia_01` (carga inicial e avanço de 1 dia) mais duas cópias da carga: uma
    para a segunda chamada sem reset (volta antes de abrir a sessão) e outra, com um arquivo
    intruso, recriada com `reset=True`, o que também compara duas gerações independentes."""
    pasta = tmp_path_factory.mktemp("fatia01")
    carga_inicial = fatia_01.etapas[0]
    carga = _conteudo(carga_inicial.raiz)

    sem_reset = DestinoLocal(pasta / "sem_reset" / "medalhao_01_bronze")
    shutil.copytree(carga_inicial.raiz, sem_reset.raiz)
    segunda = gerar_fatia(ARTIGO, sem_reset, escala=ESCALA, roteiro=fatia_01.roteiro)
    depois_da_segunda = _conteudo(sem_reset.raiz)

    b = DestinoLocal(pasta / "b" / "medalhao_01_bronze")
    shutil.copytree(carga_inicial.raiz, b.raiz)
    intruso = b.raiz / "bronze" / "sobra_de_execucao_anterior.txt"
    intruso.write_text("lixo", encoding="utf-8")
    recriada = gerar_fatia(ARTIGO, b, escala=ESCALA, reset=True, spark=spark, roteiro=fatia_01.roteiro)
    return {
        "a": fatia_01.final, "b": b, "carga": carga, "primeira": fatia_01.resultados[0], "segunda": segunda,
        "depois_da_segunda": depois_da_segunda, "avanco": fatia_01.resultados[1], "recriada": recriada,
        "intruso": intruso,
    }


def test_so_os_sistemas_da_fatia_chegam_a_landing_e_nao_ha_gabarito(fatia):
    assert set(carregar_roteiro().fatia(ARTIGO).sistemas) == SISTEMAS
    landing = _conteudo(fatia["a"].volume)

    assert {c.split("/")[0] for c in landing} == SISTEMAS | {"_controle"}
    assert not (fatia["a"].raiz / "gabarito").exists()
    assert not any("gabarito" in c for c in landing)


def test_fronteira_do_primeiro_destino_depois_da_carga_e_do_avanco(fatia):
    raiz = fatia["a"].raiz
    for schema in ("landing", "bronze", "silver", "gold"):
        assert (raiz / schema).is_dir(), f"schema {schema} não foi criado"
    for schema in ("bronze", "silver", "gold"):
        assert _arquivos(raiz / schema) == [], f"o gerador escreveu em {schema}"
    assert all(c.startswith("landing/") for c in fatia["carga"])


def test_cada_sistema_entrega_no_seu_formato(fatia):
    landing = _sem_relogio(_conteudo(fatia["a"].volume))
    por_sistema: dict[str, list[tuple[str, bytes]]] = {s: [] for s in SISTEMAS}
    for caminho, conteudo in landing.items():
        sistema, entidade, data_lote, nome = caminho.split("/")
        assert nome.startswith(f"{entidade}_{data_lote.replace('-', '')}_"), caminho
        assert nome.endswith(EXTENSOES[sistema]), caminho
        por_sistema[sistema].append((caminho, conteudo))
    assert all(por_sistema.values()), {s: len(v) for s, v in por_sistema.items()}

    for _, conteudo in por_sistema["embarque"]:  # CDC em JSON lines
        for linha in conteudo.decode("utf-8").splitlines():
            registro = json.loads(linha)
            assert registro["op"] in ("I", "U", "D") and isinstance(registro["seq"], int)
    for caminho, conteudo in por_sistema["patio"]:  # snapshot em CSV com cabeçalho
        linhas = list(csv.reader(io.StringIO(conteudo.decode("utf-8"))))
        assert linhas and all(len(l) == len(linhas[0]) for l in linhas), caminho
    for _, conteudo in por_sistema["passagemhub"]:  # uma página da API por arquivo
        pagina = json.loads(conteudo)
        assert {"pagina", "total_paginas", "eventos"} <= set(pagina)
        assert all(isinstance(e["comprador"], dict) for e in pagina["eventos"])
    for _, conteudo in por_sistema["rastro"]:  # JSON lines aninhado
        for linha in conteudo.decode("utf-8").splitlines():
            evento = json.loads(linha)
            assert isinstance(evento["dispositivo"], dict) and isinstance(evento["alertas"], list)


def test_grava_municipios_do_ibge_na_landing_pela_data_simulada(fatia):
    pasta = fatia["a"].volume / "ibge" / "municipios" / CARGA_INICIAL.isoformat()
    csvs = list(pasta.glob("*.csv"))
    assert len(csvs) == 1
    linhas = csvs[0].read_text(encoding="utf-8").splitlines()
    assert linhas[0].startswith("cod_municipio,nome_municipio,cod_uf,sigla_uf")
    assert len(linhas) - 1 == 5571
    assert any(linha.startswith("3550308,São Paulo,35,SP,São Paulo,3,SE,Sudeste") for linha in linhas)


def test_relogio_comeca_na_data_da_carga_inicial(fatia):
    assert fatia["primeira"].gerou
    assert fatia["primeira"].data_relogio == CARGA_INICIAL


def test_segunda_chamada_sem_reset_nao_altera_nada_e_informa_o_relogio(fatia):
    segunda = fatia["segunda"]

    assert not segunda.gerou
    assert segunda.data_relogio == CARGA_INICIAL
    assert "31/12/2025" in segunda.mensagem
    assert fatia["depois_da_segunda"] == fatia["carga"]


def test_avancar_um_dia_so_cria_lotes_do_dia_novo_dos_sistemas_da_fatia(fatia):
    avanco, carga = fatia["avanco"], fatia["carga"]
    depois = _conteudo(fatia["a"].raiz)

    assert avanco.data_relogio == date(2026, 1, 1)
    assert {c: depois[c] for c in carga} == carga, "nenhum arquivo já entregue muda"
    novos = [c.removeprefix("landing/landing/") for c in depois if c not in carga]
    novos_de_sistema = [c for c in novos if not c.startswith("_controle/")]
    assert {c.split("/")[2] for c in novos_de_sistema} == {DIA_NOVO}
    assert {"patio", "rastro"} <= {c.split("/")[0] for c in novos_de_sistema} <= SISTEMAS - {"ibge"}
    assert avanco.arquivos == len(novos_de_sistema)


def test_reset_recria_o_ambiente_do_zero_com_o_mesmo_conteudo(fatia):
    assert fatia["recriada"].gerou
    assert not fatia["intruso"].exists()
    assert _conteudo(fatia["b"].raiz) == fatia["carga"], "duas gerações da fatia gravam os mesmos bytes"
