"""Carga completa da série, observada pelo que `gerar_carga_inicial`/`avancar_tempo` gravam no
destino local: é o que o `00_setup` deixa para o artigo 01 construir a Bronze e para os
artigos seguintes construírem sobre ela.

Confere todos os sistemas na landing, cada um no seu formato, o catálogo de defeitos inteiro
registrado no gabarito, a convergência de cliente, veículo e geografia entre as divisões,
idempotência, avanço do relógio, `reset` e a fronteira do destino (o Silver só com o
gabarito, o Gold vazio).

Para poupar a cota, a carga é gerada uma única vez, na geração compartilhada `carga_completa`
(escala 0,03, carga inicial e avanço de 1 dia, pré-aquecida em paralelo e com cache). A
segunda chamada sem reset volta antes de gerar e o `reset=True` roda sobre uma cópia da carga
com os sistemas reduzidos ao IBGE, o que basta para provar que o lakehouse é recriado do zero.
Marcado como `lento` (carga oficial inteira).
"""

import csv
import io
import itertools
import json
import shutil
from datetime import date
from pathlib import Path

import geracoes
import pyarrow.parquet as pq
import pytest
from geracoes import conteudo as _conteudo

from modelbricks_gerador import DestinoLocal, carregar_roteiro, gerar_carga_inicial

pytestmark = pytest.mark.lento

ROTEIRO = carregar_roteiro()
SISTEMAS = set(ROTEIRO.carga.sistemas)
ESCALA = geracoes.ESCALA_CARGA_COMPLETA
CARGA_INICIAL = date(2025, 12, 31)
DIA_NOVO = "2026-01-01"
EXTENSAO = {"json": ".json", "json_aninhado": ".json", "parquet": ".parquet", "csv": ".csv", "csv_latin1": ".csv"}

# Divisão (empresa) dona de cada sistema, como o gabarito a chama.
DIVISAO = {nome: sistema.empresa for nome, sistema in ROTEIRO.sistemas.items()}


def _arquivos(pasta: Path) -> list[Path]:
    return sorted(p for p in pasta.rglob("*") if p.is_file())


def _sem_relogio(conteudo: dict[str, bytes]) -> dict[str, bytes]:
    return {c: v for c, v in conteudo.items() if not c.startswith("_controle/")}


def _mundo(destino: DestinoLocal) -> dict[str, list[dict]]:
    """As tabelas do mundo verdadeiro no gabarito (sem o mapa de chaves e os defeitos, que
    dependem dos sistemas da carga)."""
    tabelas = geracoes.gabarito(destino)
    return {nome: linhas for nome, linhas in tabelas.items() if nome not in ("mapa_chaves", "defeitos")}


@pytest.fixture(scope="module")
def carga(carga_completa, spark, tmp_path_factory):
    """A geração `carga_completa` (carga inicial e avanço de 1 dia) mais duas cópias da carga:
    uma para a segunda chamada sem reset e outra, com um schema intruso no Silver (o que um
    artigo teria criado), recriada com `reset=True`."""
    pasta = tmp_path_factory.mktemp("carga")
    carga_inicial = carga_completa.etapas[0]
    landing = _conteudo(carga_inicial)

    sem_reset = DestinoLocal(pasta / "sem_reset", geracoes.AMBIENTE)
    shutil.copytree(carga_inicial.raiz, sem_reset.raiz)
    segunda = gerar_carga_inicial(sem_reset, escala=ESCALA, roteiro=carga_completa.roteiro)
    depois_da_segunda = geracoes.arvore(sem_reset)

    b = DestinoLocal(pasta / "b", geracoes.AMBIENTE)
    shutil.copytree(carga_inicial.raiz, b.raiz)
    intruso = b.pasta("silver") / "ods"
    intruso.mkdir()
    # O reset recria o lakehouse do zero; para poupar a cota, recria só o IBGE (carga reduzida),
    # que é a parte da landing que se pode comparar byte a byte com a da geração compartilhada.
    recriada = gerar_carga_inicial(b, escala=ESCALA, reset=True, spark=spark, roteiro=geracoes.roteiro_so_ibge())
    return {
        "a": carga_completa.final, "roteiro": carga_completa.roteiro, "carga_inicial": carga_inicial, "b": b, "landing": landing,
        "primeira": carga_completa.resultados[0], "segunda": segunda, "depois_da_segunda": depois_da_segunda,
        "avanco": carga_completa.resultados[1], "recriada": recriada, "intruso": intruso,
    }


def test_todos_os_sistemas_chegam_a_landing_e_o_gabarito_fica_fora_dela(carga):
    landing = _conteudo(carga["a"])

    assert {c.split("/")[0] for c in landing} == SISTEMAS | {"_controle"}
    for sistema in SISTEMAS:
        assert geracoes.arquivos(carga["a"], sistema), f"{sistema} sem arquivos na landing"
    assert not any("gabarito" in c for c in landing)


def test_fronteira_do_destino_bronze_com_as_landings_silver_so_com_gabarito_e_gold_vazio(carga):
    destino = carga["a"]
    assert {p.name for p in destino.raiz.iterdir()} == set(destino.catalogos.values())
    assert {p.name for p in destino.pasta("bronze").iterdir()} == SISTEMAS | {"_controle"}
    for sistema in SISTEMAS:
        assert destino.pasta_sistema(sistema).is_dir(), f"o Volume landing de {sistema} não foi criado"
    assert {p.name for p in destino.pasta("silver").iterdir()} == {"gabarito"}
    assert _arquivos(destino.pasta("gold")) == [], "o gerador escreveu no catálogo Gold"


def test_cada_sistema_entrega_no_seu_formato(carga):
    landing = _sem_relogio(_conteudo(carga["a"]))
    por_sistema: dict[str, list[tuple[str, bytes]]] = {s: [] for s in SISTEMAS}
    for caminho, conteudo in landing.items():
        sistema, entidade, data_lote, nome = caminho.split("/")
        assert nome.startswith(f"{entidade}_{data_lote.replace('-', '')}_"), caminho
        assert nome.endswith(EXTENSAO[ROTEIRO.sistemas[sistema].formato]), caminho
        por_sistema[sistema].append((caminho, conteudo))
    assert all(por_sistema.values()), {s: len(v) for s, v in por_sistema.items()}

    for sistema in ("embarque", "rotacerta"):  # CDC em JSON lines
        for _, conteudo in por_sistema[sistema]:
            for linha in conteudo.decode("utf-8").splitlines():
                registro = json.loads(linha)
                assert registro["op"] in ("I", "U", "D") and isinstance(registro["seq"], int)
    for sistema in ("patio", "apolice", "ibge"):  # CSV em UTF-8 com cabeçalho
        for caminho, conteudo in por_sistema[sistema]:
            linhas = list(csv.reader(io.StringIO(conteudo.decode("utf-8"))))
            assert linhas and all(len(l) == len(linhas[0]) for l in linhas), caminho
    for sistema in ("solticket", "solfin"):  # CSV com ; em latin1
        for caminho, conteudo in por_sistema[sistema]:
            linhas = list(csv.reader(io.StringIO(conteudo.decode("latin1")), delimiter=";"))
            assert linhas and all(len(l) == len(linhas[0]) for l in linhas), caminho
    for caminho, conteudo in por_sistema["nucleo"]:  # Parquet
        assert pq.read_table(io.BytesIO(conteudo)).num_rows > 0, caminho
    for _, conteudo in por_sistema["passagemhub"]:  # uma página da API por arquivo
        pagina = json.loads(conteudo)
        assert {"pagina", "total_paginas", "eventos"} <= set(pagina)
        assert all(isinstance(e["comprador"], dict) for e in pagina["eventos"])
    for _, conteudo in por_sistema["rastro"]:  # JSON lines aninhado
        for linha in conteudo.decode("utf-8").splitlines():
            evento = json.loads(linha)
            assert isinstance(evento["dispositivo"], dict) and isinstance(evento["alertas"], list)


def test_a_bronze_recebe_o_catalogo_de_defeitos_inteiro_registrado_no_gabarito(carga):
    """A Bronze da série é uma só, então os defeitos que os artigos da Silver corrigem já
    chegam na carga inicial, cada um registrado em `gabarito.defeitos`."""
    roteiro = carregar_roteiro(carga["roteiro"])  # o roteiro de teste, com as taxas da geração
    esperados = {
        (tipo, sistema)
        for tipo, defeito in roteiro.defeitos.items()
        for sistema, taxa in defeito.taxas.items()
        if not isinstance(taxa, float) or taxa > 0
    }
    registrados = {(r["tipo"], r["sistema"]) for r in geracoes.gabarito(carga["carga_inicial"])["defeitos"]}

    assert esperados - registrados == set(), "todo defeito do catálogo aparece na carga inicial"
    assert registrados <= esperados


def test_grava_municipios_do_ibge_na_landing_pela_data_simulada(carga):
    pasta = carga["a"].pasta_sistema("ibge") / "municipios" / CARGA_INICIAL.isoformat()
    csvs = list(pasta.glob("*.csv"))
    assert len(csvs) == 1
    linhas = csvs[0].read_text(encoding="utf-8").splitlines()
    assert linhas[0].startswith("cod_municipio,nome_municipio,cod_uf,sigla_uf")
    assert len(linhas) - 1 == 5571
    assert any(linha.startswith("3550308,São Paulo,35,SP,São Paulo,3,SE,Sudeste") for linha in linhas)


def test_relogio_comeca_na_data_da_carga_inicial(carga):
    assert carga["primeira"].gerou
    assert carga["primeira"].data_relogio == CARGA_INICIAL
    relogios = carga["carga_inicial"].listar("_controle/relogio")
    assert relogios == [f"_controle/relogio/{CARGA_INICIAL.isoformat()}.json"]


def test_segunda_chamada_sem_reset_nao_altera_nada_e_informa_o_relogio(carga):
    segunda = carga["segunda"]

    assert not segunda.gerou
    assert segunda.data_relogio == CARGA_INICIAL
    assert "31/12/2025" in segunda.mensagem
    assert carga["depois_da_segunda"] == geracoes.arvore(carga["carga_inicial"])


def test_avancar_um_dia_so_cria_lotes_do_dia_novo(carga):
    avanco, landing = carga["avanco"], carga["landing"]
    depois = _conteudo(carga["a"])

    assert avanco.data_relogio == date(2026, 1, 1)
    assert {c: depois[c] for c in landing} == landing, "nenhum arquivo já entregue muda"
    novos = [c for c in depois if c not in landing]
    novos_de_sistema = [c for c in novos if not c.startswith("_controle/")]
    assert {c.split("/")[2] for c in novos_de_sistema} == {DIA_NOVO}
    assert {"patio", "rastro", "embarque"} <= {c.split("/")[0] for c in novos_de_sistema} <= SISTEMAS - {"ibge"}
    assert avanco.arquivos == len(novos_de_sistema)


def test_reset_recria_o_lakehouse_do_zero_com_o_mesmo_conteudo(carga):
    assert carga["recriada"].gerou
    assert not carga["intruso"].exists(), "o reset apaga também o que os artigos criaram"
    ibge_e_relogio = {c: v for c, v in carga["landing"].items() if c.startswith(("ibge/", "_controle/"))}
    assert ibge_e_relogio
    assert _conteudo(carga["b"]) == ibge_e_relogio, "o lakehouse recriado grava os mesmos bytes"
    assert _mundo(carga["b"]) == _mundo(carga["carga_inicial"]), "e o mesmo mundo verdadeiro"


# Integração entre as divisões: o que a 3FN, o Business Vault e os Data Marts integram.


def _ids_por_sistema(mapa_chaves: list[dict], tabela: str) -> dict[str, set]:
    ids: dict[str, set] = {}
    for r in mapa_chaves:
        if r["tabela_verdadeira"] == tabela:
            ids.setdefault(r["sistema"], set()).add(r["id_verdadeiro"])
    return ids


def _ids_por_divisao(mapa_chaves: list[dict], tabela: str) -> dict[str, set]:
    por_divisao: dict[str, set] = {}
    for sistema, ids in _ids_por_sistema(mapa_chaves, tabela).items():
        por_divisao.setdefault(DIVISAO[sistema], set()).update(ids)
    return por_divisao


def test_o_mapa_de_chaves_cobre_todos_os_sistemas_com_entidades_do_mundo(carga):
    mapa = geracoes.gabarito(carga["a"])["mapa_chaves"]

    # O SolFin e o IBGE não têm chave de entidade do mundo verdadeiro no mapa; os demais têm.
    assert {r["sistema"] for r in mapa} == SISTEMAS - {"solfin", "ibge"}


@pytest.mark.parametrize(
    "tabela, divisoes",
    [
        ("pessoas", {"mb_viacao", "mb_veiculos", "mb_seguros", "evs"}),  # a mesma pessoa compra passagem e seguro
        ("empresas_pj", {"mb_cargas", "mb_veiculos", "mb_seguros", "mb"}),  # frete, caminhão, seguro, fornecedor
        ("veiculos", {"mb_veiculos", "mb_cargas", "mb"}),  # DMS, TMS, telemetria e ERP como ativo
    ],
)
def test_cliente_e_veiculo_convergem_entre_todas_as_divisoes(carga, tabela, divisoes):
    por_divisao = _ids_por_divisao(geracoes.gabarito(carga["a"])["mapa_chaves"], tabela)

    assert set(por_divisao) >= divisoes
    for a, b in itertools.combinations(sorted(divisoes), 2):
        assert por_divisao[a] & por_divisao[b], f"nenhum registro de {tabela} em comum entre {a} e {b}"


def test_veiculos_da_frota_reaparecem_na_telemetria_e_no_erp_como_ativo(carga):
    tabelas = geracoes.gabarito(carga["a"])
    por_sistema = _ids_por_sistema(tabelas["mapa_chaves"], "veiculos")
    frota = {v["id_veiculo"] for v in tabelas["veiculos"] if v["frota_propria"]}

    assert frota
    assert frota <= por_sistema["patio"] and frota <= por_sistema["rastro"] and frota <= por_sistema["nucleo"]


def test_geografia_toda_localizacao_das_divisoes_existe_nos_municipios_do_ibge(carga):
    destino = carga["a"]
    pasta = destino.pasta_sistema("ibge") / "municipios" / CARGA_INICIAL.isoformat()
    codigos = {
        int(linha["cod_municipio"])
        for arquivo in pasta.glob("*.csv")
        for linha in csv.DictReader(io.StringIO(arquivo.read_text(encoding="utf-8")))
    }
    tabelas = geracoes.gabarito(destino)
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
