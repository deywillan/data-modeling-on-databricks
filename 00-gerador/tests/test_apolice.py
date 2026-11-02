"""*Apólice+* (sistema de apólices da ModelBricks Seguros): incremental por `updated_at` em CSV,
com valor monetário como texto ("1.234,56").

Observado só pelo que `gerar_carga_inicial`/`avancar_tempo` gravam: os arquivos da landing e as
tabelas do schema `gabarito`.

Para poupar a cota, o módulo usa a geração `principal` (ver `geracoes.py`), compartilhada
com os outros sistemas, cujo roteiro de teste faz toda venda de veículo a um segurado levar
o seguro (para que a escala mínima tenha seguros vendidos junto com veículos).
"""

import csv
import io
import math
import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal, carregar_roteiro

ESCALA = geracoes.PRINCIPAL.escala
CARGA_INICIAL = date(2025, 12, 31)
ULTIMO_DIA = date(2026, 1, 12)
ENTIDADES = {"segurados": "codigo_segurado", "apolices": "numero_apolice"}
MONETARIAS = ("importancia_segurada", "premio")
PADRAO = re.compile(r"apolice/([a-z_]+)/(\d{4}-\d{2}-\d{2})/\1_(\d{8})_0001\.csv")
TEXTO_MONETARIO = re.compile(r"\d{1,3}(\.\d{3})*,\d{2}")


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    return geracoes.arquivos(destino, "apolice")


def _linhas(arquivos: dict[str, bytes]) -> dict[str, list[dict]]:
    """Linhas entregues por entidade (texto, como no CSV), com a data do lote e o arquivo."""
    linhas = defaultdict(list)
    for caminho, conteudo in arquivos.items():
        _, entidade, data_lote, _ = caminho.split("/")
        for linha in csv.DictReader(io.StringIO(conteudo.decode("utf-8"))):
            linhas[entidade].append({**linha, "_lote": date.fromisoformat(data_lote), "_arquivo": caminho})
    return linhas


def _instante(texto: str) -> datetime:
    return datetime.strptime(texto, "%Y-%m-%d %H:%M:%S")


def _versoes(linhas: list[dict], chave: str) -> dict:
    por_chave = defaultdict(list)
    for linha in sorted(linhas, key=lambda x: x["updated_at"]):
        por_chave[linha[chave]].append(linha)
    return por_chave


def _texto_monetario(valor: Decimal) -> str:
    """Formatação brasileira de referência: milhar com ponto e centavos com vírgula."""
    return f"{valor:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _numero(texto: str) -> Decimal:
    return Decimal(texto.replace(".", "").replace(",", "."))


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    return geracoes.gabarito(destino, "apolice")


def _segurados_verdadeiros(gabarito) -> dict[str, tuple[str, int]]:
    """codigo_segurado → (tabela verdadeira, id verdadeiro), pelo mapa de chaves."""
    return {
        m["chave_sistema"]: (m["tabela_verdadeira"], m["id_verdadeiro"])
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == "apolice" and m["entidade"] == "segurados"
    }


@pytest.fixture(scope="module")
def gerado(principal):
    """Carga inicial e avanço de 12 dias (geração `principal`)."""
    assert carregar_roteiro(principal.roteiro).parametros.apolice.seguro_na_venda == 1.0
    assert principal.resultados[-1].data_relogio == ULTIMO_DIA
    destino = principal.final
    arquivos = _arquivos(destino)
    return destino, _arquivos(principal.etapas[0]), arquivos, _linhas(arquivos), _gabarito(destino)


def test_lote_diario_em_csv_so_com_linhas_alteradas_no_dia(gerado):
    _, carga, arquivos, linhas, _ = gerado

    assert {c.split("/")[1] for c in arquivos} == set(ENTIDADES)
    for caminho, conteudo in arquivos.items():
        casamento = PADRAO.fullmatch(caminho)
        assert casamento, caminho
        assert casamento[3] == casamento[2].replace("-", "")
        cabecalho = conteudo.decode("utf-8").split("\n", 1)[0]
        assert "updated_at" in cabecalho and not any(c.startswith('"_') for c in cabecalho.split(","))
    for entidade in ENTIDADES:
        for linha in linhas[entidade]:
            assert _instante(linha["updated_at"]).date() == linha["_lote"], "updated_at fora do dia do lote"
    datas_carga = {date.fromisoformat(c.split("/")[2]) for c in carga}
    assert min(datas_carga) >= date(2024, 1, 1) and max(datas_carga) <= CARGA_INICIAL
    assert len({d for d in datas_carga}) > 300, "lotes espalhados pelos dois anos de história"
    assert max(date.fromisoformat(c.split("/")[2]) for c in arquivos) == ULTIMO_DIA


def test_valores_monetarios_sempre_como_texto_com_virgula(gerado):
    _, _, arquivos, linhas, _ = gerado

    assert linhas["apolices"]
    for linha in linhas["apolices"]:
        for coluna in MONETARIAS:
            assert TEXTO_MONETARIO.fullmatch(linha[coluna]), f"{coluna}: {linha[coluna]!r}"
        assert _numero(linha["premio"]) < _numero(linha["importancia_segurada"])
    grandes = [l for l in linhas["apolices"] if _numero(l["importancia_segurada"]) >= 1000]
    assert grandes and all("." in l["importancia_segurada"] for l in grandes), "milhar separado por ponto"
    # A vírgula decimal convive com o separador do CSV: o campo vem entre aspas.
    exemplo = linhas["apolices"][0]
    assert f'"{exemplo["premio"]}"' in arquivos[exemplo["_arquivo"]].decode("utf-8")


def test_segurados_sao_os_clientes_da_seguradora_e_se_sobrepoem_aos_de_passagem(gerado):
    _, _, _, linhas, gabarito = gerado
    verdadeiros = _segurados_verdadeiros(gabarito)
    tabela = {"pessoa": "pessoas", "empresa_pj": "empresas_pj"}
    clientes = defaultdict(set)
    for p in gabarito["participacoes"]:
        if p["papel"] == "cliente" and p["tipo_entidade"] in tabela:
            clientes[p["empresa"]].add((tabela[p["tipo_entidade"]], p["id_entidade"]))
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    empresas = {e["id_empresa"]: e for e in gabarito["empresas_pj"]}
    defeitos_cpf = {
        r["chave_sistema"]: r for r in gabarito["defeitos"] if r["sistema"] == "apolice" and r["tipo"] == "cpf_fora_do_padrao"
    }

    entregues = {l["codigo_segurado"] for l in linhas["segurados"]}
    assert entregues == set(verdadeiros), "todo segurado entregue está no mapa, e vice-versa"
    assert set(verdadeiros.values()) == clientes["mb_seguros"], "segurados = clientes da ModelBricks Seguros"
    assert len(set(verdadeiros.values())) == len(verdadeiros)
    sobreposicao = set(verdadeiros.values()) & clientes["mb_viacao"]
    assert sobreposicao == clientes["mb_seguros"] & clientes["mb_viacao"] and sobreposicao, \
        "quem faz seguro e viaja aparece nos dois conforme a participação"
    for linha in linhas["segurados"]:
        origem, id_verdadeiro = verdadeiros[linha["codigo_segurado"]]
        if origem == "pessoas":
            pessoa = pessoas[id_verdadeiro]
            assert (linha["tipo_pessoa"], linha["nome"], linha["cnpj"]) == ("PF", pessoa["nome"], "")
            defeito = defeitos_cpf.get(linha["codigo_segurado"])
            if defeito is None:
                assert linha["cpf"] == pessoa["cpf"]
            else:
                assert (defeito["valor_original"], defeito["valor_sujo"]) == (pessoa["cpf"], linha["cpf"])
                assert linha["cpf"] != pessoa["cpf"]
        else:
            empresa = empresas[id_verdadeiro]
            assert (linha["tipo_pessoa"], linha["nome"], linha["cnpj"], linha["cpf"]) == (
                "PJ", empresa["razao_social"], empresa["cnpj"], "")


def test_seguros_vendidos_junto_com_veiculos(gerado):
    _, _, _, linhas, gabarito = gerado
    verdadeiros = _segurados_verdadeiros(gabarito)
    por_entidade = {v: k for k, v in verdadeiros.items()}
    cadastro = {
        k: min(_instante(v["updated_at"]).date() for v in versoes)
        for k, versoes in _versoes(linhas["segurados"], "codigo_segurado").items()
    }
    veiculos = {v["id_veiculo"]: v for v in gabarito["veiculos"]}
    esperadas = {}
    for venda in gabarito["vendas_veiculos"]:
        comprador = ("pessoas", venda["id_pessoa"]) if venda["id_pessoa"] else ("empresas_pj", venda["id_empresa"])
        codigo = por_entidade.get(comprador)
        if codigo and cadastro[codigo] < venda["data_venda"] <= ULTIMO_DIA:
            esperadas[veiculos[venda["id_veiculo"]]["placa"]] = (codigo, venda)

    com_veiculo = {}
    for numero, versoes in _versoes(linhas["apolices"], "numero_apolice").items():
        if versoes[0]["placa"]:
            com_veiculo[versoes[0]["placa"]] = versoes[0]
    assert esperadas, "a escala do teste tem vendas de veículo a segurados"
    assert set(com_veiculo) == set(esperadas), "toda venda a segurado leva o seguro (roteiro do teste)"
    for placa, emissao in com_veiculo.items():
        codigo, venda = esperadas[placa]
        assert emissao["codigo_segurado"] == codigo and emissao["ramo"] == "veiculo"
        assert date.fromisoformat(emissao["inicio_vigencia"]) == venda["data_venda"] == _instante(emissao["updated_at"]).date()
        assert emissao["importancia_segurada"] == _texto_monetario(venda["valor_venda"])


def test_ciclo_de_vida_das_apolices(gerado):
    _, _, _, linhas, _ = gerado
    roteiro = carregar_roteiro()
    p = roteiro.parametros.apolice
    segurados = {
        k: min(_instante(v["updated_at"]) for v in versoes)
        for k, versoes in _versoes(linhas["segurados"], "codigo_segurado").items()
    }
    apolices = _versoes(linhas["apolices"], "numero_apolice")
    avulsas_ate_a_carga = 0

    for numero, versoes in apolices.items():
        emissao = versoes[0]
        assert emissao["status"] == "ativa" and emissao["data_cancelamento"] == ""
        assert segurados[emissao["codigo_segurado"]] < _instante(emissao["updated_at"]), "segurado cadastrado antes"
        inicio, fim = date.fromisoformat(emissao["inicio_vigencia"]), date.fromisoformat(emissao["fim_vigencia"])
        assert 360 <= (fim - inicio).days <= 370
        if emissao["placa"]:
            assert emissao["ramo"] == "veiculo"
        else:
            ramos = p.ramos_pf if emissao["ramo"] in p.ramos_pf else p.ramos_pj
            assert emissao["ramo"] in ramos
            avulsas_ate_a_carga += _instante(emissao["updated_at"]).date() <= CARGA_INICIAL
        assert len(versoes) <= 2
        if len(versoes) == 2:
            final = versoes[1]
            if final["status"] == "cancelada":
                cancelamento = date.fromisoformat(final["data_cancelamento"])
                assert inicio < cancelamento <= fim and _instante(final["updated_at"]).date() == cancelamento
            else:
                assert final["status"] == "encerrada" and _instante(final["updated_at"]).date() > fim
            fixos = ("status", "data_cancelamento", "updated_at", "_lote", "_arquivo")
            assert {k: v for k, v in final.items() if k not in fixos} == {
                k: v for k, v in emissao.items() if k not in fixos
            }
    esperado = roteiro.volumes.apolices * ESCALA
    assert abs(avulsas_ate_a_carga - esperado) <= 0.15 * esperado, f"{avulsas_ate_a_carga} avulsas (~{esperado:.0f})"
    # Taxa de cancelamento medida nas apólices cuja vigência inteira já passou.
    completas = [v for v in apolices.values() if date.fromisoformat(v[0]["fim_vigencia"]) < ULTIMO_DIA]
    canceladas = sum(v[-1]["status"] == "cancelada" for v in completas)
    desvio = math.sqrt(p.cancelamento * (1 - p.cancelamento) / len(completas))
    assert abs(canceladas / len(completas) - p.cancelamento) <= 4 * desvio + 1 / len(completas)
    encerradas = sum(v[-1]["status"] == "encerrada" for v in apolices.values())
    assert encerradas > 0, "apólices de 2024 encerram ao fim da vigência"


def test_valor_monetario_como_texto_registrado_por_coluna_no_gabarito(gerado):
    _, _, arquivos, linhas, gabarito = gerado
    roteiro = carregar_roteiro()
    registros = [r for r in gabarito["defeitos"] if r["sistema"] == "apolice" and r["tipo"] == "valor_monetario_texto"]
    por_arquivo = defaultdict(list)
    for linha in linhas["apolices"]:
        por_arquivo[linha["_arquivo"]].append(linha)

    assert {(r["arquivo"], r["coluna"]) for r in registros} == {(a, c) for a in por_arquivo for c in MONETARIAS}
    assert len(registros) == len(por_arquivo) * len(MONETARIAS)
    for r in registros:
        assert r["granularidade"] == "coluna" and r["entidade"] == "apolices"
        assert r["chave_sistema"] is None and r["id_verdadeiro"] is None and r["seq"] is None
        assert r["data_entrega"] == date.fromisoformat(r["arquivo"].split("/")[2])
        assert r["valor_sujo"] in {l[r["coluna"]] for l in por_arquivo[r["arquivo"]]}
        assert Decimal(r["valor_original"]) == _numero(r["valor_sujo"])
    tipos = {r["tipo"] for r in gabarito["defeitos"] if r["sistema"] == "apolice"}
    assert tipos == {"valor_monetario_texto", "cpf_fora_do_padrao"}
    assert max(r["data_entrega"] for r in registros) > CARGA_INICIAL, "o avancar_tempo acrescenta os registros"


@pytest.mark.lento
def test_gerar_em_partes_produz_os_mesmos_arquivos(gerado, principal_em_partes):
    _, _, completo, _, _ = gerado

    assert _arquivos(principal_em_partes.final) == completo
