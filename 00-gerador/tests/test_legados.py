"""Legados do Expresso Vale do Sol (*SolTicket* e *SolFin*) e a aquisição de 01/07/2025.

Observado só pelo que `gerar_fatia`/`avancar_tempo` gravam: os arquivos da landing e as
tabelas do schema `gabarito`. O módulo usa a geração `legados` (ver `geracoes.py`): uma
fatia dos legados junto com o *Embarque* (para a sobreposição de clientes), com gabarito,
gerada até a segunda-feira 05/01/2026, com taxas mais altas que as do Estudo de Caso nos
defeitos raros do *SolTicket*, para medi-los com folga estatística. O teste lento compara
com a `legados_em_partes`, gerada até a véspera da aquisição e avançada uma semana.
"""

import csv
import io
import math
import re
from collections import defaultdict
from datetime import date, timedelta

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal, carregar_roteiro

ESCALA = geracoes.LEGADOS.escala
AQUISICAO = date(2025, 7, 1)
ULTIMO_DIA = date(2026, 1, 5)  # segunda-feira: a primeira foto semanal depois da virada do ano
ENTIDADES = {
    "solticket": {"clientes": "cod_cliente", "linhas": "cod_linha", "passagens": "num_bilhete"},
    "solfin": {"fornecedores": "cod_fornecedor", "titulos_pagar": "num_titulo"},
}


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    return {
        p.relative_to(destino.volume).as_posix(): p.read_bytes()
        for sistema in ENTIDADES
        for p in sorted((destino.volume / sistema).rglob("*"))
        if p.is_file()
    }


def _fotos(arquivos: dict[str, bytes], sistema: str) -> dict[date, dict[str, dict[str, dict]]]:
    """data → entidade → chave → linha (dicionário de textos, como no CSV)."""
    fotos: dict[date, dict[str, dict[str, dict]]] = defaultdict(dict)
    for caminho, conteudo in arquivos.items():
        sistema_, entidade, data_foto, _ = caminho.split("/")
        if sistema_ != sistema:
            continue
        linhas = list(csv.DictReader(io.StringIO(conteudo.decode("latin-1")), delimiter=";"))
        chave = ENTIDADES[sistema][entidade]
        por_chave = {linha[chave]: linha for linha in linhas}
        assert len(por_chave) == len(linhas), f"chave repetida em {caminho}"
        fotos[date.fromisoformat(data_foto)][entidade] = por_chave
    return dict(sorted(fotos.items()))


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    return geracoes.gabarito(destino)


def _mapa(gabarito, sistema: str, entidade: str) -> dict[int, str]:
    return {
        m["id_verdadeiro"]: m["chave_sistema"]
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == sistema and m["entidade"] == entidade
    }


def _datas_esperadas(fim: date) -> list[date]:
    """A data da aquisição e, depois dela, toda segunda-feira."""
    datas, d = [AQUISICAO], AQUISICAO + timedelta(days=1)
    while d <= fim:
        if d.weekday() == 0:
            datas.append(d)
        d += timedelta(days=1)
    return datas


@pytest.fixture(scope="module")
def gerado(legados):
    """Fatia de Embarque + legados até a segunda-feira 05/01/2026 (geração `legados`)."""
    assert legados.resultados[-1].data_relogio == ULTIMO_DIA
    destino = legados.final
    return destino, legados.roteiro, _arquivos(destino), _gabarito(destino)


def test_nada_antes_da_aquisicao_e_depois_uma_foto_por_semana(gerado):
    _, _, arquivos, _ = gerado
    fim = ULTIMO_DIA
    padrao = re.compile(r"(solticket|solfin)/([a-z_]+)/(\d{4}-\d{2}-\d{2})/\2_(\d{8})_0001\.csv")

    datas = defaultdict(set)
    for caminho in arquivos:
        casamento = padrao.fullmatch(caminho)
        assert casamento, caminho
        assert casamento[2] in ENTIDADES[casamento[1]]
        assert casamento[4] == casamento[3].replace("-", "")
        datas[casamento[1], casamento[2]].add(date.fromisoformat(casamento[3]))
    for sistema, entidades in ENTIDADES.items():
        for entidade in entidades:
            assert sorted(datas[sistema, entidade]) == _datas_esperadas(fim), (sistema, entidade)
    assert ULTIMO_DIA in datas["solticket", "passagens"]


def test_a_carga_historica_inteira_chega_no_dia_da_aquisicao(gerado):
    _, roteiro, arquivos, _ = gerado
    historica = _fotos(arquivos, "solticket")[AQUISICAO]["passagens"]
    vendas = sorted(date.fromisoformat(p["data_hora_venda"][:10]) for p in historica.values())
    assert vendas[0] < date(2024, 1, 15), "a carga histórica começa no início da história"
    assert vendas[-1] <= AQUISICAO
    esperado = carregar_roteiro(roteiro).volumes.passagens_solticket * ESCALA
    assert 0.8 * esperado <= len(historica) <= 1.2 * esperado, len(historica)

    titulos = _fotos(arquivos, "solfin")[AQUISICAO]["titulos_pagar"]
    esperado = carregar_roteiro(roteiro).parametros.solfin.titulos_pagar * ESCALA
    assert 0.8 * esperado <= len(titulos) <= 1.2 * esperado, len(titulos)
    assert min(t["data_emissao"] for t in titulos.values()) < "2024-01-15"


def test_os_arquivos_sao_csv_ponto_e_virgula_em_latin1(gerado):
    _, _, arquivos, _ = gerado
    com_acento = set()
    for caminho, conteudo in arquivos.items():
        sistema, entidade = caminho.split("/")[:2]
        texto = conteudo.decode("latin-1")
        assert texto.splitlines()[0].split(";")[0] == ENTIDADES[sistema][entidade], "a chave é a primeira coluna"
        if not texto.isascii():
            com_acento.add((sistema, entidade))
            with pytest.raises(UnicodeDecodeError):
                conteudo.decode("utf-8")
    assert {("solticket", "clientes"), ("solticket", "linhas"), ("solfin", "fornecedores"),
            ("solfin", "titulos_pagar")} <= com_acento


def test_latin1_registrado_por_arquivo_no_gabarito(gerado):
    _, _, arquivos, gabarito = gerado
    registros = [r for r in gabarito["defeitos"] if r["tipo"] == "encoding_latin1"]
    assert all(r["granularidade"] == "arquivo" for r in registros)
    assert all(r["data_entrega"].isoformat() == r["arquivo"].split("/")[2] for r in registros)
    assert all((r["sistema"], r["entidade"]) == tuple(r["arquivo"].split("/")[:2]) for r in registros)
    assert sorted(r["arquivo"] for r in registros) == sorted(arquivos), "um registro por arquivo entregue"


def test_fotos_semanais_completas_com_as_mudancas_do_periodo(gerado):
    _, _, arquivos, _ = gerado
    for sistema, entidade, situacao, transicoes, criacao in (
        ("solticket", "passagens", "situacao", {("emitida", "cancelada"), ("emitida", "utilizada")}, "data_hora_venda"),
        ("solfin", "titulos_pagar", "situacao", {("aberto", "pago")}, "data_emissao"),
    ):
        fotos = _fotos(arquivos, sistema)
        datas = list(fotos)
        vistas = set()
        for anterior, atual in zip(datas, datas[1:]):
            antes, depois = fotos[anterior][entidade], fotos[atual][entidade]
            assert set(antes) <= set(depois), f"{sistema}.{entidade}: linha some da foto de {atual}"
            for k in set(depois) - set(antes):
                assert anterior < date.fromisoformat(depois[k][criacao][:10]) <= atual, (entidade, k)
            for k in set(antes):
                if antes[k] != depois[k]:
                    alteradas = {c for c in antes[k] if antes[k][c] != depois[k][c]}
                    assert alteradas <= {situacao, "data_pagamento"}, (entidade, k, alteradas)
                    vistas.add((antes[k][situacao], depois[k][situacao]))
        assert vistas == transicoes, (sistema, vistas)


def test_clientes_do_vale_do_sol_se_sobrepoem_aos_do_embarque_com_outra_chave(gerado):
    _, _, arquivos, gabarito = gerado
    solticket = _mapa(gabarito, "solticket", "clientes")
    embarque = _mapa(gabarito, "embarque", "clientes")
    clientes_evs = {
        p["id_entidade"] for p in gabarito["participacoes"]
        if p["papel"] == "cliente" and p["empresa"] == "evs" and p["tipo_entidade"] == "pessoa"
    }
    assert set(solticket) == clientes_evs
    comuns = set(solticket) & set(embarque)
    assert comuns, "há pessoas clientes das duas viações"
    assert all(solticket[k] != embarque[k] for k in comuns), "as chaves dos sistemas diferem"

    ultima = _fotos(arquivos, "solticket")[max(_fotos(arquivos, "solticket"))]["clientes"]
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    grafias = [
        k for k in comuns
        if (ultima[solticket[k]]["cpf"], ultima[solticket[k]]["nome"]) != (pessoas[k]["cpf"], pessoas[k]["nome"])
    ]
    assert grafias, "parte dos clientes em comum aparece com outra grafia no SolTicket"
    assert set(_mapa(gabarito, "solticket", "linhas")) == {
        r["id_rota"] for r in gabarito["rotas"] if r["empresa"] == "evs"}
    assert set(_mapa(gabarito, "solfin", "fornecedores")) <= {e["id_empresa"] for e in gabarito["empresas_pj"]}


def _dentro_da_tolerancia(observados: int, populacao: int, taxa: float) -> bool:
    desvio = math.sqrt(taxa * (1 - taxa) / populacao)
    return abs(observados / populacao - taxa) <= 4 * desvio + 1 / populacao


def test_defeitos_do_solticket_aparecem_na_taxa_do_roteiro_e_estao_registrados(gerado):
    _, roteiro, arquivos, gabarito = gerado
    defeitos = carregar_roteiro(roteiro).defeitos
    taxas = {tipo: d.taxas.get("solticket") for tipo, d in defeitos.items()}
    fotos = _fotos(arquivos, "solticket")
    ultima = fotos[max(fotos)]
    codigos = {m["cod_municipio"] for m in gabarito["municipios"]}
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    registrados = defaultdict(set)
    for r in gabarito["defeitos"]:
        if r["sistema"] != "solticket" or r["granularidade"] != "linha":
            continue
        assert list(r["artigos"]) == defeitos[r["tipo"]].artigos
        assert r["data_entrega"] in fotos
        if r["tipo"] == "pessoa_duplicada_no_sistema":  # chave da cópia; a original em valor_original
            assert r["chave_sistema"] == r["valor_sujo"]
            registrados[r["tipo"]].add((r["valor_original"], r["valor_sujo"]))
        else:
            registrados[r["tipo"]].add((r["chave_sistema"], r["valor_sujo"]))
    assert set(registrados) == {t for t, taxa in taxas.items() if isinstance(taxa, float)}

    # Cada cliente da landing é uma pessoa do mundo, pela chave original ou pela da cópia.
    original_da_copia = {sujo: orig for orig, sujo in registrados["pessoa_duplicada_no_sistema"]}
    id_da_chave = {v: k for k, v in _mapa(gabarito, "solticket", "clientes").items()}
    clientes = ultima["clientes"]
    assert set(original_da_copia) <= set(clientes)
    observados = defaultdict(set)
    for chave, cliente in clientes.items():
        pessoa = pessoas[id_da_chave[original_da_copia.get(chave, chave)]]
        if cliente["cpf"] != pessoa["cpf"]:
            observados["cpf_fora_do_padrao"].add((chave, cliente["cpf"]))
        if cliente["nome"] != pessoa["nome"]:
            observados["nome_abreviado_ou_com_erro"].add((chave, cliente["nome"]))
        valido = re.fullmatch(r"\d{7}", cliente["cod_municipio"]) and int(cliente["cod_municipio"]) in codigos
        if not valido:
            observados["municipio_fora_do_padrao"].add((chave, cliente["cod_municipio"]))
    linhas_validas = set(ultima["linhas"])
    observados["fk_orfa"] = {(k, p["cod_linha"]) for k, p in ultima["passagens"].items() if p["cod_linha"] not in linhas_validas}
    observados["pessoa_duplicada_no_sistema"] = {(original_da_copia[k], k) for k in original_da_copia}
    for tipo, ocorrencias in observados.items():
        assert ocorrencias == registrados[tipo], f"{tipo}: landing e gabarito divergem"

    originais = [k for k in clientes if k not in original_da_copia]
    populacoes = {
        "cpf_fora_do_padrao": len(clientes), "nome_abreviado_ou_com_erro": len(clientes),
        "municipio_fora_do_padrao": len(clientes), "pessoa_duplicada_no_sistema": len(originais),
        "fk_orfa": len(ultima["passagens"]),
    }
    assert len(originais) > 200
    for tipo, populacao in populacoes.items():
        assert _dentro_da_tolerancia(len(observados[tipo]), populacao, taxas[tipo]), (
            f"{tipo}: {len(observados[tipo])}/{populacao}")


@pytest.mark.lento
def test_gerar_em_partes_produz_as_mesmas_fotos_e_nada_antes_da_aquisicao(gerado, legados_em_partes):
    """Carga até a véspera da aquisição (nada dos legados) e avanço de uma semana: o
    `avancar_tempo` entrega a carga histórica e a foto semanal seguinte."""
    _, _, completo, _ = gerado
    vespera, uma_semana = legados_em_partes.etapas
    assert legados_em_partes.resultados[0].data_relogio == AQUISICAO - timedelta(days=1)
    assert _arquivos(vespera) == {}, "nada dos legados antes da aquisição"

    assert _arquivos(uma_semana) == {c: v for c, v in completo.items() if c.split("/")[2] <= "2025-07-07"}
    assert {c.split("/")[2] for c in _arquivos(uma_semana)} == {"2025-07-01", "2025-07-07"}
