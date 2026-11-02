"""*Pátio* (DMS da ModelBricks Veículos): snapshots completos em CSV na landing.

Observado só pelo que `gerar_carga_inicial`/`avancar_tempo` gravam: os arquivos da landing e as
tabelas do schema `gabarito`. Para medir as taxas dos defeitos com folga estatística, o
módulo usa a geração `patio` (ver `geracoes.py`): uma carga só do *Pátio* (com gabarito)
na escala 0,2, até 03/01/2026, com a taxa de município fora do padrão mais alta que a do
Estudo de Caso. O teste lento compara com a `patio_em_partes`, cujo `avancar_tempo`
atravessa o fim do período de fotos diárias fechado em 31/12/2025.
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

CARGA_INICIAL = date(2025, 12, 31)
ULTIMO_DIA = date(2026, 1, 3)
ENTIDADES = {
    "lojas": "id_loja",
    "clientes": "id_cliente",
    "veiculos": "id_veiculo",
    "vendas": "id_venda",
    "ordens_servico": "id_ordem_servico",
}


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    return geracoes.arquivos(destino, "patio")


def _fotos(arquivos: dict[str, bytes]) -> dict[date, dict[str, dict[str, dict]]]:
    """data → entidade → chave → linha (dicionário de textos, como no CSV)."""
    fotos: dict[date, dict[str, dict[str, dict]]] = defaultdict(dict)
    for caminho, conteudo in arquivos.items():
        _, entidade, data_foto, _ = caminho.split("/")
        linhas = list(csv.DictReader(io.StringIO(conteudo.decode("utf-8"))))
        chave = ENTIDADES[entidade]
        por_chave = {linha[chave]: linha for linha in linhas}
        assert len(por_chave) == len(linhas), f"chave repetida em {caminho}"
        fotos[date.fromisoformat(data_foto)][entidade] = por_chave
    return dict(sorted(fotos.items()))


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    return geracoes.gabarito(destino)


def _mapa(gabarito, entidade: str, tabela: str) -> dict[int, str]:
    return {
        m["id_verdadeiro"]: m["chave_sistema"]
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == "patio" and m["entidade"] == entidade and m["tabela_verdadeira"] == tabela
    }


@pytest.fixture(scope="module")
def gerado(patio):
    """Carga só do Pátio até 03/01/2026 (geração `patio`)."""
    assert patio.resultados[-1].data_relogio == ULTIMO_DIA
    destino = patio.final
    return destino, patio.roteiro, _arquivos(destino), _gabarito(destino)


def _datas_esperadas(fim: date) -> list[date]:
    """Fim de cada mês de jan/2024 a nov/2025, todos os dias de dez/2025 e os seguintes."""
    datas, d = [], date(2024, 1, 1)
    while d <= fim:
        fim_do_mes = (d + timedelta(days=1)).month != d.month
        if d >= date(2025, 12, 1) or fim_do_mes:
            datas.append(d)
        d += timedelta(days=1)
    return datas


def test_uma_pasta_por_data_de_foto_com_um_csv_por_entidade_na_frequencia_decidida(gerado):
    _, _, arquivos, _ = gerado
    padrao = re.compile(r"patio/([a-z_]+)/(\d{4}-\d{2}-\d{2})/\1_(\d{8})_0001\.csv")
    fim = ULTIMO_DIA

    datas = defaultdict(set)
    for caminho in arquivos:
        casamento = padrao.fullmatch(caminho)
        assert casamento, caminho
        assert casamento[1] in ENTIDADES
        assert casamento[3] == casamento[2].replace("-", "")
        datas[casamento[1]].add(date.fromisoformat(casamento[2]))
    for entidade in ENTIDADES:
        assert sorted(datas[entidade]) == _datas_esperadas(fim), entidade

    for caminho, conteudo in arquivos.items():
        entidade = caminho.split("/")[1]
        cabecalho = conteudo.decode("utf-8").splitlines()[0].split(",")
        assert cabecalho[0] == ENTIDADES[entidade], "a chave é a primeira coluna"


def test_cada_foto_e_completa_com_todas_as_vendas_e_clientes_vigentes(gerado):
    _, _, arquivos, gabarito = gerado
    fotos = _fotos(arquivos)
    venda_por_id = {v["id_venda"]: v for v in gabarito["vendas_veiculos"]}
    id_venda = _mapa(gabarito, "vendas", "vendas_veiculos")
    id_veiculo = _mapa(gabarito, "veiculos", "veiculos")
    clientes_pf = _mapa(gabarito, "clientes", "pessoas")
    clientes_pj = _mapa(gabarito, "clientes", "empresas_pj")
    assert set(id_venda) == set(venda_por_id)

    for data_foto, foto in fotos.items():
        vendidas = {k for k, v in venda_por_id.items() if v["data_venda"] <= data_foto}
        assert set(foto["vendas"]) == {id_venda[k] for k in vendidas}, data_foto
        assert {id_veiculo[venda_por_id[k]["id_veiculo"]] for k in vendidas} <= set(foto["veiculos"])
        for k in vendidas:
            venda = foto["vendas"][id_venda[k]]
            assert venda["id_veiculo"] == id_veiculo[venda_por_id[k]["id_veiculo"]]
            assert venda["data_venda"] == venda_por_id[k]["data_venda"].isoformat()
            assert foto["veiculos"][venda["id_veiculo"]]["status"] == "vendido"
            comprador = venda_por_id[k]
            if comprador["tipo_comprador"] == "pf":
                assert venda["id_cliente"] == clientes_pf[comprador["id_pessoa"]]
            elif comprador["tipo_comprador"] == "pj":
                assert venda["id_cliente"] == clientes_pj[comprador["id_empresa"]]
            assert venda["id_cliente"] in foto["clientes"], "o comprador está cadastrado na foto"

    ultima = fotos[max(fotos)]
    clientes_veiculos = {
        (p["tipo_entidade"], p["id_entidade"])
        for p in gabarito["participacoes"]
        if p["papel"] == "cliente" and p["empresa"] == "mb_veiculos"
    }
    assert {("pessoa", k) for k in clientes_pf} | {("empresa_pj", k) for k in clientes_pj} == clientes_veiculos
    assert set(clientes_pf.values()) | set(clientes_pj.values()) <= set(ultima["clientes"])
    assert len(ultima["vendas"]) == len(venda_por_id), "todas as vendas da história estão na última foto"


def test_veiculos_da_frota_propria_estao_no_dms_vendidos_as_empresas_do_grupo(gerado):
    _, roteiro, arquivos, gabarito = gerado
    ultima = _fotos(arquivos)[max(_fotos(arquivos))]
    nomes_grupo = carregar_roteiro(roteiro).parametros.patio.clientes_grupo
    id_veiculo = _mapa(gabarito, "veiculos", "veiculos")
    frota = [v for v in gabarito["veiculos"] if v["frota_propria"]]
    assert frota

    venda_do_veiculo = {v["id_veiculo"]: v for v in ultima["vendas"].values()}
    for veiculo in frota:
        chave = id_veiculo[veiculo["id_veiculo"]]
        registro = ultima["veiculos"][chave]
        assert (registro["placa"], registro["marca"], registro["modelo"]) == (
            veiculo["placa"], veiculo["marca"], veiculo["modelo"])
        comprador = ultima["clientes"][venda_do_veiculo[chave]["id_cliente"]]
        assert comprador["nome"] == nomes_grupo[veiculo["empresa_frota"]]
        assert comprador["tipo_pessoa"] == "J"


IMUTAVEIS = {
    "lojas": None,
    "clientes": {"id_cliente", "tipo_pessoa", "nome", "documento", "data_nascimento", "data_cadastro"},
    "veiculos": {"id_veiculo", "placa", "marca", "modelo", "tipo", "ano_fabricacao", "data_entrada"},
    "vendas": None,
    "ordens_servico": {"id_ordem_servico", "id_veiculo", "id_cliente", "tipo_servico", "data_abertura"},
}
DATA_DE_CRIACAO = {"clientes": "data_cadastro", "vendas": "data_venda", "ordens_servico": "data_abertura", "veiculos": "data_entrada"}


def test_diferencas_entre_fotos_consecutivas_sao_as_mudancas_geradas(gerado):
    _, _, arquivos, _ = gerado
    fotos = _fotos(arquivos)
    datas = list(fotos)
    mudancas = defaultdict(set)

    for anterior, atual in zip(datas, datas[1:]):
        for entidade, imutaveis in IMUTAVEIS.items():
            antes, depois = fotos[anterior][entidade], fotos[atual][entidade]
            excluidas = set(antes) - set(depois)
            if excluidas:
                assert entidade == "ordens_servico", f"{entidade}: linha some da foto de {atual}"
                assert all(antes[k]["status"] == "aberta" for k in excluidas), "só a OS aberta é cancelada"
                mudancas[entidade, "D"] |= excluidas
            for k in set(depois) - set(antes):
                criacao = DATA_DE_CRIACAO.get(entidade)
                if criacao:
                    assert anterior < date.fromisoformat(depois[k][criacao]) <= atual, (entidade, k)
                mudancas[entidade, "I"].add(k)
            for k in set(antes) & set(depois):
                if antes[k] != depois[k]:
                    alteradas = {c for c in antes[k] if antes[k][c] != depois[k][c]}
                    assert imutaveis is not None, f"{entidade} {k} mudou em {atual}"
                    assert not alteradas & imutaveis, (entidade, k, alteradas)
                    mudancas[entidade, "U"].add(k)
                    if entidade == "veiculos":
                        assert (antes[k]["status"], depois[k]["status"]) == ("em_estoque", "vendido")
                    if entidade == "ordens_servico":
                        assert (antes[k]["status"], depois[k]["status"]) == ("aberta", "concluida")
                        assert date.fromisoformat(depois[k]["data_conclusao"]) <= atual
    for esperado in [("clientes", "U"), ("veiculos", "U"), ("ordens_servico", "U"), ("ordens_servico", "D"),
                     ("vendas", "I"), ("ordens_servico", "I")]:
        assert mudancas[esperado], f"nenhuma mudança {esperado} entre as fotos"
    depois_da_carga = [d for d in datas if d > CARGA_INICIAL]
    assert any(fotos[d]["ordens_servico"] != fotos[d - timedelta(days=1)]["ordens_servico"] for d in depois_da_carga), (
        "as fotos do período sem fim (o do avancar_tempo) trazem mudanças da oficina"
    )


@pytest.mark.lento
def test_gerar_em_partes_produz_as_mesmas_fotos_tambem_no_avancar_tempo(gerado, patio_em_partes):
    """Carga até 30/12/2025 e avanço de 4 dias: o avanço entrega a última foto do período
    diário fechado e as do período sem fim, as mesmas da geração de uma vez."""
    _, _, completo, _ = gerado
    carga, depois = (_arquivos(etapa) for etapa in patio_em_partes.etapas)
    assert patio_em_partes.resultados[-1].data_relogio == ULTIMO_DIA

    assert {date.fromisoformat(c.split("/")[2]) for c in carga} == set(_datas_esperadas(date(2025, 12, 30)))
    assert {c: v for c, v in depois.items() if c in carga} == carga, "o avanço não muda a carga inicial"
    novas = {c.split("/")[2] for c in depois if c not in carga}
    assert novas == {"2025-12-31", "2026-01-01", "2026-01-02", "2026-01-03"}
    assert depois == completo


def _municipio_valido(valor: str, codigos: set[int]) -> bool:
    return bool(re.fullmatch(r"\d{7}", valor)) and int(valor) in codigos


def _dentro_da_tolerancia(observados: int, populacao: int, taxa: float) -> bool:
    desvio = math.sqrt(taxa * (1 - taxa) / populacao)
    return abs(observados / populacao - taxa) <= 4 * desvio + 1 / populacao


def test_defeitos_do_patio_aparecem_na_taxa_do_roteiro_e_estao_registrados(gerado):
    _, roteiro, arquivos, gabarito = gerado
    fotos = _fotos(arquivos)
    codigos = {m["cod_municipio"] for m in gabarito["municipios"]}
    taxas = {tipo: d.taxas.get("patio") for tipo, d in carregar_roteiro(roteiro).defeitos.items()}

    observados = defaultdict(set)
    for foto in fotos.values():
        for k, cliente in foto["clientes"].items():
            if not _municipio_valido(cliente["cod_municipio"], codigos):
                observados["municipio_fora_do_padrao"].add((k, "cod_municipio", cliente["cod_municipio"]))
            for coluna in ("email", "telefone"):
                if cliente[coluna] == "":
                    observados["contato_nulo"].add((k, coluna, None))
    registrados = defaultdict(set)
    for r in gabarito["defeitos"]:
        assert (r["sistema"], r["entidade"], r["granularidade"]) == ("patio", "clientes", "linha")
        assert r["data_entrega"] in fotos
        registrados[r["tipo"]].add((r["chave_sistema"], r["coluna"], r["valor_sujo"]))
    assert set(registrados) == {t for t, taxa in taxas.items() if taxa is not None}
    for tipo, ocorrencias in observados.items():
        assert ocorrencias == registrados[tipo], f"{tipo}: landing e gabarito divergem"

    ultima = fotos[max(fotos)]["clientes"]
    populacao = len(ultima)
    assert populacao > 300
    sujos = {
        "municipio_fora_do_padrao": {k for k, c in ultima.items() if not _municipio_valido(c["cod_municipio"], codigos)},
        "contato_nulo": {k for k, c in ultima.items() if c["email"] == "" or c["telefone"] == ""},
    }
    for tipo, chaves in sujos.items():
        assert _dentro_da_tolerancia(len(chaves), populacao, taxas[tipo]), f"{tipo}: {len(chaves)}/{populacao}"

    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    for r in gabarito["defeitos"]:
        if r["tipo"] == "municipio_fora_do_padrao" and r["tabela_verdadeira"] == "pessoas":
            assert r["valor_original"] and _municipio_valido(r["valor_original"], codigos)
            assert pessoas[r["id_verdadeiro"]]["cod_municipio"] in codigos
