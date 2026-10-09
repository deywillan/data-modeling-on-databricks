"""*Rastro* (telemetria da holding): muitos arquivos pequenos de JSON aninhado na landing.

Observado só pelo que `gerar_fatia`/`avancar_tempo` gravam. Usa a geração `principal`
(ver `geracoes.py`), compartilhada com os outros sistemas, cuja fatia de teste tem gabarito
e a janela de telemetria de 90 dias do Estudo de Caso.
"""

import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal, carregar_roteiro

ESCALA = geracoes.PRINCIPAL.escala  # 20 veículos na frota própria
CARGA_INICIAL = date(2025, 12, 31)
JANELA_DIAS = 90  # janela de telemetria da carga inicial (Estudo de Caso)
DIAS_AVANCADOS = 12  # cobre o maior atraso de evento do roteiro (3 dias)
ARQUIVO = re.compile(r"rastro/eventos/(\d{4}-\d{2}-\d{2})/eventos_(\d{8})_(\d{4})\.json")


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    return geracoes.arquivos(destino, "rastro")


def _eventos(arquivos: dict[str, bytes]) -> list[dict]:
    eventos = []
    for caminho, conteudo in arquivos.items():
        lote = date.fromisoformat(ARQUIVO.fullmatch(caminho)[1])
        for linha in conteudo.decode("utf-8").splitlines():
            eventos.append({**json.loads(linha), "_lote": lote, "_arquivo": caminho})
    return eventos


def _ts(evento: dict) -> datetime:
    return datetime.fromisoformat(evento["ts_evento"].replace("Z", "+00:00"))


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    return geracoes.gabarito(destino, "rastro")


@pytest.fixture(scope="module")
def gerado(principal):
    """Carga inicial e avanço de 12 dias (geração `principal`)."""
    roteiro = carregar_roteiro(principal.roteiro)
    assert roteiro.fatia(geracoes.PRINCIPAL.artigo).janela_telemetria_dias == JANELA_DIAS
    assert principal.resultados[-1].data_relogio == CARGA_INICIAL + timedelta(days=DIAS_AVANCADOS)
    destino = principal.final
    return destino, _arquivos(principal.etapas[0]), _arquivos(destino), _gabarito(destino), roteiro


def test_carga_inicial_traz_so_os_ultimos_90_dias_de_telemetria(gerado):
    _, carga, *_ = gerado
    eventos = _eventos(carga)
    primeiro_dia = CARGA_INICIAL - timedelta(days=JANELA_DIAS - 1)

    assert eventos
    assert min(_ts(e).date() for e in eventos) == primeiro_dia
    assert max(e["_lote"] for e in eventos) == CARGA_INICIAL
    assert all(ARQUIVO.fullmatch(c) for c in carga)
    assert {ARQUIVO.fullmatch(c)[1] for c in carga} == {
        (primeiro_dia + timedelta(days=i)).isoformat() for i in range(JANELA_DIAS)
    }


def test_evento_chega_como_json_aninhado(gerado):
    _, carga, *_ = gerado
    eventos = _eventos(carga)
    evento = eventos[0]

    assert set(evento) - {"_lote", "_arquivo"} == {
        "id_evento", "ts_evento", "dispositivo", "posicao", "telemetria", "alertas",
    }
    assert set(evento["dispositivo"]) == {"id_dispositivo", "placa", "tipo_veiculo", "empresa"}
    assert set(evento["posicao"]) == {"latitude", "longitude", "velocidade_kmh", "direcao_graus"}
    assert set(evento["telemetria"]) == {"odometro_km", "combustivel_pct", "temperatura_motor_c", "rpm"}
    alertas = [a for e in eventos for a in e["alertas"]]
    assert alertas and {a["tipo"] for a in alertas} == {"excesso_velocidade", "frenagem_brusca"}


def test_muitos_arquivos_pequenos_por_dia_tambem_no_avancar_tempo(gerado):
    _, carga, depois, _, roteiro = gerado
    r = roteiro.parametros.rastro
    janelas_em_operacao = (r.horario_operacao.maximo - r.horario_operacao.minimo) * r.arquivos_por_dia // 24
    por_dia = Counter(ARQUIVO.fullmatch(c)[1] for c in depois)
    novos = {c for c in depois if c not in carga}

    assert {c: depois[c] for c in carga} == carga  # a landing nunca muda
    assert {ARQUIVO.fullmatch(c)[1] for c in novos} == {
        (CARGA_INICIAL + timedelta(days=i)).isoformat() for i in range(1, DIAS_AVANCADOS + 1)
    }
    assert janelas_em_operacao > 1
    assert all(n >= janelas_em_operacao for n in por_dia.values())
    for caminho in depois:
        dia, dia_compacto, _ = ARQUIVO.fullmatch(caminho).groups()
        assert dia.replace("-", "") == dia_compacto


def test_telemetria_vem_dos_veiculos_da_frota_propria(gerado):
    _, _, depois, gabarito, _ = gerado
    veiculos = {v["id_veiculo"]: v for v in gabarito["veiculos"]}
    mapa = {
        int(m["chave_sistema"]): m["id_verdadeiro"]
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == "rastro" and m["coluna"] == "id_dispositivo"
    }
    frota = {p["id_entidade"] for p in gabarito["participacoes"] if p["papel"] == "frota"}
    eventos = _eventos(depois)

    assert {mapa[e["dispositivo"]["id_dispositivo"]] for e in eventos} == frota
    for e in eventos:
        veiculo = veiculos[mapa[e["dispositivo"]["id_dispositivo"]]]
        assert veiculo["frota_propria"]
        assert e["dispositivo"]["placa"] == veiculo["placa"]
        assert e["dispositivo"]["tipo_veiculo"] == veiculo["tipo"]
        assert e["dispositivo"]["empresa"] == veiculo["empresa_frota"]


def test_volume_da_janela_segue_o_roteiro_e_nao_a_escala(gerado):
    _, carga, depois, gabarito, roteiro = gerado
    r = roteiro.parametros.rastro
    eventos_por_dia = (r.horario_operacao.maximo - r.horario_operacao.minimo) * 60 // r.intervalo_eventos_minutos
    frota_na_escala = round(roteiro.volumes.veiculos_frota_propria * ESCALA)
    primeiro_dia = CARGA_INICIAL - timedelta(days=JANELA_DIAS - 1)
    vendas = [v["data_venda"] for v in gabarito["vendas_veiculos"] if v["tipo_comprador"] == "grupo"]
    # Cada veículo opera do dia seguinte à venda em diante.
    dias_em_operacao = sum(
        (CARGA_INICIAL - max(primeiro_dia, venda + timedelta(days=1))).days + 1
        for venda in vendas
        if venda < CARGA_INICIAL
    )
    ocorridos_na_janela = [e for e in _eventos(depois) if _ts(e).date() <= CARGA_INICIAL]

    assert len(vendas) == frota_na_escala
    assert len(ocorridos_na_janela) == dias_em_operacao * eventos_por_dia
    assert len(_eventos(carga)) <= frota_na_escala * eventos_por_dia * JANELA_DIAS


def test_eventos_atrasados_chegam_em_lote_posterior_e_ficam_no_gabarito(gerado):
    _, _, depois, gabarito, roteiro = gerado
    eventos = _eventos(depois)
    atrasados = {e["id_evento"]: e for e in eventos if _ts(e).date() < e["_lote"]}
    registrados = [d for d in gabarito["defeitos"] if d["sistema"] == "rastro"]
    taxa = roteiro.defeitos["evento_atrasado"].taxas["rastro"]

    assert len({e["id_evento"] for e in eventos}) == len(eventos)  # nenhum evento repetido
    assert 0.5 * taxa < len(atrasados) / len(eventos) < 1.5 * taxa
    assert {d["tipo"] for d in registrados} == {"evento_atrasado"}
    assert {d["chave_sistema"]: d["data_entrega"] for d in registrados} == {
        i: e["_lote"] for i, e in atrasados.items()
    }
    assert all(1 <= (e["_lote"] - _ts(e).date()).days <= 3 for e in atrasados.values())
