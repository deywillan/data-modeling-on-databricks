"""Relógio simulado avançado por `avancar_tempo`, observado pela API, pela landing e pelo
gabarito.

Usa a geração `principal` (carga inicial até 31/12/2025 e 12 dias avançados de uma vez) e,
nos testes lentos, a `principal_em_partes` (carga até 09/01/2026, um avanço de 1 dia e outro de 2),
que deve chegar ao mesmo conteúdo. O pico de updates de tarifa do *Embarque* em 12/01/2026
é verificado em `test_embarque.py`.
"""

import json
import shutil
from datetime import date, timedelta

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal, avancar_tempo, gerar_fatia

CARGA_INICIAL = date(2025, 12, 31)
ULTIMO_DIA = date(2026, 1, 12)
DIAS_NOVOS = {(CARGA_INICIAL + timedelta(days=i)).isoformat() for i in range(1, 13)}
SISTEMAS_EM_PARTES = geracoes.FATIAS_DE_TESTE["94-partes"]["sistemas"]


def _conteudo(pasta):
    return {p.relative_to(pasta).as_posix(): p.read_bytes() for p in pasta.rglob("*") if p.is_file()}


def _sem_relogio(conteudo):
    return {c: v for c, v in conteudo.items() if not c.startswith("_controle/")}


def _dos_sistemas(conteudo, sistemas):
    return {c: v for c, v in conteudo.items() if c.split("/")[0] in sistemas}


def _sem_ordem(linhas):
    return sorted(linhas, key=lambda r: sorted((k, str(v)) for k, v in r.items()))


def test_avancar_cria_so_arquivos_novos_e_move_o_relogio(principal, tmp_path):
    carga = _conteudo(principal.etapas[0].volume)
    destino, resultado = principal.final, principal.resultados[-1]

    assert resultado.data_relogio == ULTIMO_DIA
    depois = _conteudo(destino.volume)
    assert {c: depois[c] for c in carga} == carga
    novos = _sem_relogio({c: v for c, v in depois.items() if c not in carga})
    assert {c.split("/")[2] for c in novos} == DIAS_NOVOS
    assert resultado.arquivos == len(novos)
    for schema in ("bronze", "silver", "gold"):
        assert not any((destino.raiz / schema).rglob("*"))

    # Uma nova carga inicial no mesmo destino não muda nada (cópia: a geração pode estar no cache).
    copia = DestinoLocal(tmp_path / destino.raiz.name)
    shutil.copytree(destino.raiz, copia.raiz)
    de_novo = gerar_fatia("90-sistemas", copia, escala=0.05, roteiro=principal.roteiro)
    assert (de_novo.gerou, de_novo.data_relogio) == (False, ULTIMO_DIA)
    assert _conteudo(copia.raiz) == _conteudo(destino.raiz)


@pytest.mark.lento
def test_avancar_de_uma_vez_ou_em_mais_de_uma_chamada_grava_o_mesmo_conteudo(principal, principal_em_partes):
    de_uma_vez, em_partes = principal.final, principal_em_partes.final
    resultados = principal_em_partes.resultados[1:]

    assert [r.data_relogio for r in resultados] == [date(2026, 1, 10), date(2026, 1, 12)]
    novos_de_uma_vez = [
        c for c in _dos_sistemas(_sem_relogio(_conteudo(de_uma_vez.volume)), SISTEMAS_EM_PARTES)
        if c.split("/")[2] >= "2026-01-10"
    ]
    assert sum(r.arquivos for r in resultados) == len(novos_de_uma_vez)
    assert _dos_sistemas(_sem_relogio(_conteudo(em_partes.volume)), SISTEMAS_EM_PARTES) == _dos_sistemas(
        _sem_relogio(_conteudo(de_uma_vez.volume)), SISTEMAS_EM_PARTES
    )
    gabarito_uma_vez, gabarito_em_partes = geracoes.gabarito(de_uma_vez), geracoes.gabarito(em_partes)
    for nome in ("mapa_chaves", "defeitos"):  # a geração em partes não tem o Rastro
        gabarito_uma_vez[nome] = [r for r in gabarito_uma_vez[nome] if r["sistema"] in SISTEMAS_EM_PARTES]
    assert gabarito_uma_vez.keys() == gabarito_em_partes.keys()
    for tabela in gabarito_uma_vez:
        assert _sem_ordem(gabarito_uma_vez[tabela]) == _sem_ordem(gabarito_em_partes[tabela]), tabela


def test_mapa_de_chaves_cobre_as_chaves_entregues_pelo_avanco(principal):
    carga = _conteudo(principal.etapas[0].volume)
    destino = principal.final
    gabarito = geracoes.gabarito(destino)
    mapa = {(m["sistema"], m["coluna"], m["chave_sistema"]) for m in gabarito["mapa_chaves"]}
    # Chaves que um defeito inventou de propósito (cópia duplicada, FK órfã) não têm mapa.
    sujas = {r["valor_sujo"] for r in gabarito["defeitos"] if r["tipo"] in ("pessoa_duplicada_no_sistema", "fk_orfa")}
    novos = [v for c, v in _conteudo(destino.volume).items() if c not in carga and c.startswith("embarque/")]
    entregues = {
        ("embarque", coluna, str(linha[coluna]))
        for conteudo in novos
        for linha in map(json.loads, conteudo.decode().splitlines())
        for coluna in ("id_cliente", "id_tarifa", "cod_linha")
        if linha.get(coluna) is not None and str(linha[coluna]) not in sujas
    }

    assert {coluna for _, coluna, _ in entregues} >= {"id_cliente", "cod_linha"}
    assert entregues <= mapa


def test_avancar_sem_carga_inicial_falha_com_orientacao(destino):
    # A falha vem antes de abrir a sessão Spark: não consome cota.
    with pytest.raises(RuntimeError, match="gerar_fatia"):
        avancar_tempo("01-bronze", destino, 1)


def test_job_rejeita_dias_menor_que_um_antes_de_abrir_sessao():
    from modelbricks_gerador import cli

    with pytest.raises(SystemExit):
        cli.avancar_tempo(["--artigo", "01-bronze", "--dias", "0"])
