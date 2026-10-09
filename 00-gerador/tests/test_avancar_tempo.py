"""Relógio simulado avançado por `avancar_tempo`, observado pela API, pela landing e pelo
gabarito.

Para poupar a cota, uma única carga inicial curta (até `CARGA_CURTA`) é copiada para dois
destinos: um avança 3 dias de uma vez e o outro 1 + 1 + 1. O pico de updates de tarifa do
*Embarque* em 12/01/2026 é verificado em `test_embarque.py`, que também avança o relógio.
"""

import json
import shutil
from datetime import date

import pyarrow.parquet as pq
import pytest

from modelbricks_gerador import DestinoLocal, avancar_tempo, gerar_fatia

ESCALA = 0.05  # a menor escala com rotas suficientes para a Viação (Embarque)
ARTIGO = "03-3fn"  # fatia com gabarito e com o Embarque
CARGA_CURTA = date(2024, 3, 31)  # carga curta: barateia o teste e deixa clientes por cadastrar
DIAS_NOVOS = {"2024-04-01", "2024-04-02", "2024-04-03"}


def _conteudo(pasta):
    return {p.relative_to(pasta).as_posix(): p.read_bytes() for p in pasta.rglob("*") if p.is_file()}


def _sem_relogio(conteudo):
    return {c: v for c, v in conteudo.items() if not c.startswith("_controle/")}


def _gabarito(destino):
    pasta = destino.raiz / "gabarito"
    return {p.name: pq.read_table(p).to_pylist() for p in sorted(pasta.iterdir()) if p.is_dir()}


def _sem_ordem(linhas):
    return sorted(linhas, key=lambda r: sorted((k, str(v)) for k, v in r.items()))


@pytest.fixture(scope="module")
def tres_dias(spark, tmp_path_factory):
    pasta = tmp_path_factory.mktemp("avancar")
    de_uma_vez = DestinoLocal(pasta / "uma_vez" / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, de_uma_vez, escala=ESCALA, ate=CARGA_CURTA, spark=spark)
    carga = _conteudo(de_uma_vez.volume)
    dia_a_dia = DestinoLocal(pasta / "dia_a_dia" / "medalhao_03_3fn")
    shutil.copytree(de_uma_vez.raiz, dia_a_dia.raiz)

    resultado = avancar_tempo(ARTIGO, de_uma_vez, 3, spark=spark)
    resultados = [avancar_tempo(ARTIGO, dia_a_dia, 1, spark=spark) for _ in range(3)]
    return carga, de_uma_vez, resultado, dia_a_dia, resultados


def test_avancar_cria_so_arquivos_novos_e_move_o_relogio(spark, tres_dias):
    carga, destino, resultado, *_ = tres_dias

    assert resultado.data_relogio == date(2024, 4, 3)
    depois = _conteudo(destino.volume)
    assert {c: depois[c] for c in carga} == carga
    novos = _sem_relogio({c: v for c, v in depois.items() if c not in carga})
    assert {c.split("/")[2] for c in novos} == DIAS_NOVOS
    assert resultado.arquivos == len(novos)
    for schema in ("bronze", "silver", "gold"):
        assert not any((destino.raiz / schema).rglob("*"))
    assert gerar_fatia(ARTIGO, destino, escala=ESCALA, spark=spark).data_relogio == date(2024, 4, 3)


def test_avancar_tres_dias_de_uma_vez_ou_um_por_vez_grava_o_mesmo_conteudo(tres_dias):
    _, de_uma_vez, resultado, dia_a_dia, resultados = tres_dias

    assert [r.data_relogio for r in resultados] == [date(2024, 4, 1), date(2024, 4, 2), date(2024, 4, 3)]
    assert sum(r.arquivos for r in resultados) == resultado.arquivos
    assert _sem_relogio(_conteudo(dia_a_dia.volume)) == _sem_relogio(_conteudo(de_uma_vez.volume))
    gabarito_uma_vez, gabarito_dia_a_dia = _gabarito(de_uma_vez), _gabarito(dia_a_dia)
    assert gabarito_uma_vez.keys() == gabarito_dia_a_dia.keys()
    for tabela in gabarito_uma_vez:
        assert _sem_ordem(gabarito_uma_vez[tabela]) == _sem_ordem(gabarito_dia_a_dia[tabela]), tabela


def test_mapa_de_chaves_cobre_as_chaves_entregues_pelo_avanco(tres_dias):
    carga, destino, *_ = tres_dias
    gabarito = _gabarito(destino)
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


def test_avancar_sem_carga_inicial_falha_com_orientacao(spark, destino):
    with pytest.raises(RuntimeError, match="gerar_fatia"):
        avancar_tempo("01-bronze", destino, 1, spark=spark)


def test_job_rejeita_dias_menor_que_um_antes_de_abrir_sessao():
    from modelbricks_gerador import cli

    with pytest.raises(SystemExit):
        cli.avancar_tempo(["--artigo", "01-bronze", "--dias", "0"])
