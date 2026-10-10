"""Expressões SQL comuns (`expressoes.py`): o texto que elas montam, sem Spark.

O comportamento no Spark (determinismo dos dados) é coberto pelos testes de cada sistema; aqui
só se trava a forma da SQL, que não pode mudar sem mudar os dados gerados.
"""

from modelbricks_gerador.expressoes import (
    instante,
    inteiro_e_fracao,
    sorteio,
    uniforme,
    uniforme_do_lote,
)
from modelbricks_gerador.roteiro import Intervalo


def test_uniforme_e_o_hash_das_colunas_em_uma_fracao():
    assert uniforme(123, "id") == "(pmod(xxhash64(123, id), 1000000) / 1000000.0)"
    assert uniforme_do_lote(5) == "(pmod(xxhash64(semente, n, 5), 1000000) / 1000000.0)"


def test_sorteio_acumula_os_pesos_em_ordem_alfabetica_e_o_ultimo_fecha():
    sql = sorteio({"site": 1, "guiche": 3}, "U")

    assert sql == "CASE WHEN U < 0.75 THEN 'guiche' ELSE 'site' END"


def test_sorteio_de_um_nome_so_devolve_o_literal_e_escapa_aspas_e_barras():
    assert sorteio({"D'Água": 1}, "U") == "'D\\'Água'"
    assert sorteio({"a\\b": 1}, "U") == "'a\\\\b'"


def test_instante_fica_na_faixa_de_horas_do_dia():
    sql = instante("data", Intervalo(minimo=8, maximo=18), "semente", "n", 3)

    assert sql == (
        "timestamp_seconds(unix_seconds(cast(data AS timestamp)) + 28800 "
        "+ pmod(xxhash64(semente, n, 3), 36000))"
    )


def test_inteiro_e_fracao_separa_a_taxa_por_dia():
    inteiro, fracao = inteiro_e_fracao(3.25)

    assert (inteiro, fracao) == (3, 0.25)
