"""Relógio simulado avançado por `avancar_tempo`, observado pela API e pela landing."""

from datetime import date

import pytest

from modelbricks_gerador import avancar_tempo, gerar_fatia


def _conteudo(pasta):
    return {p.relative_to(pasta).as_posix(): p.read_bytes() for p in pasta.rglob("*") if p.is_file()}


def test_avancar_move_o_relogio_sem_alterar_arquivos_existentes(spark, destino):
    gerar_fatia("01-bronze", destino, spark=spark)
    antes = _conteudo(destino.raiz)

    resultado = avancar_tempo("01-bronze", destino, 3, spark=spark)

    assert resultado.data_relogio == date(2026, 1, 3)
    depois = _conteudo(destino.raiz)
    assert {k: depois[k] for k in antes} == antes
    for schema in ("bronze", "silver", "gold"):
        assert not any((destino.raiz / schema).rglob("*"))
    assert gerar_fatia("01-bronze", destino, spark=spark).data_relogio == date(2026, 1, 3)


def test_avancar_sem_carga_inicial_falha_com_orientacao(spark, destino):
    with pytest.raises(RuntimeError, match="gerar_fatia"):
        avancar_tempo("01-bronze", destino, 1, spark=spark)
