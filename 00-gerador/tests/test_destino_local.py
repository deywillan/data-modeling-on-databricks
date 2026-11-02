"""Onde o destino grava: os três catálogos da série e um schema por sistema, sem Spark.

O `DestinoLocal` espelha em pastas a estrutura do Unity Catalog:

    <raiz>/bronze_<amb>/<sistema>/landing/<entidade>/<data>/<arquivo>
    <raiz>/bronze_<amb>/_controle/relogio/<data>.json
    <raiz>/silver_<amb>/gabarito/<tabela>/parte-NNNN.parquet
    <raiz>/gold_<amb>/

Os sistemas são falsos (como em `test_recuperacao.py`), então nada abre sessão no workspace.
"""

from datetime import timedelta

import pytest
from falsos import CARGA, SPARK_FALSO, roteiro_falso as _roteiro

from modelbricks_gerador import DestinoLocal, avancar_tempo, gerar_carga_inicial
from modelbricks_gerador.escritor import Arquivo
from modelbricks_gerador.relogio import Relogio, gravar_relogio, ler_relogio


def _tudo(raiz) -> set[str]:
    return {p.relative_to(raiz).as_posix() for p in raiz.rglob("*")}


def test_a_carga_inicial_cria_os_tres_catalogos_da_serie_com_um_schema_por_sistema(tmp_path, sistemas_falsos):
    destino = DestinoLocal(tmp_path / "ws", "t1")

    gerar_carga_inicial(destino, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())

    assert {p.name for p in destino.raiz.iterdir()} == {"bronze_t1", "silver_t1", "gold_t1"}
    bronze = destino.raiz / "bronze_t1"
    assert {p.name for p in bronze.iterdir()} == {"falso_a", "falso_b", "_controle"}
    assert (bronze / "falso_a" / "landing" / "itens" / "2024-01-01" / "itens_20240101_0001.json").is_file()
    assert (bronze / "falso_b" / "landing" / "itens" / "2024-01-03" / "itens_20240103_0001.json").is_file()
    assert (bronze / "_controle" / "relogio" / f"{CARGA.isoformat()}.json").is_file()
    assert {p.name for p in (destino.raiz / "silver_t1").iterdir()} == {"gabarito"}, "o Silver só tem o gabarito"
    assert _tudo(destino.raiz / "gold_t1") == set()


def test_o_gabarito_fica_no_silver(tmp_path, sistemas_falsos):
    sistemas_falsos(defeitos=True)
    destino = DestinoLocal(tmp_path / "ws", "t1")

    gerar_carga_inicial(destino, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())
    avancar_tempo(destino, 2, spark=SPARK_FALSO, roteiro=_roteiro())

    partes = sorted((destino.raiz / "silver_t1" / "gabarito" / "defeitos").glob("parte-*.parquet"))
    assert len(partes) == 2, "a carga inicial cria a tabela de defeitos e o avanço acrescenta uma parte"
    assert not (destino.raiz / "bronze_t1" / "gabarito").exists()
    assert not (destino.raiz / "gold_t1" / "gabarito").exists()


def test_o_relogio_e_lido_e_gravado_em_controle(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", "t1")
    destino.preparar(["falso_a"])

    gravar_relogio(destino, Relogio(CARGA, 0.5, 7))

    assert ler_relogio(destino) == Relogio(CARGA, 0.5, 7)
    assert destino.listar("_controle/relogio") == [f"_controle/relogio/{CARGA.isoformat()}.json"]


def test_listar_e_ler_usam_o_caminho_com_o_sistema_na_frente(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", "t1")
    destino.preparar(["falso_a", "falso_b"])
    destino.gravar_arquivos([
        Arquivo("falso_a/itens/2024-01-01/itens_20240101_0001.json", b"a\n"),
        Arquivo("falso_b/itens/2024-01-01/itens_20240101_0001.json", b"b\n"),
    ])

    assert destino.listar("falso_a") == ["falso_a/itens/2024-01-01/itens_20240101_0001.json"]
    assert destino.listar("falso_c") == []
    assert destino.listar("") == [
        "falso_a/itens/2024-01-01/itens_20240101_0001.json",
        "falso_b/itens/2024-01-01/itens_20240101_0001.json",
    ]
    assert destino.ler("falso_b/itens/2024-01-01/itens_20240101_0001.json") == b"b\n"


def test_arquivo_de_sistema_fora_da_carga_e_recusado(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", "t1")
    destino.preparar(["falso_a"])

    with pytest.raises(ValueError, match="falso_b"):
        destino.gravar_arquivos([Arquivo("falso_b/itens/2024-01-01/x.json", b"x")])


def test_o_reset_derruba_os_tres_catalogos_do_ambiente(tmp_path, sistemas_falsos):
    destino = DestinoLocal(tmp_path / "ws", "t1")
    gerar_carga_inicial(destino, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())
    for camada in ("bronze", "silver", "gold"):
        (destino.raiz / f"{camada}_t1" / "sobra.txt").write_text("de uma execução anterior")
    outro = tmp_path / "ws" / "bronze_t2"  # catálogo de outro ambiente: não é tocado
    outro.mkdir()

    resultado = gerar_carga_inicial(destino, ate=CARGA, reset=True, spark=SPARK_FALSO, roteiro=_roteiro())

    assert resultado.gerou
    assert not any(destino.raiz.glob("*_t1/sobra.txt"))
    assert outro.is_dir()


def test_ambientes_diferentes_convivem_na_mesma_raiz(tmp_path, sistemas_falsos):
    dev = DestinoLocal(tmp_path / "ws", "dev")
    prd = DestinoLocal(tmp_path / "ws", "prd")
    gerar_carga_inicial(dev, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())
    gerar_carga_inicial(prd, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())

    avancar_tempo(dev, 1, spark=SPARK_FALSO, roteiro=_roteiro())

    assert ler_relogio(dev).data_simulada == CARGA + timedelta(days=1)
    assert ler_relogio(prd).data_simulada == CARGA
