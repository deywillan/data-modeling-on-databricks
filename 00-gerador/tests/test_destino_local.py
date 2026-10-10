"""Onde o destino grava: três catálogos por artigo e um schema por sistema, sem Spark.

O `DestinoLocal` espelha em pastas a estrutura do Unity Catalog:

    <raiz>/aNN_bronze_<amb>/<sistema>/landing/<entidade>/<data>/<arquivo>
    <raiz>/aNN_bronze_<amb>/_controle/relogio/<data>.json
    <raiz>/aNN_silver_<amb>/gabarito/<tabela>/parte-NNNN.parquet   (só nas fatias com gabarito)
    <raiz>/aNN_gold_<amb>/

Os sistemas são falsos (como em `test_recuperacao.py`), então nada abre sessão no workspace.
"""

from datetime import timedelta

import pytest
from falsos import ARTIGO, CARGA, SPARK_FALSO, roteiro_falso as _roteiro

from modelbricks_gerador import DestinoLocal, avancar_tempo, gerar_fatia
from modelbricks_gerador.escritor import Arquivo
from modelbricks_gerador.relogio import Relogio, gravar_relogio, ler_relogio


def _tudo(raiz) -> set[str]:
    return {p.relative_to(raiz).as_posix() for p in raiz.rglob("*")}


def test_a_carga_inicial_cria_os_tres_catalogos_com_um_schema_por_sistema(tmp_path, sistemas_falsos):
    destino = DestinoLocal(tmp_path / "ws", ARTIGO, "t1")

    gerar_fatia(ARTIGO, destino, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())

    assert {p.name for p in destino.raiz.iterdir()} == {"a01_bronze_t1", "a01_silver_t1", "a01_gold_t1"}
    bronze = destino.raiz / "a01_bronze_t1"
    assert {p.name for p in bronze.iterdir()} == {"falso_a", "falso_b", "_controle"}
    assert (bronze / "falso_a" / "landing" / "itens" / "2024-01-01" / "itens_20240101_0001.json").is_file()
    assert (bronze / "falso_b" / "landing" / "itens" / "2024-01-03" / "itens_20240103_0001.json").is_file()
    assert (bronze / "_controle" / "relogio" / f"{CARGA.isoformat()}.json").is_file()
    assert _tudo(destino.raiz / "a01_silver_t1") == set(), "o Silver nasce vazio (fatia sem gabarito)"
    assert _tudo(destino.raiz / "a01_gold_t1") == set()


def test_o_gabarito_fica_no_silver_e_so_nas_fatias_que_o_tem(tmp_path, sistemas_falsos):
    sistemas_falsos(defeitos=True)
    destino = DestinoLocal(tmp_path / "ws", ARTIGO, "t1")
    roteiro = _roteiro(gabarito=True)
    # A carga inicial com gabarito precisa do mundo (Spark): o estado "depois dela" é montado à mão.
    destino.preparar(["falso_a", "falso_b"], gabarito=True)
    gravar_relogio(destino, Relogio(CARGA, ARTIGO, 1.0, roteiro.versao))

    avancar_tempo(ARTIGO, destino, 2, spark=SPARK_FALSO, roteiro=roteiro)

    partes = sorted((destino.raiz / "a01_silver_t1" / "gabarito" / "defeitos").glob("parte-*.parquet"))
    assert partes, "o gabarito é gravado no catálogo Silver"
    assert not (destino.raiz / "a01_bronze_t1" / "gabarito").exists()
    assert not (destino.raiz / "a01_gold_t1" / "gabarito").exists()


def test_o_relogio_e_lido_e_gravado_em_controle(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", ARTIGO, "t1")
    destino.preparar(["falso_a"])

    gravar_relogio(destino, Relogio(CARGA, ARTIGO, 0.5, 7))

    assert ler_relogio(destino) == Relogio(CARGA, ARTIGO, 0.5, 7)
    assert destino.listar("_controle/relogio") == [f"_controle/relogio/{CARGA.isoformat()}.json"]


def test_listar_e_ler_usam_o_caminho_com_o_sistema_na_frente(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", ARTIGO, "t1")
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


def test_arquivo_de_sistema_fora_da_fatia_e_recusado(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", ARTIGO, "t1")
    destino.preparar(["falso_a"])

    with pytest.raises(ValueError, match="falso_b"):
        destino.gravar_arquivos([Arquivo("falso_b/itens/2024-01-01/x.json", b"x")])


def test_o_reset_derruba_os_tres_catalogos(tmp_path, sistemas_falsos):
    destino = DestinoLocal(tmp_path / "ws", ARTIGO, "t1")
    gerar_fatia(ARTIGO, destino, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())
    for camada in ("bronze", "silver", "gold"):
        (destino.raiz / f"a01_{camada}_t1" / "sobra.txt").write_text("de uma execução anterior")
    outro = tmp_path / "ws" / "a02_bronze_t1"  # catálogo de outro artigo: não é tocado
    outro.mkdir()

    resultado = gerar_fatia(ARTIGO, destino, ate=CARGA, reset=True, spark=SPARK_FALSO, roteiro=_roteiro())

    assert resultado.gerou
    assert not any(destino.raiz.glob("a01_*_t1/sobra.txt"))
    assert outro.is_dir()


def test_o_destino_de_outro_artigo_e_recusado_antes_de_criar_qualquer_coisa(tmp_path):
    destino = DestinoLocal(tmp_path / "ws", "02-historizacao", "t1")

    with pytest.raises(ValueError, match="02-historizacao"):
        gerar_fatia(ARTIGO, destino, roteiro=_roteiro())  # falha antes de abrir a sessão

    assert not (tmp_path / "ws").exists()


def test_ambientes_diferentes_convivem_na_mesma_raiz(tmp_path, sistemas_falsos):
    dev = DestinoLocal(tmp_path / "ws", ARTIGO, "dev")
    prd = DestinoLocal(tmp_path / "ws", ARTIGO, "prd")
    gerar_fatia(ARTIGO, dev, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())
    gerar_fatia(ARTIGO, prd, ate=CARGA, spark=SPARK_FALSO, roteiro=_roteiro())

    avancar_tempo(ARTIGO, dev, 1, spark=SPARK_FALSO, roteiro=_roteiro())

    assert ler_relogio(dev).data_simulada == CARGA + timedelta(days=1)
    assert ler_relogio(prd).data_simulada == CARGA
