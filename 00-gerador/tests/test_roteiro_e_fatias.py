"""Roteiro e registro de fatias, observados pela API pública."""

from importlib import resources

import pytest
import yaml

from modelbricks_gerador import RoteiroInvalido, gerar_fatia

FATIAS_COM_GABARITO = {"03-3fn", "05-raw-vault", "06-business-vault", "10-data-marts"}
FATIAS_SEM_GABARITO = {"01-bronze", "02-historizacao", "04-ods", "07-star-schema", "08-snowflake", "09-obt"}


@pytest.mark.parametrize("artigo", sorted(FATIAS_COM_GABARITO | FATIAS_SEM_GABARITO))
def test_as_dez_fatias_estao_declaradas_e_o_gabarito_so_existe_quando_pedido(spark, destino, artigo):
    gerar_fatia(artigo, destino, spark=spark)

    assert (destino.raiz / "gabarito").is_dir() == (artigo in FATIAS_COM_GABARITO)
    assert not any("gabarito" in p.as_posix() for p in destino.volume.rglob("*"))


def test_artigo_sem_fatia_falha_listando_as_opcoes(spark, destino):
    with pytest.raises(KeyError, match="99-inexistente.*01-bronze"):
        gerar_fatia("99-inexistente", destino, spark=spark)


def _roteiro_padrao() -> dict:
    texto = resources.files("modelbricks_gerador").joinpath("roteiro.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(texto)


def _gravar(dados: dict, caminho):
    caminho.write_text(yaml.safe_dump(dados, allow_unicode=True), encoding="utf-8")
    return caminho


def test_roteiro_sem_campo_obrigatorio_falha_apontando_o_campo(spark, destino, tmp_path):
    dados = _roteiro_padrao()
    del dados["seed_base"]
    caminho = _gravar(dados, tmp_path / "roteiro_quebrado.yaml")

    with pytest.raises(RoteiroInvalido) as erro:
        gerar_fatia("01-bronze", destino, spark=spark, roteiro=caminho)

    mensagem = str(erro.value)
    assert "roteiro_quebrado.yaml" in mensagem
    assert "seed_base" in mensagem
    assert not destino.raiz.exists(), "nada pode ser criado com roteiro inválido"


def test_roteiro_incoerente_aponta_cada_problema(spark, destino, tmp_path):
    dados = _roteiro_padrao()
    dados["defeitos"]["cdc_duplicado"]["taxas"]["embarque"] = 1.5
    dados["fatias"]["01-bronze"]["sistemas"].append("sistema_que_nao_existe")
    caminho = _gravar(dados, tmp_path / "roteiro_incoerente.yaml")

    with pytest.raises(RoteiroInvalido) as erro:
        gerar_fatia("01-bronze", destino, spark=spark, roteiro=caminho)

    mensagem = str(erro.value)
    assert "defeitos.cdc_duplicado.taxas.embarque" in mensagem
    assert "fatias.01-bronze: sistema desconhecido 'sistema_que_nao_existe'" in mensagem


def test_roteiro_que_nao_e_yaml_valido_falha_com_mensagem_clara(spark, destino, tmp_path):
    caminho = tmp_path / "roteiro.yaml"
    caminho.write_text("versao: [1\n", encoding="utf-8")

    with pytest.raises(RoteiroInvalido, match="Não foi possível ler o roteiro"):
        gerar_fatia("01-bronze", destino, spark=spark, roteiro=caminho)
