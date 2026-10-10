"""Roteiro e registro de fatias, observados pela API pública.

Os erros de roteiro e de artigo acontecem antes de abrir a sessão Spark, então esses testes
rodam sem cota. A geração das fatias inteiras dos artigos fica nos testes lentos.
"""

from importlib import resources

import pytest
import yaml

from modelbricks_gerador import RoteiroInvalido, carregar_roteiro, gerar_fatia
from modelbricks_gerador.sistemas import PROJECOES

FATIAS_COM_GABARITO = {"03-3fn", "05-raw-vault", "06-business-vault", "10-data-marts"}
FATIAS_SEM_GABARITO = {"01-bronze", "02-historizacao", "04-ods", "07-star-schema", "08-snowflake", "09-obt"}


def test_as_dez_fatias_estao_declaradas_com_gabarito_so_quando_pedido_e_sistemas_com_projecao():
    fatias = carregar_roteiro().fatias

    assert set(fatias) == FATIAS_COM_GABARITO | FATIAS_SEM_GABARITO
    assert {a for a, f in fatias.items() if f.gabarito} == FATIAS_COM_GABARITO
    sem_projecao = {(a, s) for a, f in fatias.items() for s in f.sistemas if s not in PROJECOES}
    assert not sem_projecao, "todo sistema de fatia tem projeção no gerador"


def test_fatias_05_e_06_tem_os_mesmos_sistemas_e_gabarito():
    fatias = carregar_roteiro().fatias

    assert fatias["05-raw-vault"].sistemas == fatias["06-business-vault"].sistemas
    assert fatias["05-raw-vault"].gabarito == fatias["06-business-vault"].gabarito


def _confere_fatia_inteira(artigo, gerada):
    destino = gerada.final

    assert gerada.resultados[-1].gerou
    assert (destino.raiz / "gabarito").is_dir() == (artigo in FATIAS_COM_GABARITO)
    assert any((destino.raiz / "gabarito").glob("*/*.parquet")), "gabarito declarado e vazio"
    assert not any("gabarito" in p.as_posix() for p in destino.volume.rglob("*"))
    raizes = {p.name for p in destino.volume.iterdir()}
    assert raizes == set(carregar_roteiro().fatia(artigo).sistemas) | {"_controle"}


# Fatias inteiras dos artigos que recebem os legados: a 05 (a 06 tem os mesmos sistemas,
# conferido acima) e a 10, que tem todos os sistemas. As demais combinações já são geradas
# nos outros módulos. Geradas nas gerações `fatia_05` e `fatia_10`.


@pytest.mark.lento
def test_fatia_05_com_os_legados_gera_com_gabarito_fora_da_landing(fatia_05):
    _confere_fatia_inteira("05-raw-vault", fatia_05)


@pytest.mark.lento
def test_fatia_10_com_todos_os_sistemas_gera_com_gabarito_fora_da_landing(fatia_10):
    _confere_fatia_inteira("10-data-marts", fatia_10)


def test_artigo_sem_fatia_falha_listando_as_opcoes(destino):
    with pytest.raises(KeyError, match="99-inexistente.*01-bronze"):
        gerar_fatia("99-inexistente", destino)


def _roteiro_padrao() -> dict:
    texto = resources.files("modelbricks_gerador").joinpath("roteiro.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(texto)


def _gravar(dados: dict, caminho):
    caminho.write_text(yaml.safe_dump(dados, allow_unicode=True), encoding="utf-8")
    return caminho


def test_roteiro_sem_campo_obrigatorio_falha_apontando_o_campo(destino, tmp_path):
    dados = _roteiro_padrao()
    del dados["seed_base"]
    caminho = _gravar(dados, tmp_path / "roteiro_quebrado.yaml")

    with pytest.raises(RoteiroInvalido) as erro:
        gerar_fatia("01-bronze", destino, roteiro=caminho)

    mensagem = str(erro.value)
    assert "roteiro_quebrado.yaml" in mensagem
    assert "seed_base" in mensagem
    assert not destino.raiz.exists(), "nada pode ser criado com roteiro inválido"


def test_roteiro_incoerente_aponta_cada_problema(destino, tmp_path):
    dados = _roteiro_padrao()
    dados["defeitos"]["cdc_duplicado"]["taxas"]["embarque"] = 1.5
    dados["fatias"]["01-bronze"]["sistemas"].append("sistema_que_nao_existe")
    caminho = _gravar(dados, tmp_path / "roteiro_incoerente.yaml")

    with pytest.raises(RoteiroInvalido) as erro:
        gerar_fatia("01-bronze", destino, roteiro=caminho)

    mensagem = str(erro.value)
    assert "defeitos.cdc_duplicado.taxas.embarque" in mensagem
    assert "fatias.01-bronze: sistema desconhecido 'sistema_que_nao_existe'" in mensagem


def test_roteiro_que_nao_e_yaml_valido_falha_com_mensagem_clara(destino, tmp_path):
    caminho = tmp_path / "roteiro.yaml"
    caminho.write_text("versao: [1\n", encoding="utf-8")

    with pytest.raises(RoteiroInvalido, match="Não foi possível ler o roteiro"):
        gerar_fatia("01-bronze", destino, roteiro=caminho)


def test_roteiro_de_teste_e_valido_e_so_acrescenta_fatias_de_teste(tmp_path):
    """O roteiro de teste único (geracoes.py) passa na validação e não muda as fatias dos artigos."""
    import geracoes

    oficial = carregar_roteiro()
    for variante in ("teste", "embarque_sujo"):
        caminho = tmp_path / f"{variante}.yaml"
        caminho.write_text(geracoes.texto_do_roteiro(variante), encoding="utf-8")
        assert set(carregar_roteiro(caminho).fatias) == set(oficial.fatias) | set(geracoes.FATIAS_DE_TESTE)
        dados = geracoes.dados_do_roteiro(variante)
        assert {a: f for a, f in dados["fatias"].items() if a in oficial.fatias} == _roteiro_padrao()["fatias"]
        assert set(dados["fatias"]) - set(oficial.fatias) == set(geracoes.FATIAS_DE_TESTE)
        assert (dados["volumes"], dados["mundo"], dados["datas"]) == (
            _roteiro_padrao()["volumes"], _roteiro_padrao()["mundo"], _roteiro_padrao()["datas"]
        ), "o mundo verdadeiro do roteiro de teste é o oficial"
