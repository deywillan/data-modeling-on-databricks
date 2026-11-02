"""Roteiro e carga da série, observados pela API pública.

Os erros de roteiro acontecem antes de abrir a sessão Spark, então esses testes rodam sem
cota. A geração da carga completa fica em `test_carga_inicial.py` (lento).
"""

from importlib import resources

import geracoes
import pytest
import yaml

from modelbricks_gerador import RoteiroInvalido, carregar_roteiro, gerar_carga_inicial
from modelbricks_gerador.sistemas import PROJECOES


def test_a_carga_leva_todos_os_sistemas_para_a_bronze_e_todos_tem_projecao():
    """A série constrói um único lakehouse: o 00_setup grava todos os sistemas de uma vez."""
    roteiro = carregar_roteiro()

    assert set(roteiro.carga.sistemas) == set(roteiro.sistemas)
    assert len(roteiro.carga.sistemas) == len(set(roteiro.carga.sistemas))
    assert not [s for s in roteiro.carga.sistemas if s not in PROJECOES], "todo sistema da carga tem projeção"


def test_o_roteiro_nao_tem_fatias_por_artigo():
    dados = _roteiro_padrao()

    assert "fatias" not in dados
    assert set(dados["carga"]) == {"sistemas"}


def test_o_roteiro_nao_guarda_o_mapa_defeito_artigo_nem_o_nome_do_catalogo(destino, tmp_path):
    """O mapa defeito → artigo fica só no Estudo de Caso (decisão de 10/10/2026), e o nome do
    catálogo sai de `catalogos.nome_catalogo`, não do roteiro."""
    dados = _roteiro_padrao()
    assert not any("artigos" in defeito for defeito in dados["defeitos"].values())
    assert "catalogo" not in dados["carga"]
    dados["defeitos"]["cdc_duplicado"]["artigos"] = [2]
    caminho = _gravar(dados, tmp_path / "roteiro_com_mapa.yaml")

    with pytest.raises(RoteiroInvalido, match="artigos"):
        gerar_carga_inicial(destino, roteiro=caminho)


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
        gerar_carga_inicial(destino, roteiro=caminho)

    mensagem = str(erro.value)
    assert "roteiro_quebrado.yaml" in mensagem
    assert "seed_base" in mensagem
    assert not destino.raiz.exists(), "nada pode ser criado com roteiro inválido"


def test_roteiro_incoerente_aponta_cada_problema(destino, tmp_path):
    dados = _roteiro_padrao()
    dados["defeitos"]["cdc_duplicado"]["taxas"]["embarque"] = 1.5
    dados["carga"]["sistemas"] += ["sistema_que_nao_existe", "embarque"]
    caminho = _gravar(dados, tmp_path / "roteiro_incoerente.yaml")

    with pytest.raises(RoteiroInvalido) as erro:
        gerar_carga_inicial(destino, roteiro=caminho)

    mensagem = str(erro.value)
    assert "defeitos.cdc_duplicado.taxas.embarque" in mensagem
    assert "carga.sistemas: sistema desconhecido 'sistema_que_nao_existe'" in mensagem
    assert "carga.sistemas: sistema repetido" in mensagem


def test_roteiro_que_nao_e_yaml_valido_falha_com_mensagem_clara(destino, tmp_path):
    caminho = tmp_path / "roteiro.yaml"
    caminho.write_text("versao: [1\n", encoding="utf-8")

    with pytest.raises(RoteiroInvalido, match="Não foi possível ler o roteiro"):
        gerar_carga_inicial(destino, roteiro=caminho)


@pytest.mark.parametrize("variante", ["teste", "embarque_sujo"])
def test_roteiro_de_teste_e_valido_e_so_troca_os_sistemas_da_carga(tmp_path, variante):
    """O roteiro de teste único (geracoes.py) passa na validação, com a carga oficial ou a de
    uma geração, e mantém o mundo verdadeiro oficial."""
    oficial = carregar_roteiro()
    for sistemas in (None, geracoes.PRINCIPAL.sistemas):
        caminho = tmp_path / f"{variante}.yaml"
        caminho.write_text(geracoes.texto_do_roteiro(variante, sistemas), encoding="utf-8")
        assert carregar_roteiro(caminho).carga.sistemas == list(sistemas or oficial.carga.sistemas)
    dados = geracoes.dados_do_roteiro(variante)
    assert (dados["volumes"], dados["mundo"], dados["datas"]) == (
        _roteiro_padrao()["volumes"], _roteiro_padrao()["mundo"], _roteiro_padrao()["datas"]
    ), "o mundo verdadeiro do roteiro de teste é o oficial"
