"""Nome dos catálogos: uma função só (camada + ambiente), validada, sem Spark."""

import pytest

from modelbricks_gerador import catalogos_da_serie, nome_catalogo


@pytest.mark.parametrize(
    ("camada", "ambiente", "esperado"),
    [
        ("bronze", "dev", "bronze_dev"),
        ("silver", "prd", "silver_prd"),
        ("gold", "dev", "gold_dev"),
        ("silver", "t3f9a2c1b", "silver_t3f9a2c1b"),
    ],
)
def test_nome_do_catalogo_junta_camada_e_ambiente(camada, ambiente, esperado):
    assert nome_catalogo(camada, ambiente) == esperado


@pytest.mark.parametrize("camada", ["", "landing", "Bronze", "platina"])
def test_camada_invalida_e_recusada(camada):
    with pytest.raises(ValueError, match="camada"):
        nome_catalogo(camada, "dev")


@pytest.mark.parametrize("ambiente", ["", "Dev", "dev-1", "dev;drop", "1dev", "d" * 17, "dev prd", "dev`"])
def test_ambiente_invalido_e_recusado(ambiente):
    with pytest.raises(ValueError, match="ambiente"):
        nome_catalogo("bronze", ambiente)


def test_os_tres_catalogos_da_serie_na_ordem_das_camadas():
    assert catalogos_da_serie("prd") == {"bronze": "bronze_prd", "silver": "silver_prd", "gold": "gold_prd"}


@pytest.mark.parametrize("job", ["gerar_carga_inicial", "avancar_tempo"])
def test_os_jobs_recusam_ambiente_invalido_antes_de_abrir_sessao(job, capsys):
    from modelbricks_gerador import cli

    with pytest.raises(SystemExit):
        getattr(cli, job)(["--ambiente", "Dev; DROP CATALOG x"])

    assert "ambiente inválido" in capsys.readouterr().err


@pytest.mark.parametrize("job", ["gerar_carga_inicial", "avancar_tempo"])
def test_os_jobs_nao_recebem_mais_o_artigo(job, capsys):
    """A série tem um lakehouse só: os jobs do gerador não dependem do artigo."""
    from modelbricks_gerador import cli

    with pytest.raises(SystemExit):
        getattr(cli, job)(["--artigo", "01-bronze", "--ambiente", "dev"])

    assert "--artigo" in capsys.readouterr().err
