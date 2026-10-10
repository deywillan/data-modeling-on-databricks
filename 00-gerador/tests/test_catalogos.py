"""Nome dos catálogos: uma função só (artigo + camada + ambiente), validada, sem Spark."""

import pytest

from modelbricks_gerador import catalogos_do_artigo, nome_catalogo


@pytest.mark.parametrize(
    ("artigo", "camada", "ambiente", "esperado"),
    [
        ("03-3fn", "bronze", "dev", "a03_bronze_dev"),
        ("03-3fn", "silver", "prd", "a03_silver_prd"),
        ("10-data-marts", "gold", "dev", "a10_gold_dev"),
        ("05", "silver", "t3f9a2c1b", "a05_silver_t3f9a2c1b"),
    ],
)
def test_nome_do_catalogo_junta_artigo_camada_e_ambiente(artigo, camada, ambiente, esperado):
    assert nome_catalogo(artigo, camada, ambiente) == esperado


@pytest.mark.parametrize("artigo", ["", "3fn", "1-bronze", "abc", "003-x", "a03"])
def test_artigo_invalido_e_recusado(artigo):
    with pytest.raises(ValueError, match="artigo"):
        nome_catalogo(artigo, "bronze", "dev")


@pytest.mark.parametrize("camada", ["", "landing", "Bronze", "platina"])
def test_camada_invalida_e_recusada(camada):
    with pytest.raises(ValueError, match="camada"):
        nome_catalogo("01-bronze", camada, "dev")


@pytest.mark.parametrize("ambiente", ["", "Dev", "dev-1", "dev;drop", "1dev", "d" * 17, "dev prd", "dev`"])
def test_ambiente_invalido_e_recusado(ambiente):
    with pytest.raises(ValueError, match="ambiente"):
        nome_catalogo("01-bronze", "bronze", ambiente)


def test_os_tres_catalogos_do_artigo_na_ordem_das_camadas():
    assert catalogos_do_artigo("05-raw-vault", "prd") == {
        "bronze": "a05_bronze_prd", "silver": "a05_silver_prd", "gold": "a05_gold_prd",
    }


@pytest.mark.parametrize("job", ["gerar_fatia", "avancar_tempo"])
def test_os_jobs_recusam_ambiente_invalido_antes_de_abrir_sessao(job, capsys):
    from modelbricks_gerador import cli

    with pytest.raises(SystemExit):
        getattr(cli, job)(["--artigo", "01-bronze", "--ambiente", "Dev; DROP CATALOG x"])

    assert "ambiente inválido" in capsys.readouterr().err


def test_o_job_recusa_artigo_sem_fatia_antes_de_abrir_sessao(capsys):
    from modelbricks_gerador import cli

    with pytest.raises(SystemExit):
        cli.gerar_fatia(["--artigo", "99-inexistente", "--ambiente", "dev"])

    assert "99-inexistente" in capsys.readouterr().err
