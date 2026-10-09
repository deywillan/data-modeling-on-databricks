"""Fixtures compartilhadas dos testes do gerador.

Os testes geram dados no Spark remoto via Databricks Connect serverless e gravam num
diretório local. O perfil da CLI nunca é versionado: defina-o no ambiente, por exemplo

    DATABRICKS_CONFIG_PROFILE=<seu-perfil> OPENSSL_CONF=/dev/null uv run pytest

(`OPENSSL_CONF=/dev/null` só é necessário em ambientes em que pyarrow + OpenSSL quebram.)
"""

import os

import pytest

from modelbricks_gerador import DestinoLocal


@pytest.fixture(scope="session")
def spark():
    """Uma única sessão Connect serverless por execução do pytest (poupa a cota diária)."""
    if not (os.environ.get("DATABRICKS_CONFIG_PROFILE") or os.environ.get("DATABRICKS_HOST")):
        pytest.skip("Defina DATABRICKS_CONFIG_PROFILE para rodar os testes (Databricks Connect serverless).")
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless().getOrCreate()


@pytest.fixture
def destino(tmp_path):
    """Destino em diretório local, isolado por teste."""
    return DestinoLocal(tmp_path / "medalhao_teste")
