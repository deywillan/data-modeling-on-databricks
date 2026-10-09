"""Fixtures compartilhadas dos testes do gerador.

Os testes geram dados no Spark remoto via Databricks Connect serverless e gravam num
diretório local. O perfil da CLI nunca é versionado: defina-o no ambiente, por exemplo

    DATABRICKS_CONFIG_PROFILE=<seu-perfil> OPENSSL_CONF=/dev/null uv run pytest

(`OPENSSL_CONF=/dev/null` só é necessário em ambientes em que pyarrow + OpenSSL quebram.)

Custo: cada geração consome a cota diária do workspace. As gerações ficam em `geracoes.py`
e são pedidas pelas fixtures de sessão abaixo, então cada uma roda no máximo uma vez por
sessão e fica no cache local entre sessões (`--sem-cache` ignora o cache). A execução padrão
deixa de fora os testes marcados como `lento` (gerações extras para comparar determinismo e
geração em partes, fatias inteiras, Unity Catalog). Rode-os nos tickets de marco com
`pytest -m "lento or not lento"`.

Pré-aquecimento: as gerações que os testes selecionados pedem rodam em paralelo (até
`GERACOES_EM_PARALELO` por vez, na mesma sessão Connect) logo depois da coleta, em vez de
uma depois da outra na primeira vez em que algum teste pede a fixture. O tempo na parede cai
para o da geração mais longa; o trabalho no workspace é o mesmo.
"""

import atexit
import os
import shutil
import tempfile
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal


GERACOES_EM_PARALELO = 3
# Fixture → geração, na ordem em que o pré-aquecimento as inicia (as mais longas primeiro).
_GERACOES = {
    "principal_em_partes": geracoes.PRINCIPAL_EM_PARTES,
    "principal": geracoes.PRINCIPAL,
    "legados": geracoes.LEGADOS,
    "legados_em_partes": geracoes.LEGADOS_EM_PARTES,
    "patio": geracoes.PATIO,
    "patio_em_partes": geracoes.PATIO_EM_PARTES,
    "mundo_de_novo": geracoes.MUNDO,
}
_SEM_CACHE_SEMPRE = {"mundo_de_novo"}
_PROMETIDAS: dict[str, Future] = {}
_SESSAO = threading.Lock()


def _perfil_definido() -> bool:
    return bool(os.environ.get("DATABRICKS_CONFIG_PROFILE") or os.environ.get("DATABRICKS_HOST"))


def _abrir_sessao():
    from databricks.connect import DatabricksSession

    with _SESSAO:
        return DatabricksSession.builder.serverless().getOrCreate()


def _gerar(nome: str, usar_cache: bool):
    # Dentro do diretório do cache (ignorado pelo git): o temporário do sistema pode ser um
    # link simbólico, o que quebra os caminhos relativos do destino local.
    geracoes.CACHE.mkdir(exist_ok=True)
    pasta = tempfile.mkdtemp(prefix=f".temporario-{nome}-", dir=geracoes.CACHE)
    atexit.register(shutil.rmtree, pasta, ignore_errors=True)
    return geracoes.obter(_GERACOES[nome], Path(pasta), _abrir_sessao, usar_cache=usar_cache)


def pytest_collection_finish(session):
    """Inicia em paralelo as gerações que os testes selecionados pedem."""
    if not _perfil_definido():
        return
    pedidas = {f for item in session.items for f in item.fixturenames}
    nomes = [n for n in _GERACOES if n in pedidas]
    if len(nomes) < 2:
        return
    usar_cache = not session.config.getoption("--sem-cache")
    executor = ThreadPoolExecutor(max_workers=GERACOES_EM_PARALELO, thread_name_prefix="geracao")
    for nome in nomes:
        _PROMETIDAS[nome] = executor.submit(_gerar, nome, usar_cache and nome not in _SEM_CACHE_SEMPRE)
    executor.shutdown(wait=False)


def pytest_addoption(parser):
    parser.addoption(
        "--sem-cache", action="store_true", default=False,
        help="gera de novo as gerações compartilhadas, sem ler nem gravar o cache local dos testes",
    )


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


def _obter(geracao: geracoes.Geracao, request, tmp_path_factory) -> geracoes.Gerada:
    prometida = _PROMETIDAS.get(request.fixturename)
    if prometida is not None:
        return prometida.result()
    return geracoes.obter(
        geracao,
        tmp_path_factory.mktemp(geracao.nome),
        lambda: request.getfixturevalue("spark"),
        usar_cache=not request.config.getoption("--sem-cache"),
    )


# Gerações da execução padrão.


@pytest.fixture(scope="session")
def principal(request, tmp_path_factory):
    """Fatia de teste com Embarque (taxas oficiais), Núcleo, RotaCerta, Apólice+, PassagemHub
    e Rastro na escala 0,05: carga inicial até 31/12/2025 e avanço de 12 dias de uma vez."""
    return _obter(geracoes.PRINCIPAL, request, tmp_path_factory)


@pytest.fixture(scope="session")
def legados(request, tmp_path_factory):
    """SolTicket, SolFin e Embarque (com todos os defeitos de linha) na escala 0,1, até 05/01/2026."""
    return _obter(geracoes.LEGADOS, request, tmp_path_factory)


@pytest.fixture(scope="session")
def patio(request, tmp_path_factory):
    """Pátio na escala 0,2 (população de clientes para medir taxas), até 03/01/2026."""
    return _obter(geracoes.PATIO, request, tmp_path_factory)


# Gerações usadas só por testes marcados como `lento`.


@pytest.fixture(scope="session")
def principal_em_partes(request, tmp_path_factory):
    return _obter(geracoes.PRINCIPAL_EM_PARTES, request, tmp_path_factory)


@pytest.fixture(scope="session")
def legados_em_partes(request, tmp_path_factory):
    return _obter(geracoes.LEGADOS_EM_PARTES, request, tmp_path_factory)


@pytest.fixture(scope="session")
def patio_em_partes(request, tmp_path_factory):
    return _obter(geracoes.PATIO_EM_PARTES, request, tmp_path_factory)


@pytest.fixture(scope="session")
def mundo_de_novo(request, tmp_path_factory):
    """O mundo verdadeiro gerado de novo, sempre sem cache, para comparar com o da principal."""
    if "mundo_de_novo" in _PROMETIDAS:
        return _PROMETIDAS["mundo_de_novo"].result()
    return geracoes.obter(
        geracoes.MUNDO, tmp_path_factory.mktemp("mundo"), lambda: request.getfixturevalue("spark"), usar_cache=False,
    )
