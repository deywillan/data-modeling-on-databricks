"""Alternativa de contingência do `DestinoUC`: catálogo único `medalhao` com schemas prefixados.

O Free Edition atual permite `CREATE CATALOG`, então não dá para provocar a falta de
permissão no workspace. Aqui o Unity Catalog é simulado na fronteira externa (SQL e API de
arquivos) por um falso mínimo que nega `CREATE CATALOG` para o catálogo do artigo.
"""

import io
import re
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import AlreadyExists, NotFound

from modelbricks_gerador import DestinoUC, gerar_fatia


class _UnityCatalogFalso:
    """Simula o SQL de DDL e a API `files` do Unity Catalog em memória."""

    def __init__(self, catalogos_proibidos: set[str]):
        self.proibidos = catalogos_proibidos
        self.catalogos: dict[str, set[str]] = {}
        self.volumes: set[str] = set()
        self.arquivos: dict[str, bytes] = {}
        self.conf = SimpleNamespace(set=lambda *_: None)
        self.files = self

    # SparkSession.sql
    def sql(self, comando: str):
        linhas: list[tuple] = []
        if m := re.fullmatch(r"SHOW CATALOGS LIKE '(\w+)'", comando):
            linhas = [(m[1],)] if m[1] in self.catalogos else []
        elif m := re.fullmatch(r"SHOW SCHEMAS IN (\w+) LIKE '(\w+)\*'", comando):
            linhas = [(s,) for s in sorted(self.catalogos[m[1]]) if s.startswith(m[2])]
        elif m := re.fullmatch(r"CREATE CATALOG IF NOT EXISTS (\w+)", comando):
            if m[1] in self.proibidos:
                raise RuntimeError(f"[PERMISSION_DENIED] User does not have CREATE CATALOG on Metastore. SQLSTATE: 42501")
            self.catalogos.setdefault(m[1], set())
        elif m := re.fullmatch(r"CREATE SCHEMA IF NOT EXISTS (\w+)\.(\w+)", comando):
            self.catalogos[m[1]].add(m[2])
        elif m := re.fullmatch(r"CREATE VOLUME IF NOT EXISTS (\w+)\.(\w+)\.(\w+)", comando):
            assert m[2] in self.catalogos[m[1]]
            self.volumes.add(f"/Volumes/{m[1]}/{m[2]}/{m[3]}")
        elif m := re.fullmatch(r"DROP SCHEMA IF EXISTS (\w+)\.(\w+) CASCADE", comando):
            self.catalogos[m[1]].discard(m[2])
            self.volumes = {v for v in self.volumes if not v.startswith(f"/Volumes/{m[1]}/{m[2]}/")}
            self.arquivos = {k: v for k, v in self.arquivos.items() if not k.startswith(f"/Volumes/{m[1]}/{m[2]}/")}
        else:
            raise AssertionError(f"SQL inesperado no falso: {comando}")
        return SimpleNamespace(collect=lambda: linhas)

    # WorkspaceClient.files
    def upload(self, caminho: str, conteudo: io.BytesIO, *, overwrite: bool):
        assert any(caminho.startswith(v + "/") for v in self.volumes), f"Volume inexistente: {caminho}"
        if caminho in self.arquivos and not overwrite:
            raise AlreadyExists("The file being created already exists.")
        self.arquivos[caminho] = conteudo.read()

    def list_directory_contents(self, pasta: str):
        filhos = {c for c in self.arquivos if c.startswith(pasta + "/")}
        if not filhos:
            raise NotFound(pasta)
        diretos = {pasta + "/" + c[len(pasta) + 1 :].split("/")[0] for c in filhos}
        return [SimpleNamespace(path=p, is_directory=p not in self.arquivos) for p in sorted(diretos)]

    def download(self, caminho: str):
        return SimpleNamespace(contents=io.BytesIO(self.arquivos[caminho]))


@pytest.fixture
def uc():
    return _UnityCatalogFalso(catalogos_proibidos={"medalhao_01_bronze"})


def _destino(uc):
    return DestinoUC("medalhao_01_bronze", spark=uc, workspace=uc)


def test_sem_permissao_de_catalogo_usa_medalhao_com_schemas_prefixados(uc, capsys):
    resultado = gerar_fatia("01-bronze", _destino(uc), spark=uc)

    assert resultado.gerou
    assert "Aviso: sem permissão para criar o catálogo medalhao_01_bronze" in capsys.readouterr().out
    assert set(uc.catalogos) == {"medalhao"}
    assert uc.catalogos["medalhao"] == {"a01_landing", "a01_bronze", "a01_silver", "a01_gold"}
    assert uc.volumes == {"/Volumes/medalhao/a01_landing/landing"}
    assert "/Volumes/medalhao/a01_landing/landing/ibge/municipios/2025-12-31/municipios_20251231_0001.csv" in uc.arquivos
    assert "/Volumes/medalhao/a01_landing/landing/_controle/relogio/2025-12-31.json" in uc.arquivos


def test_alternativa_e_idempotente_e_reset_so_apaga_os_schemas_do_artigo(uc):
    gerar_fatia("01-bronze", _destino(uc), spark=uc)
    uc.catalogos["medalhao"].add("a03_bronze")  # outro artigo no mesmo catálogo

    segunda = gerar_fatia("01-bronze", _destino(uc), spark=uc)
    assert not segunda.gerou

    uc.catalogos["medalhao"].add("a01_sobra")
    terceira = gerar_fatia("01-bronze", _destino(uc), spark=uc, reset=True)

    assert terceira.gerou
    assert uc.catalogos["medalhao"] == {"a01_landing", "a01_bronze", "a01_silver", "a01_gold", "a03_bronze"}
