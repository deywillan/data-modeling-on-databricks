"""`DestinoUC` contra um Unity Catalog simulado na fronteira externa (SQL e API de arquivos).

Confere, sem abrir sessão nem gastar cota, quais catálogos, schemas e Volumes o destino cria
e em que caminho de Volume cada arquivo cai. O teste no workspace de verdade fica em
`test_destino_uc.py` (marcado como `lento`).
"""

import io
import re
from types import SimpleNamespace

import geracoes
import pytest
from databricks.sdk.errors import AlreadyExists, NotFound

from modelbricks_gerador import ArquivoJaExiste, DestinoUC, gerar_carga_inicial
from modelbricks_gerador.escritor import Arquivo

# O falso não executa Spark (o mundo verdadeiro fica vazio, fixture `mundo_vazio`), e o que se
# testa aqui é só onde o destino grava.
_SO_IBGE = geracoes.roteiro_so_ibge()
_CSV_IBGE = "ibge/municipios/2025-12-31/municipios_20251231_0001.csv"


class _UnityCatalogFalso:
    """Simula o SQL de DDL e a API `files` do Unity Catalog em memória."""

    def __init__(self, catalogos_proibidos: frozenset[str] = frozenset()):
        self.proibidos = catalogos_proibidos
        self.catalogos: dict[str, set[str]] = {}
        self.volumes: set[str] = set()
        self.arquivos: dict[str, bytes] = {}
        self.tabelas: dict[str, list] = {}
        self.comandos: list[str] = []
        self.conf = SimpleNamespace(set=lambda *_: None)
        self.files = self

    # SparkSession.sql
    def sql(self, comando: str):
        self.comandos.append(comando)
        linhas: list[tuple] = []
        if m := re.fullmatch(r"CREATE CATALOG IF NOT EXISTS (\w+)", comando):
            if m[1] in self.proibidos:
                raise RuntimeError("[PERMISSION_DENIED] User does not have CREATE CATALOG on Metastore.")
            self.catalogos.setdefault(m[1], {"default", "information_schema"})
        elif m := re.fullmatch(r"SHOW SCHEMAS IN (\w+)", comando):
            linhas = [(s,) for s in sorted(self.catalogos[m[1]])]
        elif m := re.fullmatch(r"CREATE SCHEMA IF NOT EXISTS (\w+)\.(\w+)", comando):
            self.catalogos[m[1]].add(m[2])
        elif m := re.fullmatch(r"CREATE VOLUME IF NOT EXISTS (\w+)\.(\w+)\.(\w+)", comando):
            assert m[2] in self.catalogos[m[1]], f"schema inexistente: {comando}"
            self.volumes.add(f"/Volumes/{m[1]}/{m[2]}/{m[3]}")
        elif m := re.fullmatch(r"DROP CATALOG IF EXISTS (\w+) CASCADE", comando):
            self.catalogos.pop(m[1], None)
            prefixo = f"/Volumes/{m[1]}/"
            self.volumes = {v for v in self.volumes if not v.startswith(prefixo)}
            self.arquivos = {k: v for k, v in self.arquivos.items() if not k.startswith(prefixo)}
        else:
            raise AssertionError(f"SQL inesperado no falso: {comando}")
        return SimpleNamespace(collect=lambda: linhas)

    # SparkSession.createDataFrame: só a tabela vazia de defeitos da carga inicial
    def createDataFrame(self, linhas, _esquema):
        return _TabelaFalsa(self, list(linhas))

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


class _TabelaFalsa:
    """O mínimo de um DataFrame que `api._gravar_defeitos` e `DestinoUC.gravar_gabarito` usam."""

    def __init__(self, uc: _UnityCatalogFalso, linhas: list):
        self.uc, self.linhas = uc, linhas
        self.write = self

    def select(self, *_):
        return self

    def orderBy(self, *_):
        return self

    def mode(self, _modo):
        return self

    def saveAsTable(self, nome: str):
        catalogo, schema, _ = nome.split(".")
        assert schema in self.uc.catalogos[catalogo], f"schema inexistente: {nome}"
        self.uc.tabelas[nome] = self.linhas


@pytest.fixture
def uc(mundo_vazio):
    return _UnityCatalogFalso()


def _destino(uc, ambiente="t1"):
    return DestinoUC(ambiente, spark=uc, workspace=uc)


def test_a_carga_inicial_cria_os_tres_catalogos_da_serie_com_um_schema_por_sistema(uc):
    resultado = gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE)

    assert resultado.gerou
    assert set(uc.catalogos) == {"bronze_t1", "silver_t1", "gold_t1"}
    assert uc.catalogos["bronze_t1"] == {"default", "information_schema", "ibge", "_controle"}
    assert uc.catalogos["silver_t1"] == {"default", "information_schema", "gabarito"}, "Silver só com o gabarito"
    assert uc.catalogos["gold_t1"] == {"default", "information_schema"}
    assert uc.volumes == {"/Volumes/bronze_t1/ibge/landing", "/Volumes/bronze_t1/_controle/relogio"}


def test_cada_arquivo_cai_no_volume_do_proprio_sistema_e_o_relogio_em_controle(uc):
    gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE)

    assert "/Volumes/bronze_t1/ibge/landing/municipios/2025-12-31/municipios_20251231_0001.csv" in uc.arquivos
    assert "/Volumes/bronze_t1/_controle/relogio/2025-12-31.json" in uc.arquivos
    assert len(uc.arquivos) == 2


def test_o_gabarito_vai_para_um_schema_do_silver(uc):
    gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE)

    assert set(uc.tabelas) == {"silver_t1.gabarito.defeitos"}, "a tabela de defeitos nasce mesmo vazia"
    assert "gabarito" not in uc.catalogos["bronze_t1"]


def test_listar_e_ler_usam_os_caminhos_do_contrato(uc):
    destino = _destino(uc)
    gerar_carga_inicial(destino, spark=uc, roteiro=_SO_IBGE)

    assert sorted(destino.listar("")) == sorted([_CSV_IBGE, "_controle/relogio/2025-12-31.json"])
    assert destino.listar("ibge/municipios") == [_CSV_IBGE]
    assert destino.listar("_controle") == ["_controle/relogio/2025-12-31.json"]
    assert destino.listar("rastro") == []
    assert destino.ler(_CSV_IBGE).startswith(b"cod_municipio,nome_municipio")


def test_a_landing_nunca_sobrescreve_mas_aceita_os_mesmos_bytes(uc):
    destino = _destino(uc)
    gerar_carga_inicial(destino, spark=uc, roteiro=_SO_IBGE)
    conteudo = destino.ler(_CSV_IBGE)

    destino.gravar_arquivos([Arquivo(_CSV_IBGE, conteudo)])
    with pytest.raises(ArquivoJaExiste):
        destino.gravar_arquivos([Arquivo(_CSV_IBGE, b"outro")])

    assert destino.ler(_CSV_IBGE) == conteudo


def test_reexecutar_nao_cria_nada_e_o_reset_apaga_so_os_tres_catalogos_do_ambiente(uc):
    gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE)
    uc.sql("CREATE CATALOG IF NOT EXISTS bronze_prd")  # outro ambiente
    uc.sql("CREATE SCHEMA IF NOT EXISTS silver_t1.ods")  # o schema que um artigo da Silver criou

    segunda = gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE)
    assert not segunda.gerou
    assert "ods" in uc.catalogos["silver_t1"], "rodar o setup de novo não apaga o que os artigos construíram"

    terceira = gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE, reset=True)

    assert terceira.gerou
    assert "ods" not in uc.catalogos["silver_t1"], "o reset recomeça a série do zero"
    assert set(uc.catalogos) == {"bronze_t1", "silver_t1", "gold_t1", "bronze_prd"}


def test_sem_permissao_para_criar_catalogo_o_erro_diz_qual(uc):
    uc.proibidos = frozenset({"silver_t1"})

    with pytest.raises(PermissionError, match="silver_t1"):
        gerar_carga_inicial(_destino(uc), spark=uc, roteiro=_SO_IBGE)


def test_ambiente_invalido_e_recusado_na_construcao(uc):
    with pytest.raises(ValueError, match="ambiente"):
        _destino(uc, ambiente="dev; DROP CATALOG x")
