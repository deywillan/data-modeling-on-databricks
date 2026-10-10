"""Destino no Unity Catalog: três catálogos por artigo, um schema por sistema e o gabarito no Silver.

Funciona igual na máquina local (Databricks Connect) e dentro do job serverless:

- DDL (catálogos, schemas, Volumes, `reset`) por SQL na sessão Spark;
- arquivos da landing pelo SDK (`WorkspaceClient().files.upload(..., overwrite=False)`), em
  paralelo, com a não-sobrescrita garantida pelo próprio serviço;
- gabarito com `saveAsTable`, direto do DataFrame, sem coletar.

Estrutura (veja o cabeçalho de `destino.py`): `aNN_bronze_<amb>.<sistema>.landing` para os
arquivos de cada sistema, `aNN_bronze_<amb>._controle.relogio` para o relógio e
`aNN_silver_<amb>.gabarito` para as tabelas do gabarito. O Silver e o Gold nascem sem tabelas.
"""

import io
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import TYPE_CHECKING

from .catalogos import catalogos_do_artigo, validar_ambiente
from .destino import (
    SCHEMA_CONTROLE,
    SCHEMA_GABARITO,
    VOLUME_LANDING,
    VOLUME_RELOGIO,
    ArquivoJaExiste,
    Local,
    caminho_do_contrato,
    localizar,
)
from .escritor import Arquivo

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import DataFrame, SparkSession

# A pesquisa técnica inicial sugeriu cerca de 16 threads para o upload ao Volume. Mantemos 32
# de propósito: na medição do ticket 17 (escala 1, Free Edition), 32 threads reduziram em cerca
# de 20% o tempo de gravação da landing em relação a 16.
_THREADS_UPLOAD = 32
_THREADS_DDL = 8
_SCHEMAS_DO_SISTEMA = {"information_schema", "default"}  # criados pelo Unity Catalog em todo catálogo


class DestinoUC:
    """Destino no Unity Catalog para os três catálogos de um artigo em um ambiente
    (ex.: `03-3fn` e `dev` → `a03_bronze_dev`, `a03_silver_dev`, `a03_gold_dev`).

    `spark` e `workspace` são opcionais: sem eles, usa a sessão e o cliente do ambiente
    (o job serverless ou o perfil local da CLI).
    """

    def __init__(
        self,
        artigo: str,
        ambiente: str = "dev",
        *,
        spark: "SparkSession | None" = None,
        workspace: "WorkspaceClient | None" = None,
    ):
        self.artigo = artigo
        self.ambiente = validar_ambiente(ambiente)
        self.catalogos = catalogos_do_artigo(artigo, ambiente)
        self._spark = spark
        self._workspace = workspace

    # --- contrato Destino -------------------------------------------------------------

    def preparar(self, sistemas: Sequence[str], gabarito: bool = False, reset: bool = False) -> None:
        if reset:
            self._em_paralelo(self._apagar_catalogo, self.catalogos.values())
        self._em_paralelo(self._criar_catalogo, self.catalogos.values())
        bronze, silver = self.catalogos["bronze"], self.catalogos["silver"]
        volumes = [(bronze, sistema, VOLUME_LANDING) for sistema in sistemas]
        volumes.append((bronze, SCHEMA_CONTROLE, VOLUME_RELOGIO))
        self._em_paralelo(lambda v: self._criar_schema(*v), volumes)
        if gabarito:
            self._sql(f"CREATE SCHEMA IF NOT EXISTS {silver}.{SCHEMA_GABARITO}")

    def gravar_arquivos(self, arquivos: Iterable[Arquivo]) -> None:
        from databricks.sdk.errors import AlreadyExists

        def enviar(arquivo: Arquivo) -> None:
            try:
                self.workspace.files.upload(
                    self._caminho_no_volume(arquivo.caminho), io.BytesIO(arquivo.conteudo), overwrite=False
                )
            except AlreadyExists:
                # Idêntico ao que já está lá (nova tentativa de uma chamada que caiu): conta como gravado.
                if self.ler(arquivo.caminho) != arquivo.conteudo:
                    raise ArquivoJaExiste(
                        f"A landing nunca sobrescreve: {arquivo.caminho} já existe com outro conteúdo."
                    ) from None

        with ThreadPoolExecutor(max_workers=_THREADS_UPLOAD) as executor:
            # list() propaga a primeira exceção (ex.: ArquivoJaExiste).
            list(executor.map(enviar, list(arquivos)))

    def listar(self, prefixo: str) -> list[str]:
        prefixo = prefixo.strip("/")
        if not prefixo:
            raizes = [(schema, self._volume_do_schema(schema)) for schema in self._schemas_do_bronze()]
        elif prefixo == SCHEMA_CONTROLE:
            raizes = [(SCHEMA_CONTROLE, VOLUME_RELOGIO)]
        else:
            local = localizar(prefixo)
            raizes = [(local.schema, local.volume)]
        caminhos = []
        for schema, volume in raizes:
            base = f"{self.raiz_do_volume(schema, volume)}/"
            for caminho in self._listar_arquivos(base):
                contrato = caminho_do_contrato(Local(schema, volume, caminho.removeprefix(base)))
                if not prefixo or contrato == prefixo or contrato.startswith(prefixo + "/"):
                    caminhos.append(contrato)
        return sorted(caminhos)

    def ler(self, caminho: str) -> bytes:
        return self.workspace.files.download(self._caminho_no_volume(caminho)).contents.read()

    def gravar_gabarito(self, tabela: str, df: "DataFrame", acrescentar: bool = False) -> None:
        destino = f"{self.catalogos['silver']}.{SCHEMA_GABARITO}.{tabela}"
        df.write.mode("append" if acrescentar else "overwrite").saveAsTable(destino)

    def apagar_do_gabarito(self, tabela: str, coluna: str, a_partir_de: date) -> None:
        schema = f"{self.catalogos['silver']}.{SCHEMA_GABARITO}"
        if self._sql(f"SHOW TABLES IN {schema} LIKE '{tabela}'").collect():
            self._sql(f"DELETE FROM {schema}.{tabela} WHERE {coluna} >= DATE'{a_partir_de.isoformat()}'")

    # --- onde as coisas ficam ---------------------------------------------------------

    def raiz_do_volume(self, schema: str, volume: str = VOLUME_LANDING) -> str:
        """Raiz de um Volume do Bronze, ex.: `/Volumes/a01_bronze_dev/embarque/landing`."""
        return f"/Volumes/{self.catalogos['bronze']}/{schema}/{volume}"

    @property
    def spark(self) -> "SparkSession":
        if self._spark is None:
            from databricks.connect import DatabricksSession

            self._spark = DatabricksSession.builder.getOrCreate()
        return self._spark

    @property
    def workspace(self) -> "WorkspaceClient":
        if self._workspace is None:
            from databricks.sdk import WorkspaceClient

            self._workspace = WorkspaceClient()
        return self._workspace

    # --- internos ---------------------------------------------------------------------

    def _sql(self, comando: str):
        return self.spark.sql(comando)

    def _em_paralelo(self, funcao, itens) -> None:
        with ThreadPoolExecutor(max_workers=_THREADS_DDL) as executor:
            list(executor.map(funcao, list(itens)))  # list() propaga a primeira exceção

    def _caminho_no_volume(self, caminho: str) -> str:
        local = localizar(caminho)
        return f"{self.raiz_do_volume(local.schema, local.volume)}/{local.resto}"

    def _criar_catalogo(self, catalogo: str) -> None:
        try:
            self._sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
        except Exception as erro:
            if "PERMISSION_DENIED" in str(erro) or "does not have CREATE CATALOG" in str(erro):
                raise PermissionError(
                    f"Sem permissão para criar o catálogo {catalogo}. O gerador precisa de CREATE CATALOG "
                    "no metastore (o Free Edition permite)."
                ) from erro
            raise

    def _apagar_catalogo(self, catalogo: str) -> None:
        self._sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")

    def _criar_schema(self, catalogo: str, schema: str, volume: str) -> None:
        self._sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema}")
        self._sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{schema}.{volume}")

    def _schemas_do_bronze(self) -> list[str]:
        linhas = self._sql(f"SHOW SCHEMAS IN {self.catalogos['bronze']}").collect()
        return sorted(linha[0] for linha in linhas if linha[0] not in _SCHEMAS_DO_SISTEMA)

    @staticmethod
    def _volume_do_schema(schema: str) -> str:
        return VOLUME_RELOGIO if schema == SCHEMA_CONTROLE else VOLUME_LANDING

    def _listar_arquivos(self, base: str) -> list[str]:
        from databricks.sdk.errors import NotFound

        caminhos, pendentes = [], [base.rstrip("/")]
        while pendentes:
            try:
                itens = list(self.workspace.files.list_directory_contents(pendentes.pop()))
            except NotFound:
                continue
            for item in itens:
                if item.is_directory:
                    pendentes.append(item.path)
                else:
                    caminhos.append(item.path)
        return caminhos


__all__ = ["DestinoUC"]
