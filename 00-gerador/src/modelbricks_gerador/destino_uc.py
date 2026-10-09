"""Destino no Unity Catalog: catálogo do artigo, schemas, Volume `landing` e gabarito.

Funciona igual na máquina local (Databricks Connect) e dentro do job serverless:

- DDL (catálogo, schemas, Volume, `reset`) por SQL na sessão Spark;
- arquivos da landing pelo SDK (`WorkspaceClient().files.upload(..., overwrite=False)`), em
  paralelo, com a não-sobrescrita garantida pelo próprio serviço;
- gabarito com `saveAsTable`, direto do DataFrame, sem coletar.

Se o usuário não puder criar catálogos, o destino usa a alternativa de contingência: o
catálogo único `medalhao`, com schemas prefixados pelo número do artigo (`a01_landing`,
`a01_bronze`...). A troca é avisada na saída e é transparente para o resto do gerador.
"""

import io
import re
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Literal

from .destino import ArquivoJaExiste
from .escritor import Arquivo

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import DataFrame, SparkSession

CATALOGO_ALTERNATIVO = "medalhao"
VOLUME_LANDING = "landing"
_THREADS_UPLOAD = 16
_NUMERO_ARTIGO = re.compile(r"^medalhao_(\d{2})_")

Modo = Literal["catalogo", "alternativo"]


class DestinoUC:
    """Destino no Unity Catalog para o catálogo `catalogo` (ex.: `medalhao_01_bronze`).

    `spark` e `workspace` são opcionais: sem eles, usa a sessão e o cliente do ambiente
    (o job serverless ou o perfil local da CLI).
    """

    def __init__(
        self,
        catalogo: str,
        *,
        spark: "SparkSession | None" = None,
        workspace: "WorkspaceClient | None" = None,
    ):
        self.catalogo = catalogo
        numero = _NUMERO_ARTIGO.match(catalogo)
        self.prefixo_alternativo = f"a{numero[1]}_" if numero else f"{catalogo}_"
        self._spark = spark
        self._workspace = workspace
        self._modo: Modo | None = None

    # --- contrato Destino -------------------------------------------------------------

    def preparar(self, schemas: Sequence[str], reset: bool = False) -> None:
        if reset:
            self._apagar()
        self._garantir_catalogo()
        for schema in schemas:
            self._sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema(schema)}")
        self._sql(f"CREATE VOLUME IF NOT EXISTS {self.schema('landing')}.{VOLUME_LANDING}")

    def gravar_arquivos(self, arquivos: Iterable[Arquivo]) -> None:
        from databricks.sdk.errors import AlreadyExists

        volume = self.volume

        def enviar(arquivo: Arquivo) -> None:
            try:
                self.workspace.files.upload(
                    f"{volume}/{arquivo.caminho}", io.BytesIO(arquivo.conteudo), overwrite=False
                )
            except AlreadyExists:
                raise ArquivoJaExiste(f"A landing nunca sobrescreve: {arquivo.caminho} já existe.") from None

        with ThreadPoolExecutor(max_workers=_THREADS_UPLOAD) as executor:
            # list() propaga a primeira exceção (ex.: ArquivoJaExiste).
            list(executor.map(enviar, list(arquivos)))

    def listar(self, prefixo: str) -> list[str]:
        from databricks.sdk.errors import NotFound

        volume = self.volume
        caminhos, pendentes = [], [f"{volume}/{prefixo}".rstrip("/")]
        while pendentes:
            try:
                itens = list(self.workspace.files.list_directory_contents(pendentes.pop()))
            except NotFound:
                continue
            for item in itens:
                if item.is_directory:
                    pendentes.append(item.path)
                else:
                    caminhos.append(item.path.removeprefix(f"{volume}/"))
        return sorted(caminhos)

    def ler(self, caminho: str) -> bytes:
        return self.workspace.files.download(f"{self.volume}/{caminho}").contents.read()

    def gravar_gabarito(self, tabela: str, df: "DataFrame") -> None:
        df.write.mode("overwrite").saveAsTable(f"{self.schema('gabarito')}.{tabela}")

    # --- onde as coisas ficam ---------------------------------------------------------

    @property
    def modo(self) -> Modo:
        """`catalogo` (padrão) ou `alternativo` (catálogo único com schemas prefixados)."""
        return self._modo_existente() or "catalogo"

    def schema(self, nome: str) -> str:
        """Nome qualificado do schema, ex.: `medalhao_01_bronze.bronze` ou `medalhao.a01_bronze`."""
        if self.modo == "alternativo":
            return f"{CATALOGO_ALTERNATIVO}.{self.prefixo_alternativo}{nome}"
        return f"{self.catalogo}.{nome}"

    @property
    def volume(self) -> str:
        """Raiz do Volume `landing`, ex.: `/Volumes/medalhao_01_bronze/landing/landing`."""
        return "/Volumes/" + self.schema("landing").replace(".", "/") + f"/{VOLUME_LANDING}"

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

    def _existe_catalogo(self, nome: str) -> bool:
        return bool(self._sql(f"SHOW CATALOGS LIKE '{nome}'").collect())

    def _schemas_alternativos(self) -> list[str]:
        if not self._existe_catalogo(CATALOGO_ALTERNATIVO):
            return []
        linhas = self._sql(f"SHOW SCHEMAS IN {CATALOGO_ALTERNATIVO} LIKE '{self.prefixo_alternativo}*'").collect()
        return [linha[0] for linha in linhas]

    def _modo_existente(self) -> Modo | None:
        """Modo de um ambiente já criado (ou `None` se ainda não existe nada)."""
        if self._modo is None:
            if self._existe_catalogo(self.catalogo):
                self._modo = "catalogo"
            elif f"{self.prefixo_alternativo}landing" in self._schemas_alternativos():
                self._modo = "alternativo"
        return self._modo

    def _garantir_catalogo(self) -> None:
        if self._modo_existente():
            return
        try:
            self._sql(f"CREATE CATALOG IF NOT EXISTS {self.catalogo}")
            self._modo = "catalogo"
        except Exception as erro:
            if not _sem_permissao(erro):
                raise
            print(
                f"Aviso: sem permissão para criar o catálogo {self.catalogo}. Usando a alternativa: "
                f"catálogo {CATALOGO_ALTERNATIVO} com schemas prefixados ({self.prefixo_alternativo}landing, "
                f"{self.prefixo_alternativo}bronze...)."
            )
            if not self._existe_catalogo(CATALOGO_ALTERNATIVO):
                self._sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO_ALTERNATIVO}")
            self._modo = "alternativo"

    def _apagar(self) -> None:
        modo = self._modo_existente()
        if modo == "catalogo":
            self._sql(f"DROP CATALOG IF EXISTS {self.catalogo} CASCADE")
        elif modo == "alternativo":
            for schema in self._schemas_alternativos():
                self._sql(f"DROP SCHEMA IF EXISTS {CATALOGO_ALTERNATIVO}.{schema} CASCADE")
        self._modo = None


def _sem_permissao(erro: Exception) -> bool:
    texto = str(erro)
    return "PERMISSION_DENIED" in texto or "does not have CREATE CATALOG" in texto


__all__ = ["CATALOGO_ALTERNATIVO", "DestinoUC"]
