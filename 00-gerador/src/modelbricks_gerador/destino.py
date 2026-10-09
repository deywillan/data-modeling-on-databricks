"""Destino: onde o gerador guarda o que produziu.

O destino não gera nem serializa nada: recebe bytes prontos do escritor único e os guarda
no Volume `landing`, e recebe DataFrames do gabarito para gravar como tabelas. Há duas
implementações: diretório local (testes) e Unity Catalog (jobs no workspace).

Todos os caminhos de arquivo são relativos à raiz do Volume `landing`, com `/` como
separador (ex.: `ibge/municipios/2025-12-31/municipios_20251231_0001.csv`).
"""

import shutil
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from .escritor import Arquivo

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ArquivoJaExiste(FileExistsError):
    """A landing nunca sobrescreve: o arquivo já foi entregue antes."""


class Destino(Protocol):
    """Contrato que toda implementação de destino cumpre."""

    def preparar(self, schemas: Sequence[str], reset: bool = False) -> None:
        """Cria os schemas (e o Volume `landing`) que faltarem. Com `reset`, apaga tudo antes."""

    def gravar_arquivos(self, arquivos: Iterable[Arquivo]) -> None:
        """Grava arquivos novos na landing. Levanta `ArquivoJaExiste` se algum já existir."""

    def listar(self, prefixo: str) -> list[str]:
        """Caminhos (relativos à landing, ordenados) dos arquivos sob o prefixo; vazio se não houver."""

    def ler(self, caminho: str) -> bytes:
        """Conteúdo de um arquivo da landing."""

    def gravar_gabarito(self, tabela: str, df: "DataFrame") -> None:
        """Grava (substituindo) uma tabela no schema `gabarito`."""


class DestinoLocal:
    """Destino em diretório local, espelho do Unity Catalog para os testes.

    Layout: `<raiz>/<schema>/` para cada schema; o Volume `landing` fica em
    `<raiz>/landing/landing/` (como `/Volumes/<catálogo>/landing/landing/`) e cada tabela
    do gabarito em `<raiz>/gabarito/<tabela>/parte-0000.parquet`.
    """

    def __init__(self, raiz: str | Path):
        self.raiz = Path(raiz)
        self.volume = self.raiz / "landing" / "landing"

    def preparar(self, schemas: Sequence[str], reset: bool = False) -> None:
        if reset and self.raiz.exists():
            shutil.rmtree(self.raiz)
        for schema in schemas:
            (self.raiz / schema).mkdir(parents=True, exist_ok=True)
        self.volume.mkdir(parents=True, exist_ok=True)

    def gravar_arquivos(self, arquivos: Iterable[Arquivo]) -> None:
        for arquivo in arquivos:
            alvo = self._caminho(arquivo.caminho)
            alvo.parent.mkdir(parents=True, exist_ok=True)
            try:
                with alvo.open("xb") as saida:
                    saida.write(arquivo.conteudo)
            except FileExistsError:
                raise ArquivoJaExiste(f"A landing nunca sobrescreve: {arquivo.caminho} já existe.") from None

    def listar(self, prefixo: str) -> list[str]:
        base = self._caminho(prefixo)
        if not base.is_dir():
            return []
        return sorted(p.relative_to(self.volume).as_posix() for p in base.rglob("*") if p.is_file())

    def ler(self, caminho: str) -> bytes:
        return self._caminho(caminho).read_bytes()

    def gravar_gabarito(self, tabela: str, df: "DataFrame") -> None:
        import pyarrow.parquet as pq

        pasta = self.raiz / "gabarito" / tabela
        if pasta.exists():
            shutil.rmtree(pasta)
        pasta.mkdir(parents=True)
        pq.write_table(df.toArrow(), pasta / "parte-0000.parquet")

    def _caminho(self, relativo: str) -> Path:
        alvo = (self.volume / relativo).resolve()
        if not alvo.is_relative_to(self.volume.resolve()):
            raise ValueError(f"Caminho fora da landing: {relativo}")
        return alvo
