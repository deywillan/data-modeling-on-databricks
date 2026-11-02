"""Destino: onde o gerador guarda o que produziu.

O destino não gera nem serializa nada: recebe bytes prontos do escritor único e os guarda
nos Volumes do catálogo Bronze da série, e recebe DataFrames do gabarito para gravar como
tabelas no catálogo Silver. Há duas implementações: diretório local (testes) e Unity Catalog
(jobs no workspace). As duas seguem a mesma estrutura:

    bronze_<amb>/<sistema>/landing/<entidade>/<data>/<arquivo>   Volume `landing` de cada sistema
    bronze_<amb>/_controle/relogio/<data>.json                  relógio simulado
    silver_<amb>/gabarito/<tabela>                              mundo verdadeiro, chaves e defeitos
    gold_<amb>/                                                 nasce vazio

Os caminhos de arquivo do contrato começam pelo sistema (ex.:
`ibge/municipios/2025-12-31/municipios_20251231_0001.csv`) ou por `_controle/relogio/`, com `/`
como separador; o destino os mapeia para o schema e o Volume certos (`localizar`).
"""

import shutil
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from .catalogos import catalogos_da_serie, validar_ambiente
from .escritor import Arquivo

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SCHEMA_CONTROLE = "_controle"  # o Unity Catalog aceita o nome com o sublinhado (verificado no Free Edition)
VOLUME_LANDING = "landing"  # Volume de cada schema de sistema
VOLUME_RELOGIO = "relogio"  # Volume do schema de controle
SCHEMA_GABARITO = "gabarito"  # no catálogo Silver


class ArquivoJaExiste(FileExistsError):
    """A landing nunca sobrescreve: já existe um arquivo com esse caminho e outro conteúdo."""


@dataclass(frozen=True)
class Local:
    """Onde um caminho do contrato fica: schema, Volume e o caminho dentro do Volume."""

    schema: str
    volume: str
    resto: str


def localizar(caminho: str) -> Local:
    """Mapeia um caminho do contrato para schema, Volume e caminho no Volume.

    `embarque/passagens/2025-12-31/x.csv` → schema `embarque`, Volume `landing`;
    `_controle/relogio/2025-12-31.json` → schema `_controle`, Volume `relogio`."""
    partes = [p for p in caminho.split("/") if p]
    if not partes:
        raise ValueError("Caminho vazio: o primeiro componente é o sistema (ou _controle).")
    if partes[0] == SCHEMA_CONTROLE:
        if len(partes) < 2:
            raise ValueError(f"Caminho incompleto em {SCHEMA_CONTROLE}: {caminho}")
        return Local(SCHEMA_CONTROLE, partes[1], "/".join(partes[2:]))
    return Local(partes[0], VOLUME_LANDING, "/".join(partes[1:]))


def caminho_do_contrato(local: Local) -> str:
    """O inverso de `localizar`: o caminho do contrato de um arquivo em `local`."""
    if local.schema == SCHEMA_CONTROLE:
        return f"{SCHEMA_CONTROLE}/{local.volume}/{local.resto}"
    return f"{local.schema}/{local.resto}"


class Destino(Protocol):
    """Contrato que toda implementação de destino cumpre."""

    ambiente: str  # ambiente dos catálogos (ex.: `dev`)

    def preparar(self, sistemas: Sequence[str], reset: bool = False) -> None:
        """Cria o que faltar: os três catálogos da série, um schema por sistema no Bronze (com o
        Volume `landing`), o schema de controle (com o Volume do relógio) e o schema `gabarito`
        no Silver. Com `reset`, apaga os três catálogos antes."""

    def gravar_arquivos(self, arquivos: Iterable[Arquivo]) -> None:
        """Grava arquivos novos na landing, sem nunca sobrescrever. Um arquivo que já existe
        com exatamente os mesmos bytes conta como gravado (é o que torna segura a nova tentativa
        de uma chamada que caiu no meio); com bytes diferentes, levanta `ArquivoJaExiste`."""

    def listar(self, prefixo: str) -> list[str]:
        """Caminhos do contrato (ordenados) dos arquivos sob o prefixo (`""` lista tudo);
        vazio se não houver."""

    def ler(self, caminho: str) -> bytes:
        """Conteúdo de um arquivo."""

    def gravar_gabarito(self, tabela: str, df: "DataFrame", acrescentar: bool = False) -> None:
        """Grava uma tabela no schema `gabarito` do Silver, substituindo-a (padrão) ou, com
        `acrescentar`, somando as linhas às que já existem."""

    def apagar_do_gabarito(self, tabela: str, coluna: str, a_partir_de: date) -> None:
        """Remove da tabela do gabarito as linhas cuja `coluna` (uma data) é `a_partir_de` ou
        posterior; linhas sem data ficam. Não faz nada se a tabela ainda não existe."""


class DestinoLocal:
    """Destino em diretório local, espelho do Unity Catalog para os testes.

    Layout: `<raiz>/<catálogo>/<schema>/<volume>/...`, com os mesmos nomes do Unity Catalog
    (veja o cabeçalho do módulo). Cada tabela do gabarito vira uma pasta
    `<raiz>/silver_<amb>/gabarito/<tabela>/parte-0000.parquet`. Ambientes diferentes convivem
    na mesma raiz, em catálogos de nomes diferentes.
    """

    def __init__(self, raiz: str | Path, ambiente: str = "dev"):
        self.raiz = Path(raiz)
        self.ambiente = validar_ambiente(ambiente)
        self.catalogos = catalogos_da_serie(ambiente)

    def pasta(self, camada: str) -> Path:
        """Pasta do catálogo da camada (`bronze`, `silver` ou `gold`)."""
        return self.raiz / self.catalogos[camada]

    def pasta_sistema(self, sistema: str) -> Path:
        """Raiz do Volume `landing` do sistema."""
        return self.pasta("bronze") / sistema / VOLUME_LANDING

    def pasta_gabarito(self) -> Path:
        return self.pasta("silver") / SCHEMA_GABARITO

    def preparar(self, sistemas: Sequence[str], reset: bool = False) -> None:
        if reset:
            for camada in self.catalogos:
                shutil.rmtree(self.pasta(camada), ignore_errors=True)
        for camada in self.catalogos:
            self.pasta(camada).mkdir(parents=True, exist_ok=True)
        for sistema in sistemas:
            self.pasta_sistema(sistema).mkdir(parents=True, exist_ok=True)
        (self.pasta("bronze") / SCHEMA_CONTROLE / VOLUME_RELOGIO).mkdir(parents=True, exist_ok=True)
        self.pasta_gabarito().mkdir(parents=True, exist_ok=True)

    def gravar_arquivos(self, arquivos: Iterable[Arquivo]) -> None:
        for arquivo in arquivos:
            local = localizar(arquivo.caminho)
            if not (self.pasta("bronze") / local.schema).is_dir():
                raise ValueError(
                    f"O schema '{local.schema}' não existe no catálogo {self.catalogos['bronze']}: "
                    "o sistema não faz parte da carga."
                )
            alvo = self._caminho(arquivo.caminho)
            alvo.parent.mkdir(parents=True, exist_ok=True)
            try:
                with alvo.open("xb") as saida:
                    saida.write(arquivo.conteudo)
            except FileExistsError:
                if alvo.read_bytes() != arquivo.conteudo:
                    raise ArquivoJaExiste(
                        f"A landing nunca sobrescreve: {arquivo.caminho} já existe com outro conteúdo."
                    ) from None

    def listar(self, prefixo: str) -> list[str]:
        prefixo = prefixo.strip("/")
        bronze = self.pasta("bronze")
        if not bronze.is_dir():
            return []
        schemas = [prefixo.split("/")[0]] if prefixo else sorted(p.name for p in bronze.iterdir() if p.is_dir())
        caminhos = []
        for schema in schemas:
            for volume in sorted(p for p in (bronze / schema).glob("*") if p.is_dir()):
                for arquivo in volume.rglob("*"):
                    if arquivo.is_file():
                        resto = arquivo.relative_to(volume).as_posix()
                        caminho = caminho_do_contrato(Local(schema, volume.name, resto))
                        if not prefixo or caminho == prefixo or caminho.startswith(prefixo + "/"):
                            caminhos.append(caminho)
        return sorted(caminhos)

    def ler(self, caminho: str) -> bytes:
        return self._caminho(caminho).read_bytes()

    def gravar_gabarito(self, tabela: str, df: "DataFrame", acrescentar: bool = False) -> None:
        import pyarrow.parquet as pq

        pasta = self.pasta_gabarito() / tabela
        if pasta.exists() and not acrescentar:
            shutil.rmtree(pasta)
        pasta.mkdir(parents=True, exist_ok=True)
        parte = len(list(pasta.glob("parte-*.parquet")))
        pq.write_table(df.toArrow(), pasta / f"parte-{parte:04d}.parquet")

    def apagar_do_gabarito(self, tabela: str, coluna: str, a_partir_de: date) -> None:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        # Cada parte é regravada no lugar (nunca apagada): `gravar_gabarito` numera a próxima
        # parte pela contagem, então remover uma do meio faria a seguinte sobrescrever outra.
        for caminho in sorted((self.pasta_gabarito() / tabela).glob("parte-*.parquet")):
            dados = pq.read_table(caminho)
            limite = pa.scalar(a_partir_de, dados.schema.field(coluna).type)
            manter = pc.or_(pc.is_null(dados[coluna]), pc.less(dados[coluna], limite))
            if not pc.all(manter).as_py():
                pq.write_table(dados.filter(manter), caminho)

    def _caminho(self, caminho: str) -> Path:
        local = localizar(caminho)
        volume = self.pasta("bronze") / local.schema / local.volume
        alvo = (volume / local.resto).resolve()
        if not alvo.is_relative_to(volume.resolve()):
            raise ValueError(f"Caminho fora da landing: {caminho}")
        return alvo
