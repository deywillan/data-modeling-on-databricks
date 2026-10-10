"""Confere, por SQL e pela API de arquivos, o que o setup de um artigo deixou no Unity Catalog.

Uso (da pasta 00-gerador/, com o perfil da sua CLI):

    DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py medalhao_01_bronze
    DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py medalhao_01_bronze --arquivos

Imprime schemas, Volumes, tabelas por schema e, da landing, o número de arquivos e o tamanho
por pasta de primeiro nível (um sistema, o controle do relógio). Com `--arquivos`, lista também
cada arquivo (tamanho e data de modificação). A listagem percorre as pastas em paralelo
(`--threads`, 16 por padrão): sequencial, 3,7 mil arquivos levavam cerca de 20 minutos.
Termina com erro se bronze, silver ou gold tiverem alguma tabela.
"""

import argparse
import sys
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


def listar_landing(w: WorkspaceClient, raiz: str, threads: int) -> list:
    """Todos os arquivos sob `raiz`, percorrendo as pastas em paralelo."""

    def listar(pasta: str) -> list:
        return list(w.files.list_directory_contents(pasta))

    arquivos = []
    with ThreadPoolExecutor(max_workers=threads) as pool:
        pendentes = {pool.submit(listar, raiz)}
        while pendentes:
            prontos, pendentes = wait(pendentes, return_when=FIRST_COMPLETED)
            for pronto in prontos:
                for item in pronto.result():
                    if item.is_directory:
                        pendentes.add(pool.submit(listar, item.path))
                    else:
                        arquivos.append(item)
    return sorted(arquivos, key=lambda item: item.path)


def main(catalogo: str, arquivos: bool, threads: int) -> int:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    print("Schemas:", ", ".join(r[0] for r in spark.sql(f"SHOW SCHEMAS IN {catalogo}").collect()))
    print("Volumes em landing:", ", ".join(r["volume_name"] for r in spark.sql(f"SHOW VOLUMES IN {catalogo}.landing").collect()))
    erros = 0
    for schema in ("bronze", "silver", "gold"):
        tabelas = spark.sql(f"SHOW TABLES IN {catalogo}.{schema}").count()
        print(f"Tabelas em {schema}: {tabelas}")
        erros += tabelas

    w = WorkspaceClient()
    raiz = f"/Volumes/{catalogo}/landing/landing"
    inicio = time.monotonic()
    itens = listar_landing(w, raiz, threads)
    segundos = time.monotonic() - inicio

    por_pasta: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for item in itens:
        pasta = item.path.removeprefix(raiz + "/").split("/")[0]
        por_pasta[pasta][0] += 1
        por_pasta[pasta][1] += item.file_size
    print("Landing por pasta (arquivos, bytes):")
    for pasta, (n, tamanho) in sorted(por_pasta.items()):
        print(f"  {pasta}: {n} arquivos, {tamanho} bytes ({tamanho / 1e6:.1f} MB)")
    total = sum(tamanho for _, tamanho in por_pasta.values())
    print(f"Landing total: {len(itens)} arquivos, {total} bytes ({total / 1e6:.1f} MB); listagem em {segundos:.0f} s")

    if arquivos:
        print("Arquivos na landing:")
        for item in itens:
            quando = datetime.fromtimestamp(item.last_modified / 1000, tz=timezone.utc)
            print(f"  {item.path}  {item.file_size} bytes  {quando:%Y-%m-%d %H:%M:%S} UTC")
    return 1 if erros else 0


if __name__ == "__main__":
    analisador = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    analisador.add_argument("catalogo")
    analisador.add_argument("--arquivos", action="store_true", help="lista também cada arquivo")
    analisador.add_argument("--threads", type=int, default=16, help="pastas listadas em paralelo")
    argumentos = analisador.parse_args()
    sys.exit(main(argumentos.catalogo, argumentos.arquivos, argumentos.threads))
