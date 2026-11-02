"""Confere, por SQL e pela API de arquivos, o que o `00_setup` deixou no Unity Catalog.

Uso (da pasta 00-gerador/, com o perfil da sua CLI):

    DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py dev
    DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py dev --arquivos
    DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py dev --relogio 2026-01-01

O argumento é o ambiente dos catálogos (`dev`, `prd` ou o de um teste). Imprime os três
catálogos da série (`bronze_<amb>`, `silver_<amb>`, `gold_<amb>`), os schemas do
Bronze com seus Volumes, as tabelas por schema e, da landing de cada sistema, o número de
arquivos e o tamanho. Com `--arquivos`, lista também cada arquivo (tamanho e data de
modificação). A listagem percorre as pastas em paralelo (`--threads`, 16 por padrão):
sequencial, 3,7 mil arquivos levavam cerca de 20 minutos.

Confere o estado logo depois do setup, antes de rodar os pipelines dos artigos. Termina com
erro se: o Bronze não tiver exatamente um schema por sistema da carga mais o `_controle`;
algum catálogo tiver tabelas fora do `gabarito` do Silver; faltar o schema `gabarito`; ou, com
`--relogio AAAA-MM-DD`, o relógio simulado não estiver nessa data (o relógio atual é o arquivo
de maior data no Volume `relogio` do schema `_controle`).
"""

import argparse
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

from modelbricks_gerador import carregar_roteiro, catalogos_da_serie

SCHEMAS_DO_UNITY_CATALOG = {"default", "information_schema"}


def listar_volume(w: WorkspaceClient, raiz: str, threads: int) -> list:
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


def schemas_de(spark, catalogo: str) -> set[str]:
    linhas = spark.sql(f"SHOW SCHEMAS IN {catalogo}").collect()
    return {linha[0] for linha in linhas} - SCHEMAS_DO_UNITY_CATALOG


def main(ambiente: str, arquivos: bool, threads: int, relogio: str | None = None) -> int:
    carga = carregar_roteiro().carga
    catalogos = catalogos_da_serie(ambiente)
    spark = DatabricksSession.builder.serverless().getOrCreate()
    erros = 0

    existentes = {linha[0] for linha in spark.sql("SHOW CATALOGS").collect()}
    print("Catálogos:", ", ".join(f"{c} ({'ok' if c in existentes else 'AUSENTE'})" for c in catalogos.values()))
    if not set(catalogos.values()) <= existentes:
        return 1

    bronze, silver, gold = catalogos["bronze"], catalogos["silver"], catalogos["gold"]
    schemas = schemas_de(spark, bronze)
    esperados = set(carga.sistemas) | {"_controle"}
    print(f"Schemas em {bronze}:", ", ".join(sorted(schemas)))
    if schemas != esperados:
        print(f"  ERRO: esperado {', '.join(sorted(esperados))}")
        erros += 1
    for schema in sorted(schemas):
        volumes = [r["volume_name"] for r in spark.sql(f"SHOW VOLUMES IN {bronze}.{schema}").collect()]
        tabelas = spark.sql(f"SHOW TABLES IN {bronze}.{schema}").count()
        print(f"  {schema}: Volumes {', '.join(volumes) or '-'}; tabelas {tabelas}")
        erros += tabelas
        if volumes != (["relogio"] if schema == "_controle" else ["landing"]):
            erros += 1

    schemas_silver = schemas_de(spark, silver)
    print(f"Schemas em {silver}:", ", ".join(sorted(schemas_silver)) or "(nenhum)")
    if schemas_silver != {"gabarito"}:
        print("  ERRO: esperado só o schema gabarito")
        erros += 1
    for schema in sorted(schemas_silver):
        tabelas = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {silver}.{schema}").collect()]
        print(f"  {schema}: {len(tabelas)} tabelas ({', '.join(sorted(tabelas))})")
        if schema != "gabarito":
            erros += len(tabelas)
    schemas_gold = schemas_de(spark, gold)
    print(f"Schemas em {gold}:", ", ".join(sorted(schemas_gold)) or "(nenhum)")
    erros += len(schemas_gold)

    w = WorkspaceClient()
    inicio = time.monotonic()
    itens_por_schema = {
        schema: listar_volume(w, f"/Volumes/{bronze}/{schema}/{'relogio' if schema == '_controle' else 'landing'}", threads)
        for schema in sorted(schemas & esperados)
    }
    segundos = time.monotonic() - inicio
    print("Landing por sistema (arquivos, bytes):")
    total_arquivos = total_bytes = 0
    for schema, itens in itens_por_schema.items():
        tamanho = sum(item.file_size for item in itens)
        total_arquivos, total_bytes = total_arquivos + len(itens), total_bytes + tamanho
        print(f"  {schema}: {len(itens)} arquivos, {tamanho} bytes ({tamanho / 1e6:.1f} MB)")
    print(f"Landing total: {total_arquivos} arquivos, {total_bytes} bytes ({total_bytes / 1e6:.1f} MB); listagem em {segundos:.0f} s")

    if relogio is not None:
        datas = sorted(
            item.path.rsplit("/", 1)[1].removesuffix(".json")
            for item in itens_por_schema.get("_controle", []) if item.path.endswith(".json")
        )
        atual = datas[-1] if datas else None
        print(f"Relógio: {atual} (esperado {relogio})")
        if atual != relogio:
            erros += 1

    if arquivos:
        print("Arquivos na landing:")
        for itens in itens_por_schema.values():
            for item in itens:
                quando = datetime.fromtimestamp(item.last_modified / 1000, tz=timezone.utc)
                print(f"  {item.path}  {item.file_size} bytes  {quando:%Y-%m-%d %H:%M:%S} UTC")
    print("Conferência:", "FALHOU" if erros else "ok")
    return 1 if erros else 0


if __name__ == "__main__":
    analisador = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    analisador.add_argument("ambiente", help="ambiente dos catálogos, ex.: dev")
    analisador.add_argument("--arquivos", action="store_true", help="lista também cada arquivo")
    analisador.add_argument("--threads", type=int, default=16, help="pastas listadas em paralelo")
    analisador.add_argument("--relogio", metavar="AAAA-MM-DD", help="data em que o relógio simulado deve estar")
    argumentos = analisador.parse_args()
    sys.exit(main(argumentos.ambiente, argumentos.arquivos, argumentos.threads, argumentos.relogio))
