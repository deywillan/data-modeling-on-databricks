"""Confere, por SQL e pela API de arquivos, o que o setup de um artigo deixou no Unity Catalog.

Uso (da pasta 00-gerador/, com o perfil da sua CLI):

    DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py medalhao_01_bronze

Imprime schemas, Volumes, tabelas por schema e arquivos da landing (com tamanho e data de
modificação), e termina com erro se bronze, silver ou gold tiverem alguma tabela.
"""

import sys
from datetime import datetime, timezone

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


def main(catalogo: str) -> int:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    print("Schemas:", ", ".join(r[0] for r in spark.sql(f"SHOW SCHEMAS IN {catalogo}").collect()))
    print("Volumes em landing:", ", ".join(r["volume_name"] for r in spark.sql(f"SHOW VOLUMES IN {catalogo}.landing").collect()))
    erros = 0
    for schema in ("bronze", "silver", "gold"):
        tabelas = spark.sql(f"SHOW TABLES IN {catalogo}.{schema}").count()
        print(f"Tabelas em {schema}: {tabelas}")
        erros += tabelas

    w = WorkspaceClient()
    pendentes = [f"/Volumes/{catalogo}/landing/landing"]
    print("Arquivos na landing:")
    while pendentes:
        for item in w.files.list_directory_contents(pendentes.pop()):
            if item.is_directory:
                pendentes.append(item.path)
            else:
                quando = datetime.fromtimestamp(item.last_modified / 1000, tz=timezone.utc)
                print(f"  {item.path}  {item.file_size} bytes  {quando:%Y-%m-%d %H:%M:%S} UTC")
    return 1 if erros else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
