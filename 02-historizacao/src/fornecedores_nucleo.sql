-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Historização: exclusão lógica nos fornecedores do *Núcleo*
-- MAGIC
-- MAGIC O *Núcleo* não manda operações `I`, `U` e `D`. Manda a linha inteira sempre que ela muda, com `updated_at`, e
-- MAGIC marca a exclusão com `excluido = true` (*soft delete*). O mesmo `AUTO CDC` das tarifas serve a esse padrão de
-- MAGIC entrega: `SEQUENCE BY updated_at` e `APPLY AS DELETE WHEN excluido = true`.
-- MAGIC
-- MAGIC A Bronze do *Núcleo* vem de Parquet e já tem os tipos do arquivo, então o fluxo lê a Bronze direto e só deixa de
-- MAGIC fora, com `COLUMNS * EXCEPT`, a flag de exclusão, a coluna `_rescued_data` do Auto Loader e os metadados de
-- MAGIC ingestão. Com `SCD TYPE 1`, o fornecedor excluído some da tabela; com `SCD TYPE 2`, a versão dele seria fechada,
-- MAGIC e a tabela continuaria sabendo que ele existiu.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE nucleo_fornecedores_scd1
COMMENT 'Fornecedores (pessoa jurídica) do Grupo ModelBricks (sistema Núcleo), em SCD1: uma linha por fornecedor
ativo, com a versão mais recente. Aplicada pelo AUTO CDC sobre a Bronze incremental (nucleo.fornecedores), na ordem
de updated_at; a versão com excluido = true remove o fornecedor (exclusão lógica na fonte, exclusão física aqui).
Exemplo de AUTO CDC sobre entrega incremental do artigo 02 (Historização).';

CREATE FLOW nucleo_fornecedores_scd1_fluxo
AS AUTO CDC INTO nucleo_fornecedores_scd1
FROM STREAM(${catalogo_bronze}.nucleo.fornecedores)
KEYS (codigo_fornecedor)
APPLY AS DELETE WHEN excluido = true
SEQUENCE BY updated_at
COLUMNS * EXCEPT (excluido, _rescued_data,
                  _ingestion_at, _file_name, _system_origin, _load_type)
STORED AS SCD TYPE 1;
