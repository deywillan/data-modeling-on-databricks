-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *Apólice+* (sistema da ModelBricks Seguros)
-- MAGIC
-- MAGIC Incremental por `updated_at` em CSV: cada lote traz as linhas alteradas no dia. O valor monetário chega como texto no formato brasileiro (`"1.234,56"`) e fica assim: a conversão para número é trabalho da Silver.
-- MAGIC
-- MAGIC As colunas ficam em `STRING`, exatamente como estavam no arquivo (`inferColumnTypes => false`). O que não couber no schema conhecido vai para `_rescued_data`, que o `read_files` acrescenta por padrão.
-- MAGIC
-- MAGIC Tabelas: `segurados`, `apolices`, no schema `apolice` do catálogo da Bronze, ao lado do Volume `landing`.
-- MAGIC
-- MAGIC Toda tabela leva os quatro metadados da série: `_ingestion_at` (instante da ingestão), `_file_name` (nome do arquivo
-- MAGIC de origem), `_system_origin` (sistema de origem) e `_load_type` (`append`). O catálogo vem do parâmetro
-- MAGIC `catalogo_bronze` do pipeline (`bronze_dev` no target `dev`).
-- MAGIC
-- MAGIC Toda tabela também leva as mesmas proteções (explicadas no README do artigo):
-- MAGIC
-- MAGIC - `pipelines.reset.allowed = false`: um full refresh do pipeline não apaga esta tabela. A Bronze é o único lugar
-- MAGIC   onde o histórico das entregas existe, e a landing não é backup dela.
-- MAGIC - `delta.appendOnly = true`: `UPDATE` e `DELETE` são recusados. O `OPTIMIZE` continua funcionando, porque não altera
-- MAGIC   dados.
-- MAGIC - `delta.logRetentionDuration` e `delta.deletedFileRetentionDuration` em 30 dias: o time travel cobre o mesmo período
-- MAGIC   do log.
-- MAGIC - `delta.enableTypeWidening = true`: se a fonte alargar um tipo (`INT` para `BIGINT`), a ingestão não quebra.
-- MAGIC - `CLUSTER BY AUTO`: o Databricks escolhe as colunas de clustering pelas consultas, no lugar de partição.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE apolice.segurados
CLUSTER BY AUTO
COMMENT 'Segurados (pessoa física e jurídica) da ModelBricks Seguros, em lotes incrementais por updated_at vindos do
Apólice+. Cada lote diário traz as linhas alteradas no dia, com a imagem completa: uma linha por versão.
Colunas em STRING, como no CSV. Colunas: codigo_segurado (chave), tipo_pessoa (PF ou PJ), nome, cpf (vazio
para PJ), cnpj (vazio para PF), data_nascimento, email, telefone, cod_municipio (IBGE), data_cadastro e
updated_at. Para o cadastro atual, use a versão de maior updated_at por codigo_segurado. O cpf pode vir com
máscara ou sem os zeros à esquerda. Bronze append-only: cada arquivo entra como chegou, sem correção (a
correção é feita na Silver). Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo de
origem na landing), _system_origin (sistema) e _load_type (append).'
TBLPROPERTIES (
  'pipelines.reset.allowed'            = 'false',
  'delta.appendOnly'                   = 'true',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.enableTypeWidening'           = 'true'
)
AS SELECT
  *,
  current_timestamp()   AS _ingestion_at,
  _metadata.file_name   AS _file_name,
  'apolice'             AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/apolice/landing/segurados/',
  format => 'csv', 
  header => true,
  inferColumnTypes => false
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE apolice.apolices
CLUSTER BY AUTO
COMMENT 'Apólices de seguro da ModelBricks Seguros, em lotes incrementais por updated_at vindos do Apólice+. Cada lote
diário traz as linhas alteradas no dia, com a imagem completa: uma linha por versão. Colunas em STRING, como
no CSV. Colunas: numero_apolice (chave), codigo_segurado (apolice.segurados), ramo (vida, residencial, viagem,
empresarial, responsabilidade_civil, transporte ou veiculo), placa (só no ramo veiculo), inicio_vigencia,
fim_vigencia, importancia_segurada e premio (texto no formato brasileiro, 1.234,56), status (ativa, cancelada
ou encerrada), data_cancelamento e updated_at. Para o status atual, use a versão de maior updated_at por
numero_apolice. Bronze append-only: cada arquivo entra como chegou, sem correção (a correção é feita na
Silver). Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing),
_system_origin (sistema) e _load_type (append).'
TBLPROPERTIES (
  'pipelines.reset.allowed'            = 'false',
  'delta.appendOnly'                   = 'true',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.enableTypeWidening'           = 'true'
)
AS SELECT
  *,
  current_timestamp()   AS _ingestion_at,
  _metadata.file_name   AS _file_name,
  'apolice'             AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/apolice/landing/apolices/',
  format => 'csv', 
  header => true,
  inferColumnTypes => false
);
