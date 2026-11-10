-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *Núcleo* (ERP corporativo da holding)
-- MAGIC
-- MAGIC Incremental por `updated_at` em Parquet: cada lote traz as linhas alteradas no dia, com os tipos do próprio arquivo. A exclusão é lógica e chega como mais uma versão, com `excluido = true`.
-- MAGIC
-- MAGIC As colunas ficam com os tipos que o próprio Parquet carrega. O que não couber no schema conhecido vai para `_rescued_data`.
-- MAGIC
-- MAGIC Tabelas: `colaboradores`, `fornecedores`, `titulos_pagar`, `notas_fiscais`, `ativos`, no schema `nucleo` do catálogo da Bronze, ao lado do Volume `landing`.
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

CREATE OR REFRESH STREAMING TABLE nucleo.colaboradores
CLUSTER BY AUTO
COMMENT 'Colaboradores das empresas do Grupo ModelBricks. Lote incremental do Núcleo, ERP corporativo do Grupo
ModelBricks: cada lote diário traz as linhas alteradas no dia, com a imagem completa, e uma linha por versão.
Controle: updated_at (instante da mudança) e excluido (true na exclusão lógica, sempre a última versão). Para
o estado atual, use a versão de maior updated_at por chave e descarte excluido = true. Tipos do próprio
Parquet. Colunas: matricula (chave), nome, cpf, empresa (mb holding, mb_viacao, mb_cargas ou mb_veiculos),
cargo e data_admissao. Cada mudança de cargo gera uma versão nova. Bronze append-only: cada arquivo entra como
chegou, sem correção (a correção é feita na Silver). Metadados de ingestão: _ingestion_at (instante da carga),
_file_name (arquivo de origem na landing), _system_origin (sistema) e _load_type (append).'
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
  'nucleo'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/nucleo/landing/colaboradores/',
  format => 'parquet'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE nucleo.fornecedores
CLUSTER BY AUTO
COMMENT 'Fornecedores (pessoa jurídica) do Grupo ModelBricks. Lote incremental do Núcleo, ERP corporativo do Grupo
ModelBricks: cada lote diário traz as linhas alteradas no dia, com a imagem completa, e uma linha por versão.
Controle: updated_at (instante da mudança) e excluido (true na exclusão lógica, sempre a última versão). Para
o estado atual, use a versão de maior updated_at por chave e descarte excluido = true. Tipos do próprio
Parquet. Colunas: codigo_fornecedor (chave), cnpj, razao_social, email, telefone e cod_municipio (IBGE da
sede). Trocas de telefone geram versões novas; o fornecedor encerrado chega com excluido = true. Bronze
append-only: cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de
ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing), _system_origin
(sistema) e _load_type (append).'
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
  'nucleo'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/nucleo/landing/fornecedores/',
  format => 'parquet'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE nucleo.titulos_pagar
CLUSTER BY AUTO
COMMENT 'Títulos a pagar (contas a pagar) das empresas do Grupo ModelBricks. Lote incremental do Núcleo, ERP
corporativo do Grupo ModelBricks: cada lote diário traz as linhas alteradas no dia, com a imagem completa, e
uma linha por versão. Controle: updated_at (instante da mudança) e excluido (true na exclusão lógica, sempre a
última versão). Para o estado atual, use a versão de maior updated_at por chave e descarte excluido = true.
Tipos do próprio Parquet. Colunas: id_titulo (chave), codigo_fornecedor (nucleo.fornecedores), empresa
devedora (mb, mb_viacao, mb_cargas ou mb_veiculos), categoria (combustivel, pecas, manutencao, servicos,
aluguel ou outros), valor (R$), data_emissao, data_vencimento, status (aberto ou pago) e data_pagamento. O
título cancelado chega com excluido = true. Bronze append-only: cada arquivo entra como chegou, sem correção
(a correção é feita na Silver). Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo
de origem na landing), _system_origin (sistema) e _load_type (append).'
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
  'nucleo'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/nucleo/landing/titulos_pagar/',
  format => 'parquet'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE nucleo.notas_fiscais
CLUSTER BY AUTO
COMMENT 'Notas fiscais de venda de veículos emitidas pela ModelBricks Veículos, uma por venda e com versão única. Lote
incremental do Núcleo, ERP corporativo do Grupo ModelBricks: cada lote diário traz as linhas alteradas no dia,
com a imagem completa, e uma linha por versão. Controle: updated_at (instante da mudança) e excluido (true na
exclusão lógica, sempre a última versão). Para o estado atual, use a versão de maior updated_at por chave e
descarte excluido = true. Tipos do próprio Parquet. Colunas: numero_nota (chave), empresa_emitente,
data_emissao, tipo_cliente (pf, pj ou grupo), documento_cliente (nulo nas vendas para o grupo), nome_cliente,
descricao, placa e valor_total (R$). Bronze append-only: cada arquivo entra como chegou, sem correção (a
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
  'nucleo'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/nucleo/landing/notas_fiscais/',
  format => 'parquet'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE nucleo.ativos
CLUSTER BY AUTO
COMMENT 'Veículos da frota própria registrados como ativo imobilizado da ModelBricks Viação (ônibus) ou da ModelBricks
Cargas (caminhões). Lote incremental do Núcleo, ERP corporativo do Grupo ModelBricks: cada lote diário traz as
linhas alteradas no dia, com a imagem completa, e uma linha por versão. Controle: updated_at (instante da
mudança) e excluido (true na exclusão lógica, sempre a última versão). Para o estado atual, use a versão de
maior updated_at por chave e descarte excluido = true. Tipos do próprio Parquet. Colunas: id_ativo (chave),
empresa, descricao, placa (liga com patio.veiculos, rotacerta.caminhoes e rastro.eventos), ano_fabricacao,
data_aquisicao, valor_aquisicao e valor_contabil (R$, depreciado: uma versão nova no último dia de cada mês).
Bronze append-only: cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de
ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing), _system_origin
(sistema) e _load_type (append).'
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
  'nucleo'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/nucleo/landing/ativos/',
  format => 'parquet'
);
