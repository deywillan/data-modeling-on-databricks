-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *Embarque* (bilhetagem da ModelBricks Viação)
-- MAGIC
-- MAGIC CDC em JSON lines: uma linha por operação (`I`, `U` ou `D`), com `op`, `seq` e `ts_operacao`. A Bronze guarda as operações como vieram, sem aplicá-las: transformar operações em estado é trabalho da Silver.
-- MAGIC
-- MAGIC O JSON fica inteiro na coluna `payload`, do tipo VARIANT (`singleVariantColumn`): o valor e a precisão dos números chegam exatos, e um campo novo na fonte aparece dentro do documento sem quebrar a ingestão. Consulte um campo com o caminho e o tipo, por exemplo `payload:seq::BIGINT`.
-- MAGIC
-- MAGIC Tabelas: `clientes`, `tarifas`, `passagens`, no schema `embarque` do catálogo da Bronze, ao lado do Volume `landing`.
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
-- MAGIC - `delta.enableVariantShredding = true`: os campos mais comuns do `payload` são gravados como colunas tipadas
-- MAGIC   dentro do Parquet (*shredding*), e extrair um campo lê só ele, não o documento inteiro. Streaming tables de
-- MAGIC   pipeline não ligam o *shredding* sozinhas, como faz o `CREATE TABLE`; por isso a propriedade é declarada aqui.
-- MAGIC - `CLUSTER BY AUTO`: o Databricks escolhe as colunas de clustering pelas consultas, no lugar de partição.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE embarque.clientes
CLUSTER BY AUTO
COMMENT 'Clientes pessoa física da ModelBricks Viação (ônibus), vindos do Embarque, o sistema de venda de passagens no
guichê e no site. Uma linha por operação de CDC, em JSON na coluna VARIANT payload (leia com
payload:campo::TIPO). Controle: op (I inclusão, U atualização, D exclusão), seq (ordem global das operações no
sistema) e ts_operacao (instante da operação). Cada cliente tem um único I, no dia do cadastro. Campos:
id_cliente (chave), nome, cpf (11 dígitos), data_nascimento, email, telefone, cod_municipio (código IBGE da
residência) e data_cadastro. Cerca de 1% das operações chega duplicada, com a mesma seq. Bronze append-only:
cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de ingestão:
_ingestion_at (instante da carga), _file_name (arquivo de origem na landing), _system_origin (sistema) e
_load_type (append).'
TBLPROPERTIES (
  'pipelines.reset.allowed'            = 'false',
  'delta.appendOnly'                   = 'true',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.enableTypeWidening'           = 'true',
  'delta.enableVariantShredding'       = 'true'
)
AS SELECT
  payload,
  current_timestamp()   AS _ingestion_at,
  _metadata.file_name   AS _file_name,
  'embarque'            AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/embarque/landing/clientes/',
  format => 'json',
  singleVariantColumn => 'payload'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE embarque.tarifas
CLUSTER BY AUTO
COMMENT 'Tarifas das linhas de ônibus da ModelBricks Viação, vindas do Embarque, uma por linha. Uma linha por operação
de CDC, em JSON na coluna VARIANT payload (leia com payload:campo::TIPO). Controle: op (I inclusão, U
atualização, D exclusão), seq (ordem global das operações no sistema) e ts_operacao (instante da operação).
Campos: id_tarifa (chave), cod_linha (código da linha, referenciado por embarque.passagens),
cod_municipio_origem e cod_municipio_destino (IBGE), valor (R$) e vigente_desde. Em 12/01/2026 todas as
tarifas recebem um U com reajuste de 8,5%. Para o valor vigente, use a operação de maior seq por id_tarifa.
Cerca de 1% das operações chega duplicada, com a mesma seq. Bronze append-only: cada arquivo entra como
chegou, sem correção (a correção é feita na Silver). Metadados de ingestão: _ingestion_at (instante da carga),
_file_name (arquivo de origem na landing), _system_origin (sistema) e _load_type (append).'
TBLPROPERTIES (
  'pipelines.reset.allowed'            = 'false',
  'delta.appendOnly'                   = 'true',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.enableTypeWidening'           = 'true',
  'delta.enableVariantShredding'       = 'true'
)
AS SELECT
  payload,
  current_timestamp()   AS _ingestion_at,
  _metadata.file_name   AS _file_name,
  'embarque'            AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/embarque/landing/tarifas/',
  format => 'json',
  singleVariantColumn => 'payload'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE embarque.passagens
CLUSTER BY AUTO
COMMENT 'Passagens de ônibus vendidas pela ModelBricks Viação no guichê e no site, vindas do Embarque. Uma linha por
operação de CDC, em JSON na coluna VARIANT payload (leia com payload:campo::TIPO). Controle: op (I inclusão, U
atualização, D exclusão), seq (ordem global das operações no sistema) e ts_operacao (instante da operação). I
é a venda, U é a mudança de status e D é o estorno (no D só vêm op, seq, ts_operacao e a chave). Campos:
id_passagem (chave), id_cliente (embarque.clientes), cod_linha (embarque.tarifas), data_hora_venda,
data_viagem, poltrona, canal (guiche ou site), valor (R$) e status (emitida, remarcada, cancelada ou
utilizada). Para o estado atual, use a operação de maior seq por id_passagem. Cerca de 1% das operações chega
duplicada, com a mesma seq. As vendas pelo marketplace estão em passagemhub.eventos_venda. Bronze append-only:
cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de ingestão:
_ingestion_at (instante da carga), _file_name (arquivo de origem na landing), _system_origin (sistema) e
_load_type (append).'
TBLPROPERTIES (
  'pipelines.reset.allowed'            = 'false',
  'delta.appendOnly'                   = 'true',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.enableTypeWidening'           = 'true',
  'delta.enableVariantShredding'       = 'true'
)
AS SELECT
  payload,
  current_timestamp()   AS _ingestion_at,
  _metadata.file_name   AS _file_name,
  'embarque'            AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/embarque/landing/passagens/',
  format => 'json',
  singleVariantColumn => 'payload'
);
