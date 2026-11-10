-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *RotaCerta* (TMS da ModelBricks Cargas)
-- MAGIC
-- MAGIC CDC em JSON lines, como o *Embarque*: uma linha por operação, guardada como veio.
-- MAGIC
-- MAGIC O JSON fica inteiro na coluna `payload`, do tipo VARIANT (`singleVariantColumn`): o valor e a precisão dos números chegam exatos, e um campo novo na fonte aparece dentro do documento sem quebrar a ingestão. Consulte um campo com o caminho e o tipo, por exemplo `payload:seq::BIGINT`.
-- MAGIC
-- MAGIC Tabelas: `clientes`, `caminhoes`, `fretes`, no schema `rotacerta` do catálogo da Bronze, ao lado do Volume `landing`.
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

CREATE OR REFRESH STREAMING TABLE rotacerta.clientes
CLUSTER BY AUTO
COMMENT 'Empresas clientes (pessoa jurídica) da ModelBricks Cargas, vindas do RotaCerta, o TMS (sistema de gestão de
transporte). Uma linha por operação de CDC, em JSON na coluna VARIANT payload (leia com payload:campo::TIPO).
Controle: op (I inclusão, U atualização, D exclusão), seq (ordem global das operações no sistema) e
ts_operacao (instante da operação). Cada cliente tem um único I. Campos: id_cliente (chave), cnpj (14
dígitos), razao_social, email, telefone, cod_municipio (IBGE da sede, origem dos fretes) e data_cadastro.
Bronze append-only: cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de
ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing), _system_origin
(sistema) e _load_type (append).'
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
  'rotacerta'           AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/rotacerta/landing/clientes/',
  format => 'json',
  singleVariantColumn => 'payload'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE rotacerta.caminhoes
CLUSTER BY AUTO
COMMENT 'Caminhões da frota própria da ModelBricks Cargas, vindos do RotaCerta. Uma linha por operação de CDC, em JSON
na coluna VARIANT payload (leia com payload:campo::TIPO). Controle: op (I inclusão, U atualização, D
exclusão), seq (ordem global das operações no sistema) e ts_operacao (instante da operação). Cada caminhão tem
um único I, no dia da aquisição. Campos: id_caminhao (chave), placa (liga com patio.veiculos, rastro.eventos e
nucleo.ativos), marca, modelo, ano_fabricacao e data_aquisicao. Bronze append-only: cada arquivo entra como
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
  'rotacerta'           AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/rotacerta/landing/caminhoes/',
  format => 'json',
  singleVariantColumn => 'payload'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE rotacerta.fretes
CLUSTER BY AUTO
COMMENT 'Ordens de frete da ModelBricks Cargas, da abertura à entrega, vindas do RotaCerta. Uma linha por operação de
CDC, em JSON na coluna VARIANT payload (leia com payload:campo::TIPO). Controle: op (I inclusão, U
atualização, D exclusão), seq (ordem global das operações no sistema) e ts_operacao (instante da operação). I
é a abertura e cada U é uma mudança de status, com a imagem completa do frete (5 a 8 operações por frete).
Campos: id_frete (chave), id_cliente (rotacerta.clientes), id_caminhao (rotacerta.caminhoes),
cod_municipio_origem e cod_municipio_destino (IBGE), peso_kg, valor_frete (R$), data_hora_abertura,
data_prevista_entrega e status (aberto, coleta_agendada, coletado, em_transito, em_transbordo,
em_rota_de_entrega, entrega_frustrada, entregue). Cerca de 2% das mudanças chegam atrasadas, com seq fora de
ordem: para ordenar o histórico de um frete, use ts_operacao. Bronze append-only: cada arquivo entra como
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
  'rotacerta'           AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/rotacerta/landing/fretes/',
  format => 'json',
  singleVariantColumn => 'payload'
);
