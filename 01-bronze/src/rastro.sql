-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *Rastro* (telemetria da frota própria)
-- MAGIC
-- MAGIC JSON lines aninhado, um evento por linha, em até 24 arquivos pequenos por dia. Cada evento traz o dispositivo, a posição, a telemetria do motor e uma lista de alertas, tudo dentro do VARIANT.
-- MAGIC
-- MAGIC O JSON fica inteiro na coluna `payload`, do tipo VARIANT (`singleVariantColumn`): o valor e a precisão dos números chegam exatos, e um campo novo na fonte aparece dentro do documento sem quebrar a ingestão. Consulte um campo com o caminho e o tipo, por exemplo `payload:seq::BIGINT`.
-- MAGIC
-- MAGIC Tabelas: `eventos`, no schema `rastro` do catálogo da Bronze, ao lado do Volume `landing`.
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

CREATE OR REFRESH STREAMING TABLE rastro.eventos
CLUSTER BY AUTO
COMMENT 'Eventos de GPS e telemetria da frota própria (ônibus da ModelBricks Viação e caminhões da ModelBricks Cargas),
vindos do Rastro, a cada 15 minutos de operação. Uma linha por evento, em JSON aninhado na coluna VARIANT
payload (leia com payload:campo::TIPO). Campos: id_evento (chave), ts_evento, dispositivo (id_dispositivo,
placa, tipo_veiculo onibus ou caminhao, empresa mb_viacao ou mb_cargas), posicao (latitude, longitude,
velocidade_kmh, direcao_graus), telemetria (odometro_km, combustivel_pct, temperatura_motor_c, rpm) e alertas
(lista com tipo excesso_velocidade ou frenagem_brusca e valor). A placa liga com patio.veiculos,
rotacerta.caminhoes e nucleo.ativos. Cerca de 2% dos eventos chegam de 1 a 3 dias atrasados: para a data do
evento, use ts_evento, não _ingestion_at. A carga inicial traz só os 90 dias até 31/12/2025. Bronze append-
only: cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de ingestão:
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
  'rastro'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/rastro/landing/eventos/',
  format => 'json',
  singleVariantColumn => 'payload'
);
