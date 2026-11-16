-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Historização: SCD1 e SCD2 nas tarifas do *Embarque*
-- MAGIC
-- MAGIC As duas tabelas saem da mesma origem, a Bronze de CDC das tarifas (`embarque.tarifas`), e diferem só no
-- MAGIC `STORED AS SCD TYPE`. Em 12/01/2026 todas as tarifas recebem um `U` com reajuste de 8,5%: o SCD1 guarda só o
-- MAGIC valor novo, e o SCD2 guarda as duas versões, com a validade em `__START_AT` e `__END_AT`.
-- MAGIC
-- MAGIC - **Tipagem:** a Bronze guarda o JSON inteiro em `payload` (VARIANT). A view temporária `tarifas_cdc` extrai
-- MAGIC   cada campo com o caminho e o tipo, uma vez, para os dois fluxos.
-- MAGIC - **`SEQUENCE BY ts_operacao`:** o `AUTO CDC` usa a coluna de sequência para preencher `__START_AT` e
-- MAGIC   `__END_AT`. Com `ts_operacao`, a validade é um instante, que dá para cruzar com a data da venda; com `seq`,
-- MAGIC   seria um número.
-- MAGIC - **Reentrega:** cerca de 1% das operações chega duas vezes, com a mesma `seq` e o mesmo `ts_operacao`. O
-- MAGIC   `AUTO CDC` reconhece a operação já aplicada, e a tabela não ganha versão a mais.
-- MAGIC - **CDF:** o pipeline liga o Change Data Feed clássico nas tabelas de `AUTO CDC` por conta própria, e o feed
-- MAGIC   só vale a partir da versão em que ele foi ligado. A tabela SCD1 é criada com a propriedade em `false`, e o
-- MAGIC   CDF automático assume, com feed desde a primeira versão. O artigo usa esse feed na seção dos mecanismos.
-- MAGIC - **Metadados da Bronze** (`_ingestion_at`, `_file_name`, `_system_origin`, `_load_type`) não vão para a
-- MAGIC   Silver: no SCD2, uma coluna que muda a cada entrega abriria versões falsas.

-- COMMAND ----------

CREATE TEMPORARY VIEW tarifas_cdc
AS SELECT
  payload:op::STRING                      AS op,
  payload:ts_operacao::TIMESTAMP          AS ts_operacao,
  payload:id_tarifa::BIGINT               AS id_tarifa,
  payload:cod_linha::BIGINT               AS cod_linha,
  payload:cod_municipio_origem::INT       AS cod_municipio_origem,
  payload:cod_municipio_destino::INT      AS cod_municipio_destino,
  payload:valor::DECIMAL(10,2)            AS valor,
  payload:vigente_desde::DATE             AS vigente_desde
FROM STREAM(${catalogo_bronze}.embarque.tarifas);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE embarque_tarifas_scd1
COMMENT 'Tarifas das linhas de ônibus da ModelBricks Viação (sistema Embarque), em SCD1: uma linha por tarifa, com o
valor atual. Depois do reajuste de 12/01/2026, o valor anterior não existe mais nesta tabela. Aplicada pelo AUTO CDC
sobre a Bronze de CDC (embarque.tarifas), na ordem de ts_operacao; operações D removem a tarifa. Exemplo de SCD1 do
artigo 02 (Historização). CDF automático (delta.enableChangeDataFeed = false), com feed desde a primeira versão.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false');

CREATE FLOW embarque_tarifas_scd1_fluxo
AS AUTO CDC INTO embarque_tarifas_scd1
FROM STREAM(tarifas_cdc)
KEYS (id_tarifa)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY ts_operacao
COLUMNS * EXCEPT (op, ts_operacao)
STORED AS SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE embarque_tarifas_scd2
COMMENT 'Tarifas das linhas de ônibus da ModelBricks Viação (sistema Embarque), em SCD2: uma linha por versão de
tarifa, válida de __START_AT (inclusive) a __END_AT (exclusive); __END_AT nulo indica a versão vigente. O reajuste de
12/01/2026 fecha a versão original e abre uma nova. Aplicada pelo AUTO CDC sobre a Bronze de CDC
(embarque.tarifas), na ordem de ts_operacao; operações D fecham a versão. Para o valor na data de um fato, junte
por cod_linha com data >= __START_AT e (__END_AT IS NULL ou data < __END_AT). Exemplo de SCD2 do artigo 02
(Historização).';

CREATE FLOW embarque_tarifas_scd2_fluxo
AS AUTO CDC INTO embarque_tarifas_scd2
FROM STREAM(tarifas_cdc)
KEYS (id_tarifa)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY ts_operacao
COLUMNS * EXCEPT (op, ts_operacao)
STORED AS SCD TYPE 2;
