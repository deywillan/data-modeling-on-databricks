-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consultas de verificação do artigo 02 (Historização)
-- MAGIC
-- MAGIC Rode depois do pipeline `02_historizacao`. Este notebook **não** faz parte do pipeline: ele só consulta as
-- MAGIC tabelas que o pipeline criou. Os nomes usam o target `dev` (`bronze_dev`, `silver_dev`, `gold_dev`); no target
-- MAGIC `prod`, troque o sufixo por `_prd`.
-- MAGIC
-- MAGIC Para ver o reajuste de tarifas de 12/01/2026 como mudança, rode o pipeline uma vez com a carga inicial, avance o
-- MAGIC relógio (`avancar_tempo` com `dias=15`, a partir de 31/12/2025) e rode `01_bronze` e `02_historizacao` de novo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD1 × SCD2 nas tarifas
-- MAGIC
-- MAGIC Depois do reajuste, cada tarifa tem uma linha no SCD1 e duas versões no SCD2. A reentrega de CDC (cerca de 1%
-- MAGIC das operações chega duas vezes) não gera versão a mais: o máximo de versões por tarifa continua 2.

-- COMMAND ----------

SELECT 'scd1' AS tabela, count(*) AS linhas, count(DISTINCT id_tarifa) AS tarifas, NULL AS maximo_de_versoes
FROM silver_dev.historizacao.embarque_tarifas_scd1
UNION ALL
SELECT 'scd2', count(*), count(DISTINCT id_tarifa), max(versoes)
FROM (
  SELECT id_tarifa, count(*) OVER (PARTITION BY id_tarifa) AS versoes
  FROM silver_dev.historizacao.embarque_tarifas_scd2
);

-- COMMAND ----------

-- Uma tarifa nas duas tabelas
WITH uma AS (SELECT min(id_tarifa) AS id_tarifa FROM silver_dev.historizacao.embarque_tarifas_scd2)
SELECT 'scd1' AS tabela, t.id_tarifa, t.valor, NULL AS inicio, NULL AS fim
FROM silver_dev.historizacao.embarque_tarifas_scd1 t JOIN uma USING (id_tarifa)
UNION ALL
SELECT 'scd2', t.id_tarifa, t.valor, t.__START_AT, t.__END_AT
FROM silver_dev.historizacao.embarque_tarifas_scd2 t JOIN uma USING (id_tarifa)
ORDER BY tabela, inicio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A tarifa vigente na data da venda
-- MAGIC
-- MAGIC O valor de cada passagem do *Embarque* é a tarifa vigente no dia da venda. Com o SCD2, todas as passagens
-- MAGIC batem, antes e depois do reajuste; com o SCD1, só as vendidas depois de 12/01/2026. O `DISTINCT` descarta as
-- MAGIC operações reentregues, que a Bronze guarda duas vezes.

-- COMMAND ----------

WITH vendas AS (
  SELECT DISTINCT
    payload:id_passagem::BIGINT          AS id_passagem,
    payload:cod_linha::BIGINT            AS cod_linha,
    payload:data_hora_venda::TIMESTAMP   AS data_hora_venda,
    payload:valor::DECIMAL(10,2)         AS valor
  FROM bronze_dev.embarque.passagens
  WHERE payload:op::STRING = 'I'
)
SELECT
  date_trunc('month', v.data_hora_venda)   AS mes,
  count(*)                                 AS passagens,
  count_if(v.valor = t2.valor)             AS bate_com_scd2,
  count_if(v.valor = t1.valor)             AS bate_com_scd1
FROM vendas v
JOIN silver_dev.historizacao.embarque_tarifas_scd2 t2
  ON  t2.cod_linha = v.cod_linha
  AND v.data_hora_venda >= t2.__START_AT
  AND (t2.__END_AT IS NULL OR v.data_hora_venda < t2.__END_AT)
JOIN silver_dev.historizacao.embarque_tarifas_scd1 t1
  ON  t1.cod_linha = v.cod_linha
WHERE v.data_hora_venda >= '2025-11-01'
GROUP BY ALL
ORDER BY mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A reentrega de CDC na Bronze
-- MAGIC
-- MAGIC A Bronze guarda as duas cópias de cada operação reentregue, em arquivos de dias diferentes.

-- COMMAND ----------

SELECT payload:seq::BIGINT AS seq, count(*) AS entregas, collect_list(_file_name) AS arquivos
FROM bronze_dev.embarque.tarifas
GROUP BY ALL
HAVING count(*) > 1
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD2 a partir das fotos do *Pátio*
-- MAGIC
-- MAGIC O cliente com mais versões. Até novembro de 2025, as versões começam no último dia de um mês (fotos mensais);
-- MAGIC a partir de dezembro, em qualquer dia (fotos diárias).

-- COMMAND ----------

WITH mais_versoes AS (
  SELECT id_cliente
  FROM silver_dev.historizacao.patio_clientes_scd2
  GROUP BY id_cliente
  ORDER BY count(*) DESC, id_cliente
  LIMIT 1
)
SELECT c.id_cliente, c.telefone, c.email, c.cod_municipio, c.__START_AT, c.__END_AT
FROM silver_dev.historizacao.patio_clientes_scd2 c
JOIN mais_versoes USING (id_cliente)
ORDER BY c.__START_AT;

-- COMMAND ----------

-- Versões abertas por mês: a granularidade muda em dezembro de 2025
SELECT
  date_trunc('month', __START_AT)                 AS mes,
  count(*)                                        AS versoes_abertas,
  count(DISTINCT __START_AT)                      AS dias_distintos
FROM silver_dev.historizacao.patio_clientes_scd2
GROUP BY ALL
ORDER BY mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD0 ao lado do SCD2
-- MAGIC
-- MAGIC No *Pátio*, as datas nunca mudam na fonte, então as duas tabelas coincidem: a consulta deve devolver zero.
-- MAGIC A diferença apareceria no dia em que a fonte corrigisse uma data, que o SCD2 acompanharia e o SCD0, não.

-- COMMAND ----------

SELECT count(*) AS clientes_com_data_diferente
FROM silver_dev.historizacao.patio_clientes_scd2 s
JOIN silver_dev.historizacao.patio_clientes_atributos_fixos f USING (id_cliente)
WHERE s.__END_AT IS NULL
  AND (s.data_nascimento IS DISTINCT FROM f.data_nascimento
       OR s.data_cadastro IS DISTINCT FROM f.data_cadastro);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exclusão lógica nos fornecedores do *Núcleo*
-- MAGIC
-- MAGIC Os fornecedores com `excluido = true` na versão mais recente não aparecem na tabela SCD1.

-- COMMAND ----------

WITH ultima AS (
  SELECT codigo_fornecedor, max_by(excluido, updated_at) AS excluido
  FROM bronze_dev.nucleo.fornecedores
  GROUP BY codigo_fornecedor
)
SELECT
  count_if(NOT u.excluido)                          AS ativos_na_bronze,
  count_if(u.excluido)                              AS excluidos_na_bronze,
  (SELECT count(*) FROM silver_dev.historizacao.nucleo_fornecedores_scd1) AS linhas_no_scd1
FROM ultima u;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Accumulating snapshot do frete
-- MAGIC
-- MAGIC Mediana de horas da coleta à entrega e fretes entregues depois da data prevista.

-- COMMAND ----------

SELECT
  percentile(timestampdiff(HOUR, coletado_em, entregue_em), 0.5)  AS horas_mediana_coleta_a_entrega,
  count_if(to_date(entregue_em) > data_prevista_entrega)          AS entregues_com_atraso,
  count(*)                                                        AS fretes_entregues
FROM gold_dev.historizacao.fato_ciclo_frete
WHERE entregue_em IS NOT NULL;

-- COMMAND ----------

-- Fretes por etapa alcançada: os em andamento têm as etapas seguintes nulas
SELECT status_atual, count(*) AS fretes
FROM gold_dev.historizacao.fato_ciclo_frete
GROUP BY ALL
ORDER BY fretes DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Periodic snapshot do valor contábil
-- MAGIC
-- MAGIC Patrimônio da frota por mês. O valor contábil é semiaditivo: soma-se entre ativos de um mesmo mês, nunca
-- MAGIC entre meses.

-- COMMAND ----------

SELECT mes_referencia, empresa, count(*) AS ativos, sum(valor_contabil) AS valor_contabil_total
FROM gold_dev.historizacao.fato_valor_contabil_mensal
GROUP BY ALL
ORDER BY mes_referencia, empresa;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mecanismos: Change Data Feed e time travel
-- MAGIC
-- MAGIC A tabela SCD1 de tarifas usa o CDF automático. No commit do reajuste, cada tarifa aparece como
-- MAGIC `update_preimage` (valor antigo) e `update_postimage` (valor novo). `_commit_timestamp` é o instante em que o
-- MAGIC pipeline gravou a mudança, não 12/01/2026: o feed conhece a história da tabela, não a do negócio.

-- COMMAND ----------

SHOW TBLPROPERTIES silver_dev.historizacao.embarque_tarifas_scd1;

-- COMMAND ----------

SELECT id_tarifa, valor, _change_type, _commit_version, _commit_timestamp
FROM table_changes('silver_dev.historizacao.embarque_tarifas_scd1', 0)
WHERE id_tarifa = (SELECT min(id_tarifa) FROM silver_dev.historizacao.embarque_tarifas_scd1)
ORDER BY _commit_version, _change_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O time travel mostra a tabela inteira numa versão anterior. No histórico, a versão do primeiro `MERGE` é a carga
-- MAGIC inicial, e a do último, o reajuste. Se você rodou o pipeline mais vezes antes do reajuste, troque o número abaixo. O time travel depende da retenção do log e dos arquivos: não é histórico de
-- MAGIC negócio.

-- COMMAND ----------

DESCRIBE HISTORY silver_dev.historizacao.embarque_tarifas_scd1;

-- COMMAND ----------

SELECT id_tarifa, valor
FROM silver_dev.historizacao.embarque_tarifas_scd1 VERSION AS OF 2  -- o primeiro MERGE, antes do reajuste
ORDER BY id_tarifa
LIMIT 10;
