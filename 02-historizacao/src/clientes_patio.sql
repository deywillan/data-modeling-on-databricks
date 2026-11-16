-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Historização: SCD2 a partir de fotos e SCD0 nos clientes do *Pátio*
-- MAGIC
-- MAGIC O *Pátio* não manda operações: manda a foto inteira de cada tabela (mensal até 30/11/2025, diária a partir de
-- MAGIC 01/12/2025). A Bronze guarda todas as fotos em append, e a data de cada uma está no nome do arquivo
-- MAGIC (`clientes_AAAAMMDD_NNNN.csv`).
-- MAGIC
-- MAGIC **SCD2 (`patio_clientes_scd2`).** O `AUTO CDC ... FROM SNAPSHOT` compara as fotos em ordem e deduz as mudanças:
-- MAGIC
-- MAGIC - `WITH VERSION` escolhe a próxima foto ainda não processada (a data da foto é a versão);
-- MAGIC - a view temporária `patio_clientes_fotos` dá os tipos da Silver e tira a data da foto do nome do arquivo;
-- MAGIC - `FROM SNAPSHOT` lê essa foto pela view;
-- MAGIC - a primeira atualização percorre todas as fotos da carga inicial, uma por vez; as seguintes, só as novas;
-- MAGIC - um cliente que some da foto seguinte tem a versão fechada nessa data (exclusão por ausência);
-- MAGIC - `TRACK HISTORY ON` limita as versões novas às mudanças de contato e endereço. As demais colunas **não** ficam
-- MAGIC   congeladas: são sobrescritas na versão vigente, como no SCD1.
-- MAGIC
-- MAGIC **SCD0 (`patio_clientes_atributos_fixos`).** O `AUTO CDC` só oferece SCD1 e SCD2. O SCD0 de verdade é o primeiro
-- MAGIC valor que a Bronze recebeu, e uma materialized view o encontra com `min_by` pela foto mais antiga.
-- MAGIC
-- MAGIC Como as fotos espaçadas só enxergam o que captaram, duas trocas de telefone no mesmo mês, antes de dezembro de
-- MAGIC 2025, viram uma versão só.

-- COMMAND ----------

CREATE TEMPORARY VIEW patio_clientes_fotos
AS SELECT
  to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto,
  CAST(id_cliente AS BIGINT)        AS id_cliente,
  tipo_pessoa,
  nome,
  documento,
  CAST(data_nascimento AS DATE)     AS data_nascimento,
  email,
  telefone,
  CAST(cod_municipio AS INT)        AS cod_municipio,
  logradouro,
  CAST(numero AS INT)               AS numero,
  cep,
  CAST(data_cadastro AS DATE)       AS data_cadastro
FROM ${catalogo_bronze}.patio.clientes;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE patio_clientes_scd2
COMMENT 'Clientes pessoa física e jurídica da ModelBricks Veículos (sistema Pátio), em SCD2 deduzido das fotos
completas: uma linha por versão, válida de __START_AT (data da foto em que a mudança apareceu) a __END_AT (data da
foto em que foi substituída ou em que o cliente sumiu). __END_AT nulo indica a versão vigente. Versões novas só
para mudanças de email, telefone, cod_municipio, logradouro, numero e cep (TRACK HISTORY ON); as outras colunas são
sobrescritas na versão vigente. Até 30/11/2025 as fotos são mensais, e as versões começam no último dia do mês; a
partir de 01/12/2025 são diárias. Para as datas fixas (SCD0), use patio_clientes_atributos_fixos. Exemplo de SCD2
a partir de snapshot do artigo 02 (Historização).';

CREATE FLOW patio_clientes_scd2_fluxo
AS AUTO CDC INTO patio_clientes_scd2
FROM SNAPSHOT (
  SELECT * EXCEPT (data_foto)
  FROM patio_clientes_fotos
  WHERE data_foto = (SELECT version FROM current_snapshot_version())
)
WITH VERSION (
  SELECT DISTINCT data_foto AS version
  FROM patio_clientes_fotos
  WHERE NOT EXISTS (SELECT 1 FROM last_snapshot_version())
     OR data_foto > (SELECT version FROM last_snapshot_version())
  ORDER BY version
  LIMIT 1
)
KEYS (id_cliente)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, telefone, cod_municipio, logradouro, numero, cep;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW patio_clientes_atributos_fixos
COMMENT 'Atributos fixos (SCD0) dos clientes da ModelBricks Veículos (sistema Pátio): uma linha por cliente, com o
primeiro valor de data_nascimento e data_cadastro recebido na Bronze, isto é, o da foto mais antiga em que o cliente
apareceu. Uma correção posterior na fonte não altera esta tabela, ao contrário de patio_clientes_scd2. Junte com o
SCD2 por id_cliente. Exemplo de SCD0 do artigo 02 (Historização).'
AS SELECT
  CAST(id_cliente AS BIGINT)                               AS id_cliente,
  CAST(min_by(data_nascimento, _file_name) AS DATE)        AS data_nascimento,
  CAST(min_by(data_cadastro, _file_name) AS DATE)          AS data_cadastro
FROM ${catalogo_bronze}.patio.clientes
GROUP BY 1;
