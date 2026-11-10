-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Espelho do IBGE na Bronze
-- MAGIC
-- MAGIC A tabela `ibge.municipios` é a única da Bronze que não segue o append-only: a cada carga, ela é
-- MAGIC substituída pelo conteúdo do arquivo mais recente da landing (overwrite). É um desvio consciente,
-- MAGIC aceito porque a lista de municípios é pequena, muda pouco e ninguém precisa saber como ela era antes.
-- MAGIC As listas anteriores continuam no Volume `landing`.
-- MAGIC
-- MAGIC Boas práticas de overwrite aplicadas aqui:
-- MAGIC
-- MAGIC 1. `CREATE OR REPLACE TABLE`, e não `DROP` + `CREATE`: a troca é uma transação só, a tabela nunca
-- MAGIC    deixa de existir para quem a lê e o histórico de versões é mantido (time travel e `RESTORE`).
-- MAGIC 2. Guarda de volume antes da troca: um arquivo vazio ou truncado faz a tarefa falhar, e a versão
-- MAGIC    anterior da tabela continua valendo.
-- MAGIC 3. Conferência das linhas sem descartar nenhuma: a Bronze marca, a Silver corrige.
-- MAGIC
-- MAGIC Parâmetros (o job `01_ibge` os passa; os padrões abaixo servem para rodar o notebook à mão no target `dev`):
-- MAGIC `landing`, a pasta dos arquivos de municípios no Volume, e `tabela`, a tabela da Bronze.

-- COMMAND ----------

CREATE WIDGET TEXT landing DEFAULT '/Volumes/bronze_dev/ibge/landing/municipios/';
CREATE WIDGET TEXT tabela DEFAULT 'bronze_dev.ibge.municipios';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Guarda de volume
-- MAGIC
-- MAGIC O arquivo mais recente é o de maior nome: o nome começa pela entidade e segue com a data
-- MAGIC (`municipios_AAAAMMDD_NNNN.csv`). Se ele tiver menos de 5.000 linhas, a tarefa falha aqui,
-- MAGIC antes de tocar na tabela. Sem essa guarda, uma extração vazia viraria uma tabela vazia.

-- COMMAND ----------

WITH arquivos AS (
  SELECT _metadata.file_name AS _file_name
  FROM read_files(
    :landing, 
    format => 'csv', 
    header => true, 
    inferColumnTypes => false
  )
),
ultimo AS (
  SELECT * 
  FROM arquivos 
  WHERE _file_name = (SELECT max(_file_name) FROM arquivos)
)
SELECT
  max(_file_name) AS arquivo,
  count(*)        AS linhas,
  CASE WHEN count(*) < 5000
       THEN raise_error(concat('Lista do IBGE com ', count(*), ' linhas (mínimo 5000): a tabela não foi substituída.'))
  END             AS guarda
FROM ultimo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Substituição da tabela
-- MAGIC
-- MAGIC Todas as colunas ficam como `STRING`, como chegaram (`inferColumnTypes => false`); a tipagem é
-- MAGIC trabalho da Silver. Os metadados são os mesmos de toda tabela Bronze. Aqui, `_ingestion_at` é o
-- MAGIC instante desta carga, o mesmo para todas as linhas, e `_load_type = 'overwrite'` avisa quem consulta
-- MAGIC que a tabela não guarda as listas anteriores.
-- MAGIC
-- MAGIC Das proteções das streaming tables da Bronze, o espelho fica só com a retenção de 30 dias: é ela que mantém as
-- MAGIC versões anteriores ao alcance do `RESTORE` se uma lista ruim passar pela guarda.
-- MAGIC
-- MAGIC - `delta.appendOnly` não se aplica, porque a tabela é substituída a cada carga.
-- MAGIC - `pipelines.reset.allowed` só vale para tabelas de pipeline.
-- MAGIC - `delta.enableTypeWidening` não tem efeito, porque todas as colunas são `STRING`.
-- MAGIC - `CLUSTER BY AUTO` não compensa numa tabela de pouco mais de 5.500 linhas.

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:tabela)
COMMENT 'Espelho da lista oficial de municípios brasileiros do IBGE, com a hierarquia geográfica: uma linha por
município, só da lista mais recente da landing (a tabela é substituída a cada carga; as listas anteriores
ficam na landing). Colunas em STRING: cod_municipio (7 dígitos, o código usado por todos os sistemas do grupo
em endereços, rotas e lojas), nome_municipio, cod_uf, sigla_uf, nome_uf, cod_regiao, sigla_regiao,
nome_regiao, cod_regiao_imediata, nome_regiao_imediata, cod_regiao_intermediaria e nome_regiao_intermediaria.
Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing),
_system_origin (sistema) e _load_type (overwrite).'
TBLPROPERTIES (
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
)
AS WITH arquivos AS (
  SELECT *, _metadata.file_name AS _file_name
  FROM read_files(
    :landing, 
    format => 'csv', 
    header => true, 
    inferColumnTypes => false
  )
)
SELECT
  *,
  current_timestamp() AS _ingestion_at,
  'ibge'              AS _system_origin,
  'overwrite'         AS _load_type
FROM arquivos
WHERE _file_name = (SELECT max(_file_name) FROM arquivos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Conferência
-- MAGIC
-- MAGIC Só informa, não descarta nem falha: o código de município tem de ter 7 dígitos, e o nome não pode
-- MAGIC faltar. Linhas fora disso continuam na tabela, para a Silver tratar.

-- COMMAND ----------

SELECT
  any_value(_file_name)                                              AS arquivo,
  count(*)                                                           AS linhas,
  count_if(cod_municipio IS NULL OR NOT cod_municipio RLIKE '^[0-9]{7}$') AS codigo_fora_do_padrao,
  count_if(nome_municipio IS NULL OR trim(nome_municipio) = '')      AS sem_nome
FROM IDENTIFIER(:tabela);
