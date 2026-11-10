-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *SolFin* (ERP legado do Expresso Vale do Sol)
-- MAGIC
-- MAGIC Carga histórica única na data da aquisição e, depois, fotos semanais, em CSV com `;` e `latin1`, como o *SolTicket*.
-- MAGIC
-- MAGIC As colunas ficam em `STRING`, como estavam no arquivo (`inferColumnTypes => false`), e o que não couber no schema conhecido vai para `_rescued_data`.
-- MAGIC
-- MAGIC Tabelas: `fornecedores`, `titulos_pagar`, no schema `solfin` do catálogo da Bronze, ao lado do Volume `landing`.
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

CREATE OR REFRESH STREAMING TABLE solfin.fornecedores
CLUSTER BY AUTO
COMMENT 'Fornecedores (pessoa jurídica) do Expresso Vale do Sol, em fotos vindas do SolFin, o ERP legado da empresa
adquirida, ainda não migrado para o Núcleo. A primeira foto (01/07/2025, dia da aquisição) é a carga histórica
desde 01/01/2024; depois chega uma foto completa por semana, às segundas-feiras. Cada foto entra inteira: uma
linha por registro por foto, com a data da foto em _file_name. Para o estado atual, use só a foto mais
recente. Colunas em STRING, lidas do CSV em latin1 (ISO-8859-1). Colunas: cod_fornecedor (chave),
razao_social, cnpj, telefone, cod_municipio (IBGE da sede) e data_cadastro. A mesma empresa pode ser
fornecedora no Núcleo (nucleo.fornecedores), com outra chave. Bronze append-only: cada arquivo entra como
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
  'solfin'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/solfin/landing/fornecedores/',
  format => 'csv', header => true,
  inferColumnTypes => false,
  sep => ';', encoding => 'ISO-8859-1'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE solfin.titulos_pagar
CLUSTER BY AUTO
COMMENT 'Títulos a pagar (contas a pagar) do Expresso Vale do Sol, em fotos vindas do SolFin. A primeira foto
(01/07/2025, dia da aquisição) é a carga histórica desde 01/01/2024; depois chega uma foto completa por
semana, às segundas-feiras. Cada foto entra inteira: uma linha por registro por foto, com a data da foto em
_file_name. Para o estado atual, use só a foto mais recente. Colunas em STRING, lidas do CSV em latin1
(ISO-8859-1). Colunas: num_titulo (chave), cod_fornecedor (solfin.fornecedores), categoria (Combustível,
Manutenção, Peças, Pedágio, Serviços ou Aluguel, com grafia diferente das categorias do Núcleo), valor (R$),
data_emissao, data_vencimento, situacao (aberto ou pago) e data_pagamento. Bronze append-only: cada arquivo
entra como chegou, sem correção (a correção é feita na Silver). Metadados de ingestão: _ingestion_at (instante
da carga), _file_name (arquivo de origem na landing), _system_origin (sistema) e _load_type (append).'
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
  'solfin'              AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/solfin/landing/titulos_pagar/',
  format => 'csv', header => true,
  inferColumnTypes => false,
  sep => ';', encoding => 'ISO-8859-1'
);
