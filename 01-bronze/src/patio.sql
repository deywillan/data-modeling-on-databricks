-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *Pátio* (DMS da ModelBricks Veículos)
-- MAGIC
-- MAGIC Snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro). Cada foto entra em append, e a data no `_file_name` é a data da foto. Updates e deletes não chegam como operações: aparecem como diferenças entre uma foto e a seguinte.
-- MAGIC
-- MAGIC As colunas ficam em `STRING`, exatamente como estavam no arquivo (`inferColumnTypes => false`). O que não couber no schema conhecido vai para `_rescued_data`, que o `read_files` acrescenta por padrão.
-- MAGIC
-- MAGIC Tabelas: `lojas`, `clientes`, `veiculos`, `vendas`, `ordens_servico`, no schema `patio` do catálogo da Bronze, ao lado do Volume `landing`.
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

CREATE OR REFRESH STREAMING TABLE patio.lojas
CLUSTER BY AUTO
COMMENT 'Lojas da ModelBricks Veículos, concessionária de ônibus e caminhões, em fotos completas vindas do Pátio (DMS).
Cada arquivo é a tabela inteira na data da foto (mensal até 30/11/2025, diária a partir de 01/12/2025), e cada
foto entra inteira: uma linha por registro por foto. A data da foto está em _file_name
(entidade_AAAAMMDD_NNNN.csv). Para o estado atual, use só a foto mais recente; somar todas as fotos conta o
mesmo registro várias vezes. Colunas em STRING, como no CSV. Colunas: id_loja (chave), nome e cod_municipio
(IBGE). Bronze append-only: cada arquivo entra como chegou, sem correção (a correção é feita na Silver).
Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing),
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
  'patio'               AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/patio/landing/lojas/',
  format => 'csv', header => true,
  inferColumnTypes => false
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE patio.clientes
CLUSTER BY AUTO
COMMENT 'Clientes pessoa física e jurídica da ModelBricks Veículos (inclusive a Viação e a Cargas, que compram a frota
própria), em fotos completas vindas do Pátio (DMS). Cada arquivo é a tabela inteira na data da foto (mensal
até 30/11/2025, diária a partir de 01/12/2025), e cada foto entra inteira: uma linha por registro por foto. A
data da foto está em _file_name (entidade_AAAAMMDD_NNNN.csv). Para o estado atual, use só a foto mais recente;
somar todas as fotos conta o mesmo registro várias vezes. Colunas em STRING, como no CSV. Colunas: id_cliente
(chave), tipo_pessoa (F ou J), nome, documento (CPF ou CNPJ), data_nascimento, email, telefone, cod_municipio
(IBGE), logradouro, numero, cep e data_cadastro. Mudanças de telefone, e-mail ou endereço só aparecem
comparando uma foto com a anterior. Contato pode vir vazio, e cod_municipio pode vir com 6 ou 8 dígitos.
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
  'patio'               AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/patio/landing/clientes/',
  format => 'csv', header => true,
  inferColumnTypes => false
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE patio.veiculos
CLUSTER BY AUTO
COMMENT 'Ônibus e caminhões que passaram pelo pátio da ModelBricks Veículos, inclusive a frota própria do grupo, em
fotos completas vindas do Pátio (DMS). Cada arquivo é a tabela inteira na data da foto (mensal até 30/11/2025,
diária a partir de 01/12/2025), e cada foto entra inteira: uma linha por registro por foto. A data da foto
está em _file_name (entidade_AAAAMMDD_NNNN.csv). Para o estado atual, use só a foto mais recente; somar todas
as fotos conta o mesmo registro várias vezes. Colunas em STRING, como no CSV. Colunas: id_veiculo (chave),
placa (liga com rotacerta.caminhoes, rastro.eventos e nucleo.ativos), marca, modelo, tipo (onibus ou
caminhao), ano_fabricacao, status (em_estoque ou vendido) e data_entrada. Bronze append-only: cada arquivo
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
  'patio'               AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/patio/landing/veiculos/',
  format => 'csv', header => true,
  inferColumnTypes => false
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE patio.vendas
CLUSTER BY AUTO
COMMENT 'Vendas de veículos da ModelBricks Veículos, uma por veículo, em fotos completas vindas do Pátio (DMS). Cada
arquivo é a tabela inteira na data da foto (mensal até 30/11/2025, diária a partir de 01/12/2025), e cada foto
entra inteira: uma linha por registro por foto. A data da foto está em _file_name
(entidade_AAAAMMDD_NNNN.csv). Para o estado atual, use só a foto mais recente; somar todas as fotos conta o
mesmo registro várias vezes. Colunas em STRING, como no CSV. Colunas: id_venda (chave), id_veiculo
(patio.veiculos), id_cliente (patio.clientes), id_loja (patio.lojas), data_venda e valor_venda (R$). A venda
se repete em todas as fotos seguintes: para contar vendas, use só a foto mais recente. Bronze append-only:
cada arquivo entra como chegou, sem correção (a correção é feita na Silver). Metadados de ingestão:
_ingestion_at (instante da carga), _file_name (arquivo de origem na landing), _system_origin (sistema) e
_load_type (append).'
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
  'patio'               AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/patio/landing/vendas/',
  format => 'csv', header => true,
  inferColumnTypes => false
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE patio.ordens_servico
CLUSTER BY AUTO
COMMENT 'Ordens de serviço da oficina da ModelBricks Veículos, para veículos já vendidos, em fotos completas vindas do
Pátio (DMS). Cada arquivo é a tabela inteira na data da foto (mensal até 30/11/2025, diária a partir de
01/12/2025), e cada foto entra inteira: uma linha por registro por foto. A data da foto está em _file_name
(entidade_AAAAMMDD_NNNN.csv). Para o estado atual, use só a foto mais recente; somar todas as fotos conta o
mesmo registro várias vezes. Colunas em STRING, como no CSV. Colunas: id_ordem_servico (chave), id_veiculo
(patio.veiculos), id_cliente (patio.clientes), tipo_servico, data_abertura, status (aberta ou concluida),
data_conclusao e valor (R$, preenchido na conclusão). A ordem cancelada some da foto seguinte (a exclusão é a
ausência). Bronze append-only: cada arquivo entra como chegou, sem correção (a correção é feita na Silver).
Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing),
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
  'patio'               AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/patio/landing/ordens_servico/',
  format => 'csv', header => true,
  inferColumnTypes => false
);
