-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: *PassagemHub* (marketplace parceiro da Viação)
-- MAGIC
-- MAGIC Páginas de uma API em JSON aninhado, uma página por arquivo. Cada página traz a lista de eventos de venda de um dia,
-- MAGIC e cada evento tem dentro de si o pedido, o comprador com o contato, a viagem com origem e destino e os valores. A
-- MAGIC partir de 05/01/2026, os eventos passam a trazer também o objeto `pagamento`.
-- MAGIC
-- MAGIC A página inteira fica na coluna `payload`, do tipo VARIANT (`singleVariantColumn`): uma linha da tabela por página.
-- MAGIC O campo novo aparece dentro do documento sem nenhuma alteração neste notebook.
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

-- MAGIC %md
-- MAGIC ## O defeito fica guardado e contado
-- MAGIC
-- MAGIC Em parte dos pedidos, o comprador vem sem e-mail ou sem telefone. A expectation abaixo marca as páginas com algum
-- MAGIC comprador nessa situação e **não tem cláusula `ON VIOLATION`**, e essa ausência é a decisão: a página é gravada
-- MAGIC normalmente, e o pipeline só conta a falha.
-- MAGIC
-- MAGIC - Com `ON VIOLATION DROP ROW`, a página sumiria da Bronze, e com ela todos os pedidos corretos que vieram junto.
-- MAGIC - Com `ON VIOLATION FAIL UPDATE`, um único cadastro incompleto pararia a ingestão do marketplace inteiro.
-- MAGIC
-- MAGIC O `::STRING` converte o `null` do JSON em `NULL` do SQL e também cobre o campo ausente. A correção é trabalho da Silver.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE passagemhub.eventos_venda (
  CONSTRAINT contato_do_comprador_preenchido EXPECT (
    NOT exists(
      cast(payload:eventos AS ARRAY<VARIANT>),
      e -> e:comprador.contato.email::STRING IS NULL
        OR e:comprador.contato.telefone::STRING IS NULL
    )
  )
)
CLUSTER BY AUTO
COMMENT 'Páginas da API do PassagemHub, marketplace parceiro que vende passagens das linhas da ModelBricks Viação pelo
app e pelo site. Uma linha por página (até 100 eventos de um dia), em JSON aninhado na coluna VARIANT payload.
Raiz: pagina, por_pagina, total_paginas, total_eventos, data_referencia e eventos (lista). Cada evento:
id_evento (chave), tipo (venda, cancelamento ou remarcacao), ocorrido_em, pedido (codigo, canal, criado_em),
comprador (id_cliente, nome, cpf, contato.email, contato.telefone), viagem (cod_trecho, origem.cod_municipio,
destino.cod_municipio, data, poltrona), valores (tarifa, taxa_servico, total) e, a partir de 05/01/2026,
pagamento (forma, parcelas). Para analisar os eventos, expanda payload:eventos com variant_explode. Os
compradores são os mesmos clientes do Embarque, com outra chave e grafia; cpf, nome e contato podem vir fora
do padrão ou nulos. Bronze append-only: cada arquivo entra como chegou, sem correção (a correção é feita na
Silver). Metadados de ingestão: _ingestion_at (instante da carga), _file_name (arquivo de origem na landing),
_system_origin (sistema) e _load_type (append).'
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
  'passagemhub'         AS _system_origin,
  'append'              AS _load_type
FROM STREAM read_files(
  '/Volumes/${catalogo_bronze}/passagemhub/landing/eventos_venda/',
  format => 'json',
  singleVariantColumn => 'payload'
);
