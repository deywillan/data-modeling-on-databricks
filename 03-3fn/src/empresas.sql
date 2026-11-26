-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN: empresas (`empresa_historico` e `empresa`)
-- MAGIC
-- MAGIC A pessoa jurídica segue o mesmo desenho da pessoa física (`pessoas.sql`), com o **CNPJ** como chave de integração:
-- MAGIC `empresa_id = sha2(cnpj, 256)`. Aparece em três sistemas: *Pátio* (cliente `tipo_pessoa = 'J'`, inclusive as empresas
-- MAGIC do próprio grupo que compram a frota), *Apólice+* (segurado `PJ`) e *Núcleo* (fornecedor). `pessoa` e `empresa` são
-- MAGIC tabelas separadas, sem *party model*: os atributos e as regras de cada uma são diferentes.
-- MAGIC
-- MAGIC **Sobre o CNPJ.** A normalização (`normaliza_documento(cnpj, 14)`) tira a máscara e repõe zeros à esquerda, e a
-- MAGIC preparação só exige que sobrem 14 dígitos. Não há `cnpj_valido`, por dois motivos. Os dados da série (2024 e 2025)
-- MAGIC só têm CNPJ numérico e bem formado, e o catálogo de defeitos não mexe em CNPJ. E, desde julho de 2026, a Receita
-- MAGIC emite CNPJ **alfanumérico**: `normaliza_documento` e uma coluna numérica destruiriam esses documentos, então um CNPJ
-- MAGIC novo precisa de função própria (o dígito verificador continua módulo 11, calculado sobre o código ASCII menos 48).
-- MAGIC O artigo explica o ponto; o código da série fica no formato numérico.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## *Pátio*: clientes pessoa jurídica (a partir das fotos)
-- MAGIC
-- MAGIC Usa a view `patio_cliente_foto`, criada em `pessoas.sql` (as duas entidades saem das mesmas fotos).

-- COMMAND ----------

CREATE OR REFRESH PRIVATE STREAMING TABLE empresa_patio (
  empresa_id    STRING NOT NULL,
  chave_origem  STRING NOT NULL,
  cnpj          STRING,
  nome          STRING,
  email         STRING,
  telefone      STRING,
  municipio_id  STRING,
  logradouro    STRING,
  numero        INT,
  cep           STRING,
  data_cadastro DATE,
  __START_AT    DATE NOT NULL,
  __END_AT      DATE,
  CONSTRAINT pk_empresa_patio PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Clientes pessoa jurídica da ModelBricks Veículos (sistema Pátio), incluindo a ModelBricks Viação e a ModelBricks Cargas, que compram a frota própria. SCD2 deduzido das fotos completas: uma linha por empresa e versão, válida de __START_AT (data da foto em que a mudança apareceu) a __END_AT (nulo = vigente). nome é a razão social. empresa_id é o hash do CNPJ normalizado; chave_origem é o id_cliente do Pátio.';

CREATE FLOW empresa_patio_fluxo
AS AUTO CDC INTO empresa_patio
FROM SNAPSHOT (
  SELECT
    sha2(documento, 256) AS empresa_id,
    chave_origem,
    documento            AS cnpj,
    nome, email, telefone, municipio_id, logradouro, numero, cep, data_cadastro
  FROM patio_cliente_foto
  WHERE tipo_pessoa = 'J' AND documento_confere
    AND data_foto = (SELECT version FROM current_snapshot_version())
)
WITH VERSION (
  SELECT DISTINCT data_foto AS version
  FROM patio_cliente_foto
  WHERE NOT EXISTS (SELECT 1 FROM last_snapshot_version())
     OR data_foto > (SELECT version FROM last_snapshot_version())
  ORDER BY version
  LIMIT 1
)
KEYS (chave_origem)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, telefone, municipio_id, logradouro, numero, cep;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## *Apólice+*: segurados pessoa jurídica

-- COMMAND ----------

CREATE TEMPORARY VIEW apolice_segurado_pj_prep (
  CONSTRAINT cnpj_com_14_digitos EXPECT (cnpj IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  to_timestamp(updated_at)                                   AS updated_at,
  sha2(${catalogo_silver}.3fn.normaliza_documento(cnpj, 14), 256) AS empresa_id,
  codigo_segurado                                            AS chave_origem,
  ${catalogo_silver}.3fn.normaliza_documento(cnpj, 14)       AS cnpj,
  nome,
  email,
  telefone,
  cod_municipio                                              AS municipio_id,
  to_date(data_cadastro)                                     AS data_cadastro
FROM STREAM(${catalogo_bronze}.apolice.segurados)
WHERE tipo_pessoa = 'PJ';

-- COMMAND ----------

CREATE OR REFRESH PRIVATE STREAMING TABLE empresa_apolice (
  empresa_id    STRING NOT NULL,
  chave_origem  STRING NOT NULL,
  cnpj          STRING,
  nome          STRING,
  email         STRING,
  telefone      STRING,
  municipio_id  STRING,
  data_cadastro DATE,
  __START_AT    TIMESTAMP NOT NULL,
  __END_AT      TIMESTAMP,
  CONSTRAINT pk_empresa_apolice PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Segurados pessoa jurídica da ModelBricks Seguros (sistema Apólice+), em SCD2: uma linha por empresa e versão do cadastro, válida de __START_AT a __END_AT (nulo = vigente). Versões novas só para mudanças de e-mail, telefone e município. nome é a razão social. empresa_id é o hash do CNPJ normalizado; chave_origem é o codigo_segurado. Ordem de updated_at.';

CREATE FLOW empresa_apolice_fluxo
AS AUTO CDC INTO empresa_apolice
FROM STREAM(apolice_segurado_pj_prep)
KEYS (chave_origem)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (updated_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, telefone, municipio_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## *Núcleo*: fornecedores

-- COMMAND ----------

CREATE TEMPORARY VIEW nucleo_fornecedor_prep (
  CONSTRAINT cnpj_com_14_digitos EXPECT (cnpj IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  updated_at,
  excluido,
  sha2(${catalogo_silver}.3fn.normaliza_documento(cnpj, 14), 256) AS empresa_id,
  CAST(codigo_fornecedor AS STRING)                          AS chave_origem,
  ${catalogo_silver}.3fn.normaliza_documento(cnpj, 14)       AS cnpj,
  razao_social                                               AS nome,
  email,
  telefone,
  CAST(cod_municipio AS STRING)                              AS municipio_id
FROM STREAM(${catalogo_bronze}.nucleo.fornecedores);

-- COMMAND ----------

CREATE OR REFRESH PRIVATE STREAMING TABLE empresa_nucleo (
  empresa_id   STRING NOT NULL,
  chave_origem STRING NOT NULL,
  cnpj         STRING,
  nome         STRING,
  email        STRING,
  telefone     STRING,
  municipio_id STRING,
  __START_AT   TIMESTAMP NOT NULL,
  __END_AT     TIMESTAMP,
  CONSTRAINT pk_empresa_nucleo PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Fornecedores do Grupo ModelBricks (sistema Núcleo), em SCD2: uma linha por empresa e versão do cadastro, válida de __START_AT a __END_AT (nulo = vigente). Versões novas para mudanças de e-mail, telefone e município; a exclusão lógica (excluido = true, fornecedor encerrado) fecha a versão vigente, e a empresa continua existindo. nome é a razão social. empresa_id é o hash do CNPJ normalizado; chave_origem é o codigo_fornecedor. Ordem de updated_at. Atenção: o gerador entrega excluido nulo em parte das versões de atualização, e APPLY AS DELETE WHEN excluido = true trata nulo como "não exclui" (quem filtra com NOT excluido perde essas versões).';

CREATE FLOW empresa_nucleo_fluxo
AS AUTO CDC INTO empresa_nucleo
FROM STREAM(nucleo_fornecedor_prep)
KEYS (chave_origem)
APPLY AS DELETE WHEN excluido = true
SEQUENCE BY updated_at
COLUMNS * EXCEPT (updated_at, excluido)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, telefone, municipio_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A entidade `empresa` (system of record)

-- COMMAND ----------
CREATE OR REFRESH MATERIALIZED VIEW empresa_historico (
  empresa_id    STRING    NOT NULL COMMENT 'A empresa (chave própria: SHA-256 do CNPJ normalizado).',
  cnpj          STRING    NOT NULL COMMENT 'CNPJ normalizado (14 dígitos numéricos, sem máscara).',
  nome          STRING COMMENT 'Razão social da system of record (Apólice+, depois Núcleo e Pátio).',
  email         STRING COMMENT 'E-mail da system of record.',
  telefone      STRING COMMENT 'Telefone da system of record.',
  municipio_id  STRING COMMENT 'Município da system of record, só com código IBGE de 7 dígitos.',
  logradouro    STRING COMMENT 'Rua (só o Pátio registra).',
  numero        INT    COMMENT 'Número (só o Pátio registra).',
  cep           STRING COMMENT 'CEP (só o Pátio registra).',
  fonte_nome    STRING COMMENT 'Sistema de onde veio a razão social.',
  qtd_sistemas  INT    COMMENT 'Em quantos sistemas a empresa tinha uma versão vigente no início desta versão (0 = nenhum).',
  valido_desde  TIMESTAMP NOT NULL COMMENT 'Início da versão.',
  valido_ate    TIMESTAMP COMMENT 'Fim da versão, exclusivo; nulo na versão vigente.',
  CONSTRAINT pk_empresa_historico PRIMARY KEY (empresa_id, valido_desde)
)
COMMENT 'A empresa (pessoa jurídica) integrada pelo método system of record, com histórico: uma linha por CNPJ e versão. Em cada instante em que alguma fonte mudou, escolhe-se, para cada atributo, o valor da fonte de maior prioridade (regra_system_of_record) entre as versões vigentes naquele instante; versões consecutivas iguais são fundidas. qtd_sistemas = 0 marca o período em que nenhum sistema tinha versão vigente (fornecedor encerrado que só existe no Núcleo).'
AS
WITH versoes AS (
  SELECT empresa_id, cnpj, 'patio' AS sistema, chave_origem, CAST(__START_AT AS TIMESTAMP) AS inicio, CAST(__END_AT AS TIMESTAMP) AS fim,
         nome, email, telefone, municipio_id, logradouro, numero, cep
  FROM empresa_patio
  UNION ALL
  SELECT empresa_id, cnpj, 'apolice', chave_origem, __START_AT, __END_AT, nome, email, telefone, municipio_id,
         CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING)
  FROM empresa_apolice
  UNION ALL
  SELECT empresa_id, cnpj, 'nucleo', chave_origem, __START_AT, __END_AT, nome, email, telefone, municipio_id,
         CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING)
  FROM empresa_nucleo
),
pesos AS (
  SELECT sistema,
    10 - max(CASE WHEN atributo = 'nome'         THEN prioridade END) AS peso_nome,
    10 - max(CASE WHEN atributo = 'email'        THEN prioridade END) AS peso_email,
    10 - max(CASE WHEN atributo = 'telefone'     THEN prioridade END) AS peso_tel,
    10 - max(CASE WHEN atributo = 'municipio_id' THEN prioridade END) AS peso_mun
  FROM regra_system_of_record
  WHERE entidade = 'empresa'
  GROUP BY sistema
),
instantes AS (
  SELECT DISTINCT empresa_id, cnpj, t
  FROM versoes LATERAL VIEW explode(array(inicio, fim)) x AS t
  WHERE t IS NOT NULL
),
estado AS (
  SELECT
    i.empresa_id, i.cnpj, i.t AS valido_desde,
    max_by(v.nome, CASE WHEN v.nome IS NOT NULL THEN struct(p.peso_nome, v.inicio) END)                         AS nome,
    max_by(v.email, CASE WHEN v.email IS NOT NULL THEN struct(p.peso_email, v.inicio) END)                       AS email,
    max_by(v.telefone, CASE WHEN v.telefone IS NOT NULL THEN struct(p.peso_tel, v.inicio) END)                      AS telefone,
    max_by(v.municipio_id, CASE WHEN v.municipio_id RLIKE '^[0-9]{7}$' THEN struct(p.peso_mun, v.inicio) END) AS municipio_id,
    max_by(v.logradouro, CASE WHEN v.sistema = 'patio' THEN v.inicio END) AS logradouro,
    max_by(v.numero,     CASE WHEN v.sistema = 'patio' THEN v.inicio END) AS numero,
    max_by(v.cep,        CASE WHEN v.sistema = 'patio' THEN v.inicio END) AS cep,
    max_by(v.sistema, CASE WHEN v.nome IS NOT NULL THEN struct(p.peso_nome, v.inicio) END) AS fonte_nome,
    CAST(count(DISTINCT v.sistema) AS INT)              AS qtd_sistemas
  FROM instantes i
  LEFT JOIN versoes v ON v.empresa_id = i.empresa_id AND v.inicio <= i.t AND (v.fim IS NULL OR v.fim > i.t)
  LEFT JOIN pesos p ON p.sistema = v.sistema
  GROUP BY i.empresa_id, i.cnpj, i.t
),
com_marca AS (
  SELECT *,
    to_json(struct(nome, email, telefone, municipio_id, logradouro, numero, cep, fonte_nome, qtd_sistemas > 0)) AS assinatura
  FROM estado
),
mudancas AS (
  SELECT * FROM com_marca
  QUALIFY assinatura IS DISTINCT FROM lag(assinatura) OVER (PARTITION BY empresa_id ORDER BY valido_desde)
)
SELECT
  empresa_id, cnpj, nome, email, telefone, municipio_id, logradouro, numero, cep, fonte_nome, qtd_sistemas,
  valido_desde,
  lead(valido_desde) OVER (PARTITION BY empresa_id ORDER BY valido_desde) AS valido_ate
FROM mudancas;
-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `empresa`: o estado atual, para os fatos apontarem
-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW empresa (
  empresa_id    STRING NOT NULL COMMENT 'Chave própria da 3FN: SHA-256 do CNPJ normalizado (64 caracteres hexadecimais).',
  cnpj          STRING NOT NULL COMMENT 'CNPJ normalizado (14 dígitos numéricos, sem máscara).',
  nome          STRING COMMENT 'Razão social da system of record.',
  email         STRING COMMENT 'E-mail da system of record.',
  telefone      STRING COMMENT 'Telefone da system of record.',
  municipio_id  STRING COMMENT 'Município da system of record, só com código IBGE de 7 dígitos.',
  logradouro    STRING COMMENT 'Rua (só o Pátio registra).',
  numero        INT    COMMENT 'Número (só o Pátio registra).',
  cep           STRING COMMENT 'CEP (só o Pátio registra).',
  fonte_nome    STRING COMMENT 'Sistema de onde veio a razão social.',
  qtd_sistemas  INT    COMMENT 'Em quantos sistemas a empresa tem versão vigente.',
  CONSTRAINT pk_empresa PRIMARY KEY (empresa_id) RELY
)
COMMENT 'A entidade de negócio empresa, no estado atual, pelo método system of record: uma linha por CNPJ, com a última versão de empresa_historico que tem algum sistema vigente (um fornecedor encerrado que só existe no Núcleo fica com o último valor conhecido). A unicidade vem do QUALIFY, e por isso a PK leva RELY.'
AS
SELECT empresa_id, cnpj, nome, email, telefone, municipio_id, logradouro, numero, cep, fonte_nome, qtd_sistemas
FROM empresa_historico
WHERE qtd_sistemas > 0
QUALIFY row_number() OVER (PARTITION BY empresa_id ORDER BY valido_desde DESC) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `empresa_sistema`: a correspondência de chaves (só as chaves)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW empresa_sistema (
  sistema      STRING    NOT NULL COMMENT 'Sistema de origem: patio, apolice ou nucleo.',
  chave_origem STRING    NOT NULL COMMENT 'Chave da empresa no sistema (id_cliente, codigo_segurado ou codigo_fornecedor), como texto.',
  empresa_id   STRING    NOT NULL COMMENT 'A empresa (empresa).',
  papel        STRING    NOT NULL COMMENT 'Papel da empresa nesse sistema: comprador, segurado ou fornecedor.',
  valido_desde TIMESTAMP NOT NULL COMMENT 'Primeira versão da chave no sistema.',
  valido_ate   TIMESTAMP          COMMENT 'Fim da última versão; nulo enquanto a chave está vigente no sistema.',
  CONSTRAINT pk_empresa_sistema PRIMARY KEY (sistema, chave_origem),
  CONSTRAINT fk_empresa_sistema_registra_empresa FOREIGN KEY (empresa_id) REFERENCES ${catalogo_silver}.3fn.empresa (empresa_id)
)
COMMENT 'A correspondência entre a empresa integrada e a chave que cada sistema lhe deu, com o papel que ela tem no sistema: uma linha por sistema e chave. Guarda só as chaves; os atributos estão em empresa e empresa_historico, com o valor da fonte eleita.'
AS
SELECT
  u.sistema, u.chave_origem, u.empresa_id, u.papel,
  min(u.inicio) AS valido_desde,
  CASE WHEN count(u.fim) = count(*) THEN max(u.fim) END AS valido_ate
FROM (
  SELECT 'patio' AS sistema, 'comprador' AS papel, chave_origem, empresa_id, CAST(__START_AT AS TIMESTAMP) AS inicio, CAST(__END_AT AS TIMESTAMP) AS fim FROM empresa_patio
  UNION ALL SELECT 'apolice', 'segurado', chave_origem, empresa_id, __START_AT, __END_AT FROM empresa_apolice
  UNION ALL SELECT 'nucleo', 'fornecedor', chave_origem, empresa_id, __START_AT, __END_AT FROM empresa_nucleo
) u
JOIN empresa e ON e.empresa_id = u.empresa_id
GROUP BY u.sistema, u.papel, u.chave_origem, u.empresa_id;
