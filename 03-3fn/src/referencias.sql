-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN: tabelas de referência (`regiao`, `uf`, `municipio`, `linha`, `loja`)
-- MAGIC
-- MAGIC As tabelas que os fatos e as entidades apontam. Nenhuma tem histórico: são o estado atual de cada referência.
-- MAGIC
-- MAGIC **A lista do IBGE, em 3FN.** O espelho da Bronze (`ibge.municipios`) repete, em cada município, a sigla e o nome
-- MAGIC da UF e da região. Isso é uma dependência transitiva (`município → UF → nome da UF`): a 3FN a desmonta em três
-- MAGIC tabelas, `municipio → uf → regiao`. Quem quiser "município com UF e região" volta a juntar as três, e a Gold
-- MAGIC (artigo 08) faz esse caminho de volta de propósito. As regiões imediata e intermediária do IBGE ficam de fora: o
-- MAGIC grupo não as usa.
-- MAGIC
-- MAGIC **Sem correção do código de município.** O `municipio_id` chega como cada sistema o gravou. Em `linha` e `loja`, a
-- MAGIC expectation `municipio_padrao_ibge` conta o código fora do padrão do IBGE (nos dados da série, zero: é rede de
-- MAGIC proteção). O defeito real está nos clientes do *Pátio*, que gravam cerca de 2% dos códigos com 6 ou 8 dígitos: a
-- MAGIC 3FN não os corrige, a regra de *system of record* não os elege para `pessoa` e `empresa`, e a consulta de
-- MAGIC verificação os conta na Bronze.
-- MAGIC
-- MAGIC **A FK declarada não cria dependência de execução.** O pipeline monta a ordem de atualização pelas leituras entre as
-- MAGIC tabelas, e `FOREIGN KEY ... REFERENCES` não é uma leitura. Sem ela, o filho e o pai podem atualizar ao mesmo tempo, e o
-- MAGIC filho falha com `UC_REFERENTIAL_CONSTRAINT_DOES_NOT_EXIST` quando valida a FK contra um pai que está sendo
-- MAGIC atualizado. Por isso cada tabela deste modelo **lê o pai com uma junção** (`uf` junta `regiao`, `municipio` junta `uf`
-- MAGIC e assim por diante): a junção dá a ordem de execução e, de quebra, faz valer a FK que o Delta não impõe. Onde o
-- MAGIC valor deve ser mantido como veio (o município com defeito), a junção é `LEFT JOIN` e a expectation conta o que não casou.
-- MAGIC
-- MAGIC As chaves são as do próprio IBGE (`regiao_id`, `uf_id`, `municipio_id`), porque o código oficial já é único e
-- MAGIC estável. `linha` e `loja` usam a chave que o sistema de origem dá, já que cada uma vem de um sistema só.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW regiao (
  regiao_id STRING NOT NULL COMMENT 'Código IBGE da região (1 dígito).',
  sigla     STRING          COMMENT 'Sigla da região (N, NE, SE, S, CO).',
  nome      STRING          COMMENT 'Nome da região.',
  CONSTRAINT pk_regiao PRIMARY KEY (regiao_id) RELY
)
COMMENT 'Grandes regiões do Brasil (IBGE). Uma linha por região, derivada do espelho bronze ibge.municipios. 3FN: o nome da região depende só do código da região.'
AS SELECT DISTINCT
  cod_regiao   AS regiao_id,
  sigla_regiao AS sigla,
  nome_regiao  AS nome
FROM ${catalogo_bronze}.ibge.municipios;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW uf (
  uf_id     STRING NOT NULL COMMENT 'Código IBGE da UF (2 dígitos).',
  sigla     STRING          COMMENT 'Sigla da UF (SP, MG...).',
  nome      STRING          COMMENT 'Nome da UF.',
  regiao_id STRING          COMMENT 'Região da UF (regiao).',
  CONSTRAINT pk_uf PRIMARY KEY (uf_id) RELY,
  CONSTRAINT fk_uf_pertence_a_regiao FOREIGN KEY (regiao_id) REFERENCES ${catalogo_silver}.3fn.regiao (regiao_id)
)
COMMENT 'Unidades da Federação (IBGE). Uma linha por UF, derivada do espelho bronze ibge.municipios. 3FN: o nome e a região da UF dependem só do código da UF.'
AS SELECT DISTINCT
  m.cod_uf     AS uf_id,
  m.sigla_uf   AS sigla,
  m.nome_uf    AS nome,
  r.regiao_id
FROM ${catalogo_bronze}.ibge.municipios m
JOIN regiao r ON r.regiao_id = m.cod_regiao;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW municipio (
  municipio_id STRING NOT NULL COMMENT 'Código IBGE do município (7 dígitos), o mesmo que os sistemas do grupo usam em endereços, rotas e lojas.',
  nome         STRING          COMMENT 'Nome do município.',
  uf_id        STRING          COMMENT 'UF do município (uf).',
  CONSTRAINT pk_municipio PRIMARY KEY (municipio_id) RELY,
  CONSTRAINT fk_municipio_pertence_a_uf FOREIGN KEY (uf_id) REFERENCES ${catalogo_silver}.3fn.uf (uf_id)
)
COMMENT 'Municípios brasileiros (IBGE), a referência geográfica da 3FN. Uma linha por município, derivada do espelho bronze ibge.municipios (a lista mais recente). A UF e a região ficam em uf e regiao (3FN).'
AS SELECT
  m.cod_municipio  AS municipio_id,
  m.nome_municipio AS nome,
  u.uf_id
FROM ${catalogo_bronze}.ibge.municipios m
JOIN uf u ON u.uf_id = m.cod_uf;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW linha (
  linha_id               BIGINT NOT NULL COMMENT 'Código da linha de ônibus (cod_linha do Embarque, 6 dígitos).',
  municipio_origem_id    STRING          COMMENT 'Município de origem (municipio).',
  municipio_destino_id   STRING          COMMENT 'Município de destino (municipio).',
  CONSTRAINT municipio_padrao_ibge EXPECT (municipio_origem_id RLIKE '^[0-9]{7}$' AND municipio_destino_id RLIKE '^[0-9]{7}$'),
  CONSTRAINT pk_linha PRIMARY KEY (linha_id),
  CONSTRAINT fk_linha_parte_de_municipio FOREIGN KEY (municipio_origem_id) REFERENCES ${catalogo_silver}.3fn.municipio (municipio_id),
  CONSTRAINT fk_linha_chega_em_municipio FOREIGN KEY (municipio_destino_id) REFERENCES ${catalogo_silver}.3fn.municipio (municipio_id)
)
COMMENT 'Linhas de ônibus da ModelBricks Viação (sistema Embarque): a estrutura da linha, com origem e destino. A tarifa e o histórico de reajustes ficam fora: são do artigo 02 (Historização). Derivada da última operação de CDC de cada linha em embarque.tarifas (ordem de ts_operacao).'
AS
WITH operacoes AS (
  SELECT
    payload:op::STRING                  AS op,
    payload:seq::BIGINT                 AS seq,
    payload:ts_operacao::TIMESTAMP      AS ts_operacao,
    payload:cod_linha::BIGINT           AS linha_id,
    CAST(payload:cod_municipio_origem::INT  AS STRING) AS municipio_origem_id,
    CAST(payload:cod_municipio_destino::INT AS STRING) AS municipio_destino_id
  FROM ${catalogo_bronze}.embarque.tarifas
)
SELECT o.linha_id, mo.municipio_id AS municipio_origem_id, md.municipio_id AS municipio_destino_id
FROM (
  SELECT * FROM operacoes
  WHERE op <> 'D'
  QUALIFY row_number() OVER (PARTITION BY linha_id ORDER BY ts_operacao DESC, seq DESC) = 1
) o
LEFT JOIN municipio mo ON mo.municipio_id = o.municipio_origem_id
LEFT JOIN municipio md ON md.municipio_id = o.municipio_destino_id;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW loja (
  loja_id      BIGINT NOT NULL COMMENT 'Chave da loja no Pátio (id_loja, 5 dígitos).',
  nome         STRING          COMMENT 'Nome da loja.',
  municipio_id STRING          COMMENT 'Município da loja (municipio).',
  CONSTRAINT municipio_padrao_ibge EXPECT (municipio_id RLIKE '^[0-9]{7}$'),
  CONSTRAINT pk_loja PRIMARY KEY (loja_id),
  CONSTRAINT fk_loja_fica_em_municipio FOREIGN KEY (municipio_id) REFERENCES ${catalogo_silver}.3fn.municipio (municipio_id)
)
COMMENT 'Lojas da ModelBricks Veículos (sistema Pátio). Derivada da foto mais recente de patio.lojas (a data da foto está no nome do arquivo): o Pátio entrega a tabela inteira a cada foto, e o estado atual é a última.'
AS
WITH fotos AS (
  SELECT *, to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto
  FROM ${catalogo_bronze}.patio.lojas
)
SELECT
  CAST(f.id_loja AS BIGINT) AS loja_id,
  f.nome,
  m.municipio_id
FROM fotos f
LEFT JOIN municipio m ON m.municipio_id = f.cod_municipio
WHERE f.data_foto = (SELECT max(data_foto) FROM fotos);
