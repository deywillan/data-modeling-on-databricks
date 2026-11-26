-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN: pessoas (`pessoa_historico` e `pessoa`)
-- MAGIC
-- MAGIC A mesma pessoa é cliente do *Embarque* (passageira), do *Pátio* (compradora), do *Apólice+* (segurada) e do
-- MAGIC *Núcleo* (colaboradora), cada sistema com a sua chave. Este notebook integra as quatro fontes pelo **CPF**:
-- MAGIC
-- MAGIC 1. **Preparação por fonte** (views temporárias): tipa os campos, normaliza o CPF (`normaliza_documento`) e
-- MAGIC    valida o dígito verificador (`cpf_valido`) numa coluna (`cpf_confere`), porque o pipeline não aceita função do
-- MAGIC    Unity Catalog dentro de uma expectation. A expectation com `ON VIOLATION DROP ROW` **conta e descarta** o
-- MAGIC    registro de CPF inválido: ele não vira pessoa, e a contagem fica no *event log* do pipeline. O registro continua
-- MAGIC    na Bronze, que nunca é alterada. (No *Pátio*, o fluxo de fotos não aceita expectation: o descarte é um filtro e a
-- MAGIC    contagem sai de uma consulta à Bronze.) Nos dados da série, essa contagem dá zero: o catálogo de defeitos só injeta
-- MAGIC    defeitos de formato (máscara, zeros perdidos), que a normalização resolve.
-- MAGIC 2. **Um alvo SCD2 por sistema** (privado, exceto `colaborador`) (`pessoa_embarque`, `pessoa_patio`, `pessoa_apolice`, `colaborador`), com o
-- MAGIC    histórico de contato e endereço. Um alvo por sistema, e não um só para todos, porque o *Pátio* entrega fotos
-- MAGIC    (`AUTO CDC ... FROM SNAPSHOT`), e esse tipo de fluxo só divide alvo com outros fluxos em SCD1, nunca em SCD2. Os
-- MAGIC    outros três sistemas entregam CDC ou `updated_at` e poderiam dividir um alvo: mantemos o mesmo desenho nos quatro.
-- MAGIC 3. **`pessoa_historico`** e **`pessoa`** (*system of record*): `pessoa_historico` tem uma linha por CPF e versão, com **um
-- MAGIC    valor por atributo**, vindo da fonte que a `regra_system_of_record` elege (Apólice+, depois Embarque, Pátio e Núcleo); uma
-- MAGIC    versão nova nasce quando muda o valor **eleito**, e não quando muda o de uma fonte que a regra ignora. `pessoa` é o
-- MAGIC    estado atual (uma linha por CPF), e é a ela que os fatos apontam. `pessoa_sistema` guarda só a correspondência
-- MAGIC    de chaves (que chave cada sistema deu à pessoa, e o papel dela ali). O valor das fontes não eleitas não é guardado. Os alvos SCD2 do item 2 são **privados** (`CREATE PRIVATE STREAMING TABLE`): servem de entrada ao
-- MAGIC    histórico, existem só dentro do pipeline e não aparecem no schema. O `colaborador` é público porque guarda o vínculo
-- MAGIC    (empresa do grupo, cargo, admissão), que só o Núcleo conhece e não é atributo que diverge.
-- MAGIC
-- MAGIC **Chave própria:** `pessoa_id = sha2(cpf, 256)`. É determinística: o mesmo CPF gera sempre a mesma chave, mesmo depois
-- MAGIC de um *full refresh*, e não depende da ordem de chegada. `IDENTITY` não serve, porque não é suportada em alvo de
-- MAGIC `AUTO CDC`. O hash é pseudonimização, não anonimização.
-- MAGIC
-- MAGIC **Sequência pela data da fonte:** `ts_operacao` (*Embarque*), `updated_at` (*Apólice+* e *Núcleo*) e a data da foto
-- MAGIC (*Pátio*). Nunca `_ingestion_at`, que é o relógio da carga.
-- MAGIC
-- MAGIC **Os alvos SCD2.** Só o contato e o endereço abrem versão (`TRACK HISTORY ON`); nome, nascimento e data de cadastro
-- MAGIC ficam na linha sem gerar histórico (a Parte 2 mostrou que o SCD0 puro não tem mecanismo pronto). A PK inclui
-- MAGIC `__START_AT`, e essa coluna é declarada com as demais, com o mesmo tipo da coluna de `SEQUENCE BY`: `TIMESTAMP` no
-- MAGIC *Embarque*, no *Apólice+* e no *Núcleo*; `DATE` no *Pátio*, em que a versão é a data da foto (mensal até novembro de
-- MAGIC 2025, diária depois).
-- MAGIC
-- MAGIC **Como a regra é aplicada** (`pessoa_historico`). Em cada instante em que alguma fonte abriu ou fechou uma versão,
-- MAGIC olham-se as versões vigentes naquele instante e, para cada atributo, `max_by(valor, struct(peso, inicio))` escolhe
-- MAGIC a de maior prioridade. O peso vem da `regra_system_of_record` (10 menos a prioridade); o `inicio` desempata a favor
-- MAGIC do cadastro mais recente quando a pessoa tem dois no mesmo sistema; e o `CASE WHEN ... IS NOT NULL` faz a regra cair
-- MAGIC para a fonte seguinte quando a preferida não tem valor (sem ele, o `max_by` devolveria o nulo da preferida).
-- MAGIC Versões consecutivas iguais são fundidas, comparando uma assinatura em JSON dos valores eleitos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## *Embarque*: passageiros

-- COMMAND ----------

-- A função do Unity Catalog não pode ficar dentro da expectation (o pipeline recusa função SQL do catálogo ali):
-- a validação vira uma coluna (cpf_confere), e a expectation lê a coluna.
CREATE TEMPORARY VIEW embarque_cliente_prep (
  CONSTRAINT cpf_com_digito_valido EXPECT (cpf_confere) ON VIOLATION DROP ROW
)
AS SELECT
  *,
  ${catalogo_silver}.3fn.cpf_valido(cpf) AS cpf_confere
FROM (
  SELECT
    payload:ts_operacao::TIMESTAMP                                  AS ts_operacao,
    sha2(${catalogo_silver}.3fn.normaliza_documento(payload:cpf::STRING, 11), 256) AS pessoa_id,
    CAST(payload:id_cliente::BIGINT AS STRING)                      AS chave_origem,
    ${catalogo_silver}.3fn.normaliza_documento(payload:cpf::STRING, 11) AS cpf,
    payload:nome::STRING                                            AS nome,
    payload:data_nascimento::DATE                                   AS data_nascimento,
    payload:email::STRING                                           AS email,
    payload:telefone::STRING                                        AS telefone,
    CAST(payload:cod_municipio::INT AS STRING)                      AS municipio_id,
    payload:data_cadastro::DATE                                     AS data_cadastro
  FROM STREAM(${catalogo_bronze}.embarque.clientes)
);

-- COMMAND ----------

CREATE OR REFRESH PRIVATE STREAMING TABLE pessoa_embarque (
  pessoa_id       STRING NOT NULL,
  chave_origem    STRING NOT NULL,
  cpf             STRING,
  nome            STRING,
  data_nascimento DATE,
  email           STRING,
  telefone        STRING,
  municipio_id    STRING,
  data_cadastro   DATE,
  __START_AT      TIMESTAMP NOT NULL,
  __END_AT        TIMESTAMP,
  CONSTRAINT pk_pessoa_embarque PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Passageiros da ModelBricks Viação (sistema Embarque), em SCD2: uma linha por pessoa e versão do cadastro, válida de __START_AT a __END_AT (nulo = versão vigente). pessoa_id é o hash do CPF normalizado e chave_origem é o id_cliente do Embarque. O Embarque não registra mudanças cadastrais, então hoje cada cliente tem uma versão; o desenho é o mesmo dos outros sistemas. Cliente de CPF inválido não entra (a contagem está no event log). Ordem de ts_operacao.';

CREATE FLOW pessoa_embarque_fluxo
AS AUTO CDC INTO pessoa_embarque
FROM STREAM(embarque_cliente_prep)
KEYS (chave_origem)
SEQUENCE BY ts_operacao
COLUMNS * EXCEPT (ts_operacao, cpf_confere)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, telefone, municipio_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## *Pátio*: compradores pessoa física (a partir das fotos)

-- COMMAND ----------

-- A view serve aos dois alvos do Pátio (pessoa e empresa): o tipo_pessoa separa as duas entidades. O documento é
-- normalizado com o tamanho do tipo (CPF 11, CNPJ 14). O CPF precisa passar em cpf_valido e o CNPJ só precisa ter
-- 14 dígitos (a validação do CNPJ alfanumérico está fora do escopo: veja o artigo).
--
-- Aqui não há expectation: o fluxo AUTO CDC ... FROM SNAPSHOT não aceita expectations, e o teste mostrou que a
-- expectation da view que o alimenta não registra métrica. O descarte é um filtro explícito (documento_confere) nos
-- fluxos, e a contagem do que foi recusado sai de uma consulta à Bronze (consultas/verificacao.sql).
CREATE TEMPORARY VIEW patio_cliente_foto
AS SELECT
  *,
  CASE WHEN tipo_pessoa = 'F' THEN ${catalogo_silver}.3fn.cpf_valido(documento)
       ELSE documento IS NOT NULL END AS documento_confere
FROM (
  SELECT
    to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto,
    tipo_pessoa,
    ${catalogo_silver}.3fn.normaliza_documento(documento, CASE WHEN tipo_pessoa = 'F' THEN 11 ELSE 14 END) AS documento,
    id_cliente                       AS chave_origem,
    nome,
    CAST(data_nascimento AS DATE)    AS data_nascimento,
    email,
    telefone,
    cod_municipio                    AS municipio_id,
    logradouro,
    CAST(numero AS INT)              AS numero,
    cep,
    CAST(data_cadastro AS DATE)      AS data_cadastro
  FROM ${catalogo_bronze}.patio.clientes
);

-- COMMAND ----------

CREATE OR REFRESH PRIVATE STREAMING TABLE pessoa_patio (
  pessoa_id       STRING NOT NULL,
  chave_origem    STRING NOT NULL,
  cpf             STRING,
  nome            STRING,
  data_nascimento DATE,
  email           STRING,
  telefone        STRING,
  municipio_id    STRING,
  logradouro      STRING,
  numero          INT,
  cep             STRING,
  data_cadastro   DATE,
  __START_AT      DATE NOT NULL,
  __END_AT        DATE,
  CONSTRAINT pk_pessoa_patio PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Compradores pessoa física da ModelBricks Veículos (sistema Pátio), em SCD2 deduzido das fotos completas: uma linha por pessoa e versão, válida de __START_AT (data da foto em que a mudança apareceu) a __END_AT (nulo = vigente). Versões novas só para mudanças de e-mail, telefone, município, logradouro, número e CEP. Até 30/11/2025 as fotos são mensais; a partir de 01/12/2025, diárias. pessoa_id é o hash do CPF normalizado; chave_origem é o id_cliente do Pátio.';

CREATE FLOW pessoa_patio_fluxo
AS AUTO CDC INTO pessoa_patio
FROM SNAPSHOT (
  SELECT
    sha2(documento, 256) AS pessoa_id,
    chave_origem,
    documento            AS cpf,
    nome, data_nascimento, email, telefone, municipio_id, logradouro, numero, cep, data_cadastro
  FROM patio_cliente_foto
  WHERE tipo_pessoa = 'F' AND documento_confere
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
-- MAGIC ## *Apólice+*: segurados pessoa física

-- COMMAND ----------

CREATE TEMPORARY VIEW apolice_segurado_pf_prep (
  CONSTRAINT cpf_com_digito_valido EXPECT (cpf_confere) ON VIOLATION DROP ROW
)
AS SELECT
  *,
  ${catalogo_silver}.3fn.cpf_valido(cpf) AS cpf_confere
FROM (
  SELECT
    to_timestamp(updated_at)                                  AS updated_at,
    sha2(${catalogo_silver}.3fn.normaliza_documento(cpf, 11), 256) AS pessoa_id,
    codigo_segurado                                           AS chave_origem,
    ${catalogo_silver}.3fn.normaliza_documento(cpf, 11)       AS cpf,
    nome,
    to_date(data_nascimento)                                  AS data_nascimento,
    email,
    telefone,
    cod_municipio                                             AS municipio_id,
    to_date(data_cadastro)                                    AS data_cadastro
  FROM STREAM(${catalogo_bronze}.apolice.segurados)
  WHERE tipo_pessoa = 'PF'
);

-- COMMAND ----------

CREATE OR REFRESH PRIVATE STREAMING TABLE pessoa_apolice (
  pessoa_id       STRING NOT NULL,
  chave_origem    STRING NOT NULL,
  cpf             STRING,
  nome            STRING,
  data_nascimento DATE,
  email           STRING,
  telefone        STRING,
  municipio_id    STRING,
  data_cadastro   DATE,
  __START_AT      TIMESTAMP NOT NULL,
  __END_AT        TIMESTAMP,
  CONSTRAINT pk_pessoa_apolice PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Segurados pessoa física da ModelBricks Seguros (sistema Apólice+), em SCD2: uma linha por pessoa e versão do cadastro, válida de __START_AT a __END_AT (nulo = vigente). Versões novas só para mudanças de e-mail, telefone e município (a troca de telefone abre uma versão nova). pessoa_id é o hash do CPF normalizado (o Apólice+ entrega o CPF com máscara ou sem os zeros à esquerda em cerca de 5% dos segurados, e a normalização corrige); chave_origem é o codigo_segurado. Ordem de updated_at.';

CREATE FLOW pessoa_apolice_fluxo
AS AUTO CDC INTO pessoa_apolice
FROM STREAM(apolice_segurado_pf_prep)
KEYS (chave_origem)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (updated_at, cpf_confere)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, telefone, municipio_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## *Núcleo*: colaboradores

-- COMMAND ----------

-- O Núcleo só conhece o nome e o CPF do colaborador, mais o vínculo (empresa do grupo, cargo e admissão). A
-- exclusão lógica (excluido = true) fecha a versão no SCD2, e a pessoa continua existindo.
CREATE TEMPORARY VIEW nucleo_colaborador_prep (
  CONSTRAINT cpf_com_digito_valido EXPECT (cpf_confere) ON VIOLATION DROP ROW
)
AS SELECT
  *,
  ${catalogo_silver}.3fn.cpf_valido(cpf) AS cpf_confere
FROM (
  SELECT
    updated_at,
    excluido,
    sha2(${catalogo_silver}.3fn.normaliza_documento(cpf, 11), 256) AS pessoa_id,
    CAST(matricula AS STRING)                                 AS chave_origem,
    ${catalogo_silver}.3fn.normaliza_documento(cpf, 11)       AS cpf,
    nome,
    empresa                                                   AS empresa_grupo,
    cargo,
    data_admissao
  FROM STREAM(${catalogo_bronze}.nucleo.colaboradores)
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE colaborador (
  pessoa_id     STRING NOT NULL,
  chave_origem  STRING NOT NULL,
  cpf           STRING,
  nome          STRING,
  empresa_grupo STRING,
  cargo         STRING,
  data_admissao DATE,
  __START_AT    TIMESTAMP NOT NULL,
  __END_AT      TIMESTAMP,
  CONSTRAINT pk_colaborador PRIMARY KEY (chave_origem, __START_AT)
)
COMMENT 'Colaboradores do Grupo ModelBricks (sistema Núcleo), em SCD2: uma linha por colaborador e versão do vínculo, válida de __START_AT a __END_AT (nulo = vigente). Versão nova a cada mudança de cargo ou de empresa do grupo; a exclusão lógica (excluido = true) fecha a versão vigente. empresa_grupo é a empresa do vínculo (mb, mb_viacao, mb_cargas ou mb_veiculos). pessoa_id é o hash do CPF normalizado; chave_origem é a matrícula. Ordem de updated_at.';

CREATE FLOW colaborador_fluxo
AS AUTO CDC INTO colaborador
FROM STREAM(nucleo_colaborador_prep)
KEYS (chave_origem)
APPLY AS DELETE WHEN excluido = true
SEQUENCE BY updated_at
COLUMNS * EXCEPT (updated_at, excluido, cpf_confere)
STORED AS SCD TYPE 2
TRACK HISTORY ON empresa_grupo, cargo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A entidade `pessoa` (system of record)

-- COMMAND ----------
CREATE OR REFRESH MATERIALIZED VIEW pessoa_historico (
  pessoa_id       STRING    NOT NULL COMMENT 'A pessoa (chave própria: SHA-256 do CPF normalizado).',
  cpf             STRING    NOT NULL COMMENT 'CPF normalizado (11 dígitos).',
  nome            STRING COMMENT 'Nome da system of record (Apólice+, depois Embarque, Pátio e Núcleo).',
  data_nascimento DATE   COMMENT 'Nascimento da system of record.',
  email           STRING COMMENT 'E-mail da system of record.',
  telefone        STRING COMMENT 'Telefone da system of record.',
  municipio_id    STRING COMMENT 'Município da system of record, só com código IBGE de 7 dígitos.',
  logradouro      STRING COMMENT 'Rua (só o Pátio registra).',
  numero          INT    COMMENT 'Número (só o Pátio registra).',
  cep             STRING COMMENT 'CEP (só o Pátio registra).',
  fonte_nome      STRING COMMENT 'Sistema de onde veio o nome.',
  fonte_contato   STRING COMMENT 'Sistema de onde veio o e-mail.',
  qtd_sistemas    INT    COMMENT 'Em quantos sistemas a pessoa tinha uma versão vigente no início desta versão (0 = nenhum).',
  valido_desde    TIMESTAMP NOT NULL COMMENT 'Início da versão.',
  valido_ate      TIMESTAMP COMMENT 'Fim da versão, exclusivo; nulo na versão vigente.',
  CONSTRAINT pk_pessoa_historico PRIMARY KEY (pessoa_id, valido_desde)
)
COMMENT 'A pessoa física integrada pelo método system of record, com histórico: uma linha por CPF e versão. Em cada instante em que alguma fonte mudou, escolhe-se, para cada atributo, o valor da fonte de maior prioridade (regra_system_of_record) entre as versões vigentes naquele instante; versões consecutivas iguais são fundidas. Uma mudança numa fonte que a regra não elege para o atributo não abre versão nova. qtd_sistemas = 0 marca o período em que nenhum sistema tinha versão vigente (colaborador excluído que só existe no Núcleo).'
AS
WITH versoes AS (
  SELECT pessoa_id, cpf, 'embarque' AS sistema, chave_origem, __START_AT AS inicio, __END_AT AS fim, nome, data_nascimento, email, telefone, municipio_id,
         CAST(NULL AS STRING) AS logradouro, CAST(NULL AS INT) AS numero, CAST(NULL AS STRING) AS cep
  FROM pessoa_embarque
  UNION ALL
  SELECT pessoa_id, cpf, 'patio', chave_origem, CAST(__START_AT AS TIMESTAMP), CAST(__END_AT AS TIMESTAMP), nome, data_nascimento, email, telefone, municipio_id, logradouro, numero, cep
  FROM pessoa_patio
  UNION ALL
  SELECT pessoa_id, cpf, 'apolice', chave_origem, __START_AT, __END_AT, nome, data_nascimento, email, telefone, municipio_id,
         CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING)
  FROM pessoa_apolice
  UNION ALL
  SELECT pessoa_id, cpf, 'nucleo', chave_origem, __START_AT, __END_AT, nome, CAST(NULL AS DATE), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
         CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING)
  FROM colaborador
),
pesos AS (
  SELECT sistema,
    10 - max(CASE WHEN atributo = 'nome'            THEN prioridade END) AS peso_nome,
    10 - max(CASE WHEN atributo = 'data_nascimento' THEN prioridade END) AS peso_nasc,
    10 - max(CASE WHEN atributo = 'email'           THEN prioridade END) AS peso_email,
    10 - max(CASE WHEN atributo = 'telefone'        THEN prioridade END) AS peso_tel,
    10 - max(CASE WHEN atributo = 'municipio_id'    THEN prioridade END) AS peso_mun
  FROM regra_system_of_record
  WHERE entidade = 'pessoa'
  GROUP BY sistema
),
instantes AS (
  SELECT DISTINCT pessoa_id, cpf, t
  FROM versoes LATERAL VIEW explode(array(inicio, fim)) x AS t
  WHERE t IS NOT NULL
),
estado AS (
  SELECT
    i.pessoa_id, i.cpf, i.t AS valido_desde,
    max_by(v.nome, CASE WHEN v.nome IS NOT NULL THEN struct(p.peso_nome, v.inicio) END)                         AS nome,
    max_by(v.data_nascimento, CASE WHEN v.data_nascimento IS NOT NULL THEN struct(p.peso_nasc, v.inicio) END)              AS data_nascimento,
    max_by(v.email, CASE WHEN v.email IS NOT NULL THEN struct(p.peso_email, v.inicio) END)                       AS email,
    max_by(v.telefone, CASE WHEN v.telefone IS NOT NULL THEN struct(p.peso_tel, v.inicio) END)                      AS telefone,
    max_by(v.municipio_id, CASE WHEN v.municipio_id RLIKE '^[0-9]{7}$' THEN struct(p.peso_mun, v.inicio) END) AS municipio_id,
    max_by(v.logradouro, CASE WHEN v.sistema = 'patio' THEN v.inicio END) AS logradouro,
    max_by(v.numero,     CASE WHEN v.sistema = 'patio' THEN v.inicio END) AS numero,
    max_by(v.cep,        CASE WHEN v.sistema = 'patio' THEN v.inicio END) AS cep,
    max_by(v.sistema, CASE WHEN v.nome  IS NOT NULL THEN p.peso_nome  END) AS fonte_nome,
    max_by(v.sistema, CASE WHEN v.email IS NOT NULL THEN struct(p.peso_email, v.inicio) END) AS fonte_contato,
    CAST(count(DISTINCT v.sistema) AS INT)              AS qtd_sistemas
  FROM instantes i
  LEFT JOIN versoes v ON v.pessoa_id = i.pessoa_id AND v.inicio <= i.t AND (v.fim IS NULL OR v.fim > i.t)
  LEFT JOIN pesos p ON p.sistema = v.sistema
  GROUP BY i.pessoa_id, i.cpf, i.t
),
com_marca AS (
  SELECT *,
    to_json(struct(nome, data_nascimento, email, telefone, municipio_id, logradouro, numero, cep, fonte_nome, fonte_contato, qtd_sistemas > 0)) AS assinatura
  FROM estado
),
mudancas AS (
  SELECT * FROM com_marca
  QUALIFY assinatura IS DISTINCT FROM lag(assinatura) OVER (PARTITION BY pessoa_id ORDER BY valido_desde)
)
SELECT
  pessoa_id, cpf, nome, data_nascimento, email, telefone, municipio_id, logradouro, numero, cep,
  fonte_nome, fonte_contato, qtd_sistemas,
  valido_desde,
  lead(valido_desde) OVER (PARTITION BY pessoa_id ORDER BY valido_desde) AS valido_ate
FROM mudancas;
-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `pessoa`: o estado atual, para os fatos apontarem
-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW pessoa (
  pessoa_id       STRING NOT NULL COMMENT 'Chave própria da 3FN: SHA-256 do CPF normalizado (64 caracteres hexadecimais).',
  cpf             STRING NOT NULL COMMENT 'CPF normalizado (11 dígitos) com os dois dígitos verificadores corretos.',
  nome            STRING COMMENT 'Nome da system of record.',
  data_nascimento DATE   COMMENT 'Nascimento da system of record.',
  email           STRING COMMENT 'E-mail da system of record.',
  telefone        STRING COMMENT 'Telefone da system of record.',
  municipio_id    STRING COMMENT 'Município da system of record, só com código IBGE de 7 dígitos.',
  logradouro      STRING COMMENT 'Rua (só o Pátio registra).',
  numero          INT    COMMENT 'Número (só o Pátio registra).',
  cep             STRING COMMENT 'CEP (só o Pátio registra).',
  fonte_nome      STRING COMMENT 'Sistema de onde veio o nome.',
  fonte_contato   STRING COMMENT 'Sistema de onde veio o e-mail.',
  qtd_sistemas    INT    COMMENT 'Em quantos sistemas a pessoa tem versão vigente.',
  CONSTRAINT pk_pessoa PRIMARY KEY (pessoa_id) RELY
)
COMMENT 'A entidade de negócio pessoa física, no estado atual, pelo método system of record: uma linha por CPF, com a última versão de pessoa_historico que tem algum sistema vigente (um colaborador excluído que só existe no Núcleo fica com o último valor conhecido). A unicidade vem do QUALIFY, e por isso a PK leva RELY.'
AS
SELECT pessoa_id, cpf, nome, data_nascimento, email, telefone, municipio_id, logradouro, numero, cep, fonte_nome, fonte_contato, qtd_sistemas
FROM pessoa_historico
WHERE qtd_sistemas > 0
QUALIFY row_number() OVER (PARTITION BY pessoa_id ORDER BY valido_desde DESC) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `pessoa_sistema`: a correspondência de chaves (só as chaves)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW pessoa_sistema (
  sistema      STRING    NOT NULL COMMENT 'Sistema de origem: embarque, patio, apolice ou nucleo.',
  chave_origem STRING    NOT NULL COMMENT 'Chave da pessoa no sistema (id_cliente, codigo_segurado ou matrícula), como texto.',
  pessoa_id    STRING    NOT NULL COMMENT 'A pessoa (pessoa).',
  papel        STRING    NOT NULL COMMENT 'Papel da pessoa nesse sistema: passageiro, comprador, segurado ou colaborador.',
  valido_desde TIMESTAMP NOT NULL COMMENT 'Primeira versão da chave no sistema.',
  valido_ate   TIMESTAMP          COMMENT 'Fim da última versão; nulo enquanto a chave está vigente no sistema.',
  CONSTRAINT pk_pessoa_sistema PRIMARY KEY (sistema, chave_origem),
  CONSTRAINT fk_pessoa_sistema_registra_pessoa FOREIGN KEY (pessoa_id) REFERENCES ${catalogo_silver}.3fn.pessoa (pessoa_id)
)
COMMENT 'A correspondência entre a pessoa integrada e a chave que cada sistema lhe deu, com o papel que ela tem no sistema: uma linha por sistema e chave. Guarda só as chaves (a rastreabilidade que Inmon põe nos metadados, o mapeamento da system of record para o warehouse): os atributos estão em pessoa e pessoa_historico, com o valor da fonte eleita.'
AS
SELECT
  u.sistema, u.chave_origem, u.pessoa_id, u.papel,
  min(u.inicio) AS valido_desde,
  CASE WHEN count(u.fim) = count(*) THEN max(u.fim) END AS valido_ate
FROM (
  SELECT 'embarque' AS sistema, 'passageiro' AS papel, chave_origem, pessoa_id, __START_AT AS inicio, __END_AT AS fim FROM pessoa_embarque
  UNION ALL SELECT 'patio', 'comprador', chave_origem, pessoa_id, CAST(__START_AT AS TIMESTAMP), CAST(__END_AT AS TIMESTAMP) FROM pessoa_patio
  UNION ALL SELECT 'apolice', 'segurado', chave_origem, pessoa_id, __START_AT, __END_AT FROM pessoa_apolice
  UNION ALL SELECT 'nucleo', 'colaborador', chave_origem, pessoa_id, __START_AT, __END_AT FROM colaborador
) u
JOIN pessoa p ON p.pessoa_id = u.pessoa_id
GROUP BY u.sistema, u.papel, u.chave_origem, u.pessoa_id;
