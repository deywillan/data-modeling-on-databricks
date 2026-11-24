-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN com *system of record*: a regra
-- MAGIC
-- MAGIC Variante da 3FN do artigo 03, completa e independente (mesmo conjunto de tabelas, em `3fn_system_of_record`), para
-- MAGIC comparação. No desenho principal, `pessoa_sistema`, `empresa_sistema` e `veiculo_sistema` guardam o que **cada
-- MAGIC sistema** registrou, e a escolha entre eles fica para o Business Vault. Aqui, a escolha é feita **na integração**, como
-- MAGIC em Inmon (*Building the Data Warehouse*, cap. 9): para cada atributo, uma fonte é a *system of record* (a "melhor" que
-- MAGIC a corporação tem, pelos critérios de completude, oportunidade, precisão, proximidade da entrada e aderência ao modelo),
-- MAGIC e a entidade guarda só o valor dela. As demais fontes só valem quando a preferida não tem valor.
-- MAGIC
-- MAGIC A regra foi definida olhando os dados da série:
-- MAGIC
-- MAGIC * `Apólice+` é a preferida para nome, nascimento, contato e município de pessoas e empresas: o cadastro serve a um
-- MAGIC   contrato, vem completo e sem município fora do padrão. Depois vêm o `Embarque` (pessoas) ou o `Núcleo` (empresas),
-- MAGIC   o `Pátio` (até 8% de e-mail e telefone vazios e 2% de municípios com 6 ou 8 dígitos) e, só para o nome, o `Núcleo`.
-- MAGIC * Endereço (rua, número, CEP): só o `Pátio` registra.
-- MAGIC * Veículo: marca, modelo e tipo do `Pátio`; ano do `Pátio` e, na falta dele, do `Núcleo`. (Essa parte da regra está
-- MAGIC   escrita direto no SQL de `veiculo`, porque não depende de ordem entre vários sistemas.)
-- MAGIC
-- MAGIC **O que se ganha e o que se perde.** Uma linha por entidade, pronta para consumo, sem escolha a fazer. Perde-se o valor
-- MAGIC das fontes não eleitas (o e-mail do `Pátio`, quando o `Apólice+` tem outro) e mudar a regra obriga a reprocessar. O
-- MAGIC histórico é mantido em `pessoa_historico` e `empresa_historico`, a partir de alvos SCD2 por sistema que são **privados**
-- MAGIC (existem só dentro do pipeline e não aparecem no schema). A versão nova nasce quando muda o valor **eleito**.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW regra_system_of_record (
  entidade   STRING NOT NULL COMMENT 'pessoa ou empresa.',
  atributo   STRING NOT NULL COMMENT 'Atributo da entidade integrada.',
  sistema    STRING NOT NULL COMMENT 'Sistema de origem.',
  prioridade INT    NOT NULL COMMENT '1 = system of record do atributo; 2, 3... = reserva, usada quando a anterior não tem valor.',
  CONSTRAINT pk_regra_system_of_record PRIMARY KEY (entidade, atributo, sistema)
)
COMMENT 'A regra de system of record, uma linha por entidade, atributo e sistema, em ordem de preferência. É a decisão de negócio que o desenho principal adia e que aqui é aplicada na integração.'
AS
SELECT * FROM VALUES
  ('pessoa', 'nome',            'apolice', 1), ('pessoa', 'nome',            'embarque', 2), ('pessoa', 'nome',            'patio', 3), ('pessoa', 'nome',            'nucleo', 4),
  ('pessoa', 'data_nascimento', 'apolice', 1), ('pessoa', 'data_nascimento', 'embarque', 2), ('pessoa', 'data_nascimento', 'patio', 3),
  ('pessoa', 'email',           'apolice', 1), ('pessoa', 'email',           'embarque', 2), ('pessoa', 'email',           'patio', 3),
  ('pessoa', 'telefone',        'apolice', 1), ('pessoa', 'telefone',        'embarque', 2), ('pessoa', 'telefone',        'patio', 3),
  ('pessoa', 'municipio_id',    'apolice', 1), ('pessoa', 'municipio_id',    'embarque', 2), ('pessoa', 'municipio_id',    'patio', 3),
  ('empresa', 'nome',           'apolice', 1), ('empresa', 'nome',           'nucleo',   2), ('empresa', 'nome',           'patio', 3),
  ('empresa', 'email',          'apolice', 1), ('empresa', 'email',          'nucleo',   2), ('empresa', 'email',          'patio', 3),
  ('empresa', 'telefone',       'apolice', 1), ('empresa', 'telefone',       'nucleo',   2), ('empresa', 'telefone',       'patio', 3),
  ('empresa', 'municipio_id',   'apolice', 1), ('empresa', 'municipio_id',   'nucleo',   2), ('empresa', 'municipio_id',   'patio', 3)
AS t(entidade, atributo, sistema, prioridade);
