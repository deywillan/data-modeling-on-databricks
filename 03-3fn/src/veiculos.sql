-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN: veículos (`veiculo` e `ativo`)
-- MAGIC
-- MAGIC O mesmo ônibus ou caminhão aparece em três sistemas, e a **placa** liga todos: o *Pátio* (`veiculos`), o *Núcleo*
-- MAGIC (`ativos`, a frota própria) e o *Apólice+* (as apólices do ramo `veiculo`). `veiculo` tem uma linha por placa e,
-- MAGIC nesta variante, **um valor por atributo**: marca, modelo e tipo vêm do *Pátio* (única fonte que descreve o veículo em
-- MAGIC colunas) e o ano vem do *Pátio* e, na falta dele, do *Núcleo*. As placas que só aparecem em apólices ficam com esses
-- MAGIC atributos vazios. Não há `veiculo_sistema`. A regra está em `regra_system_of_record`.
-- MAGIC
-- MAGIC A placa Mercosul (`LLLNLNN`) não tem dígito verificador: a normalização só tira separadores e põe em maiúsculas
-- MAGIC (`normaliza_placa`). Todas as tabelas deste notebook são materialized views sobre a Bronze.

-- COMMAND ----------
CREATE OR REFRESH MATERIALIZED VIEW veiculo (
  veiculo_id     STRING NOT NULL COMMENT 'Chave própria da 3FN: SHA-256 da placa normalizada (64 caracteres hexadecimais).',
  placa          STRING NOT NULL COMMENT 'Placa normalizada (maiúsculas, sem hífen), a chave de integração do veículo.',
  marca          STRING COMMENT 'Fabricante (Pátio).',
  modelo         STRING COMMENT 'Modelo (Pátio).',
  tipo           STRING COMMENT 'onibus ou caminhao (Pátio).',
  ano_fabricacao INT    COMMENT 'Ano de fabricação: Pátio e, na falta dele, Núcleo.',
  qtd_sistemas   INT    COMMENT 'Em quantos sistemas de cadastro (Pátio e Núcleo) o veículo aparece.',
  CONSTRAINT pk_veiculo PRIMARY KEY (veiculo_id) RELY
)
COMMENT 'A entidade de negócio veículo (ônibus ou caminhão), integrada pelo método system of record: uma linha por placa que aparece no Pátio, na frota do Núcleo ou nas apólices do Apólice+, com marca, modelo e tipo do Pátio e o ano do Pátio ou, na falta dele, do Núcleo. As placas só de apólice ficam com os atributos vazios. A unicidade da chave vem do SELECT DISTINCT/GROUP BY, e por isso a PK leva RELY.'
AS
WITH patio AS (
  SELECT ${catalogo_silver}.3fn.normaliza_placa(placa) AS placa, marca, modelo, tipo, CAST(ano_fabricacao AS INT) AS ano_fabricacao
  FROM ${catalogo_bronze}.patio.veiculos
  QUALIFY to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd')
        = max(to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd')) OVER ()
),
nucleo AS (
  SELECT ${catalogo_silver}.3fn.normaliza_placa(placa) AS placa, CAST(ano_fabricacao AS INT) AS ano_fabricacao
  FROM ${catalogo_bronze}.nucleo.ativos
  QUALIFY row_number() OVER (PARTITION BY id_ativo ORDER BY updated_at DESC) = 1
),
placas AS (
  SELECT DISTINCT placa FROM (
    SELECT ${catalogo_silver}.3fn.normaliza_placa(placa) AS placa FROM ${catalogo_bronze}.patio.veiculos
    UNION ALL SELECT ${catalogo_silver}.3fn.normaliza_placa(placa) FROM ${catalogo_bronze}.nucleo.ativos
    UNION ALL SELECT ${catalogo_silver}.3fn.normaliza_placa(placa) FROM ${catalogo_bronze}.apolice.apolices
  ) WHERE placa IS NOT NULL
)
SELECT
  sha2(p.placa, 256)                                    AS veiculo_id,
  p.placa,
  any_value(pt.marca)                                   AS marca,
  any_value(pt.modelo)                                  AS modelo,
  any_value(pt.tipo)                                    AS tipo,
  coalesce(any_value(pt.ano_fabricacao), any_value(nu.ano_fabricacao)) AS ano_fabricacao,
  CAST(count(DISTINCT pt.placa) + count(DISTINCT nu.placa) AS INT)     AS qtd_sistemas
FROM placas p
LEFT JOIN patio  pt ON pt.placa = p.placa
LEFT JOIN nucleo nu ON nu.placa = p.placa
GROUP BY p.placa;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ativo (
  ativo_id        BIGINT NOT NULL COMMENT 'Chave do ativo no Núcleo (id_ativo, 7 dígitos).',
  veiculo_id      STRING          COMMENT 'O veículo da frota (veiculo).',
  empresa_grupo   STRING          COMMENT 'Empresa do grupo dona do ativo: mb_viacao (ônibus) ou mb_cargas (caminhões).',
  descricao       STRING          COMMENT 'Tipo, marca e modelo, em texto livre.',
  data_aquisicao  DATE            COMMENT 'Dia da compra na ModelBricks Veículos.',
  valor_aquisicao DECIMAL(14,2)   COMMENT 'Valor da nota de compra, em R$.',
  valor_contabil  DECIMAL(14,2)   COMMENT 'Valor contábil depreciado na última versão, em R$.',
  CONSTRAINT veiculo_existe EXPECT (veiculo_id IS NOT NULL),
  CONSTRAINT pk_ativo PRIMARY KEY (ativo_id),
  CONSTRAINT fk_ativo_e_o_veiculo FOREIGN KEY (veiculo_id) REFERENCES ${catalogo_silver}.3fn.veiculo (veiculo_id)
)
COMMENT 'Veículos da frota própria do grupo, registrados como ativo imobilizado no Núcleo, no estado atual: uma linha por ativo, com o valor contábil da última versão (o Núcleo grava uma versão nova no último dia de cada mês, com a depreciação). O histórico mensal desse valor é o periodic snapshot do artigo 02. Ativos com exclusão lógica ficam de fora.'
AS
SELECT
  a.id_ativo        AS ativo_id,
  v.veiculo_id,
  a.empresa         AS empresa_grupo,
  a.descricao,
  a.data_aquisicao,
  a.valor_aquisicao,
  a.valor_contabil
FROM (
  SELECT * FROM ${catalogo_bronze}.nucleo.ativos
  QUALIFY row_number() OVER (PARTITION BY id_ativo ORDER BY updated_at DESC) = 1
) a
LEFT JOIN veiculo v ON v.veiculo_id = sha2(${catalogo_silver}.3fn.normaliza_placa(a.placa), 256)
WHERE coalesce(a.excluido, false) = false;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW veiculo_sistema (
  sistema      STRING NOT NULL COMMENT 'Sistema de origem: patio ou nucleo.',
  chave_origem STRING NOT NULL COMMENT 'Chave do veículo no sistema (id_veiculo no Pátio, id_ativo no Núcleo), como texto.',
  veiculo_id   STRING NOT NULL COMMENT 'O veículo (veiculo).',
  CONSTRAINT pk_veiculo_sistema PRIMARY KEY (sistema, chave_origem),
  CONSTRAINT fk_veiculo_sistema_registra_veiculo FOREIGN KEY (veiculo_id) REFERENCES ${catalogo_silver}.3fn.veiculo (veiculo_id)
)
COMMENT 'A correspondência entre o veículo integrado e a chave que o Pátio e o Núcleo lhe deram. Guarda só as chaves; os atributos estão em veiculo, com o valor da fonte eleita. O Apólice+ só informa a placa e não aparece aqui.'
AS
SELECT u.sistema, u.chave_origem, u.veiculo_id
FROM (
  SELECT DISTINCT 'patio' AS sistema, id_veiculo AS chave_origem, sha2(${catalogo_silver}.3fn.normaliza_placa(placa), 256) AS veiculo_id
  FROM ${catalogo_bronze}.patio.veiculos
  WHERE ${catalogo_silver}.3fn.normaliza_placa(placa) IS NOT NULL
  UNION ALL
  SELECT DISTINCT 'nucleo', CAST(id_ativo AS STRING), sha2(${catalogo_silver}.3fn.normaliza_placa(placa), 256)
  FROM ${catalogo_bronze}.nucleo.ativos
  WHERE ${catalogo_silver}.3fn.normaliza_placa(placa) IS NOT NULL
) u
JOIN veiculo v ON v.veiculo_id = u.veiculo_id;
