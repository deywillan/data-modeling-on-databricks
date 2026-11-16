-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Historização: *accumulating* e *periodic snapshot* (Gold)
-- MAGIC
-- MAGIC "Snapshot" aqui está no sentido de Kimball, como tipo de tabela fato, e não como cópia periódica da fonte (que
-- MAGIC é o que o *Pátio* entrega). Os dois são fatos prontos para consumo, por isso ficam em
-- MAGIC `${catalogo_gold}.historizacao`, e não na Silver.
-- MAGIC
-- MAGIC **Accumulating snapshot (`fato_ciclo_frete`).** Uma linha por frete do *RotaCerta*, com o instante de cada
-- MAGIC etapa. Cada mudança de status chega como um `U` na Bronze de CDC; a materialized view vira essas operações de
-- MAGIC lado. Um frete em andamento tem as etapas seguintes nulas, preenchidas nas próximas atualizações: é o
-- MAGIC "acumular" do nome. Cerca de 2% das mudanças chegam atrasadas, com `seq` fora de ordem, por isso a ordem vem de
-- MAGIC `ts_operacao`. As etapas opcionais (coleta agendada, transbordo, entrega frustrada) ficam de fora.
-- MAGIC
-- MAGIC **Periodic snapshot (`fato_valor_contabil_mensal`).** Uma linha por ativo da frota e por mês, com o valor
-- MAGIC contábil no fim do mês. O *Núcleo* já grava uma versão de cada ativo no último dia de cada mês, com o valor
-- MAGIC depreciado; a materialized view fica com a última versão de cada mês.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ${catalogo_gold}.historizacao.fato_ciclo_frete
COMMENT 'Accumulating snapshot do ciclo do frete da ModelBricks Cargas (sistema RotaCerta): uma linha por frete, com
o instante em que ele entrou em cada etapa (aberto_em, coletado_em, em_transito_em, em_rota_em, entregue_em). Etapa
ainda não alcançada fica nula. As durações entre etapas saem da diferença entre as colunas. Atributos do frete
(cliente, destino, previsão) vêm da operação mais recente, pela ordem de ts_operacao. Exemplo de accumulating
snapshot do artigo 02 (Historização).'
AS WITH operacoes AS (
  SELECT
    payload:id_frete::BIGINT                AS id_frete,
    payload:ts_operacao::TIMESTAMP          AS ts_operacao,
    payload:id_cliente::BIGINT              AS id_cliente,
    payload:cod_municipio_origem::INT       AS cod_municipio_origem,
    payload:cod_municipio_destino::INT      AS cod_municipio_destino,
    payload:data_prevista_entrega::DATE     AS data_prevista_entrega,
    payload:status::STRING                  AS status
  FROM ${catalogo_bronze}.rotacerta.fretes
  WHERE payload:op::STRING IN ('I', 'U')
)
SELECT
  id_frete,
  max_by(id_cliente, ts_operacao)                                   AS id_cliente,
  max_by(cod_municipio_origem, ts_operacao)                         AS cod_municipio_origem,
  max_by(cod_municipio_destino, ts_operacao)                        AS cod_municipio_destino,
  max_by(data_prevista_entrega, ts_operacao)                        AS data_prevista_entrega,
  max_by(status, ts_operacao)                                       AS status_atual,
  min(CASE WHEN status = 'aberto'             THEN ts_operacao END) AS aberto_em,
  min(CASE WHEN status = 'coletado'           THEN ts_operacao END) AS coletado_em,
  min(CASE WHEN status = 'em_transito'        THEN ts_operacao END) AS em_transito_em,
  min(CASE WHEN status = 'em_rota_de_entrega' THEN ts_operacao END) AS em_rota_em,
  min(CASE WHEN status = 'entregue'           THEN ts_operacao END) AS entregue_em
FROM operacoes
GROUP BY id_frete;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ${catalogo_gold}.historizacao.fato_valor_contabil_mensal
COMMENT 'Periodic snapshot do valor contábil da frota própria (sistema Núcleo, ativos): uma linha por ativo e por
mês, com o valor contábil depreciado da última versão do mês. Medida semiaditiva: some entre ativos num mesmo mês
(patrimônio da frota), nunca entre meses. Ativos excluídos logicamente deixam de aparecer a partir do mês da
exclusão. Exemplo de periodic snapshot do artigo 02 (Historização).'
AS WITH versoes AS (
  SELECT
    id_ativo,
    empresa,
    placa,
    updated_at,
    excluido,
    valor_aquisicao,
    valor_contabil,
    last_day(to_date(updated_at)) AS mes_referencia
  FROM ${catalogo_bronze}.nucleo.ativos
),
fim_de_mes AS (
  SELECT
    id_ativo,
    mes_referencia,
    max_by(empresa, updated_at)          AS empresa,
    max_by(placa, updated_at)            AS placa,
    max_by(valor_aquisicao, updated_at)  AS valor_aquisicao,
    max_by(valor_contabil, updated_at)   AS valor_contabil,
    max_by(excluido, updated_at)         AS excluido
  FROM versoes
  GROUP BY id_ativo, mes_referencia
)
SELECT
  id_ativo,
  mes_referencia,
  empresa,
  placa,
  valor_aquisicao,
  valor_contabil
FROM fim_de_mes
WHERE NOT excluido;
