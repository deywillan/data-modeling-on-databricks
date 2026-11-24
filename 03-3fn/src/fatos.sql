-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN: os fatos do grupo (`passagem`, `venda_veiculo`, `apolice`, `titulo_pagar`, `nota_fiscal`)
-- MAGIC
-- MAGIC Os registros dos processos de negócio, ligados às entidades integradas (`pessoa`, `empresa`, `veiculo`, `loja`,
-- MAGIC `linha`). Aqui a 3FN faz a integração de verdade: a passagem do *Embarque* aponta para `pessoa`, e não para o
-- MAGIC `id_cliente` do sistema; a venda do *Pátio*, a apólice e a nota fiscal do *Núcleo* apontam para a mesma `pessoa`,
-- MAGIC `empresa` e `veiculo`.
-- MAGIC
-- MAGIC **Materialized views sobre a Bronze.** Cada fato é o estado atual do registro em seu sistema: a última operação do
-- MAGIC CDC (*Embarque*), a última versão por `updated_at` (*Núcleo* e *Apólice+*) ou a foto mais recente (*Pátio*). O
-- MAGIC histórico do ciclo de vida (status da passagem, do título) é tema do artigo 02, e o estado atual propagado com
-- MAGIC deletes é o ODS (artigo 04): a diferença é que a 3FN **não apaga** o registro estornado ou cancelado, só o marca.
-- MAGIC
-- MAGIC **As chaves integradas vêm por junção, e não por recálculo.** O fato procura o `pessoa_id` na tabela do sistema
-- MAGIC (`pessoa_embarque`, `pessoa_patio`...) e o confirma em `pessoa`. O cliente cujo CPF foi recusado não tem
-- MAGIC `pessoa_id`, e a expectation conta o fato órfão. A junção com a entidade também dá a ordem de execução: a FK
-- MAGIC declarada não cria dependência no pipeline (veja `referencias.sql`).
-- MAGIC
-- MAGIC **Expectations como imposição.** PK e FK são só informativas no Delta. Quem as faz valer é o pipeline: aqui, as
-- MAGIC expectations contam o fato sem comprador, sem veículo ou sem fornecedor. Nos dados da série elas contam zero (a
-- MAGIC FK órfã injetada existe só no *SolTicket*, que a 3FN não lê): são rede de proteção. A órfã real é medida na Bronze
-- MAGIC dos legados, na seção "Comparando" do artigo.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW passagem (
  passagem_id        BIGINT NOT NULL COMMENT 'Chave da passagem no Embarque (AAAAMMDD da venda e um sequencial de 5 dígitos).',
  pessoa_id          STRING          COMMENT 'O passageiro que comprou (pessoa).',
  linha_id           BIGINT          COMMENT 'A linha da viagem (linha).',
  data_hora_venda    TIMESTAMP       COMMENT 'Instante da venda.',
  data_viagem        DATE            COMMENT 'Data da viagem; a nova data, se remarcada.',
  poltrona           INT             COMMENT 'Poltrona, de 1 a 46.',
  canal              STRING          COMMENT 'guiche ou site.',
  valor              DECIMAL(10,2)   COMMENT 'Valor pago, em R$.',
  status             STRING          COMMENT 'Último status antes do estorno: emitida, remarcada, cancelada ou utilizada.',
  estornada          BOOLEAN NOT NULL COMMENT 'Verdadeiro se o Embarque estornou a passagem (operação D). A 3FN a mantém e a marca; o ODS a apaga.',
  CONSTRAINT passageiro_existe EXPECT (pessoa_id IS NOT NULL),
  CONSTRAINT linha_existe EXPECT (linha_id IS NOT NULL),
  CONSTRAINT pk_passagem PRIMARY KEY (passagem_id),
  CONSTRAINT fk_passagem_comprada_por_pessoa FOREIGN KEY (pessoa_id) REFERENCES ${catalogo_silver}.3fn.pessoa (pessoa_id),
  CONSTRAINT fk_passagem_percorre_linha FOREIGN KEY (linha_id) REFERENCES ${catalogo_silver}.3fn.linha (linha_id)
)
COMMENT 'Passagens de ônibus vendidas pela ModelBricks Viação no guichê e no site (sistema Embarque), no estado atual: a última operação de CDC de cada passagem (ordem de ts_operacao, e seq para desempatar). A passagem estornada (operação D) continua na tabela, com estornada = true e os valores da última versão antes do estorno. As vendas do marketplace (PassagemHub) e do SolTicket não entram: a 3FN foi desenhada para os sistemas do grupo antes da aquisição.'
AS
WITH operacoes AS (
  SELECT
    payload:op::STRING                  AS op,
    payload:seq::BIGINT                 AS seq,
    payload:ts_operacao::TIMESTAMP      AS ts_operacao,
    payload:id_passagem::BIGINT         AS passagem_id,
    CAST(payload:id_cliente::BIGINT AS STRING) AS chave_cliente,
    payload:cod_linha::BIGINT           AS linha_id,
    payload:data_hora_venda::TIMESTAMP  AS data_hora_venda,
    payload:data_viagem::DATE           AS data_viagem,
    payload:poltrona::INT               AS poltrona,
    payload:canal::STRING               AS canal,
    payload:valor::DECIMAL(10,2)        AS valor,
    payload:status::STRING              AS status
  FROM ${catalogo_bronze}.embarque.passagens
),
ultima_versao AS (
  SELECT * FROM operacoes
  WHERE op <> 'D'
  QUALIFY row_number() OVER (PARTITION BY passagem_id ORDER BY ts_operacao DESC, seq DESC) = 1
),
estornos AS (
  SELECT DISTINCT passagem_id FROM operacoes WHERE op = 'D'
),
passageiro AS (
  SELECT DISTINCT e.chave_origem, p.pessoa_id
  FROM pessoa_embarque e
  JOIN pessoa p ON p.pessoa_id = e.pessoa_id
)
SELECT
  v.passagem_id,
  p.pessoa_id,
  l.linha_id,
  v.data_hora_venda,
  v.data_viagem,
  v.poltrona,
  v.canal,
  v.valor,
  v.status,
  e.passagem_id IS NOT NULL AS estornada
FROM ultima_versao v
LEFT JOIN passageiro p ON p.chave_origem = v.chave_cliente
LEFT JOIN linha l      ON l.linha_id = v.linha_id
LEFT JOIN estornos e   ON e.passagem_id = v.passagem_id;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW venda_veiculo (
  venda_veiculo_id BIGINT NOT NULL COMMENT 'Chave da venda no Pátio (id_venda, 8 dígitos).',
  veiculo_id       STRING          COMMENT 'O veículo vendido (veiculo).',
  pessoa_id        STRING          COMMENT 'O comprador, quando pessoa física (pessoa). Nulo se o comprador for empresa.',
  empresa_id       STRING          COMMENT 'O comprador, quando pessoa jurídica (empresa), inclusive a Viação e a Cargas. Nulo se for pessoa física.',
  loja_id          BIGINT          COMMENT 'A loja da venda (loja).',
  data_venda       DATE            COMMENT 'Dia da venda.',
  valor_venda      DECIMAL(14,2)   COMMENT 'Valor da venda, em R$.',
  CONSTRAINT comprador_existe EXPECT (pessoa_id IS NOT NULL OR empresa_id IS NOT NULL),
  CONSTRAINT comprador_e_pessoa_ou_empresa EXPECT (pessoa_id IS NULL OR empresa_id IS NULL),
  CONSTRAINT veiculo_existe EXPECT (veiculo_id IS NOT NULL),
  CONSTRAINT pk_venda_veiculo PRIMARY KEY (venda_veiculo_id),
  CONSTRAINT fk_venda_veiculo_vende_veiculo FOREIGN KEY (veiculo_id) REFERENCES ${catalogo_silver}.3fn.veiculo (veiculo_id),
  CONSTRAINT fk_venda_veiculo_comprada_por_pessoa FOREIGN KEY (pessoa_id) REFERENCES ${catalogo_silver}.3fn.pessoa (pessoa_id),
  CONSTRAINT fk_venda_veiculo_comprada_por_empresa FOREIGN KEY (empresa_id) REFERENCES ${catalogo_silver}.3fn.empresa (empresa_id),
  CONSTRAINT fk_venda_veiculo_feita_na_loja FOREIGN KEY (loja_id) REFERENCES ${catalogo_silver}.3fn.loja (loja_id)
)
COMMENT 'Vendas de veículos da ModelBricks Veículos (sistema Pátio): uma linha por venda, lida da foto mais recente de patio.vendas. O comprador é uma pessoa física (pessoa_id) ou uma empresa (empresa_id), nunca os dois: a 3FN modela as duas como entidades separadas, e a venda aponta para a que for. O comprador do grupo (a Viação e a Cargas, que compram a frota própria) é uma empresa como outra qualquer.'
AS
WITH fotos AS (
  SELECT *, to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto
  FROM ${catalogo_bronze}.patio.vendas
),
atual AS (
  SELECT * FROM fotos WHERE data_foto = (SELECT max(data_foto) FROM fotos)
),
comprador_pf AS (
  SELECT DISTINCT c.chave_origem, p.pessoa_id
  FROM pessoa_patio c JOIN pessoa p ON p.pessoa_id = c.pessoa_id
),
veiculo_patio AS (
  SELECT DISTINCT id_veiculo, sha2(${catalogo_silver}.3fn.normaliza_placa(placa), 256) AS veiculo_id
  FROM ${catalogo_bronze}.patio.veiculos
  WHERE ${catalogo_silver}.3fn.normaliza_placa(placa) IS NOT NULL
),
comprador_pj AS (
  SELECT DISTINCT c.chave_origem, e.empresa_id
  FROM empresa_patio c JOIN empresa e ON e.empresa_id = c.empresa_id
)
SELECT
  CAST(v.id_venda AS BIGINT)  AS venda_veiculo_id,
  ve.veiculo_id,
  pf.pessoa_id,
  pj.empresa_id,
  l.loja_id,
  CAST(v.data_venda AS DATE)  AS data_venda,
  CAST(v.valor_venda AS DECIMAL(14,2)) AS valor_venda
FROM atual v
LEFT JOIN comprador_pf pf ON pf.chave_origem = v.id_cliente
LEFT JOIN comprador_pj pj ON pj.chave_origem = v.id_cliente
LEFT JOIN veiculo_patio vp ON vp.id_veiculo = v.id_veiculo
LEFT JOIN veiculo ve ON ve.veiculo_id = vp.veiculo_id
LEFT JOIN loja l ON l.loja_id = CAST(v.id_loja AS BIGINT);

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW apolice (
  apolice_id           BIGINT NOT NULL COMMENT 'Número da apólice no Apólice+ (AAAAMMDD da emissão e um sequencial de 5 dígitos).',
  pessoa_id            STRING          COMMENT 'O segurado, quando pessoa física (pessoa). Nulo se for empresa.',
  empresa_id           STRING          COMMENT 'O segurado, quando pessoa jurídica (empresa). Nulo se for pessoa física.',
  veiculo_id           STRING          COMMENT 'O veículo segurado, só no ramo veiculo (veiculo).',
  ramo                 STRING          COMMENT 'vida, residencial, viagem, empresarial, responsabilidade_civil, transporte ou veiculo.',
  inicio_vigencia      DATE            COMMENT 'Dia da emissão.',
  fim_vigencia         DATE            COMMENT 'Fim da vigência.',
  importancia_segurada DECIMAL(16,2)   COMMENT 'Valor segurado, em R$ (o Apólice+ o entrega como texto 1.234,56).',
  premio               DECIMAL(16,2)   COMMENT 'Prêmio anual, em R$ (o Apólice+ o entrega como texto 1.234,56).',
  status               STRING          COMMENT 'ativa, cancelada ou encerrada, na última versão.',
  data_cancelamento    DATE            COMMENT 'Dia do cancelamento, se houve.',
  CONSTRAINT segurado_existe EXPECT (pessoa_id IS NOT NULL OR empresa_id IS NOT NULL),
  CONSTRAINT valor_convertido EXPECT (importancia_segurada IS NOT NULL AND premio IS NOT NULL),
  CONSTRAINT pk_apolice PRIMARY KEY (apolice_id),
  CONSTRAINT fk_apolice_contratada_por_pessoa FOREIGN KEY (pessoa_id) REFERENCES ${catalogo_silver}.3fn.pessoa (pessoa_id),
  CONSTRAINT fk_apolice_contratada_por_empresa FOREIGN KEY (empresa_id) REFERENCES ${catalogo_silver}.3fn.empresa (empresa_id),
  CONSTRAINT fk_apolice_cobre_veiculo FOREIGN KEY (veiculo_id) REFERENCES ${catalogo_silver}.3fn.veiculo (veiculo_id)
)
COMMENT 'Apólices de seguro da ModelBricks Seguros (sistema Apólice+), no estado atual: a última versão de cada apólice (ordem de updated_at), com status e cancelamento. O Apólice+ entrega importância segurada e prêmio como texto no formato brasileiro (1.234,56) em todas as linhas, e a 3FN os converte em DECIMAL; a expectation valor_convertido conta o que não converteu. O segurado é pessoa física (pessoa_id) ou empresa (empresa_id).'
AS
WITH versoes AS (
  SELECT *
  FROM ${catalogo_bronze}.apolice.apolices
  QUALIFY row_number() OVER (PARTITION BY numero_apolice ORDER BY to_timestamp(updated_at) DESC) = 1
),
segurado_pf AS (
  SELECT DISTINCT c.chave_origem, p.pessoa_id
  FROM pessoa_apolice c JOIN pessoa p ON p.pessoa_id = c.pessoa_id
),
segurado_pj AS (
  SELECT DISTINCT c.chave_origem, e.empresa_id
  FROM empresa_apolice c JOIN empresa e ON e.empresa_id = c.empresa_id
)
SELECT
  CAST(a.numero_apolice AS BIGINT)  AS apolice_id,
  pf.pessoa_id,
  pj.empresa_id,
  ve.veiculo_id,
  a.ramo,
  to_date(a.inicio_vigencia)        AS inicio_vigencia,
  to_date(a.fim_vigencia)           AS fim_vigencia,
  ${catalogo_silver}.3fn.converte_valor_brl(a.importancia_segurada) AS importancia_segurada,
  ${catalogo_silver}.3fn.converte_valor_brl(a.premio)               AS premio,
  a.status,
  to_date(a.data_cancelamento)      AS data_cancelamento
FROM versoes a
LEFT JOIN segurado_pf pf ON pf.chave_origem = a.codigo_segurado
LEFT JOIN segurado_pj pj ON pj.chave_origem = a.codigo_segurado
LEFT JOIN veiculo ve ON ve.veiculo_id = sha2(${catalogo_silver}.3fn.normaliza_placa(a.placa), 256);

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW titulo_pagar (
  titulo_pagar_id BIGINT NOT NULL COMMENT 'Chave do título no Núcleo (AAAAMMDD da emissão e um sequencial de 4 dígitos).',
  empresa_id      STRING          COMMENT 'O fornecedor a quem se deve (empresa).',
  empresa_grupo   STRING          COMMENT 'Empresa do grupo devedora: mb, mb_viacao, mb_cargas ou mb_veiculos.',
  categoria       STRING          COMMENT 'combustivel, pecas, manutencao, servicos, aluguel ou outros.',
  valor           DECIMAL(12,2)   COMMENT 'Valor do título, em R$.',
  data_emissao    DATE            COMMENT 'Dia da emissão.',
  data_vencimento DATE            COMMENT 'Dia do vencimento.',
  status          STRING          COMMENT 'aberto ou pago, na última versão.',
  data_pagamento  DATE            COMMENT 'Dia do pagamento; nulo enquanto aberto.',
  cancelado       BOOLEAN NOT NULL COMMENT 'Verdadeiro se o Núcleo o excluiu logicamente (excluido = true). A 3FN o mantém e o marca.',
  CONSTRAINT fornecedor_existe EXPECT (empresa_id IS NOT NULL),
  CONSTRAINT pk_titulo_pagar PRIMARY KEY (titulo_pagar_id),
  CONSTRAINT fk_titulo_pagar_devido_a_empresa FOREIGN KEY (empresa_id) REFERENCES ${catalogo_silver}.3fn.empresa (empresa_id)
)
COMMENT 'Títulos a pagar (contas a pagar) das empresas do Grupo ModelBricks (sistema Núcleo), no estado atual: a última versão de cada título (ordem de updated_at). O título cancelado (excluido = true) continua na tabela, com cancelado = true. empresa_id é o fornecedor; empresa_grupo, a empresa do grupo que deve.'
AS
WITH versoes AS (
  SELECT *
  FROM ${catalogo_bronze}.nucleo.titulos_pagar
  QUALIFY row_number() OVER (PARTITION BY id_titulo ORDER BY updated_at DESC) = 1
),
fornecedor AS (
  SELECT DISTINCT c.chave_origem, e.empresa_id
  FROM empresa_nucleo c JOIN empresa e ON e.empresa_id = c.empresa_id
)
SELECT
  t.id_titulo          AS titulo_pagar_id,
  f.empresa_id,
  t.empresa            AS empresa_grupo,
  t.categoria,
  t.valor,
  t.data_emissao,
  t.data_vencimento,
  t.status,
  t.data_pagamento,
  coalesce(t.excluido, false) AS cancelado
FROM versoes t
LEFT JOIN fornecedor f ON f.chave_origem = CAST(t.codigo_fornecedor AS STRING);

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW nota_fiscal (
  nota_fiscal_id   BIGINT NOT NULL COMMENT 'Número da nota no Núcleo (8 dígitos).',
  venda_veiculo_id BIGINT          COMMENT 'A venda que a nota fatura (venda_veiculo), ligada pela placa.',
  pessoa_id        STRING          COMMENT 'O cliente da nota, quando pessoa física (pessoa).',
  empresa_id       STRING          COMMENT 'O cliente da nota, quando pessoa jurídica (empresa). Nulo nas vendas para o grupo, que a nota não identifica por documento.',
  tipo_cliente     STRING          COMMENT 'pf, pj ou grupo (venda para a Viação ou a Cargas).',
  data_emissao     DATE            COMMENT 'Dia da emissão.',
  valor_total      DECIMAL(14,2)   COMMENT 'Valor da nota, em R$.',
  CONSTRAINT venda_encontrada EXPECT (venda_veiculo_id IS NOT NULL),
  CONSTRAINT cliente_identificado EXPECT (tipo_cliente = 'grupo' OR pessoa_id IS NOT NULL OR empresa_id IS NOT NULL),
  CONSTRAINT pk_nota_fiscal PRIMARY KEY (nota_fiscal_id),
  CONSTRAINT fk_nota_fiscal_fatura_venda_veiculo FOREIGN KEY (venda_veiculo_id) REFERENCES ${catalogo_silver}.3fn.venda_veiculo (venda_veiculo_id),
  CONSTRAINT fk_nota_fiscal_emitida_para_pessoa FOREIGN KEY (pessoa_id) REFERENCES ${catalogo_silver}.3fn.pessoa (pessoa_id),
  CONSTRAINT fk_nota_fiscal_emitida_para_empresa FOREIGN KEY (empresa_id) REFERENCES ${catalogo_silver}.3fn.empresa (empresa_id)
)
COMMENT 'Notas fiscais de venda de veículos da ModelBricks Veículos (sistema Núcleo, o ERP), no estado atual. É um ponto de integração entre dois sistemas: a nota (Núcleo) e a venda (Pátio) descrevem o mesmo negócio, e a 3FN as liga pela placa do veículo. O cliente da nota é identificado pelo documento (CPF ou CNPJ normalizado), e não pelo nome. Nas vendas para o grupo, o Núcleo não grava documento, e o cliente fica nulo.'
AS
WITH versoes AS (
  SELECT *
  FROM ${catalogo_bronze}.nucleo.notas_fiscais
  QUALIFY row_number() OVER (PARTITION BY numero_nota ORDER BY updated_at DESC) = 1
     AND coalesce(excluido, false) = false
)
SELECT
  n.numero_nota                AS nota_fiscal_id,
  vv.venda_veiculo_id,
  p.pessoa_id,
  e.empresa_id,
  n.tipo_cliente,
  n.data_emissao,
  n.valor_total
FROM versoes n
LEFT JOIN venda_veiculo vv ON vv.veiculo_id = sha2(${catalogo_silver}.3fn.normaliza_placa(n.placa), 256)
LEFT JOIN pessoa p  ON n.tipo_cliente = 'pf'
                   AND p.pessoa_id = sha2(${catalogo_silver}.3fn.normaliza_documento(n.documento_cliente, 11), 256)
LEFT JOIN empresa e ON n.tipo_cliente = 'pj'
                   AND e.empresa_id = sha2(${catalogo_silver}.3fn.normaliza_documento(n.documento_cliente, 14), 256);
