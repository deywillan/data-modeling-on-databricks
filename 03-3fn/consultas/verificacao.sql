-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consultas de verificação do artigo 03 (3FN)
-- MAGIC
-- MAGIC Rode depois do pipeline `03_3fn`. Este notebook **não** faz parte do pipeline: ele só consulta as tabelas que o
-- MAGIC pipeline criou, a Bronze e o gabarito. Os nomes usam o target `dev` (`bronze_dev`, `silver_dev`); no target `prod`,
-- MAGIC troque o sufixo por `_prd`.
-- MAGIC
-- MAGIC As contagens dependem de quanto o relógio da série avançou: elas crescem a cada `avancar_tempo`. O que não muda é a
-- MAGIC relação entre elas (por exemplo, as pessoas da 3FN contra as pessoas do gabarito).
-- MAGIC
-- MAGIC Seções:
-- MAGIC 1. O que o modelo integrou
-- MAGIC 2. Conferir com o gabarito
-- MAGIC 3. Uma pessoa, vários sistemas (e quem é a *system of record* de cada atributo)
-- MAGIC 4. O histórico da pessoa
-- MAGIC 5. PK e FK são informativas: o que o pipeline garante e o que não garante
-- MAGIC 6. Expectations no *event log*
-- MAGIC 7. Documentos recusados
-- MAGIC 8. A 3FN diante da aquisição (consultas só na Bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. O que o modelo integrou

-- COMMAND ----------

-- As tabelas do schema 3fn (as tabelas __* são internas: das materialized views e dos alvos privados por sistema)
SELECT table_name AS tabela, table_type AS tipo
FROM silver_dev.information_schema.tables
WHERE table_schema = '3fn' AND table_name NOT LIKE '\\_\\_%' AND table_name NOT LIKE 'event_log%'
ORDER BY table_name;

-- COMMAND ----------

-- Quantas pessoas, empresas e veículos: uma linha por entidade de negócio
SELECT 'pessoa' AS entidade, count(*) AS linhas FROM silver_dev.3fn.pessoa
UNION ALL SELECT 'empresa', count(*) FROM silver_dev.3fn.empresa
UNION ALL SELECT 'veiculo', count(*) FROM silver_dev.3fn.veiculo;

-- COMMAND ----------

-- A pergunta da diretoria: "quantos clientes o grupo tem?" Somando os sistemas, o número infla; pela entidade, não.
SELECT
  count(DISTINCT sistema, chave_origem) AS chaves_de_sistema,
  count(DISTINCT pessoa_id)             AS pessoas_integradas,
  count(DISTINCT sistema, chave_origem) - count(DISTINCT pessoa_id) AS contagem_a_mais_sem_integracao
FROM silver_dev.3fn.pessoa_sistema;

-- COMMAND ----------

-- Papéis de cada pessoa: quem é só passageiro, quem é passageiro e segurado...
WITH papeis AS (
  SELECT pessoa_id, array_sort(collect_set(papel)) AS papeis
  FROM silver_dev.3fn.pessoa_sistema
  GROUP BY pessoa_id
)
SELECT array_join(papeis, ' + ') AS combinacao_de_papeis, count(*) AS pessoas
FROM papeis
GROUP BY 1
ORDER BY 2 DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Conferir com o gabarito
-- MAGIC
-- MAGIC O `gabarito` tem a verdade: quais pessoas e empresas existem de fato e em que sistemas aparecem (`mapa_chaves`). O
-- MAGIC pipeline nunca o lê; aqui ele só serve de régua. A integração acertou se o número de pessoas e empresas da 3FN for
-- MAGIC o das entidades verdadeiras que aparecem nos sistemas lidos.

-- COMMAND ----------

WITH pessoas_verdadeiras AS (
  -- pessoas que aparecem como cliente no Embarque, no Pátio e no Apólice+
  SELECT DISTINCT id_verdadeiro AS id
  FROM silver_dev.gabarito.mapa_chaves
  WHERE sistema IN ('embarque', 'patio', 'apolice') AND tabela_verdadeira = 'pessoas'
  UNION
  -- e as que são colaboradoras no Núcleo (a chave verdadeira do Núcleo é a do colaborador)
  SELECT c.id_pessoa
  FROM silver_dev.gabarito.mapa_chaves m
  JOIN silver_dev.gabarito.colaboradores c ON c.id_colaborador = m.id_verdadeiro
  WHERE m.sistema = 'nucleo' AND m.tabela_verdadeira = 'colaboradores'
),
empresas_verdadeiras AS (
  SELECT DISTINCT id_verdadeiro
  FROM silver_dev.gabarito.mapa_chaves
  WHERE sistema IN ('patio', 'apolice', 'nucleo') AND tabela_verdadeira = 'empresas_pj'
)
SELECT 'pessoas' AS entidade,
       (SELECT count(*) FROM pessoas_verdadeiras)    AS no_gabarito,
       (SELECT count(*) FROM silver_dev.3fn.pessoa)  AS na_3fn
UNION ALL
-- A 3FN tem 2 empresas a mais que o gabarito: a ModelBricks Viação e a ModelBricks Cargas, cadastradas como
-- clientes no Pátio (compram a frota própria). Elas não são empresas_pj do mundo verdadeiro.
SELECT 'empresas',
       (SELECT count(*) FROM empresas_verdadeiras),
       (SELECT count(*) FROM silver_dev.3fn.empresa);

-- COMMAND ----------

-- Quais são as empresas "a mais"? As do próprio grupo, que o Pátio cadastra como cliente.
SELECT e.cnpj, e.nome, s.sistema
FROM silver_dev.3fn.empresa e
JOIN silver_dev.3fn.empresa_sistema s USING (empresa_id)
WHERE s.valido_ate IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM silver_dev.gabarito.mapa_chaves m
    JOIN silver_dev.3fn.empresa_sistema es
      ON es.sistema = m.sistema AND es.chave_origem = m.chave_sistema
    WHERE es.empresa_id = e.empresa_id AND m.tabela_verdadeira = 'empresas_pj'
  );

-- COMMAND ----------

-- Cada chave de sistema virou a pessoa certa? Compara, chave a chave, a pessoa que a 3FN deu com a pessoa do gabarito.
-- Duas chaves do gabarito com o mesmo id_verdadeiro devem ter a mesma pessoa_id na 3FN, e só elas.
-- (O Núcleo fica de fora desta comparação: o mapa de chaves dele aponta para o colaborador, e não para a pessoa.)
WITH chaves AS (
  SELECT DISTINCT s.pessoa_id, m.id_verdadeiro
  FROM silver_dev.3fn.pessoa_sistema s
  JOIN silver_dev.gabarito.mapa_chaves m
    ON m.sistema = s.sistema AND m.chave_sistema = s.chave_origem
  WHERE m.tabela_verdadeira = 'pessoas'
)
SELECT
  count(DISTINCT pessoa_id)      AS pessoas_na_3fn,
  count(DISTINCT id_verdadeiro)  AS pessoas_no_gabarito,
  count(*)                       AS pares,
  -- 0 em ambos: ninguém foi juntado errado nem ficou separado
  count(*) - count(DISTINCT pessoa_id)     AS juntadas_a_mais,
  count(*) - count(DISTINCT id_verdadeiro) AS separadas_a_mais
FROM chaves;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Uma pessoa, vários sistemas
-- MAGIC
-- MAGIC A pergunta do artigo: quem viaja pela Viação, compra na concessionária e tem apólice? Cada sistema conhece uma parte
-- MAGIC da resposta, com a sua chave. Na 3FN, os três são papéis (`passageiro`, `comprador`, `segurado`) da mesma `pessoa`.

-- COMMAND ----------

-- Quantas pessoas têm os três papéis: viaja (Embarque), compra (Pátio) e tem apólice (Apólice+)
SELECT count(*) AS viajam_compram_e_tem_apolice
FROM (
  SELECT pessoa_id
  FROM silver_dev.3fn.pessoa_sistema
  WHERE papel IN ('passageiro', 'comprador', 'segurado')
  GROUP BY pessoa_id
  HAVING count(DISTINCT papel) = 3
);

-- COMMAND ----------

-- Uma dessas pessoas, e as chaves que cada sistema deu a ela (três chaves de sistema, uma só pessoa_id)
WITH uma AS (
  SELECT pessoa_id
  FROM silver_dev.3fn.pessoa_sistema
  WHERE papel IN ('passageiro', 'comprador', 'segurado')
  GROUP BY pessoa_id
  HAVING count(DISTINCT papel) = 3
  ORDER BY pessoa_id
  LIMIT 1
)
SELECT s.papel, s.sistema, s.chave_origem, s.valido_desde, s.valido_ate
FROM silver_dev.3fn.pessoa_sistema s
JOIN uma USING (pessoa_id)
ORDER BY s.sistema;

-- COMMAND ----------

-- E os atributos dela: um valor só por atributo, com o sistema que a regra de system of record elegeu
WITH uma AS (
  SELECT pessoa_id
  FROM silver_dev.3fn.pessoa_sistema
  WHERE papel IN ('passageiro', 'comprador', 'segurado')
  GROUP BY pessoa_id
  HAVING count(DISTINCT papel) = 3
  ORDER BY pessoa_id
  LIMIT 1
)
SELECT p.nome, p.email, p.telefone, p.municipio_id, p.fonte_nome, p.fonte_contato, p.qtd_sistemas
FROM silver_dev.3fn.pessoa p
JOIN uma USING (pessoa_id);

-- COMMAND ----------

-- A regra, escrita como dado: para cada atributo, a ordem dos sistemas (1 = system of record, depois as reservas)
SELECT entidade, atributo, array_join(transform(array_sort(collect_list(struct(prioridade, sistema))), x -> x.sistema), ' > ') AS ordem
FROM silver_dev.3fn.regra_system_of_record
GROUP BY entidade, atributo
ORDER BY entidade, atributo;

-- COMMAND ----------

-- De onde vieram o nome e o e-mail das pessoas que estão em mais de um sistema
SELECT qtd_sistemas, fonte_contato, count(*) AS pessoas
FROM silver_dev.3fn.pessoa
WHERE qtd_sistemas > 1
GROUP BY ALL
ORDER BY 1, 3 DESC;

-- COMMAND ----------

-- Onde os sistemas divergem: o e-mail do Pátio e o do Apólice+, para quem está nos dois (última versão de cada um, na
-- Bronze). A 3FN guarda o do Apólice+ (maior prioridade); o do Pátio não é guardado.
WITH fotos AS (
  SELECT *, to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto
  FROM bronze_dev.patio.clientes
),
pat AS (
  SELECT sha2(silver_dev.3fn.normaliza_documento(documento, 11), 256) AS pessoa_id, email
  FROM fotos
  WHERE tipo_pessoa = 'F' AND data_foto = (SELECT max(data_foto) FROM fotos)
),
apo AS (
  SELECT sha2(silver_dev.3fn.normaliza_documento(cpf, 11), 256) AS pessoa_id, email
  FROM bronze_dev.apolice.segurados
  WHERE tipo_pessoa = 'PF'
  QUALIFY row_number() OVER (PARTITION BY codigo_segurado ORDER BY to_timestamp(updated_at) DESC) = 1
)
SELECT count(*)                                  AS pessoas_nos_dois,
       count_if(pat.email <> apo.email)          AS email_diferente,
       count_if(p.email = apo.email)             AS e_o_do_apolice_na_3fn
FROM pat JOIN apo USING (pessoa_id)
JOIN silver_dev.3fn.pessoa p USING (pessoa_id);

-- COMMAND ----------

-- O que essa pessoa fez nas três empresas: passagens, veículos comprados e apólices, pela mesma pessoa_id
WITH uma AS (
  SELECT pessoa_id
  FROM silver_dev.3fn.pessoa_sistema
  WHERE papel IN ('passageiro', 'comprador', 'segurado')
  GROUP BY pessoa_id
  HAVING count(DISTINCT papel) = 3
  ORDER BY pessoa_id
  LIMIT 1
)
SELECT 'passagem' AS fato, cast(p.passagem_id AS STRING) AS id, cast(p.data_viagem AS STRING) AS data, p.status, p.valor
FROM silver_dev.3fn.passagem p JOIN uma USING (pessoa_id)
UNION ALL
SELECT 'venda_veiculo', cast(v.venda_veiculo_id AS STRING), cast(v.data_venda AS STRING), 'vendida', v.valor_venda
FROM silver_dev.3fn.venda_veiculo v JOIN uma USING (pessoa_id)
UNION ALL
SELECT 'apolice', cast(a.apolice_id AS STRING), cast(a.inicio_vigencia AS STRING), a.status, a.premio
FROM silver_dev.3fn.apolice a JOIN uma USING (pessoa_id)
ORDER BY 1, 3;

-- COMMAND ----------

-- A conferência com o gabarito: as pessoas que o mundo verdadeiro põe como clientes das três empresas
-- (Viação, Veículos e Seguros) devem ser as mesmas que a 3FN encontrou, e só elas
WITH verdadeiras AS (
  SELECT id_entidade
  FROM silver_dev.gabarito.participacoes
  WHERE tipo_entidade = 'pessoa' AND papel = 'cliente'
  GROUP BY id_entidade
  HAVING count_if(empresa = 'mb_viacao') > 0 AND count_if(empresa = 'mb_veiculos') > 0 AND count_if(empresa = 'mb_seguros') > 0
),
na_3fn AS (
  SELECT DISTINCT m.id_verdadeiro AS id_entidade
  FROM silver_dev.3fn.pessoa_sistema s
  JOIN silver_dev.gabarito.mapa_chaves m
    ON m.sistema = s.sistema AND m.chave_sistema = s.chave_origem AND m.tabela_verdadeira = 'pessoas'
  WHERE s.papel IN ('passageiro', 'comprador', 'segurado')
  GROUP BY m.id_verdadeiro
  HAVING count(DISTINCT s.papel) = 3
)
SELECT (SELECT count(*) FROM verdadeiras)                                        AS no_gabarito,
       (SELECT count(*) FROM na_3fn)                                             AS na_3fn,
       (SELECT count(*) FROM verdadeiras v JOIN na_3fn n USING (id_entidade))    AS em_comum;

-- COMMAND ----------

-- Dois papéis, para comparação: quem viaja e também tem apólice
SELECT count(*) AS pessoas_que_viajam_e_tem_apolice
FROM (
  SELECT pessoa_id
  FROM silver_dev.3fn.pessoa_sistema
  WHERE papel IN ('passageiro', 'segurado')
  GROUP BY pessoa_id
  HAVING count(DISTINCT papel) = 2
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. O histórico da pessoa
-- MAGIC
-- MAGIC `pessoa_historico` tem uma versão nova a cada vez que muda o valor **eleito** de algum atributo. Uma mudança no
-- MAGIC *Pátio* que o *Apólice+* "esconde" (porque o Apólice+ manda no e-mail) não abre versão. O vínculo do colaborador
-- MAGIC (cargo e empresa do grupo) tem a sua tabela, `colaborador`, em SCD2.

-- COMMAND ----------

-- Versões por pessoa: quantas pessoas têm mais de uma versão
SELECT versoes, count(*) AS pessoas
FROM (SELECT pessoa_id, count(*) AS versoes FROM silver_dev.3fn.pessoa_historico GROUP BY pessoa_id)
GROUP BY versoes
ORDER BY versoes;

-- COMMAND ----------

-- Uma pessoa com troca de telefone: a versão anterior é fechada, a nova abre (e a fonte do contato pode mudar)
SELECT telefone, fonte_contato, qtd_sistemas, valido_desde, valido_ate
FROM silver_dev.3fn.pessoa_historico
WHERE pessoa_id = (
  SELECT min(pessoa_id) FROM (
    SELECT pessoa_id FROM silver_dev.3fn.pessoa_historico
    GROUP BY pessoa_id HAVING count(DISTINCT telefone) > 1
  )
)
ORDER BY valido_desde;

-- COMMAND ----------

-- Uma mudança de cargo no Núcleo: SCD2 do vínculo do colaborador
SELECT chave_origem AS matricula, empresa_grupo, cargo, __START_AT AS inicio, __END_AT AS fim
FROM silver_dev.3fn.colaborador
WHERE chave_origem = (
  SELECT min(chave_origem) FROM (
    SELECT chave_origem FROM silver_dev.3fn.colaborador GROUP BY chave_origem HAVING count(*) > 1
  )
)
ORDER BY inicio;

-- COMMAND ----------

-- MAGIC ## 5. PK e FK são informativas
-- MAGIC
-- MAGIC As constraints existem no catálogo (Catalog Explorer desenha o diagrama ER a partir delas), mas o Delta **não as
-- MAGIC impõe**. Quem garante é o pipeline, e a garantia só vale onde alguém a escreveu: uma junção, uma expectation. As
-- MAGIC consultas abaixo procuram a violação diretamente.

-- COMMAND ----------

-- As constraints declaradas
SELECT tc.table_name AS tabela, tc.constraint_name AS constraint, tc.constraint_type AS tipo
FROM silver_dev.information_schema.table_constraints tc
WHERE tc.table_schema = '3fn' AND tc.table_name NOT LIKE '\\_\\_%'
ORDER BY tc.table_name, tc.constraint_type DESC, tc.constraint_name;

-- COMMAND ----------

-- PK duplicada? (esperado: 0 em todas)
SELECT 'pessoa' AS tabela, count(*) - count(DISTINCT pessoa_id) AS duplicadas FROM silver_dev.3fn.pessoa
UNION ALL SELECT 'empresa', count(*) - count(DISTINCT empresa_id) FROM silver_dev.3fn.empresa
UNION ALL SELECT 'veiculo', count(*) - count(DISTINCT veiculo_id) FROM silver_dev.3fn.veiculo
UNION ALL SELECT 'passagem', count(*) - count(DISTINCT passagem_id) FROM silver_dev.3fn.passagem
UNION ALL SELECT 'venda_veiculo', count(*) - count(DISTINCT venda_veiculo_id) FROM silver_dev.3fn.venda_veiculo
UNION ALL SELECT 'apolice', count(*) - count(DISTINCT apolice_id) FROM silver_dev.3fn.apolice
UNION ALL SELECT 'titulo_pagar', count(*) - count(DISTINCT titulo_pagar_id) FROM silver_dev.3fn.titulo_pagar
UNION ALL SELECT 'nota_fiscal', count(*) - count(DISTINCT nota_fiscal_id) FROM silver_dev.3fn.nota_fiscal
UNION ALL SELECT 'pessoa_sistema', count(*) - count(DISTINCT sistema, chave_origem) FROM silver_dev.3fn.pessoa_sistema
UNION ALL SELECT 'empresa_sistema', count(*) - count(DISTINCT sistema, chave_origem) FROM silver_dev.3fn.empresa_sistema
UNION ALL SELECT 'pessoa_historico', count(*) - count(DISTINCT pessoa_id, valido_desde) FROM silver_dev.3fn.pessoa_historico
UNION ALL SELECT 'empresa_historico', count(*) - count(DISTINCT empresa_id, valido_desde) FROM silver_dev.3fn.empresa_historico;

-- COMMAND ----------

-- FK sem pai: a FK declarada de pessoa para municipio é informativa, mas a regra de system of record só elege código de
-- município que existe no IBGE (7 dígitos). Esperado: 0 (o código fora do padrão não chega a pessoa).
SELECT count(*) AS pessoas_com_municipio_sem_pai
FROM silver_dev.3fn.pessoa p
LEFT JOIN silver_dev.3fn.municipio m ON m.municipio_id = p.municipio_id
WHERE p.municipio_id IS NOT NULL AND m.municipio_id IS NULL;

-- COMMAND ----------

-- O que ficou de fora: o Pátio grava 2% dos municípios com 6 ou 8 dígitos (foto mais recente). A 3FN não os corrige:
-- o valor não é eleito. Quando o Pátio é a única fonte da pessoa, o município dela fica vazio.
WITH fotos AS (
  SELECT *, to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto
  FROM bronze_dev.patio.clientes
),
atual AS (SELECT * FROM fotos WHERE data_foto = (SELECT max(data_foto) FROM fotos) AND tipo_pessoa = 'F')
SELECT length(a.cod_municipio) AS digitos, count(*) AS clientes_do_patio
FROM atual a
LEFT JOIN silver_dev.3fn.municipio m ON m.municipio_id = a.cod_municipio
WHERE m.municipio_id IS NULL
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- O fato aponta para a mesma pessoa que o sistema de origem? A nota fiscal (Núcleo) e a venda (Pátio) descrevem o
-- mesmo negócio por caminhos diferentes (documento x chave do cliente). Esperado: 0 divergências.
SELECT count(*) AS notas_com_comprador_diferente_da_venda
FROM silver_dev.3fn.nota_fiscal n
JOIN silver_dev.3fn.venda_veiculo v USING (venda_veiculo_id)
WHERE n.tipo_cliente <> 'grupo'
  AND coalesce(n.pessoa_id, n.empresa_id) <> coalesce(v.pessoa_id, v.empresa_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Expectations no *event log*
-- MAGIC
-- MAGIC As expectations registram quantos registros passaram e quantos falharam, por dataset e por atualização. A consulta
-- MAGIC soma **todas** as atualizações de cada expectation (as das streaming tables só aparecem nas atualizações que
-- MAGIC receberam linhas novas). Duas ressalvas:
-- MAGIC
-- MAGIC - uma expectation de view temporária é contada uma vez por fluxo que lê a view;
-- MAGIC - nas materialized views com expectation, a contagem é a da tabela inteira a cada atualização (elas se recalculam),
-- MAGIC   então somar atualizações diferentes conta as mesmas linhas mais de uma vez. Para a contagem da última atualização,
-- MAGIC   a consulta seguinte filtra por ela.

-- COMMAND ----------

-- A última atualização: o que as expectations das materialized views contaram
WITH ultima AS (
  SELECT origin.update_id AS update_id
  FROM event_log(TABLE(silver_dev.3fn.pessoa))
  WHERE event_type = 'create_update'
  ORDER BY timestamp DESC
  LIMIT 1
)
SELECT d.dataset, d.name AS expectation, sum(d.passed_records) AS passaram, sum(d.failed_records) AS falharam
FROM (
  SELECT explode(from_json(details:flow_progress.data_quality.expectations,
           'array<struct<name:string,dataset:string,passed_records:int,failed_records:int>>')) AS d
  FROM event_log(TABLE(silver_dev.3fn.pessoa))
  WHERE event_type = 'flow_progress'
    AND origin.update_id = (SELECT update_id FROM ultima)
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
GROUP BY ALL
ORDER BY 1, 2;

-- COMMAND ----------

-- As expectations das streaming tables (documento recusado): somadas em todas as atualizações
SELECT d.dataset, d.name AS expectation, sum(d.passed_records) AS passaram, sum(d.failed_records) AS recusados
FROM (
  SELECT explode(from_json(details:flow_progress.data_quality.expectations,
           'array<struct<name:string,dataset:string,passed_records:int,failed_records:int>>')) AS d
  FROM event_log(TABLE(silver_dev.3fn.pessoa))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
WHERE d.name IN ('cpf_com_digito_valido', 'cnpj_com_14_digitos')
GROUP BY ALL
ORDER BY 1, 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Documentos recusados
-- MAGIC
-- MAGIC O CPF que não passa em `cpf_valido` não vira pessoa. Nos dados da série, nenhum é recusado: o catálogo de defeitos só
-- MAGIC injeta defeitos de **formato**, que a normalização resolve. As consultas abaixo contam, na Bronze,
-- MAGIC o que a normalização resolveu e o que a validação recusaria. Os exemplos fixos mostram o efeito.

-- COMMAND ----------

-- Exemplos fixos: o que cada função faz com uma entrada
SELECT entrada,
       silver_dev.3fn.normaliza_documento(entrada, 11)                                   AS normalizado,
       silver_dev.3fn.cpf_valido(silver_dev.3fn.normaliza_documento(entrada, 11))         AS passa_na_validacao
FROM VALUES
  ('111.444.777-35'), ('111.444.777-36'), ('529.982.247-25'), ('111'),
  ('111.111.111-11'), ('000.000.000-00'), ('5299822472'), ('abc'), (CAST(NULL AS STRING)) AS t(entrada);

-- COMMAND ----------

-- CPFs do Apólice+ (segurados PF): quantos vieram fora do padrão, quantos a normalização resolveu e quantos a
-- validação recusaria. Mede a última versão de cada segurado.
WITH atual AS (
  SELECT *
  FROM bronze_dev.apolice.segurados
  WHERE tipo_pessoa = 'PF'
  QUALIFY row_number() OVER (PARTITION BY codigo_segurado ORDER BY to_timestamp(updated_at) DESC) = 1
)
SELECT count(*)                                                              AS segurados_pf,
       count_if(cpf NOT RLIKE '^[0-9]{11}$')                                 AS cpf_fora_do_padrao,
       count_if(silver_dev.3fn.cpf_valido(silver_dev.3fn.normaliza_documento(cpf, 11)))      AS validos_apos_normalizar,
       count_if(NOT silver_dev.3fn.cpf_valido(silver_dev.3fn.normaliza_documento(cpf, 11)))  AS recusados
FROM atual;

-- COMMAND ----------

-- CPFs do Pátio (clientes pessoa física) na foto mais recente: o descarte do Pátio é um filtro, não uma expectation,
-- então esta consulta é a contagem dos recusados.
WITH fotos AS (
  SELECT *, to_date(regexp_extract(_file_name, '_([0-9]{8})_', 1), 'yyyyMMdd') AS data_foto
  FROM bronze_dev.patio.clientes
),
atual AS (SELECT * FROM fotos WHERE data_foto = (SELECT max(data_foto) FROM fotos) AND tipo_pessoa = 'F')
SELECT count(*) AS clientes_pf,
       count_if(NOT silver_dev.3fn.cpf_valido(silver_dev.3fn.normaliza_documento(documento, 11))) AS recusados
FROM atual;

-- COMMAND ----------

-- Valor monetário como texto no Apólice+: "1.234,56" -> DECIMAL. Quantas linhas não converteram? (esperado: 0)
SELECT count(*)                                                             AS apolices_versoes,
       count_if(silver_dev.3fn.converte_valor_brl(premio) IS NULL)          AS premio_nao_converteu,
       count_if(silver_dev.3fn.converte_valor_brl(importancia_segurada) IS NULL) AS importancia_nao_converteu
FROM bronze_dev.apolice.apolices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. A 3FN diante da aquisição (ponte para o artigo 05)
-- MAGIC
-- MAGIC A 3FN **não lê** o *SolTicket* nem o *SolFin* (os legados do Expresso Vale do Sol, adquirido em 01/07/2025), embora
-- MAGIC estejam na Bronze desde o artigo 01. As consultas abaixo medem, só na Bronze, o que quebraria se a 3FN os recebesse.
-- MAGIC Cada achado é uma linha da tabela "o que muda em cada tabela da 3FN" do artigo.

-- COMMAND ----------

-- SolTicket: CPFs fora do padrão e municípios fora do padrão, na foto mais recente de clientes
WITH atual AS (
  SELECT *
  FROM bronze_dev.solticket.clientes
  WHERE _file_name = (SELECT max(_file_name) FROM bronze_dev.solticket.clientes)
)
SELECT count(*)                                                                   AS clientes,
       count_if(cpf NOT RLIKE '^[0-9]{11}$')                                      AS cpf_fora_do_padrao,
       count_if(silver_dev.3fn.cpf_valido(silver_dev.3fn.normaliza_documento(cpf, 11))) AS cpf_valido_apos_normalizar,
       count_if(cod_municipio NOT RLIKE '^[0-9]{7}$')                             AS municipio_fora_do_padrao
FROM atual;

-- COMMAND ----------

-- SolTicket: a mesma pessoa cadastrada duas vezes (mesmo CPF normalizado, dois cod_cliente). A 3FN integra por CPF, então
-- as duas cópias virariam a mesma pessoa_id: um problema para a correspondência (que cópia é "a" chave do sistema?).
WITH atual AS (
  SELECT *, silver_dev.3fn.normaliza_documento(cpf, 11) AS cpf_norm
  FROM bronze_dev.solticket.clientes
  WHERE _file_name = (SELECT max(_file_name) FROM bronze_dev.solticket.clientes)
)
SELECT count(*) AS cpfs_repetidos, sum(copias) AS linhas_envolvidas
FROM (SELECT cpf_norm, count(*) AS copias FROM atual GROUP BY cpf_norm HAVING count(*) > 1);

-- COMMAND ----------

-- SolTicket: passagens que apontam para uma linha que não existe (a FK órfã). A 3FN teria de escolher entre rejeitar
-- e aceitar a passagem sem linha.
WITH p AS (
  SELECT * FROM bronze_dev.solticket.passagens
  WHERE _file_name = (SELECT max(_file_name) FROM bronze_dev.solticket.passagens)
),
l AS (
  SELECT cod_linha FROM bronze_dev.solticket.linhas
  WHERE _file_name = (SELECT max(_file_name) FROM bronze_dev.solticket.linhas)
)
SELECT count(*) AS passagens, count_if(l.cod_linha IS NULL) AS sem_linha
FROM p LEFT JOIN l ON l.cod_linha = p.cod_linha;

-- COMMAND ----------

-- SolFin x Núcleo: as categorias de título a pagar. Grafia, acento e lista de valores não coincidem; o de-para é decisão do
-- negócio (de quem?), e a 3FN não pode inventá-lo.
SELECT 'solfin' AS sistema, categoria, count(*) AS titulos
FROM bronze_dev.solfin.titulos_pagar
WHERE _file_name = (SELECT max(_file_name) FROM bronze_dev.solfin.titulos_pagar)
GROUP BY categoria
UNION ALL
SELECT 'nucleo', categoria, count(*)
FROM silver_dev.3fn.titulo_pagar
GROUP BY categoria
ORDER BY sistema, categoria;
