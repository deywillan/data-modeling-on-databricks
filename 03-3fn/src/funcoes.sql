-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3FN: funções de padronização de documentos
-- MAGIC
-- MAGIC Funções SQL do Unity Catalog usadas pelo pipeline da 3FN para integrar pessoas pelo CPF. Ficam no schema
-- MAGIC `3fn` do catálogo Silver e são criadas **antes** do pipeline, porque o SQL do pipeline não cria funções:
-- MAGIC ele só as chama.
-- MAGIC
-- MAGIC | Função | O que faz |
-- MAGIC |---|---|
-- MAGIC | `normaliza_documento(doc, tamanho)` | Tira a máscara e repõe os zeros à esquerda perdidos. Não diz se o documento é válido. |
-- MAGIC | `digito_verificador_cpf(base)` | Calcula um dígito verificador do CPF (módulo 11) a partir dos 9 ou 10 primeiros dígitos. |
-- MAGIC | `cpf_valido(cpf)` | Diz se um CPF já normalizado (11 dígitos) é matematicamente válido. |
-- MAGIC | `normaliza_placa(placa)` | Placa só com letras e dígitos, em maiúsculas: a chave de integração do veículo. |
-- MAGIC | `converte_valor_brl(texto)` | Converte o valor monetário em texto no formato brasileiro (`"1.234,56"`) em `DECIMAL`. |
-- MAGIC
-- MAGIC **O cálculo do CPF.** Os dois últimos dígitos do CPF são verificadores, calculados a partir dos anteriores:
-- MAGIC
-- MAGIC 1. multiplique os 9 primeiros dígitos pelos pesos 10, 9, 8... 2 e some os produtos;
-- MAGIC 2. calcule o resto da divisão da soma por 11: se o resto for 0 ou 1, o primeiro dígito verificador é 0; senão,
-- MAGIC    é 11 menos o resto;
-- MAGIC 3. repita com os 10 primeiros dígitos (os 9 e o dígito que acabou de calcular), com os pesos 11, 10... 2,
-- MAGIC    para obter o segundo dígito verificador.
-- MAGIC
-- MAGIC Exemplo com `111.444.777-35`: a soma do passo 1 é 162, e 162 mod 11 = 8, então o primeiro dígito é 11 - 8 = 3.
-- MAGIC
-- MAGIC Sequências de um só dígito (`111.111.111-11`, `000.000.000-00`) passam no cálculo, mas não são CPFs emitidos e
-- MAGIC são recusadas. Passar no cálculo prova só que o número é bem formado: não diz se o CPF existe nem se pertence à
-- MAGIC pessoa do cadastro. Essa confirmação é uma consulta à Receita Federal, fora do escopo do lakehouse.
-- MAGIC
-- MAGIC **Por que validar antes de integrar.** A normalização repõe zeros à esquerda, e isso transforma lixo em algo com
-- MAGIC cara de CPF: `'111'` vira `'00000000111'`. Sem a validação, esse "CPF" viraria uma pessoa na 3FN.
-- MAGIC
-- MAGIC Parâmetro: `catalogo`, o catálogo Silver da série (`silver_dev` no target `dev`).

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'silver_dev';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS 3fn
COMMENT 'Silver em 3FN: o modelo integrado do Grupo ModelBricks (artigo 03).';

-- COMMAND ----------

USE SCHEMA 3fn;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION normaliza_documento(doc STRING, tamanho INT)
RETURNS STRING
COMMENT 'Documento só com dígitos e com o tamanho informado (11 para CPF, 14 para CNPJ numérico): tira a máscara e repõe os zeros à esquerda perdidos. NULL se não houver dígito ou se houver mais dígitos que o tamanho. Não valida o documento.'
RETURN CASE
  WHEN length(regexp_replace(doc, '[^0-9]', '')) BETWEEN 1 AND tamanho
    THEN lpad(regexp_replace(doc, '[^0-9]', ''), tamanho, '0')
END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION digito_verificador_cpf(base STRING)
RETURNS INT
COMMENT 'Dígito verificador do CPF (módulo 11). Com os 9 primeiros dígitos (pesos 10 a 2), devolve o primeiro dígito verificador; com os 10 primeiros (pesos 11 a 2), o segundo.'
RETURN aggregate(
  sequence(1, length(base)),
  0,
  -- soma ponderada: o primeiro dígito tem o maior peso (tamanho da base + 1), e o último, peso 2
  (soma, i) -> soma + try_cast(substr(base, i, 1) AS INT) * (length(base) + 2 - i),
  -- resto 0 ou 1 dá dígito 0; os demais, 11 menos o resto
  soma -> CASE WHEN pmod(soma, 11) < 2 THEN 0 ELSE 11 - pmod(soma, 11) END
);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cpf_valido(cpf STRING)
RETURNS BOOLEAN
COMMENT 'TRUE se o CPF, já normalizado (11 dígitos, sem máscara), é matematicamente válido: os dois dígitos verificadores conferem e não é uma sequência de um só dígito. FALSE nos demais casos, inclusive NULL.'
RETURN coalesce(
  cpf RLIKE '^[0-9]{11}$'
  AND NOT cpf RLIKE '^([0-9])\\1{10}$'
  AND try_cast(substr(cpf, 10, 1) AS INT) = digito_verificador_cpf(substr(cpf, 1, 9))
  AND try_cast(substr(cpf, 11, 1) AS INT) = digito_verificador_cpf(substr(cpf, 1, 10)),
  false
);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION normaliza_placa(placa STRING)
RETURNS STRING
COMMENT 'Placa só com letras e dígitos, em maiúsculas (ABC1D23): tira hífen, espaço e qualquer outro separador. NULL se a placa for nula ou ficar vazia.'
RETURN nullif(upper(regexp_replace(placa, '[^A-Za-z0-9]', '')), '');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION converte_valor_brl(texto STRING)
RETURNS DECIMAL(16,2)
COMMENT 'Converte o valor monetário em texto no formato brasileiro ("1.234,56": ponto de milhar, vírgula decimal) em DECIMAL(16,2). NULL se o texto for nulo ou não for um valor; try_cast evita que o modo ANSI do serverless interrompa o pipeline.'
RETURN try_cast(replace(replace(trim(texto), '.', ''), ',', '.') AS DECIMAL(16,2));
