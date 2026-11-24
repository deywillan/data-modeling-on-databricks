# 03 — 3FN

> **Este artigo segue uma ordem incremental de construção.** A série constrói um único lakehouse com a arquitetura medalhão, e cada artigo acrescenta uma parte a ele: o 00 prepara o projeto e a landing zone, o 01 monta a Bronze, os artigos 02 a 06 constroem a Silver a partir da Bronze, e os artigos 07 a 10 constroem a Gold a partir da Silver. Antes deste artigo, construa a landing zone com o `00_setup` (artigo 00) e rode o pipeline do artigo 01 (e o job `01_ibge`): este artigo lê o que eles construíram e não funciona sozinho. O artigo 02 não é pré-requisito.

## Onde este artigo se encaixa

| Camada | Lê de | Grava em |
|---|---|---|
| Silver | a Bronze (`bronze_dev`), construída no artigo 01 | `silver_dev.3fn` |

O sufixo `_dev` vem do target `dev` do bundle, o padrão. Com `-t prod`, os catálogos são `bronze_prd`, `silver_prd` e `gold_prd`.

## Como preparar o ambiente

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
```

A landing zone já deve estar pronta: ela é construída no artigo 00, com o `00_setup`, que roda uma vez para a série inteira ([Como gerar os dados](../00-gerador/README.md#como-gerar-os-dados)). Se você ainda não a construiu, comece por ali e siga a ordem dos artigos até aqui.

## As fontes que este artigo usa

O artigo trabalha com estes sistemas, já ingeridos na Bronze pelo artigo 01:

| Schema | Sistema | Como entrega |
|---|---|---|
| `nucleo` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `patio` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `apolice` | *Apólice+*, sistema da ModelBricks Seguros | incremental por `updated_at` em CSV, com valor monetário como texto (`"1.234,56"`) |

### O gabarito

O schema `silver_dev.gabarito`, gravado pelo `00_setup`, mostra a verdade por trás dos arquivos: as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `municipios`...), o `mapa_chaves` (a chave de cada entidade verdadeira em cada sistema) e os `defeitos` injetados, com valor original e valor sujo. Este artigo o usa para conferir a sua solução, por exemplo, quantos clientes duplicados a integração encontrou. Os pipelines nunca leem o gabarito.

## Como construir a 3FN

Depois do deploy, e com a Bronze já construída pelo pipeline `01_bronze` (e o espelho do IBGE, pelo job `01_ibge`), crie as funções e rode o pipeline do artigo:

```bash
databricks bundle run 03_funcoes -p <seu-perfil>
databricks bundle run 03_3fn -p <seu-perfil>
```

- **`03_funcoes`** é um job com uma tarefa SQL que cria cinco funções do Unity Catalog em `silver_dev.3fn` (código em [`src/funcoes.sql`](src/funcoes.sql)). Elas ficam fora do pipeline porque o SQL do pipeline só chama funções, não as cria. Rode uma vez; rode de novo só se alterar as funções.
- **`03_3fn`** (Lakeflow Spark Declarative Pipelines, em compute serverless) lê a Bronze em `bronze_dev` e grava o modelo integrado em `silver_dev.3fn`. Ele **não** lê as tabelas do artigo 02: a lógica de SCD2 se repete aqui, de propósito, para que cada modelagem da Silver parta dos mesmos dados da Bronze.

**Primeira execução.** Num workspace sem o pipeline, a primeira atualização do `03_3fn` terminou duas vezes com `FAILED unexpectedly` (todos os fluxos concluídos, sem erro de tabela). Rodar `databricks bundle run 03_3fn` de novo termina com sucesso; se acontecer com você, repita o comando.

O pipeline lê quatro sistemas (*Núcleo*, *Embarque*, *Pátio* e *Apólice+*) e o espelho do IBGE. Os legados *SolTicket* e *SolFin* já estão na Bronze, mas a 3FN **os ignora** de propósito: o modelo foi desenhado para os sistemas do grupo antes da aquisição. As consultas de verificação medem o que quebraria se eles entrassem.

### As funções

| Função | O que faz |
|---|---|
| `normaliza_documento(doc, tamanho)` | Tira a máscara e repõe os zeros à esquerda (CPF com 11, CNPJ com 14). `NULL` se não houver dígito ou se houver dígito demais. Não valida |
| `digito_verificador_cpf(base)` | O cálculo módulo 11 do dígito verificador do CPF, com `aggregate` |
| `cpf_valido(cpf)` | Os dois dígitos conferem e não é uma sequência de um só dígito |
| `normaliza_placa(placa)` | Placa em maiúsculas, sem separadores: a chave de integração do veículo |
| `converte_valor_brl(texto)` | `"1.234,56"` em `DECIMAL(16,2)`, sem interromper o pipeline se o texto não for um valor |

### As tabelas

Um notebook SQL por grupo de entidades, em [`src/`](src/). O nome das tabelas está no singular e é o da entidade de negócio, e não o de um sistema (`pessoa`, e não `embarque_clientes`).

| Notebook | Tabelas | O que são |
|---|---|---|
| [`referencias.sql`](src/referencias.sql) | `regiao`, `uf`, `municipio`, `linha`, `loja` | Referências sem histórico. A lista do IBGE, que na Bronze repete UF e região em cada município, é desmontada em três tabelas (3FN) |
| [`regra.sql`](src/regra.sql) | `regra_system_of_record` | A regra, escrita como dado: para cada atributo de pessoa e empresa, a ordem dos sistemas |
| [`pessoas.sql`](src/pessoas.sql) | `pessoa`, `pessoa_historico`, `pessoa_sistema`, `colaborador` | A pessoa física integrada pelo CPF: o estado atual (`pessoa`), as versões (`pessoa_historico`), a correspondência de chaves (`pessoa_sistema`) e o vínculo do colaborador (`colaborador`, SCD2) |
| [`empresas.sql`](src/empresas.sql) | `empresa`, `empresa_historico`, `empresa_sistema` | A pessoa jurídica integrada pelo CNPJ, com o mesmo desenho |
| [`veiculos.sql`](src/veiculos.sql) | `veiculo`, `veiculo_sistema`, `ativo` | O veículo integrado pela placa (*Pátio*, frota do *Núcleo* e apólices do *Apólice+*), com marca, modelo e tipo do *Pátio* |
| [`fatos.sql`](src/fatos.sql) | `passagem`, `venda_veiculo`, `apolice`, `titulo_pagar`, `nota_fiscal` | Os registros dos processos de negócio, ligados às entidades integradas |

As escolhas do código, explicadas no artigo:

- **Chave própria por hash.** `pessoa_id = sha2(cpf, 256)`, `empresa_id = sha2(cnpj, 256)`, `veiculo_id = sha2(placa, 256)`. O hash é determinístico (a mesma entidade gera a mesma chave depois de um *full refresh*) e serve a `AUTO CDC`, que não aceita colunas `IDENTITY` no alvo. É pseudonimização, não anonimização.
- **Um valor por atributo, pela *system of record*.** Quando dois sistemas gravam valores diferentes para o mesmo atributo da mesma pessoa, a 3FN guarda o valor da fonte que a `regra_system_of_record` elege (Apólice+, depois Embarque, Pátio e Núcleo, para pessoa; Apólice+, Núcleo e Pátio, para empresa), e cai para a seguinte quando a preferida não tem valor. É o método de Inmon: a escolha acontece na integração. As colunas `fonte_nome` e `fonte_contato` dizem de onde veio o valor. A ordem da regra é uma proposta que vem dos dados da série: numa empresa, é decisão do negócio, e se muda editando a tabela de regra. O valor das fontes não eleitas não é guardado.
- **Desempate: o cadastro mais recente.** Se a mesma pessoa tem dois cadastros no mesmo sistema, vale o de versão mais recente.
- **Histórico da escolha.** `pessoa_historico` e `empresa_historico` têm uma versão nova a cada vez que muda o valor **eleito**; uma mudança numa fonte que a regra ignora não abre versão. `pessoa` e `empresa` são o estado atual, e os fatos apontam para elas.
- **A correspondência de chaves fica à parte.** `pessoa_sistema`, `empresa_sistema` e `veiculo_sistema` guardam só a chave que cada sistema deu à entidade (e o papel dela ali), para voltar à origem.
- **Um alvo SCD2 por sistema, privado.** O *Pátio* entrega fotos (`AUTO CDC ... FROM SNAPSHOT`), e esse tipo de fluxo só divide um alvo com outros fluxos em SCD1, nunca em SCD2. Os alvos por sistema (`CREATE PRIVATE STREAMING TABLE`) alimentam o histórico e não aparecem no schema; só o `colaborador` (o vínculo, que só o *Núcleo* conhece) é público.
- **Ordem pela data da fonte** (`ts_operacao`, `updated_at`, data da foto), nunca por `_ingestion_at`.
- **Validação do CPF antes de integrar.** A normalização repõe os zeros e, sozinha, transforma `'111'` em `'00000000111'`. O CPF que não passa em `cpf_valido` não vira pessoa. A função fica numa coluna da view de preparação (`cpf_confere`), porque o pipeline não aceita função do Unity Catalog dentro de uma expectation.
- **Nada que a fonte entrega é corrigido além do formato.** O código de município fora do padrão do *Pátio* não é corrigido: como a regra só elege código que existe no IBGE, ele fica de fora, e a consulta de verificação o conta na Bronze.
- **Fatos como materialized views sobre a Bronze**, com o estado atual de cada registro. A passagem estornada e o título cancelado continuam na tabela, marcados (`estornada`, `cancelado`).
- **Metadados da Bronze não vão para a Silver.** No SCD2, uma coluna que muda a cada entrega abriria versões que não correspondem a nenhuma mudança na fonte.

### PK e FK

As chaves primárias e estrangeiras estão declaradas no código das tabelas e aparecem no Catalog Explorer (o diagrama ER é desenhado a partir delas). Elas são **informativas**: o Delta não impede uma duplicata nem uma FK órfã. Duas consequências aparecem no código:

- **A FK não cria dependência de execução.** O pipeline ordena as atualizações pelas leituras entre as tabelas, e `FOREIGN KEY ... REFERENCES` não é uma leitura. Sem uma leitura do pai, o filho e o pai atualizam ao mesmo tempo, e o filho pode falhar com `UC_REFERENTIAL_CONSTRAINT_DOES_NOT_EXIST`. Por isso cada tabela do modelo **junta o pai** na própria consulta (`municipio` junta `uf`, `passagem` junta `pessoa`...). A junção dá a ordem e, de quebra, faz valer a FK.
- **Quem impõe é a expectation.** As expectations contam o fato sem comprador, sem veículo ou sem fornecedor, e o código de município fora do padrão. Nos dados da série, quase todas contam zero: são rede de proteção. A constraint só muda pela declaração no código do pipeline (`ALTER TABLE` em streaming table falha).
- **`RELY` só onde a unicidade é garantida pela construção** (`pessoa`, `empresa`, `veiculo`, `regiao`, `uf` e `municipio`, por `SELECT DISTINCT` ou pela chave oficial).

O schema se chama `3fn` e funciona sem crase nas consultas (`silver_dev.3fn.pessoa`). A plataforma, porém, o exibe entre crases (`DESCRIBE SCHEMA`, `SHOW CREATE TABLE`), e ferramentas fora do Databricks podem exigir aspas, porque o identificador começa com dígito.

## Como conferir o resultado

As consultas do artigo estão no notebook [`consultas/verificacao.sql`](consultas/verificacao.sql), que o deploy envia para a pasta do bundle no workspace. Ele não faz parte do pipeline. Cobre: o que a integração juntou, a conferência com o `gabarito` (pessoas e empresas encontradas contra as verdadeiras), uma pessoa em vários sistemas e a fonte eleita de cada atributo (as 291 que viajam, compram um veículo e têm apólice, conferidas com o gabarito), o histórico da pessoa, as PK e FK que o pipeline não garante, as expectations no *event log*, os documentos recusados e, só na Bronze, o que os legados da aquisição quebrariam.

Para ver o histórico trabalhar, avance o relógio, rode a Bronze e depois este pipeline, e procure uma troca de telefone no *Pátio* (foto diária) ou no *Apólice+*: se o sistema é o eleito para o telefone, `pessoa_historico` ganha uma versão nova e a anterior é fechada.

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=15
databricks bundle run 01_bronze -p <seu-perfil>
databricks bundle run 03_3fn -p <seu-perfil>
```

## Como avançar o tempo (`avancar_tempo`)

Depois do `00_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio da série:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
```

O job gera os lotes dos dias seguintes de todos os sistemas e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

Depois de avançar o tempo, rode de novo o pipeline do artigo 01 e então o deste artigo: cada camada só enxerga o que a anterior já processou.

## Recursos do Free Edition

O workspace do Free Edition tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Rode os pipelines um de cada vez, na ordem da série.

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines dos artigos criaram, e grava a carga inicial de novo; depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```
