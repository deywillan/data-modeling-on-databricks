# 01 — Bronze

> **Este artigo segue uma ordem incremental de construção.** A série constrói um único lakehouse com a arquitetura medalhão, e cada artigo acrescenta uma parte a ele: o 00 prepara o projeto e a landing zone, o 01 monta a Bronze, os artigos 02 a 06 constroem a Silver a partir da Bronze, e os artigos 07 a 10 constroem a Gold a partir da Silver. Este artigo parte da landing zone pronta, construída no artigo 00 com o `00_setup`, e constrói a camada Bronze **inteira**, com todos os sistemas do grupo: todos os artigos seguintes leem dela.

## Onde este artigo se encaixa

| Camada | Lê de | Grava em |
|---|---|---|
| Bronze | os arquivos das landings, nos Volumes de `bronze_dev`, gravados pelo `00_setup` | as tabelas da Bronze em `bronze_dev`, um schema por sistema, ao lado do Volume `landing` |

O sufixo `_dev` vem do target `dev` do bundle, o padrão. Com `-t prod`, os catálogos são `bronze_prd`, `silver_prd` e `gold_prd`.

## Como preparar o ambiente

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
```

A landing zone já deve estar pronta: o projeto e a landing são preparados no artigo 00, com o `00_setup`, que roda uma vez para a série inteira. Ele cria os catálogos `bronze_dev`, `silver_dev` e `gold_dev`, grava nos Volumes `landing` a carga inicial de todos os sistemas (arquivos de 01/01/2024 a 31/12/2025) e o gabarito em `silver_dev.gabarito`. Se você ainda não o rodou, siga o passo a passo em [Como gerar os dados](../00-gerador/README.md#como-gerar-os-dados), no README do gerador.

## As fontes que este artigo usa

Todos os sistemas do grupo já estão na landing, um schema por sistema no catálogo `bronze_dev`, e este artigo ingere todos eles:

| Schema | Sistema | Como entrega |
|---|---|---|
| `nucleo` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `rastro` | *Rastro*, telemetria da frota própria | muitos arquivos pequenos de JSON aninhado por dia; a carga inicial traz só os últimos 90 dias |
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `passagemhub` | *PassagemHub*, marketplace parceiro da Viação | páginas de uma API em JSON aninhado |
| `rotacerta` | *RotaCerta*, TMS da ModelBricks Cargas | CDC de fretes em JSON, como o *Embarque* |
| `patio` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `apolice` | *Apólice+*, sistema da ModelBricks Seguros | incremental por `updated_at` em CSV, com valor monetário como texto (`"1.234,56"`) |
| `solticket` | *SolTicket*, bilhetagem legada do Expresso Vale do Sol, empresa adquirida pelo grupo | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |
| `solfin` | *SolFin*, ERP legado do Expresso Vale do Sol | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |
| `ibge` | municípios do IBGE | a lista completa em CSV, uma vez por ano (2 de janeiro): as de 2024 e 2025 na carga inicial e a de 2026 no `avancar_tempo` |

Os arquivos ficam em `/Volumes/bronze_dev/<sistema>/landing/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `/Volumes/bronze_dev/_controle/relogio/`; os pipelines leem só os schemas dos sistemas, nunca o `_controle`.

Os arquivos trazem os defeitos do catálogo do gerador (CPF fora do padrão, contato nulo, CDC reentregue, evento atrasado, encoding `latin1`...). A Bronze **preserva** esses defeitos e, no máximo, os marca; a correção acontece nos artigos da Silver. O [README do gerador](../00-gerador/README.md) explica de onde vêm os dados, o relógio, o roteiro e o catálogo de defeitos.

## Como construir a Bronze

Depois do deploy, rode o pipeline do artigo e, em seguida, o job do espelho do IBGE:

```bash
databricks bundle run 01_bronze -p <seu-perfil>
databricks bundle run 01_ibge -p <seu-perfil>
```

O pipeline `01_bronze` (Lakeflow Spark Declarative Pipelines, em compute serverless) cria uma streaming table append-only para cada entidade de cada sistema, lendo a landing com Auto Loader (`STREAM read_files`). O código fica em [`src/`](src/), um notebook SQL por sistema:

| Notebook | Tabelas | Como guarda |
|---|---|---|
| [`embarque.sql`](src/embarque.sql) | `embarque.clientes`, `tarifas`, `passagens` | JSON em `VARIANT` (`payload`), uma operação de CDC por linha |
| [`rotacerta.sql`](src/rotacerta.sql) | `rotacerta.clientes`, `caminhoes`, `fretes` | JSON em `VARIANT`, como o *Embarque* |
| [`rastro.sql`](src/rastro.sql) | `rastro.eventos` | JSON aninhado em `VARIANT`, um evento por linha |
| [`passagemhub.sql`](src/passagemhub.sql) | `passagemhub.eventos_venda` | JSON aninhado em `VARIANT`, uma página da API por linha, com expectation de contato em modo de aviso |
| [`patio.sql`](src/patio.sql) | `patio.lojas`, `clientes`, `veiculos`, `vendas`, `ordens_servico` | CSV em `STRING`; cada foto entra em append |
| [`apolice.sql`](src/apolice.sql) | `apolice.segurados`, `apolices` | CSV em `STRING`; o valor monetário fica como texto |
| [`nucleo.sql`](src/nucleo.sql) | `nucleo.colaboradores`, `fornecedores`, `titulos_pagar`, `notas_fiscais`, `ativos` | Parquet com os tipos do arquivo |
| [`solticket.sql`](src/solticket.sql) | `solticket.clientes`, `linhas`, `passagens` | CSV com `;` lido em `ISO-8859-1`, em `STRING` |
| [`solfin.sql`](src/solfin.sql) | `solfin.fornecedores`, `titulos_pagar` | CSV com `;` lido em `ISO-8859-1`, em `STRING` |
| [`ibge_municipios.sql`](src/ibge_municipios.sql) | `ibge.municipios` | espelho (overwrite), no job `01_ibge`, fora do pipeline |

Toda tabela leva os mesmos quatro metadados: `_ingestion_at` (instante da ingestão), `_file_name` (nome do arquivo de origem), `_system_origin` (sistema de origem) e `_load_type` (`append`, ou `overwrite` no espelho do IBGE). O catálogo vem do target do bundle: o pipeline recebe `catalogo_bronze` (`bronze_dev` no target `dev`) e o job recebe os caminhos já com o catálogo.

### As proteções das tabelas

Toda streaming table da Bronze é criada com `CLUSTER BY AUTO` e as mesmas propriedades (o *shredding*, só nas tabelas JSON):

| Propriedade | Valor | Por quê |
|---|---|---|
| `pipelines.reset.allowed` | `false` | É a mais importante. Um full refresh apaga a streaming table e a reconstrói a partir da landing, e a landing não é backup da Bronze: um arquivo que já saiu de lá não volta. Com a propriedade, o full refresh do pipeline passa por cima desta tabela, que continua recebendo só os arquivos novos. |
| `delta.appendOnly` | `true` | Recusa `UPDATE` e `DELETE` e garante na tabela o append-only que a camada promete. O `OPTIMIZE` e a compactação continuam funcionando, porque não alteram dados. |
| `delta.logRetentionDuration` | `interval 30 days` | Histórico para time travel e reprocessamento. Precisa ser maior ou igual a `deletedFileRetentionDuration`. |
| `delta.deletedFileRetentionDuration` | `interval 30 days` | Numa tabela append-only, o `VACUUM` só remove arquivos substituídos pela compactação. Com 30 dias, o time travel cobre o mesmo período do log. |
| `delta.enableTypeWidening` | `true` | Se a fonte alargar um tipo (`INT` para `BIGINT`), a ingestão não quebra. Na prática, só vale para o Parquet do *Núcleo*: o JSON fica em `VARIANT` e o CSV em `STRING`. |
| `delta.enableVariantShredding` | `true` | Só nas tabelas JSON (*Embarque*, *PassagemHub*, *Rastro* e *RotaCerta*). Grava os campos mais comuns do `payload` como colunas tipadas dentro do Parquet (*shredding*), e extrair um campo lê só ele, não o documento inteiro. O `CREATE TABLE` liga o *shredding* sozinho, mas a streaming table de um pipeline não: sem a propriedade, ela lê o VARIANT inteiro a cada consulta. Nos campos de dentro de uma lista, como os eventos de cada página do *PassagemHub*, o ganho é menor. |
| Clustering | `CLUSTER BY AUTO` | O Databricks escolhe e ajusta as colunas de clustering pelas consultas. Substitui `PARTITIONED BY` e Z-order, e o volume da série é pequeno demais para particionar. |

O espelho do IBGE recebe só as duas propriedades de retenção, que mantêm as versões anteriores ao alcance do `RESTORE`: ele é substituído a cada carga (sem `appendOnly`), não está no pipeline (sem `reset.allowed`), só tem colunas `STRING` (sem type widening) e é pequeno demais para clustering.

Para recomeçar a Bronze do zero, o caminho continua sendo o `00_setup` com `reset=true`, que apaga e recria os catálogos. O full refresh do pipeline deixa de servir para isso.

## Como avançar o tempo (`avancar_tempo`)

Depois do `00_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio da série:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
```

O job gera os lotes dos dias seguintes de todos os sistemas e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub*, o reajuste de tarifas do *Embarque* e a lista de municípios do IBGE de 2026, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

Depois de avançar o tempo, rode `01_bronze` e `01_ibge` de novo. O pipeline processa só os arquivos novos; o job substitui o espelho do IBGE pela lista mais recente.

## O espelho do IBGE (`01_ibge`)

A tabela `bronze_dev.ibge.municipios` é a única da Bronze que não é append-only: a cada carga, ela é substituída pelo conteúdo do arquivo mais recente da landing. Como o pipeline só cria streaming tables e materialized views, o espelho fica num job à parte, com um notebook SQL ([`src/ibge_municipios.sql`](src/ibge_municipios.sql)) que:

1. confere o volume do arquivo mais recente e falha se ele tiver menos de 5.000 linhas, sem tocar na tabela;
2. substitui a tabela com `CREATE OR REPLACE TABLE`, numa transação só, mantendo o histórico de versões;
3. informa quantas linhas têm código de município fora do padrão ou nome vazio, sem descartar nenhuma.

```bash
databricks bundle run 01_ibge -p <seu-perfil>
```

Na carga inicial, a landing tem as listas de 2024 e 2025 (5.570 municípios cada), e a tabela fica com a de 2025. Depois de avançar o relógio além de 02/01/2026 e rodar o job de novo, a tabela passa a ter a lista de 2026, com 5.571 municípios (entra Boa Esperança do Norte, MT). As listas anteriores continuam na landing, e as versões anteriores da tabela ficam acessíveis por time travel até o `VACUUM`:

```sql
DESCRIBE HISTORY bronze_dev.ibge.municipios;
SELECT count(*) FROM bronze_dev.ibge.municipios VERSION AS OF 0;
```

## Recursos do Free Edition

O workspace do Free Edition tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Rode os pipelines um de cada vez, na ordem da série.

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines dos artigos criaram, e grava a carga inicial de novo; depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```
