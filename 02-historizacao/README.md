# 02 — Historização

> **Este artigo segue uma ordem incremental de construção.** A série constrói um único lakehouse com a arquitetura medalhão, e cada artigo acrescenta uma parte a ele: o 00 prepara o projeto e a landing zone, o 01 monta a Bronze, os artigos 02 a 06 constroem a Silver a partir da Bronze, e os artigos 07 a 10 constroem a Gold a partir da Silver. Antes deste artigo, construa a landing zone com o `00_setup` (artigo 00) e rode o pipeline do artigo 01: este artigo lê o que eles construíram e não funciona sozinho.

## Onde este artigo se encaixa

| Camada | Lê de | Grava em |
|---|---|---|
| transversal, com exemplos na Silver | a Bronze (`bronze_dev`), construída no artigo 01 | um schema próprio em `silver_dev` |

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
| `patio` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `nucleo` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `rotacerta` | *RotaCerta*, TMS da ModelBricks Cargas | CDC de fretes em JSON, como o *Embarque* |

## Como construir a historização

Depois do deploy, e com a Bronze já construída pelo pipeline `01_bronze`, rode o pipeline do artigo:

```bash
databricks bundle run 02_historizacao -p <seu-perfil>
```

O pipeline `02_historizacao` (Lakeflow Spark Declarative Pipelines, em compute serverless) lê a Bronze em `bronze_dev` e grava os padrões de historização em `silver_dev.historizacao`. Os dois snapshots de Kimball, que já são fatos prontos para consumo, ficam em `gold_dev.historizacao`. O pipeline cria os dois schemas se eles ainda não existirem. O código fica em [`src/`](src/), um notebook SQL por exemplo do artigo:

| Notebook | Tabela | Padrão | Mecanismo |
|---|---|---|---|
| [`tarifas_embarque.sql`](src/tarifas_embarque.sql) | `silver_dev.historizacao.embarque_tarifas_scd1` | SCD1 | `AUTO CDC` sobre o CDC do *Embarque* |
| | `silver_dev.historizacao.embarque_tarifas_scd2` | SCD2 | `AUTO CDC` sobre a mesma origem |
| [`clientes_patio.sql`](src/clientes_patio.sql) | `silver_dev.historizacao.patio_clientes_scd2` | SCD2 | `AUTO CDC ... FROM SNAPSHOT` sobre as fotos do *Pátio* |
| | `silver_dev.historizacao.patio_clientes_atributos_fixos` | SCD0 | materialized view com o primeiro valor recebido |
| [`fornecedores_nucleo.sql`](src/fornecedores_nucleo.sql) | `silver_dev.historizacao.nucleo_fornecedores_scd1` | SCD1 com exclusão lógica | `AUTO CDC` sobre o incremental do *Núcleo* |
| [`snapshots_gold.sql`](src/snapshots_gold.sql) | `gold_dev.historizacao.fato_ciclo_frete` | *accumulating snapshot* | materialized view sobre o CDC do *RotaCerta* |
| | `gold_dev.historizacao.fato_valor_contabil_mensal` | *periodic snapshot* | materialized view sobre as versões mensais dos ativos do *Núcleo* |

Como o schema `historizacao` reúne tabelas de vários sistemas, o nome de cada tabela começa pelo sistema de origem.

Algumas escolhas do código, explicadas no artigo:

- **Tipagem na Silver.** A Bronze guarda o JSON em `VARIANT` e o CSV em `STRING`. A Silver dá tipo a cada campo (`payload:valor::DECIMAL(10,2)`, `CAST(numero AS INT)`). O Parquet do *Núcleo* já chega tipado.
- **Ordem pela data da fonte.** O `AUTO CDC` ordena as operações por `ts_operacao` (*Embarque*) e `updated_at` (*Núcleo*), e as fotos do *Pátio* pela data no nome do arquivo. Nunca por `_ingestion_at`, que é o instante da carga. No *RotaCerta*, cerca de 2% das mudanças chegam atrasadas, com `seq` fora de ordem, e o *accumulating snapshot* também ordena por `ts_operacao`.
- **Sem metadados da Bronze na Silver.** `_ingestion_at`, `_file_name`, `_system_origin` e `_load_type` ficam na Bronze. No SCD2, uma coluna que muda a cada entrega abriria versões que não correspondem a nenhuma mudança na fonte.
- **CDF automático na tabela SCD1 de tarifas.** O pipeline liga o Change Data Feed clássico nas tabelas de `AUTO CDC`, e o feed só vale a partir da versão em que ele foi ligado. A tabela é criada com `delta.enableChangeDataFeed = false`, e o CDF automático assume, com feed desde a primeira versão.
- **Colunas fora do `TRACK HISTORY ON` não ficam congeladas.** No SCD2 dos clientes do *Pátio*, elas são sobrescritas na versão vigente, como no SCD1. Por isso o SCD0 das datas é uma tabela à parte.

## Como conferir o resultado

As consultas do artigo estão no notebook [`consultas/verificacao.sql`](consultas/verificacao.sql), que o deploy envia para a pasta do bundle no workspace. Ele não faz parte do pipeline.

O reajuste de tarifas acontece em 12/01/2026, depois do fim da carga inicial. Para vê-lo como mudança nas tabelas SCD1 e SCD2 e no Change Data Feed, rode o pipeline uma vez com a carga inicial, avance o relógio e rode os pipelines de novo, na ordem:

```bash
databricks bundle run 02_historizacao -p <seu-perfil>
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=15
databricks bundle run 01_bronze -p <seu-perfil>
databricks bundle run 02_historizacao -p <seu-perfil>
```


## Como avançar o tempo (`avancar_tempo`)

Depois do `00_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio da série:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
```

O job gera os lotes dos dias seguintes de todos os sistemas e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

Depois de avançar o tempo, rode de novo o pipeline do artigo 01, e então o deste artigo: cada camada só enxerga o que a anterior já processou.

## Recursos do Free Edition

O workspace do Free Edition tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Rode os pipelines um de cada vez, na ordem da série.

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines dos artigos criaram, e grava a carga inicial de novo; depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```
