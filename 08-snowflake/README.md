# 08 — Snowflake

> **Este artigo segue uma ordem incremental de construção.** A série constrói um único lakehouse com a arquitetura medalhão, e cada artigo acrescenta uma parte a ele: o 00 prepara o projeto e a landing zone, o 01 monta a Bronze, os artigos 02 a 06 constroem a Silver a partir da Bronze, e os artigos 07 a 10 constroem a Gold a partir da Silver. Antes deste artigo, construa a landing zone com o `00_setup` (artigo 00) e rode os pipelines dos artigos 01 a 07, nesta ordem: este artigo lê o que eles construíram e não funciona sozinho.

## Onde este artigo se encaixa

| Camada | Lê de | Grava em |
|---|---|---|
| Gold | a Silver (`silver_dev`), construída nos artigos 02 a 06 | um schema próprio em `gold_dev` |

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
| `ibge` | municípios do IBGE | um CSV de referência, gravado na carga inicial |


## Como avançar o tempo (`avancar_tempo`)

Depois do `00_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio da série:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
```

O job gera os lotes dos dias seguintes de todos os sistemas e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

Depois de avançar o tempo, rode de novo os pipelines dos artigos 01 a 07, nesta ordem, e então o deste artigo: cada camada só enxerga o que a anterior já processou.

## Recursos do Free Edition

O workspace do Free Edition tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Rode os pipelines um de cada vez, na ordem da série.

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines dos artigos criaram, e grava a carga inicial de novo; depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```
