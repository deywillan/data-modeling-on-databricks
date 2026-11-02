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
| `ibge` | municípios do IBGE | um CSV de referência, gravado na carga inicial |

Os arquivos ficam em `/Volumes/bronze_dev/<sistema>/landing/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `/Volumes/bronze_dev/_controle/relogio/`; os pipelines leem só os schemas dos sistemas, nunca o `_controle`.

Os arquivos trazem os defeitos do catálogo do gerador (CPF fora do padrão, contato nulo, CDC reentregue, evento atrasado, encoding `latin1`...). A Bronze **preserva** esses defeitos e, no máximo, os marca; a correção acontece nos artigos da Silver. O [README do gerador](../00-gerador/README.md) explica de onde vêm os dados, o relógio, o roteiro e o catálogo de defeitos.

## Como avançar o tempo (`avancar_tempo`)

Depois do `00_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio da série:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
```

O job gera os lotes dos dias seguintes de todos os sistemas e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

Depois de avançar o tempo, rode o pipeline deste artigo para processar os lotes novos.

## Recursos do Free Edition

O workspace do Free Edition tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Rode os pipelines um de cada vez, na ordem da série.

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines dos artigos criaram, e grava a carga inicial de novo; depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```
