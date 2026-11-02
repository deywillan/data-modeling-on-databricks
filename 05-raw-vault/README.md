# 05 — Raw Vault

> **Este artigo segue uma ordem incremental de construção.** A série constrói um único lakehouse com a arquitetura medalhão, e cada artigo acrescenta uma parte a ele: o 00 prepara o projeto e a landing zone, o 01 monta a Bronze, os artigos 02 a 06 constroem a Silver a partir da Bronze, e os artigos 07 a 10 constroem a Gold a partir da Silver. Antes deste artigo, construa a landing zone com o `00_setup` (artigo 00) e rode os pipelines dos artigos 01 a 04, nesta ordem: este artigo lê o que eles construíram e não funciona sozinho.

## Onde este artigo se encaixa

| Camada | Lê de | Grava em |
|---|---|---|
| Silver | a Bronze (`bronze_dev`), construída no artigo 01 | um schema próprio em `silver_dev` |

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
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `passagemhub` | *PassagemHub*, marketplace parceiro da Viação | páginas de uma API em JSON aninhado |
| `solticket` | *SolTicket*, bilhetagem legada do Expresso Vale do Sol, empresa adquirida pelo grupo | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |
| `solfin` | *SolFin*, ERP legado do Expresso Vale do Sol | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |

A aquisição do Expresso Vale do Sol acontece em julho de 2025: os sistemas legados (*SolTicket* e *SolFin*) entregam toda a sua história de uma vez nessa data, e é esse o momento que o Raw Vault precisa absorver sem remodelar o que já existe.

### O gabarito

O schema `silver_dev.gabarito`, gravado pelo `00_setup`, mostra a verdade por trás dos arquivos: as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `municipios`...), o `mapa_chaves` (a chave de cada entidade verdadeira em cada sistema) e os `defeitos` injetados, com valor original e valor sujo. Este artigo o usa para conferir a sua solução, por exemplo, quantos clientes duplicados a integração encontrou. Os pipelines nunca leem o gabarito.

## Como avançar o tempo (`avancar_tempo`)

Depois do `00_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio da série:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
```

O job gera os lotes dos dias seguintes de todos os sistemas e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

Depois de avançar o tempo, rode de novo os pipelines dos artigos 01 a 04, nesta ordem, e então o deste artigo: cada camada só enxerga o que a anterior já processou.

## Recursos do Free Edition

O workspace do Free Edition tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Rode os pipelines um de cada vez, na ordem da série.

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines dos artigos criaram, e grava a carga inicial de novo; depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```
