# 10 — Data Marts

## Como preparar os dados (`10_setup`)

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
databricks bundle run 10_setup -p <seu-perfil>
```

O job roda em compute serverless e deixa pronto, no Unity Catalog:

| Onde | O quê |
|---|---|
| `a10_bronze_dev` | catálogo Bronze: um schema por sistema da tabela abaixo, cada um com o Volume `landing` (arquivos de 01/01/2024 a 31/12/2025), mais o schema `_controle`, com o relógio simulado |
| `a10_silver_dev`, `a10_gold_dev` | catálogos vazios, que o pipeline do artigo vai popular |
| `a10_silver_dev.gabarito` | tabelas Delta com a "verdade" por trás da landing, para conferir a integração (veja abaixo) |

O sufixo `_dev` vem do target `dev` do bundle, o padrão. Com `-t prod`, os mesmos catálogos ganham o sufixo `_prd` (`a10_bronze_prd`, `a10_silver_prd` e `a10_gold_prd`).

Os sistemas deste artigo, um schema por sistema no catálogo Bronze:

| Schema | Sistema | Como entrega |
|---|---|---|
| `nucleo` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `rastro` | *Rastro*, telemetria da frota própria | muitos arquivos pequenos de JSON aninhado por dia; a carga inicial traz só os últimos dias (a janela de telemetria da fatia, no roteiro) |
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `passagemhub` | *PassagemHub*, marketplace parceiro da Viação | páginas de uma API em JSON aninhado |
| `rotacerta` | *RotaCerta*, TMS da ModelBricks Cargas | CDC de fretes em JSON, como o *Embarque* |
| `patio` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `apolice` | *Apólice+*, sistema da ModelBricks Seguros | incremental por `updated_at` em CSV, com valor monetário como texto (`"1.234,56"`) |
| `solticket` | *SolTicket*, bilhetagem legada do Expresso Vale do Sol, empresa adquirida pelo grupo | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |
| `solfin` | *SolFin*, ERP legado do Expresso Vale do Sol | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |
| `ibge` | municípios do IBGE | um CSV de referência, gravado na carga inicial |

Os arquivos ficam em `/Volumes/a10_bronze_dev/<sistema>/landing/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `/Volumes/a10_bronze_dev/_controle/relogio/`; os pipelines leem só os schemas dos sistemas, nunca o `_controle`. O [README do gerador](../00-gerador/README.md) explica de onde vêm esses dados, o relógio, o roteiro e os defeitos que os arquivos trazem de propósito.

Este é o único artigo com todos os sistemas do grupo. Como a mesma população mestre alimenta todos eles, os pontos de convergência aparecem nas divisões: a mesma pessoa pode comprar passagem, seguro e veículo, a mesma empresa pode contratar frete e comprar caminhão, os veículos vendidos pelo *Pátio* reaparecem no *Núcleo* (como ativo), no *RotaCerta* e na telemetria do *Rastro*, e as rotas, bases e lojas referenciam os municípios do IBGE. Os sistemas em comum com os outros artigos trazem os mesmos dados neles: o mesmo cliente tem os mesmos cadastros, com as mesmas chaves; os defeitos do catálogo só existem nos artigos que têm gabarito. Cada artigo tem os seus catálogos e roda sem os outros.

### O gabarito

O schema `gabarito`, no catálogo `a10_silver_dev`, mostra a verdade por trás dos arquivos: as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `municipios`...), o `mapa_chaves` (a chave de cada entidade verdadeira em cada sistema deste artigo) e os `defeitos` injetados, com valor original e valor sujo. Serve para conferir a sua integração, por exemplo, quantos clientes duplicados ela encontrou. Ele nunca vai para a landing, e o pipeline do artigo não o lê.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido. Os números do artigo usam a escala padrão.
- `reset` (padrão `false`): com `true`, apaga os três catálogos do artigo e recria tudo do zero.

```bash
databricks bundle run 10_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

O job precisa da permissão de criar catálogos, que o Free Edition concede.

## Como avançar o tempo (`avancar_tempo`)

Depois do `10_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=10-data-marts,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo, grava apenas arquivos novos (nenhum arquivo já entregue muda) e atualiza o gabarito. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

## Um artigo por vez

Cada artigo tem os próprios catálogos e roda sem depender dos outros, mas rode **um artigo por vez** no Free Edition. O workspace tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Termine o artigo antes de começar o próximo e, se quiser liberar espaço, apague os catálogos dele:

```sql
DROP CATALOG a10_bronze_dev CASCADE;
DROP CATALOG a10_silver_dev CASCADE;
DROP CATALOG a10_gold_dev CASCADE;
```

Para refazer o artigo depois, basta rodar o `10_setup` de novo.
