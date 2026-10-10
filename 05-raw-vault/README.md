# 05 — Raw Vault

## Como preparar os dados (`05_setup`)

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
databricks bundle run 05_setup -p <seu-perfil>
```

O job roda em compute serverless e deixa pronto, no Unity Catalog:

| Onde | O quê |
|---|---|
| `a05_bronze_dev` | catálogo Bronze: um schema por sistema da tabela abaixo, cada um com o Volume `landing` (arquivos de 01/01/2024 a 31/12/2025), mais o schema `_controle`, com o relógio simulado |
| `a05_silver_dev`, `a05_gold_dev` | catálogos vazios, que o pipeline do artigo vai popular |
| `a05_silver_dev.gabarito` | tabelas Delta com a "verdade" por trás da landing, para conferir a integração (veja abaixo) |

O sufixo `_dev` vem do target `dev` do bundle, o padrão. Com `-t prod`, os mesmos catálogos ganham o sufixo `_prd` (`a05_bronze_prd`, `a05_silver_prd` e `a05_gold_prd`).

Os sistemas deste artigo, um schema por sistema no catálogo Bronze:

| Schema | Sistema | Como entrega |
|---|---|---|
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `passagemhub` | *PassagemHub*, marketplace parceiro da Viação | páginas de uma API em JSON aninhado |
| `solticket` | *SolTicket*, bilhetagem legada do Expresso Vale do Sol, empresa adquirida pelo grupo | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |
| `solfin` | *SolFin*, ERP legado do Expresso Vale do Sol | carga histórica única na data da aquisição e, depois, snapshots periódicos, em CSV com `;` e `latin1` |

Os arquivos ficam em `/Volumes/a05_bronze_dev/<sistema>/landing/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `/Volumes/a05_bronze_dev/_controle/relogio/`; os pipelines leem só os schemas dos sistemas, nunca o `_controle`. O [README do gerador](../00-gerador/README.md) explica de onde vêm esses dados, o relógio, o roteiro e os defeitos que os arquivos trazem de propósito.

Os sistemas são os mesmos do artigo [06 — Business Vault](../06-business-vault/README.md) e trazem os mesmos dados: o mesmo cliente tem os mesmos cadastros, com as mesmas chaves e os mesmos defeitos. Cada artigo tem os seus catálogos e roda sem o outro.

### O gabarito

O schema `gabarito`, no catálogo `a05_silver_dev`, mostra a verdade por trás dos arquivos: as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `municipios`...), o `mapa_chaves` (a chave de cada entidade verdadeira em cada sistema deste artigo) e os `defeitos` injetados, com valor original e valor sujo. Serve para conferir a sua integração, por exemplo, quantos clientes duplicados ela encontrou. Ele nunca vai para a landing, e o pipeline do artigo não o lê.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido. Os números do artigo usam a escala padrão.
- `reset` (padrão `false`): com `true`, apaga os três catálogos do artigo e recria tudo do zero.

```bash
databricks bundle run 05_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

O job precisa da permissão de criar catálogos, que o Free Edition concede.

## Como avançar o tempo (`avancar_tempo`)

Depois do `05_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=05-raw-vault,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo, grava apenas arquivos novos (nenhum arquivo já entregue muda) e atualiza o gabarito. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como a coluna nova no payload do *PassagemHub* e o reajuste de tarifas do *Embarque*, para que você os veja acontecer. As datas estão na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

## Um artigo por vez

Cada artigo tem os próprios catálogos e roda sem depender dos outros, mas rode **um artigo por vez** no Free Edition. O workspace tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Termine o artigo antes de começar o próximo e, se quiser liberar espaço, apague os catálogos dele:

```sql
DROP CATALOG a05_bronze_dev CASCADE;
DROP CATALOG a05_silver_dev CASCADE;
DROP CATALOG a05_gold_dev CASCADE;
```

Para refazer o artigo depois, basta rodar o `05_setup` de novo.
