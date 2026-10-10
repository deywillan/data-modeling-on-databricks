# 03 — 3FN

## Como preparar os dados (`03_setup`)

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
databricks bundle run 03_setup -p <seu-perfil>
```

O job roda em compute serverless e deixa pronto, no Unity Catalog:

| Onde | O quê |
|---|---|
| `a03_bronze_dev` | catálogo Bronze: um schema por sistema da tabela abaixo, cada um com o Volume `landing` (arquivos de 01/01/2024 a 31/12/2025), mais o schema `_controle`, com o relógio simulado |
| `a03_silver_dev`, `a03_gold_dev` | catálogos vazios, que o pipeline do artigo vai popular |
| `a03_silver_dev.gabarito` | tabelas Delta com a "verdade" por trás da landing, para conferir a integração (veja abaixo) |

O sufixo `_dev` vem do target `dev` do bundle, o padrão. Com `-t prod`, os mesmos catálogos ganham o sufixo `_prd` (`a03_bronze_prd`, `a03_silver_prd` e `a03_gold_prd`).

Os sistemas deste artigo, um schema por sistema no catálogo Bronze:

| Schema | Sistema | Como entrega |
|---|---|---|
| `nucleo` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `embarque` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `patio` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `apolice` | *Apólice+*, sistema da ModelBricks Seguros | incremental por `updated_at` em CSV, com valor monetário como texto (`"1.234,56"`) |

Os arquivos ficam em `/Volumes/a03_bronze_dev/<sistema>/landing/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `/Volumes/a03_bronze_dev/_controle/relogio/`; os pipelines leem só os schemas dos sistemas, nunca o `_controle`. O [README do gerador](../00-gerador/README.md) explica de onde vêm esses dados, o relógio, o roteiro e os defeitos que os arquivos trazem de propósito.

Os sistemas em comum com o artigo [02 — Historização](../02-historizacao/README.md) (*Núcleo*, *Embarque* e *Pátio*) trazem os mesmos dados nos dois: o mesmo cliente tem os mesmos cadastros, com as mesmas chaves, exceto pelos defeitos que este artigo injeta e registra no gabarito (o 02 não tem defeitos). Cada artigo tem os seus catálogos e roda sem o outro.

### O gabarito

O schema `gabarito`, no catálogo `a03_silver_dev`, mostra a verdade por trás dos arquivos: as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `participacoes`...), o `mapa_chaves` (a chave de cada entidade verdadeira em cada sistema deste artigo) e os `defeitos` injetados, com valor original e valor sujo. Serve para conferir a sua integração, por exemplo, quantos clientes duplicados ela encontrou. Ele nunca vai para a landing, e o pipeline do artigo não o lê.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido. Os números do artigo usam a escala padrão.
- `reset` (padrão `false`): com `true`, apaga os três catálogos do artigo e recria tudo do zero.

```bash
databricks bundle run 03_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

O job precisa da permissão de criar catálogos, que o Free Edition concede.

## Como avançar o tempo (`avancar_tempo`)

Depois do `03_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=03-3fn,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo, grava apenas arquivos novos (nenhum arquivo já entregue muda) e atualiza o gabarito. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. O reajuste de tarifas do *Embarque* fica de propósito depois de 31/12/2025, para que você o veja acontecer. A data está na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

## Um artigo por vez

Cada artigo tem os próprios catálogos e roda sem depender dos outros, mas rode **um artigo por vez** no Free Edition. O workspace tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Termine o artigo antes de começar o próximo e, se quiser liberar espaço, apague os catálogos dele:

```sql
DROP CATALOG a03_bronze_dev CASCADE;
DROP CATALOG a03_silver_dev CASCADE;
DROP CATALOG a03_gold_dev CASCADE;
```

Para refazer o artigo depois, basta rodar o `03_setup` de novo.
