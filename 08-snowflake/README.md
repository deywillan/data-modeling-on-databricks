# 08 — Snowflake

## Como preparar os dados (`08_setup`)

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
databricks bundle run 08_setup -p <seu-perfil>
```

O job roda em compute serverless e deixa pronto, no Unity Catalog:

| Onde | O quê |
|---|---|
| `medalhao_08_snowflake.landing` | Volume `landing` com os arquivos dos sistemas de origem deste artigo, de 01/01/2024 a 31/12/2025 |
| `medalhao_08_snowflake.bronze`, `.silver`, `.gold` | schemas vazios, que o pipeline do artigo vai popular |

Este artigo não usa o schema `gabarito`.

Os sistemas deste artigo, um por pasta da landing:

| Pasta | Sistema | Como entrega |
|---|---|---|
| `patio/` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `ibge/` | municípios do IBGE | um CSV de referência, gravado na carga inicial |

Os arquivos ficam em `/Volumes/medalhao_08_snowflake/landing/landing/<sistema>/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `_controle/relogio/`; os pipelines leem só as pastas dos sistemas, nunca `_controle/`. O [README do gerador](../00-gerador/README.md) explica de onde vêm esses dados, o relógio, o roteiro e os defeitos que os arquivos trazem de propósito.

Os sistemas em comum com os outros artigos trazem os mesmos dados neles: o mesmo cliente tem os mesmos cadastros, com as mesmas chaves e os mesmos defeitos. Cada artigo tem o seu catálogo e roda sem os outros.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido. Os números do artigo usam a escala padrão.
- `reset` (padrão `false`): com `true`, apaga o catálogo inteiro e recria do zero.

```bash
databricks bundle run 08_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

Se o seu usuário não puder criar catálogos, o job avisa e usa a alternativa: o catálogo `medalhao`, com os schemas `a08_landing`, `a08_bronze`, `a08_silver` e `a08_gold`.

## Como avançar o tempo (`avancar_tempo`)

Depois do `08_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=08-snowflake,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes.

## Um artigo por vez

Cada artigo tem o próprio catálogo e roda sem depender dos outros, mas rode **um artigo por vez** no Free Edition. O workspace tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Termine o artigo antes de começar o próximo e, se quiser liberar espaço, apague o catálogo dele:

```sql
DROP CATALOG medalhao_08_snowflake CASCADE;
```

Para refazer o artigo depois, basta rodar o `08_setup` de novo.
