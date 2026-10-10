# 02 — Historização

## Como preparar os dados (`02_setup`)

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
databricks bundle run 02_setup -p <seu-perfil>
```

O job roda em compute serverless e deixa pronto, no Unity Catalog:

| Onde | O quê |
|---|---|
| `medalhao_02_historizacao.landing` | Volume `landing` com os arquivos dos sistemas de origem deste artigo, de 01/01/2024 a 31/12/2025 |
| `medalhao_02_historizacao.bronze`, `.silver`, `.gold` | schemas vazios, que o pipeline do artigo vai popular |

Este artigo não usa o schema `gabarito`.

Os sistemas deste artigo, um por pasta da landing:

| Pasta | Sistema | Como entrega |
|---|---|---|
| `patio/` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `embarque/` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `nucleo/` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `rotacerta/` | *RotaCerta*, TMS da ModelBricks Cargas | CDC de fretes em JSON, como o *Embarque* |

Os arquivos ficam em `/Volumes/medalhao_02_historizacao/landing/landing/<sistema>/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `_controle/relogio/`; os pipelines leem só as pastas dos sistemas, nunca `_controle/`. O [README do gerador](../00-gerador/README.md) explica de onde vêm esses dados, o relógio, o roteiro e os defeitos que os arquivos trazem de propósito.

Os sistemas em comum com o artigo [03 — 3FN](../03-3fn/README.md) (*Núcleo*, *Embarque* e *Pátio*) trazem os mesmos dados nos dois: o mesmo cliente tem os mesmos cadastros, com as mesmas chaves e os mesmos defeitos. Cada artigo tem o seu catálogo e roda sem o outro.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido. Os números do artigo usam a escala padrão.
- `reset` (padrão `false`): com `true`, apaga o catálogo inteiro e recria do zero.

```bash
databricks bundle run 02_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

Se o seu usuário não puder criar catálogos, o job avisa e usa a alternativa: o catálogo `medalhao`, com os schemas `a02_landing`, `a02_bronze`, `a02_silver` e `a02_gold`.

## Como avançar o tempo (`avancar_tempo`)

Depois do `02_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=02-historizacao,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. O reajuste de tarifas do *Embarque* fica de propósito depois de 31/12/2025, para que você o veja acontecer. A data está na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

## Um artigo por vez

Cada artigo tem o próprio catálogo e roda sem depender dos outros, mas rode **um artigo por vez** no Free Edition. O workspace tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Termine o artigo antes de começar o próximo e, se quiser liberar espaço, apague o catálogo dele:

```sql
DROP CATALOG medalhao_02_historizacao CASCADE;
```

Para refazer o artigo depois, basta rodar o `02_setup` de novo.
