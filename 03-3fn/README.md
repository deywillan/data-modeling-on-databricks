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
| `medalhao_03_3fn.landing` | Volume `landing` com os arquivos dos sistemas de origem deste artigo, de 01/01/2024 a 31/12/2025 |
| `medalhao_03_3fn.bronze`, `.silver`, `.gold` | schemas vazios, que o pipeline do artigo vai popular |
| `medalhao_03_3fn.gabarito` | tabelas Delta com a "verdade" por trás da landing, para conferir a integração (veja abaixo) |

Os sistemas deste artigo, um por pasta da landing:

| Pasta | Sistema | Como entrega |
|---|---|---|
| `nucleo/` | *Núcleo*, ERP corporativo da holding | incremental por `updated_at` em Parquet, com exclusão lógica (`excluido`) |
| `embarque/` | *Embarque*, bilhetagem da ModelBricks Viação | CDC em JSON: uma linha por operação `I`/`U`/`D` |
| `patio/` | *Pátio*, DMS da ModelBricks Veículos | snapshot completo em CSV: a foto inteira de cada tabela na data (mensal até novembro de 2025 e diária a partir de dezembro) |
| `apolice/` | *Apólice+*, sistema da ModelBricks Seguros | incremental por `updated_at` em CSV, com valor monetário como texto (`"1.234,56"`) |

Os arquivos ficam em `/Volumes/medalhao_03_3fn/landing/landing/<sistema>/<entidade>/<AAAA-MM-DD>/`, em que a data é o dia simulado em que o lote chegou. O relógio simulado fica em `_controle/relogio/`; os pipelines leem só as pastas dos sistemas, nunca `_controle/`. O [README do gerador](../00-gerador/README.md) explica de onde vêm esses dados, o relógio, o roteiro e os defeitos que os arquivos trazem de propósito.

Os sistemas em comum com o artigo [02 — Historização](../02-historizacao/README.md) (*Núcleo*, *Embarque* e *Pátio*) trazem os mesmos dados nos dois: o mesmo cliente tem os mesmos cadastros, com as mesmas chaves e os mesmos defeitos. Cada artigo tem o seu catálogo e roda sem o outro.

### O gabarito

O schema `gabarito` mostra a verdade por trás dos arquivos: as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `participacoes`...), o `mapa_chaves` (a chave de cada entidade verdadeira em cada sistema deste artigo) e os `defeitos` injetados, com valor original e valor sujo. Serve para conferir a sua integração, por exemplo, quantos clientes duplicados ela encontrou. Ele nunca vai para a landing, e o pipeline do artigo não o lê.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido. Os números do artigo usam a escala padrão.
- `reset` (padrão `false`): com `true`, apaga o catálogo inteiro e recria do zero.

```bash
databricks bundle run 03_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

Se o seu usuário não puder criar catálogos, o job avisa e usa a alternativa: o catálogo `medalhao`, com os schemas `a03_landing`, `a03_bronze`, `a03_silver`, `a03_gold` e `a03_gabarito`.

## Como avançar o tempo (`avancar_tempo`)

Depois do `03_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=03-3fn,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo, grava apenas arquivos novos (nenhum arquivo já entregue muda) e atualiza o gabarito. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. O reajuste de tarifas do *Embarque* fica de propósito depois de 31/12/2025, para que você o veja acontecer. A data está na seção `eventos:` do [roteiro](../00-gerador/src/modelbricks_gerador/roteiro.yaml).

## Um artigo por vez

Cada artigo tem o próprio catálogo e roda sem depender dos outros, mas rode **um artigo por vez** no Free Edition. O workspace tem uma cota diária de compute (estourá-la desliga o compute pelo resto do dia) e aceita poucas execuções simultâneas de jobs e pipelines. Termine o artigo antes de começar o próximo e, se quiser liberar espaço, apague o catálogo dele:

```sql
DROP CATALOG medalhao_03_3fn CASCADE;
```

Para refazer o artigo depois, basta rodar o `03_setup` de novo.
