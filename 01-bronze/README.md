# 01 — Bronze

## Como preparar os dados (`01_setup`)

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) autenticada num perfil do seu workspace (funciona no Databricks Free Edition) e [`uv`](https://docs.astral.sh/uv/) instalado, que o deploy usa para construir o gerador de dados.

Na raiz do repositório:

```bash
databricks bundle deploy -p <seu-perfil>
databricks bundle run 01_setup -p <seu-perfil>
```

O job roda em compute serverless e deixa pronto, no Unity Catalog:

| Onde | O quê |
|---|---|
| `medalhao_01_bronze.landing` | Volume `landing` com os arquivos dos sistemas de origem da fatia deste artigo |
| `medalhao_01_bronze.bronze`, `.silver`, `.gold` | schemas vazios, que o pipeline do artigo vai popular |

O relógio simulado fica em `/Volumes/medalhao_01_bronze/landing/landing/_controle/relogio/`. Os pipelines leem só as pastas dos sistemas, nunca `_controle/`.

Parâmetros do job:

- `escala` (padrão `1`): multiplica os volumes de dados. Use `0.1` para um teste rápido.
- `reset` (padrão `false`): com `true`, apaga o catálogo inteiro e recria do zero.

```bash
databricks bundle run 01_setup -p <seu-perfil> --params escala=0.1,reset=true
```

Rodar o job de novo sem `reset` não altera nada: ele só informa a data do relógio.

Se o seu usuário não puder criar catálogos, o job avisa e usa a alternativa: o catálogo `medalhao`, com os schemas `a01_landing`, `a01_bronze`, `a01_silver` e `a01_gold`.

## Como avançar o tempo (`avancar_tempo`)

Depois do `01_setup`, o relógio simulado está em 31/12/2025. Para ver novos lotes chegarem à landing, avance o relógio e rode o pipeline de novo:

```bash
databricks bundle run avancar_tempo -p <seu-perfil> --params artigo=01-bronze,dias=1
```

O job gera os lotes dos dias seguintes ao relógio só para os sistemas deste artigo e grava apenas arquivos novos: nenhum arquivo já entregue muda. Avançar 3 dias de uma vez produz o mesmo conteúdo que avançar 1 dia três vezes. Alguns eventos do roteiro ficam de propósito depois de 31/12/2025, como o reajuste de tarifas do *Embarque* em 12/01/2026, para que você os veja acontecer.
