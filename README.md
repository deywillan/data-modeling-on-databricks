# Um caminho para implementação da Arquitetura Medalhão

Conteúdo prático da série de artigos **"Um caminho para implementação da Arquitetura Medalhão"**, sobre a aplicação de técnicas de modelagem de dados nas camadas Bronze, Silver e Gold no Databricks.

Todos os exemplos usam o **Grupo ModelBricks**, uma holding fictícia com empresas de transporte de passageiros, de cargas, uma concessionária de veículos e uma corretora de seguros. Cada empresa tem seus próprios sistemas de origem, com formatos, chaves e defeitos próprios. Os dados são sintéticos e gerados por código (pasta [`00-gerador/`](00-gerador/README.md)).

## Um lakehouse, construído em ordem

A série constrói **um único lakehouse** com a arquitetura medalhão, e cada artigo acrescenta uma parte a ele:

```
00 landing zone ──► 01 Bronze ──► 02 a 06 Silver ──► 07 a 10 Gold
   (00_setup)       (todos os     (todas as modelagens   (a partir
                     sistemas)     leem a mesma Bronze)    da Silver)
```

As pastas **não são autocontidas**: cada artigo lê o que os anteriores construíram. Siga a ordem dos artigos. O README de cada pasta diz o que precisa estar pronto antes dele.

| Artigo | Pasta | Camada |
|---|---|---|
| 00 — Abertura: o projeto e a landing zone | [`00-gerador/`](00-gerador/README.md) | landing zone |
| 01 — Bronze | [`01-bronze/`](01-bronze/README.md) | Bronze |
| 02 — Historização | [`02-historizacao/`](02-historizacao/README.md) | transversal, com exemplos na Silver |
| 03 — 3FN | [`03-3fn/`](03-3fn/README.md) | Silver |
| 04 — ODS | [`04-ods/`](04-ods/README.md) | Silver |
| 05 — Raw Vault | [`05-raw-vault/`](05-raw-vault/README.md) | Silver |
| 06 — Business Vault | [`06-business-vault/`](06-business-vault/README.md) | Silver |
| 07 — Star Schema | [`07-star-schema/`](07-star-schema/README.md) | Gold |
| 08 — Snowflake | [`08-snowflake/`](08-snowflake/README.md) | Gold |
| 09 — OBT | [`09-obt/`](09-obt/README.md) | Gold |
| 10 — Data Marts | [`10-data-marts/`](10-data-marts/README.md) | Gold |

## Pré-requisitos

- Um workspace Databricks. O [Databricks Free Edition](https://www.databricks.com/learn/free-edition) basta: todo o código roda em compute serverless.
- A [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install), autenticada num perfil do seu workspace:

  ```bash
  databricks auth login --host <url-do-seu-workspace> --profile <seu-perfil>
  ```

- O [`uv`](https://docs.astral.sh/uv/), que o deploy usa para construir o gerador de dados.

Para rodar a série, basta isso: os jobs e os pipelines rodam no workspace. Para rodar na sua máquina os testes e os scripts do gerador, com Python 3.12, use o `uv sync` em `00-gerador/` ou, com pip, o [`requirements.txt`](requirements.txt):

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

## Como rodar

O repositório é um único bundle (Declarative Automation Bundles), com o `databricks.yml` nesta pasta. Todos os comandos rodam aqui, na raiz.

1. **Instale os recursos no workspace.** Rode de novo sempre que atualizar o repositório.

   ```bash
   databricks bundle deploy -p <seu-perfil>
   ```

2. **Construa a landing zone (artigo 00), uma vez só.** O job cria os catálogos `bronze_dev`, `silver_dev` e `gold_dev` e grava a carga inicial de todos os sistemas. Detalhes em [Como gerar os dados](00-gerador/README.md#como-gerar-os-dados).

   ```bash
   databricks bundle run 00_setup -p <seu-perfil>
   ```

3. **Rode o pipeline de cada artigo, na ordem.** O nome do recurso e o que conferir estão no README da pasta do artigo.

   ```bash
   databricks bundle run <recurso-do-artigo> -p <seu-perfil>
   ```

4. **Avance o tempo, quando quiser ver dados novos chegarem.** Depois, rode de novo os pipelines dos artigos que você já construiu, na ordem da série.

   ```bash
   databricks bundle run avancar_tempo -p <seu-perfil> --params dias=1
   ```

Os comandos usam o target `dev`, o padrão, que gera catálogos com sufixo `_dev`. Com `-t prod` em todos os comandos, os catálogos ganham o sufixo `_prd`.

## Um job ou pipeline por vez

O Free Edition tem limites que afetam a série ([documentação](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)):

- **um pipeline ativo por tipo de pipeline**: o pipeline de um artigo precisa terminar antes de você iniciar o do próximo;
- **no máximo 5 tarefas de job simultâneas** por conta;
- uma **cota de uso** de compute: ao estourá-la, o compute do workspace é desligado pelo resto do dia. Os dados e as configurações não são apagados.

Por isso, rode **um artigo de cada vez**, na ordem, e espere cada execução terminar antes da próxima.

## Recomeçar e limpar

Para recomeçar a série do zero, rode o `00_setup` com `reset=true`. Ele apaga os três catálogos, inclusive as tabelas que os pipelines criaram, e grava a carga inicial de novo. Depois, refaça os artigos a partir do 01.

```bash
databricks bundle run 00_setup -p <seu-perfil> --params reset=true
```

Para liberar espaço ao terminar, remova os recursos do bundle e apague os três catálogos num editor SQL do workspace:

```bash
databricks bundle destroy -p <seu-perfil>
```

```sql
DROP CATALOG bronze_dev CASCADE;
DROP CATALOG silver_dev CASCADE;
DROP CATALOG gold_dev CASCADE;
```
