# IBGE: municípios, UFs e regiões

Dado público de referência, não é um sistema do grupo. É a lista oficial de municípios brasileiros com a hierarquia geográfica do IBGE. Todos os sistemas usam o código de município do IBGE nos endereços, nas rotas e nas lojas, então este arquivo é a dimensão geográfica comum a todas as bases.

| | |
|---|---|
| Schema na Bronze | `ibge` |
| Formato | CSV com cabeçalho, separador `,`, UTF-8 |
| Arquivos | uma lista completa por ano, todo dia 2 de janeiro: `ibge/municipios/2024-01-02/municipios_20240102_0001.csv` e `2025-01-02/...` na carga inicial, `2026-01-02/...` no `avancar_tempo` |
| Entidade | `municipios` |
| Volume | 5.570 municípios nas listas de 2024 e 2025 e 5.571 a partir da de 2026, com 27 UFs e 5 regiões, independentemente da escala |
| Artigos | 01, 08 e 10 |

O arquivo vem versionado no pacote do gerador (`src/modelbricks_gerador/dados/ibge_municipios.csv`), extraído da [API de Localidades do IBGE](https://servicodados.ibge.gov.br/api/docs/localidades) pelo script `scripts/extrair_ibge.py`. O setup não depende de rede.

## Entregas

O grupo baixa a lista de municípios do IBGE uma vez por ano, no dia 2 de janeiro, e a landing recebe a lista **inteira** a cada vez, não só o que mudou. A carga inicial traz as listas de 2024 e 2025; o `avancar_tempo` traz a de 2026 quando o relógio passa por 02/01/2026 (e uma por ano depois disso).

As listas não são idênticas. Boa Esperança do Norte (MT, código `5101837`) foi instalado em 1º de janeiro de 2025 e só aparece a partir da lista de 2026, então as de 2024 e 2025 têm 5.570 municípios e a de 2026 tem 5.571. Nenhum sistema do grupo atua em MT (`mundo.ufs_atuacao` no roteiro), então a diferença não deixa endereço sem município. O gabarito (`silver_dev.gabarito.municipios`) tem a lista completa, com os 5.571.

É essa sequência que o artigo 01 usa para mostrar o espelho: a tabela `bronze_dev.ibge.municipios` é substituída pelo conteúdo do arquivo mais recente a cada carga, e as listas anteriores continuam na landing.

## Diagrama ER

O arquivo é uma única tabela desnormalizada. O primeiro diagrama mostra a tabela como ela chega. O segundo mostra a hierarquia que está embutida nela e que pode ser normalizada na Silver, por exemplo num Snowflake.

```mermaid
erDiagram
    municipios {
        int cod_municipio PK "7 dígitos"
        string nome_municipio
        int cod_uf
        string sigla_uf
        string nome_uf
        int cod_regiao
        string sigla_regiao
        string nome_regiao
        int cod_regiao_imediata
        string nome_regiao_imediata
        int cod_regiao_intermediaria
        string nome_regiao_intermediaria
    }
```

```mermaid
erDiagram
    regiao ||--|{ uf : "contém"
    uf ||--|{ regiao_intermediaria : "contém"
    regiao_intermediaria ||--|{ regiao_imediata : "contém"
    regiao_imediata ||--|{ municipio : "contém"

    regiao {
        int cod_regiao PK
        string sigla_regiao
        string nome_regiao
    }
    uf {
        int cod_uf PK
        string sigla_uf UK
        string nome_uf
        int cod_regiao FK
    }
    regiao_intermediaria {
        int cod_regiao_intermediaria PK
        string nome_regiao_intermediaria
        int cod_uf FK
    }
    regiao_imediata {
        int cod_regiao_imediata PK
        string nome_regiao_imediata
        int cod_regiao_intermediaria FK
    }
    municipio {
        int cod_municipio PK
        string nome_municipio
        int cod_regiao_imediata FK
    }
```

## Dicionário de dados

| Coluna | Tipo | Descrição |
|---|---|---|
| `cod_municipio` | int | Código do município no IBGE, 7 dígitos (o último é verificador). |
| `nome_municipio` | string | Nome do município, com acentos. |
| `cod_uf` | int | Código da UF, 2 dígitos. São os dois primeiros dígitos do `cod_municipio`. |
| `sigla_uf` | string | Sigla da UF (`ES`, `SP`...). |
| `nome_uf` | string | Nome da UF. |
| `cod_regiao` | int | Código da grande região, de 1 a 5. |
| `sigla_regiao` | string | `N`, `NE`, `SE`, `S` ou `CO`. |
| `nome_regiao` | string | Norte, Nordeste, Sudeste, Sul ou Centro-Oeste. |
| `cod_regiao_imediata` | int | Região geográfica imediata (6 dígitos), 510 no país. |
| `nome_regiao_imediata` | string | Nome da região imediata. |
| `cod_regiao_intermediaria` | int | Região geográfica intermediária (4 dígitos), 133 no país. |
| `nome_regiao_intermediaria` | string | Nome da região intermediária. |

## Uso nas outras bases

- O grupo atua em 10 UFs: ES, MG, RJ, SP, BA, GO, DF, PR, SC e RS (3.523 municípios). Rotas, bases, lojas, pessoas e empresas do mundo verdadeiro usam municípios dessas UFs. O arquivo traz o país inteiro.
- O defeito "município fora do padrão" (no *Pátio* e no *SolTicket*) produz códigos com 6 ou 8 dígitos, que não casam com este arquivo.
- O mundo verdadeiro também tem a tabela `municipios`, com o mesmo conteúdo, no `gabarito`.
