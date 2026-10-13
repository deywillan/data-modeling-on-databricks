# 00 — Gerador de dados do Grupo ModelBricks

Este pacote gera os dados sintéticos usados em todos os artigos da série. O Grupo ModelBricks é uma holding **fictícia**, com empresas de transporte de passageiros (ModelBricks Viação), de cargas (ModelBricks Cargas), uma concessionária de ônibus e caminhões (ModelBricks Veículos) e uma corretora de seguros (ModelBricks Seguros). Cada empresa tem seus próprios sistemas, e cada sistema entrega arquivos com formato, chaves e defeitos próprios, como acontece numa empresa de verdade.

Você não precisa rodar nada desta pasta diretamente. O job `<NN>_setup` de cada artigo chama o gerador informando a sua **fatia**, e o job `avancar_tempo` gera os dias seguintes. Os dois são instalados pelo `databricks bundle deploy` na raiz do repositório; o README de cada artigo explica como rodá-los. Este documento explica **de onde vêm os dados** que você vai encontrar na landing.

## Como os dados nascem

```
roteiro.yaml ──► mundo verdadeiro ──► projeção de cada sistema ──► arquivos na landing
                 (pessoas, empresas,   (chave, formato, padrão       (Volumes do catálogo
                  veículos, rotas...)   de entrega e defeitos)        Bronze do artigo)
```

1. **Mundo verdadeiro.** Primeiro o gerador cria, com o [dbldatagen](https://github.com/databrickslabs/dbldatagen) e o Faker em `pt_BR`, as entidades "reais" do grupo: pessoas, empresas clientes, veículos, rotas, bases, lojas, colaboradores e municípios do IBGE. Também decide quem é cliente de quem: uma parte das pessoas é cliente de mais de uma empresa do grupo, e os ônibus e caminhões que a concessionária vende à própria holding passam a fazer parte da frota.
2. **Projeção por sistema.** Cada sistema de origem enxerga só a parte do mundo que lhe interessa e a registra do seu jeito: com chave própria, grafia própria (o mesmo cliente pode aparecer em maiúsculas num sistema e com o CPF sem máscara em outro), o seu padrão de entrega e os seus defeitos.
3. **Landing.** O resultado é gravado **só como arquivos** no Volume `landing` do schema de cada sistema, no catálogo Bronze do artigo. O gerador nunca cria tabelas nos catálogos Bronze, Silver e Gold (a única exceção é o `gabarito`, no Silver, nas fatias que o têm): quem os popula é o pipeline do artigo, lendo a landing como numa ingestão real.

O mundo verdadeiro é sempre gerado inteiro, em qualquer artigo. Por isso, o mesmo cliente tem os mesmos dados em todos os artigos; muda só quais sistemas vão para a landing e, nos artigos sem gabarito, a ausência dos defeitos (veja o catálogo de defeitos, abaixo).

## Sistemas de origem

| Sistema | Empresa | Entrega | Formato | Entidades (pastas do Volume `landing`) |
|---|---|---|---|---|
| *Embarque* | ModelBricks Viação (guichê e site) | CDC: uma linha por operação `I`/`U`/`D`, com `seq` e `ts_operacao` | JSON lines | `embarque/clientes`, `tarifas`, `passagens` |
| *PassagemHub* | marketplace parceiro da Viação | API paginada: eventos de venda, cancelamento e remarcação | JSON aninhado (uma página por arquivo) | `passagemhub/eventos_venda` |
| *Pátio* | ModelBricks Veículos (vendas e oficina) | snapshot completo: a foto inteira de cada tabela na data | CSV | `patio/lojas`, `clientes`, `veiculos`, `vendas`, `ordens_servico` |
| *Rastro* | telemetria da frota própria | stream simulado: muitos arquivos pequenos por dia | JSON lines aninhado | `rastro/eventos` |
| *Núcleo* | ERP corporativo da holding | incremental por `updated_at`, com exclusão lógica | Parquet | `nucleo/colaboradores`, `fornecedores`, `titulos_pagar`, `notas_fiscais`, `ativos` |
| *RotaCerta* | TMS da ModelBricks Cargas | CDC de fretes, como o *Embarque* | JSON lines | `rotacerta/clientes`, `caminhoes`, `fretes` |
| *Apólice+* | ModelBricks Seguros | incremental por `updated_at` | CSV | `apolice/segurados`, `apolices` |
| *SolTicket*, *SolFin* | sistemas legados do Expresso Vale do Sol, empresa adquirida pelo grupo | carga histórica única na data da aquisição e, depois, snapshots periódicos | CSV com `;` em `latin1` | `solticket/`, `solfin/` |
| IBGE | dado público de referência | arquivo único na carga inicial | CSV | `ibge/municipios` |

A pasta [`docs/`](docs/README.md) documenta cada base: diagrama ER em Mermaid, dicionário de dados, ciclo de vida dos registros e defeitos. O `gabarito` também está lá. A docstring de cada módulo em [`src/modelbricks_gerador/sistemas/`](src/modelbricks_gerador/sistemas/) traz as regras de geração.

### Organização dos catálogos

Cada artigo tem três catálogos, um por camada, com o ambiente do target do bundle no fim do nome (`dev` gera `_dev`, `prod` gera `_prd`). O ambiente é a variável `ambiente` do `databricks.yml`.

```
a03_bronze_dev                                  Bronze do artigo 03 no target dev
├── <sistema>                                   um schema por sistema da fatia (o IBGE também conta)
│   └── landing (Volume)                        /Volumes/a03_bronze_dev/<sistema>/landing/
│       └── <entidade>/<AAAA-MM-DD>/<entidade>_<AAAAMMDD>_<NNNN>.<ext>
└── _controle
    └── relogio (Volume)                        /Volumes/a03_bronze_dev/_controle/relogio/<AAAA-MM-DD>.json
a03_silver_dev                                  nasce vazio; só tem o schema gabarito nas fatias que o têm
a03_gold_dev                                    nasce vazio
```

- A data na pasta é a **data simulada** em que o lote chegou. Assim você vê o que chegou em cada dia.
- Nenhum arquivo é sobrescrito: cada lote é um arquivo novo, e o Auto Loader trata cada um como novo.
- O schema `_controle` guarda o relógio (abaixo) e não é dado de sistema. Os pipelines leem só os schemas dos sistemas.
- O nome dos catálogos sai de uma função só, `catalogos.nome_catalogo(artigo, camada, ambiente)`, que valida os três.

## Fatias

Cada artigo usa só os sistemas de que precisa. Essa escolha é a **fatia** do artigo, declarada na seção `fatias:` do [`roteiro.yaml`](src/modelbricks_gerador/roteiro.yaml):

```yaml
fatias:
  01-bronze:
    sistemas: [embarque, patio, passagemhub, rastro, ibge]
    gabarito: false
    janela_telemetria_dias: 90
```

- `sistemas`: os sistemas que vão para a landing. Os demais não aparecem.
- `gabarito`: se o catálogo Silver do artigo recebe o schema `gabarito` (veja abaixo). Os catálogos do artigo não saem do roteiro, e sim do número do artigo e do ambiente; cada artigo tem os seus, então rodar um não exige rodar outro.
- `janela_telemetria_dias`: quantos dias de telemetria do *Rastro* entram na carga inicial. A telemetria é o maior volume do grupo, então a carga inicial traz só os últimos dias, seja qual for a escala.

## Relógio simulado

A história do grupo começa em `datas.inicio` e a carga inicial vai até `datas.carga_inicial` (as duas no roteiro). O `<NN>_setup` grava todos os lotes desse período de uma vez e acerta o relógio do artigo na data da carga inicial.

Depois disso, o job `avancar_tempo` gera os lotes dos dias seguintes ao relógio, só para os sistemas da fatia, e avança o relógio. O ciclo do leitor é **avançar o tempo → rodar o pipeline → observar o que mudou**.

- O relógio fica no Volume `relogio` do schema `_controle` do catálogo Bronze: cada avanço grava um arquivo novo, e o relógio atual é o de maior data.
- O avanço só cria arquivos novos; nenhum arquivo já entregue muda.
- Os dados são **determinísticos**: a mesma escala, a mesma versão do roteiro e a mesma data produzem sempre os mesmos bytes. Avançar 3 dias de uma vez produz o mesmo que avançar 1 dia três vezes, e os seus números batem com os do artigo.
- Rodar o `<NN>_setup` de novo, sem `reset`, não altera nada e só informa a data do relógio. Com `reset=true`, os três catálogos do artigo são apagados e recriados do zero.
- Se o roteiro mudou de versão (ou a escala é outra) desde a carga inicial do catálogo, o `<NN>_setup` sem `reset` e o `avancar_tempo` param com um erro que manda recomeçar com `reset=true`: dados de gerações diferentes não se misturam na mesma landing.
- Se um job cair no meio (rede, cota, cancelamento), basta rodá-lo de novo: o relógio só avança no fim, um arquivo que já está na landing com os mesmos bytes conta como entregue (bytes diferentes continuam sendo erro) e o gabarito do intervalo não duplica.

## Roteiro

O [`roteiro.yaml`](src/modelbricks_gerador/roteiro.yaml) é a **única fonte de números** do gerador. O código não tem volumes, taxas ou datas espalhados; quando este README ou um artigo citar um número, ele vem de lá. O roteiro contém:

| Seção | O quê |
|---|---|
| `seed_base` | semente de todo o gerador; as sementes de cada tabela, sistema e dia derivam dela |
| `datas` | início da história e data da carga inicial |
| `volumes` | quantidade de cada entidade na escala 1 |
| `parametros` | comportamento de cada sistema (canais de venda, horários, taxas de cancelamento, granularidade dos arquivos...) |
| `mundo` e `catalogos` | como o mundo verdadeiro é gerado e quem é cliente de qual empresa |
| `sistemas` | entrega e formato de cada sistema |
| `eventos` | acontecimentos com data marcada (abaixo) |
| `defeitos` | o catálogo de defeitos (abaixo) |
| `fatias` | os sistemas de cada artigo |

Toda mudança no roteiro muda os dados gerados e, por isso, incrementa o campo `versao`, que fica registrado no relógio de cada catálogo.

### Escala

O parâmetro `escala` do `<NN>_setup` multiplica linearmente os volumes do roteiro: `1` é o tamanho de referência, e valores como `0.1` servem para um teste rápido. A escala muda os dados, então compare os seus números com os do artigo usando a mesma escala.

Nas populações distribuídas entre empresas (rotas, bases, lojas e colaboradores), o roteiro garante ao menos uma instância por empresa (`mundo.minimo_por_empresa`), de modo que mesmo a escala `0.01` entrega todas as divisões com dados (a Viação, por exemplo, sempre tem rota e passagens).

Na escala 1, no Free Edition, o `<NN>_setup` leva de poucos minutos (cerca de 4 minutos para o artigo 01 e para o 05) a cerca de 6 minutos (artigo 10, com todos os sistemas e cerca de 2 GB na landing). O job imprime, por sistema, os arquivos, o tamanho e o tempo de geração e de gravação. Para conferir o que o setup deixou no catálogo, rode, a partir de `00-gerador/`, `DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run python scripts/conferir_catalogo.py <artigo> <ambiente>` (por exemplo, `01-bronze dev`): ele resume os catálogos, os schemas por sistema e a landing de cada sistema (`--arquivos` lista cada arquivo; `--relogio AAAA-MM-DD` confere a data do relógio simulado). Para exercitar o ciclo inteiro de um artigo num ambiente descartável (setup, setup repetido, `avancar_tempo`, `reset` e limpeza), há o `scripts/teste_fumaca.sh <seu-perfil> [artigo] [escala]`, que usa o ambiente `fumaca` (ou o de `AMBIENTE=...`) e nunca toca em `dev` nem em `prd`.

### Eventos com data

Alguns acontecimentos da história do grupo têm data marcada na seção `eventos:` do roteiro, cada um com os sistemas que afeta e uma descrição:

- a **aquisição do Expresso Vale do Sol**, cujos sistemas legados entregam toda a sua história de uma vez na data da aquisição;
- uma **coluna nova** no payload da API do *PassagemHub* (*schema evolution*): antes da data, nenhum evento traz o campo; a partir dela, todos trazem;
- um **reajuste de tarifas em massa** no *Embarque*, que atualiza todas as tarifas no mesmo dia.

Os dois últimos ficam de propósito **depois** da carga inicial: você os vê acontecer ao avançar o relógio com o `avancar_tempo`, em vez de recebê-los já embutidos nos dados.

## Catálogo de defeitos

Dados de empresas reais têm defeitos, e os artigos precisam deles para mostrar onde cada um é tratado. O gerador injeta defeitos conhecidos nos arquivos dos sistemas, nunca no mundo verdadeiro. A seção `defeitos:` do roteiro define, para cada tipo, os sistemas afetados e a taxa em cada um:

| Defeito | Exemplo |
|---|---|
| `cpf_fora_do_padrao` | CPF sem máscara, com máscara ou com zeros à esquerda perdidos |
| `nome_abreviado_ou_com_erro` | "Mª" em vez de "Maria", "Jose" em vez de "José" |
| `pessoa_duplicada_no_sistema` | a mesma pessoa cadastrada duas vezes no mesmo sistema |
| `contato_nulo` | e-mail ou telefone nulo |
| `evento_atrasado` | o evento chega num lote de data posterior à dele (*late-arriving*) |
| `cdc_duplicado` | a mesma operação de CDC (mesma `seq`) entregue de novo num lote posterior |
| `fk_orfa` | chave estrangeira que aponta para um registro inexistente |
| `municipio_fora_do_padrao` | código de município fora do padrão do IBGE |
| `encoding_latin1` | arquivo inteiro em `latin1`, com acentos quebrados se lido como UTF-8 |
| `valor_monetario_texto` | valor monetário como texto com vírgula (`"1.234,56"`) |

A regra da série é que a Bronze **preserva** esses defeitos (no máximo os marca) e a correção acontece na Silver. Por isso eles estão nos arquivos desde a carga inicial.

Os defeitos só são injetados nas fatias **com gabarito** (`gabarito: true` no roteiro), porque só nelas há a tabela `gabarito.defeitos` para conferir a correção. Nas fatias sem gabarito os arquivos saem limpos: o mesmo cliente aparece, na fatia sem gabarito, com o valor original, e, na fatia com gabarito, com o defeito registrado. A regra é a própria flag `gabarito` da fatia, aplicada em um só lugar (`Contexto.defeitos`).

## Gabarito

Algumas fatias recebem o schema `gabarito` no catálogo Silver do artigo (`a03_silver_dev.gabarito`), com tabelas Delta que mostram a "verdade" por trás da landing:

- as entidades do mundo verdadeiro (`pessoas`, `empresas_pj`, `veiculos`, `participacoes`...);
- `mapa_chaves`: a chave de cada entidade verdadeira em cada sistema, inclusive as chaves das cópias duplicadas de pessoas (o defeito `pessoa_duplicada_no_sistema` as identifica);
- `defeitos`: cada defeito injetado, com sistema, entidade, chave no sistema, valor original, valor sujo e o arquivo em que chegou.

O gabarito serve para conferir uma solução, por exemplo, quantos clientes duplicados a sua integração encontrou. Ele nunca é gravado na landing (fica no Silver, fora dos Volumes dos sistemas), para que não seja ingerido por engano, e os pipelines não o leem.

## Para quem quer mexer no gerador

O pacote é Python 3.12 e usa o [`uv`](https://docs.astral.sh/uv/). O `databricks bundle deploy` o constrói como wheel, instalado no ambiente serverless dos jobs.

| Módulo | Papel |
|---|---|
| `api.py` | a API pública: `gerar_fatia` e `avancar_tempo` |
| `cli.py` | os pontos de entrada dos jobs |
| `roteiro.py` e `roteiro.yaml` | o roteiro e a sua validação |
| `mundo.py` | o mundo verdadeiro |
| `sistemas/` | uma projeção por sistema de origem |
| `cdc.py`, `incremental.py`, `snapshot.py` | os padrões de entrega |
| `defeitos.py` | o catálogo de defeitos |
| `escritor.py` | a serialização dos arquivos (JSON, CSV, Parquet) |
| `catalogos.py` | o nome dos catálogos (artigo + camada + ambiente), validado |
| `destino.py`, `destino_uc.py` | onde gravar: diretório local (testes) ou Unity Catalog (jobs) |
| `relogio.py` | o relógio simulado |

Os testes chamam a API pública com um destino em diretório local e inspecionam os arquivos gravados. O Spark roda no seu workspace via Databricks Connect serverless, então os testes consomem a cota do workspace. Para gastar pouco:

- os testes não geram dados por conta própria: pedem uma das gerações compartilhadas de `tests/geracoes.py` (fixtures de sessão em `tests/conftest.py`), e cada geração roda no máximo uma vez por execução do pytest;
- o resultado de cada geração fica em `00-gerador/.cache-testes/` (fora do Git), sob o hash do código do gerador e do roteiro. Enquanto o código não muda, uma nova execução não abre sessão no workspace. `--sem-cache` gera tudo de novo;
- logo depois da coleta, as gerações que os testes escolhidos pedem rodam em paralelo (até três por vez, na mesma sessão), em vez de uma depois da outra;
- a execução padrão deixa de fora os testes marcados como `lento` (gerações extras de determinismo e de geração em partes, fatias inteiras dos artigos e o Unity Catalog). Com o cache vazio, a execução padrão leva cerca de 9 minutos e a suíte completa, cerca de 28; com o cache cheio, a execução padrão leva menos de 30 segundos.

```bash
cd 00-gerador
# execução padrão (sem os testes lentos)
DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run pytest
# só um módulo
DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run pytest tests/test_embarque.py
# suíte completa, sem cache
DATABRICKS_CONFIG_PROFILE=<seu-perfil> uv run pytest -m "lento or not lento" --sem-cache
```

Em alguns ambientes, a combinação de `pyarrow` e OpenSSL exige `OPENSSL_CONF=/dev/null` antes do comando.
