"""Gerações compartilhadas dos testes: o roteiro de teste único, as fatias de teste e um
executor com cache local.

Cada chamada de `gerar_fatia`/`avancar_tempo` recalcula o mundo verdadeiro e os sistemas da
fatia no Spark remoto e consome a cota diária do workspace. Por isso os testes não geram
nada por conta própria: pedem uma das gerações declaradas aqui, por fixture de sessão
(`conftest.py`), e cada geração roda no máximo uma vez por sessão de testes.

- **Roteiro de teste único.** Uma cópia do roteiro oficial com os ajustes que os testes de
  cada sistema precisam (taxas de defeito mais altas para medir com folga, página pequena no
  *PassagemHub* etc.). Cada ajuste só mexe no próprio sistema, então os sistemas podem ser
  gerados juntos na mesma fatia. A variante `embarque_sujo` também dá ao *Embarque* taxas de
  todos os defeitos de linha (`test_defeitos.py`); os testes do *Embarque* (`test_embarque.py`)
  precisam do *Embarque* com as taxas oficiais, por isso são duas variantes.
- **Fatias de teste.** Declaradas no roteiro de teste com números 90 a 98 (fora das fatias
  dos artigos).
- **Cache local.** O resultado de cada geração (uma cópia do destino depois de cada passo e
  os `Resultado`s) fica em `00-gerador/.cache-testes/`, sob uma chave que é o hash do código
  do gerador, do roteiro usado e da própria geração. Mudou o código ou o roteiro, a chave
  muda e a geração roda de novo. Para ignorar o cache, use `pytest --sem-cache`.
"""

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from importlib import resources
from pathlib import Path

import pyarrow.parquet as pq
import yaml

from modelbricks_gerador import DestinoLocal, Resultado, avancar_tempo, carregar_roteiro, gerar_fatia

RAIZ_GERADOR = Path(__file__).resolve().parents[1]
CACHE = RAIZ_GERADOR / ".cache-testes"
CARGA_INICIAL = date(2025, 12, 31)
# Com várias gerações em paralelo na mesma sessão, o servidor Connect às vezes invalida o cursor
# de um resultado grande (INVALID_CURSOR.POSITION_NOT_AVAILABLE); a geração é repetida uma vez.
TENTATIVAS = 2

# Ajustes do roteiro de teste, por sistema (cada um só afeta o próprio sistema).
TAXAS_SOLTICKET = {"pessoa_duplicada_no_sistema": 0.10, "fk_orfa": 0.05, "municipio_fora_do_padrao": 0.10}
TAXA_MUNICIPIO_PATIO = 0.10
TAXA_ATRASO_NUCLEO = 0.05
EVENTOS_POR_PAGINA_PASSAGEMHUB = 5
# Variante `embarque_sujo`: todos os defeitos de linha no Embarque.
TAXAS_EMBARQUE = {
    "cpf_fora_do_padrao": 0.10,
    "nome_abreviado_ou_com_erro": 0.10,
    "pessoa_duplicada_no_sistema": 0.05,
    "contato_nulo": 0.15,
    "evento_atrasado": 0.02,
    "cdc_duplicado": 0.01,
    "fk_orfa": 0.02,
}

# Sistemas gerados juntos na geração principal (as projeções são independentes entre si).
SISTEMAS = ["embarque", "nucleo", "rotacerta", "apolice", "passagemhub", "rastro"]
FATIAS_DE_TESTE = {
    "90-sistemas": {"sistemas": SISTEMAS, "janela_telemetria_dias": 90},
    "91-legados": {"sistemas": ["embarque", "solticket", "solfin"]},
    "92-patio": {"sistemas": ["patio"]},
    "93-mundo": {"sistemas": ["ibge"]},
    # O Rastro e o IBGE dependem da data da carga inicial (janela de telemetria e data do
    # arquivo de municípios), então não entram na comparação da geração em partes.
    "94-partes": {"sistemas": ["embarque", "nucleo", "rotacerta", "apolice", "passagemhub"]},
    "98-legados": {"sistemas": ["solticket", "solfin"]},
}


def dados_do_roteiro(variante: str) -> dict:
    """Roteiro de teste como dicionário: `teste` ou `embarque_sujo`."""
    texto = resources.files("modelbricks_gerador").joinpath("roteiro.yaml").read_text(encoding="utf-8")
    dados = yaml.safe_load(texto)
    defeitos, parametros = dados["defeitos"], dados["parametros"]
    for tipo, taxa in TAXAS_SOLTICKET.items():  # test_legados: defeitos raros com folga estatística
        defeitos[tipo]["taxas"]["solticket"] = taxa
    defeitos["municipio_fora_do_padrao"]["taxas"]["patio"] = TAXA_MUNICIPIO_PATIO  # test_patio
    defeitos["evento_atrasado"]["taxas"]["nucleo"] = TAXA_ATRASO_NUCLEO  # test_nucleo: motor incremental
    parametros["apolice"]["seguro_na_venda"] = 1.0  # test_apolice: seguros vendidos com veículos
    parametros["passagemhub"]["eventos_por_pagina"] = EVENTOS_POR_PAGINA_PASSAGEMHUB  # várias páginas por dia
    if variante == "embarque_sujo":
        for tipo, taxa in TAXAS_EMBARQUE.items():
            defeitos[tipo]["taxas"]["embarque"] = taxa
    elif variante != "teste":
        raise ValueError(f"variante de roteiro desconhecida: {variante}")
    for artigo, fatia in FATIAS_DE_TESTE.items():
        numero, tema = artigo.split("-", 1)
        dados["fatias"][artigo] = {
            "catalogo": f"medalhao_{numero}_{tema.replace('-', '_')}", "gabarito": True, **fatia,
        }
    return dados


def texto_do_roteiro(variante: str) -> str:
    return yaml.safe_dump(dados_do_roteiro(variante), allow_unicode=True, sort_keys=True)


@dataclass(frozen=True)
class Geracao:
    """Uma sequência de chamadas da API num destino novo.

    `passos`: `("gerar", data_ate)` e `("avancar", dias)`, na ordem. O destino é copiado
    depois de cada passo (`Gerada.etapas`), para os testes compararem o antes e o depois.
    """

    nome: str
    artigo: str
    escala: float
    roteiro: str  # variante do roteiro de teste: "teste" ou "embarque_sujo"
    passos: tuple[tuple[str, object], ...]


# Gerações da execução padrão.
PRINCIPAL = Geracao(  # carga inicial e avanço até o reajuste de tarifas (12/01/2026)
    "principal", "90-sistemas", 0.05, "teste", (("gerar", CARGA_INICIAL), ("avancar", 12)),
)
LEGADOS = Geracao(  # até a segunda-feira 05/01/2026, com o Embarque sujo (test_defeitos)
    "legados", "91-legados", 0.1, "embarque_sujo", (("gerar", date(2026, 1, 5)),),
)
PATIO = Geracao("patio", "92-patio", 0.2, "teste", (("gerar", date(2026, 1, 3)),))

# Gerações dos testes marcados como `lento`.
PRINCIPAL_EM_PARTES = Geracao(  # mesma história da PRINCIPAL, cortada em outros pontos
    "principal_em_partes", "94-partes", 0.05, "teste",
    (("gerar", date(2026, 1, 9)), ("avancar", 1), ("avancar", 2)),
)
LEGADOS_EM_PARTES = Geracao(  # véspera da aquisição e uma semana de avanço
    "legados_em_partes", "98-legados", 0.1, "embarque_sujo", (("gerar", date(2025, 6, 30)), ("avancar", 7)),
)
PATIO_EM_PARTES = Geracao(  # o avanço atravessa o fim do período diário fechado (31/12/2025)
    "patio_em_partes", "92-patio", 0.2, "teste", (("gerar", date(2025, 12, 30)), ("avancar", 4)),
)
MUNDO = Geracao("mundo", "93-mundo", 0.05, "teste", (("gerar", CARGA_INICIAL),))
# Fatias inteiras dos artigos (as fatias oficiais do roteiro).
ESCALA_FATIA_INTEIRA = 0.03
# Escala mínima do projeto (test_escala_minima): a Viação tem de ter rota e, portanto, passagens.
ESCALA_MINIMA = 0.01
FATIA_01_MINIMA = Geracao(
    "fatia_01_minima", "01-bronze", ESCALA_MINIMA, "teste", (("gerar", CARGA_INICIAL),),
)
FATIA_01 = Geracao(  # carga inicial e avanço de 1 dia (test_gerar_fatia)
    "fatia_01", "01-bronze", ESCALA_FATIA_INTEIRA, "teste", (("gerar", CARGA_INICIAL), ("avancar", 1)),
)
FATIA_02 = Geracao("fatia_02", "02-historizacao", ESCALA_FATIA_INTEIRA, "teste", (("gerar", CARGA_INICIAL),))
FATIA_03 = Geracao("fatia_03", "03-3fn", ESCALA_FATIA_INTEIRA, "teste", (("gerar", CARGA_INICIAL),))
FATIA_05 = Geracao("fatia_05", "05-raw-vault", ESCALA_FATIA_INTEIRA, "teste", (("gerar", CARGA_INICIAL),))
FATIA_10 = Geracao("fatia_10", "10-data-marts", ESCALA_FATIA_INTEIRA, "teste", (("gerar", CARGA_INICIAL),))


@dataclass(frozen=True)
class Gerada:
    """O que uma geração deixou: o destino depois de cada passo e os resultados da API.

    Os destinos são só para leitura (podem estar no cache): copie antes de chamar a API.
    """

    roteiro: Path
    etapas: tuple[DestinoLocal, ...]
    resultados: tuple[Resultado, ...]

    @property
    def final(self) -> DestinoLocal:
        return self.etapas[-1]


def obter(geracao: Geracao, pasta_temporaria: Path, obter_spark, usar_cache: bool = True) -> Gerada:
    """Devolve a geração do cache ou a executa (e a guarda no cache, se `usar_cache`).

    `obter_spark` só é chamado quando a geração precisa rodar: com o cache cheio, os testes
    rodam sem abrir sessão no workspace.
    """
    roteiro = texto_do_roteiro(geracao.roteiro)
    pasta = CACHE / f"{geracao.nome}-{_chave(geracao, roteiro)[:16]}"
    if usar_cache and (pasta / "resultados.json").exists():
        return _ler(pasta, geracao)

    # Com cache, o trabalho fica dentro do cache para o `os.replace` final não cruzar discos.
    base = CACHE if usar_cache else pasta_temporaria
    for tentativa in range(TENTATIVAS):
        trabalho = base / f".em-andamento-{geracao.nome}-{uuid.uuid4().hex[:8]}"
        trabalho.mkdir(parents=True)
        (trabalho / "roteiro.yaml").write_text(roteiro, encoding="utf-8")
        try:
            _executar(geracao, trabalho, obter_spark())
            break
        except BaseException as erro:
            shutil.rmtree(trabalho, ignore_errors=True)
            if tentativa + 1 == TENTATIVAS or "INVALID_CURSOR" not in str(erro):
                raise
    if not usar_cache:
        return _ler(trabalho, geracao)
    shutil.rmtree(pasta, ignore_errors=True)
    try:
        os.replace(trabalho, pasta)
    except OSError:  # outra sessão gravou a mesma chave ao mesmo tempo: as duas são iguais
        return _ler(trabalho, geracao)
    return _ler(pasta, geracao)


def _executar(geracao: Geracao, trabalho: Path, spark) -> None:
    caminho_roteiro = trabalho / "roteiro.yaml"
    catalogo = carregar_roteiro(caminho_roteiro).fatia(geracao.artigo).catalogo
    destino = DestinoLocal(trabalho / "destino" / catalogo)
    resultados = []
    for indice, (passo, valor) in enumerate(geracao.passos):
        if passo == "gerar":
            r = gerar_fatia(geracao.artigo, destino, escala=geracao.escala, ate=valor, spark=spark,
                            roteiro=caminho_roteiro)
        else:
            r = avancar_tempo(geracao.artigo, destino, valor, spark=spark, roteiro=caminho_roteiro)
        resultados.append({**asdict(r), "data_relogio": r.data_relogio.isoformat()})
        shutil.copytree(destino.raiz, trabalho / f"etapa-{indice}" / catalogo)
    shutil.rmtree(trabalho / "destino")
    # Gravado por último: a presença dele marca a geração como completa no cache.
    (trabalho / "resultados.json").write_text(json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8")


def _ler(pasta: Path, geracao: Geracao) -> Gerada:
    roteiro = pasta / "roteiro.yaml"
    catalogo = carregar_roteiro(roteiro).fatia(geracao.artigo).catalogo
    resultados = json.loads((pasta / "resultados.json").read_text(encoding="utf-8"))
    return Gerada(
        roteiro=roteiro,
        etapas=tuple(DestinoLocal(pasta / f"etapa-{i}" / catalogo) for i in range(len(geracao.passos))),
        resultados=tuple(Resultado(**{**r, "data_relogio": date.fromisoformat(r["data_relogio"])}) for r in resultados),
    )


def _chave(geracao: Geracao, roteiro: str) -> str:
    """Hash do código do gerador (e das dependências travadas), do roteiro e da geração."""
    h = hashlib.sha256()
    pacote = RAIZ_GERADOR / "src" / "modelbricks_gerador"
    arquivos = sorted(p for p in pacote.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    for p in [*arquivos, RAIZ_GERADOR / "pyproject.toml", RAIZ_GERADOR / "uv.lock"]:
        if p.exists():
            h.update(p.relative_to(RAIZ_GERADOR).as_posix().encode())
            h.update(p.read_bytes())
    h.update(roteiro.encode())
    h.update(repr(geracao).encode())
    return h.hexdigest()


def roteiro_so_ibge():
    """O roteiro oficial com a fatia 01-bronze reduzida ao IBGE: para testar onde um destino
    grava (Unity Catalog), sem pagar a geração dos sistemas."""
    roteiro = carregar_roteiro()
    fatia = roteiro.fatias["01-bronze"].model_copy(update={"sistemas": ["ibge"]})
    return roteiro.model_copy(update={"fatias": {**roteiro.fatias, "01-bronze": fatia}})


# Leitura do que foi gravado (comum aos módulos de teste).


def arquivos(destino: DestinoLocal, sistema: str) -> dict[str, bytes]:
    """Arquivos da landing de um sistema: caminho relativo à landing → conteúdo."""
    pasta = destino.volume / sistema
    return {p.relative_to(destino.volume).as_posix(): p.read_bytes() for p in sorted(pasta.rglob("*")) if p.is_file()}


def gabarito(destino: DestinoLocal, sistema: str | None = None) -> dict[str, list[dict]]:
    """Tabelas do gabarito (todas as partes de cada uma). Com `sistema`, `mapa_chaves` e
    `defeitos` ficam só com as linhas dele (a fatia de teste tem vários sistemas)."""
    pasta = destino.raiz / "gabarito"
    tabelas = {
        p.name: [r for parte in sorted(p.glob("*.parquet")) for r in pq.read_table(parte).to_pylist()]
        for p in sorted(pasta.iterdir()) if p.is_dir()
    }
    if sistema is not None:
        for nome in ("mapa_chaves", "defeitos"):
            tabelas[nome] = [r for r in tabelas.get(nome, []) if r["sistema"] == sistema]
    return tabelas
