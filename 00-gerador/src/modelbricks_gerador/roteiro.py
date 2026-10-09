"""Roteiro: carregador validado do `roteiro.yaml`, a única fonte de números do gerador.

Interface: `carregar_roteiro(caminho=None) -> Roteiro` e `Roteiro.fatia(artigo) -> Fatia`.
Roteiro inválido levanta `RoteiroInvalido` com a lista dos campos problemáticos.
"""

import re
from datetime import date
from importlib import resources
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

ROTEIRO_PADRAO = "roteiro.yaml"
_ARTIGO = re.compile(r"^(\d{2})-([a-z0-9-]+)$")


class RoteiroInvalido(ValueError):
    """O roteiro não pôde ser lido ou não passou na validação."""


class _Modelo(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Datas(_Modelo):
    inicio: date
    carga_inicial: date


class Volumes(_Modelo):
    pessoas: int = Field(gt=0)
    empresas_pj: int = Field(gt=0)
    veiculos: int = Field(gt=0)
    veiculos_frota_propria: int = Field(gt=0)
    passagens_embarque_passagemhub: int = Field(gt=0)
    passagens_solticket: int = Field(gt=0)
    fretes_rotacerta: int = Field(gt=0)
    vendas_veiculos: int = Field(gt=0)
    apolices: int = Field(gt=0)


class Intervalo(_Modelo):
    minimo: int = Field(ge=0)
    maximo: int = Field(ge=0)


class Parametros(_Modelo):
    pessoas_em_mais_de_uma_empresa: float = Field(ge=0, le=1)
    eventos_por_frete: Intervalo
    telemetria_dias_carga_inicial: int = Field(gt=0)


class Sistema(_Modelo):
    nome: str
    empresa: str
    entrega: Literal["cdc", "incremental", "snapshot", "api", "stream", "historico_mais_snapshot", "referencia"]
    formato: Literal["json", "json_aninhado", "parquet", "csv", "csv_latin1"]


class Evento(_Modelo):
    data: date
    sistemas: list[str]
    descricao: str


class Defeito(_Modelo):
    descricao: str
    taxas: dict[str, float | Literal["arquivo", "coluna"]]
    artigos: list[int]


class Fatia(_Modelo):
    """O que o `00_setup` de um artigo grava: sistemas, gabarito e janela de telemetria."""

    artigo: str
    catalogo: str
    sistemas: list[str] = Field(min_length=1)
    gabarito: bool
    janela_telemetria_dias: int | None = Field(default=None, gt=0)


class Roteiro(_Modelo):
    versao: int
    seed_base: int
    datas: Datas
    fuso_horario: str
    volumes: Volumes
    parametros: Parametros
    sistemas: dict[str, Sistema]
    eventos: dict[str, Evento]
    defeitos: dict[str, Defeito]
    fatias: dict[str, Fatia]

    @model_validator(mode="before")
    @classmethod
    def _artigo_nas_fatias(cls, dados):
        if isinstance(dados, dict) and isinstance(dados.get("fatias"), dict):
            dados = dict(dados)
            dados["fatias"] = {
                artigo: {"artigo": artigo, **fatia} if isinstance(fatia, dict) else fatia
                for artigo, fatia in dados["fatias"].items()
            }
        return dados

    @model_validator(mode="after")
    def _coerencia(self):
        erros = []
        if self.datas.inicio >= self.datas.carga_inicial:
            erros.append("datas.inicio deve ser anterior a datas.carga_inicial")
        intervalo = self.parametros.eventos_por_frete
        if intervalo.minimo > intervalo.maximo:
            erros.append("parametros.eventos_por_frete.minimo é maior que o máximo")
        for nome, evento in self.eventos.items():
            erros += [f"eventos.{nome}: sistema desconhecido '{s}'" for s in evento.sistemas if s not in self.sistemas]
        for nome, defeito in self.defeitos.items():
            for sistema, taxa in defeito.taxas.items():
                if sistema not in self.sistemas:
                    erros.append(f"defeitos.{nome}: sistema desconhecido '{sistema}'")
                if isinstance(taxa, float) and not 0 <= taxa <= 1:
                    erros.append(f"defeitos.{nome}.taxas.{sistema}: taxa {taxa} fora de 0 a 1")
        for artigo, fatia in self.fatias.items():
            casamento = _ARTIGO.match(artigo)
            if not casamento:
                erros.append(f"fatias.{artigo}: o artigo deve seguir o padrão 'NN-tema'")
            elif fatia.catalogo != f"medalhao_{casamento[1]}_{casamento[2].replace('-', '_')}":
                erros.append(f"fatias.{artigo}: catálogo '{fatia.catalogo}' fora do padrão medalhao_NN_<tema>")
            erros += [f"fatias.{artigo}: sistema desconhecido '{s}'" for s in fatia.sistemas if s not in self.sistemas]
        if erros:
            raise ValueError("; ".join(erros))
        return self

    def fatia(self, artigo: str) -> Fatia:
        """Fatia declarada para o artigo; `KeyError` com as opções válidas se não existir."""
        try:
            return self.fatias[artigo]
        except KeyError:
            raise KeyError(f"Artigo '{artigo}' não tem fatia no roteiro. Opções: {', '.join(self.fatias)}") from None


def carregar_roteiro(caminho: str | Path | None = None) -> Roteiro:
    """Lê e valida o roteiro. Sem `caminho`, usa o `roteiro.yaml` empacotado no gerador."""
    origem = str(caminho) if caminho else ROTEIRO_PADRAO
    try:
        if caminho:
            texto = Path(caminho).read_text(encoding="utf-8")
        else:
            texto = resources.files(__package__).joinpath(ROTEIRO_PADRAO).read_text(encoding="utf-8")
        dados = yaml.safe_load(texto)
    except (OSError, yaml.YAMLError) as erro:
        raise RoteiroInvalido(f"Não foi possível ler o roteiro {origem}: {erro}") from erro
    if not isinstance(dados, dict):
        raise RoteiroInvalido(f"Roteiro {origem} inválido: o conteúdo deve ser um mapeamento YAML.")
    try:
        return Roteiro.model_validate(dados)
    except ValidationError as erro:
        detalhes = "\n".join(
            f"  - {'.'.join(str(p) for p in e['loc']) or '(raiz)'}: {e['msg']}" for e in erro.errors()
        )
        raise RoteiroInvalido(f"Roteiro {origem} inválido:\n{detalhes}") from None
