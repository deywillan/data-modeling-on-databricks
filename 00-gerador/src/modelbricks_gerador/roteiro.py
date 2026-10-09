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
    rotas: int = Field(gt=0)
    bases: int = Field(gt=0)
    lojas: int = Field(gt=0)
    colaboradores: int = Field(gt=0)


class Intervalo(_Modelo):
    minimo: int = Field(ge=0)
    maximo: int = Field(ge=0)


class ParametrosEmbarque(_Modelo):
    """Comportamento da bilhetagem da Viação (*Embarque*)."""

    fracao_passagens: float = Field(gt=0, le=1)  # das passagens Embarque + PassagemHub
    canais: dict[str, float] = Field(min_length=1)
    horario_vendas: Intervalo  # horas do dia (início inclusive, fim exclusive)
    antecedencia_compra_dias: Intervalo
    poltronas: int = Field(gt=0)
    estorno: float = Field(ge=0, le=1)  # exclusão física da passagem (operação D)
    cancelamento: float = Field(ge=0, le=1)
    remarcacao: float = Field(ge=0, le=1)
    adiamento_remarcacao_dias: Intervalo


class Parametros(_Modelo):
    pessoas_em_mais_de_uma_empresa: float = Field(ge=0, le=1)
    eventos_por_frete: Intervalo
    telemetria_dias_carga_inicial: int = Field(gt=0)
    embarque: ParametrosEmbarque


class Periodo(_Modelo):
    inicio: date
    fim: date


Pesos = dict[str, float]


class Participacao(_Modelo):
    pessoas: Pesos = Field(min_length=2)
    empresas_pj: Pesos = Field(min_length=2)
    empresas_pj_em_mais_de_uma_empresa: float = Field(ge=0, le=1)


class Rotas(_Modelo):
    empresas: Pesos = Field(min_length=1)
    distancia_km: Intervalo
    tarifa_por_km: float = Field(gt=0)


class ParametrosMundo(_Modelo):
    """Parâmetros do mundo verdadeiro (entidades mestres e participação nas empresas)."""

    tamanho_pools: int = Field(gt=0)
    ufs_atuacao: list[str] = Field(min_length=1)
    idade_pessoas: Intervalo
    abertura_empresas: Periodo
    admissao_colaboradores: Periodo
    ano_fabricacao_veiculos: Intervalo
    participacao: Participacao
    vendas_veiculos_para_pj: float = Field(ge=0, le=1)
    rotas: Rotas
    bases: Pesos = Field(min_length=1)
    lojas: Pesos = Field(min_length=1)
    colaboradores: Pesos = Field(min_length=1)


class ModeloVeiculo(_Modelo):
    marca: str
    modelo: str
    tipo: Literal["onibus", "caminhao"]
    preco_minimo: float = Field(gt=0)
    preco_maximo: float = Field(gt=0)


class Catalogos(_Modelo):
    dominios_email: list[str] = Field(min_length=1)
    ddds: list[int] = Field(min_length=1)
    ramos_empresa: list[str] = Field(min_length=1)
    tipos_base: dict[str, str]
    nomes_loja: dict[str, str]
    cargos: dict[str, list[str]]
    modelos_veiculos: list[ModeloVeiculo] = Field(min_length=1)


class Sistema(_Modelo):
    nome: str
    empresa: str
    entrega: Literal["cdc", "incremental", "snapshot", "api", "stream", "historico_mais_snapshot", "referencia"]
    formato: Literal["json", "json_aninhado", "parquet", "csv", "csv_latin1"]


class Evento(_Modelo):
    data: date
    sistemas: list[str]
    descricao: str
    parametros: dict[str, float] = Field(default_factory=dict)


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
    mundo: ParametrosMundo
    catalogos: Catalogos
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
        erros += self._coerencia_do_mundo()
        erros += self._coerencia_do_embarque()
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

    def _coerencia_do_mundo(self) -> list[str]:
        erros = []
        v, m, c = self.volumes, self.mundo, self.catalogos
        if not v.veiculos_frota_propria <= v.vendas_veiculos <= v.veiculos:
            erros.append("volumes: é preciso veiculos_frota_propria <= vendas_veiculos <= veiculos")
        if v.colaboradores > v.pessoas:
            erros.append("volumes.colaboradores maior que volumes.pessoas")
        for nome in ("idade_pessoas", "ano_fabricacao_veiculos"):
            intervalo = getattr(m, nome)
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"mundo.{nome}.minimo é maior que o máximo")
        for nome in ("abertura_empresas", "admissao_colaboradores"):
            if getattr(m, nome).inicio > getattr(m, nome).fim:
                erros.append(f"mundo.{nome}.inicio é posterior ao fim")
        if m.rotas.distancia_km.minimo > m.rotas.distancia_km.maximo:
            erros.append("mundo.rotas.distancia_km.minimo é maior que o máximo")
        grupos = {
            "mundo.participacao.pessoas": m.participacao.pessoas,
            "mundo.participacao.empresas_pj": m.participacao.empresas_pj,
            "mundo.rotas.empresas": m.rotas.empresas,
            "mundo.bases": m.bases,
            "mundo.lojas": m.lojas,
            "mundo.colaboradores": m.colaboradores,
        }
        for campo, pesos in grupos.items():
            erros += [f"{campo}: peso negativo em '{e}'" for e, p in pesos.items() if p < 0]
            if sum(pesos.values()) <= 0:
                erros.append(f"{campo}: a soma dos pesos deve ser positiva")
        erros += [f"catalogos.tipos_base: falta a empresa '{e}'" for e in m.bases if e not in c.tipos_base]
        erros += [f"catalogos.nomes_loja: falta a empresa '{e}'" for e in m.lojas if e not in c.nomes_loja]
        erros += [f"catalogos.cargos: falta a empresa '{e}'" for e in m.colaboradores if not c.cargos.get(e)]
        tipos = {modelo.tipo for modelo in c.modelos_veiculos}
        if tipos != {"onibus", "caminhao"}:
            erros.append("catalogos.modelos_veiculos: é preciso ao menos um ônibus e um caminhão")
        erros += [
            f"catalogos.modelos_veiculos: preço mínimo maior que o máximo em '{x.modelo}'"
            for x in c.modelos_veiculos
            if x.preco_minimo > x.preco_maximo
        ]
        return erros

    def _coerencia_do_embarque(self) -> list[str]:
        erros = []
        e = self.parametros.embarque
        for nome in ("horario_vendas", "antecedencia_compra_dias", "adiamento_remarcacao_dias"):
            intervalo = getattr(e, nome)
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.embarque.{nome}.minimo é maior que o máximo")
        if e.horario_vendas.minimo < 1 or e.horario_vendas.maximo > 23 or e.horario_vendas.minimo == e.horario_vendas.maximo:
            erros.append("parametros.embarque.horario_vendas deve ficar entre 1 e 23 h, com fim depois do início")
        if e.adiamento_remarcacao_dias.minimo < 1:
            erros.append("parametros.embarque.adiamento_remarcacao_dias.minimo deve ser pelo menos 1")
        if e.estorno + e.cancelamento + e.remarcacao > 1:
            erros.append("parametros.embarque: estorno + cancelamento + remarcacao passa de 1")
        if any(p < 0 for p in e.canais.values()) or sum(e.canais.values()) <= 0:
            erros.append("parametros.embarque.canais: pesos devem ser não negativos com soma positiva")
        reajuste = self.eventos.get("reajuste_tarifas_embarque")
        if reajuste is None or "percentual" not in reajuste.parametros:
            erros.append("eventos.reajuste_tarifas_embarque: falta o evento ou o parâmetro 'percentual'")
        return erros

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
        linhas = []
        for e in erro.errors():
            campo = ".".join(str(p) for p in e["loc"])
            for problema in e["msg"].removeprefix("Value error, ").split("; "):
                linhas.append(f"  - {campo}: {problema}" if campo else f"  - {problema}")
        detalhes = "\n".join(linhas)
        raise RoteiroInvalido(f"Roteiro {origem} inválido:\n{detalhes}") from None
