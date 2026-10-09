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
    ordens_servico_patio: int = Field(gt=0)


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


class IntervaloReal(_Modelo):
    minimo: float
    maximo: float


class AreaOperacao(_Modelo):
    latitude: IntervaloReal
    longitude: IntervaloReal


class ParametrosRastro(_Modelo):
    """Comportamento da telemetria da holding (*Rastro*)."""

    arquivos_por_dia: int = Field(gt=0, le=1440)  # janelas iguais do dia, um arquivo por janela
    lotes_de_veiculos: int = Field(gt=0, le=100)  # arquivos por janela (frota dividida em lotes)
    intervalo_eventos_minutos: int = Field(gt=0)
    horario_operacao: Intervalo  # horas do dia (início inclusive, fim exclusive)
    km_por_dia: Intervalo
    velocidade_kmh: Intervalo
    limite_velocidade_kmh: int = Field(gt=0)
    frenagem_brusca: float = Field(ge=0, le=1)
    consumo_combustivel_pct_dia: Intervalo
    temperatura_motor_c: Intervalo
    rpm: Intervalo
    area_operacao: AreaOperacao


class ParametrosPassagemHub(_Modelo):
    """Comportamento do marketplace parceiro da Viação (*PassagemHub*)."""

    eventos_por_pagina: int = Field(gt=0)
    canais: dict[str, float] = Field(min_length=1)
    horario_vendas: Intervalo  # horas do dia (início inclusive, fim exclusive)
    antecedencia_compra_dias: Intervalo
    poltronas: int = Field(gt=0)
    cancelamento: float = Field(ge=0, le=1)
    remarcacao: float = Field(ge=0, le=1)
    adiamento_remarcacao_dias: Intervalo
    taxa_servico: float = Field(ge=0)
    formas_pagamento: dict[str, float] = Field(min_length=1)
    parcelas_cartao: Intervalo


class ParametrosNucleo(_Modelo):
    """Comportamento do ERP corporativo (*Núcleo*). Taxas "ao ano" por registro."""

    horario_implantacao: Intervalo  # horas do dia (início inclusive, fim exclusive)
    horario_expediente: Intervalo
    mudanca_cargo_ao_ano: float = Field(ge=0, le=1)
    fornecedores: float = Field(gt=0, le=1)  # fração das empresas PJ
    fornecedores_na_implantacao: float = Field(ge=0, le=1)
    atualizacao_fornecedor_ao_ano: float = Field(ge=0, le=1)
    exclusao_fornecedor_ao_ano: float = Field(ge=0, le=1)
    titulos_pagar: int = Field(gt=0)  # na escala 1, de datas.inicio a datas.carga_inicial
    empresas_titulos: dict[str, float] = Field(min_length=1)
    categorias_despesa: dict[str, float] = Field(min_length=1)
    valor_titulo: Intervalo
    prazo_vencimento_dias: Intervalo
    dias_pagamento_apos_vencimento: Intervalo
    cancelamento_titulo: float = Field(ge=0, le=1)
    depreciacao_ao_ano: float = Field(ge=0, le=1)


class EtapaFrete(_Modelo):
    status: str
    opcional: bool = False


class ParametrosRotaCerta(_Modelo):
    """Comportamento do TMS da Cargas (*RotaCerta*): ciclo de status dos fretes."""

    horario_operacao: Intervalo  # horas do dia (início inclusive, fim exclusive)
    dias_entre_eventos: Intervalo
    prazo_entrega_dias: Intervalo
    peso_kg: Intervalo
    frete_por_kg: float = Field(gt=0)
    ciclo: list[EtapaFrete] = Field(min_length=2)


class PeriodoSnapshot(_Modelo):
    """Frequência das fotos até `ate` (inclusive); sem `ate`, vale dali em diante."""

    ate: date | None = None
    frequencia: Literal["diaria", "semanal", "mensal"]
    dia_semana: int | None = Field(default=None, ge=0, le=6)  # semanal: 0 = segunda


class AgendaSnapshots(_Modelo):
    """Datas em que um sistema de snapshot entrega fotos completas (ver `snapshot.py`)."""

    desde: date | None = None  # nenhuma foto antes desta data (ex.: aquisição)
    periodos: list[PeriodoSnapshot] = Field(min_length=1)

    @model_validator(mode="after")
    def _coerencia(self):
        erros = []
        ates = [p.ate for p in self.periodos]
        if ates[-1] is not None or any(a is None for a in ates[:-1]):
            erros.append("só o último período não tem 'ate'")
        elif any(a >= b for a, b in zip(ates[:-2], ates[1:-1])):
            erros.append("os 'ate' dos períodos devem ser crescentes")
        for periodo in self.periodos:
            if (periodo.frequencia == "semanal") != (periodo.dia_semana is not None):
                erros.append("'dia_semana' é obrigatório na frequência semanal e só nela")
        if erros:
            raise ValueError("; ".join(erros))
        return self


class MudancasCadastro(_Modelo):
    maximo_por_cliente: int = Field(ge=0)
    intervalo_dias: Intervalo
    pesos: dict[Literal["telefone", "email", "endereco"], float] = Field(min_length=1)


class OrdensServico(_Modelo):
    duracao_dias: Intervalo
    cancelamento: float = Field(ge=0, le=1)
    valor: Intervalo
    tipos: list[str] = Field(min_length=1)


class ParametrosPatio(_Modelo):
    """Comportamento do DMS da ModelBricks Veículos (*Pátio*)."""

    snapshots: AgendaSnapshots
    horario_operacao: Intervalo  # horas do dia (início inclusive, fim exclusive)
    dias_em_estoque: Intervalo
    mudancas_cadastro: MudancasCadastro
    ordens_servico: OrdensServico
    clientes_grupo: dict[str, str] = Field(min_length=1)  # empresa do grupo → razão social


class ParametrosApolice(_Modelo):
    """Comportamento do sistema de apólices da ModelBricks Seguros (*Apólice+*)."""

    horario_expediente: Intervalo  # horas do dia (início inclusive, fim exclusive)
    atualizacao_segurado_ao_ano: float = Field(ge=0, le=1)
    vigencia_meses: int = Field(gt=0)
    ramos_pf: dict[str, float] = Field(min_length=1)
    ramos_pj: dict[str, float] = Field(min_length=1)
    importancia_segurada: Intervalo
    taxa_premio: IntervaloReal
    seguro_na_venda: float = Field(ge=0, le=1)
    taxa_premio_veiculo: IntervaloReal
    cancelamento: float = Field(ge=0, le=1)


class Parametros(_Modelo):
    pessoas_em_mais_de_uma_empresa: float = Field(ge=0, le=1)
    eventos_por_frete: Intervalo
    telemetria_dias_carga_inicial: int = Field(gt=0)
    embarque: ParametrosEmbarque
    rastro: ParametrosRastro
    passagemhub: ParametrosPassagemHub
    nucleo: ParametrosNucleo
    rotacerta: ParametrosRotaCerta
    patio: ParametrosPatio
    apolice: ParametrosApolice


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
    atraso_dias: Intervalo | None = None  # defeitos de entrega: dias até a linha chegar


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
        erros += self._coerencia_do_rastro()
        erros += self._coerencia_do_passagemhub()
        erros += self._coerencia_do_nucleo()
        erros += self._coerencia_do_rotacerta()
        erros += self._coerencia_do_patio()
        erros += self._coerencia_do_apolice()
        for nome, evento in self.eventos.items():
            erros += [f"eventos.{nome}: sistema desconhecido '{s}'" for s in evento.sistemas if s not in self.sistemas]
        for nome, defeito in self.defeitos.items():
            for sistema, taxa in defeito.taxas.items():
                if sistema not in self.sistemas:
                    erros.append(f"defeitos.{nome}: sistema desconhecido '{sistema}'")
                if isinstance(taxa, float) and not 0 <= taxa <= 1:
                    erros.append(f"defeitos.{nome}.taxas.{sistema}: taxa {taxa} fora de 0 a 1")
            atraso = defeito.atraso_dias
            if atraso is not None and not 1 <= atraso.minimo <= atraso.maximo:
                erros.append(f"defeitos.{nome}.atraso_dias: é preciso 1 <= minimo <= maximo")
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

    def _coerencia_do_rastro(self) -> list[str]:
        erros = []
        r = self.parametros.rastro
        for nome in ("horario_operacao", "km_por_dia", "velocidade_kmh", "consumo_combustivel_pct_dia",
                     "temperatura_motor_c", "rpm"):
            intervalo = getattr(r, nome)
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.rastro.{nome}.minimo é maior que o máximo")
        if not 0 <= r.horario_operacao.minimo < r.horario_operacao.maximo <= 24:
            erros.append("parametros.rastro.horario_operacao deve ficar entre 0 e 24 h, com fim depois do início")
        if r.horario_operacao.maximo * 60 - r.horario_operacao.minimo * 60 < r.intervalo_eventos_minutos:
            erros.append("parametros.rastro.intervalo_eventos_minutos maior que o horário de operação")
        if r.arquivos_por_dia * r.lotes_de_veiculos > 9999:
            erros.append("parametros.rastro: arquivos_por_dia × lotes_de_veiculos passa de 9999 arquivos por dia")
        if r.consumo_combustivel_pct_dia.maximo > 100:
            erros.append("parametros.rastro.consumo_combustivel_pct_dia passa de 100")
        for eixo in ("latitude", "longitude"):
            faixa = getattr(r.area_operacao, eixo)
            if faixa.minimo > faixa.maximo:
                erros.append(f"parametros.rastro.area_operacao.{eixo}.minimo é maior que o máximo")
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

    def _coerencia_do_passagemhub(self) -> list[str]:
        erros = []
        p = self.parametros.passagemhub
        for nome in ("horario_vendas", "antecedencia_compra_dias", "adiamento_remarcacao_dias", "parcelas_cartao"):
            intervalo = getattr(p, nome)
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.passagemhub.{nome}.minimo é maior que o máximo")
        if p.horario_vendas.maximo > 24 or p.horario_vendas.minimo == p.horario_vendas.maximo:
            erros.append("parametros.passagemhub.horario_vendas deve ficar entre 0 e 24 h, com fim depois do início")
        if p.adiamento_remarcacao_dias.minimo < 1 or p.parcelas_cartao.minimo < 1:
            erros.append("parametros.passagemhub: adiamento_remarcacao_dias e parcelas_cartao começam em 1")
        if p.cancelamento + p.remarcacao > 1:
            erros.append("parametros.passagemhub: cancelamento + remarcacao passa de 1")
        for nome in ("canais", "formas_pagamento"):
            pesos = getattr(p, nome)
            if any(v < 0 for v in pesos.values()) or sum(pesos.values()) <= 0:
                erros.append(f"parametros.passagemhub.{nome}: pesos devem ser não negativos com soma positiva")
        if "coluna_nova_passagemhub" not in self.eventos:
            erros.append("eventos.coluna_nova_passagemhub: falta o evento")
        return erros

    def _coerencia_do_nucleo(self) -> list[str]:
        erros = []
        n = self.parametros.nucleo
        for nome in ("horario_implantacao", "horario_expediente", "valor_titulo",
                     "prazo_vencimento_dias", "dias_pagamento_apos_vencimento"):
            intervalo = getattr(n, nome)
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.nucleo.{nome}.minimo é maior que o máximo")
        for nome in ("horario_implantacao", "horario_expediente"):
            intervalo = getattr(n, nome)
            if intervalo.maximo > 24 or intervalo.minimo == intervalo.maximo:
                erros.append(f"parametros.nucleo.{nome} deve ficar entre 0 e 24 h, com fim depois do início")
        if n.prazo_vencimento_dias.minimo < 1 or n.valor_titulo.minimo < 1:
            erros.append("parametros.nucleo: prazo_vencimento_dias e valor_titulo começam em 1")
        for nome in ("empresas_titulos", "categorias_despesa"):
            pesos = getattr(n, nome)
            if any(v < 0 for v in pesos.values()) or sum(pesos.values()) <= 0:
                erros.append(f"parametros.nucleo.{nome}: pesos devem ser não negativos com soma positiva")
        return erros

    def _coerencia_do_rotacerta(self) -> list[str]:
        erros = []
        r = self.parametros.rotacerta
        for nome in ("horario_operacao", "dias_entre_eventos", "prazo_entrega_dias", "peso_kg"):
            intervalo = getattr(r, nome)
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.rotacerta.{nome}.minimo é maior que o máximo")
        h = r.horario_operacao
        if h.minimo < 1 or h.maximo > 24 or h.minimo == h.maximo:
            erros.append("parametros.rotacerta.horario_operacao deve ficar entre 1 e 24 h, com fim depois do início")
        if r.dias_entre_eventos.minimo < 1 or r.prazo_entrega_dias.minimo < 1 or r.peso_kg.minimo < 1:
            erros.append("parametros.rotacerta: dias_entre_eventos, prazo_entrega_dias e peso_kg começam em 1")
        status = [etapa.status for etapa in r.ciclo]
        if len(set(status)) != len(status):
            erros.append("parametros.rotacerta.ciclo: status repetido")
        if r.ciclo[0].opcional or r.ciclo[-1].opcional:
            erros.append("parametros.rotacerta.ciclo: a primeira e a última etapa não podem ser opcionais")
        obrigatorias = sum(not etapa.opcional for etapa in r.ciclo)
        eventos = self.parametros.eventos_por_frete
        if not obrigatorias <= eventos.minimo <= eventos.maximo <= len(r.ciclo):
            erros.append(
                f"parametros.eventos_por_frete deve ficar entre as {obrigatorias} etapas obrigatórias "
                f"e as {len(r.ciclo)} etapas do ciclo do rotacerta"
            )
        return erros

    def _coerencia_do_patio(self) -> list[str]:
        erros = []
        p = self.parametros.patio
        intervalos = {
            "horario_operacao": p.horario_operacao,
            "dias_em_estoque": p.dias_em_estoque,
            "mudancas_cadastro.intervalo_dias": p.mudancas_cadastro.intervalo_dias,
            "ordens_servico.duracao_dias": p.ordens_servico.duracao_dias,
            "ordens_servico.valor": p.ordens_servico.valor,
        }
        for nome, intervalo in intervalos.items():
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.patio.{nome}.minimo é maior que o máximo")
        if p.horario_operacao.minimo < 1 or p.horario_operacao.maximo > 23 or p.horario_operacao.minimo == p.horario_operacao.maximo:
            erros.append("parametros.patio.horario_operacao deve ficar entre 1 e 23 h, com fim depois do início")
        if p.dias_em_estoque.minimo < 1:
            erros.append("parametros.patio.dias_em_estoque.minimo deve ser pelo menos 1")
        if p.mudancas_cadastro.intervalo_dias.minimo < 1:
            erros.append("parametros.patio.mudancas_cadastro.intervalo_dias.minimo deve ser pelo menos 1")
        pesos = p.mudancas_cadastro.pesos
        if any(v < 0 for v in pesos.values()) or sum(pesos.values()) <= 0:
            erros.append("parametros.patio.mudancas_cadastro.pesos: pesos não negativos com soma positiva")
        erros += [
            f"parametros.patio.clientes_grupo: empresa '{e}' não compra frota própria"
            for e in p.clientes_grupo if e not in ("mb_viacao", "mb_cargas")
        ]
        erros += [
            f"parametros.patio.clientes_grupo: falta a empresa '{e}'"
            for e in ("mb_viacao", "mb_cargas") if e not in p.clientes_grupo
        ]
        return erros

    def _coerencia_do_apolice(self) -> list[str]:
        erros = []
        p = self.parametros.apolice
        intervalos = {
            "horario_expediente": p.horario_expediente, "importancia_segurada": p.importancia_segurada,
            "taxa_premio": p.taxa_premio, "taxa_premio_veiculo": p.taxa_premio_veiculo,
        }
        for nome, intervalo in intervalos.items():
            if intervalo.minimo > intervalo.maximo:
                erros.append(f"parametros.apolice.{nome}.minimo é maior que o máximo")
        if p.horario_expediente.maximo > 24 or p.horario_expediente.minimo == p.horario_expediente.maximo:
            erros.append("parametros.apolice.horario_expediente deve ficar entre 0 e 24 h, com fim depois do início")
        if p.importancia_segurada.minimo < 1 or p.taxa_premio.minimo <= 0 or p.taxa_premio_veiculo.minimo <= 0:
            erros.append("parametros.apolice: importancia_segurada e taxas de prêmio devem ser positivas")
        for nome in ("ramos_pf", "ramos_pj"):
            pesos = getattr(p, nome)
            if any(v < 0 for v in pesos.values()) or sum(pesos.values()) <= 0:
                erros.append(f"parametros.apolice.{nome}: pesos devem ser não negativos com soma positiva")
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
