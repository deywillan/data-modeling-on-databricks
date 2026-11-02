"""*Rastro*: plataforma de telemetria da holding, entregue como muitos arquivos pequenos de
JSON aninhado (um stream simulado por arquivos, sem Kafka e sem `withStreaming`).

Entidade única `eventos`: um evento de GPS e telemetria por veículo a cada
`intervalo_eventos_minutos` dentro do `horario_operacao`, só dos veículos da **frota
própria** do mundo verdadeiro (os vendidos pela ModelBricks Veículos às empresas do grupo),
a partir do dia seguinte à venda. O veículo é identificado pela chave do sistema
(`id_dispositivo`, ver `chaves.py`) e também pela placa, que é o ponto de conformação com o
*Pátio*. Linha JSON:

    {"id_evento", "ts_evento",
     "dispositivo": {"id_dispositivo", "placa", "tipo_veiculo", "empresa"},
     "posicao": {"latitude", "longitude", "velocidade_kmh", "direcao_graus"},
     "telemetria": {"odometro_km", "combustivel_pct", "temperatura_motor_c", "rpm"},
     "alertas": [{"tipo", "valor"}, ...]}

Entrega: o dia é dividido em `arquivos_por_dia` janelas iguais e a frota em
`lotes_de_veiculos` lotes; cada (janela, lote) com eventos vira um arquivo
`rastro/eventos/<AAAA-MM-DD>/eventos_<AAAAMMDD>_<NNNN>.json`, com as linhas em ordem de
chegada. A granularidade fica toda no roteiro (`parametros.rastro`).

Janela: na carga inicial só entram os eventos dos últimos
`parametros.telemetria_dias_carga_inicial` dias até a data da carga, independentemente da
escala; no `avancar_tempo`, os dias novos. Cada evento nasce do seed do dia (seed base,
sistema, data), então gerar um intervalo em partes ou de uma vez dá os mesmos eventos.

Defeito: evento atrasado (`defeitos.py`), que chega 1 a 3 dias depois num arquivo de um
lote posterior. Os eventos são coletados em blocos de dias (até `_EVENTOS_POR_COLETA`
eventos por bloco) para limitar a memória do driver sem multiplicar as idas ao Spark.
"""

from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo, registros, tabela_de_registros
from ..escritor import Arquivo, caminho_landing
from ..expressoes import uniforme
from ..mundo import linhas as linhas_na_escala
from .base import Contexto, Entrega, Lote, calendario, materializar

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "rastro"
ENTIDADE = "eventos"
_EVENTOS_POR_COLETA = 500_000  # eventos por ida ao Spark, no máximo (limita a memória do driver)
_SEGUNDOS_DIA = 86400


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto)
    primeiro_evento = projecao.primeiro_evento(inicio, fim)
    lotes, defeitos = [], []
    dia = max(inicio, primeiro_evento)
    while dia <= fim:
        ultimo = min(fim, dia + timedelta(days=projecao.dias_por_coleta - 1))
        eventos = projecao.entregues(max(primeiro_evento, dia - timedelta(days=projecao.folga)), dia, ultimo)
        lotes += projecao.lotes(eventos)
        defeitos += projecao.defeitos_entregues(eventos)
        dia = ultimo + timedelta(days=1)
    tabela = tabela_de_registros(contexto.spark, defeitos)
    return Entrega(lotes=lotes, defeitos=[tabela] if tabela is not None else [], mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.rastro
        self.defeitos = contexto.defeitos(SISTEMA)
        # Atraso máximo dos defeitos de entrega, mais um dia para o ajuste de até uma hora.
        self.folga = self.defeitos.folga_dias() + 1
        operacao = self.p.horario_operacao
        self.eventos_por_dia = (operacao.maximo - operacao.minimo) * 60 // self.p.intervalo_eventos_minutos
        frota = linhas_na_escala(contexto.roteiro.volumes.veiculos_frota_propria, contexto.escala)
        self.dias_por_coleta = max(1, _EVENTOS_POR_COLETA // (frota * self.eventos_por_dia))

    def primeiro_evento(self, inicio: date, fim: date) -> date:
        """Dia do evento mais antigo que a entrega pode conter."""
        if self.contexto.carga_inicial:
            janela = self.roteiro.parametros.telemetria_dias_carga_inicial
            return max(inicio, fim - timedelta(days=janela - 1))
        return max(self.roteiro.datas.inicio, inicio - timedelta(days=self.folga))

    def chave(self, coluna_id: str) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, "veiculos", coluna_id, 6)

    # -- frota vinda do mundo verdadeiro --------------------------------------------------

    @cached_property
    def frota(self) -> "DataFrame":
        """Veículos da frota própria: chave do sistema, placa, empresa, início da operação,
        quilometragem diária e lote de entrega."""
        mundo = self.contexto.mundo
        km = self.p.km_por_dia
        semente_veiculo = self.roteiro.seed_base
        frota = mundo["participacoes"].where("tipo_entidade = 'veiculo' AND papel = 'frota'").selectExpr(
            "id_entidade AS id_veiculo", "empresa", "desde"
        )
        return materializar(self.contexto, frota.join(mundo["veiculos"], "id_veiculo").selectExpr(
            "id_veiculo",
            f"{self.chave('id_veiculo')} AS id_dispositivo",
            "placa", "tipo", "empresa", "desde",
            f"{km.minimo} + pmod(xxhash64({semente_veiculo}, '{SISTEMA}', 'km', id_veiculo), {km.maximo - km.minimo + 1}) AS km_dia",
            f"cast(pmod(xxhash64({semente_veiculo}, '{SISTEMA}', 'lote', id_veiculo), {self.p.lotes_de_veiculos}) AS int) AS lote",
        ))

    def mapa_chaves(self) -> "DataFrame":
        return mapa(self.frota, SISTEMA, ENTIDADE, "id_dispositivo", "veiculos", "id_veiculo")

    # -- eventos --------------------------------------------------------------------------

    def eventos(self, primeiro: date, ultimo: date) -> "DataFrame":
        """Todos os eventos ocorridos de `primeiro` a `ultimo`, com as colunas de controle."""
        from pyspark.sql import functions as F

        p = self.p
        n = self.eventos_por_dia
        lat, lon = p.area_operacao.latitude, p.area_operacao.longitude
        vel, cons, temp, rpm = p.velocidade_kmh, p.consumo_combustivel_pct_dia, p.temperatura_motor_c, p.rpm

        def por_veiculo_e_dia(numero: int) -> str:
            return uniforme("semente", "id_veiculo", numero)

        def por_evento(numero: int) -> str:  # `k` é o sequencial do evento no dia
            return uniforme("semente", "id_veiculo", "k", numero)

        fracao = f"(k / {max(n - 1, 1)}.0)"

        def entre(minimo, maximo, sorteado: str) -> str:
            return f"({minimo!r} + ({maximo!r} - {minimo!r}) * {sorteado})"

        lat_o, lat_d = (entre(lat.minimo, lat.maximo, por_veiculo_e_dia(numero)) for numero in (2, 3))
        lon_o, lon_d = (entre(lon.minimo, lon.maximo, por_veiculo_e_dia(numero)) for numero in (4, 5))
        velocidade = f"cast(round({entre(vel.minimo, vel.maximo, por_evento(6))}, 1) AS decimal(5,1))"
        alertas = (
            "filter(array("
            f"CASE WHEN velocidade_kmh > {p.limite_velocidade_kmh} "
            "THEN named_struct('tipo', 'excesso_velocidade', 'valor', velocidade_kmh) END, "
            f"CASE WHEN {por_evento(10)} < {p.frenagem_brusca!r} "
            "THEN named_struct('tipo', 'frenagem_brusca', 'valor', velocidade_kmh) END"
            "), a -> a IS NOT NULL)"
        )
        dias = calendario(self.contexto, SISTEMA, ultimo).where(F.col("data") >= F.lit(primeiro))
        return (
            dias.crossJoin(self.frota)
            .where("data > desde")  # o veículo opera a partir do dia seguinte à venda
            .select("*", F.explode(F.sequence(F.lit(0), F.lit(n - 1))).alias("k"))
            .selectExpr(
                "*",
                f"timestamp_seconds(unix_seconds(cast(data AS timestamp)) + {p.horario_operacao.minimo * 3600} "
                f"+ k * {p.intervalo_eventos_minutos * 60} "
                f"+ pmod(xxhash64(semente, id_veiculo, k, 1), {min(60, p.intervalo_eventos_minutos * 60)})) AS ts_evento",
                f"cast(round({lat_o} + ({lat_d} - {lat_o}) * {fracao}, 6) AS decimal(9,6)) AS latitude",
                f"cast(round({lon_o} + ({lon_d} - {lon_o}) * {fracao}, 6) AS decimal(9,6)) AS longitude",
                f"cast(pmod(round(degrees(atan2({lon_d} - {lon_o}, {lat_d} - {lat_o}))), 360) AS int) AS direcao_graus",
                f"{velocidade} AS velocidade_kmh",
                f"cast(round((datediff(data, desde) - 1) * km_dia + km_dia * {fracao}, 1) AS decimal(10,1)) AS odometro_km",
                f"cast(round(100 - {entre(cons.minimo, cons.maximo, por_veiculo_e_dia(7))} * {fracao}, 1) AS decimal(4,1)) AS combustivel_pct",
                f"cast(round({entre(temp.minimo, temp.maximo, por_evento(8))}, 1) AS decimal(4,1)) AS temperatura_motor_c",
                f"cast({rpm.minimo} + pmod(xxhash64(semente, id_veiculo, k, 9), {rpm.maximo - rpm.minimo + 1}) AS int) AS rpm",
                "id_veiculo AS _id_verdadeiro",
            )
            .withColumn("id_evento", F.expr("concat(cast(id_dispositivo AS string), '-', date_format(ts_evento, 'yyyyMMddHHmmss'))"))
            .withColumn("alertas", F.expr(alertas))
        )

    def entregues(self, primeiro: date, de: date, ate: date) -> "DataFrame":
        """Eventos ocorridos desde `primeiro` que chegam de `de` a `ate`, com o arquivo de
        chegada (`data`, `sequencial`) e a linha JSON pronta."""
        from pyspark.sql import functions as F

        p = self.p
        alvo = Alvo(ENTIDADE, ("id_evento",), "veiculos", "_id_verdadeiro")
        df = self.defeitos.evento_atrasado(self.eventos(primeiro, ate), alvo, "ts_evento")
        if "_entrega" not in df.columns:
            df = df.withColumn("_entrega", F.col("ts_evento"))
        linha = (
            "to_json(named_struct("
            "'id_evento', id_evento, 'ts_evento', ts_evento, "
            "'dispositivo', named_struct('id_dispositivo', id_dispositivo, 'placa', placa, "
            "'tipo_veiculo', tipo, 'empresa', empresa), "
            "'posicao', named_struct('latitude', latitude, 'longitude', longitude, "
            "'velocidade_kmh', velocidade_kmh, 'direcao_graus', direcao_graus), "
            "'telemetria', named_struct('odometro_km', odometro_km, 'combustivel_pct', combustivel_pct, "
            "'temperatura_motor_c', temperatura_motor_c, 'rpm', rpm), "
            "'alertas', alertas))"
        )
        janela = (
            f"cast(floor((unix_seconds(_entrega) - unix_seconds(cast(to_date(_entrega) AS timestamp))) "
            f"* {p.arquivos_por_dia} / {_SEGUNDOS_DIA}) AS int)"
        )
        return (
            df.where(F.to_date("_entrega").between(F.lit(de), F.lit(ate)))
            .selectExpr(
                "*",
                "cast(to_date(_entrega) AS string) AS _data_lote",
                f"{janela} * {p.lotes_de_veiculos} + lote + 1 AS _sequencial",
                f"{linha} AS _linha",
            )
        )

    def lotes(self, entregues: "DataFrame") -> list[Lote]:
        """Coleta os eventos entregues e os separa em um arquivo por (dia, sequencial)."""
        tabela = (
            entregues.select("_data_lote", "_sequencial", "_linha", "_entrega", "id_evento")
            .orderBy("_data_lote", "_sequencial", "_entrega", "id_evento")
            .select("_data_lote", "_sequencial", "_linha")
            .toArrow()
        )
        datas = tabela.column("_data_lote").to_pylist()
        sequenciais = tabela.column("_sequencial").to_pylist()
        linhas = tabela.column("_linha")
        lotes, inicio = [], 0
        for i in range(1, len(datas) + 1):
            if i < len(datas) and (datas[i], sequenciais[i]) == (datas[inicio], sequenciais[inicio]):
                continue
            data_lote = date.fromisoformat(datas[inicio])
            conteudo = "".join(f"{linha}\n" for linha in linhas.slice(inicio, i - inicio).to_pylist())
            caminho = caminho_landing(SISTEMA, ENTIDADE, data_lote, "json", sequenciais[inicio])
            lotes.append(Lote(SISTEMA, ENTIDADE, data_lote, "json_aninhado", i - inicio,
                              Arquivo(caminho, conteudo.encode("utf-8"))))
            inicio = i
        return lotes

    def defeitos_entregues(self, entregues: "DataFrame") -> list:
        """Registros de `gabarito.defeitos` das linhas sujas entregues."""
        if "_defeitos" not in entregues.columns:
            return []
        tabela = (
            entregues.where("size(_defeitos) > 0")
            .selectExpr(f"'{ENTIDADE}' AS entidade", "_data_lote AS data", "_defeitos", "id_evento")
            .orderBy("data", "id_evento")
            .toArrow()
        )
        return registros(tabela, SISTEMA, seq_coluna=None)


__all__ = ["projetar"]
