"""*Apólice+*: sistema de apólices da ModelBricks Seguros, entregue como incremental por
`updated_at` em CSV, com valor monetário como texto ("1.234,56").

Cada lote diário traz as linhas alteradas no dia, com a imagem completa da linha e o
`updated_at` da mudança, como o *Núcleo* (mesmo motor, `incremental.py`), mas em CSV e sem
*soft delete*: o fim de uma apólice é uma mudança de `status`.

| Entidade | Chave | Origem | Versões |
|---|---|---|---|
| `segurados` | `codigo_segurado` | clientes da ModelBricks Seguros na participação do mundo (PF e PJ) | cadastro; troca de telefone |
| `apolices` | `numero_apolice` | fato do sistema (avulsas) e vendas de veículos do mundo a segurados | emissão (`ativa`); cancelamento ou encerramento |

Os segurados são exatamente os clientes da ModelBricks Seguros em `participacoes`, cadastrados
no dia em que passaram a ser clientes (`desde`): quem também viaja pela Viação aparece nos
dois sistemas, na proporção do mundo verdadeiro. A chave do segurado deriva do id verdadeiro
(pessoa ou empresa) e o mapa vai para `gabarito.mapa_chaves`.

Apólices avulsas: emitidas dia a dia na proporção dos segurados já cadastrados, somando
`volumes.apolices` (na escala) de `datas.inicio` à carga inicial e seguindo no mesmo ritmo no
`avancar_tempo`. Seguros vendidos junto com veículos: cada venda de veículo do mundo a um
segurado já cadastrado leva o seguro (ramo `veiculo`, com a placa) na fração
`seguro_na_venda`, com a importância segurada igual ao valor da venda. Vigência de
`vigencia_meses`; a apólice é cancelada durante a vigência (`cancelamento`) ou encerrada no
dia seguinte ao fim dela. Números no `roteiro.yaml` (`parametros.apolice`).

Defeitos (`defeitos.py`): valor monetário como texto em `importancia_segurada` e `premio`
(coluna inteira, registrado por arquivo em `gabarito.defeitos`), CPF fora do padrão nos
segurados e, se o roteiro der taxa, evento atrasado.
"""

import io
from bisect import bisect_left
from dataclasses import replace
from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo, Defeitos, tabela_de_registros
from ..incremental import CSV, Versoes, entrega_incremental
from ..mundo import linhas as linhas_na_escala
from ..roteiro import Intervalo
from ..sementes import semente
from .base import Contexto, Entrega, Lote, calendario, materializar, semente_do_lote

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "apolice"
MONETARIAS = ("importancia_segurada", "premio")
_DIAS_ANO = 365
_BASE_VENDAS = 90000  # numeração das apólices vendidas com veículos dentro do dia


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    apolices = projecao.defeitos.valor_monetario_texto(projecao.versoes_apolices(), MONETARIAS)
    versoes = [
        Versoes("segurados", ("codigo_segurado",), projecao.sujar_segurados(projecao.versoes_segurados())),
        Versoes("apolices", ("numero_apolice",), projecao.atrasar(apolices, Alvo("apolices", ("numero_apolice",)))),
    ]
    entrega = entrega_incremental(SISTEMA, versoes, inicio, fim, CSV)
    return replace(
        entrega,
        defeitos=entrega.defeitos + projecao.registros_valor_texto(entrega.lotes),
        mapa_chaves=projecao.mapa_chaves(),
    )


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.apolice
        self.fim = fim
        self.inicio = contexto.roteiro.datas.inicio
        self.defeitos = Defeitos(contexto.roteiro, SISTEMA)

    def semente(self, *partes: str) -> int:
        return semente(self.roteiro.seed_base, SISTEMA, *partes)

    @staticmethod
    def u(semente_: "int | str", *colunas: str) -> str:
        """Uniforme em [0, 1) por hash estável das colunas."""
        return f"(pmod(xxhash64({semente_}, {', '.join(colunas)}), 1000000) / 1000000.0)"

    @staticmethod
    def instante(data: str, faixa: Intervalo, semente_: "int | str", *colunas: str) -> str:
        """Timestamp no dia `data`, na faixa de horas, sorteado pelas colunas."""
        return (
            f"timestamp_seconds(unix_seconds(cast({data} AS timestamp)) + {faixa.minimo * 3600} "
            f"+ pmod(xxhash64({semente_}, {', '.join(colunas)}), {(faixa.maximo - faixa.minimo) * 3600}))"
        )

    @staticmethod
    def sorteio(pesos: dict[str, float], u: str) -> str:
        nomes, total, acumulado, casos = sorted(pesos), sum(pesos.values()), 0.0, []
        for nome in nomes[:-1]:
            acumulado += pesos[nome] / total
            casos.append(f"WHEN {u} < {acumulado!r} THEN '{nome}'")
        return f"CASE {' '.join(casos)} ELSE '{nomes[-1]}' END" if casos else f"'{nomes[-1]}'"

    # -- defeitos ---------------------------------------------------------------------------

    def sujar_segurados(self, df: "DataFrame") -> "DataFrame":
        # Só os segurados PF têm CPF (nos PJ a coluna é nula e o defeito não se aplica).
        df = self.defeitos.cpf_fora_do_padrao(
            df, Alvo("segurados", ("codigo_segurado",), "pessoas", "_id_verdadeiro"), "cpf")
        return self.atrasar(df, Alvo("segurados", ("codigo_segurado",)))

    def atrasar(self, df: "DataFrame", alvo: Alvo) -> "DataFrame":
        return self.defeitos.evento_atrasado(df, alvo, instante="updated_at")

    def registros_valor_texto(self, lotes: list[Lote]) -> list["DataFrame"]:
        """Um registro por arquivo de apólices e coluna monetária, com o primeiro valor do
        arquivo como exemplo (o sujo, como chegou, e o original, como número)."""
        if self.defeitos.taxa("valor_monetario_texto") != "coluna":
            return []
        import pyarrow as pa
        import pyarrow.csv as pcsv

        linhas = []
        for lote in lotes:
            if lote.entidade != "apolices":
                continue
            tabela = pcsv.read_csv(
                io.BytesIO(lote.arquivo.conteudo),
                convert_options=pcsv.ConvertOptions(
                    include_columns=list(MONETARIAS), column_types={c: pa.string() for c in MONETARIAS}),
            )
            for coluna in MONETARIAS:
                sujo = tabela.column(coluna)[0].as_py()
                original = sujo.replace(".", "").replace(",", ".")
                linhas.append(self.defeitos.linha_agregada(
                    "valor_monetario_texto", lote.entidade, lote.data_simulada, arquivo=lote.arquivo.caminho,
                    coluna=coluna, valor_original=original, valor_sujo=sujo,
                ))
        registro = tabela_de_registros(self.contexto.spark, linhas)
        return [registro] if registro is not None else []

    # -- segurados ---------------------------------------------------------------------------

    @cached_property
    def segurados(self) -> "DataFrame":
        """Clientes da ModelBricks Seguros (PF e PJ), numerados (`_indice`) pela ordem de
        cadastro: no dia `d`, os segurados já cadastrados são os de `_indice` < ativos(d)."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        mundo = self.contexto.mundo
        clientes = mundo["participacoes"].where("papel = 'cliente' AND empresa = 'mb_seguros'")
        pf = clientes.where("tipo_entidade = 'pessoa'").join(
            mundo["pessoas"].withColumnRenamed("id_pessoa", "id_entidade"), "id_entidade",
        ).selectExpr(
            "id_entidade AS _id_verdadeiro", "'pessoas' AS _tabela", "id_entidade * 2 AS _id_unico",
            "'PF' AS tipo_pessoa", "nome", "cpf", "cast(NULL AS string) AS cnpj", "data_nascimento",
            "email", "telefone", "cod_municipio", "desde AS data_cadastro",
        )
        pj = clientes.where("tipo_entidade = 'empresa_pj'").join(
            mundo["empresas_pj"].withColumnRenamed("id_empresa", "id_entidade"), "id_entidade",
        ).selectExpr(
            "id_entidade AS _id_verdadeiro", "'empresas_pj' AS _tabela", "id_entidade * 2 + 1 AS _id_unico",
            "'PJ' AS tipo_pessoa", "razao_social AS nome", "cast(NULL AS string) AS cpf", "cnpj",
            "cast(NULL AS date) AS data_nascimento", "email", "telefone", "cod_municipio", "desde AS data_cadastro",
        )
        chave = chave_do_sistema(self.roteiro.seed_base, SISTEMA, "segurados", "_id_unico", 7)
        return materializar(self.contexto, (
            pf.unionByName(pj)
            .withColumn("codigo_segurado", F.expr(chave))
            .withColumn("_indice", F.row_number().over(Window.orderBy("data_cadastro", "codigo_segurado")) - 1)
        ))

    def mapa_chaves(self) -> "DataFrame":
        """Mapa acumulado: todos os segurados, qualquer que seja o intervalo."""
        return mapa(
            self.segurados.where("_tabela = 'pessoas'"), SISTEMA, "segurados", "codigo_segurado", "pessoas",
            "_id_verdadeiro",
        ).unionByName(mapa(
            self.segurados.where("_tabela = 'empresas_pj'"), SISTEMA, "segurados", "codigo_segurado", "empresas_pj",
            "_id_verdadeiro",
        ))

    def versoes_segurados(self) -> "DataFrame":
        """Cadastro no dia em que virou cliente; depois, uma versão por troca de telefone
        (sorteada dia a dia com o seed do lote do dia)."""
        s = self.semente("segurados")
        fixas = ["_id_verdadeiro", "codigo_segurado", "tipo_pessoa", "nome", "cpf", "cnpj", "data_nascimento", "email"]
        ordem = [*fixas, "telefone", "cod_municipio", "data_cadastro", "updated_at"]
        cadastro = self.segurados.selectExpr(
            *fixas, "telefone", "cod_municipio", "data_cadastro",
            f"{self.instante('data_cadastro', self.p.horario_expediente, s, '_id_unico')} AS updated_at",
        )
        novo_telefone = (
            "concat(substr(telefone, 1, 5), '9', "
            "lpad(cast(pmod(xxhash64(semente, _id_unico, 3), 10000) AS string), 4, '0'), '-', "
            "lpad(cast(pmod(xxhash64(semente, _id_unico, 4), 10000) AS string), 4, '0'))"
        )
        trocas = (
            calendario(self.contexto, SISTEMA, self.fim).crossJoin(self.segurados)
            .where("data > data_cadastro")
            .where(f"{self.u('semente', '_id_unico', '1')} < {self.p.atualizacao_segurado_ao_ano / _DIAS_ANO!r}")
            .selectExpr(
                *fixas, f"{novo_telefone} AS telefone", "cod_municipio", "data_cadastro",
                f"{self.instante('data', self.p.horario_expediente, 'semente', '_id_unico', '5')} AS updated_at",
            )
        )
        return cadastro.select(*ordem).unionByName(trocas.select(*ordem))

    # -- apólices ----------------------------------------------------------------------------

    def dias_com_ativos(self) -> "DataFrame":
        """Um dia por linha até `fim`, com a semente do lote, os segurados já cadastrados
        (`_ativos`) e as apólices avulsas esperadas no dia (`_esperado`)."""
        from pyspark.sql.types import DateType, DoubleType, LongType, StructField, StructType

        cadastros = sorted(r[0] for r in self.segurados.select("data_cadastro").collect())
        carga = self.roteiro.datas.carga_inicial
        dias_segurado = sum(max((carga - c).days, 0) for c in cadastros)
        avulsas = linhas_na_escala(self.roteiro.volumes.apolices, self.contexto.escala)
        por_segurado_dia = avulsas / dias_segurado if dias_segurado else 0.0
        linhas = []
        for i in range((self.fim - self.inicio).days + 1):
            dia = self.inicio + timedelta(days=i)
            ativos = bisect_left(cadastros, dia)
            linhas.append((dia, semente_do_lote(self.roteiro, SISTEMA, dia), ativos, ativos * por_segurado_dia))
        esquema = StructType([
            StructField("data", DateType(), False), StructField("semente", LongType(), False),
            StructField("_ativos", LongType(), False), StructField("_esperado", DoubleType(), False),
        ])
        return self.contexto.spark.createDataFrame(linhas, esquema)

    def avulsas(self) -> "DataFrame":
        from pyspark.sql import functions as F

        p = self.p

        def u(k: int) -> str:
            return self.u("semente", "n", str(k))

        imp, taxa = p.importancia_segurada, p.taxa_premio
        ramo = f"CASE tipo_pessoa WHEN 'PF' THEN {self.sorteio(p.ramos_pf, u(2))} ELSE {self.sorteio(p.ramos_pj, u(2))} END"
        importancia = f"cast(round({imp.minimo} + {imp.maximo - imp.minimo} * {u(3)}, 2) AS decimal(14,2))"
        return (
            self.dias_com_ativos().where("_ativos > 0")
            .withColumn("_n_dia", F.expr(
                f"cast(floor(_esperado) AS int) + CASE WHEN {self.u('semente', '0')} < _esperado - floor(_esperado) "
                "THEN 1 ELSE 0 END"))
            .where("_n_dia > 0")
            .select("data", "semente", "_ativos", F.explode(F.sequence(F.lit(1), F.col("_n_dia"))).alias("n"))
            .withColumn("_indice", F.expr("cast(pmod(xxhash64(semente, n, 1), _ativos) AS int)"))
            .join(self.segurados.select("_indice", "codigo_segurado", "tipo_pessoa"), "_indice")
            .withColumn("importancia_segurada", F.expr(importancia))
            .selectExpr(
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 100000 + n AS numero_apolice",
                "codigo_segurado", f"{ramo} AS ramo", "cast(NULL AS string) AS placa", "data AS inicio_vigencia",
                "importancia_segurada",
                f"cast(round(importancia_segurada * ({taxa.minimo!r} + {taxa.maximo - taxa.minimo!r} * {u(4)}), 2) "
                "AS decimal(14,2)) AS premio",
                f"{self.instante('data', p.horario_expediente, 'semente', 'n', '5')} AS _emitida_em",
            )
        )

    def vendidas_com_veiculos(self) -> "DataFrame":
        """Vendas de veículos do mundo a segurados já cadastrados, sorteadas por
        `seguro_na_venda`; a importância segurada é o valor da venda."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        p, s = self.p, self.semente("vendas")
        mundo = self.contexto.mundo
        vendas = mundo["vendas_veiculos"].join(mundo["veiculos"].select("id_veiculo", "placa"), "id_veiculo").selectExpr(
            "id_venda", "placa", "data_venda", "valor_venda",
            "CASE tipo_comprador WHEN 'pf' THEN 'pessoas' WHEN 'pj' THEN 'empresas_pj' END AS _tabela",
            "CASE tipo_comprador WHEN 'pf' THEN id_pessoa WHEN 'pj' THEN id_empresa END AS _id_verdadeiro",
        )
        taxa = p.taxa_premio_veiculo
        return (
            vendas.join(self.segurados.select("_tabela", "_id_verdadeiro", "codigo_segurado", "data_cadastro"),
                        ["_tabela", "_id_verdadeiro"])
            .where(f"data_venda > data_cadastro AND data_venda <= DATE'{self.fim.isoformat()}'")
            .where(f"{self.u(s, 'id_venda', '1')} < {p.seguro_na_venda!r}")
            .withColumn("_n", F.row_number().over(Window.partitionBy("data_venda").orderBy("id_venda")))
            .selectExpr(
                f"cast(date_format(data_venda, 'yyyyMMdd') AS bigint) * 100000 + {_BASE_VENDAS} + _n AS numero_apolice",
                "codigo_segurado", "'veiculo' AS ramo", "placa", "data_venda AS inicio_vigencia",
                "cast(valor_venda AS decimal(14,2)) AS importancia_segurada",
                f"cast(round(valor_venda * ({taxa.minimo!r} + {taxa.maximo - taxa.minimo!r} * {self.u(s, 'id_venda', '2')}), 2) "
                "AS decimal(14,2)) AS premio",
                f"{self.instante('data_venda', p.horario_expediente, s, 'id_venda', '3')} AS _emitida_em",
            )
        )

    def versoes_apolices(self) -> "DataFrame":
        """Emissão (`ativa`); depois, cancelamento durante a vigência ou encerramento no dia
        seguinte ao fim dela."""
        from pyspark.sql import functions as F

        p, s = self.p, self.semente("apolices")
        apolices = self.avulsas().unionByName(self.vendidas_com_veiculos()).selectExpr(
            "*", f"date_sub(add_months(inicio_vigencia, {p.vigencia_meses}), 1) AS fim_vigencia",
            f"{self.u(s, 'numero_apolice', '1')} < {p.cancelamento!r} AS _cancelada",
        )
        imagem = ["numero_apolice", "codigo_segurado", "ramo", "placa", "inicio_vigencia", "fim_vigencia",
                  "importancia_segurada", "premio"]
        cancelamento = (
            "date_add(inicio_vigencia, cast(1 + pmod(xxhash64("
            f"{s}, numero_apolice, 2), datediff(fim_vigencia, inicio_vigencia)) AS int))"
        )

        def versao(df: "DataFrame", status: str, data_cancelamento: str, updated_at: str) -> "DataFrame":
            return df.selectExpr(
                *imagem, f"'{status}' AS status", f"cast({data_cancelamento} AS date) AS data_cancelamento",
                f"{updated_at} AS updated_at",
            )

        canceladas = apolices.where("_cancelada").withColumn("_dia", F.expr(cancelamento))
        encerradas = apolices.where("NOT _cancelada").withColumn("_dia", F.expr("date_add(fim_vigencia, 1)"))
        return (
            versao(apolices, "ativa", "NULL", "_emitida_em")
            .unionByName(versao(canceladas, "cancelada", "_dia",
                                self.instante("_dia", p.horario_expediente, s, "numero_apolice", "3")))
            .unionByName(versao(encerradas, "encerrada", "NULL",
                                self.instante("_dia", p.horario_expediente, s, "numero_apolice", "4")))
        )


__all__ = ["projetar"]
