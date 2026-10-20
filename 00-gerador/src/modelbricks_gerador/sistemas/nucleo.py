"""*Núcleo*: ERP corporativo do grupo (financeiro, faturamento, fornecedores, colaboradores e
ativos de Viação, Cargas e Veículos), entregue como incremental por `updated_at` em Parquet,
com *soft delete*.

Cada lote diário traz as linhas alteradas no dia, com a imagem completa da linha, o
`updated_at` da mudança e a flag `excluido` (exclusão lógica: o registro excluído chega
como uma versão com `excluido = true`, nunca deixa de vir). O ERP não tem log de mudança:
quem consome compara versões pelo `updated_at`.

| Entidade | Chave | Origem | Versões |
|---|---|---|---|
| `colaboradores` | `matricula` | colaboradores do mundo verdadeiro | entrada (cargo do mundo); uma por mudança de cargo |
| `fornecedores` | `codigo_fornecedor` | parte das empresas PJ do mundo | cadastro; troca de telefone; exclusão lógica |
| `titulos_pagar` | `id_titulo` | fato do sistema (contas a pagar) | emissão; pagamento ou exclusão lógica (cancelamento) |
| `notas_fiscais` | `numero_nota` | vendas de veículos do mundo | emissão (faturamento da ModelBricks Veículos) |
| `ativos` | `id_ativo` | veículos da frota própria do mundo | aquisição; depreciação no fim de cada mês |

O *Núcleo* foi implantado no primeiro dia da história (`datas.inicio`): o que já existia
nele (colaboradores admitidos e fornecedores cadastrados antes) chega no lote desse dia,
de madrugada, como na carga de uma implantação. Depois disso, cada mudança chega no lote
do dia em que aconteceu. O cargo do mundo verdadeiro é o cargo de entrada do colaborador;
as mudanças de cargo (histórico usado no artigo de historização) são do sistema.

Mudanças sorteadas "ao ano" (cargo, telefone e exclusão do fornecedor) são decididas dia
a dia com o seed do lote daquele dia (seed base, sistema, data), então continuam no
`avancar_tempo`. O motor genérico (`incremental.py`) monta os lotes. Números no
`roteiro.yaml` (`parametros.nucleo`).

Defeitos (`defeitos.py`), aplicados só quando o roteiro dá taxa ao *Núcleo*: CPF fora do
padrão e nome abreviado nos colaboradores, contato nulo nos fornecedores e evento atrasado
em todas as entidades. O roteiro do Estudo de Caso não dá taxa ao *Núcleo*.
"""

from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from dataclasses import replace

from ..chaves import chave_do_sistema, mapa
from ..defeitos import Alvo
from ..incremental import PARQUET, Versoes, entrega_incremental
from ..expressoes import instante, sorteio, uniforme, uniforme_do_lote
from ..sementes import semente
from .base import Contexto, Entrega, calendario, linhas_por_dia, materializar, volume_por_dia

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SISTEMA = "nucleo"
_DIAS_ANO = 365


def projetar(contexto: Contexto, inicio: date, fim: date) -> Entrega:
    projecao = _Projecao(contexto, fim)
    versoes = [
        Versoes("colaboradores", ("matricula",), projecao.sujar_colaboradores(projecao.versoes_colaboradores())),
        Versoes("fornecedores", ("codigo_fornecedor",), projecao.sujar_fornecedores(projecao.versoes_fornecedores())),
        Versoes("titulos_pagar", ("id_titulo",), projecao.atrasar(
            projecao.versoes_titulos(inicio), Alvo("titulos_pagar", ("id_titulo",)))),
        Versoes("notas_fiscais", ("numero_nota",), projecao.atrasar(
            projecao.versoes_notas(), Alvo("notas_fiscais", ("numero_nota",), "vendas_veiculos", "_id_verdadeiro"))),
        Versoes("ativos", ("id_ativo",), projecao.atrasar(
            projecao.versoes_ativos(), Alvo("ativos", ("id_ativo",), "veiculos", "_id_verdadeiro"))),
    ]
    return replace(entrega_incremental(SISTEMA, versoes, inicio, fim, PARQUET), mapa_chaves=projecao.mapa_chaves())


class _Projecao:
    def __init__(self, contexto: Contexto, fim: date):
        self.contexto = contexto
        self.roteiro = contexto.roteiro
        self.p = contexto.roteiro.parametros.nucleo
        self.fim = fim
        self.inicio = contexto.roteiro.datas.inicio
        self.defeitos = contexto.defeitos(SISTEMA)

    def chave(self, entidade: str, coluna_id: str, digitos: int) -> str:
        return chave_do_sistema(self.roteiro.seed_base, SISTEMA, entidade, coluna_id, digitos)

    def semente(self, *partes: str) -> int:
        return semente(self.roteiro.seed_base, SISTEMA, *partes)

    def calendario(self) -> "DataFrame":
        return calendario(self.contexto, SISTEMA, self.fim)

    def mapa_chaves(self) -> "DataFrame":
        """Mapa acumulado: todas as chaves que o *Núcleo* usa, qualquer que seja o intervalo."""
        return (
            mapa(self.colaboradores, SISTEMA, "colaboradores", "matricula", "colaboradores", "_id_verdadeiro")
            .unionByName(mapa(self.fornecedores, SISTEMA, "fornecedores", "codigo_fornecedor", "empresas_pj", "_id_verdadeiro"))
            .unionByName(mapa(self.vendas, SISTEMA, "notas_fiscais", "numero_nota", "vendas_veiculos", "_id_verdadeiro"))
            .unionByName(mapa(
                self.vendas.where("tipo_comprador = 'grupo'"), SISTEMA, "ativos", "id_ativo", "veiculos", "_id_veiculo"))
        )

    # -- defeitos ---------------------------------------------------------------------------

    def sujar_colaboradores(self, df: "DataFrame") -> "DataFrame":
        alvo = Alvo("colaboradores", ("matricula",), "colaboradores", "_id_verdadeiro")
        df = self.defeitos.cpf_fora_do_padrao(df, alvo, "cpf")
        df = self.defeitos.nome_abreviado_ou_com_erro(df, alvo, "nome")
        return self.atrasar(df, alvo)

    def sujar_fornecedores(self, df: "DataFrame") -> "DataFrame":
        alvo = Alvo("fornecedores", ("codigo_fornecedor",), "empresas_pj", "_id_verdadeiro")
        return self.atrasar(self.defeitos.contato_nulo(df, alvo, ("email", "telefone")), alvo)

    def atrasar(self, df: "DataFrame", alvo: Alvo) -> "DataFrame":
        return self.defeitos.evento_atrasado(df, alvo, instante="updated_at")

    # -- expressões comuns ------------------------------------------------------------------

    def entrada(self, data: str, semente_ou_coluna: "int | str", *colunas: str) -> str:
        """Instante da primeira versão: de madrugada no dia da implantação (registros que já
        existiam antes do *Núcleo*) ou no expediente do próprio dia."""
        return (
            f"CASE WHEN {data} <= DATE'{self.inicio.isoformat()}' "
            f"THEN {instante(f'DATE{self.inicio.isoformat()!r}', self.p.horario_implantacao, semente_ou_coluna, *colunas)} "
            f"ELSE {instante(data, self.p.horario_expediente, semente_ou_coluna, *colunas)} END"
        )

    @staticmethod
    def por_dia(taxa_ao_ano: float) -> float:
        return taxa_ao_ano / _DIAS_ANO

    # -- colaboradores ------------------------------------------------------------------------

    @cached_property
    def colaboradores(self) -> "DataFrame":
        mundo = self.contexto.mundo
        return materializar(self.contexto, mundo["colaboradores"].join(
            mundo["pessoas"].select("id_pessoa", "nome", "cpf"), "id_pessoa",
        ).selectExpr(
            "id_colaborador AS _id_verdadeiro",
            f"{self.chave('colaboradores', 'id_colaborador', 6)} AS matricula",
            "nome", "cpf", "empresa", "cargo", "data_admissao",
        ))

    def versoes_colaboradores(self) -> "DataFrame":
        """Entrada com o cargo do mundo verdadeiro e uma versão por mudança de cargo. O cargo
        novo anda um passo sorteado (nunca zero) na lista de cargos distintos da empresa, então
        sempre difere do anterior."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        cargos = {
            empresa: sorted(set(lista)) for empresa, lista in self.roteiro.catalogos.cargos.items()
        }
        lista = "CASE empresa " + " ".join(
            f"WHEN '{e}' THEN array({', '.join(repr(c) for c in cs)})" for e, cs in sorted(cargos.items())
        ) + " END"
        s = self.semente("colaboradores")
        imagem = ["matricula", "nome", "cpf", "empresa", "cargo", "data_admissao"]
        base = self.colaboradores.selectExpr(
            "*", f"greatest(data_admissao, DATE'{self.inicio.isoformat()}') AS _desde", f"{lista} AS _cargos",
        )
        entrada = base.selectExpr(
            "_id_verdadeiro", *imagem, f"{self.entrada('data_admissao', s, '_id_verdadeiro')} AS updated_at",
            "false AS excluido",
        )
        janela = Window.partitionBy("_id_verdadeiro").orderBy("data")
        mudancas = (
            self.calendario().crossJoin(base)
            .where("data > _desde AND size(_cargos) > 1")
            .where(f"{uniforme('semente', '_id_verdadeiro', '1')} < {self.por_dia(self.p.mudanca_cargo_ao_ano)!r}")
            .withColumn("_passo", F.expr("1 + pmod(xxhash64(semente, _id_verdadeiro, 2), size(_cargos) - 1)"))
            .withColumn("_acumulado", F.sum("_passo").over(janela))
            .withColumn("cargo", F.expr(
                "element_at(_cargos, cast(pmod(array_position(_cargos, cargo) - 1 + _acumulado, size(_cargos)) + 1 AS int))"))
            .selectExpr(
                "_id_verdadeiro", *imagem,
                f"{instante('data', self.p.horario_expediente, 'semente', '_id_verdadeiro', '3')} AS updated_at",
                "false AS excluido",
            )
        )
        return entrada.unionByName(mudancas)

    # -- fornecedores -------------------------------------------------------------------------

    @cached_property
    def fornecedores(self) -> "DataFrame":
        """Empresas PJ do mundo que fornecem ao grupo, com o dia do cadastro no *Núcleo*."""
        s = self.semente("fornecedores")
        dias_historia = (self.roteiro.datas.carga_inicial - self.inicio).days + 1
        cadastro = (
            f"CASE WHEN {uniforme(s, 'id_empresa', '1')} < {self.p.fornecedores_na_implantacao!r} "
            f"THEN DATE'{self.inicio.isoformat()}' "
            f"ELSE date_add(DATE'{self.inicio.isoformat()}', cast(pmod(xxhash64({s}, id_empresa, 2), {dias_historia}) AS int)) END"
        )
        return materializar(self.contexto, self.contexto.mundo["empresas_pj"].where(
            f"{uniforme(s, 'id_empresa', '0')} < {self.p.fornecedores!r}"
        ).selectExpr(
            "id_empresa AS _id_verdadeiro",
            f"{self.chave('fornecedores', 'id_empresa', 6)} AS codigo_fornecedor",
            "cnpj", "razao_social", "email", "telefone", "cod_municipio",
            f"{cadastro} AS _cadastro",
        ))

    @cached_property
    def eventos_fornecedores(self) -> "DataFrame":
        """Trocas de telefone e a exclusão (a primeira sorteada encerra o fornecedor)."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        sorteados = (
            self.calendario().crossJoin(self.fornecedores.select("_id_verdadeiro", "_cadastro"))
            .where("data > _cadastro")
            .withColumn("_exclui", F.expr(
                f"{uniforme('semente', '_id_verdadeiro', '1')} < {self.por_dia(self.p.exclusao_fornecedor_ao_ano)!r}"))
            .withColumn("_atualiza", F.expr(
                f"{uniforme('semente', '_id_verdadeiro', '2')} < {self.por_dia(self.p.atualizacao_fornecedor_ao_ano)!r}"))
            .where("_exclui OR _atualiza")
            .withColumn("_exclusao", F.min(F.when(F.col("_exclui"), F.col("data"))).over(
                Window.partitionBy("_id_verdadeiro")))
            .where("_exclusao IS NULL OR data <= _exclusao")
        )
        return materializar(self.contexto, sorteados.select(
            "_id_verdadeiro", "data", "semente", "_exclusao", (F.col("data") == F.col("_exclusao")).alias("_excluido"),
        ))

    def versoes_fornecedores(self) -> "DataFrame":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        s = self.semente("fornecedores")
        imagem = ["codigo_fornecedor", "cnpj", "razao_social", "email", "cod_municipio"]
        cadastro = self.fornecedores.selectExpr(
            "_id_verdadeiro", *imagem, "telefone",
            f"{self.entrada('_cadastro', s, '_id_verdadeiro')} AS updated_at", "false AS excluido",
        )
        novo_telefone = (
            "CASE WHEN NOT _excluido THEN concat(substr(telefone, 1, 5), '9', "
            "lpad(cast(pmod(xxhash64(semente, _id_verdadeiro, 3), 10000) AS string), 4, '0'), '-', "
            "lpad(cast(pmod(xxhash64(semente, _id_verdadeiro, 4), 10000) AS string), 4, '0')) END"
        )
        janela = Window.partitionBy("_id_verdadeiro").orderBy("data").rowsBetween(Window.unboundedPreceding, 0)
        eventos = (
            self.eventos_fornecedores.join(self.fornecedores, "_id_verdadeiro")
            .withColumn("_novo_telefone", F.expr(novo_telefone))
            .withColumn("telefone", F.coalesce(F.last("_novo_telefone", ignorenulls=True).over(janela), F.col("telefone")))
            .selectExpr(
                "_id_verdadeiro", *imagem, "telefone",
                f"{instante('data', self.p.horario_expediente, 'semente', '_id_verdadeiro', '5')} AS updated_at",
                "_excluido AS excluido",
            )
        )
        colunas = ["_id_verdadeiro", "codigo_fornecedor", "cnpj", "razao_social", "email", "telefone",
                   "cod_municipio", "updated_at", "excluido"]
        return cadastro.select(*colunas).unionByName(eventos.select(*colunas))

    @cached_property
    def fornecedores_ativos(self) -> "DataFrame":
        """Por dia, os fornecedores que podem receber títulos (cadastrados antes do dia e não
        excluídos até ele), numerados para o sorteio, com o total do dia."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        exclusoes = self.eventos_fornecedores.where("_excluido").select("_id_verdadeiro", "_exclusao")
        vida = self.fornecedores.select("_id_verdadeiro", "codigo_fornecedor", "_cadastro").join(
            exclusoes, "_id_verdadeiro", "left")
        return (
            self.calendario().select("data").crossJoin(vida)
            .where("_cadastro < data AND (_exclusao IS NULL OR data < _exclusao)")
            .withColumn("_indice_fornecedor", F.row_number().over(
                Window.partitionBy("data").orderBy("codigo_fornecedor")) - 1)
            .withColumn("_fornecedores", F.count(F.lit(1)).over(Window.partitionBy("data")))
            .select("data", "codigo_fornecedor", "_indice_fornecedor", "_fornecedores")
        )

    # -- títulos a pagar ----------------------------------------------------------------------

    def versoes_titulos(self, inicio: date) -> "DataFrame":
        """Emissão (`aberto`); depois, pagamento (`pago`) ou exclusão lógica antes do vencimento."""
        from pyspark.sql import functions as F

        p, datas = self.p, self.roteiro.datas
        defasagem = p.prazo_vencimento_dias.maximo + p.dias_pagamento_apos_vencimento.maximo + 1 + self.defeitos.folga_dias()
        primeira = max(datas.inicio, inicio - timedelta(days=defasagem))
        inteiro, fracao = volume_por_dia(self.contexto, p.titulos_pagar)
        prazo, pagamento, valor = p.prazo_vencimento_dias, p.dias_pagamento_apos_vencimento, p.valor_titulo
        ativos = self.fornecedores_ativos
        totais = ativos.select("data", "_fornecedores").distinct()
        titulos = (
            linhas_por_dia(self.calendario().where(F.col("data") >= F.lit(primeira)), inteiro, fracao)
            .join(totais, "data")
            .selectExpr(
                "*",
                "cast(date_format(data, 'yyyyMMdd') AS bigint) * 10000 + n AS id_titulo",
                "pmod(xxhash64(semente, n, 1), _fornecedores) AS _indice_fornecedor",
                f"{sorteio(p.empresas_titulos, uniforme_do_lote(2))} AS empresa",
                f"{sorteio(p.categorias_despesa, uniforme_do_lote(3))} AS categoria",
                f"cast(round({valor.minimo} + {valor.maximo - valor.minimo} * {uniforme_do_lote(4)}, 2) AS decimal(12,2)) AS valor",
                "data AS data_emissao",
                f"date_add(data, cast({prazo.minimo} + pmod(xxhash64(semente, n, 5), "
                f"{prazo.maximo - prazo.minimo + 1}) AS int)) AS data_vencimento",
                f"{instante('data', p.horario_expediente, 'semente', 'n', '6')} AS _emitido_em",
                f"{uniforme_do_lote(7)} < {p.cancelamento_titulo!r} AS _cancelado",
                f"{uniforme_do_lote(8)} AS _u_momento",
                f"cast({pagamento.minimo} + pmod(xxhash64(semente, n, 9), "
                f"{pagamento.maximo - pagamento.minimo + 1}) AS int) AS _dias_pagamento",
            )
            .join(ativos.select("data", "codigo_fornecedor", "_indice_fornecedor"), ["data", "_indice_fornecedor"])
        )
        imagem = ["id_titulo", "codigo_fornecedor", "empresa", "categoria", "valor", "data_emissao", "data_vencimento"]

        def versao(df: "DataFrame", status: str, data_pagamento: str, updated_at: str, excluido: bool) -> "DataFrame":
            return df.selectExpr(
                *imagem, f"'{status}' AS status", f"cast({data_pagamento} AS date) AS data_pagamento",
                f"{updated_at} AS updated_at", f"{str(excluido).lower()} AS excluido",
            )

        emitido = "unix_seconds(_emitido_em)"
        cancelamento = (
            f"timestamp_seconds({emitido} + 1 + cast(floor(_u_momento * "
            f"(unix_seconds(cast(data_vencimento AS timestamp)) - {emitido} - 1)) AS bigint))"
        )
        pago = titulos.where("NOT _cancelado").withColumn("_pago_em", F.expr("date_add(data_vencimento, _dias_pagamento)"))
        return (
            versao(titulos, "aberto", "NULL", "_emitido_em", False)
            .unionByName(versao(titulos.where("_cancelado"), "aberto", "NULL", cancelamento, True))
            .unionByName(versao(
                pago, "pago", "_pago_em", instante("_pago_em", p.horario_expediente, "semente", "n", "10"), False))
        )

    # -- faturamento e ativos -----------------------------------------------------------------

    @cached_property
    def vendas(self) -> "DataFrame":
        """Vendas de veículos do mundo, com o veículo e o comprador (pessoa ou empresa)."""
        mundo = self.contexto.mundo
        veiculos = mundo["veiculos"].selectExpr(
            "id_veiculo", "placa", "ano_fabricacao",
            "concat(CASE tipo WHEN 'onibus' THEN 'Ônibus' ELSE 'Caminhão' END, ' ', marca, ' ', modelo) AS descricao",
        )
        pessoas = mundo["pessoas"].selectExpr("id_pessoa", "nome AS _nome_pf", "cpf AS _cpf")
        empresas = mundo["empresas_pj"].selectExpr("id_empresa", "razao_social AS _razao_social", "cnpj AS _cnpj")
        return materializar(self.contexto, (
            mundo["vendas_veiculos"].join(veiculos, "id_veiculo")
            .join(pessoas, "id_pessoa", "left")
            .join(empresas, "id_empresa", "left")
            .selectExpr(
                "id_venda AS _id_verdadeiro", "id_veiculo AS _id_veiculo",
                f"{self.chave('notas_fiscais', 'id_venda', 7)} AS numero_nota",
                f"{self.chave('ativos', 'id_veiculo', 6)} AS id_ativo",
                "empresa_vendedora", "empresa_compradora", "data_venda", "tipo_comprador",
                "CASE tipo_comprador WHEN 'pf' THEN _cpf WHEN 'pj' THEN _cnpj END AS documento_cliente",
                "CASE tipo_comprador WHEN 'pf' THEN _nome_pf WHEN 'pj' THEN _razao_social "
                "ELSE empresa_compradora END AS nome_cliente",
                "descricao", "placa", "ano_fabricacao", "valor_venda",
            )
        ))

    def versoes_notas(self) -> "DataFrame":
        s = self.semente("notas_fiscais")
        return self.vendas.selectExpr(
            "_id_verdadeiro", "numero_nota", "empresa_vendedora AS empresa_emitente", "data_venda AS data_emissao",
            "tipo_comprador AS tipo_cliente", "documento_cliente", "nome_cliente", "descricao", "placa",
            "valor_venda AS valor_total",
            f"{instante('data_venda', self.p.horario_expediente, s, '_id_verdadeiro')} AS updated_at",
            "false AS excluido",
        )

    def versoes_ativos(self) -> "DataFrame":
        """Aquisição pelo valor da nota; no fim de cada mês seguinte, o valor contábil depreciado."""
        s = self.semente("ativos")
        frota = self.vendas.where("tipo_comprador = 'grupo'").selectExpr(
            "_id_veiculo AS _id_verdadeiro", "id_ativo", "empresa_compradora AS empresa", "descricao", "placa",
            "ano_fabricacao", "data_venda AS data_aquisicao", "valor_venda AS valor_aquisicao",
        )
        imagem = ["_id_verdadeiro", "id_ativo", "empresa", "descricao", "placa", "ano_fabricacao", "data_aquisicao",
                  "valor_aquisicao"]
        aquisicao = frota.selectExpr(
            *imagem, "valor_aquisicao AS valor_contabil",
            f"{instante('data_aquisicao', self.p.horario_expediente, s, '_id_verdadeiro')} AS updated_at",
            "false AS excluido",
        )
        taxa_mes = self.p.depreciacao_ao_ano / 12
        depreciacao = (
            self.calendario().where("data = last_day(data)").crossJoin(frota)
            .where("data > last_day(data_aquisicao)")  # o mês da aquisição ainda não deprecia
            .selectExpr(
                *imagem,
                f"cast(greatest(round(valor_aquisicao * (1 - {taxa_mes!r} * "
                "cast(months_between(data, last_day(data_aquisicao)) AS int)), 2), 0) AS decimal(14,2)) AS valor_contabil",
                f"{instante('data', self.p.horario_expediente, 'semente', '_id_verdadeiro', '1')} AS updated_at",
                "false AS excluido",
            )
        )
        return aquisicao.unionByName(depreciacao)


__all__ = ["projetar"]
