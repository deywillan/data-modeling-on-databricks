"""Catálogo de defeitos: suja o que um sistema entrega e registra cada sujeira no gabarito.

Os defeitos são aplicados nas projeções, sobre os DataFrames que vão para a landing, e
nunca no mundo verdadeiro. Nada aqui conhece um sistema: a projeção diz qual coluna recebe
cada defeito, e as taxas vêm do roteiro (`defeitos.<tipo>.taxas.<sistema>`). Defeito sem
taxa para o sistema não faz nada, então a projeção pode chamar todos os que fazem sentido
para as suas colunas e deixar o roteiro decidir.

Há três granularidades, todas registradas em `gabarito.defeitos` (`ESQUEMA`):

- **linha**: funções `Defeitos.<tipo>(df, alvo, ...)`. Sorteiam as linhas pela taxa (por
  hash estável do seed base, do sistema, da entidade, do tipo e da chave da linha; nunca
  pela ordem ou pelo intervalo gerado), trocam o valor e anotam a linha na coluna interna
  `_defeitos` (lista de `ANOTACAO`). A anotação viaja com a linha; quem monta os lotes
  chama `registros(...)` sobre as linhas efetivamente entregues, com a data do lote, e
  descarta as colunas internas (as que começam com `_`) antes de serializar. Assim, toda
  linha suja entregue tem registro, e só ela;
- **arquivo** (ex.: `latin1`) e **coluna** (ex.: valor monetário como texto, aplicado por
  `Defeitos.valor_monetario_texto`): a projeção aplica a transformação antes de serializar
  e chama `Defeitos.registro_agregado(...)` (ou `linha_agregada`, para juntar vários) com o
  arquivo ou a coluna atingidos.

Ordem recomendada na projeção: duplicidade, depois os defeitos de valor e por fim os de
entrega (atraso e reentrega), para que a cópia duplicada também possa sair suja ou atrasada.

Defeitos de entrega (contrato com quem monta os lotes, ver `cdc.py`):
- `_entrega` (timestamp): instante em que a linha chega; sem a coluna, vale o do evento;
- `_reentrega` (boolean): a linha é a reentrega de outra já entregue (mesma operação).
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .expressoes import uniforme
from .roteiro import Roteiro
from .sementes import semente

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

ANOTACAO = (
    "struct<tipo:string,artigos:array<int>,coluna:string,chave_sistema:string,"
    "tabela_verdadeira:string,id_verdadeiro:bigint,valor_original:string,valor_sujo:string>"
)
COLUNAS_DEFEITOS = (
    "sistema", "entidade", "granularidade", "tipo", "artigos", "coluna", "chave_sistema",
    "tabela_verdadeira", "id_verdadeiro", "valor_original", "valor_sujo", "data_entrega", "seq", "arquivo",
)
ESQUEMA = (
    "sistema string, entidade string, granularidade string, tipo string, artigos array<int>, "
    "coluna string, chave_sistema string, tabela_verdadeira string, id_verdadeiro bigint, "
    "valor_original string, valor_sujo string, data_entrega date, seq bigint, arquivo string"
)
_SEM_ACENTO = ("áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ", "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC")
_SEGUNDOS_DIA = 86400


@dataclass(frozen=True)
class Alvo:
    """Entidade de um sistema que recebe defeitos de linha.

    `chave`: colunas da chave do sistema. `id_verdadeiro`: coluna (interna, com `_`) com o
    id do mundo verdadeiro, quando a entidade vem dele; `tabela_verdadeira`: a tabela dele.
    """

    entidade: str
    chave: tuple[str, ...]
    tabela_verdadeira: str | None = None
    id_verdadeiro: str | None = None


class Defeitos:
    """Defeitos de um sistema, com as taxas do roteiro."""

    def __init__(self, roteiro: Roteiro, sistema: str):
        self.roteiro = roteiro
        self.sistema = sistema

    # -- consulta ao roteiro ----------------------------------------------------------------

    def taxa(self, tipo: str) -> "float | str | None":
        """Taxa do tipo para o sistema (fração, `arquivo`, `coluna`) ou `None` se não tem."""
        return self.roteiro.defeitos[tipo].taxas.get(self.sistema)

    def folga_dias(self) -> int:
        """Quantos dias a mais de história a projeção precisa reconstruir para entregar um
        intervalo: soma dos atrasos máximos dos defeitos de entrega ativos no sistema."""
        return sum(
            d.atraso_dias.maximo
            for tipo, d in self.roteiro.defeitos.items()
            if d.atraso_dias is not None and self._ativo(tipo)
        )

    # -- defeitos de linha: valor ------------------------------------------------------------

    def cpf_fora_do_padrao(self, df: "DataFrame", alvo: Alvo, coluna: str) -> "DataFrame":
        """CPF sem máscara, com máscara ou sem os zeros à esquerda (o que difere do padrão)."""
        digitos = f"regexp_replace({coluna}, '[^0-9]', '')"
        mascara = (
            f"concat(substr({digitos}, 1, 3), '.', substr({digitos}, 4, 3), '.', "
            f"substr({digitos}, 7, 3), '-', substr({digitos}, 10, 2))"
        )
        sem_zeros = f"regexp_replace({digitos}, '^0+', '')"
        return self._trocar(df, alvo, "cpf_fora_do_padrao", coluna, self._variante(
            "cpf_fora_do_padrao", alvo, coluna, [digitos, mascara, sem_zeros]))

    def nome_abreviado_ou_com_erro(self, df: "DataFrame", alvo: Alvo, coluna: str) -> "DataFrame":
        """Nome sem acentos, com "Maria" como "Mª", com o nome do meio abreviado ou com uma
        letra omitida (erro de digitação)."""
        tipo = "nome_abreviado_ou_com_erro"
        posicao = f"(2 + pmod(xxhash64({self._semente(tipo, alvo)}, {self._chave(alvo)}, 2), greatest(length({coluna}) - 1, 1)))"
        variantes = [
            f"translate({coluna}, '{_SEM_ACENTO[0]}', '{_SEM_ACENTO[1]}')",
            f"regexp_replace({coluna}, '^Maria ', 'Mª ')",
            f"regexp_replace({coluna}, '^(\\\\S+) (\\\\S)\\\\S* (.+)$', '$1 $2. $3')",
            f"concat(substr({coluna}, 1, {posicao} - 1), substr({coluna}, {posicao} + 1))",
        ]
        return self._trocar(df, alvo, tipo, coluna, self._variante(tipo, alvo, coluna, variantes))

    def contato_nulo(self, df: "DataFrame", alvo: Alvo, colunas: tuple[str, ...]) -> "DataFrame":
        """Anula uma das colunas de contato (sorteada entre `colunas`) nas linhas sorteadas."""
        tipo = "contato_nulo"
        if not self._ativo(tipo):
            return df
        from pyspark.sql import functions as F

        escolha = F.expr(f"pmod(xxhash64({self._semente(tipo, alvo)}, {self._chave(alvo)}, 1), {len(colunas)})")
        for i, coluna in enumerate(colunas):
            df = self._trocar(df, alvo, tipo, coluna, F.lit(None).cast(df.schema[coluna].dataType), extra=escolha == i)
        return df

    def municipio_fora_do_padrao(self, df: "DataFrame", alvo: Alvo, coluna: str) -> "DataFrame":
        """Código IBGE de município com 6 dígitos (sem o verificador) ou com zero à esquerda
        (8 dígitos). Com o defeito ativo, a coluna passa a ser texto em todas as linhas."""
        tipo = "municipio_fora_do_padrao"
        if not self._ativo(tipo):
            return df
        from pyspark.sql import functions as F

        df = df.withColumn(coluna, F.col(coluna).cast("string"))
        variantes = [f"substr({coluna}, 1, 6)", f"lpad({coluna}, 8, '0')"]
        return self._trocar(df, alvo, tipo, coluna, self._variante(tipo, alvo, coluna, variantes))

    def fk_orfa(self, df: "DataFrame", alvo: Alvo, coluna: str, valor_inexistente: str) -> "DataFrame":
        """Troca a FK por `valor_inexistente` (expressão SQL que a projeção garante não
        existir na entidade referenciada). Sorteio pela chave: todas as linhas da mesma
        chave ficam órfãs juntas."""
        from pyspark.sql import functions as F

        valor = F.expr(valor_inexistente).cast(df.schema[coluna].dataType)
        return self._trocar(df, alvo, "fk_orfa", coluna, valor)

    # -- defeitos de linha: duplicidade -------------------------------------------------------

    def pessoa_duplicada_no_sistema(self, df: "DataFrame", alvo: Alvo, nova_chave: dict[str, str]) -> "DataFrame":
        """Acrescenta uma cópia das linhas sorteadas com outra chave do sistema.

        `nova_chave`: coluna → expressão SQL da chave da cópia (a projeção garante que não
        colide com chaves reais). A cópia carrega só a anotação da duplicidade, com a chave
        original em `valor_original` e a da cópia em `valor_sujo`.
        """
        tipo = "pessoa_duplicada_no_sistema"
        if not self._ativo(tipo):
            return df
        from pyspark.sql import functions as F

        df = self._com_anotacoes(df)
        copias = df.where(self._sorteado(tipo, alvo)).withColumn("_chave_original", self._chave_texto(alvo))
        for coluna, expressao in nova_chave.items():
            copias = copias.withColumn(coluna, F.expr(expressao).cast(df.schema[coluna].dataType))
        anotacao = self._anotacao(tipo, alvo, ",".join(alvo.chave), F.col("_chave_original"), self._chave_texto(alvo))
        copias = copias.withColumn("_defeitos", _lista(anotacao)).drop("_chave_original")
        return df.unionByName(copias)

    # -- defeitos de linha: entrega ----------------------------------------------------------

    def evento_atrasado(self, df: "DataFrame", alvo: Alvo, instante: str = "ts_operacao") -> "DataFrame":
        """A linha chega dias depois do evento (`_entrega` posterior a `instante`). Sorteio por
        linha (chave + instante). No CDC, a operação atrasada recebe `seq` do instante da
        entrega, maior que a de operações posteriores da mesma chave."""
        tipo = "evento_atrasado"
        if not self._ativo(tipo):
            return df
        from pyspark.sql import functions as F

        df = self._com_entrega(self._com_anotacoes(df), instante)
        novo = self._adiar(tipo, alvo, instante)
        anotacao = self._anotacao(tipo, alvo, None, F.col("_entrega").cast("string"), novo.cast("string"))
        sorteado = self._sorteado(tipo, alvo, instante) & ~F.col("_reentrega")
        return (
            df.withColumn("_defeitos", F.when(sorteado, F.concat("_defeitos", _lista(anotacao))).otherwise(F.col("_defeitos")))
            .withColumn("_entrega", F.when(sorteado, novo).otherwise(F.col("_entrega")))
        )

    def cdc_duplicado(self, df: "DataFrame", alvo: Alvo, instante: str = "ts_operacao") -> "DataFrame":
        """Acrescenta a reentrega das linhas sorteadas (`_reentrega` verdadeiro), dias depois
        da entrega original; no CDC, com a mesma `seq` da original."""
        tipo = "cdc_duplicado"
        if not self._ativo(tipo):
            return df
        from pyspark.sql import functions as F

        df = self._com_entrega(self._com_anotacoes(df), instante)
        sorteadas = df.where(self._sorteado(tipo, alvo, instante) & ~F.col("_reentrega"))
        novo = self._adiar(tipo, alvo, instante)
        anotacao = self._anotacao(tipo, alvo, None, F.col("_entrega").cast("string"), novo.cast("string"))
        copias = sorteadas.select(
            *[c for c in df.columns if c not in ("_defeitos", "_entrega", "_reentrega")],
            _lista(anotacao).alias("_defeitos"),
            novo.alias("_entrega"),
            F.lit(True).alias("_reentrega"),
        )
        return df.unionByName(copias)

    # -- arquivo e coluna inteiros -------------------------------------------------------------

    def valor_monetario_texto(self, df: "DataFrame", colunas: tuple[str, ...]) -> "DataFrame":
        """Coluna inteira: valores monetários como texto no formato brasileiro ("1.234,56").
        Sem a taxa `coluna` no roteiro, as colunas continuam numéricas. Quem monta os lotes
        registra o defeito por arquivo com `registro_agregado`/`linha_agregada`."""
        if self.taxa("valor_monetario_texto") != "coluna":
            return df
        from pyspark.sql import functions as F

        for coluna in colunas:
            df = df.withColumn(coluna, F.translate(F.format_number(F.col(coluna), 2), ",.", ".,"))
        return df

    def registro_agregado(
        self,
        spark: "SparkSession",
        tipo: str,
        entidade: str,
        data_entrega,
        *,
        arquivo: str | None = None,
        coluna: str | None = None,
        valor_original: str | None = None,
        valor_sujo: str | None = None,
    ) -> "DataFrame":
        """Registro de um defeito que atinge um arquivo inteiro (`arquivo`, ex.: `latin1`) ou
        uma coluna inteira (`coluna`, ex.: valor monetário como texto), com um exemplo opcional
        de valor original e sujo."""
        linha = self.linha_agregada(
            tipo, entidade, data_entrega, arquivo=arquivo, coluna=coluna,
            valor_original=valor_original, valor_sujo=valor_sujo,
        )
        return spark.createDataFrame([linha], ESQUEMA)

    def linha_agregada(
        self,
        tipo: str,
        entidade: str,
        data_entrega,
        *,
        arquivo: str | None = None,
        coluna: str | None = None,
        valor_original: str | None = None,
        valor_sujo: str | None = None,
    ) -> tuple:
        """O mesmo registro de `registro_agregado` como tupla (ordem de `COLUNAS_DEFEITOS`),
        para juntar muitos registros (um por arquivo) num só DataFrame com
        `tabela_de_registros`."""
        if arquivo is None and coluna is None:
            raise ValueError("Informe o arquivo ou a coluna atingidos pelo defeito.")
        granularidade = "coluna" if coluna is not None else "arquivo"
        return (
            self.sistema, entidade, granularidade, tipo, list(self.roteiro.defeitos[tipo].artigos), coluna, None,
            None, None, valor_original, valor_sujo, data_entrega, None, arquivo,
        )

    # -- internos ----------------------------------------------------------------------------

    def _ativo(self, tipo: str) -> bool:
        taxa = self.taxa(tipo)
        return taxa is not None and (not isinstance(taxa, float) or taxa > 0)

    def _semente(self, tipo: str, alvo: Alvo) -> int:
        return semente(self.roteiro.seed_base, "defeito", self.sistema, alvo.entidade, tipo)

    @staticmethod
    def _chave(alvo: Alvo) -> str:
        return ", ".join(alvo.chave)

    def _sorteio(self, tipo: str, alvo: Alvo, *extra: str) -> str:
        return uniforme(self._semente(tipo, alvo), *alvo.chave, *extra)

    def _sorteado(self, tipo: str, alvo: Alvo, *extra: str) -> "Column":
        from pyspark.sql import functions as F

        return F.expr(f"{self._sorteio(tipo, alvo, *extra)} < {float(self.taxa(tipo))!r}")

    def _chave_texto(self, alvo: Alvo) -> "Column":
        from pyspark.sql import functions as F

        return F.concat_ws("|", *[F.col(c).cast("string") for c in alvo.chave])

    def _anotacao(self, tipo, alvo: Alvo, coluna: str | None, original: "Column", sujo: "Column") -> "Column":
        from pyspark.sql import functions as F

        artigos = F.array(*[F.lit(a) for a in self.roteiro.defeitos[tipo].artigos]).cast("array<int>")
        id_verdadeiro = F.col(alvo.id_verdadeiro) if alvo.id_verdadeiro else F.lit(None)
        return F.struct(
            F.lit(tipo).alias("tipo"),
            artigos.alias("artigos"),
            F.lit(coluna).cast("string").alias("coluna"),
            self._chave_texto(alvo).alias("chave_sistema"),
            F.lit(alvo.tabela_verdadeira).cast("string").alias("tabela_verdadeira"),
            id_verdadeiro.cast("bigint").alias("id_verdadeiro"),
            original.cast("string").alias("valor_original"),
            sujo.cast("string").alias("valor_sujo"),
        )

    def _variante(self, tipo: str, alvo: Alvo, coluna: str, variantes: list[str]) -> "Column":
        """Uma das variantes que difere do valor original, sorteada pela chave."""
        from pyspark.sql import functions as F

        candidatas = f"filter(array({', '.join(variantes)}), v -> v IS NOT NULL AND v <> {coluna})"
        indice = f"pmod(xxhash64({self._semente(tipo, alvo)}, {self._chave(alvo)}, 1), greatest(size({candidatas}), 1))"
        # Sem variante diferente, fica o próprio valor (e a linha não conta como defeito).
        return F.expr(f"coalesce(try_element_at({candidatas}, cast({indice} AS int) + 1), {coluna})")

    def _trocar(self, df: "DataFrame", alvo: Alvo, tipo: str, coluna: str, sujo, extra=None) -> "DataFrame":
        """Troca `coluna` por `sujo` nas linhas sorteadas pela chave e anota a troca. Linhas
        em que o valor sujo não difere do original não contam como defeito."""
        if not self._ativo(tipo):
            return df
        from pyspark.sql import functions as F

        df = self._com_anotacoes(df)
        original = F.col(coluna)
        sorteado = self._sorteado(tipo, alvo) & ~(original.eqNullSafe(sujo))
        if extra is not None:
            sorteado = sorteado & extra
        anotacao = self._anotacao(tipo, alvo, coluna, original, sujo)
        return (
            df.withColumn("_defeitos", F.when(sorteado, F.concat("_defeitos", _lista(anotacao))).otherwise(F.col("_defeitos")))
            .withColumn(coluna, F.when(sorteado, sujo).otherwise(original))
        )

    def _adiar(self, tipo: str, alvo: Alvo, instante: str) -> "Column":
        """`_entrega` adiada pelos dias do roteiro, mais um ajuste de até uma hora."""
        from pyspark.sql import functions as F

        atraso = self.roteiro.defeitos[tipo].atraso_dias
        if atraso is None:
            raise ValueError(f"defeitos.{tipo}: falta atraso_dias no roteiro.")
        semente_do_tipo = self._semente(tipo, alvo)
        colunas = ", ".join((*alvo.chave, instante))
        dias = f"({atraso.minimo} + pmod(xxhash64({semente_do_tipo}, {colunas}, 1), {atraso.maximo - atraso.minimo + 1}))"
        ajuste = f"pmod(xxhash64({semente_do_tipo}, {colunas}, 2), 3600)"
        return F.expr(f"timestamp_seconds(unix_seconds(_entrega) + {dias} * {_SEGUNDOS_DIA} + {ajuste})")

    @staticmethod
    def _com_anotacoes(df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        if "_defeitos" in df.columns:
            return df
        return df.withColumn("_defeitos", F.array().cast(f"array<{ANOTACAO}>"))

    @staticmethod
    def _com_entrega(df: "DataFrame", instante: str) -> "DataFrame":
        from pyspark.sql import functions as F

        if "_entrega" not in df.columns:
            df = df.withColumn("_entrega", F.col(instante))
        if "_reentrega" not in df.columns:
            df = df.withColumn("_reentrega", F.lit(False))
        return df


def _lista(anotacao: "Column") -> "Column":
    from pyspark.sql import functions as F

    return F.array(anotacao).cast(f"array<{ANOTACAO}>")


def registros(linhas, sistema: str, entidade_coluna: str = "entidade", data_coluna: str = "data", seq_coluna: str | None = "seq"):
    """Registros de `gabarito.defeitos` (lista de tuplas na ordem de `COLUNAS_DEFEITOS`) a
    partir das linhas entregues já coletadas (`pyarrow.Table` com as colunas de entidade,
    data do lote, `seq` opcional e `_defeitos`)."""
    from datetime import date

    if "_defeitos" not in linhas.column_names:
        return []
    anotacoes = linhas.column("_defeitos").to_pylist()
    entidades = linhas.column(entidade_coluna).to_pylist()
    datas = linhas.column(data_coluna).to_pylist()
    seqs = linhas.column(seq_coluna).to_pylist() if seq_coluna else [None] * len(datas)
    saida = []
    for lista, entidade, data_lote, seq in zip(anotacoes, entidades, datas, seqs):
        for a in lista or ():
            saida.append((
                sistema, entidade, "linha", a["tipo"], a["artigos"], a["coluna"], a["chave_sistema"],
                a["tabela_verdadeira"], a["id_verdadeiro"], a["valor_original"], a["valor_sujo"],
                date.fromisoformat(data_lote) if isinstance(data_lote, str) else data_lote, seq, None,
            ))
    return saida


def tabela_de_registros(spark: "SparkSession", linhas: list) -> "DataFrame | None":
    """DataFrame no `ESQUEMA` de `gabarito.defeitos`, ou `None` se não há registros."""
    if not linhas:
        return None
    return spark.createDataFrame(linhas, ESQUEMA)


def sem_controle(df: "DataFrame") -> "DataFrame":
    """O DataFrame sem as colunas internas (`_...`), pronto para serializar."""
    return df.select(*[c for c in df.columns if not c.startswith("_")])


__all__ = [
    "Alvo", "ANOTACAO", "COLUNAS_DEFEITOS", "Defeitos", "ESQUEMA", "registros", "sem_controle", "tabela_de_registros",
]
