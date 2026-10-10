"""Mundo verdadeiro do Grupo ModelBricks: as entidades mestres com as chaves verdadeiras.

Tudo o que os sistemas de origem entregam é uma projeção deste mundo. Ele é gerado inteiro
em toda fatia (os sistemas só escolhem o que enxergam) e, nas fatias com gabarito, gravado
no schema `gabarito`. Nunca vai para a landing.

Interface: `gerar_mundo(spark, roteiro, escala) -> Mundo`. O `Mundo` é um mapeamento
nome → DataFrame (lazy) com as tabelas de `TABELAS`, e `Mundo.ordenado(nome)` devolve a
tabela em ordem total pela chave, pronta para coleta ou gravação.

| Tabela | Chave | Conteúdo |
|---|---|---|
| `municipios` | `cod_municipio` | Municípios, UFs e regiões do IBGE (arquivo versionado) |
| `pessoas` | `id_pessoa` | Pessoas físicas (CPF, nome, nascimento, contato, endereço) |
| `empresas_pj` | `id_empresa` | Empresas clientes (CNPJ, razão social, contato, endereço) |
| `veiculos` | `id_veiculo` | Ônibus e caminhões (placa, marca, modelo); `empresa_frota` na frota própria |
| `vendas_veiculos` | `id_venda` | Venda de cada veículo pela ModelBricks Veículos (ao grupo, a PJ ou a PF) |
| `rotas` | `id_rota` | Linhas de ônibus da Viação e do Expresso Vale do Sol entre municípios |
| `bases` | `id_base` | Garagens e centros de distribuição |
| `lojas` | `id_loja` | Lojas da concessionária e da corretora |
| `colaboradores` | `id_colaborador` | Vínculo de uma pessoa com uma empresa do grupo (cargo, admissão) |
| `participacoes` | `tipo_entidade, id_entidade, empresa, papel` | Quem é cliente de quem (`papel='cliente'`) e a frota própria (`papel='frota'`) |

Regras de geração (números no `roteiro.yaml`):

- dbldatagen (`dbldatagen.core`) com seed explícito por tabela, derivado de (seed base,
  tabela): a ordem das tabelas no plano não altera os dados.
- Faker `pt_BR` só no driver, em pools deduplicados (`dsl.text(values=...)`); nenhuma UDF.
- CPF, CNPJ e placa saem de expressão SQL bijetiva sobre o id: únicos e válidos.
- Sorteios feitos depois do plano usam `xxhash64(semente, id)`: determinísticos e
  independentes de particionamento.
"""

import csv
import io
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from importlib import resources
from typing import TYPE_CHECKING

from .expressoes import uniforme
from .roteiro import Roteiro
from .sementes import semente

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

CHAVES: dict[str, tuple[str, ...]] = {
    "municipios": ("cod_municipio",),
    "pessoas": ("id_pessoa",),
    "empresas_pj": ("id_empresa",),
    "veiculos": ("id_veiculo",),
    "vendas_veiculos": ("id_venda",),
    "rotas": ("id_rota",),
    "bases": ("id_base",),
    "lojas": ("id_loja",),
    "colaboradores": ("id_colaborador",),
    "participacoes": ("tipo_entidade", "id_entidade", "empresa", "papel"),
}
TABELAS = tuple(CHAVES)

# Multiplicador ímpar, primo com 2, 5 e 13: `id * A + B` módulo 10^k (ou módulo o espaço de
# placas) é uma bijeção, o que garante CPF, CNPJ e placa únicos para ids distintos.
_MULTIPLICADOR = 387420489  # 3^18
_ESPACO_PLACAS = 26**4 * 10**3  # LLL N L NN (padrão Mercosul)
_COM_ACENTO = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_SEM_ACENTO = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


@dataclass(frozen=True)
class Mundo(Mapping):
    """Tabelas do mundo verdadeiro, por nome (ver `TABELAS`)."""

    tabelas: Mapping[str, "DataFrame"]

    def __getitem__(self, nome: str) -> "DataFrame":
        return self.tabelas[nome]

    def __iter__(self) -> Iterator[str]:
        return iter(TABELAS)

    def __len__(self) -> int:
        return len(TABELAS)

    def ordenado(self, nome: str) -> "DataFrame":
        """A tabela em ordem total pela chave (para coletar ou gravar de forma determinística)."""
        return self.tabelas[nome].orderBy(*CHAVES[nome])


def linhas(volume: int, escala: float) -> int:
    """Volume da escala 1 multiplicado linearmente pela escala (no mínimo 1 linha)."""
    return max(1, round(volume * escala))


def gerar_mundo(
    spark: "SparkSession", roteiro: Roteiro, escala: float,
    materializar: "Callable[[DataFrame], DataFrame] | None" = None,
) -> Mundo:
    """Monta todas as tabelas do mundo verdadeiro para a escala.

    As tabelas são lazy. Com `materializar`, as tabelas geradas pelo dbldatagen (a base de
    todas as outras) são calculadas uma vez e as derivadas (participações, vendas) saem delas,
    em vez de repetir o plano do dbldatagen a cada tabela."""
    return _Gerador(spark, roteiro, escala, materializar).mundo()


class _Gerador:
    def __init__(
        self, spark: "SparkSession", roteiro: Roteiro, escala: float,
        materializar: "Callable[[DataFrame], DataFrame] | None" = None,
    ):
        self.spark = spark
        self.materializar = materializar
        self.roteiro = roteiro
        self.m = roteiro.mundo
        self.c = roteiro.catalogos
        v = roteiro.volumes
        self.n = {
            "pessoas": linhas(v.pessoas, escala),
            "empresas_pj": linhas(v.empresas_pj, escala),
            "veiculos": linhas(v.veiculos, escala),
            "frota": linhas(v.veiculos_frota_propria, escala),
            "vendas": linhas(v.vendas_veiculos, escala),
            "rotas": self.com_minimo(linhas(v.rotas, escala), self.m.rotas.empresas),
            "bases": self.com_minimo(linhas(v.bases, escala), self.m.bases),
            "lojas": self.com_minimo(linhas(v.lojas, escala), self.m.lojas),
            "colaboradores": self.com_minimo(linhas(v.colaboradores, escala), self.m.colaboradores),
        }
        # A escala arredonda cada volume: mantém as relações que o roteiro garante.
        self.n["vendas"] = min(max(self.n["vendas"], self.n["frota"]), self.n["veiculos"])
        self.n["colaboradores"] = min(self.n["colaboradores"], self.n["pessoas"])

    # -- utilitários ---------------------------------------------------------------------

    def com_minimo(self, total: int, pesos: dict[str, float]) -> int:
        """Total de instâncias de uma população distribuída entre empresas, com o mínimo por
        empresa do roteiro (`mundo.minimo_por_empresa`): numa escala pequena, nenhuma empresa
        fica sem instância (ex.: a Viação sem rotas)."""
        return max(total, self.m.minimo_por_empresa * len(pesos))

    def coluna_empresa(self, coluna_id: str, pesos: dict[str, float]) -> list:
        """Colunas de `empresa`: sorteio por pesos, e as primeiras instâncias fixadas, uma
        rodada por empresa (em ordem alfabética), até cumprir o mínimo por empresa."""
        from dbldatagen.core.spec import dsl
        from dbldatagen.core.spec.schema import WeightedValues

        empresas = sorted(pesos)
        garantidas = self.m.minimo_por_empresa * len(empresas)
        return [
            dsl.text("_empresa_sorteada", empresas, distribution=WeightedValues(weights=pesos)),
            dsl.expression(
                "empresa",
                f"CASE WHEN {coluna_id} <= {garantidas} "
                f"THEN element_at(array({', '.join(map(_literal, empresas))}), "
                f"cast(pmod({coluna_id} - 1, {len(empresas)}) + 1 AS int)) "
                "ELSE _empresa_sorteada END",
            ),
        ]

    def semente(self, *partes: object) -> int:
        return semente(self.roteiro.seed_base, "mundo", *partes)

    def sorteio(self, nome: str, coluna_id: str) -> str:
        """Expressão SQL uniforme em [0, 1), determinística por (nome, id)."""
        return uniforme(self.semente(nome), coluna_id)

    def pool(self, provedor: str) -> list[str]:
        """Valores distintos do Faker pt_BR (gerados no driver), só os representáveis em latin1."""
        from faker import Faker

        faker = Faker("pt_BR")
        faker.seed_instance(self.semente("pool", provedor))
        metodo = getattr(faker, provedor)
        valores = dict.fromkeys(str(metodo()).strip() for _ in range(self.m.tamanho_pools))
        return [v for v in valores if v and _latin1(v)]

    # -- montagem ------------------------------------------------------------------------

    def mundo(self) -> Mundo:
        from dbldatagen.core import DataGenPlan, generate

        municipios = self.municipios()
        self.codigos = [str(c) for c in municipios.codigos_atuacao]
        especificacoes = [
            self.pessoas(), self.empresas_pj(), self.veiculos(),
            self.rotas(), self.bases(), self.lojas(), self.colaboradores(),
        ]
        plano = DataGenPlan(tables=especificacoes, seed=self.roteiro.seed_base, default_locale="pt_BR")
        gerado = generate(self.spark, plano)
        if self.materializar is not None:
            gerado = {nome: self.materializar(df) for nome, df in gerado.items()}

        df_municipios = municipios.df
        pessoas, empresas = gerado["pessoas"], gerado["empresas_pj"]
        participacoes_clientes = self.participacoes_clientes(pessoas, empresas)
        vendas = self.vendas(gerado["veiculos"], participacoes_clientes, gerado["lojas"])
        frota = vendas.where("tipo_comprador = 'grupo'").selectExpr(
            "'veiculo' AS tipo_entidade", "id_veiculo AS id_entidade",
            "empresa_compradora AS empresa", "'frota' AS papel", "data_venda AS desde",
        )
        tabelas = {
            "municipios": df_municipios,
            "pessoas": pessoas.drop(*_auxiliares(pessoas)),
            "empresas_pj": empresas.drop(*_auxiliares(empresas)),
            "veiculos": gerado["veiculos"].drop(*_auxiliares(gerado["veiculos"])),
            "vendas_veiculos": vendas,
            "rotas": gerado["rotas"].drop(*_auxiliares(gerado["rotas"])),
            "bases": self.com_nome(gerado["bases"], df_municipios, "tipos_base"),
            "lojas": self.com_nome(gerado["lojas"], df_municipios, "nomes_loja"),
            "colaboradores": gerado["colaboradores"].drop(*_auxiliares(gerado["colaboradores"])),
            "participacoes": participacoes_clientes.unionByName(frota),
        }
        return Mundo(tabelas)

    def municipios(self) -> "_Municipios":
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        texto = resources.files(__package__).joinpath("dados/ibge_municipios.csv").read_text(encoding="utf-8")
        registros = list(csv.DictReader(io.StringIO(texto)))
        inteiros = {"cod_municipio", "cod_uf", "cod_regiao", "cod_regiao_imediata", "cod_regiao_intermediaria"}
        colunas = list(registros[0])
        esquema = StructType(
            [StructField(c, IntegerType() if c in inteiros else StringType(), False) for c in colunas]
        )
        dados = [tuple(int(r[c]) if c in inteiros else r[c] for c in colunas) for r in registros]
        ufs = set(self.m.ufs_atuacao)
        desconhecidas = ufs - {r["sigla_uf"] for r in registros}
        if desconhecidas:
            raise ValueError(f"mundo.ufs_atuacao com UF inexistente no IBGE: {sorted(desconhecidas)}")
        codigos = sorted(int(r["cod_municipio"]) for r in registros if r["sigla_uf"] in ufs)
        return _Municipios(self.spark.createDataFrame(dados, esquema), codigos)

    # -- tabelas do plano ----------------------------------------------------------------

    def tabela(self, nome: str, quantidade: int, colunas: list) -> object:
        from dbldatagen.core import PrimaryKey, TableSpec

        return TableSpec(
            name=nome, rows=quantidade, seed=self.semente("tabela", nome),
            primary_key=PrimaryKey(columns=[colunas[0].name]), columns=colunas,
        )

    def contato_e_endereco(self, email: str) -> list:
        """Colunas de telefone, e-mail, município e endereço (comuns a pessoas e empresas).

        `email` é uma expressão SQL que pode usar `_dominio` (um provedor de e-mail sorteado).
        """
        from dbldatagen.core.spec import dsl

        return [
            dsl.text("_ddd", [str(d) for d in self.c.ddds]),
            dsl.pattern("_telefone", "{digit:4}{digit:4}"),
            dsl.expression("telefone", "concat('(', _ddd, ') 9', substr(_telefone, 1, 4), '-', substr(_telefone, 5, 4))"),
            dsl.text("_dominio", self.c.dominios_email),
            dsl.expression("email", email),
            dsl.text("_municipio", self.codigos),
            dsl.expression("cod_municipio", "cast(_municipio AS int)"),
            dsl.text("logradouro", self.pool("street_name")),
            dsl.integer("numero_endereco", min=1, max=3999),
            dsl.pattern("cep", "{digit:5}-{digit:3}"),
        ]

    def participacao(self, pesos: dict[str, float], taxa_multi: float) -> list:
        """Colunas auxiliares: empresa principal, segunda empresa (quando houver) e datas."""
        from dbldatagen.core.spec import dsl
        from dbldatagen.core.spec.schema import WeightedValues

        empresas = sorted(pesos)
        outras = f"array_remove(array({', '.join(map(_literal, empresas))}), _empresa_principal)"
        inicio, fim = self.roteiro.datas.inicio.isoformat(), self.roteiro.datas.carga_inicial.isoformat()
        return [
            dsl.text("_empresa_principal", empresas, distribution=WeightedValues(weights=pesos)),
            dsl.double("_u_multi", min=0.0, max=1.0),
            dsl.integer("_k_segunda", min=0, max=999999),
            dsl.expression(
                "_segunda_empresa",
                f"CASE WHEN _u_multi < {taxa_multi} "
                f"THEN element_at({outras}, cast(pmod(_k_segunda, {len(empresas) - 1}) + 1 AS int)) END",
            ),
            dsl.timestamp("_desde_principal", start=inicio, end=fim),
            dsl.timestamp("_desde_segunda", start=inicio, end=fim),
        ]

    def pessoas(self):
        from dbldatagen.core.spec import dsl

        idade = self.m.idade_pessoas
        nascimento_min = _anos_antes(self.roteiro.datas.carga_inicial, idade.maximo)
        nascimento_max = _anos_antes(self.roteiro.datas.inicio, idade.minimo)
        email = (
            f"concat({_slug('_primeiro_nome')}, '.', {_slug('_sobrenome_final')}, "
            "cast(_n_email AS string), '@', _dominio)"
        )
        colunas = [
            dsl.pk_auto("id_pessoa"),
            *self.cpf("id_pessoa"),
            dsl.text("sexo", ["F", "M"]),
            dsl.text("_nome_f", self.pool("first_name_female")),
            dsl.text("_nome_m", self.pool("first_name_male")),
            dsl.expression("_primeiro_nome", "CASE WHEN sexo = 'F' THEN _nome_f ELSE _nome_m END"),
            dsl.text("_sobrenome_meio", self.pool("last_name")),
            dsl.text("_sobrenome_final", self.pool("last_name")),
            dsl.expression("nome", "concat_ws(' ', _primeiro_nome, _sobrenome_meio, _sobrenome_final)"),
            dsl.timestamp("_nascimento", start=nascimento_min.isoformat(), end=nascimento_max.isoformat()),
            dsl.expression("data_nascimento", "cast(_nascimento AS date)"),
            dsl.integer("_n_email", min=1, max=999),
            *self.contato_e_endereco(email),
            *self.participacao(
                self.m.participacao.pessoas, self.roteiro.parametros.pessoas_em_mais_de_uma_empresa
            ),
        ]
        return self.tabela("pessoas", self.n["pessoas"], colunas)

    def empresas_pj(self):
        from dbldatagen.core.spec import dsl

        abertura = self.m.abertura_empresas
        colunas = [
            dsl.pk_auto("id_empresa"),
            *self.cnpj("id_empresa"),
            dsl.text("_nome", self.pool("last_name")),
            dsl.text("_ramo", self.c.ramos_empresa),
            dsl.text("_sufixo", self.pool("company_suffix")),
            dsl.expression("razao_social", "concat_ws(' ', _nome, _ramo, _sufixo)"),
            dsl.timestamp("_abertura", start=abertura.inicio.isoformat(), end=abertura.fim.isoformat()),
            dsl.expression("data_abertura", "cast(_abertura AS date)"),
            *self.contato_e_endereco(
                f"concat('contato@', {_slug('_nome')}, {_slug('_ramo')}, substr(cnpj, 1, 4), '.com.br')"
            ),
            *self.participacao(
                self.m.participacao.empresas_pj,
                self.m.participacao.empresas_pj_em_mais_de_uma_empresa,
            ),
        ]
        return self.tabela("empresas_pj", self.n["empresas_pj"], colunas)

    def veiculos(self):
        from dbldatagen.core.spec import dsl

        modelos = self.c.modelos_veiculos
        ano = self.m.ano_fabricacao_veiculos

        def atributo(nome: str) -> str:
            return f"element_at(array({', '.join(_literal(getattr(m, nome)) for m in modelos)}), _modelo + 1)"

        colunas = [
            dsl.pk_auto("id_veiculo"),
            *self.placa("id_veiculo"),
            dsl.integer("_modelo", min=0, max=len(modelos) - 1),
            dsl.expression("marca", atributo("marca")),
            dsl.expression("modelo", atributo("modelo")),
            dsl.expression("tipo", atributo("tipo")),
            dsl.integer("ano_fabricacao", min=ano.minimo, max=ano.maximo),
            dsl.expression("frota_propria", f"id_veiculo <= {self.n['frota']}"),
            dsl.expression(
                "empresa_frota",
                "CASE WHEN frota_propria THEN (CASE WHEN tipo = 'onibus' THEN 'mb_viacao' ELSE 'mb_cargas' END) END",
            ),
            dsl.expression(
                "_preco_minimo", f"element_at(array({', '.join(str(m.preco_minimo) for m in modelos)}), _modelo + 1)"
            ),
            dsl.expression(
                "_preco_maximo", f"element_at(array({', '.join(str(m.preco_maximo) for m in modelos)}), _modelo + 1)"
            ),
        ]
        return self.tabela("veiculos", self.n["veiculos"], colunas)

    def rotas(self):
        from dbldatagen.core.spec import dsl

        r = self.m.rotas
        return self.tabela("rotas", self.n["rotas"], [
            dsl.pk_auto("id_rota"),
            *self.coluna_empresa("id_rota", r.empresas),
            dsl.text("_origem", self.codigos),
            dsl.text("_destino", self.codigos),
            dsl.text("_destino_alternativo", self.codigos),
            dsl.expression("cod_municipio_origem", "cast(_origem AS int)"),
            dsl.expression(
                "cod_municipio_destino",
                "cast(CASE WHEN _destino != _origem THEN _destino ELSE _destino_alternativo END AS int)",
            ),
            dsl.integer("distancia_km", min=r.distancia_km.minimo, max=r.distancia_km.maximo),
            dsl.expression("tarifa_base", f"cast(round(distancia_km * {r.tarifa_por_km}, 2) AS decimal(10,2))"),
        ])

    def bases(self):
        return self._local("bases", "id_base", self.m.bases)

    def lojas(self):
        return self._local("lojas", "id_loja", self.m.lojas)

    def _local(self, nome: str, chave: str, pesos: dict[str, float]):
        from dbldatagen.core.spec import dsl

        return self.tabela(nome, self.n[nome], [
            dsl.pk_auto(chave),
            *self.coluna_empresa(chave, pesos),
            dsl.text("_municipio", self.codigos),
            dsl.expression("cod_municipio", "cast(_municipio AS int)"),
        ])

    def colaboradores(self):
        from dbldatagen.core.spec import dsl

        pesos = self.m.colaboradores
        admissao = self.m.admissao_colaboradores
        cargo = " ".join(
            f"WHEN {_literal(empresa)} THEN element_at(array({', '.join(map(_literal, cargos))}), "
            f"cast(pmod(_k_cargo, {len(cargos)}) + 1 AS int))"
            for empresa, cargos in sorted(self.c.cargos.items())
            if empresa in pesos
        )
        return self.tabela("colaboradores", self.n["colaboradores"], [
            dsl.pk_auto("id_colaborador"),
            # As últimas pessoas do mundo são os colaboradores: uma pessoa, um vínculo.
            dsl.expression("id_pessoa", f"cast({self.n['pessoas']} - id_colaborador + 1 AS long)"),
            *self.coluna_empresa("id_colaborador", pesos),
            dsl.integer("_k_cargo", min=0, max=999999),
            dsl.expression("cargo", f"CASE empresa {cargo} END"),
            dsl.timestamp("_admissao", start=admissao.inicio.isoformat(), end=admissao.fim.isoformat()),
            dsl.expression("data_admissao", "cast(_admissao AS date)"),
        ])

    # -- documentos (expressões SQL bijetivas sobre o id) ---------------------------------

    def cpf(self, coluna_id: str) -> list:
        from dbldatagen.core.spec import dsl

        base = f"lpad(cast(pmod({coluna_id} * {_MULTIPLICADOR} + {self.semente('cpf')}, 1000000000) AS string), 9, '0')"
        return [
            dsl.expression("_cpf_base", base),
            dsl.expression("_cpf_dv1", _digito_verificador("_cpf_base", range(10, 1, -1))),
            dsl.expression("_cpf_dv2", _digito_verificador("concat(_cpf_base, _cpf_dv1)", range(11, 1, -1))),
            dsl.expression("cpf", "concat(_cpf_base, _cpf_dv1, _cpf_dv2)"),
        ]

    def cnpj(self, coluna_id: str) -> list:
        from dbldatagen.core.spec import dsl

        raiz = f"lpad(cast(pmod({coluna_id} * {_MULTIPLICADOR} + {self.semente('cnpj')}, 100000000) AS string), 8, '0')"
        pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos2 = [6, *pesos1]
        return [
            dsl.expression("_cnpj_base", f"concat({raiz}, '0001')"),
            dsl.expression("_cnpj_dv1", _digito_verificador("_cnpj_base", pesos1)),
            dsl.expression("_cnpj_dv2", _digito_verificador("concat(_cnpj_base, _cnpj_dv1)", pesos2)),
            dsl.expression("cnpj", "concat(_cnpj_base, _cnpj_dv1, _cnpj_dv2)"),
        ]

    def placa(self, coluna_id: str) -> list:
        from dbldatagen.core.spec import dsl

        n = f"pmod({coluna_id} * {_MULTIPLICADOR} + {self.semente('placa')}, {_ESPACO_PLACAS})"

        def letra(divisor: int) -> str:
            return f"char(65 + cast(pmod(div(_placa_n, {divisor}), 26) AS int))"

        def digito(divisor: int, modulo: int = 10) -> str:
            return f"cast(pmod(div(_placa_n, {divisor}), {modulo}) AS string)"

        # Da direita para a esquerda: N N L N L L L, com bases 10, 10, 26, 10, 26, 26, 26.
        return [
            dsl.expression("_placa_n", f"cast({n} AS long)"),
            dsl.expression(
                "placa",
                "concat("
                f"{letra(26 * 26 * 10 * 26 * 100)}, {letra(26 * 10 * 26 * 100)}, {letra(10 * 26 * 100)}, "
                f"{digito(26 * 100)}, {letra(100)}, {digito(10)}, {digito(1)})",
            ),
        ]

    # -- tabelas derivadas -----------------------------------------------------------------

    def participacoes_clientes(self, pessoas: "DataFrame", empresas: "DataFrame") -> "DataFrame":
        partes = []
        for df, tipo, chave in ((pessoas, "pessoa", "id_pessoa"), (empresas, "empresa_pj", "id_empresa")):
            for empresa, desde in (("_empresa_principal", "_desde_principal"), ("_segunda_empresa", "_desde_segunda")):
                partes.append(
                    df.where(f"{empresa} IS NOT NULL").selectExpr(
                        f"'{tipo}' AS tipo_entidade", f"{chave} AS id_entidade", f"{empresa} AS empresa",
                        "'cliente' AS papel", f"cast({desde} AS date) AS desde",
                    )
                )
        resultado = partes[0]
        for parte in partes[1:]:
            resultado = resultado.unionByName(parte)
        return resultado

    def vendas(self, veiculos: "DataFrame", participacoes: "DataFrame", lojas: "DataFrame") -> "DataFrame":
        """Uma venda por veículo vendido: a frota própria para o grupo, o resto para clientes
        (PJ ou PF) da ModelBricks Veículos, sempre por uma loja da concessionária."""
        datas = self.roteiro.datas
        dias = (datas.carga_inicial - datas.inicio).days
        clientes = participacoes.where("papel = 'cliente' AND empresa = 'mb_veiculos'")
        base = veiculos.where(f"id_veiculo <= {self.n['vendas']}").selectExpr(
            "id_veiculo AS id_venda", "id_veiculo", "frota_propria", "empresa_frota",
            "_preco_minimo", "_preco_maximo",
        )
        base = _sortear(base, "id_venda", clientes.where("tipo_entidade = 'empresa_pj'"), "id_entidade",
                        "_comprador_pj", self.semente("vendas", "pj"))
        base = _sortear(base, "id_venda", clientes.where("tipo_entidade = 'pessoa'"), "id_entidade",
                        "_comprador_pf", self.semente("vendas", "pf"))
        base = _sortear(base, "id_venda", lojas.where("empresa = 'mb_veiculos'"), "id_loja",
                        "id_loja", self.semente("vendas", "loja"))
        tipo = (
            "CASE WHEN frota_propria THEN 'grupo' "
            f"WHEN {self.sorteio('vendas_pj', 'id_venda')} < {self.m.vendas_veiculos_para_pj} "
            "AND _comprador_pj IS NOT NULL THEN 'pj' "
            "WHEN _comprador_pf IS NOT NULL THEN 'pf' ELSE 'pj' END"
        )
        return base.selectExpr("*", f"{tipo} AS tipo_comprador").selectExpr(
            "id_venda",
            "id_veiculo",
            "'mb_veiculos' AS empresa_vendedora",
            "id_loja",
            f"date_add(DATE'{datas.inicio.isoformat()}', "
            f"cast(pmod(xxhash64({self.semente('vendas', 'data')}, id_venda), {dias + 1}) AS int)) AS data_venda",
            "tipo_comprador",
            "CASE WHEN tipo_comprador = 'grupo' THEN empresa_frota END AS empresa_compradora",
            "CASE WHEN tipo_comprador = 'pj' THEN _comprador_pj END AS id_empresa",
            "CASE WHEN tipo_comprador = 'pf' THEN _comprador_pf END AS id_pessoa",
            f"cast(round(_preco_minimo + (_preco_maximo - _preco_minimo) * "
            f"{self.sorteio('vendas_valor', 'id_venda')}, 2) AS decimal(14,2)) AS valor_venda",
        )

    def com_nome(self, locais: "DataFrame", municipios: "DataFrame", catalogo: str) -> "DataFrame":
        prefixos = getattr(self.c, catalogo)
        prefixo = " ".join(f"WHEN {_literal(e)} THEN {_literal(p)}" for e, p in sorted(prefixos.items()))
        chave = locais.columns[0]
        return (
            locais.drop(*_auxiliares(locais))
            .join(municipios.select("cod_municipio", "nome_municipio", "sigla_uf"), "cod_municipio", "left")
            .selectExpr(
                chave, "empresa",
                f"concat(CASE empresa {prefixo} END, ' ', nome_municipio, '/', sigla_uf) AS nome",
                "cod_municipio",
            )
        )


@dataclass(frozen=True)
class _Municipios:
    df: "DataFrame"
    codigos_atuacao: list[int]


def _sortear(origem, chave_origem: str, candidatos, id_candidato: str, saida: str, semente_do_sorteio: int):
    """Para cada linha da origem, sorteia (uniforme e determinístico) um id dos candidatos.

    Sem candidatos, a coluna de saída fica nula.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Janela sem partição de propósito (o Spark avisa): os candidatos são poucos (milhares)
    # e a numeração precisa ser global para o sorteio ser determinístico.
    indexados = candidatos.select(
        F.col(id_candidato).alias(saida),
        (F.row_number().over(Window.orderBy(id_candidato)) - 1).alias("_indice"),
    )
    total = candidatos.agg(F.count(F.lit(1)).alias("_total"))
    sorteada = origem.crossJoin(total).withColumn(
        "_indice", F.expr(f"pmod(xxhash64({semente_do_sorteio}, {chave_origem}), greatest(_total, 1))")
    )
    return sorteada.join(indexados, "_indice", "left").drop("_indice", "_total")


def _digito_verificador(digitos: str, pesos) -> str:
    """Dígito verificador módulo 11 (CPF/CNPJ) em SQL: soma ponderada dos dígitos."""
    soma = " + ".join(f"cast(substr({digitos}, {i}, 1) AS int) * {p}" for i, p in enumerate(pesos, start=1))
    return f"cast(CASE WHEN pmod({soma}, 11) < 2 THEN 0 ELSE 11 - pmod({soma}, 11) END AS string)"


def _slug(coluna: str) -> str:
    """Texto em minúsculas, sem acento e só com letras e dígitos (para e-mails)."""
    return f"regexp_replace(lower(translate({coluna}, '{_COM_ACENTO}', '{_SEM_ACENTO}')), '[^a-z0-9]', '')"


def _literal(texto: str) -> str:
    return "'" + str(texto).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _latin1(texto: str) -> bool:
    try:
        texto.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return True


def _auxiliares(df: "DataFrame") -> list[str]:
    return [c for c in df.columns if c.startswith("_")]


def _anos_antes(data: date, anos: int) -> date:
    try:
        return data.replace(year=data.year - anos)
    except ValueError:  # 29/02 em ano não bissexto
        return data.replace(year=data.year - anos, day=28)


__all__ = ["CHAVES", "TABELAS", "Mundo", "gerar_mundo", "linhas"]
