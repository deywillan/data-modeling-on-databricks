"""Sistemas e sessão falsos dos testes que não abrem sessão no workspace.

Os sistemas falsos (`falso_a` e `falso_b`) entregam um arquivo por dia, com bytes que dependem
só do sistema e do dia, e, se pedido, um defeito nos dias de número ímpar. São registrados em
`PROJECOES` só durante o teste (fixture `sistemas_falsos`, em `conftest.py`), que também troca
o mundo verdadeiro por um vazio: a carga inicial grava o gabarito, e o mundo precisaria de Spark.
"""

from datetime import date, timedelta
from types import SimpleNamespace

import pyarrow as pa

from modelbricks_gerador import carregar_roteiro
from modelbricks_gerador.escritor import Arquivo, caminho_landing
from modelbricks_gerador.roteiro import Carga
from modelbricks_gerador.sistemas.base import Entrega, Lote

INICIO = date(2024, 1, 1)
CARGA = date(2024, 1, 3)
SISTEMAS_FALSOS = ("falso_a", "falso_b")
# A sessão falsa só fixa o fuso e cria a tabela vazia de defeitos da carga inicial.
SPARK_FALSO = SimpleNamespace(
    conf=SimpleNamespace(set=lambda *_: None),
    createDataFrame=lambda linhas, _esquema: FalsoDataFrame(list(linhas)),
)


def projecao_falsa(sistema: str, defeitos: bool = False):
    """Sistema falso: um arquivo por dia, com bytes que dependem só do sistema e do dia."""

    def projetar(contexto, inicio: date, fim: date) -> Entrega:
        dias = [inicio + timedelta(days=i) for i in range((fim - inicio).days + 1)]
        lotes = [
            Lote(
                sistema, "itens", dia, "json", 1,
                Arquivo(caminho_landing(sistema, "itens", dia, "json"), f'{{"dia": "{dia}"}}\n'.encode()),
            )
            for dia in dias
        ]
        tabela = FalsoDataFrame(
            [_defeito(sistema, dia) for dia in dias if dia.day % 2 == 1]
        ) if defeitos else None
        return Entrega(lotes=lotes, defeitos=[tabela] if tabela is not None else [])

    return projetar


def _defeito(sistema: str, dia: date) -> dict:
    return {
        "sistema": sistema, "entidade": "itens", "granularidade": "linha", "tipo": "cpf_fora_do_padrao",
        "coluna": "cpf", "chave_sistema": f"{dia:%Y%m%d}", "tabela_verdadeira": None,
        "id_verdadeiro": None, "valor_original": "a", "valor_sujo": "b", "data_entrega": dia, "seq": None,
        "arquivo": None,
    }


class FalsoDataFrame:
    """O mínimo de um DataFrame que `api._gravar_defeitos` e `DestinoLocal` usam."""

    def __init__(self, linhas: list[dict]):
        self.linhas = linhas

    def unionByName(self, outro):
        return FalsoDataFrame(self.linhas + outro.linhas)

    def select(self, *_):
        return self

    def orderBy(self, *_):
        return self

    def toArrow(self) -> pa.Table:
        nomes = self.linhas[0] if self.linhas else _defeito("", INICIO)
        colunas = {nome: [linha[nome] for linha in self.linhas] for nome in nomes}
        tipos = {"data_entrega": pa.date32(), "id_verdadeiro": pa.int64(), "seq": pa.int64()}
        return pa.table({n: pa.array(v, tipos.get(n, pa.string())) for n, v in colunas.items()})


def roteiro_falso():
    """O roteiro oficial com a carga trocada pelos sistemas falsos."""
    return carregar_roteiro().model_copy(update={"carga": Carga(sistemas=list(SISTEMAS_FALSOS))})
