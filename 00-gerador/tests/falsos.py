"""Sistemas e sessão falsos dos testes que não abrem sessão no workspace.

Os sistemas falsos (`falso_a` e `falso_b`) entregam um arquivo por dia, com bytes que dependem
só do sistema e do dia, e, se pedido, um defeito nos dias de número ímpar. São registrados em
`PROJECOES` só durante o teste (fixture `sistemas_falsos`, em `conftest.py`).
"""

from datetime import date, timedelta
from types import SimpleNamespace

import pyarrow as pa

from modelbricks_gerador import carregar_roteiro
from modelbricks_gerador.escritor import Arquivo, caminho_landing
from modelbricks_gerador.sistemas.base import Entrega, Lote

ARTIGO = "01-bronze"
INICIO = date(2024, 1, 1)
CARGA = date(2024, 1, 3)
SISTEMAS_FALSOS = ("falso_a", "falso_b")
SPARK_FALSO = SimpleNamespace(conf=SimpleNamespace(set=lambda *_: None))


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
        colunas = {nome: [linha[nome] for linha in self.linhas] for nome in self.linhas[0]}
        tipos = {"data_entrega": pa.date32(), "tabela_verdadeira": pa.string(), "id_verdadeiro": pa.int64(),
                 "seq": pa.int64(), "arquivo": pa.string()}
        return pa.table({n: pa.array(v, tipos.get(n)) for n, v in colunas.items()})


def roteiro_falso(gabarito: bool = False):
    """O roteiro oficial com a fatia 01-bronze trocada pelos sistemas falsos."""
    roteiro = carregar_roteiro()
    fatia = roteiro.fatia(ARTIGO).model_copy(update={"sistemas": list(SISTEMAS_FALSOS), "gabarito": gabarito})
    return roteiro.model_copy(update={"fatias": {**roteiro.fatias, ARTIGO: fatia}})
