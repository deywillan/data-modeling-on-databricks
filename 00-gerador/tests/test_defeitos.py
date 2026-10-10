"""Catálogo de defeitos observado pelo que `gerar_fatia`/`avancar_tempo` gravam.

O roteiro oficial só dá ao *Embarque* a taxa de CDC duplicado (como no Estudo de Caso).
Para exercitar os demais defeitos de linha pela mesma projeção, este módulo usa a geração
`legados` (ver `geracoes.py`), feita com a variante do roteiro de teste que dá ao *Embarque*
taxas para todos eles; a taxa observada na landing é comparada com a da variante, e cada
defeito observado com o `gabarito.defeitos`.
"""

import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime

import geracoes
import pytest

from modelbricks_gerador import DestinoLocal, carregar_roteiro

CHAVES = {"clientes": "id_cliente", "tarifas": "id_tarifa", "passagens": "id_passagem"}
TAXAS_DE_TESTE = geracoes.TAXAS_EMBARQUE


def _operacoes(destino: DestinoLocal) -> list[dict]:
    """Todas as linhas entregues pelo Embarque (reentregas inclusive), em ordem de `seq`."""
    operacoes = []
    for caminho in sorted((destino.volume / "embarque").rglob("*.json")):
        _, entidade, data_lote, _ = caminho.relative_to(destino.volume).as_posix().split("/")
        for linha in caminho.read_text(encoding="utf-8").splitlines():
            operacoes.append({**json.loads(linha), "_entidade": entidade, "_lote": date.fromisoformat(data_lote)})
    return sorted(operacoes, key=lambda o: (o["seq"], o["_lote"]))


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    """Tabelas do gabarito; `mapa_chaves` e `defeitos` só com as linhas do Embarque (a fatia
    tem outros sistemas)."""
    return geracoes.gabarito(destino, "embarque")


@pytest.fixture(scope="module")
def gerado(legados):
    """Embarque com todos os defeitos de linha, até 05/01/2026 (geração `legados`)."""
    roteiro = carregar_roteiro(legados.roteiro)
    assert {t: roteiro.defeitos[t].taxas["embarque"] for t in TAXAS_DE_TESTE} == TAXAS_DE_TESTE
    destino = legados.final
    return destino, _operacoes(destino), _gabarito(destino)


class _Observado:
    """O que a landing mostra de cada defeito, sem olhar o registro do gabarito."""

    def __init__(self, operacoes, gabarito):
        self.unicas = {}
        for op in operacoes:  # primeira entrega de cada seq; as seguintes são reentregas
            self.unicas.setdefault(op["seq"], op)
        self.reentregas = [o for o in operacoes if self.unicas[o["seq"]] is not o]
        mapa = {(m["coluna"], int(m["chave_sistema"])): m["id_verdadeiro"] for m in gabarito["mapa_chaves"]}
        pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
        ops = list(self.unicas.values())
        clientes = {o["id_cliente"]: o for o in ops if o["_entidade"] == "clientes"}
        self.clientes_originais = {k: pessoas[mapa[("id_cliente", k)]] for k in clientes if ("id_cliente", k) in mapa}
        self.duplicados = {k for k in clientes if ("id_cliente", k) not in mapa}
        self.clientes = clientes
        linhas = {o["cod_linha"] for o in ops if o["_entidade"] == "tarifas"}
        passagens = defaultdict(set)
        for o in ops:
            if o["_entidade"] == "passagens" and o["op"] != "D":
                passagens[o["id_passagem"]].add(o["cod_linha"])
        self.passagens = passagens
        self.orfas = {k for k, cods in passagens.items() if cods - linhas}
        self.atrasadas = {
            o["seq"] for o in ops if datetime.fromisoformat(o["ts_operacao"]).date() < o["_lote"]
        }
        self.ops = ops

    def por_tipo(self) -> dict[str, tuple[set, int]]:
        """tipo → (identificação de cada ocorrência observada, população elegível)."""
        originais = self.clientes_originais

        def diferentes(coluna):
            return {k for k, p in originais.items() if self.clientes[k][coluna] != p[coluna]}

        nulos = {k for k in originais if self.clientes[k].get("email") is None or self.clientes[k].get("telefone") is None}
        return {
            "cpf_fora_do_padrao": ({("clientes", k) for k in diferentes("cpf")}, len(originais)),
            "nome_abreviado_ou_com_erro": ({("clientes", k) for k in diferentes("nome")}, len(originais)),
            "contato_nulo": ({("clientes", k) for k in nulos}, len(originais)),
            "pessoa_duplicada_no_sistema": ({("clientes", k) for k in self.duplicados}, len(originais)),
            "fk_orfa": ({("passagens", k) for k in self.orfas}, len(self.passagens)),
            "evento_atrasado": ({("seq", s) for s in self.atrasadas}, len(self.ops)),
            "cdc_duplicado": ({("seq", o["seq"]) for o in self.reentregas}, len(self.ops)),
        }


def _registrados(gabarito, clientes_originais) -> dict[str, set]:
    """tipo → identificação de cada defeito registrado (na mesma forma de `_Observado`).

    Os defeitos de valor medidos em `_Observado` são os dos clientes originais; os que caem
    nas cópias duplicadas ficam de fora dos dois lados.
    """
    registrados = defaultdict(set)
    for r in gabarito["defeitos"]:
        assert r["sistema"] == "embarque" and r["granularidade"] == "linha"
        if r["tipo"] in ("evento_atrasado", "cdc_duplicado"):
            registrados[r["tipo"]].add(("seq", r["seq"]))
        elif r["tipo"] == "pessoa_duplicada_no_sistema":
            registrados[r["tipo"]].add((r["entidade"], int(r["valor_sujo"])))
        elif r["entidade"] != "clientes" or int(r["chave_sistema"]) in clientes_originais:
            registrados[r["tipo"]].add((r["entidade"], int(r["chave_sistema"])))
    return registrados


def _dentro_da_tolerancia(observados: int, populacao: int, taxa: float) -> bool:
    desvio = math.sqrt(taxa * (1 - taxa) / populacao)
    return abs(observados / populacao - taxa) <= 4 * desvio + 1 / populacao


def test_taxa_observada_de_cada_defeito_fica_na_tolerancia_do_roteiro(gerado):
    _, operacoes, gabarito = gerado
    observado = _Observado(operacoes, gabarito).por_tipo()

    assert set(observado) == set(TAXAS_DE_TESTE)
    for tipo, (ocorrencias, populacao) in observado.items():
        assert populacao > 300, f"{tipo}: população pequena demais para medir a taxa"
        assert ocorrencias, f"{tipo}: nenhum defeito observado"
        assert _dentro_da_tolerancia(len(ocorrencias), populacao, TAXAS_DE_TESTE[tipo]), (
            f"{tipo}: {len(ocorrencias)}/{populacao} longe da taxa {TAXAS_DE_TESTE[tipo]}"
        )


def test_todo_defeito_observavel_na_landing_tem_registro_no_gabarito(gerado):
    _, operacoes, gabarito = gerado
    observado = _Observado(operacoes, gabarito)
    registrados = _registrados(gabarito, set(observado.clientes_originais))
    artigos = {tipo: d.artigos for tipo, d in carregar_roteiro().defeitos.items()}

    for tipo, (ocorrencias, _) in observado.por_tipo().items():
        assert ocorrencias == registrados[tipo], f"{tipo}: landing e gabarito divergem"
    for r in gabarito["defeitos"]:
        assert list(r["artigos"]) == artigos[r["tipo"]]
        assert r["data_entrega"] is not None
    assert max(r["data_entrega"] for r in gabarito["defeitos"]) > date(2025, 12, 31), (
        "os defeitos dos lotes depois da carga de referência também são registrados"
    )


def test_defeitos_nunca_alteram_o_mundo_verdadeiro(gerado):
    _, operacoes, gabarito = gerado
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    colunas = {"cpf_fora_do_padrao": ["cpf"], "nome_abreviado_ou_com_erro": ["nome"], "contato_nulo": ["email", "telefone"]}

    conferidos = 0
    for r in gabarito["defeitos"]:
        if r["tipo"] in colunas:
            assert r["tabela_verdadeira"] == "pessoas"
            assert r["coluna"] in colunas[r["tipo"]]
            verdadeiro = pessoas[r["id_verdadeiro"]][r["coluna"]]
            assert verdadeiro == r["valor_original"] and verdadeiro != r["valor_sujo"]
            conferidos += 1
    assert conferidos
    assert all(len(p["cpf"]) == 11 and p["cpf"].isdigit() and p["email"] and p["telefone"] for p in pessoas.values())


def test_semantica_de_cdc_so_quebra_nas_reentregas_e_atrasos_registrados(gerado):
    _, operacoes, gabarito = gerado
    reentregues = {r["seq"] for r in gabarito["defeitos"] if r["tipo"] == "cdc_duplicado"}
    atrasadas = {r["seq"] for r in gabarito["defeitos"] if r["tipo"] == "evento_atrasado"}

    contagem = Counter(o["seq"] for o in operacoes)
    assert {s for s, n in contagem.items() if n > 1} == reentregues
    unicas = {}
    for op in operacoes:
        unicas.setdefault(op["seq"], op)
    for op in operacoes:
        if unicas[op["seq"]] is not op:
            assert {k: v for k, v in op.items() if k != "_lote"} == {k: v for k, v in unicas[op["seq"]].items() if k != "_lote"}
            assert op["_lote"] > unicas[op["seq"]]["_lote"], "reentrega chega num lote posterior"

    # Aplicadas em ordem de seq, as operações só saem da ordem do tempo nas atrasadas.
    por_chave = defaultdict(list)
    for op in unicas.values():
        por_chave[(op["_entidade"], op[CHAVES[op["_entidade"]]])].append(op)
    invertidas = set()
    for ops in por_chave.values():
        for anterior, posterior in zip(ops, ops[1:]):
            if posterior["ts_operacao"] < anterior["ts_operacao"]:
                invertidas.add(posterior["seq"])  # seq maior que a de uma operação posterior
    assert invertidas, "algum atraso inverte a ordem de uma chave"
    assert invertidas <= atrasadas
    for op in unicas.values():
        dia = datetime.fromisoformat(op["ts_operacao"]).date()
        assert dia == op["_lote"] or op["seq"] in atrasadas


def test_fks_orfas_aparecem_so_na_taxa_injetada(gerado):
    _, operacoes, gabarito = gerado
    observado = _Observado(operacoes, gabarito)
    registradas = {int(r["chave_sistema"]) for r in gabarito["defeitos"] if r["tipo"] == "fk_orfa"}

    assert observado.orfas == registradas
    assert _dentro_da_tolerancia(len(observado.orfas), len(observado.passagens), TAXAS_DE_TESTE["fk_orfa"])
