"""Fatias sem gabarito ficam sem defeitos (decisão do autor de 10/10/2026).

O catálogo de defeitos só vale nas fatias com gabarito (03, 05, 06 e 10), porque só nelas há
`gabarito.defeitos` para conferir a correção. Nas demais (01, 02, 04, 07, 08 e 09) os arquivos
saem limpos. A regra é a flag `gabarito` da fatia, sem lista de artigos no código.

A geração `limpa` tem os sistemas da `principal` (mais o Pátio) numa fatia sem gabarito, na mesma
escala e na mesma carga inicial: a mesma história, só que sem defeitos. Cada teste confere um
defeito que a `principal` tem (o contraste) e a `limpa` não.
"""

import csv
import io
import json
import re
from datetime import timezone

import geracoes
import pyarrow.parquet as pq
import pytest

MONETARIAS = ("importancia_segurada", "premio")


def _linhas_json(destino, sistema: str) -> list[dict]:
    return [
        json.loads(linha)
        for caminho in destino.listar(sistema)
        for linha in destino.ler(caminho).decode("utf-8").splitlines()
    ]


def _linhas_csv(destino, caminho: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(destino.ler(caminho).decode("utf-8"))))


def test_a_fatia_sem_gabarito_nao_tem_gabarito(limpa):
    destino = limpa.final

    assert not destino.pasta_gabarito().exists()
    assert not any("gabarito" in c or "defeito" in c for c in destino.listar(""))


def test_embarque_sem_reentrega_de_cdc_e_com_as_mesmas_operacoes_da_fatia_com_defeitos(limpa, principal):
    limpo = [o["seq"] for o in _linhas_json(limpa.final, "embarque")]
    sujo = [o["seq"] for o in _linhas_json(principal.etapas[0], "embarque")]

    assert len(sujo) > len(set(sujo)), "contraste: a fatia com gabarito tem reentregas"
    assert len(limpo) == len(set(limpo)), "a fatia sem gabarito não repete nenhuma operação"
    assert set(limpo) == set(sujo), "as mesmas operações, uma vez cada"


def test_nucleo_sem_evento_atrasado(limpa, principal):
    def atrasadas(destino) -> int:
        total = 0
        for caminho in destino.listar("nucleo"):
            lote = caminho.split("/")[2]
            for linha in pq.read_table(io.BytesIO(destino.ler(caminho))).to_pylist():
                total += linha["updated_at"].astimezone(timezone.utc).date().isoformat() != lote
        return total

    assert atrasadas(principal.etapas[0]) > 0, "contraste: a fatia com gabarito tem eventos atrasados"
    assert atrasadas(limpa.final) == 0


def test_passagemhub_com_nome_e_contato_da_pessoa_verdadeira(limpa, principal):
    gabarito = geracoes.gabarito(principal.etapas[0], "passagemhub")
    pessoas = {p["id_pessoa"]: p["nome"].upper() for p in gabarito["pessoas"]}
    clientes = {
        m["chave_sistema"]: m["id_verdadeiro"] for m in gabarito["mapa_chaves"] if m["coluna"] == "id_cliente"
    }

    def eventos(destino) -> list[dict]:
        return [
            e for caminho in destino.listar("passagemhub")
            for e in json.loads(destino.ler(caminho))["eventos"]
        ]

    def sujos(destino) -> int:
        return sum(
            e["comprador"]["nome"] != pessoas[clientes[e["comprador"]["id_cliente"]]]
            or e["comprador"]["contato"].get("email") is None
            or e["comprador"]["contato"].get("telefone") is None
            for e in eventos(destino)
        )

    assert sujos(principal.etapas[0]) > 0, "contraste: a fatia com gabarito tem nome e contato sujos"
    assert len(eventos(limpa.final)) > 0
    assert sujos(limpa.final) == 0


def test_patio_com_contato_e_municipio_no_padrao(limpa):
    destino = limpa.final
    ultima = max(c for c in destino.listar("patio/clientes"))
    clientes = _linhas_csv(destino, ultima)

    assert clientes
    assert all(c["email"] != "" and c["telefone"] != "" for c in clientes), "nenhum contato nulo"
    assert all(re.fullmatch(r"\d{7}", c["cod_municipio"]) for c in clientes), "município sempre com 7 dígitos"


def test_apolice_com_valor_monetario_numerico(limpa, principal):
    def textos(destino) -> int:
        return sum(
            "," in linha[coluna]
            for caminho in destino.listar("apolice/apolices")
            for linha in _linhas_csv(destino, caminho)
            for coluna in MONETARIAS
        )

    assert textos(principal.etapas[0]) > 0, "contraste: a fatia com gabarito traz o valor como texto"
    assert textos(limpa.final) == 0


@pytest.mark.lento
def test_a_mesma_pessoa_e_limpa_na_fatia_01_e_tem_o_defeito_registrado_na_fatia_10(fatia_01, fatia_10):
    # As duas fatias têm a mesma escala e a mesma história, então o Pátio é o mesmo mundo.
    registros = [
        r for r in geracoes.gabarito(fatia_10.final, "patio")["defeitos"]
        if r["entidade"] == "clientes" and r["tipo"] == "contato_nulo"
    ]
    assert registros, "a fatia 10 registra contatos nulos no Pátio"

    def cliente(destino, chave: str, data) -> dict:
        (caminho,) = destino.listar(f"patio/clientes/{data.isoformat()}")
        return next(c for c in _linhas_csv(destino, caminho) if c["id_cliente"] == chave)

    for r in registros:
        no_10 = cliente(fatia_10.final, r["chave_sistema"], r["data_entrega"])
        no_01 = cliente(fatia_01.etapas[0], r["chave_sistema"], r["data_entrega"])
        assert no_10[r["coluna"]] == "", "o defeito está nos arquivos da fatia 10"
        assert no_01[r["coluna"]] == r["valor_original"] != "", "a mesma pessoa, limpa na fatia 01"
