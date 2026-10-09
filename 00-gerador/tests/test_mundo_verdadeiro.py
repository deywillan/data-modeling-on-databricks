"""Mundo verdadeiro do Grupo ModelBricks, observado pelo gabarito gravado por `gerar_fatia`.

Os volumes esperados vêm do Estudo de Caso (escala 1: 20 mil pessoas, 1,5 mil empresas PJ,
3 mil veículos, dos quais 400 na frota própria, e 3 mil vendas); a escala multiplica
linearmente.
"""

import re

import pyarrow.parquet as pq
import pytest

from modelbricks_gerador import DestinoLocal, gerar_fatia

ARTIGO = "03-3fn"  # fatia com gabarito
ESCALA = 0.05
TABELAS = {
    "pessoas", "empresas_pj", "veiculos", "vendas_veiculos", "municipios",
    "rotas", "bases", "lojas", "colaboradores", "participacoes",
}


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    pasta = destino.raiz / "gabarito"
    return {p.name: pq.read_table(p).to_pylist() for p in sorted(pasta.iterdir()) if p.is_dir()}


@pytest.fixture(scope="module")
def mundo(spark, tmp_path_factory):
    destino = DestinoLocal(tmp_path_factory.mktemp("mundo") / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, spark=spark)
    return destino, _gabarito(destino)


def test_gabarito_tem_as_tabelas_do_mundo_e_nada_dele_vai_para_a_landing(mundo):
    destino, tabelas = mundo

    assert set(tabelas) == TABELAS
    entregues = [p.relative_to(destino.volume).as_posix() for p in destino.volume.rglob("*") if p.is_file()]
    assert not [c for c in entregues if any(t in c.split("/") for t in TABELAS) or c.endswith(".parquet")]


def test_volumes_da_escala_vem_do_estudo_de_caso(mundo):
    _, tabelas = mundo

    assert len(tabelas["pessoas"]) == 1000
    assert len(tabelas["empresas_pj"]) == 75
    assert len(tabelas["veiculos"]) == 150
    assert sum(v["frota_propria"] for v in tabelas["veiculos"]) == 20
    assert len(tabelas["vendas_veiculos"]) == 150
    assert len(tabelas["municipios"]) == 5571


def test_contagens_sao_proporcionais_a_escala(spark, tmp_path, mundo):
    _, na_escala = mundo
    destino = DestinoLocal(tmp_path / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA * 2, spark=spark)
    no_dobro = _gabarito(destino)

    for tabela in ("pessoas", "empresas_pj", "veiculos", "vendas_veiculos", "rotas", "colaboradores"):
        assert len(no_dobro[tabela]) == 2 * len(na_escala[tabela]), tabela
    assert sum(v["frota_propria"] for v in no_dobro["veiculos"]) == 40
    assert len(no_dobro["municipios"]) == len(na_escala["municipios"])  # referência, não escala


def test_mesma_escala_e_roteiro_produzem_gabarito_identico(spark, tmp_path, mundo):
    _, primeira = mundo
    destino = DestinoLocal(tmp_path / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, spark=spark)

    assert _gabarito(destino) == primeira


def _empresas_por_pessoa(tabelas) -> dict[int, set[str]]:
    empresas: dict[int, set[str]] = {}
    for p in tabelas["participacoes"]:
        if p["tipo_entidade"] == "pessoa" and p["papel"] == "cliente":
            empresas.setdefault(p["id_entidade"], set()).add(p["empresa"])
    return empresas


def test_cerca_de_15_por_cento_das_pessoas_sao_clientes_de_mais_de_uma_empresa(mundo):
    _, tabelas = mundo
    empresas = _empresas_por_pessoa(tabelas)

    assert set(empresas) == {p["id_pessoa"] for p in tabelas["pessoas"]}, "toda pessoa é cliente de alguém"
    multi = sum(len(e) > 1 for e in empresas.values()) / len(tabelas["pessoas"])
    assert 0.12 <= multi <= 0.18
    assert set().union(*empresas.values()) == {"mb_viacao", "mb_veiculos", "mb_seguros", "evs"}


def test_frota_propria_foi_vendida_pela_concessionaria_as_empresas_do_grupo(mundo):
    _, tabelas = mundo
    veiculos = {v["id_veiculo"]: v for v in tabelas["veiculos"]}
    vendas = {v["id_veiculo"]: v for v in tabelas["vendas_veiculos"]}
    frota = [v for v in veiculos.values() if v["frota_propria"]]

    assert frota
    for veiculo in frota:
        venda = vendas[veiculo["id_veiculo"]]
        assert venda["empresa_vendedora"] == "mb_veiculos"
        assert venda["tipo_comprador"] == "grupo"
        assert venda["empresa_compradora"] == {"onibus": "mb_viacao", "caminhao": "mb_cargas"}[veiculo["tipo"]]
    frota_nas_participacoes = {
        (p["id_entidade"], p["empresa"]) for p in tabelas["participacoes"] if p["papel"] == "frota"
    }
    assert frota_nas_participacoes == {(v["id_veiculo"], v["empresa_frota"]) for v in frota}


def test_demais_veiculos_foram_vendidos_a_clientes_da_concessionaria(mundo):
    _, tabelas = mundo
    veiculos = {v["id_veiculo"]: v for v in tabelas["veiculos"]}
    clientes = {
        (p["tipo_entidade"], p["id_entidade"])
        for p in tabelas["participacoes"]
        if p["empresa"] == "mb_veiculos" and p["papel"] == "cliente"
    }
    vendas = [v for v in tabelas["vendas_veiculos"] if not veiculos[v["id_veiculo"]]["frota_propria"]]

    assert {v["tipo_comprador"] for v in vendas} == {"pf", "pj"}
    for venda in vendas:
        if venda["tipo_comprador"] == "pf":
            comprador = ("pessoa", venda["id_pessoa"])
        else:
            comprador = ("empresa_pj", venda["id_empresa"])
        assert comprador in clientes, "quem compra é cliente da concessionária"
    lojas = {loja["id_loja"] for loja in tabelas["lojas"] if loja["empresa"] == "mb_veiculos"}
    assert {v["id_loja"] for v in tabelas["vendas_veiculos"]} <= lojas


def test_rotas_bases_lojas_e_cadastros_referenciam_municipios_do_ibge(mundo):
    _, tabelas = mundo
    codigos = {m["cod_municipio"] for m in tabelas["municipios"]}

    for tabela in ("pessoas", "empresas_pj", "bases", "lojas"):
        assert {r["cod_municipio"] for r in tabelas[tabela]} <= codigos, tabela
    for rota in tabelas["rotas"]:
        assert {rota["cod_municipio_origem"], rota["cod_municipio_destino"]} <= codigos
        assert rota["cod_municipio_origem"] != rota["cod_municipio_destino"]


def test_colaboradores_sao_pessoas_do_mundo_com_um_vinculo_cada(mundo):
    _, tabelas = mundo
    pessoas = {p["id_pessoa"] for p in tabelas["pessoas"]}
    vinculos = [c["id_pessoa"] for c in tabelas["colaboradores"]]

    assert set(vinculos) <= pessoas
    assert len(set(vinculos)) == len(vinculos)
    assert {c["empresa"] for c in tabelas["colaboradores"]} <= {"mb", "mb_viacao", "mb_cargas", "mb_veiculos"}


def _dv(digitos: str, pesos: list[int]) -> str:
    resto = sum(int(d) * p for d, p in zip(digitos, pesos, strict=True)) % 11
    return "0" if resto < 2 else str(11 - resto)


def test_cpf_cnpj_e_placa_sao_validos_e_unicos(mundo):
    _, tabelas = mundo
    cpfs = [p["cpf"] for p in tabelas["pessoas"]]
    cnpjs = [e["cnpj"] for e in tabelas["empresas_pj"]]
    placas = [v["placa"] for v in tabelas["veiculos"]]

    assert len(set(cpfs)) == len(cpfs)
    assert len(set(cnpjs)) == len(cnpjs)
    assert len(set(placas)) == len(placas)
    for cpf in cpfs:
        assert len(cpf) == 11 and cpf.isdigit()
        assert cpf[9] == _dv(cpf[:9], list(range(10, 1, -1)))
        assert cpf[10] == _dv(cpf[:10], list(range(11, 1, -1)))
    pesos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for cnpj in cnpjs:
        assert len(cnpj) == 14 and cnpj[8:12] == "0001"
        assert cnpj[12] == _dv(cnpj[:12], pesos)
        assert cnpj[13] == _dv(cnpj[:13], [6, *pesos])
    assert all(re.fullmatch(r"[A-Z]{3}[0-9][A-Z][0-9]{2}", placa) for placa in placas)


def test_nomes_e_enderecos_cabem_em_latin1_para_os_sistemas_legados(mundo):
    _, tabelas = mundo

    for pessoa in tabelas["pessoas"]:
        (pessoa["nome"] + pessoa["logradouro"]).encode("latin-1")
    for empresa in tabelas["empresas_pj"]:
        (empresa["razao_social"] + empresa["logradouro"]).encode("latin-1")
