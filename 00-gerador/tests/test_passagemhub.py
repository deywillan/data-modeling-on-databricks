"""*PassagemHub* (marketplace parceiro da ModelBricks Viação): API paginada em JSON aninhado.

Observado só pelo que `gerar_fatia`/`avancar_tempo` gravam: as páginas na landing e, nas
fatias com gabarito, as tabelas do schema `gabarito`.

Para poupar a cota, o módulo usa uma cópia do roteiro em que a fatia 05-raw-vault (com
gabarito) tem só o *PassagemHub* e a página é pequena, para que cada dia tenha várias
páginas na escala mínima. As taxas de defeitos são as do roteiro oficial.
"""

import json
import math
import re
from collections import defaultdict
from datetime import date, datetime
from importlib import resources

import pyarrow.parquet as pq
import pytest
import yaml

from modelbricks_gerador import DestinoLocal, avancar_tempo, carregar_roteiro, gerar_fatia

ESCALA = 0.05
ARTIGO = "05-raw-vault"
CARGA_INICIAL = date(2025, 12, 31)
COLUNA_NOVA = date(2026, 1, 5)
ULTIMO_DIA = date(2026, 1, 7)
POR_PAGINA = 5
PADRAO = re.compile(r"passagemhub/eventos_venda/(\d{4}-\d{2}-\d{2})/eventos_venda_(\d{8})_(\d{4})\.json")


def _roteiro_de_teste(pasta):
    dados = yaml.safe_load(
        resources.files("modelbricks_gerador").joinpath("roteiro.yaml").read_text(encoding="utf-8")
    )
    dados["fatias"][ARTIGO]["sistemas"] = ["passagemhub"]
    dados["parametros"]["passagemhub"]["eventos_por_pagina"] = POR_PAGINA
    caminho = pasta / "roteiro.yaml"
    caminho.write_text(yaml.safe_dump(dados, allow_unicode=True), encoding="utf-8")
    return caminho


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    pasta = destino.volume / "passagemhub"
    return {p.relative_to(destino.volume).as_posix(): p.read_bytes() for p in sorted(pasta.rglob("*")) if p.is_file()}


def _paginas(arquivos: dict[str, bytes]) -> list[tuple[str, dict]]:
    return [(caminho, json.loads(conteudo)) for caminho, conteudo in arquivos.items()]


def _eventos(arquivos: dict[str, bytes]) -> list[dict]:
    """Eventos de todas as páginas, com a data do lote e o arquivo."""
    eventos = []
    for caminho, pagina in _paginas(arquivos):
        lote = date.fromisoformat(caminho.split("/")[2])
        eventos += [{**e, "_lote": lote, "_arquivo": caminho} for e in pagina["eventos"]]
    return eventos


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    pasta = destino.raiz / "gabarito"
    return {p.name: pq.read_table(p).to_pylist() for p in sorted(pasta.iterdir()) if p.is_dir()}


@pytest.fixture(scope="module")
def roteiro(tmp_path_factory):
    return _roteiro_de_teste(tmp_path_factory.mktemp("roteiro_passagemhub"))


@pytest.fixture(scope="module")
def gerado(spark, tmp_path_factory, roteiro):
    """Carga inicial e avanço até depois da coluna nova (05/01/2026)."""
    destino = DestinoLocal(tmp_path_factory.mktemp("passagemhub") / "medalhao_05_raw_vault")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, spark=spark, roteiro=roteiro)
    carga = _arquivos(destino)
    avancar_tempo(ARTIGO, destino, (ULTIMO_DIA - CARGA_INICIAL).days, spark=spark, roteiro=roteiro)
    return destino, carga, _arquivos(destino), _gabarito(destino)


def test_paginas_json_aninhado_numeradas_por_dia_simulado(gerado):
    _, carga, arquivos, _ = gerado
    paginas_por_dia = defaultdict(list)

    for caminho, conteudo in arquivos.items():
        casamento = PADRAO.fullmatch(caminho)
        assert casamento, caminho
        data_lote = date.fromisoformat(casamento[1])
        assert casamento[2] == f"{data_lote:%Y%m%d}"
        texto = conteudo.decode("utf-8")
        assert texto.endswith("}\n") and texto.count("\n") == 1, "uma página = um documento JSON"
        pagina = json.loads(texto)
        assert pagina["pagina"] == int(casamento[3])
        assert pagina["data_referencia"] == data_lote.isoformat()
        assert pagina["por_pagina"] == POR_PAGINA
        assert 1 <= len(pagina["eventos"]) <= POR_PAGINA
        for evento in pagina["eventos"]:
            assert datetime.fromisoformat(evento["ocorrido_em"]).date() == data_lote
            assert isinstance(evento["pedido"], dict) and isinstance(evento["comprador"], dict)
            assert isinstance(evento["viagem"]["origem"], dict) and isinstance(evento["valores"], dict)
        paginas_por_dia[data_lote].append(pagina)

    for data_lote, paginas in paginas_por_dia.items():
        paginas.sort(key=lambda p: p["pagina"])
        total = sum(len(p["eventos"]) for p in paginas)
        assert [p["pagina"] for p in paginas] == list(range(1, len(paginas) + 1)), data_lote
        assert all(p["total_paginas"] == len(paginas) == math.ceil(total / POR_PAGINA) for p in paginas)
        assert all(p["total_eventos"] == total for p in paginas)
        assert all(len(p["eventos"]) == POR_PAGINA for p in paginas[:-1]), "só a última página é parcial"
        instantes = [(e["ocorrido_em"], e["id_evento"]) for p in paginas for e in p["eventos"]]
        assert instantes == sorted(instantes), "eventos em ordem de ocorrência ao longo das páginas"
    assert sum(len(p) > 1 for p in paginas_por_dia.values()) > 600, "dias com várias páginas"
    assert min(paginas_por_dia) >= date(2024, 1, 1)
    assert max(date.fromisoformat(c.split("/")[2]) for c in carga) == CARGA_INICIAL
    assert max(paginas_por_dia) == ULTIMO_DIA


def test_coluna_nova_aparece_em_todos_os_eventos_a_partir_de_05_01_2026(gerado):
    _, _, arquivos, _ = gerado
    roteiro = carregar_roteiro()
    assert roteiro.eventos["coluna_nova_passagemhub"].data == COLUNA_NOVA
    formas = set(roteiro.parametros.passagemhub.formas_pagamento)
    parcelas = roteiro.parametros.passagemhub.parcelas_cartao

    eventos = _eventos(arquivos)
    antes = [e for e in eventos if e["_lote"] < COLUNA_NOVA]
    depois = [e for e in eventos if e["_lote"] >= COLUNA_NOVA]
    assert antes and depois
    assert not any("pagamento" in e for e in antes)
    assert all("pagamento" in e for e in depois)
    for e in depois:
        assert e["pagamento"]["forma"] in formas
        maximo = parcelas.maximo if e["pagamento"]["forma"] == "cartao_credito" else 1
        assert 1 <= e["pagamento"]["parcelas"] <= maximo
    assert {e["pagamento"]["forma"] for e in depois} == formas


def test_ciclo_de_vida_dos_pedidos(gerado):
    _, _, arquivos, _ = gerado
    vendas, mudancas = {}, defaultdict(list)
    for e in sorted(_eventos(arquivos), key=lambda e: (e["ocorrido_em"], e["id_evento"])):
        codigo = e["pedido"]["codigo"]
        if e["tipo"] == "venda":
            assert codigo not in vendas, "uma venda por pedido"
            vendas[codigo] = e
        else:
            assert e["tipo"] in ("cancelamento", "remarcacao")
            assert codigo in vendas, "mudança só de pedido já vendido"
            venda = vendas[codigo]
            assert e["ocorrido_em"] > venda["ocorrido_em"]
            assert e["ocorrido_em"][:10] < venda["viagem"]["data"], "mudança antes da viagem"
            assert e["comprador"]["id_cliente"] == venda["comprador"]["id_cliente"]
            if e["tipo"] == "remarcacao":
                assert e["viagem"]["data"] > venda["viagem"]["data"]
            mudancas[codigo].append(e)
        assert e["viagem"]["data"] >= e["pedido"]["criado_em"][:10]
        assert e["valores"]["total"] == pytest.approx(e["valores"]["tarifa"] + e["valores"]["taxa_servico"], abs=0.001)
    assert all(len(m) == 1 for m in mudancas.values()), "no máximo uma mudança por pedido"
    assert {e["tipo"] for m in mudancas.values() for e in m} == {"cancelamento", "remarcacao"}
    assert {v["pedido"]["canal"] for v in vendas.values()} == set(carregar_roteiro().parametros.passagemhub.canais)

    # Volume: a parte do PassagemHub nas passagens da Viação, proporcional à escala e aos dias.
    roteiro = carregar_roteiro()
    dias_historia = (CARGA_INICIAL - roteiro.datas.inicio).days + 1
    dias = (ULTIMO_DIA - roteiro.datas.inicio).days + 1
    esperado = (
        roteiro.volumes.passagens_embarque_passagemhub * (1 - roteiro.parametros.embarque.fracao_passagens)
        * ESCALA * dias / dias_historia
    )
    assert abs(len(vendas) - esperado) <= 0.05 * esperado


def _mapa(gabarito, coluna: str) -> dict[str, int]:
    return {
        m["chave_sistema"]: m["id_verdadeiro"]
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == "passagemhub" and m["coluna"] == coluna
    }


def _cpf_com_mascara(cpf: str) -> str:
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"


def test_compradores_sao_clientes_da_viacao_com_chave_e_grafia_do_sistema(gerado):
    _, _, arquivos, gabarito = gerado
    clientes = _mapa(gabarito, "id_cliente")
    trechos = _mapa(gabarito, "cod_trecho")
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    rotas = {r["id_rota"]: r for r in gabarito["rotas"]}
    viacao = {
        p["id_entidade"] for p in gabarito["participacoes"]
        if p["tipo_entidade"] == "pessoa" and p["papel"] == "cliente" and p["empresa"] == "mb_viacao"
    }
    sujos = {(r["chave_sistema"], r["coluna"]) for r in gabarito["defeitos"] if r["sistema"] == "passagemhub"}

    assert set(clientes.values()) == viacao
    assert len(set(clientes)) == len(clientes)
    assert not set(clientes) & {str(i) for i in clientes.values()}, "chave do sistema difere da verdadeira"
    assert set(trechos.values()) == {i for i, r in rotas.items() if r["empresa"] == "mb_viacao"}
    for e in _eventos(arquivos):
        comprador, viagem, pedido = e["comprador"], e["viagem"], e["pedido"]["codigo"]
        pessoa = pessoas[clientes[comprador["id_cliente"]]]
        assert comprador["cpf"] == _cpf_com_mascara(pessoa["cpf"])
        if (pedido, "nome") not in sujos:
            assert comprador["nome"] == pessoa["nome"].upper()
        for campo in ("email", "telefone"):
            if (pedido, campo) not in sujos:
                assert comprador["contato"][campo] == pessoa[campo]
        rota = rotas[trechos[str(viagem["cod_trecho"])]]
        assert viagem["origem"]["cod_municipio"] == rota["cod_municipio_origem"]
        assert viagem["destino"]["cod_municipio"] == rota["cod_municipio_destino"]


def _dentro_da_tolerancia(observados: int, populacao: int, taxa: float) -> bool:
    desvio = math.sqrt(taxa * (1 - taxa) / populacao)
    return abs(observados / populacao - taxa) <= 4 * desvio + 1 / populacao


def test_defeitos_do_roteiro_aplicados_por_pedido_e_registrados_no_gabarito(gerado):
    _, _, arquivos, gabarito = gerado
    roteiro = carregar_roteiro()
    clientes = _mapa(gabarito, "id_cliente")
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}

    observados: dict[str, set] = defaultdict(set)
    pedidos = set()
    for e in _eventos(arquivos):
        pedido, comprador = e["pedido"]["codigo"], e["comprador"]
        pessoa = pessoas[clientes[comprador["id_cliente"]]]
        pedidos.add(pedido)
        if comprador["nome"] != pessoa["nome"].upper():
            observados["nome_abreviado_ou_com_erro"].add((pedido, "nome", e["_lote"], e["_arquivo"]))
        for campo in ("email", "telefone"):
            if comprador["contato"].get(campo) is None:
                observados["contato_nulo"].add((pedido, campo, e["_lote"], e["_arquivo"]))

    registrados: dict[str, set] = defaultdict(set)
    for r in gabarito["defeitos"]:
        assert r["sistema"] == "passagemhub" and r["granularidade"] == "linha"
        assert r["tabela_verdadeira"] == "pessoas" and r["entidade"] == "eventos_venda"
        assert list(r["artigos"]) == roteiro.defeitos[r["tipo"]].artigos
        if r["tipo"] == "nome_abreviado_ou_com_erro":
            assert r["valor_original"] == pessoas[r["id_verdadeiro"]]["nome"].upper() != r["valor_sujo"]
        registrados[r["tipo"]].add((r["chave_sistema"], r["coluna"], r["data_entrega"], r["arquivo"]))

    taxas = {t: d.taxas["passagemhub"] for t, d in roteiro.defeitos.items() if "passagemhub" in d.taxas}
    assert set(registrados) == set(taxas) == {"nome_abreviado_ou_com_erro", "contato_nulo"}
    for tipo, taxa in taxas.items():
        assert observados[tipo] == registrados[tipo], f"{tipo}: landing e gabarito divergem"
        sujos = {o[0] for o in observados[tipo]}
        assert _dentro_da_tolerancia(len(sujos), len(pedidos), taxa), f"{tipo}: {len(sujos)}/{len(pedidos)}"
    assert max(r["data_entrega"] for r in gabarito["defeitos"]) > CARGA_INICIAL


def test_gerar_em_partes_produz_as_mesmas_paginas(spark, tmp_path, roteiro, gerado):
    _, _, completo, _ = gerado
    destino = DestinoLocal(tmp_path / "medalhao_05_raw_vault")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, ate=date(2026, 1, 4), spark=spark, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 1, spark=spark, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 2, spark=spark, roteiro=roteiro)

    assert _arquivos(destino) == completo
