"""*RotaCerta* (TMS da ModelBricks Cargas): lotes CDC de fretes em JSON na landing.

Observado só pelo que `gerar_fatia`/`avancar_tempo` gravam: os arquivos da landing e, nas
fatias com gabarito, as tabelas do schema `gabarito`.

Para poupar a cota, o módulo usa uma cópia do roteiro em que a fatia 03-3fn (com gabarito)
tem só o *RotaCerta*. As taxas de defeito são as do roteiro oficial (evento atrasado).
"""

import json
import math
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from importlib import resources

import pyarrow.parquet as pq
import pytest
import yaml

from modelbricks_gerador import DestinoLocal, avancar_tempo, carregar_roteiro, gerar_fatia

ESCALA = 0.05
ARTIGO = "03-3fn"
CARGA_INICIAL = date(2025, 12, 31)
ULTIMO_DIA = date(2026, 1, 10)
ENTIDADES = {"clientes": "id_cliente", "caminhoes": "id_caminhao", "fretes": "id_frete"}
PADRAO = re.compile(r"rotacerta/(clientes|caminhoes|fretes)/(\d{4}-\d{2}-\d{2})/\1_(\d{8})_0001\.json")


def _roteiro_de_teste(pasta):
    dados = yaml.safe_load(
        resources.files("modelbricks_gerador").joinpath("roteiro.yaml").read_text(encoding="utf-8")
    )
    dados["fatias"][ARTIGO]["sistemas"] = ["rotacerta"]
    caminho = pasta / "roteiro.yaml"
    caminho.write_text(yaml.safe_dump(dados, allow_unicode=True), encoding="utf-8")
    return caminho


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    pasta = destino.volume / "rotacerta"
    return {p.relative_to(destino.volume).as_posix(): p.read_bytes() for p in sorted(pasta.rglob("*")) if p.is_file()}


def _operacoes(arquivos: dict[str, bytes]) -> list[dict]:
    """Todas as operações entregues, em ordem de `seq`, com a entidade e a data do lote."""
    linhas = []
    for caminho, conteudo in arquivos.items():
        _, entidade, data_lote, _ = caminho.split("/")
        for linha in conteudo.decode("utf-8").splitlines():
            linhas.append({**json.loads(linha), "_entidade": entidade, "_lote": date.fromisoformat(data_lote)})
    return sorted(linhas, key=lambda o: o["seq"])


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    pasta = destino.raiz / "gabarito"
    return {p.name: pq.read_table(p).to_pylist() for p in sorted(pasta.iterdir()) if p.is_dir()}


def _mapa(gabarito, coluna: str) -> dict[int, int]:
    return {
        m["id_verdadeiro"]: int(m["chave_sistema"])
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == "rotacerta" and m["coluna"] == coluna
    }


def _atrasados(gabarito) -> set[int]:
    return {r["seq"] for r in gabarito["defeitos"] if r["sistema"] == "rotacerta" and r["tipo"] == "evento_atrasado"}


def _instante(op: dict) -> datetime:
    return datetime.fromisoformat(op["ts_operacao"])


@pytest.fixture(scope="module")
def roteiro(tmp_path_factory):
    return _roteiro_de_teste(tmp_path_factory.mktemp("roteiro_rotacerta"))


@pytest.fixture(scope="module")
def gerado(spark, tmp_path_factory, roteiro):
    """Carga inicial e avanço de 10 dias."""
    destino = DestinoLocal(tmp_path_factory.mktemp("rotacerta") / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, spark=spark, roteiro=roteiro)
    carga = _arquivos(destino)
    avancar_tempo(ARTIGO, destino, (ULTIMO_DIA - CARGA_INICIAL).days, spark=spark, roteiro=roteiro)
    arquivos = _arquivos(destino)
    return destino, carga, arquivos, _operacoes(arquivos), _gabarito(destino)


def test_lotes_cdc_chegam_um_arquivo_json_por_entidade_e_dia_simulado(gerado):
    _, carga, arquivos, operacoes, gabarito = gerado
    atrasados = _atrasados(gabarito)

    assert {c.split("/")[1] for c in carga} == set(ENTIDADES)
    for caminho in arquivos:
        casamento = PADRAO.fullmatch(caminho)
        assert casamento, caminho
        assert casamento[3] == casamento[2].replace("-", "")
    for op in operacoes:
        dia = _instante(op).date()
        if op["seq"] in atrasados:
            assert 1 <= (op["_lote"] - dia).days <= 3, "atraso de 1 a 3 dias"
        else:
            assert dia == op["_lote"], f"{op['_entidade']}: ts_operacao fora do dia do lote"
    datas = {date.fromisoformat(c.split("/")[2]) for c in carga}
    assert min(datas) >= date(2024, 1, 1) and max(datas) == CARGA_INICIAL
    assert len({c.split("/")[2] for c in carga if "/fretes/" in c}) > 400, "fretes espalhados pela história"
    assert max(o["_lote"] for o in operacoes) == ULTIMO_DIA


def test_semantica_cdc_valida_na_ordem_dos_eventos(gerado):
    """Mesmas asserções do *Embarque*, na ordem do instante da operação. Na ordem de `seq`,
    só as operações atrasadas (registradas no gabarito) ficam fora de ordem."""
    _, _, _, operacoes, gabarito = gerado
    atrasados = _atrasados(gabarito)

    repetidas = Counter(o["seq"] for o in operacoes)
    assert max(repetidas.values()) == 1, "seq única (o RotaCerta não reentrega)"
    vivas: dict[str, set] = {e: set() for e in ENTIDADES}
    excluidas: dict[str, set] = {e: set() for e in ENTIDADES}
    for op in sorted(operacoes, key=lambda o: (_instante(o), o["seq"])):
        entidade, chave = op["_entidade"], op[ENTIDADES[op["_entidade"]]]
        assert op["op"] in ("I", "U", "D")
        assert chave not in excluidas[entidade], f"operação depois do D em {entidade} {chave}"
        if op["op"] == "I":
            assert chave not in vivas[entidade], f"I repetido em {entidade} {chave}"
            vivas[entidade].add(chave)
        else:
            assert chave in vivas[entidade], f"{op['op']} sem I anterior em {entidade} {chave}"
        if op["op"] == "D":
            vivas[entidade].discard(chave)
            excluidas[entidade].add(chave)
    pontuais = [o for o in operacoes if o["seq"] not in atrasados]
    assert [_instante(o) for o in pontuais] == sorted(_instante(o) for o in pontuais), "seq cresce com o tempo"
    assert atrasados, "operações atrasadas chegam depois de operações posteriores a elas"
    tipos = {(o["_entidade"], o["op"]) for o in operacoes}
    assert {("fretes", "I"), ("fretes", "U"), ("clientes", "I"), ("caminhoes", "I")} <= tipos


def test_cada_frete_tem_de_5_a_8_mudancas_de_status_em_dias_diferentes(gerado):
    _, _, _, operacoes, _ = gerado
    roteiro = carregar_roteiro()
    eventos = roteiro.parametros.eventos_por_frete
    ciclo = [etapa.status for etapa in roteiro.parametros.rotacerta.ciclo]
    obrigatorios = [etapa.status for etapa in roteiro.parametros.rotacerta.ciclo if not etapa.opcional]
    maximo_dias = (eventos.maximo - 1) * roteiro.parametros.rotacerta.dias_entre_eventos.maximo

    historico = defaultdict(list)
    for op in operacoes:
        if op["_entidade"] == "fretes":
            historico[op["id_frete"]].append(op)
    completos, quantidades = 0, Counter()
    for id_frete, ops in historico.items():
        ops.sort(key=_instante)
        assert ops[0]["op"] == "I" and all(o["op"] == "U" for o in ops[1:])
        status = [o["status"] for o in ops]
        assert status[0] == ciclo[0]
        assert status == sorted(status, key=ciclo.index) and len(set(status)) == len(status), "o ciclo só avança"
        dias = [_instante(o).date() for o in ops]
        assert len(set(dias)) == len(dias), f"frete {id_frete}: duas mudanças no mesmo dia"
        fixos = {k: v for k, v in ops[0].items() if k not in ("op", "seq", "ts_operacao", "status") and not k.startswith("_")}
        assert all({k: o[k] for k in fixos} == fixos for o in ops), "só o status muda"
        if dias[0] + timedelta(days=maximo_dias + 3) <= ULTIMO_DIA:
            assert status[-1] == ciclo[-1], f"frete {id_frete} antigo e não entregue"
            assert set(obrigatorios) <= set(status)
            assert eventos.minimo <= len(ops) <= eventos.maximo
            quantidades[len(ops)] += 1
            completos += 1
    assert completos > 500
    assert set(quantidades) == set(range(eventos.minimo, eventos.maximo + 1)), "de 5 a 8 mudanças"


def test_clientes_e_caminhoes_vem_do_mundo_verdadeiro_com_chave_propria(gerado):
    _, _, _, operacoes, gabarito = gerado
    assert {(m["entidade"], m["tabela_verdadeira"]) for m in gabarito["mapa_chaves"] if m["sistema"] == "rotacerta"} == {
        ("clientes", "empresas_pj"), ("caminhoes", "veiculos"),
    }
    clientes_cargas = {
        p["id_entidade"]: p["desde"] for p in gabarito["participacoes"]
        if p["tipo_entidade"] == "empresa_pj" and p["papel"] == "cliente" and p["empresa"] == "mb_cargas"
    }
    frota_cargas = {
        p["id_entidade"]: p["desde"] for p in gabarito["participacoes"]
        if p["tipo_entidade"] == "veiculo" and p["papel"] == "frota" and p["empresa"] == "mb_cargas"
    }
    empresas = {e["id_empresa"]: e for e in gabarito["empresas_pj"]}
    veiculos = {v["id_veiculo"]: v for v in gabarito["veiculos"]}
    id_cliente, id_caminhao = _mapa(gabarito, "id_cliente"), _mapa(gabarito, "id_caminhao")

    assert set(id_cliente) == set(clientes_cargas) and set(id_caminhao) == set(frota_cargas)
    assert all(veiculos[v]["tipo"] == "caminhao" for v in frota_cargas)
    for ids in (id_cliente, id_caminhao):
        assert len(set(ids.values())) == len(ids), "chave do sistema única"
        assert not set(ids) & set(ids.values()), "chave do sistema difere da verdadeira"

    clientes = {o["id_cliente"]: o for o in operacoes if o["_entidade"] == "clientes"}
    assert clientes == {
        id_cliente[i]: {
            **clientes[id_cliente[i]],
            "cnpj": empresas[i]["cnpj"], "razao_social": empresas[i]["razao_social"],
            "email": empresas[i]["email"], "telefone": empresas[i]["telefone"],
            "cod_municipio": empresas[i]["cod_municipio"], "data_cadastro": desde.isoformat(),
        }
        for i, desde in clientes_cargas.items()
    }
    caminhoes = {o["id_caminhao"]: o for o in operacoes if o["_entidade"] == "caminhoes"}
    assert caminhoes == {
        id_caminhao[v]: {
            **caminhoes[id_caminhao[v]],
            "placa": veiculos[v]["placa"], "marca": veiculos[v]["marca"], "modelo": veiculos[v]["modelo"],
            "ano_fabricacao": veiculos[v]["ano_fabricacao"], "data_aquisicao": desde.isoformat(),
        }
        for v, desde in frota_cargas.items()
    }

    # Todo frete referencia cliente e caminhão já cadastrados no dia da abertura.
    for op in operacoes:
        if op["_entidade"] == "fretes" and op["op"] == "I":
            abertura = _instante(op)
            assert _instante(clientes[op["id_cliente"]]) < abertura
            assert _instante(caminhoes[op["id_caminhao"]]) < abertura
            assert date.fromisoformat(op["data_prevista_entrega"]) > abertura.date()
            assert op["peso_kg"] > 0 and op["valor_frete"] > 0


def test_atrasos_do_roteiro_aplicados_e_registrados_no_gabarito(gerado):
    _, _, _, operacoes, gabarito = gerado
    roteiro = carregar_roteiro()
    taxa = roteiro.defeitos["evento_atrasado"].taxas["rotacerta"]
    registros = [r for r in gabarito["defeitos"] if r["sistema"] == "rotacerta"]

    assert {r["tipo"] for r in registros} == {"evento_atrasado"}
    assert all(r["entidade"] == "fretes" and list(r["artigos"]) == roteiro.defeitos["evento_atrasado"].artigos
               for r in registros)
    fretes = [o for o in operacoes if o["_entidade"] == "fretes"]
    observados = {(o["seq"], str(o["id_frete"]), o["_lote"]) for o in fretes if _instante(o).date() != o["_lote"]}
    assert observados == {(r["seq"], r["chave_sistema"], r["data_entrega"]) for r in registros}
    desvio = math.sqrt(taxa * (1 - taxa) / len(fretes))
    assert abs(len(observados) / len(fretes) - taxa) <= 4 * desvio
    assert max(r["data_entrega"] for r in registros) > CARGA_INICIAL


def test_gerar_em_partes_produz_os_mesmos_arquivos(spark, tmp_path, roteiro, gerado):
    _, _, completo, _, _ = gerado
    destino = DestinoLocal(tmp_path / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, ate=date(2026, 1, 4), spark=spark, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 2, spark=spark, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 4, spark=spark, roteiro=roteiro)

    assert _arquivos(destino) == completo
