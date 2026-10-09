"""*Embarque* (bilhetagem da ModelBricks Viação): lotes CDC em JSON na landing.

Observado só pelo que `gerar_fatia`/`avancar_tempo` gravam: os arquivos da landing e, nas
fatias com gabarito, as tabelas do schema `gabarito`.
"""

import json
import re
from collections import Counter
from datetime import date, datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from modelbricks_gerador import DestinoLocal, avancar_tempo, carregar_roteiro, gerar_fatia

ESCALA = 0.05  # a menor escala com rotas suficientes para a Viação
CARGA_INICIAL = date(2025, 12, 31)
REAJUSTE = date(2026, 1, 12)
ENTIDADES = {"clientes": "id_cliente", "tarifas": "id_tarifa", "passagens": "id_passagem"}


def _arquivos_embarque(destino: DestinoLocal) -> dict[str, bytes]:
    pasta = destino.volume / "embarque"
    return {p.relative_to(destino.volume).as_posix(): p.read_bytes() for p in sorted(pasta.rglob("*")) if p.is_file()}


def _linhas(arquivos: dict[str, bytes]) -> list[dict]:
    """Todas as linhas entregues (reentregas inclusive), com a entidade e a data do lote."""
    linhas = []
    for caminho, conteudo in arquivos.items():
        _, entidade, data_lote, _ = caminho.split("/")
        for linha in conteudo.decode("utf-8").splitlines():
            linhas.append({**json.loads(linha), "_entidade": entidade, "_lote": date.fromisoformat(data_lote)})
    return sorted(linhas, key=lambda o: (o["seq"], o["_lote"]))


def _operacoes(arquivos: dict[str, bytes]) -> list[dict]:
    """Operações em ordem de `seq`, sem as reentregas (o consumidor descarta a `seq` repetida)."""
    unicas: dict[int, dict] = {}
    for linha in _linhas(arquivos):
        unicas.setdefault(linha["seq"], linha)
    return list(unicas.values())


def _aplicar(operacoes: list[dict], ate: date | None = None) -> dict[str, dict]:
    """Estado do sistema depois de aplicar os lotes em ordem (até a data de lote `ate`)."""
    estado: dict[str, dict] = {entidade: {} for entidade in ENTIDADES}
    for op in operacoes:
        if ate and op["_lote"] > ate:
            continue
        entidade = op["_entidade"]
        chave = op[ENTIDADES[entidade]]
        if op["op"] == "D":
            del estado[entidade][chave]
        else:
            estado[entidade][chave] = {k: v for k, v in op.items() if k not in ("op", "seq", "ts_operacao") and not k.startswith("_")}
    return estado


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    """Tabelas do gabarito; `mapa_chaves` e `defeitos` só com as linhas do Embarque (a fatia
    tem outros sistemas)."""
    pasta = destino.raiz / "gabarito"
    tabelas = {p.name: pq.read_table(p).to_pylist() for p in sorted(pasta.iterdir()) if p.is_dir()}
    for nome in ("mapa_chaves", "defeitos"):
        tabelas[nome] = [r for r in tabelas[nome] if r["sistema"] == "embarque"]
    return tabelas


@pytest.fixture(scope="module")
def com_gabarito(spark, tmp_path_factory):
    """Fatia 03-3fn (com gabarito): carga inicial e avanço até o dia do reajuste de tarifas."""
    destino = DestinoLocal(tmp_path_factory.mktemp("embarque") / "medalhao_03_3fn")
    gerar_fatia("03-3fn", destino, escala=ESCALA, spark=spark)
    carga = _arquivos_embarque(destino)
    avancar_tempo("03-3fn", destino, (REAJUSTE - CARGA_INICIAL).days, spark=spark)
    return destino, carga, _arquivos_embarque(destino)


def test_lotes_cdc_chegam_um_arquivo_json_por_entidade_e_dia_simulado(com_gabarito):
    destino, carga, _ = com_gabarito
    reentregues = {r["seq"] for r in _gabarito(destino)["defeitos"] if r["tipo"] == "cdc_duplicado"}
    padrao = re.compile(r"embarque/(clientes|tarifas|passagens)/(\d{4}-\d{2}-\d{2})/\1_(\d{8})_0001\.json")

    assert {c.split("/")[1] for c in carga} == set(ENTIDADES)
    for caminho, conteudo in carga.items():
        casamento = padrao.fullmatch(caminho)
        assert casamento, caminho
        data_lote = date.fromisoformat(casamento[2])
        assert casamento[3] == f"{data_lote:%Y%m%d}"
        assert date(2024, 1, 1) <= data_lote <= CARGA_INICIAL
        for linha in conteudo.decode("utf-8").splitlines():
            registro = json.loads(linha)
            if registro["seq"] not in reentregues:  # a reentrega chega dias depois da operação
                assert datetime.fromisoformat(registro["ts_operacao"]).date() == data_lote
    dias_de_passagens = {c.split("/")[2] for c in carga if c.startswith("embarque/passagens/")}
    assert len(dias_de_passagens) > 600, "vendas espalhadas pelos dois anos de história"


def test_todo_u_e_d_refere_se_a_chave_inserida_antes_e_seq_e_unica(com_gabarito):
    destino, _, arquivos = com_gabarito
    operacoes = _operacoes(arquivos)
    reentregues = {r["seq"] for r in _gabarito(destino)["defeitos"] if r["tipo"] == "cdc_duplicado"}

    repetidas = Counter(o["seq"] for o in _linhas(arquivos))
    assert {s for s, n in repetidas.items() if n > 1} == reentregues, "seq repetida só nas reentregas registradas"
    vivas: dict[str, set] = {entidade: set() for entidade in ENTIDADES}
    excluidas: dict[str, set] = {entidade: set() for entidade in ENTIDADES}
    for op in operacoes:
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
            assert set(op) - {"_entidade", "_lote"} == {"op", "seq", "ts_operacao", ENTIDADES[entidade]}, "D só com a chave"
    instantes = [datetime.fromisoformat(o["ts_operacao"]) for o in operacoes]
    assert instantes == sorted(instantes), "seq cresce com o tempo da operação"
    tipos = {(o["_entidade"], o["op"]) for o in operacoes}
    assert {("passagens", "I"), ("passagens", "U"), ("passagens", "D"), ("clientes", "I"), ("tarifas", "I")} <= tipos


def test_passagens_referenciam_clientes_e_linhas_ja_cadastrados(com_gabarito):
    _, _, arquivos = com_gabarito
    clientes, linhas, status = set(), set(), set()
    for op in _operacoes(arquivos):
        if op["_entidade"] == "clientes":
            clientes.add(op["id_cliente"])
        elif op["_entidade"] == "tarifas":
            linhas.add(op["cod_linha"])
        elif op["op"] != "D":
            assert op["id_cliente"] in clientes
            assert op["cod_linha"] in linhas
            assert date.fromisoformat(op["data_viagem"]) >= datetime.fromisoformat(op["data_hora_venda"]).date()
            status.add(op["status"])
    assert status == {"emitida", "remarcada", "cancelada", "utilizada"}


def _mapa(gabarito, coluna: str) -> dict[int, int]:
    return {c["id_verdadeiro"]: int(c["chave_sistema"]) for c in gabarito["mapa_chaves"] if c["coluna"] == coluna}


def test_aplicar_os_lotes_em_ordem_reproduz_o_estado_derivavel_do_gabarito(com_gabarito):
    destino, carga, _ = com_gabarito
    gabarito = _gabarito(destino)
    estado = _aplicar(_operacoes(carga))

    id_cliente = _mapa(gabarito, "id_cliente")
    clientes_viacao = {
        p["id_entidade"]: p["desde"]
        for p in gabarito["participacoes"]
        if p["tipo_entidade"] == "pessoa" and p["papel"] == "cliente" and p["empresa"] == "mb_viacao"
    }
    assert set(id_cliente) == set(clientes_viacao)
    assert len(set(id_cliente.values())) == len(id_cliente), "chave do sistema única"
    assert not set(id_cliente) & set(id_cliente.values()), "chave do sistema difere da verdadeira"
    assert estado["clientes"] == {
        id_cliente[p["id_pessoa"]]: {
            "id_cliente": id_cliente[p["id_pessoa"]], "nome": p["nome"], "cpf": p["cpf"],
            "data_nascimento": p["data_nascimento"].isoformat(), "email": p["email"], "telefone": p["telefone"],
            "cod_municipio": p["cod_municipio"], "data_cadastro": clientes_viacao[p["id_pessoa"]].isoformat(),
        }
        for p in gabarito["pessoas"]
        if p["id_pessoa"] in clientes_viacao
    }

    id_tarifa, cod_linha = _mapa(gabarito, "id_tarifa"), _mapa(gabarito, "cod_linha")
    rotas_viacao = [r for r in gabarito["rotas"] if r["empresa"] == "mb_viacao"]
    assert rotas_viacao
    assert estado["tarifas"] == {
        id_tarifa[r["id_rota"]]: {
            "id_tarifa": id_tarifa[r["id_rota"]], "cod_linha": cod_linha[r["id_rota"]],
            "cod_municipio_origem": r["cod_municipio_origem"], "cod_municipio_destino": r["cod_municipio_destino"],
            "valor": float(r["tarifa_base"]), "vigente_desde": "2024-01-01",
        }
        for r in rotas_viacao
    }


def test_reajuste_de_tarifas_do_roteiro_atualiza_todas_as_tarifas_em_12_01_2026(com_gabarito):
    destino, _, arquivos = com_gabarito
    evento = carregar_roteiro().eventos["reajuste_tarifas_embarque"]
    assert evento.data == REAJUSTE
    percentual = Decimal(str(evento.parametros["percentual"]))
    gabarito = _gabarito(destino)
    rota_da_tarifa = {chave: rota for rota, chave in _mapa(gabarito, "id_tarifa").items()}
    tarifa_base = {r["id_rota"]: r["tarifa_base"] for r in gabarito["rotas"]}
    operacoes = _operacoes(arquivos)

    updates = [o for o in operacoes if o["_entidade"] == "tarifas" and o["op"] == "U"]
    assert {o["_lote"] for o in updates} == {REAJUSTE}
    estado = _aplicar(operacoes)
    assert sorted(o["id_tarifa"] for o in updates) == sorted(estado["tarifas"])
    for id_tarifa, tarifa in estado["tarifas"].items():
        reajustado = (tarifa_base[rota_da_tarifa[id_tarifa]] * (1 + percentual)).quantize(Decimal("0.01"))
        assert Decimal(str(tarifa["valor"])) == reajustado
        assert tarifa["vigente_desde"] == REAJUSTE.isoformat()
    vendas_no_reajuste = [o for o in operacoes if o["_entidade"] == "passagens" and o["op"] == "I" and o["_lote"] == REAJUSTE]
    assert vendas_no_reajuste
    assert {o["valor"] for o in vendas_no_reajuste} <= {t["valor"] for t in estado["tarifas"].values()}


def test_gerar_sistema_ou_dia_isoladamente_produz_o_mesmo_conteudo(spark, tmp_path, com_gabarito):
    _, carga, completo = com_gabarito
    # Outra fatia (Embarque sem os sistemas do 03-3fn), carga até 05/01/2026 e avanço de 1 dia.
    destino = DestinoLocal(tmp_path / "medalhao_07_star_schema")
    gerar_fatia("07-star-schema", destino, escala=ESCALA, ate=date(2026, 1, 5), spark=spark)
    avancar_tempo("07-star-schema", destino, 1, spark=spark)
    isolado = _arquivos_embarque(destino)

    assert isolado == {c: v for c, v in completo.items() if c.split("/")[2] <= "2026-01-06"}
    assert {c: v for c, v in isolado.items() if c.split("/")[2] <= "2025-12-31"} == carga


def test_roteiro_do_estudo_de_caso_so_reentrega_cdc_no_embarque_na_taxa_do_roteiro(com_gabarito):
    destino, _, arquivos = com_gabarito
    defeitos = _gabarito(destino)["defeitos"]
    taxa = carregar_roteiro().defeitos["cdc_duplicado"].taxas["embarque"]
    linhas = _linhas(arquivos)
    unicas = len({o["seq"] for o in linhas})

    assert {r["tipo"] for r in defeitos} == {"cdc_duplicado"}
    assert len(defeitos) == len(linhas) - unicas
    assert abs(len(defeitos) / unicas - taxa) <= 4 * (taxa * (1 - taxa) / unicas) ** 0.5
