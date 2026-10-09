"""*Núcleo* (ERP corporativo do grupo): incremental por `updated_at` em Parquet, com *soft delete*.

Observado só pelo que `gerar_fatia`/`avancar_tempo` gravam: os arquivos da landing e, nas
fatias com gabarito, as tabelas do schema `gabarito`.

Para poupar a cota, o módulo usa uma cópia do roteiro em que a fatia 03-3fn (com gabarito)
tem só o *Núcleo*. O roteiro oficial não dá taxa de defeito ao *Núcleo*; a cópia ativa o
evento atrasado para ele, para exercitar os atrasos injetados e registrados do motor
incremental (que o *Apólice+* reutiliza).
"""

import io
import math
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from importlib import resources

import pyarrow.parquet as pq
import pytest
import yaml

from modelbricks_gerador import DestinoLocal, avancar_tempo, carregar_roteiro, gerar_fatia

ESCALA = 0.05
ARTIGO = "03-3fn"
CARGA_INICIAL = date(2025, 12, 31)
ULTIMO_DIA = date(2026, 1, 10)
TAXA_ATRASO = 0.05
ENTIDADES = {
    "colaboradores": "matricula",
    "fornecedores": "codigo_fornecedor",
    "titulos_pagar": "id_titulo",
    "notas_fiscais": "numero_nota",
    "ativos": "id_ativo",
}
PADRAO = re.compile(r"nucleo/([a-z_]+)/(\d{4}-\d{2}-\d{2})/\1_(\d{8})_0001\.parquet")


def _roteiro_de_teste(pasta):
    dados = yaml.safe_load(
        resources.files("modelbricks_gerador").joinpath("roteiro.yaml").read_text(encoding="utf-8")
    )
    dados["fatias"][ARTIGO]["sistemas"] = ["nucleo"]
    dados["defeitos"]["evento_atrasado"]["taxas"]["nucleo"] = TAXA_ATRASO
    caminho = pasta / "roteiro.yaml"
    caminho.write_text(yaml.safe_dump(dados, allow_unicode=True), encoding="utf-8")
    return caminho


def _arquivos(destino: DestinoLocal) -> dict[str, bytes]:
    pasta = destino.volume / "nucleo"
    return {p.relative_to(destino.volume).as_posix(): p.read_bytes() for p in sorted(pasta.rglob("*")) if p.is_file()}


def _linhas(arquivos: dict[str, bytes]) -> dict[str, list[dict]]:
    """Linhas entregues por entidade, com a data do lote e o arquivo."""
    linhas = defaultdict(list)
    for caminho, conteudo in arquivos.items():
        _, entidade, data_lote, _ = caminho.split("/")
        for linha in pq.read_table(io.BytesIO(conteudo)).to_pylist():
            linhas[entidade].append({**linha, "_lote": date.fromisoformat(data_lote), "_arquivo": caminho})
    return linhas


def _versoes(linhas: list[dict], chave: str) -> dict:
    """Versões de cada chave em ordem de `updated_at` (como o consumidor as aplicaria)."""
    por_chave = defaultdict(list)
    for linha in sorted(linhas, key=lambda x: x["updated_at"]):
        por_chave[linha[chave]].append(linha)
    return por_chave


def _gabarito(destino: DestinoLocal) -> dict[str, list[dict]]:
    pasta = destino.raiz / "gabarito"
    return {p.name: pq.read_table(p).to_pylist() for p in sorted(pasta.iterdir()) if p.is_dir()}


def _mapa(gabarito, entidade: str) -> dict[str, int]:
    return {
        m["chave_sistema"]: m["id_verdadeiro"]
        for m in gabarito["mapa_chaves"]
        if m["sistema"] == "nucleo" and m["entidade"] == entidade
    }


@pytest.fixture(scope="module")
def roteiro(tmp_path_factory):
    return _roteiro_de_teste(tmp_path_factory.mktemp("roteiro_nucleo"))


@pytest.fixture(scope="module")
def gerado(spark, tmp_path_factory, roteiro):
    """Carga inicial e avanço de 10 dias."""
    destino = DestinoLocal(tmp_path_factory.mktemp("nucleo") / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, spark=spark, roteiro=roteiro)
    carga = _arquivos(destino)
    avancar_tempo(ARTIGO, destino, (ULTIMO_DIA - CARGA_INICIAL).days, spark=spark, roteiro=roteiro)
    arquivos = _arquivos(destino)
    return destino, carga, arquivos, _linhas(arquivos), _gabarito(destino)


def _atrasados(gabarito) -> set[tuple]:
    return {
        (r["entidade"], r["chave_sistema"], r["data_entrega"])
        for r in gabarito["defeitos"]
        if r["sistema"] == "nucleo" and r["tipo"] == "evento_atrasado"
    }


def test_lote_diario_em_parquet_so_com_linhas_alteradas_no_dia(gerado):
    _, carga, arquivos, linhas, gabarito = gerado
    atrasados = _atrasados(gabarito)

    assert {c.split("/")[1] for c in arquivos} == set(ENTIDADES)
    for caminho in arquivos:
        casamento = PADRAO.fullmatch(caminho)
        assert casamento, caminho
        assert casamento[3] == casamento[2].replace("-", "")
    for entidade, chave in ENTIDADES.items():
        for linha in linhas[entidade]:
            assert {"updated_at", "excluido"} <= set(linha)
            assert isinstance(linha["excluido"], bool)
            assert linha["updated_at"].tzinfo is not None
            dia = linha["updated_at"].astimezone(timezone.utc).date()
            if (entidade, str(linha[chave]), linha["_lote"]) in atrasados:
                assert 1 <= (linha["_lote"] - dia).days <= 3, "atraso de 1 a 3 dias"
            else:
                assert dia == linha["_lote"], f"{entidade} {linha[chave]}: updated_at fora do dia do lote"
    datas_carga = {date.fromisoformat(c.split("/")[2]) for c in carga}
    assert min(datas_carga) == date(2024, 1, 1) and max(datas_carga) <= CARGA_INICIAL
    dias_titulos = {c.split("/")[2] for c in carga if "/titulos_pagar/" in c}
    assert len(dias_titulos) > 400, "títulos espalhados pelos dois anos de história"
    assert max(date.fromisoformat(c.split("/")[2]) for c in arquivos) == ULTIMO_DIA


IMAGEM_OBRIGATORIA = {
    "titulos_pagar": ("codigo_fornecedor", "valor", "data_vencimento"),
    "fornecedores": ("cnpj", "razao_social"),
}


def _imagem(linha: dict) -> dict:
    return {k: v for k, v in linha.items() if k not in ("updated_at", "excluido") and not k.startswith("_")}


def test_exclusoes_chegam_como_soft_delete_e_encerram_o_registro(gerado):
    _, _, _, linhas, _ = gerado
    excluidas = defaultdict(int)

    for entidade, chave in ENTIDADES.items():
        for codigo, versoes in _versoes(linhas[entidade], chave).items():
            assert not versoes[0]["excluido"], f"{entidade} {codigo}: a primeira versão não é exclusão"
            marcas = [v["excluido"] for v in versoes]
            if any(marcas):
                assert marcas.count(True) == 1 and marcas[-1], f"{entidade} {codigo}: nada vem depois da exclusão"
                excluida = versoes[-1]
                assert all(excluida[c] is not None for c in IMAGEM_OBRIGATORIA[entidade]), "exclusão com a imagem"
                if entidade == "titulos_pagar":
                    assert _imagem(excluida) == _imagem(versoes[-2])
                excluidas[entidade] += 1
    assert excluidas["titulos_pagar"] > 0 and excluidas["fornecedores"] > 0
    assert set(excluidas) <= {"titulos_pagar", "fornecedores"}


def test_titulos_pagos_ou_cancelados_referenciam_fornecedores_ativos(gerado):
    _, _, _, linhas, _ = gerado
    fornecedores = _versoes(linhas["fornecedores"], "codigo_fornecedor")
    titulos = _versoes(linhas["titulos_pagar"], "id_titulo")
    roteiro = carregar_roteiro()

    for versoes in titulos.values():
        emissao = versoes[0]
        assert emissao["status"] == "aberto" and emissao["data_pagamento"] is None
        assert emissao["data_vencimento"] > emissao["data_emissao"] == emissao["updated_at"].date()
        historico = fornecedores[emissao["codigo_fornecedor"]]
        assert historico[0]["updated_at"] < emissao["updated_at"], "fornecedor cadastrado antes do título"
        assert not any(v["excluido"] and v["updated_at"] < emissao["updated_at"] for v in historico)
        if len(versoes) > 1:
            assert len(versoes) == 2
            final = versoes[1]
            if final["excluido"]:
                assert final["updated_at"].date() < emissao["data_vencimento"]
            else:
                assert final["status"] == "pago" and final["data_pagamento"] >= emissao["data_vencimento"]
                assert final["updated_at"].date() == final["data_pagamento"]

    # Volume: os títulos do roteiro, proporcionais à escala e aos dias gerados.
    dias_historia = (CARGA_INICIAL - roteiro.datas.inicio).days + 1
    dias = (ULTIMO_DIA - roteiro.datas.inicio).days + 1
    esperado = roteiro.parametros.nucleo.titulos_pagar * ESCALA * dias / dias_historia
    assert abs(len(titulos) - esperado) <= 0.1 * esperado


def test_colaboradores_trazem_o_cargo_e_as_mudancas_de_cargo(gerado):
    _, _, _, linhas, gabarito = gerado
    roteiro = carregar_roteiro()
    matriculas = _mapa(gabarito, "colaboradores")
    colaboradores = {c["id_colaborador"]: c for c in gabarito["colaboradores"]}
    pessoas = {p["id_pessoa"]: p for p in gabarito["pessoas"]}
    versoes = _versoes(linhas["colaboradores"], "matricula")

    assert set(matriculas.values()) == set(colaboradores)
    assert {str(m) for m in versoes} == set(matriculas)
    mudancas = 0
    for matricula, historico in versoes.items():
        verdadeiro = colaboradores[matriculas[str(matricula)]]
        entrada = historico[0]
        assert entrada["cargo"] == verdadeiro["cargo"], "a entrada traz o cargo do mundo verdadeiro"
        assert entrada["data_admissao"] == verdadeiro["data_admissao"]
        assert entrada["cpf"] == pessoas[verdadeiro["id_pessoa"]]["cpf"]
        assert entrada["updated_at"].date() == max(verdadeiro["data_admissao"], roteiro.datas.inicio)
        cargos = set(roteiro.catalogos.cargos[verdadeiro["empresa"]])
        for anterior, atual in zip(historico, historico[1:]):
            assert atual["cargo"] != anterior["cargo"] and atual["cargo"] in cargos
            fixos = ("cargo", "updated_at")
            assert {k: v for k, v in _imagem(atual).items() if k not in fixos} == {
                k: v for k, v in _imagem(anterior).items() if k not in fixos
            }
            mudancas += 1
    anos = ((ULTIMO_DIA - roteiro.datas.inicio).days + 1) / 365
    esperado = len(versoes) * roteiro.parametros.nucleo.mudanca_cargo_ao_ano * anos
    assert esperado / 3 <= mudancas <= esperado * 3, f"{mudancas} mudanças de cargo (esperado ~{esperado:.0f})"


def test_mapa_chave_verdadeira_para_chave_do_nucleo_no_gabarito(gerado):
    _, _, _, linhas, gabarito = gerado
    empresas = {e["id_empresa"]: e for e in gabarito["empresas_pj"]}
    vendas = {v["id_venda"]: v for v in gabarito["vendas_veiculos"]}
    veiculos = {v["id_veiculo"]: v for v in gabarito["veiculos"]}
    entidades = {m["entidade"]: m["tabela_verdadeira"] for m in gabarito["mapa_chaves"] if m["sistema"] == "nucleo"}
    assert entidades == {
        "colaboradores": "colaboradores", "fornecedores": "empresas_pj",
        "notas_fiscais": "vendas_veiculos", "ativos": "veiculos",
    }

    fornecedores = _mapa(gabarito, "fornecedores")
    for linha in linhas["fornecedores"]:
        empresa = empresas[fornecedores[str(linha["codigo_fornecedor"])]]
        assert (linha["cnpj"], linha["razao_social"]) == (empresa["cnpj"], empresa["razao_social"])
    notas = _mapa(gabarito, "notas_fiscais")
    assert set(notas.values()) == set(vendas)
    for linha in linhas["notas_fiscais"]:
        venda = vendas[notas[str(linha["numero_nota"])]]
        assert linha["valor_total"] == venda["valor_venda"] and linha["data_emissao"] == venda["data_venda"]
        assert linha["placa"] == veiculos[venda["id_veiculo"]]["placa"]
    ativos = _mapa(gabarito, "ativos")
    frota = {v["id_veiculo"] for v in veiculos.values() if v["frota_propria"]}
    assert set(ativos.values()) == frota, "o veículo da frota própria aparece no ERP como ativo"
    for linha in linhas["ativos"]:
        veiculo = veiculos[ativos[str(linha["id_ativo"])]]
        assert linha["placa"] == veiculo["placa"] and linha["empresa"] == veiculo["empresa_frota"]
        assert 0 <= linha["valor_contabil"] <= linha["valor_aquisicao"]
    for entidade in entidades:
        ids = _mapa(gabarito, entidade)
        assert not set(ids) & {str(i) for i in ids.values()}, f"{entidade}: chave do sistema difere da verdadeira"
        assert len(set(ids.values())) == len(ids), f"{entidade}: uma chave do sistema por id verdadeiro"


def test_atrasos_do_roteiro_aplicados_e_registrados_no_gabarito(gerado):
    _, _, _, linhas, gabarito = gerado
    registros = [r for r in gabarito["defeitos"] if r["sistema"] == "nucleo"]
    roteiro = carregar_roteiro()

    assert {r["tipo"] for r in registros} == {"evento_atrasado"}
    observados, total = set(), 0
    for entidade, chave in ENTIDADES.items():
        for linha in linhas[entidade]:
            total += 1
            if linha["updated_at"].astimezone(timezone.utc).date() != linha["_lote"]:
                observados.add((entidade, str(linha[chave]), linha["_lote"], linha["_arquivo"]))
    registrados = set()
    for r in registros:
        assert r["granularidade"] == "linha" and list(r["artigos"]) == roteiro.defeitos["evento_atrasado"].artigos
        registrados.add((r["entidade"], r["chave_sistema"], r["data_entrega"], r["arquivo"]))
    assert observados == registrados
    desvio = math.sqrt(TAXA_ATRASO * (1 - TAXA_ATRASO) / total)
    assert abs(len(observados) / total - TAXA_ATRASO) <= 4 * desvio + 1 / total
    assert max(r["data_entrega"] for r in registros) > CARGA_INICIAL


def test_gerar_em_partes_produz_os_mesmos_arquivos(spark, tmp_path, roteiro, gerado):
    _, _, completo, _, _ = gerado
    destino = DestinoLocal(tmp_path / "medalhao_03_3fn")
    gerar_fatia(ARTIGO, destino, escala=ESCALA, ate=date(2026, 1, 4), spark=spark, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 2, spark=spark, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 4, spark=spark, roteiro=roteiro)

    assert _arquivos(destino) == completo
