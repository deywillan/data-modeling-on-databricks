"""Recuperação de falha parcial e conferência do relógio, sem Spark.

`gerar_fatia`/`avancar_tempo` podem cair no meio (rede, cota, cancelamento do job). A nova
tentativa precisa terminar o serviço sem nunca sobrescrever a landing:

- um arquivo que já existe com os mesmos bytes conta como gravado; com bytes diferentes
  continua sendo erro;
- o gabarito do intervalo não duplica;
- o relógio só avança no fim, então a nova tentativa repete o mesmo intervalo.

Os sistemas são falsos (registrados em `PROJECOES` só durante o teste) e o destino falso cai
no ponto combinado, então nada aqui abre sessão nem gasta a cota do workspace.

Também confere o relógio: se o roteiro ou a escala mudaram desde a carga inicial, os dados
novos não combinariam com os antigos, e a API manda recomeçar com `reset`.
"""

from datetime import date, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from modelbricks_gerador import (
    ArquivoJaExiste,
    DestinoLocal,
    RelogioIncompativel,
    avancar_tempo,
    carregar_roteiro,
    gerar_fatia,
)
from modelbricks_gerador.escritor import Arquivo, caminho_landing
from modelbricks_gerador.relogio import Relogio, gravar_relogio, ler_relogio
from modelbricks_gerador.sistemas import PROJECOES
from modelbricks_gerador.sistemas.base import Entrega, Lote

ARTIGO = "01-bronze"
INICIO = date(2024, 1, 1)
CARGA = date(2024, 1, 3)
SCHEMAS = ("landing", "bronze", "silver", "gold")
SPARK_FALSO = SimpleNamespace(conf=SimpleNamespace(set=lambda *_: None))


def _projecao_falsa(sistema: str, defeitos=None):
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
        "artigos": [3], "coluna": "cpf", "chave_sistema": f"{dia:%Y%m%d}", "tabela_verdadeira": None,
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


def _roteiro(gabarito: bool = False, defeitos: bool = False):
    roteiro = carregar_roteiro()
    sistemas = ["falso_a", "falso_b"]
    fatia = roteiro.fatia(ARTIGO).model_copy(update={"sistemas": sistemas, "gabarito": gabarito})
    return roteiro.model_copy(update={"fatias": {**roteiro.fatias, ARTIGO: fatia}})


@pytest.fixture
def sistemas_falsos(monkeypatch):
    def registrar(defeitos: bool = False):
        for sistema in ("falso_a", "falso_b"):
            monkeypatch.setitem(PROJECOES, sistema, _projecao_falsa(sistema, defeitos))

    registrar()
    return registrar


class DestinoQueCai(DestinoLocal):
    """Destino local que levanta `OSError` uma vez, no ponto combinado."""

    def __init__(self, raiz, *, apos_arquivos: int | None = None, no_relogio: bool = False):
        super().__init__(raiz)
        self.apos_arquivos = apos_arquivos
        self.no_relogio = no_relogio
        self.gravados = 0
        self.caiu = False

    def gravar_arquivos(self, arquivos):
        for arquivo in arquivos:
            ponto = (
                not self.caiu
                and (self.no_relogio and arquivo.caminho.startswith("_controle/relogio/")
                     or self.apos_arquivos is not None and self.gravados >= self.apos_arquivos
                     and not arquivo.caminho.startswith("_controle/"))
            )
            if ponto:
                self.caiu = True
                raise OSError("queda simulada")
            super().gravar_arquivos([arquivo])
            self.gravados += 1


def _conteudo(destino: DestinoLocal) -> dict[str, bytes]:
    return {p.relative_to(destino.raiz).as_posix(): p.read_bytes() for p in destino.raiz.rglob("*") if p.is_file()}


def _carga_inicial(destino: DestinoLocal, roteiro, **kwargs):
    return gerar_fatia(ARTIGO, destino, ate=CARGA, spark=SPARK_FALSO, roteiro=roteiro, **kwargs)


def _referencia(tmp_path, roteiro, dias: int = 0) -> DestinoLocal:
    """Destino que fez o mesmo serviço sem nenhuma falha."""
    destino = DestinoLocal(tmp_path / "referencia" / "medalhao")
    _carga_inicial(destino, roteiro)
    if dias:
        avancar_tempo(ARTIGO, destino, dias, spark=SPARK_FALSO, roteiro=roteiro)
    return destino


# -- arquivo que já existe ---------------------------------------------------------------------


def test_arquivo_existente_com_os_mesmos_bytes_conta_como_gravado(tmp_path):
    destino = DestinoLocal(tmp_path / "medalhao")
    destino.preparar(SCHEMAS)
    arquivo = Arquivo("sis/itens/2024-01-01/itens_20240101_0001.json", b"a\n")
    destino.gravar_arquivos([arquivo])

    destino.gravar_arquivos([arquivo])

    assert destino.ler(arquivo.caminho) == b"a\n"


def test_arquivo_existente_com_outros_bytes_continua_sendo_erro_e_nao_muda(tmp_path):
    destino = DestinoLocal(tmp_path / "medalhao")
    destino.preparar(SCHEMAS)
    caminho = "sis/itens/2024-01-01/itens_20240101_0001.json"
    destino.gravar_arquivos([Arquivo(caminho, b"a\n")])

    with pytest.raises(ArquivoJaExiste):
        destino.gravar_arquivos([Arquivo(caminho, b"b\n")])

    assert destino.ler(caminho) == b"a\n"


# -- recuperação -------------------------------------------------------------------------------


def test_gerar_fatia_que_caiu_no_meio_termina_na_nova_tentativa_sem_sobrescrever(tmp_path, sistemas_falsos):
    roteiro = _roteiro()
    caindo = DestinoQueCai(tmp_path / "medalhao", apos_arquivos=2)

    with pytest.raises(OSError, match="queda simulada"):
        _carga_inicial(caindo, roteiro)

    assert ler_relogio(caindo) is None, "sem relógio, a carga inicial não foi concluída"
    parcial = _conteudo(caindo)
    assert any(c.startswith("landing/landing/falso_") for c in parcial)

    resultado = _carga_inicial(DestinoLocal(caindo.raiz), roteiro)

    assert resultado.gerou and resultado.data_relogio == CARGA
    completo = _conteudo(caindo)
    assert {c: completo[c] for c in parcial} == parcial, "o que já estava gravado não muda"
    assert completo == _conteudo(_referencia(tmp_path, roteiro))


def test_avancar_tempo_que_caiu_no_meio_termina_na_nova_tentativa(tmp_path, sistemas_falsos):
    roteiro = _roteiro()
    destino = DestinoQueCai(tmp_path / "medalhao")
    _carga_inicial(destino, roteiro)
    destino.apos_arquivos = destino.gravados + 3  # cai depois de 3 arquivos novos

    with pytest.raises(OSError, match="queda simulada"):
        avancar_tempo(ARTIGO, destino, 3, spark=SPARK_FALSO, roteiro=roteiro)

    assert ler_relogio(destino).data_simulada == CARGA, "o relógio só avança no fim"

    resultado = avancar_tempo(ARTIGO, DestinoLocal(destino.raiz), 3, spark=SPARK_FALSO, roteiro=roteiro)

    assert resultado.data_relogio == CARGA + timedelta(days=3)
    assert _conteudo(destino) == _conteudo(_referencia(tmp_path, roteiro, dias=3))


def test_gabarito_do_intervalo_nao_duplica_quando_o_avanco_cai_depois_de_gravar_os_defeitos(tmp_path, sistemas_falsos):
    sistemas_falsos(defeitos=True)
    roteiro = _roteiro(gabarito=True)
    destino = DestinoQueCai(tmp_path / "medalhao")
    # A carga inicial com gabarito precisa do mundo (Spark); o estado "depois dela" é montado à mão.
    destino.preparar((*SCHEMAS, "gabarito"))
    gravar_relogio(destino, Relogio(CARGA, ARTIGO, 1.0, roteiro.versao))
    destino.no_relogio = True  # cai ao gravar o relógio, depois de gravar landing e defeitos

    with pytest.raises(OSError, match="queda simulada"):
        avancar_tempo(ARTIGO, destino, 4, spark=SPARK_FALSO, roteiro=roteiro)

    assert ler_relogio(destino).data_simulada == CARGA
    assert _defeitos(destino), "os defeitos do intervalo já tinham sido gravados"

    avancar_tempo(ARTIGO, DestinoLocal(destino.raiz), 4, spark=SPARK_FALSO, roteiro=roteiro)

    depois = _defeitos(destino)
    assert len(depois) == len(set(depois)), "nenhum defeito aparece duas vezes"
    esperado = {(s, f"{CARGA + timedelta(days=i):%Y%m%d}") for s in ("falso_a", "falso_b") for i in range(1, 5)
                if (CARGA + timedelta(days=i)).day % 2 == 1}
    assert set(depois) == esperado


def test_avancos_seguidos_preservam_os_defeitos_dos_avancos_anteriores(tmp_path, sistemas_falsos):
    sistemas_falsos(defeitos=True)
    roteiro = _roteiro(gabarito=True)
    destino = DestinoLocal(tmp_path / "medalhao")
    destino.preparar((*SCHEMAS, "gabarito"))
    gravar_relogio(destino, Relogio(CARGA, ARTIGO, 1.0, roteiro.versao))

    avancar_tempo(ARTIGO, destino, 2, spark=SPARK_FALSO, roteiro=roteiro)
    avancar_tempo(ARTIGO, destino, 2, spark=SPARK_FALSO, roteiro=roteiro)

    depois = _defeitos(destino)
    assert len(depois) == len(set(depois))
    assert {chave for _, chave in depois} == {
        f"{CARGA + timedelta(days=i):%Y%m%d}" for i in range(1, 5) if (CARGA + timedelta(days=i)).day % 2 == 1
    }


def _defeitos(destino: DestinoLocal) -> list[tuple[str, str]]:
    import pyarrow.parquet as pq

    pasta = destino.raiz / "gabarito" / "defeitos"
    if not pasta.exists():
        return []
    return [
        (r["sistema"], r["chave_sistema"])
        for parte in sorted(pasta.glob("*.parquet")) for r in pq.read_table(parte).to_pylist()
    ]


# -- conferência do relógio --------------------------------------------------------------------


def _destino_com_relogio(tmp_path, *, versao_a_menos: int = 0, escala: float = 1.0) -> DestinoLocal:
    roteiro = carregar_roteiro()
    destino = DestinoLocal(tmp_path / "medalhao")
    destino.preparar(SCHEMAS)
    gravar_relogio(destino, Relogio(CARGA, ARTIGO, escala, roteiro.versao - versao_a_menos))
    return destino


def test_avancar_tempo_com_outra_versao_do_roteiro_manda_recomecar(tmp_path):
    destino = _destino_com_relogio(tmp_path, versao_a_menos=1)

    with pytest.raises(RelogioIncompativel, match=r"versão.*roteiro.*reset"):
        avancar_tempo(ARTIGO, destino, 1)  # falha antes de abrir a sessão

    assert ler_relogio(destino).data_simulada == CARGA


def test_gerar_fatia_sem_reset_com_outra_versao_do_roteiro_manda_recomecar(tmp_path):
    destino = _destino_com_relogio(tmp_path, versao_a_menos=1)

    with pytest.raises(RelogioIncompativel, match=r"versão.*roteiro.*reset"):
        gerar_fatia(ARTIGO, destino, escala=1.0)


def test_gerar_fatia_sem_reset_com_outra_escala_manda_recomecar(tmp_path):
    destino = _destino_com_relogio(tmp_path, escala=1.0)

    with pytest.raises(RelogioIncompativel, match=r"escala.*reset"):
        gerar_fatia(ARTIGO, destino, escala=0.5)


def test_gerar_fatia_sem_reset_com_a_mesma_versao_e_escala_continua_idempotente(tmp_path):
    destino = _destino_com_relogio(tmp_path, escala=0.5)

    resultado = gerar_fatia(ARTIGO, destino, escala=0.5)

    assert not resultado.gerou and resultado.data_relogio == CARGA
