"""Entregas anuais do IBGE: rodam sem Spark e sem cota, porque a projeção só copia o CSV versionado.

O grupo baixa a lista de municípios todo dia 2 de janeiro. A carga inicial (01/01/2024 a
31/12/2025) traz as listas de 2024 e 2025; o `avancar_tempo` traz a de 2026, a primeira com
Boa Esperança do Norte (MT). É essa segunda ingestão que o espelho do artigo 01 demonstra.
"""

from datetime import date

from modelbricks_gerador.sistemas import ibge

BOA_ESPERANCA_DO_NORTE = b"5101837,Boa Esperan\xc3\xa7a do Norte,51,MT"


def _entregas(inicio: date, fim: date) -> dict[str, bytes]:
    return {lote.arquivo.caminho: lote.arquivo.conteudo for lote in ibge.projetar(None, inicio, fim).lotes}


def test_a_carga_inicial_traz_as_listas_de_2024_e_2025_sem_o_municipio_novo():
    entregas = _entregas(date(2024, 1, 1), date(2025, 12, 31))

    assert list(entregas) == [
        "ibge/municipios/2024-01-02/municipios_20240102_0001.csv",
        "ibge/municipios/2025-01-02/municipios_20250102_0001.csv",
    ]
    for conteudo in entregas.values():
        assert conteudo.count(b"\n") - 1 == 5570
        assert BOA_ESPERANCA_DO_NORTE not in conteudo


def test_o_avanco_ate_2_de_janeiro_de_2026_traz_a_lista_com_5571_municipios():
    assert _entregas(date(2026, 1, 1), date(2026, 1, 1)) == {}, "antes do dia 2, nada do IBGE"

    (conteudo,) = _entregas(date(2026, 1, 1), date(2026, 1, 10)).values()

    assert conteudo.count(b"\n") - 1 == 5571
    assert BOA_ESPERANCA_DO_NORTE in conteudo


def test_cada_lista_e_completa_e_so_difere_pelos_municipios_novos():
    lista_2025 = set(ibge.conteudo_da_lista(2025).splitlines())
    lista_2026 = set(ibge.conteudo_da_lista(2026).splitlines())

    assert lista_2026 - lista_2025 == {linha for linha in lista_2026 if linha.startswith(BOA_ESPERANCA_DO_NORTE)}
    assert lista_2025 <= lista_2026


def test_gerar_em_partes_ou_de_uma_vez_produz_as_mesmas_entregas():
    de_uma_vez = _entregas(date(2024, 1, 1), date(2026, 1, 12))
    em_partes = {**_entregas(date(2024, 1, 1), date(2025, 12, 31)), **_entregas(date(2026, 1, 1), date(2026, 1, 12))}

    assert de_uma_vez == em_partes
