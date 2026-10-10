"""Escala mínima do projeto (0,01): a fatia 01 tem de sair completa, com os dois lados da
bilhetagem da Viação (Embarque e PassagemHub).

Antes do mínimo por empresa, `rotas: 120` virava 1 rota na escala 0,01, sorteada para o
Expresso Vale do Sol; a Viação ficava sem rotas e a fatia saía sem `embarque/tarifas`,
`embarque/passagens` e `passagemhub/eventos_venda`. O teste lê só o que `gerar_fatia`
gravou, na geração compartilhada `fatia_01_minima`. Marcado como `lento` (fatia oficial
inteira).
"""

import geracoes
import pytest

pytestmark = pytest.mark.lento


@pytest.mark.parametrize(
    ("sistema", "entidade"),
    [("embarque", "tarifas"), ("embarque", "passagens"), ("passagemhub", "eventos_venda")],
)
def test_a_fatia_01_na_escala_minima_traz_a_bilhetagem_da_viacao(fatia_01_minima, sistema, entidade):
    assert geracoes.FATIA_01_MINIMA.escala == 0.01

    arquivos = geracoes.arquivos(fatia_01_minima.final, sistema)

    assert any(f"/{entidade}/" in caminho for caminho in arquivos), f"{sistema}/{entidade} ausente na escala 0,01"
