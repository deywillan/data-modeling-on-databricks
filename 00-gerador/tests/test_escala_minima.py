"""Escala mínima do projeto (0,01): a carga completa tem de sair com os dois lados da
bilhetagem da Viação (Embarque e PassagemHub).

Antes do mínimo por empresa, `rotas: 120` virava 1 rota na escala 0,01, sorteada para o
Expresso Vale do Sol; a Viação ficava sem rotas e a carga saía sem `embarque/tarifas`,
`embarque/passagens` e `passagemhub/eventos_venda`. O teste lê só o que `gerar_carga_inicial`
gravou, na geração compartilhada `carga_minima`. Marcado como `lento` (carga oficial inteira).
"""

import geracoes
import pytest

pytestmark = pytest.mark.lento


@pytest.mark.parametrize(
    ("sistema", "entidade"),
    [("embarque", "tarifas"), ("embarque", "passagens"), ("passagemhub", "eventos_venda")],
)
def test_a_carga_na_escala_minima_traz_a_bilhetagem_da_viacao(carga_minima, sistema, entidade):
    assert geracoes.CARGA_MINIMA.escala == 0.01

    arquivos = geracoes.arquivos(carga_minima.final, sistema)

    assert any(f"/{entidade}/" in caminho for caminho in arquivos), f"{sistema}/{entidade} ausente na escala 0,01"
