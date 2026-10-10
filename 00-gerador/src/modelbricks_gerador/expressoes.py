"""Expressões SQL determinísticas comuns a todo o gerador.

Tudo aqui devolve texto SQL (nada executa no Spark) e depende só de hashes estáveis
(`xxhash64`) das colunas e sementes informadas, nunca de ordem ou de relógio. Assim o mesmo
sorteio dá o mesmo valor em qualquer execução.

- `uniforme(*colunas)`: número em [0, 1) por hash das colunas;
- `sorteio(pesos, uniforme)`: um dos nomes de `pesos`, proporcional ao peso;
- `instante(data, faixa, *colunas)`: timestamp no dia `data`, dentro de uma faixa de horas;
- `inteiro_e_fracao(x)`: parte inteira e fracionária de uma taxa por dia.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .roteiro import Intervalo

_RESOLUCAO = 1_000_000  # casas do uniforme: o hash módulo 10^6 dividido por 10^6


def uniforme(*colunas: "int | str") -> str:
    """Uniforme em [0, 1) por hash estável de `colunas` (semente e colunas, na ordem dada)."""
    return f"(pmod(xxhash64({', '.join(str(c) for c in colunas)}), {_RESOLUCAO}) / {_RESOLUCAO}.0)"


def uniforme_do_lote(numero: int) -> str:
    """Uniforme por linha de fato de um dia: hash da semente do dia (`semente`), do
    sequencial da linha no dia (`n`) e do `numero` do sorteio, que separa sorteios distintos
    da mesma linha."""
    return uniforme("semente", "n", numero)


def sorteio(pesos: dict[str, float], uniforme_: str) -> str:
    """Expressão SQL que escolhe um dos nomes de `pesos` pelo uniforme `uniforme_`, com
    probabilidade proporcional ao peso (nomes em ordem alfabética, o último fecha o `CASE`)."""
    nomes, total, acumulado, casos = sorted(pesos), sum(pesos.values()), 0.0, []
    for nome in nomes[:-1]:
        acumulado += pesos[nome] / total
        casos.append(f"WHEN {uniforme_} < {acumulado!r} THEN {_literal(nome)}")
    ultimo = _literal(nomes[-1])
    return f"CASE {' '.join(casos)} ELSE {ultimo} END" if casos else ultimo


def instante(data: str, faixa: "Intervalo", *colunas: "int | str") -> str:
    """Timestamp no dia `data`, entre as horas `faixa.minimo` e `faixa.maximo`, sorteado
    por hash de `colunas` (a primeira costuma ser a semente)."""
    return (
        f"timestamp_seconds(unix_seconds(cast({data} AS timestamp)) + {faixa.minimo * 3600} "
        f"+ pmod(xxhash64({', '.join(str(c) for c in colunas)}), {(faixa.maximo - faixa.minimo) * 3600}))"
    )


def inteiro_e_fracao(por_dia: float) -> tuple[int, float]:
    """Parte inteira e fração de uma taxa por dia (a fração vira um sorteio de +1 linha)."""
    return int(por_dia), por_dia - int(por_dia)


def _literal(texto: str) -> str:
    return "'" + texto.replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = ["instante", "inteiro_e_fracao", "sorteio", "uniforme", "uniforme_do_lote"]
