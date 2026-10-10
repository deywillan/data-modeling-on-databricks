#!/usr/bin/env bash
# Teste de fumaça do setup de um artigo no workspace (manual, fora de CI).
#
# Uso, a partir da raiz do repositório:
#   00-gerador/scripts/teste_fumaca.sh <perfil-da-cli> [artigo] [escala]
#
# Faz deploy do bundle com um catálogo de teste (sufixo _fumaca), roda o setup do artigo,
# confere o catálogo, roda de novo sem reset (nada muda), avança o relógio em 1 dia com o job
# avancar_tempo (o relógio passa para o dia seguinte e chegam lotes novos), roda com reset
# (recria do zero, relógio de volta à carga inicial) e, no fim, destrói o deploy e apaga o
# catálogo de teste.
set -euo pipefail

PERFIL="${1:?informe o perfil da CLI}"
ARTIGO="${2:-01-bronze}"
ESCALA="${3:-0.01}"
SUFIXO="${SUFIXO:-_fumaca}"  # sobrescreva para isolar execuções paralelas
JOB="${ARTIGO%%-*}_setup"
CARGA="$(cd 00-gerador && uv run python -c "from modelbricks_gerador import carregar_roteiro; print(carregar_roteiro().datas.carga_inicial)")"
DIA_SEGUINTE="$(cd 00-gerador && uv run python -c "from datetime import timedelta; from modelbricks_gerador import carregar_roteiro; print(carregar_roteiro().datas.carga_inicial + timedelta(days=1))")"
CATALOGO="$(cd 00-gerador && uv run python -c "from modelbricks_gerador import carregar_roteiro; print(carregar_roteiro().fatia('$ARTIGO').catalogo)")${SUFIXO}"

conferir() {  # $1 (opcional): data em que o relógio simulado deve estar
  (cd 00-gerador && DATABRICKS_CONFIG_PROFILE="$PERFIL" uv run python scripts/conferir_catalogo.py "$CATALOGO" ${1:+--relogio "$1"})
}

limpar() {
  echo "== Limpando: destroy do bundle e DROP CATALOG $CATALOGO"
  databricks bundle destroy -p "$PERFIL" --var "sufixo_catalogo=$SUFIXO" --auto-approve || true
  (cd 00-gerador && DATABRICKS_CONFIG_PROFILE="$PERFIL" uv run python -c \
    "from databricks.connect import DatabricksSession as S; S.builder.serverless().getOrCreate().sql('DROP CATALOG IF EXISTS $CATALOGO CASCADE')") || true
}
trap limpar EXIT

echo "== validate e deploy"
databricks bundle validate -p "$PERFIL" --var "sufixo_catalogo=$SUFIXO"
databricks bundle deploy -p "$PERFIL" --var "sufixo_catalogo=$SUFIXO"

echo "== $JOB (escala $ESCALA)"
time databricks bundle run "$JOB" -p "$PERFIL" --params "escala=$ESCALA,reset=false"
conferir "$CARGA"

echo "== $JOB de novo, sem reset (nada deve mudar)"
databricks bundle run "$JOB" -p "$PERFIL" --params "escala=$ESCALA,reset=false"
conferir "$CARGA"

echo "== avancar_tempo (1 dia: o relógio vai para $DIA_SEGUINTE e chegam lotes novos)"
databricks bundle run avancar_tempo -p "$PERFIL" --params "artigo=$ARTIGO,dias=1"
conferir "$DIA_SEGUINTE"

echo "== $JOB com reset (recria do zero; relógio de volta a $CARGA)"
databricks bundle run "$JOB" -p "$PERFIL" --params "escala=$ESCALA,reset=true"
conferir "$CARGA"
