#!/usr/bin/env bash
# Teste de fumaça do setup de um artigo no workspace (manual, fora de CI).
#
# Uso, a partir da raiz do repositório:
#   00-gerador/scripts/teste_fumaca.sh <perfil-da-cli> [artigo] [escala]
#
# Faz deploy do bundle com um ambiente de teste (variável `ambiente`, padrão "fumaca", o que dá
# catálogos como a01_bronze_fumaca), roda o setup do artigo, confere os catálogos, roda de novo
# sem reset (nada muda), avança o relógio em 1 dia com o job avancar_tempo (o relógio passa para
# o dia seguinte e chegam lotes novos), roda com reset (recria do zero, relógio de volta à carga
# inicial) e, no fim, destrói o deploy e apaga os três catálogos de teste.
#
# Variáveis de ambiente (opcionais):
#   AMBIENTE  ambiente dos catálogos de teste (padrão "fumaca"; ex.: AMBIENTE=t20 00-gerador/scripts/...)
#
# O script apaga os catálogos do ambiente informado, então recusa "dev" e "prd", que são os do uso
# real do bundle. Para não colidir com outra execução, mude o ambiente e, se o nome do bundle
# também puder colidir, troque `bundle.name` só localmente (sem commitar) durante o teste.
set -euo pipefail

PERFIL="${1:?informe o perfil da CLI}"
ARTIGO="${2:-01-bronze}"
ESCALA="${3:-0.01}"
AMBIENTE="${AMBIENTE:-fumaca}"
case "$AMBIENTE" in
  dev|prd) echo "Ambiente '$AMBIENTE' é o do uso real: o teste apagaria os seus catálogos. Use outro (ex.: AMBIENTE=fumaca)." >&2; exit 2 ;;
esac
JOB="${ARTIGO%%-*}_setup"
python_gerador() { (cd 00-gerador && uv run python -c "$1"); }
CARGA="$(python_gerador "from modelbricks_gerador import carregar_roteiro; print(carregar_roteiro().datas.carga_inicial)")"
DIA_SEGUINTE="$(python_gerador "from datetime import timedelta; from modelbricks_gerador import carregar_roteiro; print(carregar_roteiro().datas.carga_inicial + timedelta(days=1))")"
CATALOGOS="$(python_gerador "from modelbricks_gerador import catalogos_do_artigo; print(*catalogos_do_artigo('$ARTIGO', '$AMBIENTE').values())")"

conferir() {  # $1 (opcional): data em que o relógio simulado deve estar
  (cd 00-gerador && DATABRICKS_CONFIG_PROFILE="$PERFIL" uv run python scripts/conferir_catalogo.py "$ARTIGO" "$AMBIENTE" ${1:+--relogio "$1"})
}

limpar() {
  echo "== Limpando: destroy do bundle e DROP CATALOG de $CATALOGOS"
  databricks bundle destroy -p "$PERFIL" --var "ambiente=$AMBIENTE" --auto-approve || true
  for catalogo in $CATALOGOS; do
    (cd 00-gerador && DATABRICKS_CONFIG_PROFILE="$PERFIL" uv run python -c \
      "from databricks.connect import DatabricksSession as S; S.builder.serverless().getOrCreate().sql('DROP CATALOG IF EXISTS $catalogo CASCADE')") || true
  done
}
trap limpar EXIT

echo "== validate e deploy"
databricks bundle validate -p "$PERFIL" --var "ambiente=$AMBIENTE"
databricks bundle deploy -p "$PERFIL" --var "ambiente=$AMBIENTE"

echo "== $JOB (escala $ESCALA)"
time databricks bundle run "$JOB" -p "$PERFIL" --var "ambiente=$AMBIENTE" --params "escala=$ESCALA,reset=false"
conferir "$CARGA"

echo "== $JOB de novo, sem reset (nada deve mudar)"
databricks bundle run "$JOB" -p "$PERFIL" --var "ambiente=$AMBIENTE" --params "escala=$ESCALA,reset=false"
conferir "$CARGA"

echo "== avancar_tempo (1 dia: o relógio vai para $DIA_SEGUINTE e chegam lotes novos)"
databricks bundle run avancar_tempo -p "$PERFIL" --var "ambiente=$AMBIENTE" --params "artigo=$ARTIGO,dias=1"
conferir "$DIA_SEGUINTE"

echo "== $JOB com reset (recria do zero; relógio de volta a $CARGA)"
databricks bundle run "$JOB" -p "$PERFIL" --var "ambiente=$AMBIENTE" --params "escala=$ESCALA,reset=true"
conferir "$CARGA"
