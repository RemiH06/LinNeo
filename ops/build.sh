#!/usr/bin/env bash
# Construye v2 de punta a punta. Cada etapa es reanudable o idempotente, así que
# relanzar el mismo comando tras un corte sigue donde iba.
#   bash ops/build.sh            # árbol + enriquecimiento
#   bash ops/build.sh --missing  # además baja los artículos faltantes de Wikipedia y re-casa
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python

[ -f biodiversity_data/backbone_extract/Taxon.tsv ] || $PY pipeline/00_download_backbone.py
$PY pipeline/01_backbone.py
# El puente de Wikidata se refresca a mano (python pipeline/02_wikidata_ids.py):
# QLever a veces está saturado y no conviene que eso tumbe el build.
[ -f biodiversity_data/wikidata/gbif_es.tsv ] || $PY pipeline/02_wikidata_ids.py
$PY pipeline/03_enrich.py

if [ "${1:-}" = "--missing" ]; then
  $PY pipeline/fetchers/wikipedia_fetcher.py --missing es
  $PY pipeline/fetchers/wikipedia_fetcher.py --missing en
  $PY pipeline/03_enrich.py
fi
