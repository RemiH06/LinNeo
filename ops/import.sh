#!/usr/bin/env bash
# Carga v2/import en Neo4j desde cero (borra la base anterior), crea el esquema
# y reporta el tamaño del store.
#   bash ops/import.sh          # todo
#   bash ops/import.sh --slim   # sin referencias ni descripciones de GBIF (para medir)
set -euo pipefail
cd "$(dirname "$0")"
IMPORT=../biodiversity_data/v2/import
EXCLUDE='^$'
[ "${1:-}" = "--slim" ] && EXCLUDE='gbif_references|gbif_descriptions'

args=()
for f in "$IMPORT"/*.csv.gz; do
  name=$(basename "$f")
  [[ "$name" =~ $EXCLUDE ]] && continue
  if [[ "$name" == rel_* ]]; then args+=("--relationships=/import/$name")
  else args+=("--nodes=/import/$name"); fi
done

docker compose stop neo4j
docker compose run --rm --no-deps neo4j neo4j-admin database import full neo4j \
  --overwrite-destination=true --array-delimiter=U+001F --multiline-fields=true \
  --bad-tolerance=0 --max-off-heap-memory=3G "${args[@]}"
docker compose up -d neo4j

until docker compose exec -T neo4j cypher-shell "RETURN 1" >/dev/null 2>&1; do sleep 5; done
docker compose exec -T neo4j cypher-shell -f /schema.cypher
docker compose exec -T neo4j du -sh /data/databases/neo4j /data/transactions/neo4j
