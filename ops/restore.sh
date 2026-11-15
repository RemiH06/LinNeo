#!/usr/bin/env bash
# Carga un volcado de dump.sh en el Neo4j de producción y comprueba los conteos.
# Sirve igual para el primer despliegue, para publicar un import nuevo y para
# restaurar un respaldo. La API se detiene mientras tanto (el sitio da 502).
#   ops/restore.sh ~/srv/linneo-respaldos/linneo-AAAAMMDD-HHMM.dump
set -euo pipefail
dump="$(realpath "$1")"
cd "$(dirname "$0")"

prod() { docker compose -f compose.yml -f compose.prod.yml "$@"; }
# Exportada y pasada como `-e NEO4J_PASSWORD` sin valor: así no aparece en `ps`.
NEO4J_PASSWORD=$(sed -n 's/^NEO4J_PASSWORD=//p' .env)
export NEO4J_PASSWORD
cypher() {
  prod exec -T -e NEO4J_USERNAME=neo4j -e NEO4J_PASSWORD neo4j \
    cypher-shell --format plain "$1" | tail -n 1
}

# Un volumen nuevo necesita un primer arranque para crear la base system con la contraseña.
prod up -d --wait neo4j
prod stop api neo4j
prod run --rm --no-deps -T neo4j \
  neo4j-admin database load neo4j --from-stdin --overwrite-destination=true < "$dump"
prod up -d --wait

conteos="$(cypher "MATCH (n) RETURN count(n)") $(cypher "MATCH ()-[r]->() RETURN count(r)")"
echo "cargado: $conteos (nodos relaciones)"
if [ -f "$dump.conteos" ] && [ "$conteos" != "$(cat "$dump.conteos")" ]; then
  echo "no coincide con el volcado: $(cat "$dump.conteos")" >&2
  exit 1
fi
