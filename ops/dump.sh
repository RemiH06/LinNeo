#!/usr/bin/env bash
# Vuelca el grafo de desarrollo (el que arma ops/import.sh) a un archivo fechado,
# con sus conteos al lado para que restore.sh compruebe la carga.
# Community solo vuelca bases detenidas: el Neo4j de dev se apaga mientras dura.
#   ops/dump.sh [carpeta]     por defecto ~/srv/linneo-respaldos; guarda los últimos 3
set -euo pipefail
cd "$(dirname "$0")"

destino="${1:-$HOME/srv/linneo-respaldos}"
guardar=3
dev() { docker compose -f compose.yml "$@"; }
cypher() { dev exec -T neo4j cypher-shell --format plain "$1" | tail -n 1; }

mkdir -p "$destino"
archivo="$destino/linneo-$(date +%Y%m%d-%H%M).dump"
trap 'rm -f "$archivo.tmp"' EXIT

corriendo=$(dev ps --status running -q neo4j)
[ -n "$corriendo" ] || dev up -d --wait neo4j
nodos=$(cypher "MATCH (n) RETURN count(n)")
relaciones=$(cypher "MATCH ()-[r]->() RETURN count(r)")

dev stop neo4j
dev run --rm --no-deps -T neo4j neo4j-admin database dump neo4j --to-stdout > "$archivo.tmp"
mv "$archivo.tmp" "$archivo"
echo "$nodos $relaciones" > "$archivo.conteos"
[ -n "$corriendo" ] && dev start neo4j

ls -1t "$destino"/linneo-*.dump | tail -n +$((guardar + 1)) | while read -r viejo; do
  rm -f -- "$viejo" "$viejo.conteos"
done
echo "$(date '+%F %T') $archivo $(du -h "$archivo" | cut -f1), $nodos nodos y $relaciones relaciones"
