#!/usr/bin/env bash
# docker compose de producción (proyecto linneo-prod) desde cualquier cwd:
#   ops/prod.sh up -d --build | ps | logs -f api | down
set -euo pipefail
cd "$(dirname "$0")"
exec docker compose -f compose.yml -f compose.prod.yml "$@"
