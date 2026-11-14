#!/usr/bin/env bash
# Corre un script del repo con el venv del proyecto, desde cualquier cwd.
# En Pixilate se lanza como trabajo de job.ps1:
#   wsl -d Debian -- bash /home/pixilate2/srv/linneo/ops/run.sh pipeline/01_backbone.py
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
exec .venv/bin/python "$@"
