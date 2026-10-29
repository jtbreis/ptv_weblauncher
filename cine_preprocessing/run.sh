#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export REPO_ROOT="${REPO_ROOT:-$(cd ../.. && pwd)}"
export PORT="${PORT:-5001}"
if [[ "${1:-}" == "--docker" ]]; then
  exec docker compose up --build
fi
exec env REPO_ROOT="$REPO_ROOT" PORT="$PORT" python app.py
