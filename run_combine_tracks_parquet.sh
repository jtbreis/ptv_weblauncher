#!/usr/bin/env bash
# Combine selected runs' track .h5part files into one Parquet file.
#
# Usage:
#   OUTPUT_DIR=/mnt/ssd OUTPUT_BASE=data/... CASE=... RUNS=Run1,Run2 \
#     TRACKS_NAME=tracks.h5part ./ptv_weblauncher/run_combine_tracks_parquet.sh
#
# Env:
#   REPO_ROOT, OUTPUT_DIR, OUTPUT_BASE, CASE, RUNS
#   TRACKS_NAME (default tracks.h5part)
#   OUTPUT_NAME (optional parquet filename under case dir)
#   WORKERS (parallel run loaders; default 8)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ptv_weblauncher/setup_repo_python.sh
source "$SCRIPT_DIR/setup_repo_python.sh"
cd "$REPO_ROOT"

OUTPUT_DIR="${OUTPUT_DIR:-/mnt/ssd}"
OUTPUT_BASE="${OUTPUT_BASE:-data/Windtunnel_PTV_TNTI/X300cm_PTV_above_TNTI}"
CASE="${CASE:-}"
RUNS="${RUNS:-}"
TRACKS_NAME="${TRACKS_NAME:-tracks.h5part}"
OUTPUT_NAME="${OUTPUT_NAME:-}"
WORKERS="${WORKERS:-8}"

if [[ -z "$CASE" ]]; then
  echo "ERROR: CASE is required" >&2
  exit 1
fi

if ! python3 -c "import h5py, pandas, pyarrow" 2>/dev/null; then
  echo "Installing combine-tracks Python deps (h5py, pandas, pyarrow, tqdm)..."
  pip install -q -r "$REPO_ROOT/ptv_weblauncher/requirements.txt"
  pip install -q pyarrow
fi

ARGS=(
  --output-dir "$OUTPUT_DIR"
  --output-base "$OUTPUT_BASE"
  --case "$CASE"
  --tracks-name "$TRACKS_NAME"
  --workers "$WORKERS"
)
if [[ -n "$RUNS" ]]; then
  ARGS+=(--runs "$RUNS")
fi
if [[ -n "$OUTPUT_NAME" ]]; then
  ARGS+=(--output-name "$OUTPUT_NAME")
fi

echo "=== Combine tracks → parquet: case=$CASE tracks=$TRACKS_NAME runs=${RUNS:-all} ==="
python3 "$REPO_ROOT/Processing Tools/run_combine_tracks_parquet.py" "${ARGS[@]}"
echo "Combine tracks → parquet finished."
