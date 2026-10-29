#!/usr/bin/env bash
# PDF-gated iterative tracking from a high-threshold STM file.
#
# Usage:
#   OUTPUT_DIR=/mnt/ssd OUTPUT_BASE=data/... CASE=... RUNS=Run1 \
#     ./ptv_weblauncher/run_pdf_relaxed_tracking.sh
#
# Env:
#   REPO_ROOT, OUTPUT_DIR, OUTPUT_BASE, CASE, RUNS
#   STM_FILENAME (default rays_out_cpp.h5)
#   INITIAL_MAX_ERROR, MAX_ERROR, ERROR_STEP
#   PDF_VARIABLE, PDF_PERCENTILE_LO, PDF_PERCENTILE_HI
#   DT, BOX_SIZE_TRACK, WORKERS, MAX_BLOCKS, MIN_TRACK_LENGTH
#   BOX_SIZE_INITIAL_X_LO, BOX_SIZE_INITIAL_X_HI, BOX_SIZE_Y, BOX_SIZE_Z
#   MAX_BRANCH, MAX_SEEDS, USE_BIDIRECTIONAL_GRAPH, NO_3_FRAME

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
cd "$REPO_ROOT"

OUTPUT_DIR="${OUTPUT_DIR:-/mnt/ssd}"
OUTPUT_BASE="${OUTPUT_BASE:-data/Windtunnel_PTV_TNTI/X300cm_PTV_above_TNTI}"
CASE="${CASE:-TNTI_opposing_gravity}"
RUNS="${RUNS:-Run1}"
STM_FILENAME="${STM_FILENAME:-rays_out_cpp.h5}"
INITIAL_MAX_ERROR="${INITIAL_MAX_ERROR:-0.05}"
MAX_ERROR="${MAX_ERROR:-0}"
ERROR_STEP="${ERROR_STEP:-0.01}"
PDF_VARIABLE="${PDF_VARIABLE:-vx}"
PDF_PERCENTILE_LO="${PDF_PERCENTILE_LO:-1}"
PDF_PERCENTILE_HI="${PDF_PERCENTILE_HI:-99}"
DT="${DT:-1e-3}"
BOX_SIZE_TRACK="${BOX_SIZE_TRACK:-0.2}"
WORKERS="${WORKERS:-4}"
MAX_BLOCKS="${MAX_BLOCKS:-0}"
MIN_TRACK_LENGTH="${MIN_TRACK_LENGTH:-3}"
REP_RATE="${REP_RATE:-10}"
POSITION_UNIT="${POSITION_UNIT:-mm}"
BOX_SIZE_INITIAL_X_LO="${BOX_SIZE_INITIAL_X_LO:-0.0}"
BOX_SIZE_INITIAL_X_HI="${BOX_SIZE_INITIAL_X_HI:-3.5}"
BOX_SIZE_Y="${BOX_SIZE_Y:-1.5}"
BOX_SIZE_Z="${BOX_SIZE_Z:-1.5}"
MAX_BRANCH="${MAX_BRANCH:-30}"
MAX_SEEDS="${MAX_SEEDS:-0}"

IFS=',' read -ra RUN_ARR <<< "$RUNS"
for RUN in "${RUN_ARR[@]}"; do
  RUN="$(echo "$RUN" | xargs)"
  [[ -z "$RUN" ]] && continue
  RUN_DIR="$OUTPUT_DIR/$OUTPUT_BASE/$CASE/$RUN"
  STM_PATH="$RUN_DIR/$STM_FILENAME"
  if [[ ! -f "$STM_PATH" ]]; then
    echo "ERROR: STM file not found: $STM_PATH" >&2
    exit 1
  fi
  echo "=== PDF relaxed tracking: $STM_PATH ==="
  export PYTHONPATH="$REPO_ROOT:$REPO_ROOT/4BE-ETI/src:${PYTHONPATH:-}"
  if ! python3 -c "import pandas" 2>/dev/null; then
    echo "Installing PDF tracking Python deps (pandas, tqdm)..."
    pip install -q -r "$REPO_ROOT/ptv_weblauncher/requirements.txt"
  fi
  ARGS=(
    --input "$STM_PATH"
    --output-dir "$RUN_DIR"
    --initial-max-error "$INITIAL_MAX_ERROR"
    --error-step "$ERROR_STEP"
    --pdf-variable "$PDF_VARIABLE"
    --pdf-percentile-lo "$PDF_PERCENTILE_LO"
    --pdf-percentile-hi "$PDF_PERCENTILE_HI"
    --dt "$DT"
    --box-size-track "$BOX_SIZE_TRACK"
    --workers "$WORKERS"
    --min-track-length "$MIN_TRACK_LENGTH"
    --rep-rate "$REP_RATE"
    --position-unit "$POSITION_UNIT"
    --initial-x-lo "$BOX_SIZE_INITIAL_X_LO"
    --initial-x-hi "$BOX_SIZE_INITIAL_X_HI"
    --initial-y-lo "-$BOX_SIZE_Y"
    --initial-y-hi "$BOX_SIZE_Y"
    --initial-z-lo "-$BOX_SIZE_Z"
    --initial-z-hi "$BOX_SIZE_Z"
    --max-branch "$MAX_BRANCH"
    --max-seeds "$MAX_SEEDS"
  )
  if [[ -n "$MAX_ERROR" && "$MAX_ERROR" != "0" ]]; then
    ARGS+=(--max-error "$MAX_ERROR")
  fi
  if [[ -n "$MAX_BLOCKS" && "$MAX_BLOCKS" != "0" ]]; then
    ARGS+=(--max-blocks "$MAX_BLOCKS")
  fi
  if [[ "${NO_3_FRAME:-}" == "1" ]]; then
    ARGS+=(--no-3-frame)
  fi
  if [[ "${USE_BIDIRECTIONAL_GRAPH:-}" == "1" ]]; then
    ARGS+=(--bidirectional)
  fi
  python3 "$REPO_ROOT/graph_tracking/run_pdf_relaxed_tracking.py" "${ARGS[@]}"
done

echo "PDF relaxed tracking finished."
