#!/usr/bin/env bash
# Progressive box_size relaxation with per-block velocity references.
#
# Usage:
#   OUTPUT_DIR=/mnt/ssd OUTPUT_BASE=data/... CASE=... RUNS=Run1 \
#     ./ptv_weblauncher/run_box_relaxed_tracking.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
cd "$REPO_ROOT"

OUTPUT_DIR="${OUTPUT_DIR:-/mnt/ssd}"
OUTPUT_BASE="${OUTPUT_BASE:-data/Windtunnel_PTV_TNTI/X300cm_PTV_above_TNTI}"
CASE="${CASE:-TNTI_opposing_gravity}"
RUNS="${RUNS:-Run1}"
STM_FILENAME="${STM_FILENAME:-rays_out_cpp.h5}"
MAX_STEREO_ERROR="${MAX_STEREO_ERROR:-0.1}"
BOX_SIZE_MIN="${BOX_SIZE_MIN:-0.15}"
BOX_SIZE_MAX="${BOX_SIZE_MAX:-0.5}"
BOX_SIZE_STEP="${BOX_SIZE_STEP:-0.05}"
VELOCITY_VARIABLE="${VELOCITY_VARIABLE:-vx}"
VELOCITY_TARGET="${VELOCITY_TARGET:-}"
VELOCITY_SIGMA="${VELOCITY_SIGMA:-1.0}"
VELOCITY_BIN_COUNT="${VELOCITY_BIN_COUNT:-0}"
VELOCITY_BAND_MARGIN="${VELOCITY_BAND_MARGIN:-2.0}"
DT="${DT:-1e-3}"
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
  echo "=== Box relaxed tracking: $STM_PATH ==="
  export PYTHONPATH="$REPO_ROOT:$REPO_ROOT/4BE-ETI/src:${PYTHONPATH:-}"
  if ! python3 -c "import pandas" 2>/dev/null; then
    echo "Installing tracking Python deps (pandas, tqdm)..."
    pip install -q -r "$REPO_ROOT/ptv_weblauncher/requirements.txt"
  fi
  ARGS=(
    --input "$STM_PATH"
    --output-dir "$RUN_DIR"
    --max-stereo-error "$MAX_STEREO_ERROR"
    --box-size-min "$BOX_SIZE_MIN"
    --box-size-max "$BOX_SIZE_MAX"
    --box-size-step "$BOX_SIZE_STEP"
    --velocity-variable "$VELOCITY_VARIABLE"
    --velocity-sigma "$VELOCITY_SIGMA"
    --velocity-bin-count "$VELOCITY_BIN_COUNT"
    --velocity-band-margin "$VELOCITY_BAND_MARGIN"
    --dt "$DT"
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
  if [[ -n "$VELOCITY_TARGET" ]]; then
    ARGS+=(--velocity-target "$VELOCITY_TARGET")
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
  python3 "$REPO_ROOT/graph_tracking/run_box_relaxed_tracking.py" "${ARGS[@]}"
done

echo "Box relaxed tracking finished."
