#!/usr/bin/env bash
# box_size_track sensitivity: graph tracking sweep + PDF analysis.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
cd "$REPO_ROOT"

# shellcheck source=ptv_weblauncher/setup_repo_python.sh
source "$SCRIPT_DIR/setup_repo_python.sh"

export OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/data}"
export OUTPUT_BASE="${OUTPUT_BASE:-data/Windtunnel_PTV_TNTI/X300cm_PTV_above_TNTI}"

if [[ -f "$REPO_ROOT/docker/resolve_data_paths.sh" ]]; then
  # shellcheck source=docker/resolve_data_paths.sh
  source "$REPO_ROOT/docker/resolve_data_paths.sh"
  _resolve_data_paths || true
  export OUTPUT_DIR INPUT_DIR
fi

python3 "Processing Tools/run_box_size_sensitivity_tracking.py" "$@"
