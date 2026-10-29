#!/usr/bin/env bash
# Make repo Python packages importable for native (non-Docker) launcher jobs.
REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
export REPO_ROOT
export PYTHONPATH="${REPO_ROOT}/4BE-ETI/src:${REPO_ROOT}:${REPO_ROOT}/Post-Processing:${PYTHONPATH:-}"
