#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if MEMYTHOS_PROCESSOR_MAX_LINES=1 \
  scripts/memythos/processor-boundary-gate.sh >/dev/null 2>&1; then
  echo "Processor boundary gate accepted an oversized processor" >&2
  exit 1
fi

echo "Processor boundary gate rejects size regressions."
