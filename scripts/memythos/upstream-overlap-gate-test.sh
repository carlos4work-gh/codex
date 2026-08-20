#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

baseline="scripts/memythos/upstream-overlap-baseline.json"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

jq '
  .manual_overlaps -= ["codex-rs/core/src/thread_manager.rs"]
' "$baseline" >"$tmp_dir/unclassified-overlap.json"

if MEMYTHOS_OVERLAP_BASELINE="$tmp_dir/unclassified-overlap.json" \
  scripts/memythos/upstream-overlap-gate.sh >/dev/null 2>&1; then
  echo "Overlap gate accepted an unclassified manual file" >&2
  exit 1
fi

echo "Upstream overlap gate rejects unclassified manual files."
