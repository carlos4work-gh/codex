#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

baseline="scripts/memythos/upstream-overlap-baseline.json"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
overlap_path="codex-rs/core/src/thread_manager.rs"
base="$(jq -r '.base_commit' "$baseline")"

jq '
  .manual_overlaps -= ["codex-rs/core/src/thread_manager.rs"]
' "$baseline" >"$tmp_dir/unclassified-overlap.json"

GIT_INDEX_FILE="$tmp_dir/index" git read-tree "$base"
mode="$(git ls-tree "$base" -- "$overlap_path" | awk '{print $1}')"
blob="$(git rev-parse "HEAD:$overlap_path")"
GIT_INDEX_FILE="$tmp_dir/index" git update-index \
  --add --cacheinfo "$mode,$blob,$overlap_path"
tree="$(GIT_INDEX_FILE="$tmp_dir/index" git write-tree)"
synthetic_upstream="$(printf '%s\n' 'synthetic upstream overlap' | git commit-tree "$tree" -p "$base")"

if MEMYTHOS_OVERLAP_BASELINE="$tmp_dir/unclassified-overlap.json" \
  scripts/memythos/upstream-overlap-gate.sh "$synthetic_upstream" >/dev/null 2>&1; then
  echo "Overlap gate accepted an unclassified manual file" >&2
  exit 1
fi

echo "Upstream overlap gate rejects unclassified manual files."
