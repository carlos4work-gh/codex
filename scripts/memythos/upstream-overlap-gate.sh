#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

baseline="${MEMYTHOS_OVERLAP_BASELINE:-scripts/memythos/upstream-overlap-baseline.json}"
upstream_ref="${1:-upstream/main}"

if ! git rev-parse --verify "$upstream_ref" >/dev/null 2>&1; then
  echo "Missing $upstream_ref. Fetch upstream before running this gate." >&2
  exit 2
fi

for command in git jq comm sort; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Required command not found: $command" >&2
    exit 2
  fi
done

jq -e '
  .schema_version == 1
  and (.base_commit | type == "string")
  and (.maximum_manual_overlap_count | type == "number")
  and (.manual_overlaps | type == "array")
' "$baseline" >/dev/null

base_commit="$(git merge-base HEAD "$upstream_ref")"
expected_base="$(jq -r '.base_commit' "$baseline")"
if [[ "$base_commit" != "$expected_base" ]]; then
  echo "Fork base changed: expected $expected_base, found $base_commit" >&2
  echo "Reclassify the overlap intentionally before updating the baseline." >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

git diff --name-only "$base_commit"..HEAD | sort -u >"$tmp_dir/fork-files"
git diff --name-only "$base_commit".."$upstream_ref" | sort -u >"$tmp_dir/upstream-files"
comm -12 "$tmp_dir/fork-files" "$tmp_dir/upstream-files" >"$tmp_dir/overlaps"

: >"$tmp_dir/mechanical"
: >"$tmp_dir/manual"
while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  if jq -e --arg path "$path" '
    (.mechanical_exact | index($path)) != null
    or ([.mechanical_prefixes[] as $prefix | $path | startswith($prefix)] | any)
  ' "$baseline" >/dev/null; then
    echo "$path" >>"$tmp_dir/mechanical"
  else
    echo "$path" >>"$tmp_dir/manual"
  fi
done <"$tmp_dir/overlaps"

jq -r '.manual_overlaps[]' "$baseline" | sort -u >"$tmp_dir/allowed-manual"
comm -23 "$tmp_dir/manual" "$tmp_dir/allowed-manual" >"$tmp_dir/unclassified"
manual_count="$(wc -l <"$tmp_dir/manual" | tr -d ' ')"
maximum_manual="$(jq -r '.maximum_manual_overlap_count' "$baseline")"

if [[ -s "$tmp_dir/unclassified" ]]; then
  echo "New manual upstream overlaps require classification:" >&2
  sed 's/^/  - /' "$tmp_dir/unclassified" >&2
  exit 1
fi

if ((manual_count > maximum_manual)); then
  echo "Manual overlap budget exceeded: $manual_count > $maximum_manual" >&2
  exit 1
fi

fork_head="$(git rev-parse HEAD)"
upstream_head="$(git rev-parse "$upstream_ref")"
overlap_count="$(wc -l <"$tmp_dir/overlaps" | tr -d ' ')"
mechanical_count="$(wc -l <"$tmp_dir/mechanical" | tr -d ' ')"

jq -n \
  --arg base_commit "$base_commit" \
  --arg fork_head "$fork_head" \
  --arg upstream_head "$upstream_head" \
  --argjson overlap_count "$overlap_count" \
  --argjson mechanical_overlap_count "$mechanical_count" \
  --argjson manual_overlap_count "$manual_count" \
  --argjson maximum_manual_overlap_count "$maximum_manual" \
  --rawfile manual_overlaps "$tmp_dir/manual" \
  '{
    base_commit: $base_commit,
    fork_head: $fork_head,
    upstream_head: $upstream_head,
    overlap_count: $overlap_count,
    mechanical_overlap_count: $mechanical_overlap_count,
    manual_overlap_count: $manual_overlap_count,
    maximum_manual_overlap_count: $maximum_manual_overlap_count,
    manual_overlaps: ($manual_overlaps | split("\n") | map(select(length > 0)))
  }'

echo "Upstream overlap gate passed: $manual_count/$maximum_manual manual overlaps."
