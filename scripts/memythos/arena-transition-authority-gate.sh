#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
search_root="$repo_root/codex-rs/app-server/src/request_processors"

matches="$(rg -n '\.transition\((ArenaCommand|command)' "$search_root" \
  --glob '!memythos_arena_state.rs' \
  --glob '!memythos_runtime_state.rs' || true)"

if [[ -n "$matches" ]]; then
  printf '%s\n' "Direct Arena reducer transitions must use MemythosRuntimeState::transition_arena_lifecycle:" >&2
  printf '%s\n' "$matches" >&2
  exit 1
fi

printf '%s\n' "Arena transition authority gate passed."
