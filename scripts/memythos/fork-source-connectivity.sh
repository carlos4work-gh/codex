#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

require_pattern() {
  local file="$1"
  local pattern="$2"
  if ! rg --quiet --fixed-strings "$pattern" "$file"; then
    echo "Disconnected Memythos source: expected '$pattern' in $file" >&2
    exit 1
  fi
}

require_pattern codex-rs/app-server-protocol/src/protocol/v2/mod.rs "mod memythos;"
require_pattern codex-rs/app-server-protocol/src/protocol/common.rs "MemythosArena"
require_pattern codex-rs/app-server/src/request_processors.rs "mod memythos_processor;"
require_pattern codex-rs/app-server/src/request_processors.rs "mod memythos_arena_state;"
require_pattern codex-rs/app-server/src/message_processor.rs "MemythosRequestProcessor"
require_pattern codex-rs/app-server/tests/suite/v2/mod.rs "mod memythos_arena_recovery;"
require_pattern codex-rs/state/src/runtime.rs "mod arena_snapshots;"
require_pattern codex-rs/state/src/runtime.rs "mod native_mailbox;"
require_pattern codex-rs/cli/src/main.rs "mod memythos_sniff;"

duplicate_migrations="$({
  find codex-rs/state/migrations -maxdepth 1 -name '*.sql' -print
} | sed -E 's#^.*/([0-9]+)_.*#\1#' | sort | uniq -d)"
if [[ -n "$duplicate_migrations" ]]; then
  echo "Duplicate state migration versions: $duplicate_migrations" >&2
  exit 1
fi

echo "Memythos fork sources are connected and migration versions are unique."
