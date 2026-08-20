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
require_pattern codex-rs/core/src/lib.rs "mod durable_inter_agent_mailbox;"
require_pattern codex-rs/cli/src/main.rs "mod memythos_sniff;"

durable_mailbox_service="codex-rs/core/src/durable_inter_agent_mailbox.rs"
if rg --quiet 'Memythos' "$durable_mailbox_service"; then
  echo "Durable inter-agent mailbox must remain domain-neutral" >&2
  exit 1
fi

for integration_file in \
  codex-rs/core/src/thread_manager.rs \
  codex-rs/core/src/session/mod.rs; do
  if rg --quiet \
    'insert_pending_native_mailbox_communication|claim_native_mailbox_communication_for_recovery|mark_native_mailbox_communication_consumed' \
    "$integration_file"; then
    echo "Durable mailbox repository access escaped into $integration_file" >&2
    exit 1
  fi
done

rpc_count=0
while IFS= read -r variant; do
  [[ -n "$variant" ]] || continue
  rpc_count=$((rpc_count + 1))
  require_pattern codex-rs/app-server/src/message_processor.rs "ClientRequest::$variant"
done < <(
  sed -nE 's/^[[:space:]]*(Memythos[A-Za-z0-9]+) =>.*/\1/p' codex-rs/app-server-protocol/src/protocol/common.rs | sort -u
)

if ((rpc_count < 40)); then
  echo "Memythos RPC inventory unexpectedly small: $rpc_count" >&2
  exit 1
fi

duplicate_migrations="$({
  find codex-rs/state/migrations -maxdepth 1 -name '*.sql' -print
} | sed -E 's#^.*/([0-9]+)_.*#\1#' | sort | uniq -d)"
if [[ -n "$duplicate_migrations" ]]; then
  echo "Duplicate state migration versions: $duplicate_migrations" >&2
  exit 1
fi

echo "Memythos fork sources, $rpc_count RPC handlers, and migration versions are connected."
