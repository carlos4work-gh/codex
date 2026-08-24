#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

processor="codex-rs/app-server/src/request_processors/memythos_processor.rs"
processor_max="${MEMYTHOS_PROCESSOR_MAX_LINES:-4665}"
module_max="${MEMYTHOS_MODULE_MAX_LINES:-3000}"
processor_lines="$(wc -l <"$processor" | tr -d ' ')"

if ((processor_lines > processor_max)); then
  echo "Memythos processor boundary exceeded: $processor_lines > $processor_max lines" >&2
  exit 1
fi

while IFS= read -r module; do
  module_lines="$(wc -l <"$module" | tr -d ' ')"
  if ((module_lines > module_max)); then
    echo "Memythos module boundary exceeded: $module has $module_lines > $module_max lines" >&2
    exit 1
  fi
done < <(
  find codex-rs/app-server/src/request_processors -maxdepth 2 \
    -type f -name '*.rs' \
    \( -name 'memythos_*.rs' -o -path '*/memythos_processor/*.rs' \) \
    ! -name 'memythos_processor.rs' \
    ! -name 'memythos_processor_tests.rs' \
    -print | sort
)

echo "Memythos processor boundary passed: $processor_lines/$processor_max lines."
