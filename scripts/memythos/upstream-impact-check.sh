#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if ! git rev-parse --verify upstream/main >/dev/null 2>&1; then
  echo "Missing upstream/main. Run: git fetch upstream --prune" >&2
  exit 2
fi

current_branch="$(git branch --show-current)"
echo "Memythos upstream impact check"
echo "repo: $repo_root"
echo "branch: ${current_branch:-detached}"
echo

echo "Remotes"
git remote -v
echo

echo "Worktree"
git status --short --branch
echo

echo "Ahead/behind current branch vs upstream/main"
read -r left_count right_count < <(git rev-list --left-right --count HEAD...upstream/main)
echo "ahead_of_upstream: $left_count"
echo "behind_upstream: $right_count"
echo

watch_paths=(
  "codex-rs/app-server"
  "codex-rs/app-server-client"
  "codex-rs/app-server-daemon"
  "codex-rs/app-server-protocol"
  "codex-rs/app-server-transport"
  "codex-rs/ext/goal"
  "codex-rs/protocol"
)

echo "Changed files in Memythos-sensitive surfaces"
git diff --name-status HEAD...upstream/main -- "${watch_paths[@]}" || true
echo

echo "Recent upstream commits touching Memythos-sensitive surfaces"
git log --oneline --decorate --max-count=25 HEAD..upstream/main -- "${watch_paths[@]}" || true
echo

cat <<'EOF'
Suggested regression gates before integrating upstream:
- Build app-server/protocol/client crates.
- Start daemon in isolated runtime home.
- Run a live thread that emits human_highlight and technical_detail separately.
- Verify steering does not treat supervisor observation as human instruction.
- Verify parent rollup -> parent contract -> child resume does not reopen closed decisions.
- Verify clean thread close is recorded without force-close debt.
EOF
