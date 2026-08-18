# Memythos Fork Operations

This repository is the Memythos fork of OpenAI Codex. The fork is not a copy-paste vendor drop: it must keep a visible relationship with upstream so protocol, app-server, daemon, telemetry, and agent runtime changes can be evaluated before they affect Memythos behavior.

## Remote Contract

- `origin`: Memythos fork, `https://github.com/carlos4work-gh/codex.git`.
- `upstream`: OpenAI source, `https://github.com/openai/codex.git`.
- Memythos work happens on `codex/*` branches unless a release branch is explicitly created.
- `main` tracks `upstream/main` and represents the upstream baseline, not the Memythos product branch.

The fork gives Git history, upstream refs, diffing, pull requests, and branch isolation. It does not give governance by itself. Memythos still needs explicit sync windows, regression gates, and a source-of-truth for runtime behavior.

## Surfaces We Own

Memythos should avoid scattering changes across Codex internals. The preferred extension seam is app-server/protocol because it already owns live threads, event streaming, goals, extensions, telemetry, and steering.

Primary surfaces:

- `codex-rs/app-server`: live thread orchestration, event routing, turn lifecycle, extension composition.
- `codex-rs/app-server-protocol`: stable contract for Memythos layer events, channels, arenas, rollups, gates, and lifecycle states.
- `codex-rs/app-server-client`: client/runtime adapter behavior and backpressure handling.
- `codex-rs/app-server-daemon`: daemon lifecycle and socket/runtime ownership.
- `codex-rs/ext/goal`: objective tracking that Memythos can map to layer goals and arena goals.
- `codex-rs/protocol`: only when a behavior belongs below app-server and cannot be modeled as an app-server extension.

## Upstream Sync Policy

Upstream sync is a controlled engineering event, not a background update.

1. Fetch upstream.
2. Inspect impacted surfaces.
3. Classify changes as protocol, lifecycle, telemetry, tool execution, state storage, or UI/client behavior.
4. Run Memythos fork regressions before merging or rebasing.
5. Record the decision in a sync note or PR description.

Do not merge upstream directly into a product branch while an agentic runtime experiment is in progress. Create a sync branch first, for example:

```bash
git switch -c codex/memythos-upstream-sync-YYYYMMDD
```

## Regression Gates

A sync is not acceptable because it compiles. It must preserve the runtime contract that Memythos is building.

Minimum gates:

- App-server daemon starts in the intended environment and does not fall back to host state accidentally.
- A live thread can emit human highlights and technical detail as separate channels.
- A thread can be steered without confusing supervisor observation with human instruction.
- Layer arena state can move through explicit lifecycle states instead of loose flags.
- Parent rollup can request definitions and resume the lower layer without reopening closed decisions.
- Output directories are isolated by run id or timestamp.
- No agent is asked to generate bureaucratic checklist payloads that should be runtime state.

## Branching Model

- `main`: upstream tracking baseline.
- `codex/memythos-runtime-*`: implementation branches.
- `codex/memythos-sync-*`: upstream integration branches.
- `codex/memythos-spike-*`: disposable research branches.

Prefer small fork commits that isolate Memythos behavior from upstream noise. If a change touches upstream-owned internals, document why the existing extension seam was insufficient.

## What Belongs In Memythos Extensions

Memythos additions should be modeled as explicit runtime concepts:

- layer identity and layer goals;
- debate arenas and participant stances;
- state machines for gates, lifecycle, parent rollup, resume, and promotion;
- event channel separation between human highlights and technical detail;
- compact semantic progress signals;
- parent/child contracts for definitions, constraints, and resumed execution.

Avoid encoding these as ad hoc prompt text, regex extraction, or scattered booleans. Prompting can express behavior, but the runtime contract must be visible in protocol/state.

## Practical Commands

Inspect fork state:

```bash
git remote -v
git branch -vv
```

Inspect upstream impact without changing branches:

```bash
scripts/memythos/upstream-impact-check.sh
```

Create an upstream sync branch:

```bash
git fetch upstream --prune
git switch -c codex/memythos-upstream-sync-YYYYMMDD
```

The merge or rebase decision is intentionally not automated. Use the impact report first, then choose the safest integration path.
