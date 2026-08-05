# Memythos OOTB Boundary Audit - 2026-08-05

## Executive Finding

Memythos is no longer a small app-server adapter. The fork is a substantial
native extension: 111 commits, 159 changed files, 28,072 additions, and 308
deletions relative to `6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9`.

Most new volume is namespaced protocol, generated schema, tests, and one large
app-server processor. That is structurally preferable to an external sidecar.
However, the fork also changes Codex core session scheduling, root-role
configuration, protocol communication, rollout persistence, and thread-store
metadata. Those are deep runtime changes and materially increase rebase and
regression risk.

The architecture remains defensible only if the deep changes are treated as a
small set of missing generic primitives, not as a license to move arena policy
into Codex core.

## Quantitative Surface

| Surface | Approximate delta | Assessment |
| --- | ---: | --- |
| `app-server/request_processors/memythos_processor.rs` | +15,772 | Large but namespaced |
| `app-server-protocol/v2/memythos.rs` | +2,103 | Native extension contract |
| Generated JSON/TS schemas | ~6,000 | Generated consequence |
| Memythos client request variants | 39 | Broad API surface; consolidation needed |
| Core/session/thread changes | 500+ lines plus tests | High-risk fork boundary |
| Total fork | +28,072 / -308 | Deep fork, not routine customization |

## Classification

### A. Keep In App-Server: Correct Boundary

- Namespaced `Memythos*` protocol types and request variants.
- `MemythosRequestProcessor` and its app-server adapters.
- Arena, room, composition, delivery, lifecycle, continuity, and projection
  semantics.
- Dynamic room tools that call the native processor.
- App-server request dispatch and transport-specific notifications.
- Generated schemas and protocol contract tests.

These changes extend app-server as the runtime host and avoid recreating Codex
execution outside the fork.

### B. Keep As Minimal Hooks: Review And Constrain

- `MessageProcessor` construction of a single Memythos processor.
- Small hooks from thread/turn processors into native projections.
- Dynamic tool dispatch to `memythos_room`.
- Parent configuration adapters that use existing thread and goal services.

These hooks are acceptable only when they delegate to OOTB thread, turn, goal,
and tool behavior. They should not contain duplicate state machines.

### C. Deep Core Primitive 1: Independent Root Parent Role

Changed surfaces include:

- `core/src/thread_manager.rs`;
- `core/src/agent/role.rs` and role configuration;
- rollout recorder and thread-store metadata;
- app-server thread start protocol.

Purpose:

- allow an independently owned root thread to select a named role;
- compose stable developer instructions once;
- persist and restore the selected role across resume;
- leave normal child/subagent role derivation unchanged.

Assessment:

- This is a coherent generic Codex primitive, not inherently Memythos-specific.
- It is necessary for independent arena parents if root threads have no OOTB
  role-selection entry point.
- It must remain role selection and persistence only. Arena, stance, authority,
  and debate policy must not be added to core.
- It should be isolated as an upstream-quality patch series with standalone
  tests and a documented compatibility contract.

### D. Deep Core Primitive 2: Independent Parent Mailbox Wake-Up

Changed surfaces include:

- `core/src/session/input_queue.rs`;
- `core/src/tasks/mod.rs`;
- `core/src/thread_manager.rs`;
- `protocol::InterAgentCommunication`.

Purpose:

- deliver native inter-agent communication to an independently owned thread;
- preserve submission/turn identity;
- avoid folding deferred mail into an unrelated active turn;
- wake an idle target thread;
- start deferred work after the current task completes;
- preserve an optional output schema on the triggered turn.

Assessment:

- The capability is required for parent-to-parent loopback without pretending
  one parent is another parent's child.
- The implementation changes core scheduling and therefore has the highest
  concurrency/regression risk in the fork.
- `submission_id`, wake-up, deferred drain, and output-schema propagation must
  remain generic and free of arena concepts.
- Required validation includes concurrent arrivals, active-turn deferral,
  ordering, one-to-one delivery, aggregate delivery, cancellation, resume,
  crash recovery, and no duplicate wake-up.
- This patch series should be separable from Memythos app-server policy.

### E. Candidate Retirement Or Reduction

1. TypeScript arena scheduling, lifecycle, channel assembly, cursor, and
   coordination logic that duplicates native fork behavior.
2. Persisted normal-operation progress exports when native app-server
   projections already supply the information.
3. Compatibility RPC aliases that expose the same underlying operation under
   multiple Memythos request names.
4. Human-channel reconstruction, suppression, regex classification, and
   transcript compaction outside the native projection.
5. Any TS gate that attempts to repair an invalid native contract instead of
   rejecting it with evidence.

Retirement must be evidence-driven. A client may retain replay/debug export,
but exported files cannot be a second source of runtime truth.

## Primary Risks

### 1. Rebase Risk

Core session, task, and thread-manager changes overlap areas likely to evolve
upstream. A future Codex change to mailbox handling or task completion can
silently invalidate Memythos assumptions even if compilation succeeds.

### 2. Scheduling Risk

`trigger_turn` mail can arrive while a task is active. The fork now drains by
submission id and starts pending work after task completion. Ordering,
starvation, duplicate activation, and schema association are correctness
properties, not implementation details.

### 3. API Surface Risk

Thirty-nine Memythos client request variants and a 15k-line processor indicate
that one module now owns too many concerns. Namespacing avoids upstream
pollution but does not by itself provide maintainability.

### 4. Semantic Duplication Risk

The main Memythos repository still contains a broad TS legacy surface. If both
Rust and TS decide routing, phases, lifecycle, promotion, or conversation
projection, the system can pass tests while exercising the wrong authority.

### 5. False-Evidence Risk

The latest E2E is real but did not run semantic reviewers after promotion
failed. Diagnostic review and promotion authority need separate lifecycle
states so a failed run still produces causal evidence.

## Required Refactor Boundaries

### App-Server Processor Decomposition

Split the large processor internally without creating services outside
app-server:

```text
memythos/
  composition
  provisioning
  delivery
  phase_lifecycle
  contracts
  continuity
  projections
  telemetry
```

Each module should have one state authority and focused contract tests. Public
RPC compatibility can remain while internal ownership is separated.

### Core Patch Isolation

Maintain two explicit, reviewable core patch stacks:

1. root role selection and persistence;
2. independent-thread mailbox delivery and wake-up.

No Memythos type may appear in those core patches. App-server adapters consume
the generic primitives.

### TS Reduction

Classify each TS module as exactly one of:

- native client;
- E2E harness;
- report/viewer;
- compatibility pending retirement.

No new production runtime authority should be added in TS. Compatibility code
must name the native primitive that replaces it and the deletion gate.

## Validation Gates Before More Features

1. Rust regression suite for root role start, persistence, resume, and child
   role non-regression.
2. Rust concurrency suite for mailbox delivery, active-turn deferral, ordering,
   schema preservation, cancellation, and clean close.
3. Contract tests proving each TS production call maps to one native RPC.
4. No runtime state reconstructed from filenames, regex, or timestamps.
5. Focused Docker E2E proving a parent resumes without reopening protected
   decisions.
6. Diagnostic semantic reviewers run even when promotion is blocked.
7. Full multidomain Docker E2E with no deterministic fallback.
8. Upstream rebase rehearsal with the two core patch stacks applied separately.

## Recommendation

Freeze new Memythos features temporarily. Complete the resume-contract fix and
then spend the next increment reducing architectural ambiguity:

1. isolate and harden the two generic core primitives;
2. decompose the app-server processor without changing behavior;
3. inventory and retire duplicate TS runtime authority;
4. rerun focused and multidomain Docker evidence;
5. only resume feature growth after the fork boundary is demonstrably stable.

The fork is not inherently the wrong choice. The current risk comes from its
depth and duplicated authority, not from placing Memythos inside app-server.
