# App-Server Extension Strategy

Memythos should treat Codex app-server as the runtime host and add layered agentic behavior through explicit protocol and extension seams.

## Functional Target

The target is not "run a batch prompt and parse the result." The target is a live, steerable runtime where multiple agent threads can work, expose progress, ask for parent definitions, resume from gates, and close cleanly.

Memythos layers should be first-class runtime entities:

- strategic/business layer;
- BPM/end-to-end layer;
- tactical/operational layer;
- implementation/reality layer;
- human discovery/settlement interaction.

Each layer can use the same lower-level runtime but with different arena policy, role generation, escalation rules, and settlement expectations.

## Why App-Server

App-server already has the expensive primitives that Memythos should not rebuild:

- long-lived thread state;
- turn processing and steering;
- event streaming;
- goal services;
- extension registry;
- daemon/runtime ownership;
- client backpressure handling;
- telemetry and notifications.

Memythos should add semantics above those primitives:

- which layer is speaking;
- which arena is active;
- whether an event is a human highlight or technical detail;
- whether a gate blocks execution, promotion, or parent definition;
- which state machine owns the transition.

## Proposed Extension Boundary

Start with app-server protocol extensions before changing core agent behavior.

Add protocol concepts for:

- `memythos_layer_context`;
- `memythos_arena_id`;
- `memythos_participant_id`;
- `memythos_event_channel`: `human_highlight`, `technical_detail`, `artifact_payload`;
- `memythos_lifecycle_state`;
- `memythos_parent_rollup_request`;
- `memythos_resume_contract`;
- `memythos_promotion_decision`.

The app-server can carry these as explicit metadata while existing Codex internals continue handling model/tool execution.

## State Machines

Decision rules should be state machines, not loose `if/else` flags.

Initial machines:

- thread lifecycle: `running -> artifact_complete -> close_requested -> closed_cleanly`;
- parent rollup: `detect_gap -> request_parent -> await_contract -> resume_child -> validate_resume`;
- promotion: `evaluate_runtime -> evaluate_promotion -> accepted_poc_debt | reject_for_promotion`;
- channel assembly: `capture_event -> classify_channel -> compact_or_route -> persist`.

If a transition is important enough to affect the human, parent layer, or downstream agent, it should be visible in state.

## Runtime Strategy

Use app-server for live guided work:

- human-in-the-loop;
- supervisor observation;
- multi-agent debate with incremental steering;
- parent rollup and resume.

Use CLI/batch runners for bounded work:

- isolated reflection;
- artifact generation;
- regression fixtures;
- background research segments that do not need live steering.

The runtime should choose strategy by work type, not by convenience.

## Cost Discipline

The agent should not be asked to produce large internal checklists. Those belong to runtime state, tool logs, or compact artifacts.

Default outputs should be:

- compact human highlight;
- technical detail only when needed;
- final artifact payload;
- state transition record.

Long tool logs should remain available but not continuously fed back into agents unless an alert or review policy requires it.
