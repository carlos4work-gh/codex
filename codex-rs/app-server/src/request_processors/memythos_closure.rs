use std::collections::HashSet;

use codex_app_server_protocol::MemythosArenaCompositionCoordination;
use codex_app_server_protocol::MemythosArenaCompositionLease;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosArenaResumeExecutionMode;
use codex_app_server_protocol::MemythosParentRole;

use crate::request_processors::memythos_composition::is_competitive_method;
use crate::request_processors::memythos_judge::native_judge_next_action;
use crate::request_processors::memythos_observability::native_token_usage_key;
use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;

#[derive(Debug, Clone)]
pub(super) struct ArenaClosureCandidate {
    pub(super) arena_id: String,
    pub(super) layer_id: String,
    pub(super) parent_thread_ids: Vec<String>,
    pub(super) outcome: ArenaTerminalOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArenaTerminalOutcome {
    Close,
    ParentRollup,
}
pub(super) fn arena_round_key(arena_id: &str, round_id: &str) -> String {
    format!("{arena_id}::{round_id}")
}
pub(super) fn arena_coordination_and_leases<'a>(
    state: &'a MemythosRuntimeState,
    arena_id: &str,
) -> Option<(
    &'a MemythosArenaCompositionCoordination,
    &'a [MemythosArenaCompositionLease],
)> {
    if let Some(composition) = state.arena_compositions.get(arena_id) {
        return Some((&composition.contract.coordination, &composition.leases));
    }
    state
        .restored_coordination_snapshots
        .get(arena_id)
        .map(|snapshot| (&snapshot.coordination, snapshot.leases.as_slice()))
}
pub(super) fn arena_closure_candidate(
    state: &MemythosRuntimeState,
    arena_id: &str,
    completion_trigger_thread_id: &str,
) -> Option<ArenaClosureCandidate> {
    let arena = state.arenas.get(arena_id)?;
    if arena.lifecycle_state == MemythosArenaLifecycleState::ClosedCleanly {
        return None;
    }
    let (coordination, leases) = arena_coordination_and_leases(state, arena_id)?;
    let coordinator_id = coordination.concierge_participant_id.as_ref()?;
    let coordinator_thread_id = leases
        .iter()
        .find(|lease| &lease.participant_id == coordinator_id)?
        .thread_id
        .as_str();
    if !leases
        .iter()
        .any(|lease| lease.thread_id == completion_trigger_thread_id)
    {
        return None;
    }
    let active_round_id = state
        .arena_message_deliveries
        .iter()
        .rev()
        .find(|delivery| {
            delivery.arena_id == arena_id
                && delivery.receiver_thread_id == coordinator_thread_id
                && delivery.delivered_as_human_instruction
        })?
        .round_id
        .as_str();
    let deliveries = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| delivery.arena_id == arena_id && delivery.round_id == active_round_id)
        .collect::<Vec<_>>();
    if deliveries.is_empty()
        || deliveries
            .iter()
            .any(|delivery| delivery.rejection_reason.is_some())
        || deliveries.iter().any(|delivery| {
            delivery
                .receiver_turn_id
                .as_deref()
                .is_some_and(|turn_id| turn_id != "mailbox_queued")
                && delivery.status != "receiver_turn_completed"
        })
    {
        return None;
    }

    let mut terminal_outcome = ArenaTerminalOutcome::Close;
    if is_competitive_method(coordination.decision_method) {
        let policy = coordination.round_policy.as_ref()?;
        let minimum_positions = policy.minimum_competing_positions as usize;
        let distinct_completed_targets = |phase: &str| {
            deliveries
                .iter()
                .filter(|delivery| delivery.phase.as_deref() == Some(phase))
                .map(|delivery| delivery.receiver_thread_id.as_str())
                .collect::<HashSet<_>>()
                .len()
        };
        let judge_id = coordination.judge_participant_id.as_ref()?;
        let judge_thread_id = leases
            .iter()
            .find(|lease| &lease.participant_id == judge_id)?
            .thread_id
            .as_str();
        let eligible_winner_ids = leases
            .iter()
            .filter(|lease| lease.role == MemythosParentRole::Bettor.as_wire())
            .map(|lease| lease.participant_id.as_str())
            .collect::<HashSet<_>>();
        let execution_plan = state
            .arena_resume_execution_plans
            .get(&arena_round_key(arena_id, active_round_id));
        if execution_plan.is_some_and(|plan| {
            plan.mode == MemythosArenaResumeExecutionMode::ReassessAffectedPositions
        }) {
            let execution_plan = execution_plan.expect("partial resume plan was checked above");
            let affected_ids = execution_plan
                .affected_participant_ids
                .iter()
                .collect::<HashSet<_>>();
            let expected_affected_threads = leases
                .iter()
                .filter(|lease| {
                    lease.role == MemythosParentRole::Bettor.as_wire()
                        && affected_ids.contains(&lease.participant_id)
                })
                .map(|lease| lease.thread_id.as_str())
                .collect::<HashSet<_>>();
            let completed_affected_threads = deliveries
                .iter()
                .filter(|delivery| {
                    delivery.phase.as_deref() == Some("resume_reassessment")
                        && delivery.receiver_thread_id != judge_thread_id
                        && delivery.status == "receiver_turn_completed"
                })
                .map(|delivery| delivery.receiver_thread_id.as_str())
                .collect::<HashSet<_>>();
            if expected_affected_threads.is_empty()
                || completed_affected_threads != expected_affected_threads
            {
                return None;
            }
        } else {
            if distinct_completed_targets("proposal") < minimum_positions
                || distinct_completed_targets("peer_review_and_objection") < minimum_positions
                || distinct_completed_targets("bet") < minimum_positions
                || distinct_completed_targets("judge") < 1
            {
                return None;
            }
        }
        terminal_outcome = deliveries.iter().find_map(|delivery| {
            if delivery.receiver_thread_id != judge_thread_id
                || !matches!(
                    delivery.phase.as_deref(),
                    Some("judge") | Some("final_judge")
                )
                || delivery.status != "receiver_turn_completed"
            {
                return None;
            }
            let turn_id = delivery.receiver_turn_id.as_deref()?;
            let text = state
                .native_parent_turn_responses
                .get(&native_token_usage_key(judge_thread_id, turn_id))?
                .text
                .as_deref()?;
            match native_judge_next_action(text, &eligible_winner_ids).as_deref() {
                Some("close") => Some(ArenaTerminalOutcome::Close),
                Some("parent_rollup") => Some(ArenaTerminalOutcome::ParentRollup),
                _ => None,
            }
        })?;
    }

    Some(ArenaClosureCandidate {
        arena_id: arena_id.to_string(),
        layer_id: arena.layer_id.clone(),
        parent_thread_ids: leases.iter().map(|lease| lease.thread_id.clone()).collect(),
        outcome: terminal_outcome,
    })
}
