use std::collections::HashSet;

use codex_app_server_protocol::MemythosArenaMessage;

use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;

pub(super) fn native_phase_turn_refs(
    state: &MemythosRuntimeState,
    arena_id: &str,
    round_id: &str,
    phase: &str,
    eligible_thread_ids: &HashSet<&str>,
) -> Vec<String> {
    let mut refs = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| {
            delivery.arena_id == arena_id
                && delivery.round_id == round_id
                && delivery.phase.as_deref() == Some(phase)
                && eligible_thread_ids.contains(delivery.receiver_thread_id.as_str())
                && delivery.rejection_reason.is_none()
                && delivery.failure_reason.is_none()
        })
        .filter_map(|delivery| {
            delivery.receiver_turn_id.as_ref().map(|turn_id| {
                format!(
                    "app-server://threads/{}/turns/{turn_id}",
                    delivery.receiver_thread_id
                )
            })
        })
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    refs
}

pub(super) fn native_arena_parent_task_contract(
    state: &MemythosRuntimeState,
    arena_id: &str,
    thread_id: &str,
) -> Option<String> {
    let composition = state.arena_compositions.get(arena_id)?;
    let lease = composition
        .leases
        .iter()
        .find(|lease| lease.thread_id == thread_id)?;
    let participant = composition
        .contract
        .participants
        .iter()
        .find(|participant| participant.participant_id == lease.participant_id)?;
    if composition.applied_revision.is_some() {
        let final_validation_boundaries = if participant.agent_role == "judge" {
            format!(
                "\nFinal validation boundaries for this verdict:\n- {}\nValidate the verdict against every boundary. Preserve any exact predicate or invariant verbatim in the corresponding structured verdict field; do not replace it with a newly derived trigger.",
                composition.contract.completion_criteria.join("\n- "),
            )
        } else {
            String::new()
        };
        Some(format!(
            "Native current task delta for a revised arena composition:\nBounded revised objective: {}\nCurrent role objective: {}\nExpected changed contribution: {}\nExit condition: {}.\nThe arena's previously registered completion criteria remain authoritative validation boundaries. Do not restate, re-argue, or summarize them unless new evidence changes one or a criterion blocks this contribution. Focus the response on the semantic delta created by the revised objective.{}",
            composition.contract.shared_objective,
            participant.role_objective,
            participant.expected_contribution,
            participant.exit_condition,
            final_validation_boundaries,
        ))
    } else {
        Some(format!(
            "Native current task contract:\nShared arena objective: {}\nMandatory completion criteria:\n- {}\nCurrent role objective: {}\nExpected contribution: {}\nExit condition: {}.",
            composition.contract.shared_objective,
            composition.contract.completion_criteria.join("\n- "),
            participant.role_objective,
            participant.expected_contribution,
            participant.exit_condition,
        ))
    }
}

pub(super) fn append_native_arena_parent_task_contract(
    state: &MemythosRuntimeState,
    message: &mut MemythosArenaMessage,
) {
    let Some(task_contract) =
        native_arena_parent_task_contract(state, &message.arena_id, &message.to_parent_thread_id)
    else {
        return;
    };
    let execution_prompt = message
        .execution_prompt
        .take()
        .unwrap_or_else(|| message.human_summary.clone());
    message.execution_prompt = Some(format!("{execution_prompt}\n\n{task_contract}"));
}
