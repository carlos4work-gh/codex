use crate::error_code::invalid_params;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaAggregateContract;
use codex_app_server_protocol::MemythosArenaLateArrivalPolicy;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomParticipant;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadGoalStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoomDeliveryGoalTransition {
    AssignDeliveryGoal,
    PreserveGoal,
}

pub(super) fn room_delivery_goal_transition(
    status: &ThreadGoalStatus,
) -> RoomDeliveryGoalTransition {
    match status {
        ThreadGoalStatus::Paused | ThreadGoalStatus::Complete => {
            RoomDeliveryGoalTransition::AssignDeliveryGoal
        }
        ThreadGoalStatus::Active
        | ThreadGoalStatus::Blocked
        | ThreadGoalStatus::UsageLimited
        | ThreadGoalStatus::BudgetLimited => RoomDeliveryGoalTransition::PreserveGoal,
    }
}

pub(super) fn validate_parent_goal_accepts_delivery(
    goal: &ThreadGoal,
) -> Result<(), JSONRPCErrorError> {
    if goal.status == ThreadGoalStatus::BudgetLimited {
        return Err(invalid_params(format!(
            "parent thread {} exhausted its OOTB goal token budget after {} tokens; preserve the completed work and submit material cost evidence or an explicit expansion through memythos/arena/request so the native planner can choose wrap-up, expansion, or method change",
            goal.thread_id, goal.tokens_used
        )));
    }
    Ok(())
}

pub(super) fn room_delivery_goal_objective(message: &MemythosArenaMessage) -> String {
    let materialization_requirement = if message.message_kind == "human_intake" {
        concat!(
            " This intake is not complete until you invoke the native ",
            "memythos_room_send_message tool and dispatch exactly the assignments authorized by ",
            "the native execution plan in the intake. A prose statement ",
            "that you activated or will activate the arena is not materialized progress and must ",
            "not be reported as completion."
        )
    } else {
        ""
    };
    format!(
        concat!(
            "Complete only native room assignment {message_id} for phase {message_kind}. ",
            "Use the identity, stance, memory, and tools already installed on this parent. ",
            "The delivery input contains the task and its closure boundary; do not restate them. ",
            "Do not advance to another arena phase on your own. When the requested act is complete, ",
            "call update_goal with status complete.{materialization_requirement}"
        ),
        message_id = message.message_id,
        message_kind = message.message_kind,
        materialization_requirement = materialization_requirement,
    )
}

pub(super) fn goal_matches_completed_room_delivery(
    goal: &ThreadGoal,
    message_ids: &[String],
) -> bool {
    goal.status == ThreadGoalStatus::Active
        && message_ids.iter().any(|message_id| {
            goal.objective.starts_with(&format!(
                "Complete only native room assignment {message_id} for phase "
            ))
        })
}

pub(super) fn canonical_native_judge_bet_contract(
    room: &MemythosRoom,
    round_id: &str,
    judge: &MemythosRoomParticipant,
) -> Result<MemythosArenaAggregateContract, JSONRPCErrorError> {
    let expected_source_thread_ids = canonical_bettor_thread_ids(room);
    if expected_source_thread_ids.len() < 2 {
        return Err(invalid_params(format!(
            "competitive room {} requires at least two bettor parents before judge aggregation",
            room.room_id
        )));
    }
    Ok(MemythosArenaAggregateContract {
        aggregate_id: format!("{}::{round_id}::judge_bets", room.room_id),
        recipient_thread_id: judge.thread_id.clone(),
        quorum: expected_source_thread_ids.len() as u32,
        expected_source_thread_ids,
        phase_id: "bet".to_string(),
        deadline_ref: None,
        completion_criteria_ref: format!(
            "app-server://rooms/{}/rounds/{round_id}/checkpoints/all-peer-bets",
            room.room_id
        ),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    })
}

pub(super) fn canonical_native_concierge_phase_contract(
    room: &MemythosRoom,
    round_id: &str,
    concierge: &MemythosRoomParticipant,
    message_kind: &str,
) -> Result<MemythosArenaAggregateContract, JSONRPCErrorError> {
    let phase_id = match message_kind {
        "peer_proposal" => "proposal",
        "peer_review_and_objection" => "peer_review_and_objection",
        _ => {
            return Err(invalid_params(format!(
                "message kind {message_kind} is not a native concierge phase checkpoint"
            )));
        }
    };
    let expected_source_thread_ids = canonical_bettor_thread_ids(room);
    if expected_source_thread_ids.len() < 2 {
        return Err(invalid_params(format!(
            "competitive room {} requires at least two bettor parents before {phase_id} aggregation",
            room.room_id
        )));
    }
    Ok(MemythosArenaAggregateContract {
        aggregate_id: format!("{}::{round_id}::concierge_{phase_id}", room.room_id),
        recipient_thread_id: concierge.thread_id.clone(),
        quorum: expected_source_thread_ids.len() as u32,
        expected_source_thread_ids,
        phase_id: phase_id.to_string(),
        deadline_ref: None,
        completion_criteria_ref: format!(
            "app-server://rooms/{}/rounds/{round_id}/checkpoints/all-{phase_id}",
            room.room_id
        ),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    })
}

pub(super) fn canonical_native_bettor_phase_contract(
    room: &MemythosRoom,
    round_id: &str,
    recipient: &MemythosRoomParticipant,
    source_phase: &str,
) -> Result<MemythosArenaAggregateContract, JSONRPCErrorError> {
    let mut expected_source_thread_ids = canonical_bettor_thread_ids(room);
    expected_source_thread_ids.retain(|thread_id| thread_id != &recipient.thread_id);
    if expected_source_thread_ids.is_empty() {
        return Err(invalid_params(format!(
            "competitive room {} requires at least one peer source before {source_phase} fanout",
            room.room_id
        )));
    }
    Ok(MemythosArenaAggregateContract {
        aggregate_id: format!(
            "{}::{round_id}::{source_phase}::{}",
            room.room_id, recipient.thread_id
        ),
        recipient_thread_id: recipient.thread_id.clone(),
        quorum: expected_source_thread_ids.len() as u32,
        expected_source_thread_ids,
        phase_id: source_phase.to_string(),
        deadline_ref: None,
        completion_criteria_ref: format!(
            "app-server://rooms/{}/rounds/{round_id}/checkpoints/all-{source_phase}-for/{}",
            room.room_id, recipient.thread_id
        ),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    })
}

fn canonical_bettor_thread_ids(room: &MemythosRoom) -> Vec<String> {
    let mut thread_ids = room
        .participants
        .iter()
        .filter(|participant| participant.parent_role == "bettor")
        .map(|participant| participant.thread_id.clone())
        .collect::<Vec<_>>();
    thread_ids.sort();
    thread_ids.dedup();
    thread_ids
}

pub(super) fn native_concierge_checkpoint_prompt(message_kind: &str, message: &str) -> String {
    let next_action = match message_kind {
        "peer_proposal" => {
            "All expected independent proposals are now sealed in your native mailbox. Read the complete mailbox checkpoint, then dispatch exactly one peer_review_and_objection assignment to every bettor and end this turn. Do not wait synchronously for their responses."
        }
        "peer_review_and_objection" => {
            "All expected cross-reads and objections are now sealed in your native mailbox. Read the complete mailbox checkpoint, then dispatch exactly one peer_bet assignment to every bettor and end this turn. Each bettor must send its revised commitment directly to the Judge. Do not wait synchronously for their responses."
        }
        "refinement_delta" => {
            "All Judge-targeted refinement deltas are now sealed in your native mailbox. Synthesize one compact refinement packet that preserves participant attribution, evidence refs, sufficiency status, remaining tensions, and any parent-rollup request. Do not reinterpret the Judge's mandate, reopen proposal/cross-read/bet, or contact another parent. Your completed response is delivered automatically to the same Judge for one final verdict."
        }
        _ => "Read the complete sealed mailbox checkpoint and continue the native arena method.",
    };
    format!("{message}\n\nNative phase checkpoint: {next_action}")
}

pub(super) fn native_bettor_checkpoint_prompt(message_kind: &str, message: &str) -> String {
    let next_action = match message_kind {
        "peer_review_and_objection" => {
            "All expected independent proposals are now sealed in your native mailbox. Read every proposal and return only the JSON object required by the native output schema. From your own differential responsibility, declare supported_mechanism, the material mechanism_delta from the nearest peer, its decision_effect, shared_ground incorporated from rivals, residual_dissent, and the yield_condition under which you would merge or cede. Copy proposal_ref and incorporated_peer_refs exactly from the allowed app-server:// thread-turn refs in the output schema; never invent a semantic alias. Convergence supported by evidence is valid: set mechanism_state=converged and attribute the supported proposal instead of inventing opposition. Use rollup_required only when authority or a business definition blocks the mechanism comparison. Repetition without a differential contribution is not a valid delta. If this is a revised composition, report only changed mechanism and remaining material objection; do not reproduce unchanged arena constraints."
        }
        "peer_bet" => {
            "All expected peer reviews and objections are now sealed in your native mailbox. Read the complete checkpoint and use your native thread memory to return only the JSON object required by the native output schema. Make an incremental final commitment, not a repetition of your proposal or cross-read, and link both with proposal_ref and cross_read_ref. Copy both refs exactly from the allowed app-server:// thread-turn refs in the output schema; never invent a semantic alias. Resolve mechanism_state as distinct, conditioned, converged, or rollup_required. State the supported proposal and mechanism, exact mechanism_delta and decision_effect, shared_ground, residual_dissent, and yield_condition. Accept an explicit tradeoff and cost of error, and state concrete reopening_signals. Convergence is valid and withdraws redundant competition without erasing attribution; conditioned means your decisive condition changes how another mechanism can be adopted. Raise rollup_required only when genuinely blocking authority or a business definition is missing. Reference unchanged guardrails rather than restating them."
        }
        "targeted_refinement" => {
            "Use your native thread memory and answer only the Judge's targeted mandate. Return a refinement delta: what changed, which evidence supports it, whether the stated sufficiency criterion is now met, and any remaining material tension. Do not restart proposal, cross-read, or bet. Request parent rollup only if the mandate exposes missing authority or a business definition that your role cannot supply."
        }
        _ => "Read the complete sealed mailbox checkpoint and complete your assigned arena phase.",
    };
    format!("{message}\n\nNative peer checkpoint: {next_action}")
}

pub(super) fn validate_native_aggregate_contract(
    message: &MemythosArenaMessage,
    contract: &MemythosArenaAggregateContract,
) -> Result<(), JSONRPCErrorError> {
    if contract.aggregate_id.trim().is_empty()
        || contract.phase_id.trim().is_empty()
        || contract.completion_criteria_ref.trim().is_empty()
        || contract.expected_source_thread_ids.is_empty()
        || contract.quorum == 0
        || contract.quorum as usize > contract.expected_source_thread_ids.len()
    {
        return Err(invalid_params(
            "aggregate contract requires id, phase, completion criteria, expected sources, and a valid quorum",
        ));
    }
    if contract.recipient_thread_id != message.to_parent_thread_id {
        return Err(invalid_params(
            "aggregate recipient must match the message target parent",
        ));
    }
    if !contract
        .expected_source_thread_ids
        .contains(&message.from_parent_thread_id)
    {
        return Err(invalid_params(
            "aggregate message source is not declared in expected sources",
        ));
    }
    Ok(())
}

pub(super) fn phase_from_message_kind(message_kind: &str) -> Option<String> {
    match message_kind {
        "dispatch_proposals" | "peer_proposal" => Some("proposal".to_string()),
        "dispatch_cross_read"
        | "peer_cross_read"
        | "peer_objection"
        | "peer_review_and_objection" => Some("peer_review_and_objection".to_string()),
        "dispatch_bets" | "peer_bet" => Some("bet".to_string()),
        "request_judge" | "verdict_request" | "judge_verdict" => Some("judge".to_string()),
        "targeted_refinement" | "refinement_delta" => Some("targeted_refinement".to_string()),
        "request_final_judge" | "final_verdict_request" | "final_judge_verdict" => {
            Some("final_judge".to_string())
        }
        "resume_reassessment" => Some("resume_reassessment".to_string()),
        "notify_coordinator" | "judge_learning" => Some("learning".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_message_kinds_map_to_stable_phases() {
        assert_eq!(
            phase_from_message_kind("peer_proposal").as_deref(),
            Some("proposal")
        );
        assert_eq!(
            phase_from_message_kind("peer_review_and_objection").as_deref(),
            Some("peer_review_and_objection")
        );
        assert_eq!(phase_from_message_kind("peer_bet").as_deref(), Some("bet"));
        assert_eq!(
            phase_from_message_kind("judge_verdict").as_deref(),
            Some("judge")
        );
    }

    #[test]
    fn checkpoint_prompts_preserve_native_mailbox_ownership() {
        let concierge = native_concierge_checkpoint_prompt("peer_proposal", "checkpoint");
        let bettor = native_bettor_checkpoint_prompt("peer_bet", "checkpoint");

        assert!(concierge.contains("sealed in your native mailbox"));
        assert!(concierge.contains("exactly one peer_review_and_objection"));
        assert!(bettor.contains("native thread memory"));
    }
}
