use std::collections::HashSet;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaDecisionMethod;
use codex_app_server_protocol::MemythosArenaMessageDelivery;
use codex_app_server_protocol::MemythosArenaResumeExecutionMode;
use codex_app_server_protocol::MemythosParentRole;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomParticipant;

use crate::error_code::invalid_params;
use crate::request_processors::memythos_closure::arena_round_key;
use crate::request_processors::memythos_composition::is_competitive_method;
use crate::request_processors::memythos_delivery::phase_from_message_kind;
use crate::request_processors::memythos_resume::validate_resume_execution_plan_message;
use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;

pub(super) fn validate_room_message_kind(
    decision_method: Option<&MemythosArenaDecisionMethod>,
    message_kind: &str,
) -> Result<(), JSONRPCErrorError> {
    if decision_method.is_some_and(|method| is_competitive_method(method.clone()))
        && phase_from_message_kind(message_kind).is_none()
    {
        return Err(invalid_params(format!(
            "competitive arena room acts require an explicit semantic phase messageKind; {message_kind} is ambiguous. Use peer_proposal, peer_review_and_objection, peer_bet, targeted_refinement, refinement_delta, resume_reassessment, verdict_request, judge_verdict, final_verdict_request, final_judge_verdict, judge_learning, or notify_coordinator"
        )));
    }
    Ok(())
}

pub(super) fn validate_room_message_route(
    decision_method: Option<&MemythosArenaDecisionMethod>,
    message_kind: &str,
    source_role: &str,
    target_role: &str,
) -> Result<(), JSONRPCErrorError> {
    if !decision_method.is_some_and(|method| is_competitive_method(*method)) {
        return Ok(());
    }

    let valid = match message_kind {
        "peer_proposal"
        | "peer_cross_read"
        | "peer_objection"
        | "peer_review_and_objection"
        | "peer_bet" => {
            (source_role == "room_concierge" && target_role == "bettor")
                || (source_role == "bettor" && target_role == "room_concierge")
                || (message_kind == "peer_bet" && source_role == "bettor" && target_role == "judge")
        }
        "verdict_request" => source_role == "room_concierge" && target_role == "judge",
        "targeted_refinement" => source_role == "room_concierge" && target_role == "bettor",
        "refinement_delta" => source_role == "bettor" && target_role == "room_concierge",
        "final_verdict_request" => source_role == "room_concierge" && target_role == "judge",
        "final_judge_verdict" => source_role == "judge" && target_role == "room_concierge",
        "resume_reassessment" => {
            (source_role == "room_concierge" && target_role == "bettor")
                || (source_role == "bettor" && target_role == "judge")
        }
        "judge_verdict" => source_role == "judge" && target_role == "room_concierge",
        "judge_learning" => source_role == "room_concierge" && target_role == "bettor",
        "notify_coordinator" => {
            (source_role == "room_concierge"
                && matches!(target_role, "process_steward" | "coordinator"))
                || (matches!(source_role, "process_steward" | "coordinator")
                    && target_role == "room_concierge")
        }
        // Legacy protocol aliases remain available for old non-agentic callers. Native parents
        // are instructed to use the explicit peer message kinds above.
        "dispatch_proposals" | "dispatch_cross_read" | "dispatch_bets" | "request_judge" => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_params(format!(
            "competitive arena message route is invalid: {source_role} --{message_kind}--> {target_role}. Peer, targeted-refinement, and judge-learning phases flow only between room_concierge and bettor; verdict requests flow room_concierge to judge; judge verdicts flow judge to room_concierge; notify_coordinator flows between room_concierge and coordinator"
        )))
    }
}

pub(super) fn validate_competitive_round_progress(
    decision_method: Option<&MemythosArenaDecisionMethod>,
    message_kind: &str,
    room: &MemythosRoom,
    composition: Option<&MemythosArenaCompositionProvisionResponse>,
    deliveries: &[MemythosArenaMessageDelivery],
) -> Result<(), JSONRPCErrorError> {
    if !decision_method.is_some_and(|method| is_competitive_method(*method)) {
        return Ok(());
    }

    let minimum_positions = composition
        .and_then(|composition| composition.contract.coordination.round_policy.as_ref())
        .map(|policy| policy.minimum_competing_positions as usize)
        .unwrap_or(2);
    let completed_targets_for_phase = |phase: &str| {
        deliveries
            .iter()
            .filter(|delivery| {
                delivery.arena_id == room.arena_id
                    && delivery.phase.as_deref() == Some(phase)
                    && delivery.status == "receiver_turn_completed"
            })
            .map(|delivery| delivery.receiver_thread_id.as_str())
            .collect::<HashSet<_>>()
            .len()
    };

    let objection_required = composition
        .and_then(|composition| composition.contract.coordination.round_policy.as_ref())
        .is_some_and(|policy| policy.objection_required);
    let prerequisite = match message_kind {
        "peer_cross_read" | "peer_objection" | "peer_review_and_objection" => {
            Some(("proposal", "peer proposals"))
        }
        "peer_bet" if objection_required => {
            Some(("peer_review_and_objection", "peer reviews with objections"))
        }
        "peer_bet" => Some((
            "peer_review_and_objection",
            "peer reviews of competing evidence",
        )),
        "verdict_request" => Some(("bet", "explicit peer bets")),
        _ => None,
    };
    if let Some((phase, label)) = prerequisite {
        let observed = completed_targets_for_phase(phase);
        if observed < minimum_positions {
            return Err(invalid_params(format!(
                "competitive arena method cannot advance with {message_kind}: collect {minimum_positions} distinct {label} first; observed {observed}. Continue the current room round through the Room Concierge and retry this act after the missing parent responses complete"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_resume_execution_message(
    state: &MemythosRuntimeState,
    room: &MemythosRoom,
    round_id: &str,
    message_kind: &str,
    source: &MemythosRoomParticipant,
    target: &MemythosRoomParticipant,
) -> Result<(), JSONRPCErrorError> {
    let Some(plan) = state
        .arena_resume_execution_plans
        .get(&arena_round_key(&room.arena_id, round_id))
    else {
        return Ok(());
    };
    let target_is_affected = plan.mode
        != MemythosArenaResumeExecutionMode::ReassessAffectedPositions
        || source.parent_role != "room_concierge"
        || state
            .arena_compositions
            .get(&room.arena_id)
            .is_some_and(|composition| {
                composition.leases.iter().any(|lease| {
                    lease.thread_id == target.thread_id
                        && lease.role == MemythosParentRole::Bettor.as_wire()
                        && plan
                            .affected_participant_ids
                            .iter()
                            .any(|participant_id| participant_id == &lease.participant_id)
                })
            });
    validate_resume_execution_plan_message(
        plan,
        round_id,
        message_kind,
        &source.parent_role,
        &target.thread_id,
        target_is_affected,
    )
}
