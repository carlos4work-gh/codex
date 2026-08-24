use std::collections::HashSet;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaAggregateContract;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
use codex_app_server_protocol::MemythosArenaLateArrivalPolicy;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosArenaMessageDelivery;
use codex_app_server_protocol::MemythosArenaResumeExecutionMode;
use codex_app_server_protocol::MemythosParentRole;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomParticipant;

use crate::error_code::invalid_params;
use crate::request_processors::memythos_closure::arena_round_key;
use crate::request_processors::memythos_delivery::canonical_native_bettor_phase_contract;
use crate::request_processors::memythos_delivery::canonical_native_concierge_phase_contract;
use crate::request_processors::memythos_delivery::canonical_native_judge_bet_contract;
use crate::request_processors::memythos_delivery::phase_from_message_kind;
use crate::request_processors::memythos_judge::NativeJudgeVerdict;
use crate::request_processors::memythos_judge::is_valid_native_judge_verdict;
use crate::request_processors::memythos_observability::native_token_usage_key;
use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;

pub(super) fn canonical_native_judge_reassessment_contract(
    state: &MemythosRuntimeState,
    room: &MemythosRoom,
    round_id: &str,
    judge: &MemythosRoomParticipant,
) -> Result<MemythosArenaAggregateContract, JSONRPCErrorError> {
    let plan = state
        .arena_resume_execution_plans
        .get(&arena_round_key(&room.arena_id, round_id))
        .ok_or_else(|| invalid_params("partial resume round has no native execution plan"))?;
    if plan.mode != MemythosArenaResumeExecutionMode::ReassessAffectedPositions {
        return Err(invalid_params(
            "resume reassessment aggregation requires a reassess_affected_positions plan",
        ));
    }
    let composition = state
        .arena_compositions
        .get(&room.arena_id)
        .ok_or_else(|| invalid_params("partial resume round has no native composition"))?;
    let affected_ids = plan.affected_participant_ids.iter().collect::<HashSet<_>>();
    let affected_bettor_ids = composition
        .leases
        .iter()
        .filter(|lease| {
            lease.role == MemythosParentRole::Bettor.as_wire()
                && affected_ids.contains(&lease.participant_id)
        })
        .map(|lease| lease.participant_id.as_str())
        .collect::<HashSet<_>>();
    let mut expected_source_thread_ids = composition
        .leases
        .iter()
        .filter(|lease| {
            lease.role == MemythosParentRole::Bettor.as_wire()
                && affected_bettor_ids.contains(lease.participant_id.as_str())
        })
        .map(|lease| lease.thread_id.clone())
        .collect::<Vec<_>>();
    expected_source_thread_ids.sort();
    expected_source_thread_ids.dedup();
    if affected_bettor_ids.is_empty()
        || expected_source_thread_ids.len() != affected_bettor_ids.len()
    {
        return Err(invalid_params(format!(
            "partial resume expected {} active affected bettors but resolved {} live parent threads",
            affected_bettor_ids.len(),
            expected_source_thread_ids.len()
        )));
    }
    Ok(MemythosArenaAggregateContract {
        aggregate_id: format!("{}::{round_id}::judge_reassessment", room.room_id),
        recipient_thread_id: judge.thread_id.clone(),
        quorum: expected_source_thread_ids.len() as u32,
        expected_source_thread_ids,
        phase_id: "resume_reassessment".to_string(),
        deadline_ref: None,
        completion_criteria_ref: format!(
            "app-server://rooms/{}/rounds/{round_id}/checkpoints/all-affected-reassessments",
            room.room_id
        ),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    })
}

pub(super) fn canonical_native_concierge_refinement_contract(
    state: &MemythosRuntimeState,
    room: &MemythosRoom,
    round_id: &str,
    concierge: &MemythosRoomParticipant,
) -> Result<MemythosArenaAggregateContract, JSONRPCErrorError> {
    let judge_verdict = state
        .arena_message_deliveries
        .iter()
        .rev()
        .find(|delivery| {
            delivery.arena_id == room.arena_id
                && delivery.round_id == round_id
                && delivery.phase.as_deref() == Some("judge")
                && delivery.sender_thread_id
                    == room
                        .participants
                        .iter()
                        .find(|participant| participant.parent_role == "judge")
                        .map(|participant| participant.thread_id.as_str())
                        .unwrap_or_default()
        })
        .map(|delivery| delivery.human_summary.as_str())
        .and_then(|text| serde_json::from_str::<NativeJudgeVerdict>(text).ok())
        .ok_or_else(|| {
            invalid_params("targeted refinement requires a valid native judge verdict")
        })?;
    let composition = state
        .arena_compositions
        .get(&room.arena_id)
        .ok_or_else(|| invalid_params("targeted refinement requires a native composition"))?;
    let targeted_ids = judge_verdict
        .targeted_refinements
        .iter()
        .map(|refinement| refinement.participant_id.as_str())
        .collect::<HashSet<_>>();
    let mut expected_source_thread_ids = composition
        .leases
        .iter()
        .filter(|lease| targeted_ids.contains(lease.participant_id.as_str()))
        .map(|lease| lease.thread_id.clone())
        .collect::<Vec<_>>();
    expected_source_thread_ids.sort();
    expected_source_thread_ids.dedup();
    if expected_source_thread_ids.len() != targeted_ids.len() || targeted_ids.is_empty() {
        return Err(invalid_params(
            "targeted refinement verdict does not resolve to the expected live bettor parents",
        ));
    }
    Ok(MemythosArenaAggregateContract {
        aggregate_id: format!("{}::{round_id}::concierge_refinements", room.room_id),
        recipient_thread_id: concierge.thread_id.clone(),
        quorum: expected_source_thread_ids.len() as u32,
        expected_source_thread_ids,
        phase_id: "targeted_refinement".to_string(),
        deadline_ref: None,
        completion_criteria_ref: format!(
            "app-server://rooms/{}/rounds/{round_id}/checkpoints/all-targeted-refinements",
            room.room_id
        ),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    })
}
pub(super) fn native_turn_loopback_candidate(
    state: &MemythosRuntimeState,
    thread_id: &str,
    turn_id: &str,
    native_event_ref: &str,
) -> Option<MemythosArenaMessage> {
    let incoming = state
        .arena_message_deliveries
        .iter()
        .rev()
        .find(|delivery| {
            delivery.receiver_thread_id == thread_id
                && delivery.receiver_turn_id.as_deref() == Some(turn_id)
        })?;
    let phase = incoming.phase.as_deref()?;
    let response_text = state
        .native_parent_turn_responses
        .get(&native_token_usage_key(thread_id, turn_id))?
        .text
        .as_ref()?
        .trim();
    if response_text.is_empty() {
        return None;
    }
    let room = state
        .rooms
        .values()
        .find(|room| room.arena_id == incoming.arena_id)?;
    let source = room
        .participants
        .iter()
        .find(|participant| participant.thread_id == thread_id)?;
    let (target, message_kind, delivery_policy, aggregate_contract, requires_response) =
        match (source.parent_role.as_str(), phase) {
            ("bettor", "proposal") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")?;
                let contract = canonical_native_concierge_phase_contract(
                    room,
                    &incoming.round_id,
                    target,
                    "peer_proposal",
                )
                .ok()?;
                (
                    target,
                    "peer_proposal",
                    Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                    Some(contract),
                    true,
                )
            }
            ("bettor", "peer_review_and_objection") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")?;
                let contract = canonical_native_concierge_phase_contract(
                    room,
                    &incoming.round_id,
                    target,
                    "peer_review_and_objection",
                )
                .ok()?;
                (
                    target,
                    "peer_review_and_objection",
                    Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                    Some(contract),
                    true,
                )
            }
            ("bettor", "bet") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "judge")?;
                let contract =
                    canonical_native_judge_bet_contract(room, &incoming.round_id, target).ok()?;
                (
                    target,
                    "peer_bet",
                    Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                    Some(contract),
                    true,
                )
            }
            ("bettor", "resume_reassessment") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "judge")?;
                let contract = canonical_native_judge_reassessment_contract(
                    state,
                    room,
                    &incoming.round_id,
                    target,
                )
                .ok()?;
                (
                    target,
                    "resume_reassessment",
                    Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                    Some(contract),
                    true,
                )
            }
            ("bettor", "targeted_refinement") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")?;
                let contract = canonical_native_concierge_refinement_contract(
                    state,
                    room,
                    &incoming.round_id,
                    target,
                )
                .ok()?;
                (
                    target,
                    "refinement_delta",
                    Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                    Some(contract),
                    true,
                )
            }
            ("room_concierge", "targeted_refinement") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "judge")?;
                (
                    target,
                    "final_verdict_request",
                    Some(MemythosArenaDeliveryPolicy::Immediate),
                    None,
                    true,
                )
            }
            ("judge", "judge") | ("judge", "resume_reassessment") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")?;
                (
                    target,
                    "judge_verdict",
                    Some(MemythosArenaDeliveryPolicy::QueueOnly),
                    None,
                    false,
                )
            }
            ("judge", "final_judge") => {
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")?;
                (
                    target,
                    "final_judge_verdict",
                    Some(MemythosArenaDeliveryPolicy::QueueOnly),
                    None,
                    false,
                )
            }
            _ => return None,
        };
    let duplicate = state.arena_message_deliveries.iter().any(|delivery| {
        delivery.arena_id == incoming.arena_id
            && delivery.round_id == incoming.round_id
            && delivery.sender_thread_id == thread_id
            && delivery.receiver_thread_id == target.thread_id
            && delivery.phase.as_deref() == phase_from_message_kind(message_kind).as_deref()
    });
    if duplicate {
        return None;
    }
    Some(MemythosArenaMessage {
        message_id: format!("turn-loopback-{turn_id}"),
        case_id: incoming.arena_id.clone(),
        arena_id: incoming.arena_id.clone(),
        round_id: incoming.round_id.clone(),
        from_parent_thread_id: thread_id.to_string(),
        from_parent_role: source.parent_role.clone(),
        to_parent_thread_id: target.thread_id.clone(),
        to_parent_role: target.parent_role.clone(),
        message_kind: message_kind.to_string(),
        human_summary: response_text.to_string(),
        execution_prompt: None,
        context_packet_ref: native_event_ref.to_string(),
        artifact_refs: Vec::new(),
        requires_response,
        delivery_policy,
        aggregate_contract,
        response_contract: None,
        output_schema: None,
    })
}

pub(super) fn native_turn_loopback_candidates(
    state: &MemythosRuntimeState,
    thread_id: &str,
    turn_id: &str,
    native_event_ref: &str,
) -> Vec<MemythosArenaMessage> {
    let Some(incoming) = state
        .arena_message_deliveries
        .iter()
        .rev()
        .find(|delivery| {
            delivery.receiver_thread_id == thread_id
                && delivery.receiver_turn_id.as_deref() == Some(turn_id)
        })
    else {
        return Vec::new();
    };
    let Some(phase) = incoming.phase.as_deref() else {
        return Vec::new();
    };
    let Some(room) = state
        .rooms
        .values()
        .find(|room| room.arena_id == incoming.arena_id)
    else {
        return Vec::new();
    };
    let Some(source) = room
        .participants
        .iter()
        .find(|participant| participant.thread_id == thread_id)
    else {
        return Vec::new();
    };

    if source.parent_role == "room_concierge" && phase == "arena_intake" {
        return native_arena_intake_assignments(
            state,
            room,
            incoming,
            source,
            turn_id,
            native_event_ref,
        );
    }

    let Some(response_text) = state
        .native_parent_turn_responses
        .get(&native_token_usage_key(thread_id, turn_id))
        .and_then(|response| response.text.as_deref())
        .map(str::trim)
        .filter(|text| !text.is_empty())
    else {
        return Vec::new();
    };

    if source.parent_role == "judge"
        && matches!(phase, "judge" | "resume_reassessment" | "final_judge")
    {
        let mut messages =
            native_turn_loopback_candidate(state, thread_id, turn_id, native_event_ref)
                .into_iter()
                .collect::<Vec<_>>();
        let eligible_ids = state
            .arena_compositions
            .get(&incoming.arena_id)
            .map(|composition| {
                composition
                    .contract
                    .participants
                    .iter()
                    .filter(|participant| participant.agent_role == "bettor")
                    .map(|participant| participant.participant_id.as_str())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let Some(verdict) = serde_json::from_str::<NativeJudgeVerdict>(response_text)
            .ok()
            .filter(|_| is_valid_native_judge_verdict(response_text, &eligible_ids))
        else {
            return messages;
        };
        if verdict.next_action == "close" {
            let Some(concierge) = room
                .participants
                .iter()
                .find(|participant| participant.parent_role == "room_concierge")
            else {
                return messages;
            };
            let Some(composition) = state.arena_compositions.get(&incoming.arena_id) else {
                return messages;
            };
            messages.extend(verdict.contribution_attribution.iter().filter_map(|attribution| {
                let lease = composition
                    .leases
                    .iter()
                    .find(|lease| lease.participant_id == attribution.participant_id)?;
                let target = room
                    .participants
                    .iter()
                    .find(|participant| participant.thread_id == lease.thread_id)?;
                let duplicate = state.arena_message_deliveries.iter().any(|delivery| {
                    delivery.arena_id == incoming.arena_id
                        && delivery.round_id == incoming.round_id
                        && delivery.receiver_thread_id == target.thread_id
                        && delivery.phase.as_deref() == Some("learning")
                });
                if duplicate {
                    return None;
                }
                let learning = format!(
                    "The arena judge closed this round.\nWinning decision: {}\nAccepted tradeoff: {}\nYour contribution was {}.\nClaim refs: {}\nWhy: {}\nPreserved dissent for future reality checks: {}\nCarry this attribution into the next round as evidence, not as a score or an instruction to defend a rejected claim.",
                    verdict.winning_decision,
                    verdict.accepted_tradeoff,
                    attribution.disposition,
                    if attribution.claim_refs.is_empty() {
                        "none".to_string()
                    } else {
                        attribution.claim_refs.join(", ")
                    },
                    attribution.rationale,
                    if verdict.preserved_dissent.is_empty() {
                        "none".to_string()
                    } else {
                        verdict.preserved_dissent.join("; ")
                    }
                );
                Some(MemythosArenaMessage {
                    message_id: format!(
                        "judge-learning-{turn_id}-{}",
                        attribution.participant_id
                    ),
                    case_id: room.case_id.clone(),
                    arena_id: incoming.arena_id.clone(),
                    round_id: incoming.round_id.clone(),
                    from_parent_thread_id: concierge.thread_id.clone(),
                    from_parent_role: concierge.parent_role.clone(),
                    to_parent_thread_id: target.thread_id.clone(),
                    to_parent_role: target.parent_role.clone(),
                    message_kind: "judge_learning".to_string(),
                    human_summary: learning,
                    execution_prompt: None,
                    context_packet_ref: native_event_ref.to_string(),
                    artifact_refs: vec![native_event_ref.to_string()],
                    requires_response: false,
                    delivery_policy: Some(MemythosArenaDeliveryPolicy::QueueOnly),
                    aggregate_contract: None,
                    response_contract: None,
                    output_schema: None,
                })
            }));
            return messages;
        }
        if verdict.next_action != "targeted_refinement" {
            return messages;
        }
        let Some(concierge) = room
            .participants
            .iter()
            .find(|participant| participant.parent_role == "room_concierge")
        else {
            return messages;
        };
        let Some(composition) = state.arena_compositions.get(&incoming.arena_id) else {
            return messages;
        };
        messages.extend(verdict.targeted_refinements.iter().filter_map(|mandate| {
            let lease = composition
                .leases
                .iter()
                .find(|lease| lease.participant_id == mandate.participant_id)?;
            let target = room
                .participants
                .iter()
                .find(|participant| participant.thread_id == lease.thread_id)?;
            let duplicate = state.arena_message_deliveries.iter().any(|delivery| {
                delivery.arena_id == incoming.arena_id
                    && delivery.round_id == incoming.round_id
                    && delivery.receiver_thread_id == target.thread_id
                    && delivery.phase.as_deref() == Some("targeted_refinement")
            });
            if duplicate {
                return None;
            }
            let assignment = format!(
                "Judge-targeted refinement for participant {}.\nTension: {}\nRequest: {}\nObservable sufficiency criterion: {}",
                mandate.participant_id,
                mandate.tension,
                mandate.request,
                mandate.sufficiency_criterion
            );
            Some(MemythosArenaMessage {
                message_id: format!(
                    "targeted-refinement-{turn_id}-{}",
                    mandate.participant_id
                ),
                case_id: room.case_id.clone(),
                arena_id: incoming.arena_id.clone(),
                round_id: incoming.round_id.clone(),
                from_parent_thread_id: concierge.thread_id.clone(),
                from_parent_role: concierge.parent_role.clone(),
                to_parent_thread_id: target.thread_id.clone(),
                to_parent_role: target.parent_role.clone(),
                message_kind: "targeted_refinement".to_string(),
                human_summary: assignment.clone(),
                execution_prompt: Some(assignment),
                context_packet_ref: native_event_ref.to_string(),
                artifact_refs: Vec::new(),
                requires_response: true,
                delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                aggregate_contract: None,
                response_contract: Some("refinement_delta".to_string()),
                output_schema: None,
            })
        }));
        return messages;
    }

    let (message_kind, source_phase) = match (source.parent_role.as_str(), phase) {
        ("bettor", "proposal") => ("peer_review_and_objection", "proposal"),
        ("bettor", "peer_review_and_objection") => ("peer_bet", "peer_review_and_objection"),
        _ => {
            return native_turn_loopback_candidate(state, thread_id, turn_id, native_event_ref)
                .into_iter()
                .collect();
        }
    };

    if phase == "proposal" {
        let bettor_threads = room
            .participants
            .iter()
            .filter(|participant| participant.parent_role == "bettor")
            .map(|participant| participant.thread_id.as_str())
            .collect::<HashSet<_>>();
        let completed_proposals = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| {
                delivery.arena_id == incoming.arena_id
                    && delivery.round_id == incoming.round_id
                    && delivery.phase.as_deref() == Some("proposal")
                    && delivery.status == "receiver_turn_completed"
                    && bettor_threads.contains(delivery.receiver_thread_id.as_str())
            })
            .collect::<Vec<_>>();
        let completed_sources = completed_proposals
            .iter()
            .map(|delivery| delivery.receiver_thread_id.as_str())
            .collect::<HashSet<_>>();
        if completed_sources.len() != bettor_threads.len() {
            return Vec::new();
        }

        return completed_proposals
            .into_iter()
            .flat_map(|proposal| {
                let source_thread_id = proposal.receiver_thread_id.as_str();
                let source_turn_id = proposal.receiver_turn_id.as_deref()?;
                let source_participant = room
                    .participants
                    .iter()
                    .find(|participant| participant.thread_id == source_thread_id)?;
                let proposal_text = state
                    .native_parent_turn_responses
                    .get(&native_token_usage_key(source_thread_id, source_turn_id))?
                    .text
                    .as_deref()?
                    .trim();
                Some(
                    room.participants
                        .iter()
                        .filter(|target| target.parent_role == "bettor")
                        .filter(move |target| target.thread_id != source_thread_id)
                        .filter_map(move |target| {
                            let contract = canonical_native_bettor_phase_contract(
                                room,
                                &incoming.round_id,
                                target,
                                source_phase,
                            )
                            .ok()?;
                            let duplicate = state.arena_message_deliveries.iter().any(|delivery| {
                                delivery.arena_id == incoming.arena_id
                                    && delivery.round_id == incoming.round_id
                                    && delivery.sender_thread_id == source_thread_id
                                    && delivery.receiver_thread_id == target.thread_id
                                    && delivery.phase.as_deref()
                                        == phase_from_message_kind(message_kind).as_deref()
                            });
                            if duplicate {
                                return None;
                            }
                            Some(MemythosArenaMessage {
                                message_id: format!(
                                    "turn-loopback-{source_turn_id}-{}",
                                    target.thread_id
                                ),
                                case_id: incoming.arena_id.clone(),
                                arena_id: incoming.arena_id.clone(),
                                round_id: incoming.round_id.clone(),
                                from_parent_thread_id: source_thread_id.to_string(),
                                from_parent_role: source_participant.parent_role.clone(),
                                to_parent_thread_id: target.thread_id.clone(),
                                to_parent_role: target.parent_role.clone(),
                                message_kind: message_kind.to_string(),
                                human_summary: proposal_text.to_string(),
                                execution_prompt: None,
                                context_packet_ref: native_event_ref.to_string(),
                                artifact_refs: Vec::new(),
                                requires_response: true,
                                delivery_policy: Some(
                                    MemythosArenaDeliveryPolicy::AggregateThenTrigger,
                                ),
                                aggregate_contract: Some(contract),
                                response_contract: None,
                                output_schema: None,
                            })
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect();
    }

    room.participants
        .iter()
        .filter(|participant| participant.parent_role == "bettor")
        .filter(|participant| participant.thread_id != thread_id)
        .filter_map(|target| {
            let contract = canonical_native_bettor_phase_contract(
                room,
                &incoming.round_id,
                target,
                source_phase,
            )
            .ok()?;
            let duplicate = state.arena_message_deliveries.iter().any(|delivery| {
                delivery.arena_id == incoming.arena_id
                    && delivery.round_id == incoming.round_id
                    && delivery.sender_thread_id == thread_id
                    && delivery.receiver_thread_id == target.thread_id
                    && delivery.phase.as_deref() == phase_from_message_kind(message_kind).as_deref()
            });
            if duplicate {
                return None;
            }
            Some(MemythosArenaMessage {
                message_id: format!("turn-loopback-{turn_id}-{}", target.thread_id),
                case_id: incoming.arena_id.clone(),
                arena_id: incoming.arena_id.clone(),
                round_id: incoming.round_id.clone(),
                from_parent_thread_id: thread_id.to_string(),
                from_parent_role: source.parent_role.clone(),
                to_parent_thread_id: target.thread_id.clone(),
                to_parent_role: target.parent_role.clone(),
                message_kind: message_kind.to_string(),
                human_summary: response_text.to_string(),
                execution_prompt: None,
                context_packet_ref: native_event_ref.to_string(),
                artifact_refs: Vec::new(),
                requires_response: true,
                delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                aggregate_contract: Some(contract),
                response_contract: None,
                output_schema: None,
            })
        })
        .collect()
}

pub(super) fn native_arena_intake_assignments(
    state: &MemythosRuntimeState,
    room: &MemythosRoom,
    incoming: &MemythosArenaMessageDelivery,
    concierge: &MemythosRoomParticipant,
    turn_id: &str,
    native_event_ref: &str,
) -> Vec<MemythosArenaMessage> {
    let Some(plan) = state
        .arena_resume_execution_plans
        .get(&arena_round_key(&incoming.arena_id, &incoming.round_id))
    else {
        return Vec::new();
    };
    if plan.mode == MemythosArenaResumeExecutionMode::RetainDecision {
        return Vec::new();
    }

    let affected_ids = plan
        .affected_participant_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let composition = state.arena_compositions.get(&incoming.arena_id);
    let participant_id_for_thread = |thread_id: &str| {
        composition.and_then(|composition| {
            composition
                .leases
                .iter()
                .find(|lease| lease.thread_id == thread_id)
                .map(|lease| lease.participant_id.as_str())
        })
    };
    let message_kind = if plan.mode == MemythosArenaResumeExecutionMode::ReassessAffectedPositions {
        "resume_reassessment"
    } else {
        "peer_proposal"
    };
    let concierge_framing = state
        .native_parent_turn_responses
        .get(&native_token_usage_key(&concierge.thread_id, turn_id))
        .and_then(|response| response.text.as_deref())
        .map(str::trim)
        .filter(|text| !text.is_empty());

    room.participants
        .iter()
        .filter(|participant| participant.parent_role == "bettor")
        .filter(|participant| {
            plan.mode != MemythosArenaResumeExecutionMode::ReassessAffectedPositions
                || participant_id_for_thread(&participant.thread_id)
                    .is_some_and(|participant_id| affected_ids.contains(participant_id))
        })
        .filter(|target| {
            !state.arena_message_deliveries.iter().any(|delivery| {
                delivery.arena_id == incoming.arena_id
                    && delivery.round_id == incoming.round_id
                    && delivery.sender_thread_id == concierge.thread_id
                    && delivery.receiver_thread_id == target.thread_id
                    && delivery.phase.as_deref()
                        == phase_from_message_kind(message_kind).as_deref()
            })
        })
        .map(|target| {
            let assignment = if message_kind == "resume_reassessment" {
                format!(
                    "Reassess only your affected position against the new evidence and protected decisions. The planner has already accepted these cited change refs as material novelty for this partial resume: [{}]. Treat the corresponding reality evidence in the arena intake as supplied evidence; do not claim it is absent or unverified. Preserve settled scope, identify what changed, revise your commitment if warranted, and return the bounded reassessment for native Judge aggregation.\n\nArena intake: {}{}",
                    plan.cited_change_refs.join(", "),
                    incoming.human_summary,
                    concierge_framing
                        .map(|framing| format!("\n\nConcierge framing: {framing}"))
                        .unwrap_or_default(),
                )
            } else {
                format!(
                    "Produce an independent proposal from your assigned stance for this arena objective. State your thesis, evidence, tradeoffs, objections, and falsification signals. Do not coordinate with peers yet; your completed response will enter native cross-read.\n\nArena intake: {}{}",
                    incoming.human_summary,
                    concierge_framing
                        .map(|framing| format!("\n\nConcierge framing: {framing}"))
                        .unwrap_or_default(),
                )
            };
            MemythosArenaMessage {
                message_id: format!(
                    "intake-loopback-{turn_id}-{}-{message_kind}",
                    target.thread_id
                ),
                case_id: room.case_id.clone(),
                arena_id: incoming.arena_id.clone(),
                round_id: incoming.round_id.clone(),
                from_parent_thread_id: concierge.thread_id.clone(),
                from_parent_role: concierge.parent_role.clone(),
                to_parent_thread_id: target.thread_id.clone(),
                to_parent_role: target.parent_role.clone(),
                message_kind: message_kind.to_string(),
                human_summary: assignment.clone(),
                execution_prompt: Some(assignment),
                context_packet_ref: native_event_ref.to_string(),
                artifact_refs: Vec::new(),
                requires_response: true,
                delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                aggregate_contract: None,
                response_contract: Some(if message_kind == "resume_reassessment" {
                    "Return one bounded reassessment for the native Judge checkpoint."
                        .to_string()
                } else {
                    "Return one independent proposal for native peer cross-read.".to_string()
                }),
                output_schema: None,
            }
        })
        .collect()
}
