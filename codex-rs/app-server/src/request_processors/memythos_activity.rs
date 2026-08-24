use std::collections::HashMap;

use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
use codex_app_server_protocol::MemythosArenaMessageDelivery;
use codex_app_server_protocol::MemythosArenaParent;
use codex_app_server_protocol::MemythosParentContinuityStatus;
use codex_app_server_protocol::MemythosParentPeerResponseKind;
use codex_app_server_protocol::MemythosParentPeerResponseObservation;
use codex_app_server_protocol::MemythosParentThreadContinuity;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomActivityItem;
use codex_app_server_protocol::MemythosRoomActivityTurn;
use codex_app_server_protocol::MemythosSemanticAlignment;

use crate::request_processors::memythos_contracts::compact_event_refs;
use crate::request_processors::memythos_contracts::compact_summary;
use crate::request_processors::memythos_observability::native_token_usage_key;
use crate::request_processors::memythos_parent_goal::ParentGoalSnapshot;
use crate::request_processors::memythos_parent_response::ParentTurnResponse;
use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;

pub(super) fn native_delivery_activation_reason(delivery: &MemythosArenaMessageDelivery) -> String {
    match delivery.delivery_policy {
        Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger) => {
            "aggregate_checkpoint_sealed".to_string()
        }
        Some(MemythosArenaDeliveryPolicy::QueueOnly) => "mailbox_queued".to_string(),
        Some(MemythosArenaDeliveryPolicy::Immediate) | None => {
            if delivery.delivery_mechanism == "room_loopback_send_input" {
                "room_loopback_delivery".to_string()
            } else {
                "direct_parent_delivery".to_string()
            }
        }
    }
}

pub(super) fn native_participant_id_for_thread(
    state: &MemythosRuntimeState,
    arena_id: &str,
    thread_id: &str,
) -> Option<String> {
    state
        .arena_compositions
        .get(arena_id)
        .and_then(|composition| {
            composition
                .leases
                .iter()
                .find(|lease| lease.thread_id == thread_id)
                .map(|lease| lease.participant_id.clone())
        })
        .or_else(|| {
            state
                .rooms
                .values()
                .filter(|room| room.arena_id == arena_id)
                .flat_map(|room| room.participants.iter())
                .find(|participant| participant.thread_id == thread_id)
                .map(|participant| participant.parent_key.clone())
        })
}

pub(super) fn room_activity_turn_from_delivery(
    room: &MemythosRoom,
    delivery: &MemythosArenaMessageDelivery,
    native_response: Option<&ParentTurnResponse>,
    include_debug_refs: bool,
) -> Option<MemythosRoomActivityTurn> {
    let turn_id = delivery.receiver_turn_id.clone()?;
    let status = if delivery.status == "receiver_turn_completed" {
        "completed"
    } else if delivery.status.contains("failed") || delivery.rejection_reason.is_some() {
        "failed"
    } else {
        "running"
    };
    let mut refs = vec![
        format!(
            "app-server://rooms/{}/deliveries/{}",
            delivery.arena_id, delivery.delivery_id
        ),
        format!(
            "app-server://threads/{}/turns/{}",
            delivery.receiver_thread_id, turn_id
        ),
    ];
    if include_debug_refs {
        refs.extend(delivery.event_refs.clone());
    }
    let parent_key = room
        .participants
        .iter()
        .find(|participant| participant.thread_id == delivery.receiver_thread_id)
        .map(|participant| participant.parent_key.clone())
        .unwrap_or_else(|| arena_parent_key(&delivery.arena_id, &delivery.receiver_thread_id));
    let phase = delivery.phase.clone();
    let technical_summary = compact_summary(format!(
        "{} delivered {} from {} to {} as {}",
        delivery.delivery_mechanism,
        delivery.message_id,
        delivery.sender_thread_id,
        delivery.receiver_thread_id,
        delivery.status
    ));
    let delivery_ref = format!(
        "app-server://rooms/{}/deliveries/{}",
        delivery.arena_id, delivery.delivery_id
    );
    let mut items = vec![MemythosRoomActivityItem {
        item_id: Some(delivery.delivery_id.clone()),
        item_type: Some("collab_call".to_string()),
        kind: "collab_call".to_string(),
        status: delivery.status.clone(),
        summary: technical_summary.clone(),
        text: None,
        human_highlight: None,
        technical_summary: Some(technical_summary),
        artifact_ref: Some(delivery.message_id.clone()),
        event_ref: delivery_ref,
        refs: compact_event_refs(refs.clone()),
    }];
    if let Some(ParentTurnResponse {
        request_item_ref: Some(item_ref),
        request_text: Some(_),
        ..
    }) = native_response
    {
        let mut request_refs = refs.clone();
        if !request_refs.contains(item_ref) {
            request_refs.push(item_ref.clone());
        }
        items.push(MemythosRoomActivityItem {
            item_id: item_ref.rsplit('/').next().map(str::to_string),
            item_type: Some("userMessage".to_string()),
            kind: "user_message".to_string(),
            status: "completed".to_string(),
            summary: format!("Native UserMessage request for turn {turn_id}."),
            text: Some(delivery.human_summary.clone()),
            human_highlight: Some(delivery.human_summary.clone()),
            technical_summary: None,
            artifact_ref: None,
            event_ref: item_ref.clone(),
            refs: compact_event_refs(request_refs),
        });
    }
    if let Some(ParentTurnResponse {
        item_ref: Some(item_ref),
        text: Some(text),
        ..
    }) = native_response
    {
        let mut response_refs = refs;
        if !response_refs.contains(item_ref) {
            response_refs.push(item_ref.clone());
        }
        items.push(MemythosRoomActivityItem {
            item_id: item_ref.rsplit('/').next().map(str::to_string),
            item_type: Some("agentMessage".to_string()),
            kind: "agent_message".to_string(),
            status: "completed".to_string(),
            summary: format!("Native AgentMessage response for turn {turn_id}."),
            text: Some(text.clone()),
            human_highlight: Some(text.clone()),
            technical_summary: None,
            artifact_ref: None,
            event_ref: item_ref.clone(),
            refs: compact_event_refs(response_refs),
        });
    }
    Some(MemythosRoomActivityTurn {
        parent_key,
        thread_id: delivery.receiver_thread_id.clone(),
        turn_id,
        round_id: Some(delivery.round_id.clone()),
        phase,
        status: status.to_string(),
        failure_reason: delivery.failure_reason.clone(),
        items,
    })
}

pub(super) fn arena_parent_key(arena_id: &str, thread_id: &str) -> String {
    format!("{arena_id}::{thread_id}")
}

pub(super) fn build_parent_thread_continuity(
    parent: &MemythosArenaParent,
    deliveries: &[MemythosArenaMessageDelivery],
    native_token_usage_refs: &HashMap<String, String>,
    goal_snapshot: ParentGoalSnapshot,
) -> MemythosParentThreadContinuity {
    let parent_deliveries = deliveries
        .iter()
        .filter(|delivery| {
            delivery.arena_id == parent.arena_id && delivery.receiver_thread_id == parent.thread_id
        })
        .collect::<Vec<_>>();
    let turn_ids = parent_deliveries
        .iter()
        .filter_map(|delivery| delivery.receiver_turn_id.clone())
        .collect::<Vec<_>>();
    let first_turn_id = turn_ids.first().cloned();
    let latest_turn_id = turn_ids.last().cloned();
    let observed_turn_count = turn_ids.len();
    let latest_turn_completed_ref = parent_deliveries
        .iter()
        .rev()
        .find_map(|delivery| delivery.receiver_response_event_ref.clone());
    let token_usage_ref = latest_turn_id.as_ref().and_then(|turn_id| {
        native_token_usage_refs
            .get(&native_token_usage_key(&parent.thread_id, turn_id))
            .cloned()
    });
    let memory_replay_required = parent_deliveries
        .iter()
        .any(|delivery| delivery.memory_replay_required);
    let mut degraded_reasons = Vec::new();

    if memory_replay_required {
        degraded_reasons.push("at least one delivery required memory replay".to_string());
    }
    if let Some(degraded_reason) = goal_snapshot.degraded_reason.clone() {
        degraded_reasons.push(degraded_reason);
    }

    let continuity_status = match observed_turn_count {
        0 => {
            degraded_reasons.push("no receiver turns observed for parent thread".to_string());
            MemythosParentContinuityStatus::NoTurns
        }
        1 => {
            degraded_reasons.push("only one receiver turn observed".to_string());
            MemythosParentContinuityStatus::SingleTurnObserved
        }
        _ if memory_replay_required => MemythosParentContinuityStatus::Degraded,
        _ if goal_snapshot.goal_snapshot_ref.is_some() && latest_turn_completed_ref.is_some() => {
            MemythosParentContinuityStatus::Verified
        }
        _ => MemythosParentContinuityStatus::TurnContinuityObserved,
    };

    let mut evidence_refs = parent_deliveries
        .iter()
        .flat_map(|delivery| delivery.event_refs.clone())
        .collect::<Vec<_>>();
    evidence_refs.extend(goal_snapshot.evidence_refs.clone());
    if let Some(latest_turn_completed_ref) = latest_turn_completed_ref.clone() {
        evidence_refs.push(latest_turn_completed_ref);
    }
    if let Some(token_usage_ref) = token_usage_ref.clone() {
        evidence_refs.push(token_usage_ref);
    }
    evidence_refs.sort();
    evidence_refs.dedup();

    MemythosParentThreadContinuity {
        arena_id: parent.arena_id.clone(),
        thread_id: parent.thread_id.clone(),
        parent_role: parent.parent_role.clone(),
        stance_profile: parent.stance_profile.clone(),
        continuity_status,
        first_turn_id,
        latest_turn_id,
        observed_turn_count,
        memory_replay_required,
        goal_snapshot_available: goal_snapshot.goal_snapshot_ref.is_some(),
        goal_snapshot_ref: goal_snapshot.goal_snapshot_ref,
        budget_state_ref: goal_snapshot.budget_state_ref,
        goal_status: goal_snapshot.goal_status,
        token_budget: goal_snapshot.token_budget,
        tokens_used: goal_snapshot.tokens_used,
        time_used_seconds: goal_snapshot.time_used_seconds,
        latest_turn_completed_ref,
        token_usage_ref,
        evidence_refs,
        degraded_reasons,
    }
}

pub(super) fn build_parent_peer_response_observation(
    delivery: &MemythosArenaMessageDelivery,
) -> MemythosParentPeerResponseObservation {
    let observed_response_kind = if delivery.receiver_response_event_ref.is_some() {
        MemythosParentPeerResponseKind::Ack
    } else if delivery.receiver_turn_id.is_some() {
        MemythosParentPeerResponseKind::PendingResponse
    } else {
        MemythosParentPeerResponseKind::NoResponse
    };
    let semantic_alignment = if delivery.receiver_response_event_ref.is_some() {
        MemythosSemanticAlignment::Acceptable
    } else if delivery.receiver_turn_id.is_some() {
        MemythosSemanticAlignment::Pending
    } else {
        MemythosSemanticAlignment::Invalid
    };
    let actionable_next_step = match observed_response_kind {
        MemythosParentPeerResponseKind::PendingResponse => {
            Some("Wait for the receiver turn response event before promoting debate.".to_string())
        }
        MemythosParentPeerResponseKind::NoResponse => {
            Some("Retry or escalate because no receiver turn was created.".to_string())
        }
        _ => None,
    };

    MemythosParentPeerResponseObservation {
        observation_id: format!(
            "mem_observation_{}_{}",
            delivery.arena_id, delivery.message_id
        ),
        message_id: delivery.message_id.clone(),
        receiver_thread_id: delivery.receiver_thread_id.clone(),
        receiver_turn_id: delivery.receiver_turn_id.clone(),
        response_event_ref: delivery.receiver_response_event_ref.clone(),
        observed_response_kind,
        role_preserved: !delivery.delivered_as_human_instruction,
        treated_as_human_instruction: delivery.delivered_as_human_instruction,
        semantic_alignment,
        actionable_next_step,
        evidence_refs: delivery.event_refs.clone(),
    }
}
