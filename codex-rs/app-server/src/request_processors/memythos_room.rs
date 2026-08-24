use std::collections::HashSet;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaAggregateContract;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
use codex_app_server_protocol::MemythosParentConfiguration;
use codex_app_server_protocol::MemythosParentRole;
use codex_app_server_protocol::MemythosParentStance;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomActorKind;
use codex_app_server_protocol::MemythosRoomActorRef;
use codex_app_server_protocol::MemythosRoomParticipant;
use codex_app_server_protocol::MemythosRoomRegisterParams;
use serde::Deserialize;
use serde::Serialize;

use crate::error_code::invalid_params;
use crate::request_processors::memythos_parent_configuration::ParentConfigurationSnapshot;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolSendMessageArgs {
    pub(crate) target_parent_key: Option<String>,
    pub(crate) message: String,
    pub(crate) authority: String,
    pub(crate) message_kind: String,
    #[serde(default = "default_room_tool_response_contract")]
    pub(crate) response_contract: String,
    #[serde(default)]
    pub(crate) delivery_policy: Option<MemythosArenaDeliveryPolicy>,
    #[serde(default)]
    pub(crate) aggregate_contract: Option<MemythosArenaAggregateContract>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolSendToRoomArgs {
    pub(crate) target_room_id: String,
    pub(crate) message: String,
    pub(crate) authority: String,
    pub(crate) message_kind: String,
    #[serde(default = "default_cross_room_response_contract")]
    pub(crate) response_contract: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolRoom {
    pub(crate) room_id: String,
    pub(crate) arena_id: String,
    pub(crate) layer_id: String,
    pub(crate) concierge_parent_key: String,
    pub(crate) is_current_room: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolParticipant {
    pub(crate) parent_key: String,
    pub(crate) parent_role: String,
    pub(crate) stance_profile: String,
    pub(crate) is_current_parent: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolResponse {
    pub(crate) room_id: String,
    pub(crate) target_parent_key: String,
    pub(crate) target_thread_id: String,
    pub(crate) target_turn_id: String,
    pub(crate) response_item_ref: String,
    pub(crate) response_text: String,
    pub(crate) event_refs: Vec<String>,
}

fn default_room_tool_response_contract() -> String {
    "Respond in natural language with your position, rationale, limits, and next action."
        .to_string()
}

fn default_cross_room_response_contract() -> String {
    "Respond in natural language with the room outcome, unresolved definitions, and whether the caller should resume this same room.".to_string()
}
pub(super) fn validate_room_registration(
    params: &MemythosRoomRegisterParams,
) -> Result<(), JSONRPCErrorError> {
    if params.room_id.trim().is_empty() {
        return Err(invalid_params("room_id is required".to_string()));
    }
    if params.case_id.trim().is_empty() {
        return Err(invalid_params("case_id is required".to_string()));
    }
    if params.layer_id.trim().is_empty() {
        return Err(invalid_params("layer_id is required".to_string()));
    }
    if params.arena_id.trim().is_empty() {
        return Err(invalid_params("arena_id is required".to_string()));
    }
    if params.topology != "cross_parent_room" {
        return Err(invalid_params(
            "room topology must be cross_parent_room".to_string(),
        ));
    }
    if params.participants.len() < 2 {
        return Err(invalid_params(
            "room requires at least two participants".to_string(),
        ));
    }

    let mut seen_threads = HashSet::new();
    let mut seen_parent_keys = HashSet::new();
    for participant in &params.participants {
        if participant.parent_key.trim().is_empty() {
            return Err(invalid_params(
                "room participant parent_key is required".to_string(),
            ));
        }
        if participant.thread_id.trim().is_empty() {
            return Err(invalid_params(format!(
                "room participant {} must include thread_id",
                participant.parent_key
            )));
        }
        if participant.parent_role.trim().is_empty() {
            return Err(invalid_params(format!(
                "room participant {} must include parent_role",
                participant.parent_key
            )));
        }
        validate_parent_role_and_stance(
            &participant.parent_key,
            &participant.parent_role,
            &participant.stance_profile,
        )?;
        if !seen_threads.insert(participant.thread_id.clone()) {
            return Err(invalid_params(format!(
                "duplicate room participant thread: {}",
                participant.thread_id
            )));
        }
        if !seen_parent_keys.insert(participant.parent_key.clone()) {
            return Err(invalid_params(format!(
                "duplicate room participant parent_key: {}",
                participant.parent_key
            )));
        }
    }

    Ok(())
}

pub(super) fn validate_parent_role_and_stance(
    parent_key: &str,
    parent_role: &str,
    stance_profile: &str,
) -> Result<(), JSONRPCErrorError> {
    if parent_role == "observer" && stance_profile == "room_concierge" {
        return Err(invalid_params(format!(
            "room participant {parent_key} uses legacy observer + room_concierge encoding; use parent_role=room_concierge and stance_profile=coordination"
        )));
    }
    let role = MemythosParentRole::from_wire(parent_role).ok_or_else(|| {
        invalid_params(format!(
            "room participant {parent_key} has unsupported parent_role: {parent_role}"
        ))
    })?;
    let stance = MemythosParentStance::from_wire(stance_profile).ok_or_else(|| {
        invalid_params(format!(
            "room participant {parent_key} has unsupported stance_profile: {stance_profile}"
        ))
    })?;

    if role == MemythosParentRole::RoomConcierge
        && !matches!(
            stance,
            MemythosParentStance::Coordination
                | MemythosParentStance::Routing
                | MemythosParentStance::Synthesis
                | MemythosParentStance::EscalationControl
        )
    {
        return Err(invalid_params(format!(
            "room participant {parent_key} has invalid room_concierge stance_profile: {stance_profile}"
        )));
    }

    Ok(())
}

pub(super) fn room_participant_by_thread<'a>(
    room: &'a MemythosRoom,
    thread_id: &str,
) -> Option<&'a MemythosRoomParticipant> {
    room.participants
        .iter()
        .find(|participant| participant.thread_id == thread_id)
}

pub(super) fn app_server_actor_ref() -> MemythosRoomActorRef {
    MemythosRoomActorRef {
        kind: MemythosRoomActorKind::AppServer,
        thread_id: None,
        parent_key: None,
        role: None,
        stance: None,
        label: Some("app-server".to_string()),
    }
}

pub(super) fn runtime_room_concierge_actor_ref() -> MemythosRoomActorRef {
    MemythosRoomActorRef {
        kind: MemythosRoomActorKind::RoomConcierge,
        thread_id: None,
        parent_key: None,
        role: Some(MemythosParentRole::RoomConcierge),
        stance: Some(MemythosParentStance::Coordination),
        label: Some("room_concierge".to_string()),
    }
}

pub(super) fn human_actor_ref() -> MemythosRoomActorRef {
    MemythosRoomActorRef {
        kind: MemythosRoomActorKind::Human,
        thread_id: None,
        parent_key: None,
        role: None,
        stance: None,
        label: Some("human".to_string()),
    }
}

pub(super) fn room_actor_ref_for_participant(
    participant: &MemythosRoomParticipant,
) -> MemythosRoomActorRef {
    let role = MemythosParentRole::from_wire(&participant.parent_role)
        .expect("room participant role must be validated before actor ref creation");
    let stance = MemythosParentStance::from_wire(&participant.stance_profile)
        .expect("room participant stance must be validated before actor ref creation");
    MemythosRoomActorRef {
        kind: if role == MemythosParentRole::RoomConcierge {
            MemythosRoomActorKind::RoomConcierge
        } else {
            MemythosRoomActorKind::ParentThread
        },
        thread_id: Some(participant.thread_id.clone()),
        parent_key: Some(participant.parent_key.clone()),
        role: Some(role),
        stance: Some(stance),
        label: Some(participant.parent_role.clone()),
    }
}

pub(super) fn parent_configuration_for_participant(
    room: &MemythosRoom,
    participant: &MemythosRoomParticipant,
    snapshot: ParentConfigurationSnapshot,
) -> MemythosParentConfiguration {
    let role = MemythosParentRole::from_wire(&participant.parent_role)
        .expect("room participant role must be validated before setup creation");
    let stance = MemythosParentStance::from_wire(&participant.stance_profile)
        .expect("room participant stance must be validated before setup creation");
    MemythosParentConfiguration {
        thread_id: participant.thread_id.clone(),
        room_id: room.room_id.clone(),
        arena_id: room.arena_id.clone(),
        registered_role: role,
        effective_agent_role: snapshot.agent_role,
        stance,
        goal_ref: participant.goal_ref.clone(),
        authority_scope: participant.authority_scope.clone(),
        personality: snapshot.personality,
        multi_agent_mode: snapshot.multi_agent_mode,
        parent_thread_id: snapshot.parent_thread_id,
        collaboration_mode: snapshot.collaboration_mode,
        session_source: snapshot.session_source,
        config_sources: snapshot.config_sources,
        lifecycle_state: snapshot.lifecycle_state,
        blockers: snapshot.blockers,
    }
}
