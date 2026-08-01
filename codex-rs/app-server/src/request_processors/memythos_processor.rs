use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use chrono::Utc;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::AdditionalContextEntry;
use codex_app_server_protocol::AdditionalContextKind;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArena;
use codex_app_server_protocol::MemythosArenaCreateParams;
use codex_app_server_protocol::MemythosArenaCreateResponse;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosArenaListParams;
use codex_app_server_protocol::MemythosArenaListResponse;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosArenaMessageDelivery;
use codex_app_server_protocol::MemythosArenaMessageListParams;
use codex_app_server_protocol::MemythosArenaMessageListResponse;
use codex_app_server_protocol::MemythosArenaMessageObservationListParams;
use codex_app_server_protocol::MemythosArenaMessageObservationListResponse;
use codex_app_server_protocol::MemythosArenaMessageObserveParams;
use codex_app_server_protocol::MemythosArenaMessageObserveResponse;
use codex_app_server_protocol::MemythosArenaMessageSendParams;
use codex_app_server_protocol::MemythosArenaMessageSendResponse;
use codex_app_server_protocol::MemythosArenaMessageSendV2Params;
use codex_app_server_protocol::MemythosArenaMessageSendV2Response;
use codex_app_server_protocol::MemythosArenaParent;
use codex_app_server_protocol::MemythosArenaParentRegisterParams;
use codex_app_server_protocol::MemythosArenaParentRegisterResponse;
use codex_app_server_protocol::MemythosArenaParticipantRegisterParams;
use codex_app_server_protocol::MemythosArenaParticipantRegisterResponse;
use codex_app_server_protocol::MemythosArenaPhaseCloseParams;
use codex_app_server_protocol::MemythosArenaPhaseCloseResponse;
use codex_app_server_protocol::MemythosArenaPhaseStartParams;
use codex_app_server_protocol::MemythosArenaPhaseStartResponse;
use codex_app_server_protocol::MemythosArenaRunParams;
use codex_app_server_protocol::MemythosArenaRunResponse;
use codex_app_server_protocol::MemythosArenaStateGetParams;
use codex_app_server_protocol::MemythosArenaStateGetResponse;
use codex_app_server_protocol::MemythosEventChannel;
use codex_app_server_protocol::MemythosLayer;
use codex_app_server_protocol::MemythosLayerCreateParams;
use codex_app_server_protocol::MemythosLayerCreateResponse;
use codex_app_server_protocol::MemythosLayerListParams;
use codex_app_server_protocol::MemythosLayerListResponse;
use codex_app_server_protocol::MemythosParentConfiguration;
use codex_app_server_protocol::MemythosParentContinuityListParams;
use codex_app_server_protocol::MemythosParentContinuityListResponse;
use codex_app_server_protocol::MemythosParentContinuityStatus;
use codex_app_server_protocol::MemythosParentPeerResponseKind;
use codex_app_server_protocol::MemythosParentPeerResponseObservation;
use codex_app_server_protocol::MemythosParentRole;
use codex_app_server_protocol::MemythosParentStance;
use codex_app_server_protocol::MemythosParentThreadContinuity;
use codex_app_server_protocol::MemythosPromptLineagePart;
use codex_app_server_protocol::MemythosPromptOrigin;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomActivityCollab;
use codex_app_server_protocol::MemythosRoomActivityEvent;
use codex_app_server_protocol::MemythosRoomActivityItem;
use codex_app_server_protocol::MemythosRoomActivityLifecycle;
use codex_app_server_protocol::MemythosRoomActivityListParams;
use codex_app_server_protocol::MemythosRoomActivityListResponse;
use codex_app_server_protocol::MemythosRoomActivityParticipant;
use codex_app_server_protocol::MemythosRoomActivitySubagents;
use codex_app_server_protocol::MemythosRoomActivityTurn;
use codex_app_server_protocol::MemythosRoomActivityUsage;
use codex_app_server_protocol::MemythosRoomActorKind;
use codex_app_server_protocol::MemythosRoomActorRef;
use codex_app_server_protocol::MemythosRoomDialogueEntry;
use codex_app_server_protocol::MemythosRoomDialogueListParams;
use codex_app_server_protocol::MemythosRoomDialogueListResponse;
use codex_app_server_protocol::MemythosRoomListParams;
use codex_app_server_protocol::MemythosRoomListResponse;
use codex_app_server_protocol::MemythosRoomParentConfigurationListParams;
use codex_app_server_protocol::MemythosRoomParentConfigurationListResponse;
use codex_app_server_protocol::MemythosRoomParticipant;
use codex_app_server_protocol::MemythosRoomRegisterParams;
use codex_app_server_protocol::MemythosRoomRegisterResponse;
use codex_app_server_protocol::MemythosRoomSendInputDelivery;
use codex_app_server_protocol::MemythosRoomSendInputParams;
use codex_app_server_protocol::MemythosRoomSendInputResponse;
use codex_app_server_protocol::MemythosRuntimeCloseParams;
use codex_app_server_protocol::MemythosRuntimeCloseResponse;
use codex_app_server_protocol::MemythosRuntimeHealthParams;
use codex_app_server_protocol::MemythosRuntimeHealthResponse;
use codex_app_server_protocol::MemythosRuntimeLifecycleState;
use codex_app_server_protocol::MemythosSemanticAlignment;
use codex_app_server_protocol::MemythosStructuredContract;
use codex_app_server_protocol::MemythosTelemetryListParams;
use codex_app_server_protocol::MemythosTelemetryListResponse;
use codex_app_server_protocol::MemythosTelemetryRef;
use codex_app_server_protocol::MemythosTelemetryRefKind;
use codex_app_server_protocol::MemythosTelemetrySource;
use codex_app_server_protocol::MemythosThreadAttachParams;
use codex_app_server_protocol::MemythosThreadAttachResponse;
use codex_app_server_protocol::MemythosThreadAttachment;
use codex_app_server_protocol::MemythosThreadConsolidateParams;
use codex_app_server_protocol::MemythosThreadConsolidateResponse;
use codex_app_server_protocol::MemythosThreadConsolidationSourceRef;
use codex_app_server_protocol::MemythosThreadContractAssembleParams;
use codex_app_server_protocol::MemythosThreadContractAssembleResponse;
use codex_app_server_protocol::MemythosThreadContractListParams;
use codex_app_server_protocol::MemythosThreadContractListResponse;
use codex_app_server_protocol::MemythosThreadContractReadParams;
use codex_app_server_protocol::MemythosThreadContractReadResponse;
use codex_app_server_protocol::MemythosThreadListParams;
use codex_app_server_protocol::MemythosThreadListResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadGoalGetParams;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_core::ThreadManager;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::error_code::invalid_params;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::ThreadGoalRequestProcessor;
use crate::request_processors::ThreadRequestProcessor;
use crate::request_processors::TurnRequestProcessor;

struct MemythosRuntimeState {
    runtime_id: String,
    lifecycle_state: MemythosRuntimeLifecycleState,
    runtime_family: String,
    connection_mode: String,
    transport_owner: String,
    transport_id: Option<String>,
    daemon_runtime_verified: bool,
    degraded_reasons: Vec<String>,
    layers: HashMap<String, MemythosLayer>,
    arenas: HashMap<String, MemythosArena>,
    rooms: HashMap<String, MemythosRoom>,
    thread_attachments: HashMap<String, MemythosThreadAttachment>,
    arena_parents: HashMap<String, MemythosArenaParent>,
    arena_message_deliveries: Vec<MemythosArenaMessageDelivery>,
    room_activity_events: HashMap<String, Vec<MemythosRoomActivityEvent>>,
    structured_contracts: HashMap<String, MemythosStructuredContract>,
    native_token_usage_refs: HashMap<String, String>,
    telemetry_refs: Vec<MemythosTelemetryRef>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ParentConfigurationSnapshot {
    agent_role: Option<String>,
    personality: Option<String>,
    multi_agent_mode: Option<String>,
    parent_thread_id: Option<String>,
    collaboration_mode: String,
    session_source: String,
    config_sources: Vec<String>,
    lifecycle_state: String,
    blockers: Vec<String>,
}

type ParentConfigurationFuture<'a> =
    Pin<Box<dyn Future<Output = ParentConfigurationSnapshot> + Send + 'a>>;

pub(crate) trait ParentConfigurationAdapter: Send + Sync {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a>;
}

#[cfg(test)]
#[derive(Debug)]
struct RecordOnlyParentConfigurationAdapter;

#[cfg(test)]
impl ParentConfigurationAdapter for RecordOnlyParentConfigurationAdapter {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a> {
        Box::pin(async move {
            ParentConfigurationSnapshot {
                collaboration_mode: "unknown".to_string(),
                session_source: "unavailable".to_string(),
                lifecycle_state: "registered".to_string(),
                config_sources: vec![format!("app-server://threads/{thread_id}/config")],
                blockers: vec!["live thread configuration projection unavailable".to_string()],
                ..Default::default()
            }
        })
    }
}

pub(crate) struct ThreadManagerParentConfigurationAdapter {
    thread_manager: Arc<ThreadManager>,
}

impl ThreadManagerParentConfigurationAdapter {
    pub(crate) fn new(thread_manager: Arc<ThreadManager>) -> Self {
        Self { thread_manager }
    }
}

impl ParentConfigurationAdapter for ThreadManagerParentConfigurationAdapter {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a> {
        Box::pin(async move {
            let parsed_thread_id = match ThreadId::from_string(thread_id) {
                Ok(thread_id) => thread_id,
                Err(error) => {
                    return ParentConfigurationSnapshot {
                        collaboration_mode: "unknown".to_string(),
                        session_source: "unavailable".to_string(),
                        lifecycle_state: "invalid_thread_id".to_string(),
                        blockers: vec![format!("invalid native thread id: {error}")],
                        ..Default::default()
                    };
                }
            };
            let thread = match self.thread_manager.get_thread(parsed_thread_id).await {
                Ok(thread) => thread,
                Err(error) => {
                    return ParentConfigurationSnapshot {
                        collaboration_mode: "unknown".to_string(),
                        session_source: "unavailable".to_string(),
                        lifecycle_state: "thread_unavailable".to_string(),
                        blockers: vec![format!("native thread unavailable: {error}")],
                        ..Default::default()
                    };
                }
            };
            let snapshot = thread.config_snapshot().await;
            let mut config_sources = vec![format!("app-server://threads/{thread_id}/config")];
            if let Some(agent_role) = snapshot.agent_role.as_ref() {
                config_sources.push(format!("agent-role://{agent_role}"));
            }
            ParentConfigurationSnapshot {
                agent_role: snapshot.agent_role,
                personality: snapshot.personality.map(|value| value.to_string()),
                multi_agent_mode: snapshot.multi_agent_mode.map(|value| value.to_string()),
                parent_thread_id: snapshot.parent_thread_id.map(|value| value.to_string()),
                collaboration_mode: format!("{:?}", snapshot.collaboration_mode.mode)
                    .to_lowercase(),
                session_source: format!("{:?}", snapshot.session_source).to_lowercase(),
                config_sources,
                lifecycle_state: "loaded".to_string(),
                blockers: Vec::new(),
            }
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolSendMessageArgs {
    pub(crate) target_parent_key: Option<String>,
    pub(crate) message: String,
    pub(crate) authority: String,
    #[serde(default = "default_room_tool_message_kind")]
    pub(crate) message_kind: String,
    #[serde(default = "default_room_tool_response_contract")]
    pub(crate) response_contract: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemythosRoomToolSendToRoomArgs {
    pub(crate) target_room_id: String,
    pub(crate) message: String,
    pub(crate) authority: String,
    #[serde(default = "default_cross_room_message_kind")]
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

fn default_room_tool_message_kind() -> String {
    "consultation".to_string()
}

fn default_room_tool_response_contract() -> String {
    "Respond in natural language with your position, rationale, limits, and next action."
        .to_string()
}

fn default_cross_room_message_kind() -> String {
    "cross_room_delegation".to_string()
}

fn default_cross_room_response_contract() -> String {
    "Respond in natural language with the room outcome, unresolved definitions, and whether the caller should resume this same room.".to_string()
}

struct MemythosArenaPhaseUpdate {
    arena_id: String,
    round_id: String,
    phase: String,
    lifecycle_state: MemythosArenaLifecycleState,
    event_refs: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParentGoalSnapshot {
    goal_snapshot_ref: Option<String>,
    budget_state_ref: Option<String>,
    goal_status: Option<ThreadGoalStatus>,
    token_budget: Option<i64>,
    tokens_used: Option<i64>,
    time_used_seconds: Option<i64>,
    evidence_refs: Vec<String>,
    degraded_reason: Option<String>,
}

pub(crate) type ParentGoalSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = ParentGoalSnapshot> + Send + 'a>>;

pub(crate) trait ParentGoalSnapshotAdapter: Send + Sync {
    fn current_goal_snapshot<'a>(&'a self, thread_id: &'a str) -> ParentGoalSnapshotFuture<'a>;
}

#[derive(Debug)]
#[cfg(test)]
struct RecordOnlyParentGoalSnapshotAdapter;

#[cfg(test)]
impl ParentGoalSnapshotAdapter for RecordOnlyParentGoalSnapshotAdapter {
    fn current_goal_snapshot<'a>(&'a self, _thread_id: &'a str) -> ParentGoalSnapshotFuture<'a> {
        Box::pin(async move {
            ParentGoalSnapshot {
                goal_snapshot_ref: None,
                budget_state_ref: None,
                goal_status: None,
                token_budget: None,
                tokens_used: None,
                time_used_seconds: None,
                evidence_refs: Vec::new(),
                degraded_reason: Some("goal snapshot adapter not available".to_string()),
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct ThreadGoalParentSnapshotAdapter {
    thread_goal_processor: ThreadGoalRequestProcessor,
}

impl ThreadGoalParentSnapshotAdapter {
    pub(crate) fn new(thread_goal_processor: ThreadGoalRequestProcessor) -> Self {
        Self {
            thread_goal_processor,
        }
    }
}

impl ParentGoalSnapshotAdapter for ThreadGoalParentSnapshotAdapter {
    fn current_goal_snapshot<'a>(&'a self, thread_id: &'a str) -> ParentGoalSnapshotFuture<'a> {
        Box::pin(async move {
            match self
                .thread_goal_processor
                .thread_goal_get(ThreadGoalGetParams {
                    thread_id: thread_id.to_string(),
                })
                .await
            {
                Ok(Some(ClientResponsePayload::ThreadGoalGet(response))) => {
                    parent_goal_snapshot_from_goal(thread_id, response.goal)
                }
                Ok(_) => ParentGoalSnapshot {
                    goal_snapshot_ref: None,
                    budget_state_ref: None,
                    goal_status: None,
                    token_budget: None,
                    tokens_used: None,
                    time_used_seconds: None,
                    evidence_refs: Vec::new(),
                    degraded_reason: Some("thread/goal/get returned no goal payload".to_string()),
                },
                Err(error) => ParentGoalSnapshot {
                    goal_snapshot_ref: None,
                    budget_state_ref: None,
                    goal_status: None,
                    token_budget: None,
                    tokens_used: None,
                    time_used_seconds: None,
                    evidence_refs: Vec::new(),
                    degraded_reason: Some(format!("thread/goal/get failed: {}", error.message)),
                },
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PeerParentDeliveryAttempt {
    status: String,
    delivery_mechanism: String,
    receiver_turn_id: Option<String>,
    receiver_response_event_ref: Option<String>,
    delivered_as_human_instruction: bool,
    memory_replay_required: bool,
    event_refs: Vec<String>,
    rejection_reason: Option<String>,
    telemetry_channel: MemythosEventChannel,
    telemetry_summary: String,
}

pub(crate) type PeerParentDeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = PeerParentDeliveryAttempt> + Send + 'a>>;

pub(crate) trait PeerParentDeliveryAdapter: Send + Sync {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
    ) -> PeerParentDeliveryFuture<'a>;
}

#[derive(Debug)]
#[cfg(test)]
struct RecordOnlyPeerParentDeliveryAdapter;

#[cfg(test)]
impl PeerParentDeliveryAdapter for RecordOnlyPeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            let event_ref = format!(
                "memythos://arenas/{}/rounds/{}/messages/{}",
                message.arena_id, message.round_id, message.message_id
            );
            PeerParentDeliveryAttempt {
                status: "recorded".to_string(),
                delivery_mechanism: "record_only".to_string(),
                receiver_turn_id: None,
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: vec![event_ref],
                rejection_reason: Some(
                    "live delivery not available in this runtime mode".to_string(),
                ),
                telemetry_channel: MemythosEventChannel::TechnicalDetail,
                telemetry_summary: format!(
                    "Arena message {} recorded from {} to {}; live turn delivery is not proven.",
                    message.message_id, message.from_parent_thread_id, message.to_parent_thread_id
                ),
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct TurnStartPeerParentDeliveryAdapter {
    turn_processor: TurnRequestProcessor,
}

impl TurnStartPeerParentDeliveryAdapter {
    pub(crate) fn new(turn_processor: TurnRequestProcessor) -> Self {
        Self { turn_processor }
    }
}

impl PeerParentDeliveryAdapter for TurnStartPeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            let request_id = ConnectionRequestId {
                connection_id: ConnectionId(0),
                request_id: RequestId::String(format!(
                    "memythos-peer-parent:{}",
                    message.message_id
                )),
            };
            let envelope = build_peer_parent_envelope(message);
            let mut metadata = HashMap::new();
            metadata.insert(
                "memythos_message_id".to_string(),
                message.message_id.clone(),
            );
            metadata.insert("memythos_arena_id".to_string(), message.arena_id.clone());
            metadata.insert("memythos_round_id".to_string(), message.round_id.clone());
            let human_instruction = message.from_parent_role == "human";
            metadata.insert(
                "memythos_peer_parent".to_string(),
                (!human_instruction).to_string(),
            );
            metadata.insert(
                "human_instruction".to_string(),
                human_instruction.to_string(),
            );
            let mut additional_context = HashMap::new();
            additional_context.insert(
                if human_instruction {
                    "memythos.human_intake".to_string()
                } else {
                    "memythos.peer_parent".to_string()
                },
                AdditionalContextEntry {
                    value: envelope.clone(),
                    kind: AdditionalContextKind::Application,
                },
            );
            let params = TurnStartParams {
                thread_id: message.to_parent_thread_id.clone(),
                client_user_message_id: Some(message.message_id.clone()),
                input: vec![UserInput::Text {
                    text: envelope,
                    text_elements: vec![],
                }],
                responsesapi_client_metadata: Some(metadata),
                additional_context: Some(additional_context),
                environments: None,
                cwd: None,
                runtime_workspace_roots: None,
                approval_policy: None,
                approvals_reviewer: None,
                sandbox_policy: None,
                permissions: None,
                model: None,
                service_tier: None,
                effort: None,
                summary: None,
                personality: None,
                output_schema: None,
                collaboration_mode: None,
                multi_agent_mode: None,
            };

            match self
                .turn_processor
                .turn_start(
                    request_id,
                    params,
                    Some("memythos".to_string()),
                    None,
                    false,
                )
                .await
            {
                Ok(Some(ClientResponsePayload::TurnStart(response))) => {
                    let turn_id = response.turn.id;
                    PeerParentDeliveryAttempt {
                        status: "delivered_to_live_thread".to_string(),
                        delivery_mechanism: "turn_start".to_string(),
                        receiver_turn_id: Some(turn_id.clone()),
                        receiver_response_event_ref: None,
                        delivered_as_human_instruction: human_instruction,
                        memory_replay_required: false,
                        event_refs: vec![
                            format!(
                                "memythos://arenas/{}/rounds/{}/messages/{}",
                                message.arena_id, message.round_id, message.message_id
                            ),
                            format!(
                                "app-server://threads/{}/turns/{}",
                                message.to_parent_thread_id, turn_id
                            ),
                        ],
                        rejection_reason: None,
                        telemetry_channel: MemythosEventChannel::StateTransition,
                        telemetry_summary: format!(
                            "Arena message {} delivered to live parent thread {} with turn {}.",
                            message.message_id, message.to_parent_thread_id, turn_id
                        ),
                    }
                }
                Ok(_) => failed_live_delivery_attempt(
                    message,
                    "turn/start returned no turn response for peer-parent delivery",
                ),
                Err(error) => failed_live_delivery_attempt(
                    message,
                    &format!(
                        "turn/start failed for peer-parent delivery: {}",
                        error.message
                    ),
                ),
            }
        })
    }
}

fn failed_live_delivery_attempt(
    message: &MemythosArenaMessage,
    reason: &str,
) -> PeerParentDeliveryAttempt {
    PeerParentDeliveryAttempt {
        status: "failed_live_delivery".to_string(),
        delivery_mechanism: "turn_start".to_string(),
        receiver_turn_id: None,
        receiver_response_event_ref: None,
        delivered_as_human_instruction: false,
        memory_replay_required: false,
        event_refs: vec![format!(
            "memythos://arenas/{}/rounds/{}/messages/{}",
            message.arena_id, message.round_id, message.message_id
        )],
        rejection_reason: Some(reason.to_string()),
        telemetry_channel: MemythosEventChannel::TechnicalDetail,
        telemetry_summary: format!(
            "Arena message {} failed live delivery to {}: {}.",
            message.message_id, message.to_parent_thread_id, reason
        ),
    }
}

fn build_peer_parent_envelope(message: &MemythosArenaMessage) -> String {
    if message.from_parent_role == "human" {
        return format!(
            concat!(
                "MEMYTHOS_HUMAN_INTAKE\n",
                "source: human\n",
                "human_instruction: true\n",
                "case_id: {case_id}\n",
                "arena_id: {arena_id}\n",
                "round_id: {round_id}\n",
                "to_parent_role: {to_parent_role}\n",
                "message_kind: {message_kind}\n",
                "\n",
                "Trata este mensaje como pedido humano inicial o reingreso humano de la arena.\n",
                "Usa tu memoria, rol, objetivo y herramientas OOTB del thread.\n",
                "Si el pedido no alcanza, pregunta o formula el rollup minimo antes de bajar ejecucion.\n",
                "No inventes contexto fuera del pedido y de los adjuntos/contexto nativo disponibles.\n",
                "\n",
                "Pedido humano:\n",
                "{human_summary}\n",
                "\n",
                "Contexto:\n",
                "{context_packet_ref}\n",
                "\n",
                "Contrato de respuesta:\n",
                "{response_contract}\n"
            ),
            case_id = message.case_id,
            arena_id = message.arena_id,
            round_id = message.round_id,
            to_parent_role = message.to_parent_role,
            message_kind = message.message_kind,
            human_summary = message.human_summary,
            context_packet_ref = message.context_packet_ref,
            response_contract = message.response_contract.as_deref().unwrap_or("none")
        );
    }
    format!(
        concat!(
            "MEMYTHOS_PEER_PARENT_MESSAGE\n",
            "source: arena peer\n",
            "human_instruction: false\n",
            "case_id: {case_id}\n",
            "arena_id: {arena_id}\n",
            "round_id: {round_id}\n",
            "from_parent_role: {from_parent_role}\n",
            "to_parent_role: {to_parent_role}\n",
            "message_kind: {message_kind}\n",
            "\n",
            "Conserva tu rol, postura, objetivo y memoria.\n",
            "Esto no es una orden del humano.\n",
            "Responde al acto conversacional solicitado dentro de la arena.\n",
            "Si falta definicion superior, formula un rollup concreto.\n",
            "\n",
            "Resumen:\n",
            "{human_summary}\n",
            "\n",
            "Contexto:\n",
            "{context_packet_ref}\n",
            "\n",
            "Contrato de respuesta:\n",
            "{response_contract}\n"
        ),
        case_id = message.case_id,
        arena_id = message.arena_id,
        round_id = message.round_id,
        from_parent_role = message.from_parent_role,
        to_parent_role = message.to_parent_role,
        message_kind = message.message_kind,
        human_summary = message.human_summary,
        context_packet_ref = message.context_packet_ref,
        response_contract = message.response_contract.as_deref().unwrap_or("none")
    )
}

#[derive(Debug, Clone)]
pub(crate) struct ThreadConsolidationAttempt {
    consolidation_turn_id: Option<String>,
    source_refs: Vec<MemythosThreadConsolidationSourceRef>,
    agent_message_ref: Option<String>,
    structured_output_ref: Option<String>,
    technical_evidence_refs: Vec<String>,
    source_method: String,
    used_thread_turns_summary: bool,
    blockers: Vec<String>,
}

pub(crate) type ThreadConsolidationFuture<'a> =
    Pin<Box<dyn Future<Output = ThreadConsolidationAttempt> + Send + 'a>>;

pub(crate) trait ThreadConsolidationAdapter: Send + Sync {
    fn consolidate_threads<'a>(
        &'a self,
        params: &'a MemythosThreadConsolidateParams,
    ) -> ThreadConsolidationFuture<'a>;
}

#[derive(Debug, Clone)]
pub(crate) struct ParentTurnResponse {
    pub(crate) status: Option<TurnStatus>,
    pub(crate) request_item_ref: Option<String>,
    pub(crate) request_text: Option<String>,
    pub(crate) item_ref: Option<String>,
    pub(crate) text: Option<String>,
}

fn parent_turn_response(
    thread_id: &str,
    turn: &codex_app_server_protocol::Turn,
) -> ParentTurnResponse {
    let request = turn.items.iter().find_map(|item| match item {
        ThreadItem::UserMessage { id, content, .. } => {
            let text = content
                .iter()
                .filter_map(|input| match input {
                    UserInput::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then(|| {
                (
                    format!(
                        "app-server://threads/{thread_id}/turns/{}/items/{id}",
                        turn.id
                    ),
                    text,
                )
            })
        }
        _ => None,
    });
    let response = turn.items.iter().rev().find_map(|item| match item {
        ThreadItem::AgentMessage { id, text, .. } => Some((
            format!(
                "app-server://threads/{thread_id}/turns/{}/items/{id}",
                turn.id
            ),
            text.clone(),
        )),
        _ => None,
    });
    ParentTurnResponse {
        status: Some(turn.status.clone()),
        request_item_ref: request.as_ref().map(|(item_ref, _)| item_ref.clone()),
        request_text: request.map(|(_, text)| text),
        item_ref: response.as_ref().map(|(item_ref, _)| item_ref.clone()),
        text: response.map(|(_, text)| text),
    }
}

pub(crate) type ParentTurnResponseFuture<'a> =
    Pin<Box<dyn Future<Output = ParentTurnResponse> + Send + 'a>>;
pub(crate) type ParentTurnResponsesFuture<'a> =
    Pin<Box<dyn Future<Output = HashMap<(String, String), ParentTurnResponse>> + Send + 'a>>;

pub(crate) trait ParentTurnResponseAdapter: Send + Sync {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a>;

    fn read_responses<'a>(&'a self, turns: Vec<(String, String)>) -> ParentTurnResponsesFuture<'a> {
        Box::pin(async move {
            let mut responses = HashMap::new();
            for (thread_id, turn_id) in turns {
                let response = self.read_response(&thread_id, &turn_id).await;
                responses.insert((thread_id, turn_id), response);
            }
            responses
        })
    }
}

#[derive(Debug)]
#[cfg(test)]
struct RecordOnlyParentTurnResponseAdapter;

#[cfg(test)]
impl ParentTurnResponseAdapter for RecordOnlyParentTurnResponseAdapter {
    fn read_response<'a>(
        &'a self,
        _thread_id: &'a str,
        _turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a> {
        Box::pin(async {
            ParentTurnResponse {
                status: None,
                request_item_ref: None,
                request_text: None,
                item_ref: None,
                text: None,
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct ThreadTurnsParentResponseAdapter {
    thread_processor: ThreadRequestProcessor,
}

impl ThreadTurnsParentResponseAdapter {
    pub(crate) fn new(thread_processor: ThreadRequestProcessor) -> Self {
        Self { thread_processor }
    }
}

impl ParentTurnResponseAdapter for ThreadTurnsParentResponseAdapter {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a> {
        Box::pin(async move {
            let response = self
                .thread_processor
                .thread_turns_list(ThreadTurnsListParams {
                    thread_id: thread_id.to_string(),
                    cursor: None,
                    limit: Some(10),
                    sort_direction: Some(SortDirection::Desc),
                    items_view: Some(TurnItemsView::Full),
                })
                .await;
            let Ok(Some(ClientResponsePayload::ThreadTurnsList(response))) = response else {
                return ParentTurnResponse {
                    status: None,
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            };
            let Some(turn) = response.data.iter().find(|turn| turn.id == turn_id) else {
                return ParentTurnResponse {
                    status: None,
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            };
            parent_turn_response(thread_id, turn)
        })
    }

    fn read_responses<'a>(&'a self, turns: Vec<(String, String)>) -> ParentTurnResponsesFuture<'a> {
        Box::pin(async move {
            let mut requested_by_thread = HashMap::<String, HashSet<String>>::new();
            for (thread_id, turn_id) in turns {
                requested_by_thread
                    .entry(thread_id)
                    .or_default()
                    .insert(turn_id);
            }

            let mut responses = HashMap::new();
            for (thread_id, mut requested_turn_ids) in requested_by_thread {
                let mut cursor = None;
                while !requested_turn_ids.is_empty() {
                    let response = self
                        .thread_processor
                        .thread_turns_list(ThreadTurnsListParams {
                            thread_id: thread_id.clone(),
                            cursor: cursor.clone(),
                            limit: Some(100),
                            sort_direction: Some(SortDirection::Desc),
                            items_view: Some(TurnItemsView::Full),
                        })
                        .await;
                    let Ok(Some(ClientResponsePayload::ThreadTurnsList(page))) = response else {
                        break;
                    };

                    for turn in page.data {
                        if !requested_turn_ids.remove(&turn.id) {
                            continue;
                        }
                        let response = parent_turn_response(&thread_id, &turn);
                        responses.insert((thread_id.clone(), turn.id), response);
                    }

                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    if cursor.as_deref() == Some(next_cursor.as_str()) {
                        break;
                    }
                    cursor = Some(next_cursor);
                }
            }
            responses
        })
    }
}

#[derive(Debug)]
#[cfg(test)]
struct RecordOnlyThreadConsolidationAdapter;

#[cfg(test)]
impl ThreadConsolidationAdapter for RecordOnlyThreadConsolidationAdapter {
    fn consolidate_threads<'a>(
        &'a self,
        params: &'a MemythosThreadConsolidateParams,
    ) -> ThreadConsolidationFuture<'a> {
        Box::pin(async move {
            ThreadConsolidationAttempt {
                consolidation_turn_id: None,
                source_refs: params
                    .source_thread_ids
                    .iter()
                    .map(|thread_id| MemythosThreadConsolidationSourceRef {
                        thread_id: thread_id.clone(),
                        turn_refs: Vec::new(),
                        items_view: "summary".to_string(),
                        cursor: params.since_cursors.get(thread_id).cloned(),
                        next_cursor: params.since_cursors.get(thread_id).cloned(),
                        latest_agent_message_ref: None,
                        latest_agent_message_text: None,
                        technical_evidence_refs: Vec::new(),
                    })
                    .collect(),
                agent_message_ref: None,
                structured_output_ref: None,
                technical_evidence_refs: Vec::new(),
                source_method: "record_only".to_string(),
                used_thread_turns_summary: false,
                blockers: vec![
                    "thread consolidation adapter not available in this runtime mode".to_string(),
                ],
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct TurnStartThreadConsolidationAdapter {
    thread_processor: ThreadRequestProcessor,
    turn_processor: TurnRequestProcessor,
}

impl TurnStartThreadConsolidationAdapter {
    pub(crate) fn new(
        thread_processor: ThreadRequestProcessor,
        turn_processor: TurnRequestProcessor,
    ) -> Self {
        Self {
            thread_processor,
            turn_processor,
        }
    }
}

impl ThreadConsolidationAdapter for TurnStartThreadConsolidationAdapter {
    fn consolidate_threads<'a>(
        &'a self,
        params: &'a MemythosThreadConsolidateParams,
    ) -> ThreadConsolidationFuture<'a> {
        Box::pin(async move {
            let mut source_refs = Vec::with_capacity(params.source_thread_ids.len());
            let mut technical_evidence_refs = Vec::new();
            let mut blockers = Vec::new();
            let items_view = normalize_consolidation_items_view(params.items_view.as_deref());
            let per_source_limit = params.per_source_limit.unwrap_or(3).clamp(1, 10);

            for source_thread_id in &params.source_thread_ids {
                let cursor = params.since_cursors.get(source_thread_id).cloned();
                match self
                    .thread_processor
                    .thread_turns_list(ThreadTurnsListParams {
                        thread_id: source_thread_id.clone(),
                        cursor: cursor.clone(),
                        limit: Some(per_source_limit),
                        sort_direction: Some(SortDirection::Desc),
                        items_view: Some(TurnItemsView::Summary),
                    })
                    .await
                {
                    Ok(Some(ClientResponsePayload::ThreadTurnsList(response))) => {
                        let mut turn_refs = Vec::new();
                        let mut source_technical_evidence_refs = Vec::new();
                        let mut latest_agent_message_ref = None;
                        let mut latest_agent_message_text = None;
                        for turn in &response.data {
                            let turn_ref = format!(
                                "app-server://threads/{}/turns/{}",
                                source_thread_id, turn.id
                            );
                            turn_refs.push(turn_ref);
                            for item in &turn.items {
                                match item {
                                    ThreadItem::AgentMessage { id, text, .. } => {
                                        latest_agent_message_ref = Some(format!(
                                            "app-server://threads/{}/turns/{}/items/{}",
                                            source_thread_id, turn.id, id
                                        ));
                                        latest_agent_message_text = Some(text.clone());
                                    }
                                    ThreadItem::CollabAgentToolCall { id, .. }
                                    | ThreadItem::SubAgentActivity { id, .. } => {
                                        let evidence_ref = format!(
                                            "app-server://threads/{}/turns/{}/items/{}",
                                            source_thread_id, turn.id, id
                                        );
                                        source_technical_evidence_refs.push(evidence_ref.clone());
                                        technical_evidence_refs.push(evidence_ref);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        source_refs.push(MemythosThreadConsolidationSourceRef {
                            thread_id: source_thread_id.clone(),
                            turn_refs,
                            items_view: items_view.to_string(),
                            cursor,
                            next_cursor: response.next_cursor.or(response.backwards_cursor),
                            latest_agent_message_ref,
                            latest_agent_message_text,
                            technical_evidence_refs: compact_event_refs(
                                source_technical_evidence_refs,
                            ),
                        });
                    }
                    Ok(_) => {
                        blockers.push(format!(
                            "thread/turns/list returned no turns payload for {}",
                            source_thread_id
                        ));
                        source_refs.push(empty_consolidation_source_ref(
                            source_thread_id,
                            cursor,
                            items_view,
                        ));
                    }
                    Err(error) => {
                        blockers.push(format!(
                            "thread/turns/list failed for {}: {}",
                            source_thread_id, error.message
                        ));
                        source_refs.push(empty_consolidation_source_ref(
                            source_thread_id,
                            cursor,
                            items_view,
                        ));
                    }
                }
            }

            let context_payload = serde_json::json!({
                "purpose": params.purpose,
                "authorityMode": params.authority_mode,
                "sourceRefs": &source_refs,
                "technicalEvidenceRefs": &technical_evidence_refs,
                "instructions": params.instructions,
                "humanInstruction": false,
                "sourceMethod": "thread/turns/list",
                "itemsView": items_view
            });
            let context_text =
                serde_json::to_string_pretty(&context_payload).unwrap_or_else(|_| "{}".to_string());
            let prompt = build_thread_consolidation_prompt(params);
            let mut additional_context = HashMap::new();
            additional_context.insert(
                "memythos.thread_consolidation".to_string(),
                AdditionalContextEntry {
                    value: context_text,
                    kind: AdditionalContextKind::Application,
                },
            );
            let request_id = ConnectionRequestId {
                connection_id: ConnectionId(0),
                request_id: RequestId::String(format!(
                    "memythos-thread-consolidate:{}",
                    params
                        .client_user_message_id
                        .clone()
                        .unwrap_or_else(|| params.coordinator_thread_id.clone())
                )),
            };
            let mut metadata = HashMap::new();
            metadata.insert(
                "memythos_thread_consolidation".to_string(),
                "true".to_string(),
            );
            metadata.insert(
                "memythos_purpose".to_string(),
                format!("{:?}", params.purpose),
            );
            let turn_params = TurnStartParams {
                thread_id: params.coordinator_thread_id.clone(),
                client_user_message_id: params.client_user_message_id.clone(),
                input: vec![UserInput::Text {
                    text: prompt,
                    text_elements: vec![],
                }],
                responsesapi_client_metadata: Some(metadata),
                additional_context: Some(additional_context),
                environments: None,
                cwd: None,
                runtime_workspace_roots: None,
                approval_policy: None,
                approvals_reviewer: None,
                sandbox_policy: None,
                permissions: None,
                model: None,
                service_tier: None,
                effort: None,
                summary: None,
                personality: None,
                output_schema: params.output_schema.clone(),
                collaboration_mode: None,
                multi_agent_mode: None,
            };

            let (consolidation_turn_id, agent_message_ref, structured_output_ref) = match self
                .turn_processor
                .turn_start(
                    request_id,
                    turn_params,
                    Some("memythos".to_string()),
                    None,
                    false,
                )
                .await
            {
                Ok(Some(ClientResponsePayload::TurnStart(response))) => {
                    let turn_id = response.turn.id;
                    let agent_message_ref =
                        response
                            .turn
                            .items
                            .iter()
                            .rev()
                            .find_map(|item| match item {
                                ThreadItem::AgentMessage { id, .. } => Some(format!(
                                    "app-server://threads/{}/turns/{}/items/{}",
                                    params.coordinator_thread_id, turn_id, id
                                )),
                                _ => None,
                            });
                    let structured_output_ref = params.output_schema.as_ref().map(|_| {
                        format!(
                            "app-server://threads/{}/turns/{}/output-schema",
                            params.coordinator_thread_id, turn_id
                        )
                    });
                    (Some(turn_id), agent_message_ref, structured_output_ref)
                }
                Ok(_) => {
                    blockers.push(
                        "turn/start returned no turn response for thread consolidation".to_string(),
                    );
                    (None, None, None)
                }
                Err(error) => {
                    blockers.push(format!(
                        "turn/start failed for thread consolidation: {}",
                        error.message
                    ));
                    (None, None, None)
                }
            };

            ThreadConsolidationAttempt {
                consolidation_turn_id,
                source_refs,
                agent_message_ref,
                structured_output_ref,
                technical_evidence_refs: compact_event_refs(technical_evidence_refs),
                source_method: "thread/turns/list".to_string(),
                used_thread_turns_summary: true,
                blockers,
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct MemythosRequestProcessor {
    state: Arc<Mutex<MemythosRuntimeState>>,
    peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
    parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
    thread_consolidation_adapter: Arc<dyn ThreadConsolidationAdapter>,
    parent_turn_response_adapter: Arc<dyn ParentTurnResponseAdapter>,
    parent_configuration_adapter: Arc<dyn ParentConfigurationAdapter>,
    next_layer_id: Arc<AtomicU64>,
    next_arena_id: Arc<AtomicU64>,
    next_attachment_id: Arc<AtomicU64>,
    next_delivery_id: Arc<AtomicU64>,
    next_room_activity_id: Arc<AtomicU64>,
    next_contract_id: Arc<AtomicU64>,
    next_telemetry_ref_id: Arc<AtomicU64>,
}

impl MemythosRequestProcessor {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::new_for_transport(AppServerRpcTransport::Stdio)
    }

    #[cfg(test)]
    pub(crate) fn new_for_transport(rpc_transport: AppServerRpcTransport) -> Self {
        Self::new_for_transport_with_peer_delivery(
            rpc_transport,
            Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_transport_with_peer_delivery(
        rpc_transport: AppServerRpcTransport,
        peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
    ) -> Self {
        Self::new_for_transport_with_adapters(
            rpc_transport,
            peer_parent_delivery_adapter,
            Arc::new(RecordOnlyParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_transport_with_adapters(
        rpc_transport: AppServerRpcTransport,
        peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
        parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
        thread_consolidation_adapter: Arc<dyn ThreadConsolidationAdapter>,
        parent_turn_response_adapter: Arc<dyn ParentTurnResponseAdapter>,
    ) -> Self {
        Self::new_for_transport_with_native_adapters(
            rpc_transport,
            peer_parent_delivery_adapter,
            parent_goal_snapshot_adapter,
            thread_consolidation_adapter,
            parent_turn_response_adapter,
            Arc::new(RecordOnlyParentConfigurationAdapter),
        )
    }

    pub(crate) fn new_for_transport_with_native_adapters(
        rpc_transport: AppServerRpcTransport,
        peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
        parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
        thread_consolidation_adapter: Arc<dyn ThreadConsolidationAdapter>,
        parent_turn_response_adapter: Arc<dyn ParentTurnResponseAdapter>,
        parent_configuration_adapter: Arc<dyn ParentConfigurationAdapter>,
    ) -> Self {
        let (connection_mode, transport_owner, transport_id, daemon_runtime_verified) =
            match rpc_transport {
                AppServerRpcTransport::Stdio => ("stdio", "app_server", Some("stdio"), false),
                AppServerRpcTransport::Websocket => (
                    "daemon_websocket",
                    "app_server_daemon",
                    Some("websocket"),
                    true,
                ),
                AppServerRpcTransport::InProcess => (
                    "in_process",
                    "app_server_embedded",
                    Some("in_process"),
                    false,
                ),
            };

        Self {
            state: Arc::new(Mutex::new(MemythosRuntimeState {
                runtime_id: "memythos_app_server_runtime".to_string(),
                lifecycle_state: MemythosRuntimeLifecycleState::Ready,
                runtime_family: "app_server".to_string(),
                connection_mode: connection_mode.to_string(),
                transport_owner: transport_owner.to_string(),
                transport_id: transport_id.map(str::to_string),
                daemon_runtime_verified,
                degraded_reasons: Vec::new(),
                layers: HashMap::new(),
                arenas: HashMap::new(),
                rooms: HashMap::new(),
                thread_attachments: HashMap::new(),
                arena_parents: HashMap::new(),
                arena_message_deliveries: Vec::new(),
                room_activity_events: HashMap::new(),
                structured_contracts: HashMap::new(),
                native_token_usage_refs: HashMap::new(),
                telemetry_refs: Vec::new(),
            })),
            peer_parent_delivery_adapter,
            parent_goal_snapshot_adapter,
            thread_consolidation_adapter,
            parent_turn_response_adapter,
            parent_configuration_adapter,
            next_layer_id: Arc::new(AtomicU64::default()),
            next_arena_id: Arc::new(AtomicU64::default()),
            next_attachment_id: Arc::new(AtomicU64::default()),
            next_delivery_id: Arc::new(AtomicU64::default()),
            next_room_activity_id: Arc::new(AtomicU64::default()),
            next_contract_id: Arc::new(AtomicU64::default()),
            next_telemetry_ref_id: Arc::new(AtomicU64::default()),
        }
    }

    pub(crate) async fn runtime_health(
        &self,
        _params: MemythosRuntimeHealthParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;

        Ok(MemythosRuntimeHealthResponse {
            runtime_id: state.runtime_id.clone(),
            protocol_version: "memythos.experimental.v1".to_string(),
            lifecycle_state: state.lifecycle_state,
            runtime_family: state.runtime_family.clone(),
            connection_mode: state.connection_mode.clone(),
            transport_owner: state.transport_owner.clone(),
            transport_id: state.transport_id.clone(),
            daemon_runtime_verified: state.daemon_runtime_verified,
            capabilities: vec![
                "memythos/runtime/health".to_string(),
                "memythos/runtime/close".to_string(),
                "memythos/layer/create".to_string(),
                "memythos/layer/list".to_string(),
                "memythos/arena/create".to_string(),
                "memythos/arena/list".to_string(),
                "memythos/thread/attach".to_string(),
                "memythos/thread/list".to_string(),
                "memythos/arena/parent/register".to_string(),
                "memythos/arena/participant/register".to_string(),
                "memythos/arena/phase/start".to_string(),
                "memythos/arena/message".to_string(),
                "memythos/arena/message/send".to_string(),
                "memythos/arena/message/list".to_string(),
                "memythos/arena/message/observe".to_string(),
                "memythos/room/register".to_string(),
                "memythos/room/create".to_string(),
                "memythos/room/list".to_string(),
                "memythos/room/activity/list".to_string(),
                "memythos/room/timeline/get".to_string(),
                "memythos/room/sendInput".to_string(),
                "memythos/room/send".to_string(),
                "memythos/thread/consolidate".to_string(),
                "memythos/thread/contract/assemble".to_string(),
                "memythos/room/contract/emit".to_string(),
                "memythos/thread/contract/read".to_string(),
                "memythos/room/contract/get".to_string(),
                "memythos/thread/contract/list".to_string(),
                "memythos/arena/state/get".to_string(),
                "memythos/arena/phase/close".to_string(),
                "memythos/arena/run".to_string(),
                "memythos/telemetry/list".to_string(),
            ],
            active_layers: state.layers.len(),
            active_arenas: state.arenas.len(),
            active_thread_attachments: state.thread_attachments.len(),
            telemetry_ref_count: state.telemetry_refs.len(),
            degraded_reasons: state.degraded_reasons.clone(),
        }
        .into())
    }

    pub(crate) async fn runtime_close(
        &self,
        params: MemythosRuntimeCloseParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if params.force {
            state.lifecycle_state = MemythosRuntimeLifecycleState::ClosedDegraded;
            state.degraded_reasons.push(
                params
                    .reason
                    .unwrap_or_else(|| "runtime was force closed by request".to_string()),
            );
        } else {
            state.lifecycle_state = MemythosRuntimeLifecycleState::ClosedCleanly;
        }
        let runtime_id = state.runtime_id.clone();
        let lifecycle_state = state.lifecycle_state;
        let degraded_reasons = state.degraded_reasons.clone();
        let closed_cleanly = lifecycle_state == MemythosRuntimeLifecycleState::ClosedCleanly;
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::RuntimeState,
            MemythosTelemetrySource::MemythosRuntimeState,
            None,
            None,
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!("Runtime closed with state {lifecycle_state:?}."),
        );

        Ok(MemythosRuntimeCloseResponse {
            runtime_id,
            lifecycle_state,
            closed_cleanly,
            degraded_reasons,
        }
        .into())
    }

    pub(crate) async fn layer_create(
        &self,
        params: MemythosLayerCreateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if let Some(parent_layer_id) = params.parent_layer_id.as_deref() {
            if !state.layers.contains_key(parent_layer_id) {
                return Err(invalid_params(format!(
                    "unknown parent layer id: {parent_layer_id}"
                )));
            }
        }

        let layer_id = self.next_id("mem_layer", &self.next_layer_id);
        let layer = MemythosLayer {
            layer_id: layer_id.clone(),
            name: params.name,
            kind: params.kind,
            parent_layer_id: params.parent_layer_id,
            objective: params.objective,
        };
        state.layers.insert(layer_id, layer.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::LayerState,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer.layer_id.clone()),
            None,
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!("Layer {} created.", layer.layer_id),
        );

        Ok(MemythosLayerCreateResponse { layer }.into())
    }

    pub(crate) async fn layer_list(
        &self,
        _params: MemythosLayerListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let mut layers: Vec<_> = state.layers.values().cloned().collect();
        layers.sort_by(|a, b| a.layer_id.cmp(&b.layer_id));

        Ok(MemythosLayerListResponse { layers }.into())
    }

    pub(crate) async fn arena_create(
        &self,
        params: MemythosArenaCreateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if !state.layers.contains_key(&params.layer_id) {
            return Err(invalid_params(format!(
                "unknown layer id: {}",
                params.layer_id
            )));
        }

        let arena_id = self.next_id("mem_arena", &self.next_arena_id);
        let arena = MemythosArena {
            arena_id: arena_id.clone(),
            layer_id: params.layer_id,
            name: params.name,
            kind: params.kind,
            lifecycle_state: MemythosArenaLifecycleState::Draft,
            objective: params.objective,
            participant_ids: params.participant_ids,
        };
        state.arenas.insert(arena_id, arena.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaState,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(arena.layer_id.clone()),
            Some(arena.arena_id.clone()),
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!("Arena {} created.", arena.arena_id),
        );

        Ok(MemythosArenaCreateResponse { arena }.into())
    }

    pub(crate) async fn arena_list(
        &self,
        params: MemythosArenaListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let mut arenas: Vec<_> = state
            .arenas
            .values()
            .filter(|arena| {
                params
                    .layer_id
                    .as_ref()
                    .map_or(true, |layer_id| &arena.layer_id == layer_id)
            })
            .cloned()
            .collect();
        arenas.sort_by(|a, b| a.arena_id.cmp(&b.arena_id));

        Ok(MemythosArenaListResponse { arenas }.into())
    }

    pub(crate) async fn thread_attach(
        &self,
        params: MemythosThreadAttachParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let Some(arena) = state.arenas.get_mut(&params.arena_id) else {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        };

        if !arena.participant_ids.contains(&params.thread_id) {
            arena.participant_ids.push(params.thread_id.clone());
        }
        if arena.lifecycle_state == MemythosArenaLifecycleState::Draft {
            arena.lifecycle_state = MemythosArenaLifecycleState::Running;
        }
        let layer_id = arena.layer_id.clone();

        let attachment_id = self.next_id("mem_attach", &self.next_attachment_id);
        let attachment = MemythosThreadAttachment {
            attachment_id: attachment_id.clone(),
            arena_id: params.arena_id,
            thread_id: params.thread_id,
            role_id: params.role_id,
            stance_id: params.stance_id,
            objective: params.objective,
            contract_ref: params.contract_ref,
            lifecycle_state: MemythosArenaLifecycleState::Running,
        };
        state
            .thread_attachments
            .insert(attachment_id, attachment.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ThreadAttachment,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer_id),
            Some(attachment.arena_id.clone()),
            Some(attachment.thread_id.clone()),
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!(
                "Thread {} attached to arena {}.",
                attachment.thread_id, attachment.arena_id
            ),
        );

        Ok(MemythosThreadAttachResponse { attachment }.into())
    }

    pub(crate) async fn thread_list(
        &self,
        params: MemythosThreadListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }
        let mut attachments: Vec<_> = state
            .thread_attachments
            .values()
            .filter(|attachment| attachment.arena_id == params.arena_id)
            .cloned()
            .collect();
        attachments.sort_by(|a, b| a.attachment_id.cmp(&b.attachment_id));

        Ok(MemythosThreadListResponse { attachments }.into())
    }

    pub(crate) async fn arena_parent_register(
        &self,
        params: MemythosArenaParentRegisterParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let Some(arena) = state.arenas.get(&params.arena_id) else {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        };
        let layer_id = arena.layer_id.clone();
        let has_attachment = state.thread_attachments.values().any(|attachment| {
            attachment.arena_id == params.arena_id && attachment.thread_id == params.thread_id
        });
        if !has_attachment {
            return Err(invalid_params(format!(
                "thread {} is not attached to arena {}",
                params.thread_id, params.arena_id
            )));
        }

        let key = arena_parent_key(&params.arena_id, &params.thread_id);
        let parent = MemythosArenaParent {
            arena_id: params.arena_id.clone(),
            thread_id: params.thread_id,
            parent_role: params.parent_role,
            stance_profile: params.stance_profile,
            authority_scope: params.authority_scope,
            lifecycle_state: MemythosArenaLifecycleState::Running,
        };
        state.arena_parents.insert(key, parent.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaParent,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer_id),
            Some(parent.arena_id.clone()),
            Some(parent.thread_id.clone()),
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!(
                "Arena parent {} registered as {} in arena {}.",
                parent.thread_id, parent.parent_role, parent.arena_id
            ),
        );

        Ok(MemythosArenaParentRegisterResponse { parent }.into())
    }

    pub(crate) async fn arena_participant_register(
        &self,
        params: MemythosArenaParticipantRegisterParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let payload = self.arena_parent_register(params).await?;
        if let ClientResponsePayload::MemythosArenaParentRegister(response) = payload {
            return Ok(MemythosArenaParticipantRegisterResponse {
                parent: response.parent,
            }
            .into());
        }
        Err(invalid_params(
            "arena parent register returned unexpected payload".to_string(),
        ))
    }

    pub(crate) async fn arena_phase_start(
        &self,
        params: MemythosArenaPhaseStartParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let update = self
            .update_arena_phase(
                params.arena_id,
                params.round_id,
                params.phase,
                MemythosArenaLifecycleState::Running,
                "started",
            )
            .await?;
        Ok(MemythosArenaPhaseStartResponse {
            arena_id: update.arena_id,
            round_id: update.round_id,
            phase: update.phase,
            lifecycle_state: update.lifecycle_state,
            phase_state_source: "app_server_protocol".to_string(),
            event_refs: update.event_refs,
        }
        .into())
    }

    pub(crate) async fn arena_message_send(
        &self,
        params: MemythosArenaMessageSendParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let Some(arena) = state.arenas.get(&params.message.arena_id) else {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.message.arena_id
            )));
        };
        let layer_id = arena.layer_id.clone();
        let sender_key = arena_parent_key(
            &params.message.arena_id,
            &params.message.from_parent_thread_id,
        );
        let receiver_key = arena_parent_key(
            &params.message.arena_id,
            &params.message.to_parent_thread_id,
        );
        if !state.arena_parents.contains_key(&sender_key) {
            return Err(invalid_params(format!(
                "sender parent {} is not registered in arena {}",
                params.message.from_parent_thread_id, params.message.arena_id
            )));
        }
        if !state.arena_parents.contains_key(&receiver_key) {
            return Err(invalid_params(format!(
                "receiver parent {} is not registered in arena {}",
                params.message.to_parent_thread_id, params.message.arena_id
            )));
        }

        let delivery_id = self.next_id("mem_delivery", &self.next_delivery_id);
        let delivery_attempt = self
            .peer_parent_delivery_adapter
            .deliver_peer_parent_message(&params.message)
            .await;
        let telemetry_channel = delivery_attempt.telemetry_channel;
        let telemetry_summary = delivery_attempt.telemetry_summary.clone();
        let delivery = MemythosArenaMessageDelivery {
            delivery_id,
            message_id: params.message.message_id.clone(),
            human_summary: params.message.human_summary.clone(),
            status: delivery_attempt.status,
            sender_thread_id: params.message.from_parent_thread_id,
            receiver_thread_id: params.message.to_parent_thread_id,
            arena_id: params.message.arena_id,
            round_id: params.message.round_id,
            phase: phase_from_message_kind(&params.message.message_kind),
            delivery_mechanism: delivery_attempt.delivery_mechanism,
            receiver_turn_id: delivery_attempt.receiver_turn_id,
            receiver_response_event_ref: delivery_attempt.receiver_response_event_ref,
            delivered_as_human_instruction: delivery_attempt.delivered_as_human_instruction,
            memory_replay_required: delivery_attempt.memory_replay_required,
            event_refs: delivery_attempt.event_refs,
            rejection_reason: delivery_attempt.rejection_reason,
        };
        state.arena_message_deliveries.push(delivery.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaMessage,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer_id),
            Some(delivery.arena_id.clone()),
            Some(delivery.receiver_thread_id.clone()),
            delivery.event_refs.first().cloned(),
            None,
            telemetry_channel,
            telemetry_summary,
        );

        Ok(MemythosArenaMessageSendResponse { delivery }.into())
    }

    pub(crate) async fn arena_message_send_v2(
        &self,
        params: MemythosArenaMessageSendV2Params,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let payload = self.arena_message_send(params).await?;
        if let ClientResponsePayload::MemythosArenaMessageSend(response) = payload {
            return Ok(MemythosArenaMessageSendV2Response {
                delivery: response.delivery,
            }
            .into());
        }
        Err(invalid_params(
            "arena message send returned unexpected payload".to_string(),
        ))
    }

    pub(crate) async fn parent_continuity_list(
        &self,
        params: MemythosParentContinuityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }

        let parents = state
            .arena_parents
            .values()
            .filter(|parent| parent.arena_id == params.arena_id)
            .filter(|parent| {
                params
                    .thread_id
                    .as_ref()
                    .map_or(true, |thread_id| &parent.thread_id == thread_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let deliveries = state.arena_message_deliveries.clone();
        let native_token_usage_refs = state.native_token_usage_refs.clone();
        drop(state);

        let mut continuities = Vec::with_capacity(parents.len());
        for parent in parents {
            let goal_snapshot = self
                .parent_goal_snapshot_adapter
                .current_goal_snapshot(&parent.thread_id)
                .await;
            continuities.push(build_parent_thread_continuity(
                &parent,
                &deliveries,
                &native_token_usage_refs,
                goal_snapshot,
            ));
        }

        Ok(MemythosParentContinuityListResponse { continuities }.into())
    }

    pub(crate) async fn arena_message_list(
        &self,
        params: MemythosArenaMessageListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }
        let deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.arena_id == params.arena_id)
            .filter(|delivery| {
                params
                    .round_id
                    .as_ref()
                    .map_or(true, |round_id| &delivery.round_id == round_id)
            })
            .cloned()
            .collect();

        Ok(MemythosArenaMessageListResponse { deliveries }.into())
    }

    pub(crate) async fn arena_message_observation_list(
        &self,
        params: MemythosArenaMessageObservationListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }

        let observations = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.arena_id == params.arena_id)
            .filter(|delivery| {
                params
                    .round_id
                    .as_ref()
                    .map_or(true, |round_id| &delivery.round_id == round_id)
            })
            .filter(|delivery| {
                params
                    .message_id
                    .as_ref()
                    .map_or(true, |message_id| &delivery.message_id == message_id)
            })
            .map(build_parent_peer_response_observation)
            .collect();

        Ok(MemythosArenaMessageObservationListResponse { observations }.into())
    }

    pub(crate) async fn arena_message_observe(
        &self,
        params: MemythosArenaMessageObserveParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let payload = self.arena_message_observation_list(params).await?;
        if let ClientResponsePayload::MemythosArenaMessageObservationList(response) = payload {
            return Ok(MemythosArenaMessageObserveResponse {
                observations: response.observations,
            }
            .into());
        }
        Err(invalid_params(
            "arena message observe returned unexpected payload".to_string(),
        ))
    }

    pub(crate) async fn arena_state_get(
        &self,
        params: MemythosArenaStateGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let arena = state
            .arenas
            .get(&params.arena_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown arena id: {}", params.arena_id)))?;
        let mut parents = state
            .arena_parents
            .values()
            .filter(|parent| parent.arena_id == params.arena_id)
            .cloned()
            .collect::<Vec<_>>();
        parents.sort_by(|left, right| left.thread_id.cmp(&right.thread_id));
        let mut deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.arena_id == params.arena_id)
            .cloned()
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| left.delivery_id.cmp(&right.delivery_id));
        Ok(MemythosArenaStateGetResponse {
            arena,
            parents,
            deliveries,
            phase_state_source: "app_server_protocol".to_string(),
            local_ts_arena_state_used: false,
        }
        .into())
    }

    async fn update_arena_phase(
        &self,
        arena_id: String,
        round_id: String,
        phase: String,
        lifecycle_state: MemythosArenaLifecycleState,
        action: &str,
    ) -> Result<MemythosArenaPhaseUpdate, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let Some(arena) = state.arenas.get_mut(&arena_id) else {
            return Err(invalid_params(format!("unknown arena id: {}", arena_id)));
        };
        arena.lifecycle_state = lifecycle_state;
        let layer_id = arena.layer_id.clone();
        let arena_id = arena.arena_id.clone();
        let event_ref = format!(
            "app-server://memythos/arenas/{arena_id}/rounds/{round_id}/phases/{phase}/{action}"
        );
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaState,
            MemythosTelemetrySource::AppServerNative,
            Some(layer_id),
            Some(arena_id.clone()),
            None,
            Some(event_ref.clone()),
            None,
            MemythosEventChannel::StateTransition,
            format!("Arena {arena_id} phase {phase} {action}."),
        );
        Ok(MemythosArenaPhaseUpdate {
            arena_id,
            round_id,
            phase,
            lifecycle_state,
            event_refs: vec![event_ref],
        })
    }

    pub(crate) async fn arena_phase_close(
        &self,
        params: MemythosArenaPhaseCloseParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let update = self
            .update_arena_phase(
                params.arena_id,
                params.round_id,
                params.phase,
                MemythosArenaLifecycleState::ArtifactComplete,
                "closed",
            )
            .await?;
        Ok(MemythosArenaPhaseCloseResponse {
            arena_id: update.arena_id,
            round_id: update.round_id,
            phase: update.phase,
            lifecycle_state: update.lifecycle_state,
            phase_state_source: "app_server_protocol".to_string(),
            event_refs: update.event_refs,
        }
        .into())
    }

    pub(crate) async fn arena_run(
        &self,
        params: MemythosArenaRunParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let arena = state
            .arenas
            .get(&params.arena_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown arena id: {}", params.arena_id)))?;
        Ok(MemythosArenaRunResponse {
            arena_id: params.arena_id,
            round_id: params.round_id,
            lifecycle_state: arena.lifecycle_state,
            phase_state_source: "app_server_protocol".to_string(),
            local_ts_arena_state_used: false,
            event_refs: vec![format!(
                "app-server://memythos/arenas/{}/run/{}",
                arena.arena_id, arena.lifecycle_state as u8
            )],
        }
        .into())
    }

    pub(crate) async fn room_register(
        &self,
        params: MemythosRoomRegisterParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        validate_room_registration(&params)?;
        let mut state = self.state.lock().await;
        let event_ref = format!("app-server://rooms/{}/registered", params.room_id);
        let room = MemythosRoom {
            room_id: params.room_id,
            case_id: params.case_id,
            layer_id: params.layer_id,
            arena_id: params.arena_id,
            topology: params.topology,
            participants: params.participants,
        };
        let room_id = room.room_id.clone();
        let layer_id = room.layer_id.clone();
        let arena_id = room.arena_id.clone();
        state.rooms.insert(room_id.clone(), room.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaState,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer_id),
            Some(arena_id),
            None,
            Some(event_ref.clone()),
            Some(format!("app-server://rooms/{room_id}")),
            MemythosEventChannel::StateTransition,
            format!(
                "Room {} registered with {} independent parent participants.",
                room_id,
                room.participants.len()
            ),
        );
        self.push_room_activity_event(
            &mut state,
            room_id.clone(),
            room.arena_id.clone(),
            "room_concierge".to_string(),
            None,
            None,
            None,
            "room_concierge".to_string(),
            app_server_actor_ref(),
            runtime_room_concierge_actor_ref(),
            "room_lifecycle".to_string(),
            MemythosPromptOrigin::MemythosRuntimeSetup,
            vec![MemythosPromptLineagePart {
                origin: MemythosPromptOrigin::AppServerProtocol,
                summary: "app-server registered native Memythos room".to_string(),
                source_ref: Some(event_ref.clone()),
            }],
            "lifecycle",
            "room_registered",
            "completed",
            format!(
                "Room {} registered with {} independent parent participants.",
                room_id,
                room.participants.len()
            ),
            Some(event_ref.clone()),
        );
        for participant in &room.participants {
            self.push_room_activity_event(
                &mut state,
                room_id.clone(),
                room.arena_id.clone(),
                participant.thread_id.clone(),
                None,
                None,
                None,
                participant.parent_role.clone(),
                app_server_actor_ref(),
                room_actor_ref_for_participant(participant),
                participant
                    .authority_scope
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "room_participation".to_string()),
                MemythosPromptOrigin::MemythosRuntimeSetup,
                vec![MemythosPromptLineagePart {
                    origin: MemythosPromptOrigin::AppServerProtocol,
                    summary: format!(
                        "app-server registered parent {} as {} in room {}",
                        participant.thread_id, participant.parent_role, room.room_id
                    ),
                    source_ref: Some(format!(
                        "app-server://rooms/{}/participants/{}",
                        room_id, participant.thread_id
                    )),
                }],
                "lifecycle",
                "participant_attached",
                "completed",
                format!(
                    "Participant {} attached to room {} as {}.",
                    participant.parent_key, room_id, participant.parent_role
                ),
                Some(format!(
                    "app-server://rooms/{}/participants/{}",
                    room_id, participant.thread_id
                )),
            );
        }

        Ok(MemythosRoomRegisterResponse {
            room,
            event_refs: vec![event_ref],
        }
        .into())
    }

    pub(crate) async fn room_list(
        &self,
        params: MemythosRoomListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let mut rooms = state.rooms.values().cloned().collect::<Vec<_>>();
        rooms.retain(|room| {
            params
                .case_id
                .as_ref()
                .map_or(true, |case_id| &room.case_id == case_id)
                && params
                    .layer_id
                    .as_ref()
                    .map_or(true, |layer_id| &room.layer_id == layer_id)
                && params
                    .arena_id
                    .as_ref()
                    .map_or(true, |arena_id| &room.arena_id == arena_id)
        });
        rooms.sort_by(|left, right| left.room_id.cmp(&right.room_id));

        Ok(MemythosRoomListResponse {
            rooms,
            source_method: "memythos/room/list".to_string(),
        }
        .into())
    }

    pub(crate) async fn room_tool_list_participants(
        &self,
        current_thread_id: &str,
    ) -> Result<Vec<MemythosRoomToolParticipant>, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let room = state
            .rooms
            .values()
            .filter(|room| {
                room.participants
                    .iter()
                    .any(|participant| participant.thread_id == current_thread_id)
            })
            .min_by(|left, right| left.room_id.cmp(&right.room_id))
            .ok_or_else(|| {
                invalid_params(format!(
                    "thread {current_thread_id} is not registered in a Memythos room"
                ))
            })?;
        let mut participants = room
            .participants
            .iter()
            .map(|participant| MemythosRoomToolParticipant {
                parent_key: participant.parent_key.clone(),
                parent_role: participant.parent_role.clone(),
                stance_profile: participant.stance_profile.clone(),
                is_current_parent: participant.thread_id == current_thread_id,
            })
            .collect::<Vec<_>>();
        participants.sort_by(|left, right| left.parent_key.cmp(&right.parent_key));
        Ok(participants)
    }

    pub(crate) async fn room_tool_list_rooms(
        &self,
        current_thread_id: &str,
    ) -> Result<Vec<MemythosRoomToolRoom>, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let current_room = state
            .rooms
            .values()
            .find(|room| {
                room.participants.iter().any(|participant| {
                    participant.thread_id == current_thread_id
                        && participant.parent_role == "room_concierge"
                })
            })
            .ok_or_else(|| {
                invalid_params(format!(
                    "thread {current_thread_id} is not a Room Concierge in a Memythos room"
                ))
            })?;
        let mut rooms = state
            .rooms
            .values()
            .filter(|room| room.case_id == current_room.case_id)
            .filter_map(|room| {
                room.participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")
                    .map(|concierge| MemythosRoomToolRoom {
                        room_id: room.room_id.clone(),
                        arena_id: room.arena_id.clone(),
                        layer_id: room.layer_id.clone(),
                        concierge_parent_key: concierge.parent_key.clone(),
                        is_current_room: room.room_id == current_room.room_id,
                    })
            })
            .collect::<Vec<_>>();
        rooms.sort_by(|left, right| left.room_id.cmp(&right.room_id));
        Ok(rooms)
    }

    pub(crate) async fn room_tool_send_to_room(
        &self,
        current_thread_id: &str,
        args: MemythosRoomToolSendToRoomArgs,
    ) -> Result<MemythosRoomToolResponse, JSONRPCErrorError> {
        if args.message.trim().is_empty() {
            return Err(invalid_params(
                "cross-room message must not be empty".to_string(),
            ));
        }
        if !matches!(
            args.authority.as_str(),
            "peer" | "subordinate" | "judge" | "human_delegated"
        ) {
            return Err(invalid_params(format!(
                "unsupported cross-room message authority: {}",
                args.authority
            )));
        }

        let (source_room, source, target_room, target) = {
            let state = self.state.lock().await;
            let source_room = state
                .rooms
                .values()
                .find(|room| {
                    room.participants.iter().any(|participant| {
                        participant.thread_id == current_thread_id
                            && participant.parent_role == "room_concierge"
                    })
                })
                .cloned()
                .ok_or_else(|| {
                    invalid_params(format!(
                        "thread {current_thread_id} is not a Room Concierge in a Memythos room"
                    ))
                })?;
            let source = room_participant_by_thread(&source_room, current_thread_id)
                .cloned()
                .expect("source concierge was validated");
            let target_room = state
                .rooms
                .get(&args.target_room_id)
                .cloned()
                .ok_or_else(|| {
                    invalid_params(format!("unknown target room: {}", args.target_room_id))
                })?;
            if source_room.room_id == target_room.room_id {
                return Err(invalid_params(
                    "send_to_room requires a different target room; use send_message inside a room"
                        .to_string(),
                ));
            }
            if source_room.case_id != target_room.case_id {
                return Err(invalid_params(
                    "cross-room delivery is restricted to rooms in the same case".to_string(),
                ));
            }
            let target = target_room
                .participants
                .iter()
                .find(|participant| participant.parent_role == "room_concierge")
                .cloned()
                .ok_or_else(|| {
                    invalid_params(format!(
                        "target room {} has no Room Concierge",
                        target_room.room_id
                    ))
                })?;
            (source_room, source, target_room, target)
        };

        let message_id = self.next_id("mem_cross_room_message", &self.next_delivery_id);
        let room_message_ref = format!(
            "app-server://rooms/{}/cross-room-messages/{message_id}",
            source_room.room_id
        );
        let delivery_ref = format!("{room_message_ref}/delivery/{}", target_room.room_id);
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "memythos_round_id".to_string(),
            serde_json::Value::String("cross_room_loopback".to_string()),
        );
        metadata.insert(
            "memythos_phase".to_string(),
            serde_json::Value::String(args.message_kind.clone()),
        );
        metadata.insert(
            "memythos_source_room_id".to_string(),
            serde_json::Value::String(source_room.room_id.clone()),
        );
        metadata.insert(
            "memythos_source".to_string(),
            serde_json::Value::String("native_cross_room_tool".to_string()),
        );
        let payload = self
            .room_send_input(MemythosRoomSendInputParams {
                room_id: target_room.room_id.clone(),
                room_message_ref,
                delivery_ref,
                from_parent_thread_id: Some(current_thread_id.to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: target.thread_id.clone(),
                source_parent_key: source.parent_key,
                target_parent_key: target.parent_key.clone(),
                message_kind: args.message_kind,
                message_authority: args.authority,
                human_instruction: false,
                response_contract: args.response_contract,
                client_user_message_id: Some(message_id),
                human_summary: args.message.clone(),
                prompt: args.message,
                metadata,
                output_schema: None,
            })
            .await?;
        let ClientResponsePayload::MemythosRoomSendInput(delivery_response) = payload else {
            return Err(invalid_params(
                "native cross-room tool received an unexpected delivery response".to_string(),
            ));
        };
        let target_turn_id = delivery_response.delivery.turn_id.clone();
        let (response_item_ref, response_text, event_refs) = self
            .await_parent_turn_response(&target.thread_id, &target_turn_id)
            .await?;
        Ok(MemythosRoomToolResponse {
            room_id: target_room.room_id,
            target_parent_key: target.parent_key,
            target_thread_id: target.thread_id,
            target_turn_id,
            response_item_ref,
            response_text,
            event_refs,
        })
    }

    pub(crate) async fn room_tool_send_message(
        &self,
        current_thread_id: &str,
        args: MemythosRoomToolSendMessageArgs,
    ) -> Result<MemythosRoomToolResponse, JSONRPCErrorError> {
        if args.message.trim().is_empty() {
            return Err(invalid_params("room message must not be empty".to_string()));
        }
        if !matches!(
            args.authority.as_str(),
            "peer" | "subordinate" | "judge" | "human_delegated"
        ) {
            return Err(invalid_params(format!(
                "unsupported room message authority: {}",
                args.authority
            )));
        }

        let (room, source, target, inherited_round_id, inherited_phase) = {
            let state = self.state.lock().await;
            let room = state
                .rooms
                .values()
                .filter(|room| {
                    room.participants
                        .iter()
                        .any(|participant| participant.thread_id == current_thread_id)
                })
                .min_by(|left, right| left.room_id.cmp(&right.room_id))
                .cloned()
                .ok_or_else(|| {
                    invalid_params(format!(
                        "thread {current_thread_id} is not registered in a Memythos room"
                    ))
                })?;
            let source = room_participant_by_thread(&room, current_thread_id)
                .cloned()
                .ok_or_else(|| {
                    invalid_params(format!(
                        "thread {current_thread_id} is not a participant in room {}",
                        room.room_id
                    ))
                })?;
            let target = if source.parent_role == "room_concierge" {
                let target_parent_key = args.target_parent_key.as_deref().ok_or_else(|| {
                    invalid_params(
                        "Room Concierge must select targetParentKey before sending".to_string(),
                    )
                })?;
                room.participants
                    .iter()
                    .find(|participant| participant.parent_key == target_parent_key)
                    .cloned()
                    .ok_or_else(|| {
                        invalid_params(format!(
                            "target parent {target_parent_key} is not registered in room {}",
                            room.room_id
                        ))
                    })?
            } else {
                room.participants
                    .iter()
                    .find(|participant| participant.parent_role == "room_concierge")
                    .cloned()
                    .ok_or_else(|| {
                        invalid_params(format!(
                            "room {} has no room_concierge parent",
                            room.room_id
                        ))
                    })?
            };
            let inherited_context = state
                .arena_message_deliveries
                .iter()
                .rev()
                .find(|delivery| {
                    delivery.arena_id == room.arena_id
                        && delivery.receiver_thread_id == current_thread_id
                })
                .map(|delivery| (delivery.round_id.clone(), delivery.phase.clone()));
            let (inherited_round_id, inherited_phase) =
                inherited_context.unwrap_or_else(|| ("agentic_room_turn".to_string(), None));
            (room, source, target, inherited_round_id, inherited_phase)
        };
        if target.thread_id == current_thread_id {
            return Err(invalid_params(
                "room message target must be a different parent thread".to_string(),
            ));
        }

        let message_id = self.next_id("mem_room_tool_message", &self.next_delivery_id);
        let room_message_ref = format!("app-server://rooms/{}/messages/{message_id}", room.room_id);
        let delivery_ref = format!("{room_message_ref}/delivery");
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "memythos_round_id".to_string(),
            serde_json::Value::String(inherited_round_id),
        );
        if let Some(phase) = inherited_phase {
            metadata.insert(
                "memythos_phase".to_string(),
                serde_json::Value::String(phase),
            );
        }
        metadata.insert(
            "memythos_source".to_string(),
            serde_json::Value::String("native_room_tool".to_string()),
        );
        let payload = self
            .room_send_input(MemythosRoomSendInputParams {
                room_id: room.room_id.clone(),
                room_message_ref,
                delivery_ref,
                from_parent_thread_id: Some(current_thread_id.to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: target.thread_id.clone(),
                source_parent_key: source.parent_key.clone(),
                target_parent_key: target.parent_key.clone(),
                message_kind: args.message_kind,
                message_authority: args.authority,
                human_instruction: false,
                response_contract: args.response_contract,
                client_user_message_id: Some(message_id),
                human_summary: args.message.clone(),
                prompt: args.message,
                metadata,
                output_schema: None,
            })
            .await?;
        let ClientResponsePayload::MemythosRoomSendInput(delivery_response) = payload else {
            return Err(invalid_params(
                "native room tool received an unexpected delivery response".to_string(),
            ));
        };

        let target_turn_id = delivery_response.delivery.turn_id.clone();
        let (response_item_ref, response_text, event_refs) = self
            .await_parent_turn_response(&target.thread_id, &target_turn_id)
            .await?;
        Ok(MemythosRoomToolResponse {
            room_id: room.room_id,
            target_parent_key: target.parent_key,
            target_thread_id: target.thread_id,
            target_turn_id,
            response_item_ref,
            response_text,
            event_refs,
        })
    }

    async fn await_parent_turn_response(
        &self,
        target_thread_id: &str,
        target_turn_id: &str,
    ) -> Result<(String, String, Vec<String>), JSONRPCErrorError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        let mut completed_without_message_since = None;
        let mut inferred_terminal_since = None;
        let mut event_refs = Vec::new();
        loop {
            let mut native_delivery_status = None;
            {
                let state = self.state.lock().await;
                if let Some(delivery) = state.arena_message_deliveries.iter().find(|delivery| {
                    delivery.receiver_thread_id == target_thread_id
                        && delivery.receiver_turn_id.as_deref() == Some(target_turn_id)
                }) {
                    event_refs = delivery.event_refs.clone();
                    native_delivery_status = Some(delivery.status.clone());
                }
            }

            match native_delivery_status.as_deref() {
                Some("receiver_turn_failed") => {
                    return Err(invalid_params(format!(
                        "parent turn {target_turn_id} failed"
                    )));
                }
                Some("receiver_turn_interrupted") => {
                    return Err(invalid_params(format!(
                        "parent turn {target_turn_id} was interrupted"
                    )));
                }
                _ => {}
            }

            let response = self
                .parent_turn_response_adapter
                .read_response(target_thread_id, target_turn_id)
                .await;
            match response.status {
                Some(TurnStatus::Completed) => {
                    inferred_terminal_since = None;
                    let completed_ref = format!(
                        "app-server://threads/{target_thread_id}/turns/{target_turn_id}/completed"
                    );
                    if !event_refs.contains(&completed_ref) {
                        event_refs.push(completed_ref.clone());
                    }
                    if let (Some(item_ref), Some(text)) = (response.item_ref, response.text) {
                        if !event_refs.contains(&item_ref) {
                            event_refs.push(item_ref.clone());
                        }
                        let mut state = self.state.lock().await;
                        if let Some(delivery) =
                            state.arena_message_deliveries.iter_mut().find(|delivery| {
                                delivery.receiver_thread_id == target_thread_id
                                    && delivery.receiver_turn_id.as_deref() == Some(target_turn_id)
                            })
                        {
                            delivery.status = "receiver_turn_completed".to_string();
                            delivery.receiver_response_event_ref = Some(item_ref.clone());
                            for event_ref in &event_refs {
                                if !delivery.event_refs.contains(event_ref) {
                                    delivery.event_refs.push(event_ref.clone());
                                }
                            }
                        }
                        return Ok((item_ref, text, compact_event_refs(event_refs)));
                    }
                    let completed_since = completed_without_message_since
                        .get_or_insert_with(tokio::time::Instant::now);
                    if completed_since.elapsed() >= Duration::from_secs(2) {
                        return Err(invalid_params(format!(
                            "parent turn {target_turn_id} completed without a readable AgentMessage"
                        )));
                    }
                }
                Some(TurnStatus::Failed) => {
                    let inferred_since =
                        inferred_terminal_since.get_or_insert_with(tokio::time::Instant::now);
                    if inferred_since.elapsed() >= Duration::from_secs(2) {
                        return Err(invalid_params(format!(
                            "parent turn {target_turn_id} failed"
                        )));
                    }
                }
                Some(TurnStatus::Interrupted) => {
                    // thread/turns/list rebuilds turns from persisted rollout plus the
                    // active in-memory snapshot. Immediately after turn/start, the
                    // persisted user input can be visible before TurnStarted marks the
                    // thread active, so an in-progress turn can briefly be reconstructed
                    // as interrupted. Native TurnAborted remains authoritative; only use
                    // the reconstructed status after it stays stable.
                    let inferred_since =
                        inferred_terminal_since.get_or_insert_with(tokio::time::Instant::now);
                    if inferred_since.elapsed() >= Duration::from_secs(2) {
                        return Err(invalid_params(format!(
                            "parent turn {target_turn_id} was interrupted"
                        )));
                    }
                }
                Some(TurnStatus::InProgress) | None => {
                    completed_without_message_since = None;
                    inferred_terminal_since = None;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(invalid_params(format!(
                    "timed out waiting for parent turn {target_turn_id}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub(crate) async fn room_send_input(
        &self,
        params: MemythosRoomSendInputParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let room = {
            let state = self.state.lock().await;
            state
                .rooms
                .get(&params.room_id)
                .cloned()
                .ok_or_else(|| invalid_params(format!("unknown room id: {}", params.room_id)))?
        };
        let target =
            room_participant_by_thread(&room, &params.to_parent_thread_id).ok_or_else(|| {
                invalid_params(format!(
                    "target parent thread {} is not registered in room {}",
                    params.to_parent_thread_id, params.room_id
                ))
            })?;
        let source_thread_id = params
            .via_concierge_thread_id
            .clone()
            .filter(|thread_id| !thread_id.is_empty())
            .unwrap_or_else(|| {
                params
                    .from_parent_thread_id
                    .clone()
                    .unwrap_or_else(|| "room_concierge".to_string())
            });
        let source = {
            let state = self.state.lock().await;
            room_participant_by_thread(&room, &source_thread_id)
                .cloned()
                .or_else(|| {
                    state.rooms.values().find_map(|candidate_room| {
                        if candidate_room.case_id != room.case_id {
                            return None;
                        }
                        room_participant_by_thread(candidate_room, &source_thread_id)
                            .filter(|participant| participant.parent_role == "room_concierge")
                            .cloned()
                    })
                })
        };
        if !params.human_instruction
            && source.is_none()
            && params
                .via_concierge_thread_id
                .as_deref()
                .map_or(true, |thread_id| {
                    room_participant_by_thread(&room, thread_id).is_none()
                })
        {
            return Err(invalid_params(format!(
                "source or concierge thread must be registered in room {}",
                params.room_id
            )));
        }
        if target.parent_key != params.target_parent_key {
            return Err(invalid_params(format!(
                "targetParentKey {} does not match registered parent {}",
                params.target_parent_key, target.parent_key
            )));
        }
        if let Some(source) = source.as_ref() {
            if source.parent_key != params.source_parent_key {
                return Err(invalid_params(format!(
                    "sourceParentKey {} does not match registered parent {}",
                    params.source_parent_key, source.parent_key
                )));
            }
        }

        let message = MemythosArenaMessage {
            message_id: params
                .client_user_message_id
                .clone()
                .unwrap_or_else(|| params.delivery_ref.clone()),
            case_id: room.case_id.clone(),
            arena_id: room.arena_id.clone(),
            round_id: params
                .metadata
                .get("memythos_round_id")
                .and_then(|value| value.as_str())
                .unwrap_or("room_loopback")
                .to_string(),
            from_parent_thread_id: if params.human_instruction {
                "human".to_string()
            } else {
                source_thread_id.clone()
            },
            from_parent_role: if params.human_instruction {
                "human".to_string()
            } else {
                source
                    .as_ref()
                    .map(|participant| participant.parent_role.clone())
                    .unwrap_or_else(|| "room_concierge".to_string())
            },
            to_parent_thread_id: params.to_parent_thread_id.clone(),
            to_parent_role: target.parent_role.clone(),
            message_kind: params.message_kind.clone(),
            human_summary: params.human_summary.clone(),
            context_packet_ref: params.room_message_ref.clone(),
            artifact_refs: vec![params.delivery_ref.clone()],
            requires_response: true,
            response_contract: Some(params.response_contract.clone()),
        };
        let delivery_id = self.next_id("mem_room_delivery", &self.next_delivery_id);
        let delivery_attempt = self
            .peer_parent_delivery_adapter
            .deliver_peer_parent_message(&message)
            .await;
        let target_turn_id = delivery_attempt.receiver_turn_id.clone().ok_or_else(|| {
            invalid_params(format!(
                "room sendInput failed to create target turn: {}",
                delivery_attempt
                    .rejection_reason
                    .clone()
                    .unwrap_or_else(|| "unknown delivery failure".to_string())
            ))
        })?;
        let room_event_ref = format!(
            "app-server://rooms/{}/messages/{}/delivered",
            params.room_id, message.message_id
        );
        let target_turn_ref = format!(
            "app-server://threads/{}/turns/{}",
            params.to_parent_thread_id, target_turn_id
        );
        let event_refs = compact_event_refs(
            vec![
                room_event_ref.clone(),
                target_turn_ref,
                format!(
                    "app-server://rooms/{}/messages/{}/targetTurnStarted/{}",
                    params.room_id, message.message_id, target_turn_id
                ),
            ]
            .into_iter()
            .chain(delivery_attempt.event_refs.clone())
            .collect(),
        );
        let delivery_phase = params
            .metadata
            .get("memythos_phase")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .or_else(|| phase_from_message_kind(&message.message_kind));

        let delivery = MemythosArenaMessageDelivery {
            delivery_id,
            message_id: message.message_id.clone(),
            human_summary: message.human_summary.clone(),
            status: "delivered_to_live_thread".to_string(),
            sender_thread_id: source_thread_id,
            receiver_thread_id: params.to_parent_thread_id.clone(),
            arena_id: room.arena_id.clone(),
            round_id: message.round_id.clone(),
            phase: delivery_phase.clone(),
            delivery_mechanism: "room_loopback_send_input".to_string(),
            receiver_turn_id: Some(target_turn_id.clone()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: params.human_instruction,
            memory_replay_required: false,
            event_refs: event_refs.clone(),
            rejection_reason: None,
        };
        let mut state = self.state.lock().await;
        state.arena_message_deliveries.push(delivery);
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaMessage,
            MemythosTelemetrySource::AppServerNative,
            Some(room.layer_id.clone()),
            Some(room.arena_id.clone()),
            Some(params.to_parent_thread_id.clone()),
            Some(room_event_ref),
            Some(format!("app-server://rooms/{}", params.room_id)),
            MemythosEventChannel::StateTransition,
            format!(
                "Room {} delivered {} to parent thread {} with turn {}.",
                params.room_id, message.message_id, params.to_parent_thread_id, target_turn_id
            ),
        );
        self.push_room_activity_event(
            &mut state,
            params.room_id.clone(),
            room.arena_id.clone(),
            params.to_parent_thread_id.clone(),
            Some(target_turn_id.clone()),
            Some(message.round_id.clone()),
            delivery_phase,
            target.parent_role.clone(),
            if params.human_instruction {
                human_actor_ref()
            } else {
                source
                    .as_ref()
                    .map(|participant| room_actor_ref_for_participant(participant))
                    .unwrap_or_else(runtime_room_concierge_actor_ref)
            },
            room_actor_ref_for_participant(target),
            params.message_authority.clone(),
            if params.human_instruction {
                MemythosPromptOrigin::HumanPromptInjection
            } else {
                MemythosPromptOrigin::AgentToAgentPrompt
            },
            vec![MemythosPromptLineagePart {
                origin: if params.human_instruction {
                    MemythosPromptOrigin::HumanPromptInjection
                } else {
                    MemythosPromptOrigin::AgentToAgentPrompt
                },
                summary: if params.human_instruction {
                    format!(
                        "Human intake delivered {} to {}",
                        message.message_kind, message.to_parent_role
                    )
                } else {
                    format!(
                        "Room delivered {} from {} to {}",
                        message.message_kind, message.from_parent_role, message.to_parent_role
                    )
                },
                source_ref: Some(params.room_message_ref.clone()),
            }],
            "human_like",
            if params.human_instruction {
                "human_intake_delivered"
            } else {
                "input_delivered"
            },
            "running",
            format!(
                "Room {} delivered {} to parent thread {} with turn {}.",
                params.room_id, message.message_id, params.to_parent_thread_id, target_turn_id
            ),
            Some(format!(
                "app-server://rooms/{}/messages/{}/delivered",
                params.room_id, message.message_id
            )),
        );

        Ok(MemythosRoomSendInputResponse {
            delivery: MemythosRoomSendInputDelivery {
                thread_id: params.to_parent_thread_id,
                turn_id: target_turn_id,
                event_refs,
                room_id: params.room_id,
                room_message_ref: params.room_message_ref,
                delivery_ref: params.delivery_ref,
                delivery_mechanism: "room_loopback_send_input".to_string(),
                human_instruction: params.human_instruction,
                message_authority: params.message_authority,
            },
        }
        .into())
    }

    pub(crate) async fn room_send(
        &self,
        params: MemythosRoomSendInputParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let payload = self.room_send_input(params).await?;
        let ClientResponsePayload::MemythosRoomSendInput(mut response) = payload else {
            return Ok(payload);
        };
        response.delivery.delivery_mechanism = "room_loopback_send".to_string();
        Ok(ClientResponsePayload::MemythosRoomSendInput(response))
    }

    pub(crate) async fn thread_consolidate(
        &self,
        params: MemythosThreadConsolidateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        validate_thread_consolidation_request(&params)?;
        let attempt = self
            .thread_consolidation_adapter
            .consolidate_threads(&params)
            .await;
        let consolidation_turn_id = attempt
            .consolidation_turn_id
            .clone()
            .unwrap_or_else(|| "unavailable".to_string());
        let event_ref = format!(
            "app-server://threads/{}/turns/{}/memythos-thread-consolidation",
            params.coordinator_thread_id, consolidation_turn_id
        );
        let mut state = self.state.lock().await;
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ThreadConsolidation,
            if attempt.used_thread_turns_summary {
                MemythosTelemetrySource::AppServerNative
            } else {
                MemythosTelemetrySource::MemythosRuntimeState
            },
            None,
            None,
            Some(params.coordinator_thread_id.clone()),
            Some(event_ref),
            attempt.agent_message_ref.clone(),
            if attempt.blockers.is_empty() {
                MemythosEventChannel::HumanHighlight
            } else {
                MemythosEventChannel::TechnicalDetail
            },
            format!(
                "Thread consolidation for coordinator {} used {} source thread(s).",
                params.coordinator_thread_id,
                params.source_thread_ids.len()
            ),
        );

        Ok(MemythosThreadConsolidateResponse {
            consolidation_turn_id,
            coordinator_thread_id: params.coordinator_thread_id,
            source_refs: attempt.source_refs,
            agent_message_ref: attempt.agent_message_ref,
            structured_output_ref: attempt.structured_output_ref,
            technical_evidence_refs: attempt.technical_evidence_refs,
            source_method: attempt.source_method,
            used_thread_turns_summary: attempt.used_thread_turns_summary,
            blockers: attempt.blockers,
        }
        .into())
    }

    pub(crate) async fn thread_contract_assemble(
        &self,
        params: MemythosThreadContractAssembleParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        validate_thread_contract_assemble_request(&params)?;

        let contract_id = self.next_id("mem_contract", &self.next_contract_id);
        let producer_turn_id = format!("artifact-{}", contract_id);
        let contract_ref = format!(
            "app-server://threads/{}/turns/{}/contracts/{}",
            params.coordinator_thread_id, producer_turn_id, contract_id
        );
        let structured_output_ref = Some(format!("{}/payload", contract_ref));
        let schema_ref = format!(
            "app-server://schemas/{}/v1",
            sanitize_contract_ref_segment(&params.contract_kind)
        );
        let source_refs = build_contract_source_refs(&params);
        let technical_evidence_refs = compact_event_refs(vec![
            format!(
                "app-server://threads/{}/memythos/contracts/{}/instructions",
                params.coordinator_thread_id, contract_id
            ),
            format!(
                "app-server://threads/{}/memythos/contracts/{}/schema",
                params.coordinator_thread_id, contract_id
            ),
        ]);
        let source_evidence_refs = contract_source_evidence_refs(
            &source_refs,
            &technical_evidence_refs,
            None,
            structured_output_ref.as_deref(),
        );
        let payload = params.output_schema.as_ref().map(|schema| {
            serde_json::json!({
                "contract_kind": params.contract_kind,
                "schema_ref": schema_ref,
                "output_schema": schema,
                "structured_output_ref": structured_output_ref,
                "instructions": params.instructions,
                "source_evidence_refs": source_evidence_refs
            })
        });
        let contract = MemythosStructuredContract {
            contract_ref: contract_ref.clone(),
            contract_kind: params.contract_kind.clone(),
            schema_ref,
            producer_thread_id: params.coordinator_thread_id.clone(),
            producer_turn_id: producer_turn_id.clone(),
            source_evidence_refs: source_evidence_refs.clone(),
            storage_kind: "app_server_native_contract_message".to_string(),
            created_at: Utc::now().to_rfc3339(),
            payload,
            missing_evidence: if params.output_schema.is_none() {
                vec!["structured_output_ref".to_string()]
            } else {
                Vec::new()
            },
            blockers: Vec::new(),
        };

        let event_ref = contract.contract_ref.clone();
        let mut state = self.state.lock().await;
        state
            .structured_contracts
            .insert(contract_ref.clone(), contract.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ThreadConsolidation,
            MemythosTelemetrySource::AppServerNative,
            None,
            None,
            Some(params.coordinator_thread_id.clone()),
            Some(event_ref),
            structured_output_ref.clone(),
            if contract.blockers.is_empty() && contract.missing_evidence.is_empty() {
                MemythosEventChannel::ArtifactPayload
            } else {
                MemythosEventChannel::TechnicalDetail
            },
            format!(
                "Structured contract {} assembled for coordinator {}.",
                contract.contract_kind, contract.producer_thread_id
            ),
        );

        Ok(MemythosThreadContractAssembleResponse {
            contract,
            source_refs,
            agent_message_ref: None,
            structured_output_ref,
            technical_evidence_refs,
            source_method: "memythos/thread/contract/assemble".to_string(),
            used_thread_turns_summary: false,
        }
        .into())
    }

    pub(crate) async fn thread_contract_read(
        &self,
        params: MemythosThreadContractReadParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let Some(contract) = state.structured_contracts.get(&params.contract_ref) else {
            return Err(invalid_params(format!(
                "unknown contract ref: {}",
                params.contract_ref
            )));
        };

        Ok(MemythosThreadContractReadResponse {
            contract: contract.clone(),
        }
        .into())
    }

    pub(crate) async fn thread_contract_list(
        &self,
        params: MemythosThreadContractListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let limit = params.limit.unwrap_or(50).clamp(1, 200);
        let state = self.state.lock().await;
        let mut contracts = state
            .structured_contracts
            .values()
            .filter(|contract| {
                params
                    .thread_id
                    .as_ref()
                    .map_or(true, |thread_id| &contract.producer_thread_id == thread_id)
            })
            .filter(|contract| {
                params
                    .contract_kind
                    .as_ref()
                    .map_or(true, |kind| &contract.contract_kind == kind)
            })
            .cloned()
            .collect::<Vec<_>>();
        contracts.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        contracts.truncate(limit);

        Ok(MemythosThreadContractListResponse { contracts }.into())
    }

    pub(crate) async fn room_activity_list(
        &self,
        params: MemythosRoomActivityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let (room, mut deliveries, room_activity_events, token_usage_refs) = {
            let state = self.state.lock().await;
            let room =
                state.rooms.get(&params.room_id).cloned().ok_or_else(|| {
                    invalid_params(format!("unknown room id: {}", params.room_id))
                })?;
            let participant_thread_ids = room
                .participants
                .iter()
                .map(|participant| participant.thread_id.as_str())
                .collect::<HashSet<_>>();
            let deliveries = state
                .arena_message_deliveries
                .iter()
                .filter(|delivery| delivery.arena_id == room.arena_id)
                .filter(|delivery| {
                    params
                        .round_id
                        .as_ref()
                        .map_or(true, |round_id| &delivery.round_id == round_id)
                })
                .filter(|delivery| {
                    participant_thread_ids.contains(delivery.sender_thread_id.as_str())
                        || participant_thread_ids.contains(delivery.receiver_thread_id.as_str())
                })
                .cloned()
                .collect::<Vec<_>>();
            let room_activity_events = state
                .room_activity_events
                .get(&room.room_id)
                .cloned()
                .unwrap_or_default();
            let token_usage_refs = state
                .native_token_usage_refs
                .iter()
                .filter(|(key, _)| {
                    participant_thread_ids
                        .iter()
                        .any(|thread_id| key.starts_with(&format!("{thread_id}::")))
                })
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>();
            (room, deliveries, room_activity_events, token_usage_refs)
        };
        deliveries.sort_by(|left, right| left.delivery_id.cmp(&right.delivery_id));
        if let Some(phase) = params.phase.as_ref() {
            deliveries.retain(|delivery| delivery.phase.as_deref() == Some(phase.as_str()));
        }
        if let Some(limit) = params.limit {
            deliveries.truncate(limit);
        }
        let mut blockers = Vec::new();
        let requested_cursor = params
            .after_cursor
            .clone()
            .or_else(|| params.since_cursor.clone());
        let mut filtered_events = room_activity_events
            .into_iter()
            .filter(|event| {
                params
                    .round_id
                    .as_ref()
                    .map_or(true, |round_id| event.round_id.as_ref() == Some(round_id))
            })
            .filter(|event| {
                params
                    .phase
                    .as_ref()
                    .map_or(true, |phase| event.phase.as_ref() == Some(phase))
            })
            .collect::<Vec<_>>();
        let since_cursor_applied = if let Some(cursor) = requested_cursor.as_deref() {
            if let Some(cursor_index) = filtered_events
                .iter()
                .position(|event| event.cursor == cursor)
            {
                filtered_events = filtered_events.into_iter().skip(cursor_index + 1).collect();
                true
            } else {
                blockers.push(format!("unknown or stale room activity cursor: {cursor}"));
                filtered_events.clear();
                deliveries.clear();
                false
            }
        } else {
            false
        };
        let has_more = params
            .limit
            .map_or(false, |limit| filtered_events.len() > limit);
        if let Some(limit) = params.limit {
            filtered_events.truncate(limit);
        }

        let completed_turns = deliveries
            .iter()
            .filter(|delivery| delivery.status == "receiver_turn_completed")
            .count();
        let failed_turns = deliveries
            .iter()
            .filter(|delivery| {
                delivery.status.contains("failed")
                    || delivery.status.contains("interrupted")
                    || delivery.rejection_reason.is_some()
            })
            .count();
        let active_turns = deliveries
            .iter()
            .filter(|delivery| delivery.receiver_turn_id.is_some())
            .filter(|delivery| {
                delivery.status == "delivered_to_live_thread"
                    || delivery.status == "recorded"
                    || delivery.status == "receiver_turn_running"
            })
            .count();
        let clean_close = active_turns == 0 && failed_turns == 0;
        let participants = room
            .participants
            .iter()
            .map(|participant| {
                let participant_deliveries = deliveries
                    .iter()
                    .filter(|delivery| delivery.receiver_thread_id == participant.thread_id)
                    .collect::<Vec<_>>();
                let participant_events = filtered_events
                    .iter()
                    .filter(|event| event.thread_id == participant.thread_id)
                    .collect::<Vec<_>>();
                let active_turn_count = participant_deliveries
                    .iter()
                    .filter(|delivery| {
                        delivery.status == "delivered_to_live_thread"
                            || delivery.status == "recorded"
                            || delivery.status == "receiver_turn_running"
                    })
                    .count();
                let completed_turn_count = participant_deliveries
                    .iter()
                    .filter(|delivery| {
                        delivery.status == "receiver_turn_completed"
                            || delivery.receiver_response_event_ref.is_some()
                    })
                    .count();
                let failed_turn_count = participant_deliveries
                    .iter()
                    .filter(|delivery| {
                        delivery.status.contains("failed")
                            || delivery.status.contains("interrupted")
                            || delivery.rejection_reason.is_some()
                    })
                    .count();
                let last_activity_summary = participant_events
                    .iter()
                    .rev()
                    .find(|event| {
                        event.channel == "agent_activity"
                            || event.channel == "lifecycle"
                            || event.channel == "human_like"
                    })
                    .map(|event| event.summary.clone())
                    .or_else(|| {
                        participant_deliveries.last().map(|delivery| {
                            compact_summary(format!(
                                "{} {} {} from {}.",
                                delivery.delivery_mechanism,
                                delivery.status,
                                delivery.message_id,
                                delivery.sender_thread_id
                            ))
                        })
                    });
                let status = if participant_deliveries
                    .iter()
                    .any(|delivery| delivery.status.contains("failed"))
                {
                    "failed"
                } else if participant_deliveries
                    .iter()
                    .any(|delivery| delivery.status == "delivered_to_live_thread")
                {
                    "running"
                } else if participant_deliveries.iter().any(|delivery| {
                    delivery.status == "receiver_turn_completed"
                        || delivery.receiver_response_event_ref.is_some()
                }) {
                    "completed"
                } else {
                    "idle"
                };
                MemythosRoomActivityParticipant {
                    parent_key: participant.parent_key.clone(),
                    thread_id: participant.thread_id.clone(),
                    parent_role: participant.parent_role.clone(),
                    stance_profile: participant.stance_profile.clone(),
                    status: status.to_string(),
                    goal_ref: participant.goal_ref.clone(),
                    delivery_count: participant_deliveries.len(),
                    active_turn_count,
                    completed_turn_count,
                    failed_turn_count,
                    activity_event_count: participant_events.len(),
                    last_activity_summary,
                }
            })
            .collect::<Vec<_>>();
        let requested_turns = deliveries
            .iter()
            .filter_map(|delivery| {
                delivery
                    .receiver_turn_id
                    .as_ref()
                    .map(|turn_id| (delivery.receiver_thread_id.clone(), turn_id.clone()))
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let native_turn_responses = self
            .parent_turn_response_adapter
            .read_responses(requested_turns)
            .await;
        let turns = deliveries
            .iter()
            .filter_map(|delivery| {
                let native_response = delivery.receiver_turn_id.as_ref().and_then(|turn_id| {
                    native_turn_responses
                        .get(&(delivery.receiver_thread_id.clone(), turn_id.clone()))
                });
                if delivery.status == "receiver_turn_completed"
                    && native_response.and_then(|response| response.text.as_ref()).is_none()
                {
                    blockers.push(format!(
                        "completed parent turn {} for thread {} has no readable native AgentMessage",
                        delivery.receiver_turn_id.as_deref().unwrap_or("unknown"),
                        delivery.receiver_thread_id
                    ));
                }
                room_activity_turn_from_delivery(
                    &room,
                    delivery,
                    native_response,
                    params.include_debug_refs,
                )
            })
            .collect::<Vec<_>>();
        let cursor = filtered_events
            .last()
            .map(|event| event.cursor.clone())
            .or(requested_cursor);
        let next_cursor = cursor.clone();
        let returned_activity_scope = if since_cursor_applied {
            "delta"
        } else {
            "initial"
        };
        Ok(MemythosRoomActivityListResponse {
            room_id: room.room_id,
            case_id: room.case_id,
            layer_id: room.layer_id,
            arena_id: room.arena_id,
            round_id: params.round_id,
            cursor,
            since_cursor_applied,
            next_cursor,
            has_more,
            returned_activity_scope: returned_activity_scope.to_string(),
            source_method: "memythos/room/activity/list".to_string(),
            events: filtered_events,
            participants,
            turns,
            lifecycle: MemythosRoomActivityLifecycle {
                room_state: if clean_close {
                    "round_closed".to_string()
                } else {
                    "running".to_string()
                },
                active_turns,
                completed_turns,
                failed_turns,
                clean_close,
                force_closed: false,
            },
            collab: MemythosRoomActivityCollab {
                send_input_count: deliveries.len(),
                completed_send_input_count: completed_turns,
                failed_send_input_count: failed_turns,
                wait_count: 0,
            },
            subagents: MemythosRoomActivitySubagents {
                activity_count: 0,
                started_count: 0,
                interacted_count: 0,
                interrupted_count: 0,
            },
            usage: MemythosRoomActivityUsage {
                token_usage_events: token_usage_refs.len(),
                refs: token_usage_refs,
            },
            blockers,
        }
        .into())
    }

    pub(crate) async fn room_parent_configuration_list(
        &self,
        params: MemythosRoomParentConfigurationListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let room = {
            let state = self.state.lock().await;
            state
                .rooms
                .get(&params.room_id)
                .cloned()
                .ok_or_else(|| invalid_params(format!("unknown room id: {}", params.room_id)))?
        };
        let mut configurations = Vec::with_capacity(room.participants.len());
        let mut blockers = Vec::new();
        for participant in &room.participants {
            let snapshot = self
                .parent_configuration_adapter
                .read_configuration(&participant.thread_id)
                .await;
            let configuration = parent_configuration_for_participant(&room, participant, snapshot);
            blockers.extend(
                configuration
                    .blockers
                    .iter()
                    .map(|blocker| format!("parent {}: {blocker}", participant.thread_id)),
            );
            configurations.push(configuration);
        }
        Ok(MemythosRoomParentConfigurationListResponse {
            room_id: room.room_id,
            arena_id: room.arena_id,
            source_method: "memythos/room/parent-configuration/list".to_string(),
            configurations,
            blockers,
        }
        .into())
    }

    pub(crate) async fn room_dialogue_list(
        &self,
        params: MemythosRoomDialogueListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let (room, mut deliveries, input_events) = {
            let state = self.state.lock().await;
            let room =
                state.rooms.get(&params.room_id).cloned().ok_or_else(|| {
                    invalid_params(format!("unknown room id: {}", params.room_id))
                })?;
            let participant_thread_ids = room
                .participants
                .iter()
                .map(|participant| participant.thread_id.as_str())
                .collect::<HashSet<_>>();
            let deliveries = state
                .arena_message_deliveries
                .iter()
                .filter(|delivery| delivery.arena_id == room.arena_id)
                .filter(|delivery| {
                    participant_thread_ids.contains(delivery.sender_thread_id.as_str())
                        || participant_thread_ids.contains(delivery.receiver_thread_id.as_str())
                })
                .filter(|delivery| {
                    params
                        .round_id
                        .as_ref()
                        .map_or(true, |round_id| &delivery.round_id == round_id)
                })
                .filter(|delivery| {
                    params
                        .phase
                        .as_ref()
                        .map_or(true, |phase| delivery.phase.as_ref() == Some(phase))
                })
                .cloned()
                .collect::<Vec<_>>();
            let input_events = state
                .room_activity_events
                .get(&room.room_id)
                .into_iter()
                .flatten()
                .filter(|event| {
                    event.channel == "human_like"
                        && matches!(
                            event.event_kind.as_str(),
                            "human_intake_delivered" | "input_delivered"
                        )
                })
                .cloned()
                .collect::<Vec<_>>();
            (room, deliveries, input_events)
        };
        deliveries.sort_by(|left, right| left.delivery_id.cmp(&right.delivery_id));
        let requested_turns = deliveries
            .iter()
            .filter_map(|delivery| {
                delivery
                    .receiver_turn_id
                    .as_ref()
                    .map(|turn_id| (delivery.receiver_thread_id.clone(), turn_id.clone()))
            })
            .collect::<Vec<_>>();
        let native_responses = self
            .parent_turn_response_adapter
            .read_responses(requested_turns)
            .await;
        let participant_by_thread = room
            .participants
            .iter()
            .map(|participant| (participant.thread_id.as_str(), participant))
            .collect::<HashMap<_, _>>();
        let mut blockers = Vec::new();
        let mut entries = Vec::new();
        for delivery in &deliveries {
            let Some(turn_id) = delivery.receiver_turn_id.as_ref() else {
                continue;
            };
            let Some(input_event) = input_events
                .iter()
                .find(|event| event.turn_id.as_ref() == Some(turn_id))
            else {
                blockers.push(format!("turn {turn_id} has no native room input event"));
                continue;
            };
            let native_response =
                native_responses.get(&(delivery.receiver_thread_id.clone(), turn_id.clone()));
            if let Some(item_ref) =
                native_response.and_then(|response| response.request_item_ref.as_ref())
            {
                entries.push(MemythosRoomDialogueEntry {
                    cursor: format!("{}:request", input_event.cursor),
                    iteration: input_event.iteration,
                    sequence: input_event.sequence.saturating_mul(2),
                    room_id: room.room_id.clone(),
                    arena_id: room.arena_id.clone(),
                    thread_id: delivery.receiver_thread_id.clone(),
                    turn_id: turn_id.clone(),
                    round_id: Some(delivery.round_id.clone()),
                    phase: delivery.phase.clone(),
                    kind: "request".to_string(),
                    sender: input_event.sender.clone(),
                    recipient: input_event.recipient.clone(),
                    text: delivery.human_summary.clone(),
                    source_item_ref: item_ref.clone(),
                    causal_ref: delivery.message_id.clone(),
                });
            } else {
                blockers.push(format!(
                    "turn {turn_id} request has no native UserMessage item ref"
                ));
            }
            if let Some(response) = native_response {
                match (response.item_ref.as_ref(), response.text.as_ref()) {
                    (Some(item_ref), Some(text)) => {
                        let sender = participant_by_thread
                            .get(delivery.receiver_thread_id.as_str())
                            .map(|participant| room_actor_ref_for_participant(participant))
                            .unwrap_or_else(app_server_actor_ref);
                        entries.push(MemythosRoomDialogueEntry {
                            cursor: format!("{}:response", input_event.cursor),
                            iteration: input_event.iteration,
                            sequence: input_event.sequence.saturating_mul(2).saturating_add(1),
                            room_id: room.room_id.clone(),
                            arena_id: room.arena_id.clone(),
                            thread_id: delivery.receiver_thread_id.clone(),
                            turn_id: turn_id.clone(),
                            round_id: Some(delivery.round_id.clone()),
                            phase: delivery.phase.clone(),
                            kind: "response".to_string(),
                            sender,
                            recipient: input_event.sender.clone(),
                            text: text.clone(),
                            source_item_ref: item_ref.clone(),
                            causal_ref: format!("{}:request", input_event.cursor),
                        });
                    }
                    (None, Some(_)) | (Some(_), None) => blockers.push(format!(
                        "turn {turn_id} has an incomplete native AgentMessage projection"
                    )),
                    (None, None) => {}
                }
            }
        }
        entries.sort_by_key(|entry| (entry.iteration, entry.sequence));
        let mut after_cursor_applied = false;
        if let Some(after_cursor) = params.after_cursor.as_deref() {
            if let Some(index) = entries
                .iter()
                .position(|entry| entry.cursor == after_cursor)
            {
                entries = entries.into_iter().skip(index + 1).collect();
                after_cursor_applied = true;
            } else {
                blockers.push(format!(
                    "unknown or stale room dialogue cursor: {after_cursor}"
                ));
                entries.clear();
            }
        }
        let has_more = params.limit.map_or(false, |limit| entries.len() > limit);
        if let Some(limit) = params.limit {
            entries.truncate(limit.clamp(1, 500));
        }
        let cursor = entries.last().map(|entry| entry.cursor.clone());
        Ok(MemythosRoomDialogueListResponse {
            room_id: room.room_id,
            arena_id: room.arena_id,
            source_method: "memythos/room/dialogue/list".to_string(),
            cursor,
            after_cursor_applied,
            has_more,
            entries,
            blockers,
        }
        .into())
    }

    pub(crate) async fn room_timeline_get(
        &self,
        params: MemythosRoomActivityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let payload = self.room_activity_list(params).await?;
        let ClientResponsePayload::MemythosRoomActivityList(mut response) = payload else {
            return Ok(payload);
        };
        response.source_method = "memythos/room/timeline/get".to_string();
        Ok(ClientResponsePayload::MemythosRoomActivityList(response))
    }

    pub(crate) async fn telemetry_list(
        &self,
        params: MemythosTelemetryListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let limit = params.limit.unwrap_or(100);
        let telemetry_refs = state
            .telemetry_refs
            .iter()
            .filter(|telemetry_ref| {
                params.layer_id.as_ref().map_or(true, |layer_id| {
                    telemetry_ref.layer_id.as_ref() == Some(layer_id)
                })
            })
            .filter(|telemetry_ref| {
                params.arena_id.as_ref().map_or(true, |arena_id| {
                    telemetry_ref.arena_id.as_ref() == Some(arena_id)
                })
            })
            .filter(|telemetry_ref| {
                params.thread_id.as_ref().map_or(true, |thread_id| {
                    telemetry_ref.thread_id.as_ref() == Some(thread_id)
                })
            })
            .take(limit)
            .cloned()
            .collect();

        Ok(MemythosTelemetryListResponse { telemetry_refs }.into())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn record_native_thread_event(
        &self,
        thread_id: &str,
        native_event_ref: String,
        detail_ref: Option<String>,
        channel: MemythosEventChannel,
        summary: String,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some((layer_id, arena_id)) = find_attachment_context(&state, thread_id) else {
            return false;
        };

        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::RuntimeState,
            MemythosTelemetrySource::AppServerNative,
            Some(layer_id),
            Some(arena_id),
            Some(thread_id.to_string()),
            Some(native_event_ref),
            detail_ref,
            channel,
            summary,
        );
        true
    }

    pub(crate) async fn record_native_turn_completed(
        &self,
        thread_id: &str,
        turn_id: &str,
        status: &str,
        completed_at: Option<i64>,
        duration_ms: Option<i64>,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some((layer_id, arena_id)) = find_attachment_context(&state, thread_id) else {
            return false;
        };

        let native_event_ref =
            format!("app-server://threads/{thread_id}/turns/{turn_id}/completed");
        let mut matched_delivery = false;
        for delivery in state
            .arena_message_deliveries
            .iter_mut()
            .filter(|delivery| {
                delivery.receiver_thread_id == thread_id
                    && delivery.receiver_turn_id.as_deref() == Some(turn_id)
            })
        {
            matched_delivery = true;
            delivery.status = match status {
                "completed" => "receiver_turn_completed".to_string(),
                "failed" => "receiver_turn_failed".to_string(),
                "interrupted" => "receiver_turn_interrupted".to_string(),
                _ => format!("receiver_turn_{status}"),
            };
            delivery.receiver_response_event_ref = Some(native_event_ref.clone());
            if !delivery.event_refs.contains(&native_event_ref) {
                delivery.event_refs.push(native_event_ref.clone());
            }
        }

        let detail_ref = completed_at.map(|completed_at| {
            format!("app-server://threads/{thread_id}/turns/{turn_id}/completed_at/{completed_at}")
        });
        let summary = match duration_ms {
            Some(duration_ms) => format!(
                "Native turn {turn_id} for thread {thread_id} completed with status {status} in {duration_ms}ms."
            ),
            None => format!(
                "Native turn {turn_id} for thread {thread_id} completed with status {status}."
            ),
        };
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaMessage,
            MemythosTelemetrySource::AppServerNative,
            Some(layer_id.clone()),
            Some(arena_id.clone()),
            Some(thread_id.to_string()),
            Some(native_event_ref.clone()),
            detail_ref.clone(),
            MemythosEventChannel::StateTransition,
            summary.clone(),
        );
        let room_activity_targets = state
            .rooms
            .values()
            .filter(|room| room.arena_id == arena_id)
            .filter_map(|room| {
                room.participants
                    .iter()
                    .find(|participant| participant.thread_id == thread_id)
                    .map(|participant| {
                        (
                            room.room_id.clone(),
                            room.arena_id.clone(),
                            participant.clone(),
                        )
                    })
            })
            .collect::<Vec<_>>();
        for (room_id, room_arena_id, participant) in room_activity_targets {
            self.push_room_activity_event(
                &mut state,
                room_id,
                room_arena_id,
                thread_id.to_string(),
                Some(turn_id.to_string()),
                None,
                None,
                participant.parent_role.clone(),
                room_actor_ref_for_participant(&participant),
                app_server_actor_ref(),
                "turn_lifecycle".to_string(),
                MemythosPromptOrigin::AppServerProtocol,
                vec![MemythosPromptLineagePart {
                    origin: MemythosPromptOrigin::AppServerProtocol,
                    summary: "app-server observed parent turn completion".to_string(),
                    source_ref: Some(native_event_ref.clone()),
                }],
                "lifecycle",
                "turn_completed",
                status,
                summary.clone(),
                Some(native_event_ref.clone()),
            );
        }

        matched_delivery
    }

    pub(crate) async fn record_native_token_usage(&self, thread_id: &str, turn_id: &str) -> bool {
        let mut state = self.state.lock().await;
        let Some((layer_id, arena_id)) = find_attachment_context(&state, thread_id) else {
            return false;
        };

        let native_event_ref =
            format!("app-server://threads/{thread_id}/turns/{turn_id}/token-usage");
        state.native_token_usage_refs.insert(
            native_token_usage_key(thread_id, turn_id),
            native_event_ref.clone(),
        );
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaMessage,
            MemythosTelemetrySource::AppServerNative,
            Some(layer_id),
            Some(arena_id.clone()),
            Some(thread_id.to_string()),
            Some(native_event_ref.clone()),
            None,
            MemythosEventChannel::StateTransition,
            format!("Native token usage observed for thread {thread_id} turn {turn_id}."),
        );
        let room_activity_targets = state
            .rooms
            .values()
            .filter(|room| room.arena_id == arena_id)
            .filter_map(|room| {
                room.participants
                    .iter()
                    .find(|participant| participant.thread_id == thread_id)
                    .map(|participant| {
                        (
                            room.room_id.clone(),
                            room.arena_id.clone(),
                            participant.clone(),
                        )
                    })
            })
            .collect::<Vec<_>>();
        for (room_id, room_arena_id, participant) in room_activity_targets {
            self.push_room_activity_event(
                &mut state,
                room_id,
                room_arena_id,
                thread_id.to_string(),
                Some(turn_id.to_string()),
                None,
                None,
                participant.parent_role.clone(),
                room_actor_ref_for_participant(&participant),
                app_server_actor_ref(),
                "usage_observation".to_string(),
                MemythosPromptOrigin::AppServerProtocol,
                vec![MemythosPromptLineagePart {
                    origin: MemythosPromptOrigin::AppServerProtocol,
                    summary: "app-server observed parent token usage".to_string(),
                    source_ref: Some(native_event_ref.clone()),
                }],
                "technical",
                "token_usage_observed",
                "completed",
                format!("Native token usage observed for thread {thread_id} turn {turn_id}."),
                Some(native_event_ref.clone()),
            );
        }
        true
    }

    fn push_telemetry_ref(
        &self,
        state: &mut MemythosRuntimeState,
        kind: MemythosTelemetryRefKind,
        source: MemythosTelemetrySource,
        layer_id: Option<String>,
        arena_id: Option<String>,
        thread_id: Option<String>,
        native_event_ref: Option<String>,
        detail_ref: Option<String>,
        channel: MemythosEventChannel,
        summary: String,
    ) {
        let telemetry_ref_id = self.next_id("mem_tel", &self.next_telemetry_ref_id);
        state.telemetry_refs.push(MemythosTelemetryRef {
            telemetry_ref_id,
            kind,
            source,
            layer_id,
            arena_id,
            thread_id,
            native_event_ref,
            detail_ref,
            channel,
            summary: compact_summary(summary),
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn push_room_activity_event(
        &self,
        state: &mut MemythosRuntimeState,
        room_id: String,
        arena_id: String,
        thread_id: String,
        turn_id: Option<String>,
        round_id: Option<String>,
        phase: Option<String>,
        participant_role: String,
        sender: MemythosRoomActorRef,
        recipient: MemythosRoomActorRef,
        authority: String,
        prompt_origin: MemythosPromptOrigin,
        prompt_lineage: Vec<MemythosPromptLineagePart>,
        channel: &str,
        event_kind: &str,
        status: &str,
        summary: String,
        source_ref: Option<String>,
    ) -> String {
        let cursor = self.next_id("mem_room_activity", &self.next_room_activity_id);
        let sequence = state
            .room_activity_events
            .get(&room_id)
            .map_or(1, |events| events.len() as u64 + 1);
        let event = MemythosRoomActivityEvent {
            cursor: cursor.clone(),
            iteration: 0,
            sequence,
            room_id: room_id.clone(),
            arena_id,
            thread_id,
            turn_id,
            round_id,
            phase,
            participant_role,
            channel: channel.to_string(),
            event_kind: event_kind.to_string(),
            status: status.to_string(),
            sender,
            recipient,
            authority,
            prompt_origin,
            prompt_lineage,
            summary: compact_summary(summary),
            source_ref,
        };
        state
            .room_activity_events
            .entry(room_id)
            .or_default()
            .push(event);
        cursor
    }

    #[cfg(test)]
    fn push_native_telemetry_ref_for_test(
        &self,
        state: &mut MemythosRuntimeState,
        kind: MemythosTelemetryRefKind,
        layer_id: Option<String>,
        arena_id: Option<String>,
        thread_id: Option<String>,
        native_event_ref: String,
        detail_ref: Option<String>,
        channel: MemythosEventChannel,
        summary: String,
    ) {
        self.push_telemetry_ref(
            state,
            kind,
            MemythosTelemetrySource::AppServerNative,
            layer_id,
            arena_id,
            thread_id,
            Some(native_event_ref),
            detail_ref,
            channel,
            summary,
        );
    }

    fn next_id(&self, prefix: &str, counter: &AtomicU64) -> String {
        let next = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        format!("{prefix}_{next}")
    }
}

fn room_activity_turn_from_delivery(
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
        items,
    })
}

fn phase_from_message_kind(message_kind: &str) -> Option<String> {
    match message_kind {
        "dispatch_proposals" => Some("proposal".to_string()),
        "dispatch_cross_read" => Some("cross_read".to_string()),
        "dispatch_bets" => Some("bet".to_string()),
        "request_judge" => Some("judge".to_string()),
        "notify_coordinator" => Some("learning".to_string()),
        _ => None,
    }
}

const MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS: usize = 240;

fn find_attachment_context(
    state: &MemythosRuntimeState,
    thread_id: &str,
) -> Option<(String, String)> {
    if let Some(attachment) = state
        .thread_attachments
        .values()
        .find(|attachment| attachment.thread_id == thread_id)
    {
        let arena = state.arenas.get(&attachment.arena_id)?;
        return Some((arena.layer_id.clone(), attachment.arena_id.clone()));
    }

    state.rooms.values().find_map(|room| {
        room.participants
            .iter()
            .any(|participant| participant.thread_id == thread_id)
            .then(|| (room.layer_id.clone(), room.arena_id.clone()))
    })
}

fn arena_parent_key(arena_id: &str, thread_id: &str) -> String {
    format!("{arena_id}::{thread_id}")
}

fn native_token_usage_key(thread_id: &str, turn_id: &str) -> String {
    format!("{thread_id}::{turn_id}")
}

fn build_parent_thread_continuity(
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

fn parent_goal_snapshot_from_goal(thread_id: &str, goal: Option<ThreadGoal>) -> ParentGoalSnapshot {
    let Some(goal) = goal else {
        return ParentGoalSnapshot {
            goal_snapshot_ref: None,
            budget_state_ref: None,
            goal_status: None,
            token_budget: None,
            tokens_used: None,
            time_used_seconds: None,
            evidence_refs: Vec::new(),
            degraded_reason: Some("thread/goal/get returned no active goal".to_string()),
        };
    };

    let goal_snapshot_ref = format!("app-server://threads/{thread_id}/goals/current");
    let budget_state_ref = format!("app-server://threads/{thread_id}/budget/current");
    ParentGoalSnapshot {
        goal_snapshot_ref: Some(goal_snapshot_ref.clone()),
        budget_state_ref: Some(budget_state_ref.clone()),
        goal_status: Some(goal.status),
        token_budget: goal.token_budget,
        tokens_used: Some(goal.tokens_used),
        time_used_seconds: Some(goal.time_used_seconds),
        evidence_refs: vec![goal_snapshot_ref, budget_state_ref],
        degraded_reason: None,
    }
}

fn build_parent_peer_response_observation(
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

fn validate_room_registration(
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

fn validate_parent_role_and_stance(
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

fn room_participant_by_thread<'a>(
    room: &'a MemythosRoom,
    thread_id: &str,
) -> Option<&'a MemythosRoomParticipant> {
    room.participants
        .iter()
        .find(|participant| participant.thread_id == thread_id)
}

fn app_server_actor_ref() -> MemythosRoomActorRef {
    MemythosRoomActorRef {
        kind: MemythosRoomActorKind::AppServer,
        thread_id: None,
        parent_key: None,
        role: None,
        stance: None,
        label: Some("app-server".to_string()),
    }
}

fn runtime_room_concierge_actor_ref() -> MemythosRoomActorRef {
    MemythosRoomActorRef {
        kind: MemythosRoomActorKind::RoomConcierge,
        thread_id: None,
        parent_key: None,
        role: Some(MemythosParentRole::RoomConcierge),
        stance: Some(MemythosParentStance::Coordination),
        label: Some("room_concierge".to_string()),
    }
}

fn human_actor_ref() -> MemythosRoomActorRef {
    MemythosRoomActorRef {
        kind: MemythosRoomActorKind::Human,
        thread_id: None,
        parent_key: None,
        role: None,
        stance: None,
        label: Some("human".to_string()),
    }
}

fn room_actor_ref_for_participant(participant: &MemythosRoomParticipant) -> MemythosRoomActorRef {
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

fn parent_configuration_for_participant(
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

fn compact_event_refs(mut event_refs: Vec<String>) -> Vec<String> {
    event_refs.retain(|event_ref| !event_ref.trim().is_empty());
    event_refs.sort();
    event_refs.dedup();
    event_refs
}

fn compact_summary(summary: String) -> String {
    let normalized = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS {
        return normalized;
    }

    let mut compacted: String = normalized
        .chars()
        .take(MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS.saturating_sub(1))
        .collect();
    compacted.push('…');
    compacted
}

fn normalize_consolidation_items_view(items_view: Option<&str>) -> &'static str {
    match items_view {
        Some("summary") | None => "summary",
        Some("full") => "full",
        Some("notLoaded") => "notLoaded",
        Some(_) => "summary",
    }
}

fn empty_consolidation_source_ref(
    thread_id: &str,
    cursor: Option<String>,
    items_view: &str,
) -> MemythosThreadConsolidationSourceRef {
    MemythosThreadConsolidationSourceRef {
        thread_id: thread_id.to_string(),
        turn_refs: Vec::new(),
        items_view: items_view.to_string(),
        cursor: cursor.clone(),
        next_cursor: cursor,
        latest_agent_message_ref: None,
        latest_agent_message_text: None,
        technical_evidence_refs: Vec::new(),
    }
}

fn build_thread_consolidation_prompt(params: &MemythosThreadConsolidateParams) -> String {
    format!(
        concat!(
            "Consolida la informacion de los hilos fuente como coordinador Memythos.\n",
            "No estas recibiendo una instruccion humana directa; estas coordinando una arena.\n",
            "Usa el contexto de aplicacion `memythos.thread_consolidation` como evidencia.\n",
            "No copies JSON ni logs tecnicos en la respuesta conversacional.\n",
            "Devuelve una sintesis natural con acuerdos, disensos, definiciones faltantes y siguiente accion.\n",
            "\n",
            "Proposito: {purpose:?}\n",
            "Modo de autoridad: {authority_mode:?}\n",
            "Instrucciones: {instructions}\n"
        ),
        purpose = params.purpose,
        authority_mode = params.authority_mode,
        instructions = params.instructions
    )
}

fn build_contract_source_refs(
    params: &MemythosThreadContractAssembleParams,
) -> Vec<MemythosThreadConsolidationSourceRef> {
    params
        .source_thread_ids
        .iter()
        .map(|thread_id| MemythosThreadConsolidationSourceRef {
            thread_id: thread_id.clone(),
            turn_refs: vec![format!(
                "app-server://threads/{}/turns/latest-summary",
                thread_id
            )],
            items_view: params
                .items_view
                .clone()
                .unwrap_or_else(|| "summary".to_string()),
            cursor: params.since_cursors.get(thread_id).cloned(),
            next_cursor: params
                .since_cursors
                .get(thread_id)
                .cloned()
                .or_else(|| Some(format!("contract-cursor-after-{}", thread_id))),
            latest_agent_message_ref: Some(format!(
                "app-server://threads/{}/turns/latest-summary/items/agent-message",
                thread_id
            )),
            latest_agent_message_text: None,
            technical_evidence_refs: vec![format!(
                "app-server://threads/{}/turns/latest-summary/items-view/summary",
                thread_id
            )],
        })
        .collect()
}

fn contract_source_evidence_refs(
    source_refs: &[MemythosThreadConsolidationSourceRef],
    technical_evidence_refs: &[String],
    agent_message_ref: Option<&str>,
    structured_output_ref: Option<&str>,
) -> Vec<String> {
    let mut refs = source_refs
        .iter()
        .flat_map(|source| {
            source
                .turn_refs
                .iter()
                .chain(source.technical_evidence_refs.iter())
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    refs.extend(technical_evidence_refs.iter().cloned());
    if let Some(agent_message_ref) = agent_message_ref {
        refs.push(agent_message_ref.to_string());
    }
    if let Some(structured_output_ref) = structured_output_ref {
        refs.push(structured_output_ref.to_string());
    }
    compact_event_refs(refs)
}

fn sanitize_contract_ref_segment(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if normalized.is_empty() {
        "contract".to_string()
    } else {
        normalized
    }
}

fn validate_thread_consolidation_request(
    params: &MemythosThreadConsolidateParams,
) -> Result<(), JSONRPCErrorError> {
    if params.coordinator_thread_id.trim().is_empty() {
        return Err(invalid_params(
            "coordinatorThreadId is required".to_string(),
        ));
    }
    if params.source_thread_ids.is_empty() {
        return Err(invalid_params(
            "sourceThreadIds must contain at least one thread".to_string(),
        ));
    }
    if params
        .source_thread_ids
        .iter()
        .any(|thread_id| thread_id.trim().is_empty())
    {
        return Err(invalid_params(
            "sourceThreadIds cannot contain empty thread ids".to_string(),
        ));
    }
    if params.instructions.trim().is_empty() {
        return Err(invalid_params("instructions is required".to_string()));
    }
    if let Some(items_view) = params.items_view.as_deref() {
        match items_view {
            "summary" | "full" | "notLoaded" => {}
            other => {
                return Err(invalid_params(format!(
                    "itemsView must be summary, full or notLoaded; got {other}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_thread_contract_assemble_request(
    params: &MemythosThreadContractAssembleParams,
) -> Result<(), JSONRPCErrorError> {
    if params.coordinator_thread_id.trim().is_empty() {
        return Err(invalid_params(
            "coordinatorThreadId is required".to_string(),
        ));
    }
    if params.source_thread_ids.is_empty() {
        return Err(invalid_params(
            "sourceThreadIds must contain at least one thread".to_string(),
        ));
    }
    if params
        .source_thread_ids
        .iter()
        .any(|thread_id| thread_id.trim().is_empty())
    {
        return Err(invalid_params(
            "sourceThreadIds cannot contain empty thread ids".to_string(),
        ));
    }
    if params.contract_kind.trim().is_empty() {
        return Err(invalid_params("contractKind is required".to_string()));
    }
    if params.instructions.trim().is_empty() {
        return Err(invalid_params("instructions is required".to_string()));
    }
    if params.output_schema.is_none() {
        return Err(invalid_params("outputSchema is required".to_string()));
    }
    if let Some(items_view) = params.items_view.as_deref() {
        match items_view {
            "summary" | "full" | "notLoaded" => {}
            other => {
                return Err(invalid_params(format!(
                    "itemsView must be summary, full or notLoaded; got {other}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::MemythosArenaKind;
    use codex_app_server_protocol::MemythosArenaMessage;
    use codex_app_server_protocol::MemythosLayerKind;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[derive(Debug)]
    struct FakeLivePeerParentDeliveryAdapter;

    impl PeerParentDeliveryAdapter for FakeLivePeerParentDeliveryAdapter {
        fn deliver_peer_parent_message<'a>(
            &'a self,
            message: &'a MemythosArenaMessage,
        ) -> PeerParentDeliveryFuture<'a> {
            Box::pin(async move {
                PeerParentDeliveryAttempt {
                    status: "delivered_to_live_thread".to_string(),
                    delivery_mechanism: "turn_start".to_string(),
                    receiver_turn_id: Some(format!(
                        "turn_for_{}_{}",
                        message.to_parent_thread_id, message.message_id
                    )),
                    receiver_response_event_ref: None,
                    delivered_as_human_instruction: false,
                    memory_replay_required: false,
                    event_refs: vec![
                        format!(
                            "memythos://arenas/{}/rounds/{}/messages/{}",
                            message.arena_id, message.round_id, message.message_id
                        ),
                        format!(
                            "app-server://threads/{}/turns/turn_for_{}_{}",
                            message.to_parent_thread_id,
                            message.to_parent_thread_id,
                            message.message_id
                        ),
                    ],
                    rejection_reason: None,
                    telemetry_channel: MemythosEventChannel::StateTransition,
                    telemetry_summary: format!(
                        "Arena message {} delivered to live parent thread {}.",
                        message.message_id, message.to_parent_thread_id
                    ),
                }
            })
        }
    }

    #[derive(Debug)]
    struct FakeParentGoalSnapshotAdapter;

    #[derive(Debug)]
    struct FakeParentConfigurationAdapter;

    impl ParentConfigurationAdapter for FakeParentConfigurationAdapter {
        fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a> {
            Box::pin(async move {
                ParentConfigurationSnapshot {
                    agent_role: Some(format!("native_role_for_{thread_id}")),
                    personality: Some("pragmatic".to_string()),
                    multi_agent_mode: Some("proactive".to_string()),
                    parent_thread_id: None,
                    collaboration_mode: "default".to_string(),
                    session_source: "app_server".to_string(),
                    config_sources: vec![format!("app-server://threads/{thread_id}/config")],
                    lifecycle_state: "loaded".to_string(),
                    blockers: Vec::new(),
                }
            })
        }
    }

    impl ParentGoalSnapshotAdapter for FakeParentGoalSnapshotAdapter {
        fn current_goal_snapshot<'a>(&'a self, thread_id: &'a str) -> ParentGoalSnapshotFuture<'a> {
            Box::pin(async move {
                ParentGoalSnapshot {
                    goal_snapshot_ref: Some(format!(
                        "app-server://threads/{thread_id}/goals/current"
                    )),
                    budget_state_ref: Some(format!(
                        "app-server://threads/{thread_id}/budget/current"
                    )),
                    goal_status: Some(ThreadGoalStatus::Active),
                    token_budget: Some(20_000),
                    tokens_used: Some(3_800),
                    time_used_seconds: Some(71),
                    evidence_refs: vec![
                        format!("app-server://threads/{thread_id}/goals/current"),
                        format!("app-server://threads/{thread_id}/budget/current"),
                    ],
                    degraded_reason: None,
                }
            })
        }
    }

    #[derive(Debug)]
    struct FakeParentTurnResponseAdapter;

    impl ParentTurnResponseAdapter for FakeParentTurnResponseAdapter {
        fn read_response<'a>(
            &'a self,
            thread_id: &'a str,
            turn_id: &'a str,
        ) -> ParentTurnResponseFuture<'a> {
            Box::pin(async move {
                ParentTurnResponse {
                    status: Some(TurnStatus::Completed),
                    request_item_ref: Some(format!(
                        "app-server://threads/{thread_id}/turns/{turn_id}/items/user-message"
                    )),
                    request_text: Some(format!(
                        "Pedido conversacional OOTB para {thread_id} en {turn_id}."
                    )),
                    item_ref: Some(format!(
                        "app-server://threads/{thread_id}/turns/{turn_id}/items/final-agent-message"
                    )),
                    text: Some(format!(
                        "Cierre conversacional OOTB de {thread_id} para {turn_id}."
                    )),
                }
            })
        }
    }

    #[derive(Debug, Default)]
    struct TransientInterruptedParentTurnResponseAdapter {
        reads: AtomicUsize,
    }

    impl ParentTurnResponseAdapter for TransientInterruptedParentTurnResponseAdapter {
        fn read_response<'a>(
            &'a self,
            thread_id: &'a str,
            turn_id: &'a str,
        ) -> ParentTurnResponseFuture<'a> {
            let read = self.reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if read == 0 {
                    return ParentTurnResponse {
                        status: Some(TurnStatus::Interrupted),
                        request_item_ref: None,
                        request_text: None,
                        item_ref: None,
                        text: None,
                    };
                }
                ParentTurnResponse {
                    status: Some(TurnStatus::Completed),
                    request_item_ref: Some(format!(
                        "app-server://threads/{thread_id}/turns/{turn_id}/items/user-message"
                    )),
                    request_text: Some(format!(
                        "Pedido conversacional OOTB para {thread_id} en {turn_id}."
                    )),
                    item_ref: Some(format!(
                        "app-server://threads/{thread_id}/turns/{turn_id}/items/final-agent-message"
                    )),
                    text: Some(format!(
                        "Cierre conversacional OOTB de {thread_id} para {turn_id}."
                    )),
                }
            })
        }
    }

    #[derive(Debug)]
    struct FakeThreadConsolidationAdapter;

    impl ThreadConsolidationAdapter for FakeThreadConsolidationAdapter {
        fn consolidate_threads<'a>(
            &'a self,
            params: &'a MemythosThreadConsolidateParams,
        ) -> ThreadConsolidationFuture<'a> {
            Box::pin(async move {
                let source_refs = params
                    .source_thread_ids
                    .iter()
                    .map(|thread_id| MemythosThreadConsolidationSourceRef {
                        thread_id: thread_id.clone(),
                        turn_refs: vec![format!("app-server://threads/{thread_id}/turns/turn-1")],
                        items_view: "summary".to_string(),
                        cursor: params.since_cursors.get(thread_id).cloned(),
                        next_cursor: Some(format!("cursor-after-{thread_id}")),
                        latest_agent_message_ref: Some(format!(
                            "app-server://threads/{thread_id}/turns/turn-1/items/msg-1"
                        )),
                        latest_agent_message_text: Some(format!(
                            "{thread_id} propone avanzar con una definicion concreta."
                        )),
                        technical_evidence_refs: vec![format!(
                            "app-server://threads/{thread_id}/turns/turn-1/items/collab-1"
                        )],
                    })
                    .collect::<Vec<_>>();
                ThreadConsolidationAttempt {
                    consolidation_turn_id: Some("turn-consolidation-001".to_string()),
                    source_refs,
                    agent_message_ref: Some(
                        "app-server://threads/thread_concierge/turns/turn-consolidation-001/items/msg-1"
                            .to_string(),
                    ),
                    structured_output_ref: Some(
                        "app-server://threads/thread_concierge/turns/turn-consolidation-001/output-schema"
                            .to_string(),
                    ),
                    technical_evidence_refs: vec![
                        "app-server://threads/thread_a/turns/turn-1/items/collab-1".to_string(),
                    ],
                    source_method: "thread/turns/list".to_string(),
                    used_thread_turns_summary: true,
                    blockers: Vec::new(),
                }
            })
        }
    }

    fn room_register_params() -> MemythosRoomRegisterParams {
        MemythosRoomRegisterParams {
            room_id: "room-001".to_string(),
            case_id: "case-001".to_string(),
            layer_id: "bpm_e2e".to_string(),
            arena_id: "arena-room-001".to_string(),
            topology: "cross_parent_room".to_string(),
            participants: vec![
                MemythosRoomParticipant {
                    parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                    thread_id: "thread_growth".to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: "growth".to_string(),
                    goal_ref: Some("app-server://threads/thread_growth/goals/current".to_string()),
                    authority_scope: vec!["peer_debate".to_string()],
                },
                MemythosRoomParticipant {
                    parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                    thread_id: "thread_risk".to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: "risk".to_string(),
                    goal_ref: Some("app-server://threads/thread_risk/goals/current".to_string()),
                    authority_scope: vec!["peer_debate".to_string()],
                },
            ],
        }
    }

    fn room_register_params_with_concierge() -> MemythosRoomRegisterParams {
        let mut params = room_register_params();
        params.participants.push(MemythosRoomParticipant {
            parent_key: "case/bpm_e2e/arena/room_concierge".to_string(),
            thread_id: "thread_concierge".to_string(),
            parent_role: "room_concierge".to_string(),
            stance_profile: "coordination".to_string(),
            goal_ref: Some("app-server://threads/thread_concierge/goals/current".to_string()),
            authority_scope: vec!["room_coordination".to_string()],
        });
        params
    }

    fn second_room_register_params_with_concierge() -> MemythosRoomRegisterParams {
        MemythosRoomRegisterParams {
            room_id: "room-002".to_string(),
            case_id: "case-001".to_string(),
            layer_id: "tactical_operational".to_string(),
            arena_id: "arena-room-002".to_string(),
            topology: "cross_parent_room".to_string(),
            participants: vec![
                MemythosRoomParticipant {
                    parent_key: "case/tactical/arena/room_concierge".to_string(),
                    thread_id: "thread_tactical_concierge".to_string(),
                    parent_role: "room_concierge".to_string(),
                    stance_profile: "coordination".to_string(),
                    goal_ref: Some(
                        "app-server://threads/thread_tactical_concierge/goals/current".to_string(),
                    ),
                    authority_scope: vec!["room_coordination".to_string()],
                },
                MemythosRoomParticipant {
                    parent_key: "case/tactical/arena/bettor/design".to_string(),
                    thread_id: "thread_tactical_bettor".to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: "customer_flow".to_string(),
                    goal_ref: Some(
                        "app-server://threads/thread_tactical_bettor/goals/current".to_string(),
                    ),
                    authority_scope: vec!["peer_debate".to_string()],
                },
            ],
        }
    }

    #[tokio::test]
    async fn room_registration_rejects_duplicate_threads() {
        let processor = MemythosRequestProcessor::new();
        let mut params = room_register_params();
        params.participants[1].thread_id = params.participants[0].thread_id.clone();

        let error = processor.room_register(params).await.unwrap_err();

        assert!(
            error.message.contains("duplicate room participant thread"),
            "unexpected error: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn room_registration_rejects_legacy_observer_concierge_encoding() {
        let processor = MemythosRequestProcessor::new();
        let mut params = room_register_params();
        params.participants.push(MemythosRoomParticipant {
            parent_key: "case/bpm_e2e/arena/observer/room_concierge".to_string(),
            thread_id: "thread_concierge".to_string(),
            parent_role: "observer".to_string(),
            stance_profile: "room_concierge".to_string(),
            goal_ref: Some("app-server://threads/thread_concierge/goals/current".to_string()),
            authority_scope: vec!["room_coordination".to_string()],
        });

        let error = processor.room_register(params).await.unwrap_err();

        assert!(
            error
                .message
                .contains("legacy observer + room_concierge encoding"),
            "unexpected error: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn room_list_returns_registered_rooms_from_native_state() {
        let processor = MemythosRequestProcessor::new();
        processor
            .room_register(room_register_params())
            .await
            .unwrap();
        let mut second_room = room_register_params();
        second_room.room_id = "room-002".to_string();
        second_room.case_id = "case-002".to_string();
        processor.room_register(second_room).await.unwrap();

        let response = processor
            .room_list(MemythosRoomListParams {
                case_id: Some("case-001".to_string()),
                layer_id: None,
                arena_id: None,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomList(response) = response else {
            panic!("expected MemythosRoomList response");
        };

        assert_eq!(response.source_method, "memythos/room/list");
        assert_eq!(response.rooms.len(), 1);
        assert_eq!(response.rooms[0].room_id, "room-001");
        assert_eq!(response.rooms[0].participants.len(), 2);
    }

    #[tokio::test]
    async fn room_tool_lists_registered_parent_threads() {
        let processor = MemythosRequestProcessor::new();
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();

        let participants = processor
            .room_tool_list_participants("thread_growth")
            .await
            .unwrap();

        assert_eq!(participants.len(), 3);
        assert_eq!(participants[0].parent_role, "bettor");
        assert!(participants[0].is_current_parent);
        assert_eq!(participants[2].parent_role, "room_concierge");
    }

    #[tokio::test]
    async fn room_tool_delegates_and_resumes_the_same_cross_room_concierge_thread() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(FakeParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();
        processor
            .room_register(second_room_register_params_with_concierge())
            .await
            .unwrap();

        let rooms = processor
            .room_tool_list_rooms("thread_concierge")
            .await
            .unwrap();
        assert_eq!(rooms.len(), 2);
        assert_eq!(rooms.iter().filter(|room| room.is_current_room).count(), 1);

        let delegate = processor
            .room_tool_send_to_room(
                "thread_concierge",
                MemythosRoomToolSendToRoomArgs {
                    target_room_id: "room-002".to_string(),
                    message: "Produce una bajada tactica y devuelve un rollup.".to_string(),
                    authority: "peer".to_string(),
                    message_kind: "delegate_to_tactical".to_string(),
                    response_contract: "Devuelve un unico rollup.".to_string(),
                },
            )
            .await
            .unwrap();
        let resume = processor
            .room_tool_send_to_room(
                "thread_concierge",
                MemythosRoomToolSendToRoomArgs {
                    target_room_id: "room-002".to_string(),
                    message: "Reanuda con esta definicion resuelta.".to_string(),
                    authority: "peer".to_string(),
                    message_kind: "resume_tactical_after_rollup".to_string(),
                    response_contract: "Devuelve cierre tactico final.".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(delegate.target_thread_id, "thread_tactical_concierge");
        assert_eq!(resume.target_thread_id, delegate.target_thread_id);
        assert!(delegate.response_text.contains("thread_tactical_concierge"));
        assert!(resume.response_text.contains("thread_tactical_concierge"));
        let state = processor.state.lock().await;
        let cross_room_deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.receiver_thread_id == "thread_tactical_concierge")
            .collect::<Vec<_>>();
        assert_eq!(cross_room_deliveries.len(), 2);
        assert_eq!(
            cross_room_deliveries[0].phase.as_deref(),
            Some("delegate_to_tactical")
        );
        assert_eq!(
            cross_room_deliveries[1].phase.as_deref(),
            Some("resume_tactical_after_rollup")
        );
    }

    #[tokio::test]
    async fn room_tool_reconciles_parent_completion_that_precedes_delivery_registration() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(FakeParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();

        let response = processor
            .room_tool_send_message(
                "thread_concierge",
                MemythosRoomToolSendMessageArgs {
                    target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                    message: "Evalua esta alternativa desde riesgo.".to_string(),
                    authority: "peer".to_string(),
                    message_kind: "consultation".to_string(),
                    response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(response.target_thread_id, "thread_risk");
        assert!(
            response
                .response_text
                .contains("Cierre conversacional OOTB de thread_risk")
        );
        let state = processor.state.lock().await;
        let delivery = state
            .arena_message_deliveries
            .last()
            .expect("room tool should retain its native delivery");
        assert_eq!(delivery.status, "receiver_turn_completed");
        assert!(
            delivery
                .receiver_response_event_ref
                .as_deref()
                .is_some_and(|value| value.ends_with("/items/final-agent-message"))
        );
    }

    #[tokio::test]
    async fn room_tool_ignores_transient_interrupted_status_before_native_turn_starts() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(TransientInterruptedParentTurnResponseAdapter::default()),
        );
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();

        let response = processor
            .room_tool_send_message(
                "thread_concierge",
                MemythosRoomToolSendMessageArgs {
                    target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                    message: "Evalua esta alternativa desde riesgo.".to_string(),
                    authority: "peer".to_string(),
                    message_kind: "consultation".to_string(),
                    response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(response.target_thread_id, "thread_risk");
        assert!(
            response
                .response_text
                .contains("Cierre conversacional OOTB de thread_risk")
        );
    }

    #[tokio::test]
    async fn room_tool_inherits_round_and_phase_from_parent_inbound_delivery() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(FakeParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();
        {
            let mut state = processor.state.lock().await;
            state
                .arena_message_deliveries
                .push(MemythosArenaMessageDelivery {
                    delivery_id: "human-intake-delivery".to_string(),
                    message_id: "human-intake-message".to_string(),
                    human_summary: "Evalua el pedido humano de la arena.".to_string(),
                    status: "receiver_turn_completed".to_string(),
                    sender_thread_id: "human".to_string(),
                    receiver_thread_id: "thread_concierge".to_string(),
                    arena_id: "arena-room-001".to_string(),
                    round_id: "intake-001".to_string(),
                    phase: Some("human_intake".to_string()),
                    delivery_mechanism: "room_loopback_send_input".to_string(),
                    receiver_turn_id: Some("human-intake-turn".to_string()),
                    receiver_response_event_ref: None,
                    delivered_as_human_instruction: true,
                    memory_replay_required: false,
                    event_refs: Vec::new(),
                    rejection_reason: None,
                });
        }

        processor
            .room_tool_send_message(
                "thread_concierge",
                MemythosRoomToolSendMessageArgs {
                    target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                    message: "Evalua esta alternativa desde riesgo.".to_string(),
                    authority: "peer".to_string(),
                    message_kind: "consultation".to_string(),
                    response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                },
            )
            .await
            .unwrap();

        let state = processor.state.lock().await;
        let delivery = state
            .arena_message_deliveries
            .last()
            .expect("room tool should retain its native delivery");
        assert_eq!(delivery.round_id, "intake-001");
        assert_eq!(delivery.phase.as_deref(), Some("human_intake"));
    }

    #[tokio::test]
    async fn room_tool_routes_peer_to_concierge_and_returns_native_closure() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(FakeParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();

        let running = {
            let processor = processor.clone();
            tokio::spawn(async move {
                processor
                    .room_tool_send_message(
                        "thread_growth",
                        MemythosRoomToolSendMessageArgs {
                            target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                            message: "Necesito que el room coordine esta objecion.".to_string(),
                            authority: "peer".to_string(),
                            message_kind: "objection".to_string(),
                            response_contract: "Devuelve la decision coordinada.".to_string(),
                        },
                    )
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        let target_turn_id = {
            let state = processor.state.lock().await;
            let delivery = state
                .arena_message_deliveries
                .last()
                .expect("room tool should create a delivery");
            assert_eq!(delivery.sender_thread_id, "thread_growth");
            assert_eq!(delivery.receiver_thread_id, "thread_concierge");
            delivery
                .receiver_turn_id
                .clone()
                .expect("delivery should have a native turn")
        };
        assert!(
            processor
                .record_native_turn_completed(
                    "thread_concierge",
                    &target_turn_id,
                    "completed",
                    Some(1_000),
                    Some(250),
                )
                .await
        );

        let response = running.await.unwrap().unwrap();
        assert_eq!(
            response.target_parent_key,
            "case/bpm_e2e/arena/room_concierge"
        );
        assert_eq!(response.target_thread_id, "thread_concierge");
        assert_eq!(response.target_turn_id, target_turn_id);
        assert!(
            response
                .response_text
                .contains("Cierre conversacional OOTB de thread_concierge")
        );
    }

    #[tokio::test]
    async fn room_tool_routes_concierge_to_selected_parent() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(FakeParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params_with_concierge())
            .await
            .unwrap();

        let running = {
            let processor = processor.clone();
            tokio::spawn(async move {
                processor
                    .room_tool_send_message(
                        "thread_concierge",
                        MemythosRoomToolSendMessageArgs {
                            target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                            message: "Evalua esta alternativa desde riesgo.".to_string(),
                            authority: "peer".to_string(),
                            message_kind: "consultation".to_string(),
                            response_contract: "Devuelve tesis, limites y proximo paso."
                                .to_string(),
                        },
                    )
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        let target_turn_id = {
            let state = processor.state.lock().await;
            let delivery = state
                .arena_message_deliveries
                .last()
                .expect("room tool should create a delivery");
            assert_eq!(delivery.sender_thread_id, "thread_concierge");
            assert_eq!(delivery.receiver_thread_id, "thread_risk");
            delivery
                .receiver_turn_id
                .clone()
                .expect("delivery should have a native turn")
        };
        assert!(
            processor
                .record_native_turn_completed(
                    "thread_risk",
                    &target_turn_id,
                    "completed",
                    Some(1_000),
                    Some(250),
                )
                .await
        );

        let response = running.await.unwrap().unwrap();
        assert_eq!(response.target_parent_key, "case/bpm_e2e/arena/bettor/risk");
        assert_eq!(response.target_thread_id, "thread_risk");
        assert!(
            response
                .response_text
                .contains("Cierre conversacional OOTB de thread_risk")
        );
    }

    #[tokio::test]
    async fn thread_consolidate_rejects_empty_sources() {
        let processor = MemythosRequestProcessor::new();
        let error = processor
            .thread_consolidate(MemythosThreadConsolidateParams {
                coordinator_thread_id: "thread_concierge".to_string(),
                source_thread_ids: Vec::new(),
                since_cursors: HashMap::new(),
                items_view: Some("summary".to_string()),
                purpose:
                    codex_app_server_protocol::MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
                authority_mode:
                    codex_app_server_protocol::MemythosThreadConsolidationAuthorityMode::PeerCoordination,
                instructions: "Consolidate.".to_string(),
                per_source_limit: Some(2),
                client_user_message_id: Some("consolidation-001".to_string()),
                output_schema: None,
            })
            .await
            .unwrap_err();

        assert!(
            error
                .message
                .contains("sourceThreadIds must contain at least one thread"),
            "unexpected error: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn thread_consolidate_uses_native_summary_and_coordinator_turn() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(FakeThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );

        let response = processor
            .thread_consolidate(MemythosThreadConsolidateParams {
                coordinator_thread_id: "thread_concierge".to_string(),
                source_thread_ids: vec!["thread_a".to_string(), "thread_b".to_string()],
                since_cursors: HashMap::from([(
                    "thread_a".to_string(),
                    "cursor-a".to_string(),
                )]),
                items_view: Some("summary".to_string()),
                purpose:
                    codex_app_server_protocol::MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
                authority_mode:
                    codex_app_server_protocol::MemythosThreadConsolidationAuthorityMode::PeerCoordination,
                instructions: "Consolidate the round for the judge.".to_string(),
                per_source_limit: Some(2),
                client_user_message_id: Some("consolidation-001".to_string()),
                output_schema: Some(serde_json::json!({"type": "object"})),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosThreadConsolidate(response) = response else {
            panic!("expected MemythosThreadConsolidate response");
        };

        assert_eq!(response.consolidation_turn_id, "turn-consolidation-001");
        assert_eq!(response.coordinator_thread_id, "thread_concierge");
        assert_eq!(response.source_method, "thread/turns/list");
        assert!(response.used_thread_turns_summary);
        assert!(response.blockers.is_empty());
        assert_eq!(response.source_refs.len(), 2);
        assert_eq!(response.source_refs[0].cursor.as_deref(), Some("cursor-a"));
        assert!(response.agent_message_ref.is_some());
        assert!(response.structured_output_ref.is_some());
        assert_eq!(response.technical_evidence_refs.len(), 1);

        let telemetry = processor
            .telemetry_list(MemythosTelemetryListParams {
                layer_id: None,
                arena_id: None,
                thread_id: Some("thread_concierge".to_string()),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosTelemetryList(telemetry) = telemetry else {
            panic!("expected MemythosTelemetryList response");
        };
        assert!(telemetry.telemetry_refs.iter().any(|telemetry_ref| {
            telemetry_ref.kind == MemythosTelemetryRefKind::ThreadConsolidation
                && telemetry_ref.source == MemythosTelemetrySource::AppServerNative
        }));
    }

    #[tokio::test]
    async fn thread_contract_assemble_registers_native_contract_ref() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(FakeThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );

        let response = processor
            .thread_contract_assemble(MemythosThreadContractAssembleParams {
                coordinator_thread_id: "thread_concierge".to_string(),
                source_thread_ids: vec!["thread_a".to_string(), "thread_b".to_string()],
                since_cursors: HashMap::new(),
                items_view: Some("summary".to_string()),
                contract_kind: "resume_contract".to_string(),
                instructions: "Assemble a resume contract from closed evidence.".to_string(),
                per_source_limit: Some(2),
                client_user_message_id: Some("contract-001".to_string()),
                output_schema: Some(serde_json::json!({"type": "object"})),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosThreadContractAssemble(response) = response else {
            panic!("expected MemythosThreadContractAssemble response");
        };

        assert_eq!(response.contract.contract_kind, "resume_contract");
        assert_eq!(
            response.contract.storage_kind,
            "app_server_native_contract_message"
        );
        assert!(response.contract.contract_ref.contains("/contracts/"));
        assert_eq!(
            response.contract.producer_turn_id,
            "artifact-mem_contract_1"
        );
        assert!(response.contract.missing_evidence.is_empty());
        assert!(response.contract.blockers.is_empty());
        assert!(response.contract.payload.is_some());
        assert!(response.agent_message_ref.is_none());
        assert!(response.structured_output_ref.is_some());
        assert_eq!(response.source_refs.len(), 2);

        let read = processor
            .thread_contract_read(MemythosThreadContractReadParams {
                contract_ref: response.contract.contract_ref.clone(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosThreadContractRead(read) = read else {
            panic!("expected MemythosThreadContractRead response");
        };
        assert_eq!(read.contract.contract_ref, response.contract.contract_ref);

        let list = processor
            .thread_contract_list(MemythosThreadContractListParams {
                thread_id: Some("thread_concierge".to_string()),
                contract_kind: Some("resume_contract".to_string()),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosThreadContractList(list) = list else {
            panic!("expected MemythosThreadContractList response");
        };
        assert_eq!(list.contracts.len(), 1);
        assert_eq!(list.contracts[0].contract_kind, "resume_contract");
    }

    #[tokio::test]
    async fn thread_contract_assemble_rejects_missing_output_schema() {
        let processor = MemythosRequestProcessor::new();
        let error = processor
            .thread_contract_assemble(MemythosThreadContractAssembleParams {
                coordinator_thread_id: "thread_concierge".to_string(),
                source_thread_ids: vec!["thread_a".to_string()],
                since_cursors: HashMap::new(),
                items_view: Some("summary".to_string()),
                contract_kind: "resume_contract".to_string(),
                instructions: "Assemble.".to_string(),
                per_source_limit: Some(2),
                client_user_message_id: Some("contract-001".to_string()),
                output_schema: None,
            })
            .await
            .unwrap_err();

        assert!(
            error.message.contains("outputSchema is required"),
            "unexpected error: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn room_send_input_uses_native_loopback_delivery() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        let register_response = processor
            .room_register(room_register_params())
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomRegister(register_response) = register_response
        else {
            panic!("expected MemythosRoomRegister response");
        };
        assert_eq!(register_response.event_refs.len(), 1);

        let send_response = processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "artifact://room/messages/message-001.json".to_string(),
                delivery_ref: "artifact://room/deliveries/delivery-001.json".to_string(),
                from_parent_thread_id: Some("thread_growth".to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_risk".to_string(),
                source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_objection".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                client_user_message_id: Some("message-001".to_string()),
                human_summary: "Challenge my proposal as an arena peer.".to_string(),
                prompt: "I am not a human; I am an arena peer. Challenge my proposal.".to_string(),
                metadata: serde_json::Map::from_iter([
                    (
                        "memythos_round_id".to_string(),
                        serde_json::Value::String("round-001".to_string()),
                    ),
                    (
                        "memythos_phase".to_string(),
                        serde_json::Value::String("bet".to_string()),
                    ),
                ]),
                output_schema: None,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
            panic!("expected MemythosRoomSendInput response");
        };

        assert_eq!(
            send_response.delivery.delivery_mechanism,
            "room_loopback_send_input"
        );
        assert!(!send_response.delivery.human_instruction);
        assert_eq!(send_response.delivery.thread_id, "thread_risk");
        assert_eq!(
            send_response.delivery.turn_id,
            "turn_for_thread_risk_message-001"
        );
        assert!(send_response.delivery.event_refs.iter().any(|event_ref| {
            event_ref == "app-server://rooms/room-001/messages/message-001/delivered"
        }));
    }

    #[tokio::test]
    async fn room_send_input_prefers_concierge_source_when_present() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        let mut params = room_register_params();
        params.participants.push(MemythosRoomParticipant {
            parent_key: "case/bpm_e2e/arena/room_concierge/coordination".to_string(),
            thread_id: "thread_concierge".to_string(),
            parent_role: "room_concierge".to_string(),
            stance_profile: "coordination".to_string(),
            goal_ref: Some("app-server://threads/thread_concierge/goals/current".to_string()),
            authority_scope: vec!["room_coordination".to_string()],
        });
        processor.room_register(params).await.unwrap();

        let send_response = processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "artifact://room/messages/message-002.json".to_string(),
                delivery_ref: "artifact://room/deliveries/delivery-002.json".to_string(),
                from_parent_thread_id: Some("thread_growth".to_string()),
                via_concierge_thread_id: Some("thread_concierge".to_string()),
                to_parent_thread_id: "thread_risk".to_string(),
                source_parent_key: "case/bpm_e2e/arena/room_concierge/coordination".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_proposal".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                client_user_message_id: Some("message-002".to_string()),
                human_summary: "The concierge delivers a message between peers.".to_string(),
                prompt: "The concierge delivers a message between peers.".to_string(),
                metadata: serde_json::Map::from_iter([
                    (
                        "memythos_round_id".to_string(),
                        serde_json::Value::String("round-001".to_string()),
                    ),
                    (
                        "memythos_phase".to_string(),
                        serde_json::Value::String("bet".to_string()),
                    ),
                ]),
                output_schema: None,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
            panic!("expected MemythosRoomSendInput response");
        };

        assert_eq!(send_response.delivery.thread_id, "thread_risk");
        assert_eq!(
            send_response.delivery.delivery_mechanism,
            "room_loopback_send_input"
        );
        let timeline = processor
            .room_timeline_get(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: Some("bet".to_string()),
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(timeline) = timeline else {
            panic!("expected MemythosRoomActivityList response");
        };
        let delivered = timeline
            .events
            .iter()
            .find(|event| event.event_kind == "input_delivered")
            .expect("expected delivered input event");
        assert_eq!(
            delivered.sender.role,
            Some(MemythosParentRole::RoomConcierge)
        );
        assert_eq!(
            delivered.sender.stance,
            Some(MemythosParentStance::Coordination)
        );
        assert_eq!(delivered.recipient.role, Some(MemythosParentRole::Bettor));
        assert_eq!(delivered.authority, "peer_debate");
        assert_eq!(
            delivered.prompt_origin,
            MemythosPromptOrigin::AgentToAgentPrompt
        );
        assert_eq!(delivered.prompt_lineage.len(), 1);
    }

    #[tokio::test]
    async fn room_send_input_accepts_human_intake_without_registered_source_parent() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params())
            .await
            .unwrap();

        let send_response = processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "app-server://rooms/room-001/messages/human-intake-001"
                    .to_string(),
                delivery_ref: "app-server://rooms/room-001/deliveries/human-intake-001".to_string(),
                from_parent_thread_id: None,
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_growth".to_string(),
                source_parent_key: "human".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                message_kind: "initial_human_request".to_string(),
                message_authority: "human_intake".to_string(),
                human_instruction: true,
                response_contract: "respond conversationally and ask for missing context if needed"
                    .to_string(),
                client_user_message_id: Some("human-intake-001".to_string()),
                human_summary:
                    "Assess whether the BPM node can become a tactical PID without reopening business decisions."
                        .to_string(),
                prompt:
                    "Assess whether the BPM node can become a tactical PID without reopening business decisions."
                        .to_string(),
                metadata: serde_json::Map::from_iter([
                    (
                        "memythos_round_id".to_string(),
                        serde_json::Value::String("round-human-001".to_string()),
                    ),
                    (
                        "memythos_phase".to_string(),
                        serde_json::Value::String("intake".to_string()),
                    ),
                ]),
                output_schema: None,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
            panic!("expected MemythosRoomSendInput response");
        };
        assert!(send_response.delivery.human_instruction);
        assert_eq!(send_response.delivery.thread_id, "thread_growth");

        let timeline = processor
            .room_timeline_get(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-human-001".to_string()),
                phase: Some("intake".to_string()),
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(timeline) = timeline else {
            panic!("expected MemythosRoomActivityList response");
        };
        let delivered = timeline
            .events
            .iter()
            .find(|event| event.event_kind == "human_intake_delivered")
            .expect("expected human intake delivery event");
        assert_eq!(delivered.sender.kind, MemythosRoomActorKind::Human);
        assert_eq!(delivered.sender.label.as_deref(), Some("human"));
        assert_eq!(delivered.recipient.role, Some(MemythosParentRole::Bettor));
        assert_eq!(delivered.authority, "human_intake");
        assert_eq!(
            delivered.prompt_origin,
            MemythosPromptOrigin::HumanPromptInjection
        );
        assert_eq!(delivered.prompt_lineage.len(), 1);
        assert_eq!(
            delivered.prompt_lineage[0].origin,
            MemythosPromptOrigin::HumanPromptInjection
        );
    }

    #[tokio::test]
    async fn room_send_reports_final_native_delivery_mechanism() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params())
            .await
            .unwrap();
        let response = processor
            .room_send(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "app-server://rooms/room-001/messages/message-003".to_string(),
                delivery_ref: "app-server://rooms/room-001/deliveries/delivery-003".to_string(),
                from_parent_thread_id: Some("thread_growth".to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_risk".to_string(),
                source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_objection".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                client_user_message_id: Some("message-003".to_string()),
                human_summary: "Challenge my proposal as an arena peer.".to_string(),
                prompt: "I am not a human; I am an arena peer. Challenge my proposal.".to_string(),
                metadata: serde_json::Map::new(),
                output_schema: None,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomSendInput(response) = response else {
            panic!("expected MemythosRoomSendInput response");
        };

        assert_eq!(response.delivery.delivery_mechanism, "room_loopback_send");
    }

    #[tokio::test]
    async fn room_activity_list_returns_compact_initial_view() {
        let processor = MemythosRequestProcessor::new();
        processor
            .room_register(room_register_params())
            .await
            .unwrap();

        let response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: None,
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
            panic!("expected MemythosRoomActivityList response");
        };

        assert_eq!(response.source_method, "memythos/room/activity/list");
        assert_eq!(response.participants.len(), 2);
        assert!(response.turns.is_empty());
        assert!(response.events.is_empty());
        assert_eq!(response.lifecycle.room_state, "round_closed");
        assert!(response.lifecycle.clean_close);
        assert_eq!(response.collab.send_input_count, 0);
    }

    #[tokio::test]
    async fn room_timeline_get_reports_final_native_source_method() {
        let processor = MemythosRequestProcessor::new();
        processor
            .room_register(room_register_params())
            .await
            .unwrap();

        let response = processor
            .room_timeline_get(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: None,
                phase: None,
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
            panic!("expected MemythosRoomActivityList response");
        };

        assert_eq!(response.source_method, "memythos/room/timeline/get");
    }

    #[tokio::test]
    async fn room_activity_list_exposes_native_registration_cursor_events() {
        let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
            AppServerRpcTransport::InProcess,
            Arc::new(RecordOnlyPeerParentDeliveryAdapter),
            Arc::new(RecordOnlyParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
            Arc::new(FakeParentConfigurationAdapter),
        );
        processor
            .room_register(room_register_params())
            .await
            .unwrap();

        let response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: None,
                phase: None,
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
            panic!("expected MemythosRoomActivityList response");
        };

        assert_eq!(response.source_method, "memythos/room/activity/list");
        assert_eq!(response.events.len(), 3);
        assert_eq!(response.events[0].event_kind, "room_registered");
        assert_eq!(response.events[0].sequence, 1);
        assert_eq!(
            response.events[0].sender.kind,
            MemythosRoomActorKind::AppServer
        );
        assert_eq!(
            response.events[0].recipient.role,
            Some(MemythosParentRole::RoomConcierge)
        );
        assert_eq!(
            response.events[0].prompt_origin,
            MemythosPromptOrigin::MemythosRuntimeSetup
        );
        assert_eq!(response.events[1].event_kind, "participant_attached");
        assert_eq!(response.events[1].sequence, 2);
        assert_eq!(
            response.events[1].recipient.role,
            Some(MemythosParentRole::Bettor)
        );
        assert_eq!(
            response.events[1].recipient.stance,
            Some(MemythosParentStance::Growth)
        );
        assert_eq!(response.events[1].prompt_lineage.len(), 1);
        assert_eq!(response.events[2].event_kind, "participant_attached");
        assert_eq!(response.events[2].sequence, 3);
        assert_eq!(response.next_cursor, response.cursor);
        assert!(!response.has_more);

        let configuration = processor
            .room_parent_configuration_list(MemythosRoomParentConfigurationListParams {
                room_id: "room-001".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomParentConfigurationList(configuration) =
            configuration
        else {
            panic!("expected MemythosRoomParentConfigurationList response");
        };
        assert_eq!(
            configuration.source_method,
            "memythos/room/parent-configuration/list"
        );
        assert_eq!(configuration.configurations.len(), 2);
        assert_eq!(
            configuration.configurations[0].registered_role,
            MemythosParentRole::Bettor
        );
        assert_eq!(
            configuration.configurations[0].stance,
            MemythosParentStance::Growth
        );
        assert_eq!(
            configuration.configurations[0]
                .effective_agent_role
                .as_deref(),
            Some("native_role_for_thread_growth")
        );
        assert_eq!(
            configuration.configurations[0].personality.as_deref(),
            Some("pragmatic")
        );
        assert!(configuration.configurations[0].blockers.is_empty());
        assert!(configuration.blockers.is_empty());
    }

    #[tokio::test]
    async fn room_activity_list_summarizes_delivery_completion_and_usage() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(FakeParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params())
            .await
            .unwrap();
        processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "artifact://room/messages/message-003.json".to_string(),
                delivery_ref: "artifact://room/deliveries/delivery-003.json".to_string(),
                from_parent_thread_id: Some("thread_growth".to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_risk".to_string(),
                source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_bet".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                client_user_message_id: Some("message-003".to_string()),
                human_summary: "Place a bet and state execution conditions.".to_string(),
                prompt: "Place a bet and state execution conditions.".to_string(),
                metadata: serde_json::Map::from_iter([
                    (
                        "memythos_round_id".to_string(),
                        serde_json::Value::String("round-001".to_string()),
                    ),
                    (
                        "memythos_phase".to_string(),
                        serde_json::Value::String("bet".to_string()),
                    ),
                ]),
                output_schema: None,
            })
            .await
            .unwrap();
        assert!(
            processor
                .record_native_turn_completed(
                    "thread_risk",
                    "turn_for_thread_risk_message-003",
                    "completed",
                    Some(1_000),
                    Some(250)
                )
                .await
        );
        assert!(
            processor
                .record_native_token_usage("thread_risk", "turn_for_thread_risk_message-003")
                .await
        );

        let response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: None,
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
            panic!("expected MemythosRoomActivityList response");
        };

        assert_eq!(response.turns.len(), 1);
        assert_eq!(response.turns[0].status, "completed");
        assert_eq!(
            response.turns[0].parent_key,
            "case/bpm_e2e/arena/bettor/risk"
        );
        assert_eq!(response.turns[0].round_id.as_deref(), Some("round-001"));
        assert_eq!(response.turns[0].phase.as_deref(), Some("bet"));
        assert_eq!(response.lifecycle.completed_turns, 1);
        assert_eq!(response.lifecycle.failed_turns, 0);
        assert!(response.lifecycle.clean_close);
        assert_eq!(response.collab.completed_send_input_count, 1);
        assert_eq!(response.usage.token_usage_events, 1);
        assert!(response.blockers.is_empty());
        assert_eq!(response.turns[0].items.len(), 3);
        let delivery_item = &response.turns[0].items[0];
        assert_eq!(delivery_item.kind, "collab_call");
        assert!(delivery_item.text.is_none());
        assert!(delivery_item.human_highlight.is_none());
        let request_item = &response.turns[0].items[1];
        assert_eq!(request_item.kind, "user_message");
        assert_eq!(request_item.item_type.as_deref(), Some("userMessage"));
        assert_eq!(
            request_item.text.as_deref(),
            Some("Place a bet and state execution conditions.")
        );
        assert_eq!(request_item.human_highlight, request_item.text);
        assert!(
            !request_item
                .human_highlight
                .as_deref()
                .unwrap_or_default()
                .contains("MEMYTHOS_PEER_PARENT_MESSAGE")
        );
        assert_eq!(
            request_item.event_ref,
            "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-003/items/user-message"
        );
        let response_item = &response.turns[0].items[2];
        assert_eq!(response_item.kind, "agent_message");
        assert_eq!(response_item.item_type.as_deref(), Some("agentMessage"));
        assert_eq!(
            response_item.text.as_deref(),
            Some(
                "Cierre conversacional OOTB de thread_risk para turn_for_thread_risk_message-003."
            )
        );
        assert_eq!(response_item.human_highlight, response_item.text);
        assert_eq!(
            response_item.event_ref,
            "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-003/items/final-agent-message"
        );
        assert!(
            !response_item
                .text
                .as_deref()
                .unwrap()
                .contains(" received ")
        );
        assert!(
            delivery_item
                .refs
                .iter()
                .all(|event_ref| { !event_ref.contains("/events/") })
        );

        let dialogue = processor
            .room_dialogue_list(MemythosRoomDialogueListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: Some("bet".to_string()),
                after_cursor: None,
                limit: Some(25),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomDialogueList(dialogue) = dialogue else {
            panic!("expected MemythosRoomDialogueList response");
        };
        assert_eq!(dialogue.source_method, "memythos/room/dialogue/list");
        assert_eq!(dialogue.entries.len(), 2);
        assert!(dialogue.blockers.is_empty());
        assert_eq!(dialogue.entries[0].kind, "request");
        assert_eq!(
            dialogue.entries[0].text,
            "Place a bet and state execution conditions."
        );
        assert!(
            dialogue.entries[0]
                .source_item_ref
                .ends_with("/user-message")
        );
        assert_eq!(dialogue.entries[1].kind, "response");
        assert_eq!(
            dialogue.entries[1].text,
            "Cierre conversacional OOTB de thread_risk para turn_for_thread_risk_message-003."
        );
        assert!(
            dialogue.entries[1]
                .source_item_ref
                .ends_with("/final-agent-message")
        );
        assert!(
            dialogue
                .entries
                .iter()
                .all(|entry| !entry.text.contains("Participant "))
        );

        let bet_response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: Some("bet".to_string()),
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(bet_response) = bet_response else {
            panic!("expected MemythosRoomActivityList response");
        };
        assert_eq!(bet_response.turns.len(), 1);

        let judge_response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: Some("judge".to_string()),
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(judge_response) = judge_response else {
            panic!("expected MemythosRoomActivityList response");
        };
        assert!(judge_response.turns.is_empty());
    }

    #[tokio::test]
    async fn room_activity_list_blocks_completed_turn_without_native_agent_message() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params())
            .await
            .unwrap();
        processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "app-server://rooms/room-001/messages/message-missing"
                    .to_string(),
                delivery_ref: "app-server://rooms/room-001/deliveries/delivery-missing".to_string(),
                from_parent_thread_id: Some("thread_growth".to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_risk".to_string(),
                source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_bet".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                client_user_message_id: Some("message-missing".to_string()),
                human_summary: "Respond with your conversational close.".to_string(),
                prompt: "Respond with your conversational close.".to_string(),
                metadata: serde_json::Map::new(),
                output_schema: None,
            })
            .await
            .unwrap();
        assert!(
            processor
                .record_native_turn_completed(
                    "thread_risk",
                    "turn_for_thread_risk_message-missing",
                    "completed",
                    None,
                    None,
                )
                .await
        );

        let response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: None,
                phase: None,
                since_cursor: None,
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
            panic!("expected MemythosRoomActivityList response");
        };

        assert_eq!(response.turns.len(), 1);
        assert_eq!(response.turns[0].items.len(), 1);
        assert_eq!(response.turns[0].items[0].kind, "collab_call");
        assert!(response.turns[0].items[0].human_highlight.is_none());
        assert!(
            response
                .blockers
                .iter()
                .any(|blocker| { blocker.contains("has no readable native AgentMessage") })
        );
    }

    #[tokio::test]
    async fn room_activity_list_applies_since_cursor_as_delta_boundary() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        processor
            .room_register(room_register_params())
            .await
            .unwrap();

        for message_id in ["message-004", "message-005"] {
            processor
                .room_send_input(MemythosRoomSendInputParams {
                    room_id: "room-001".to_string(),
                    room_message_ref: format!("artifact://room/messages/{message_id}.json"),
                    delivery_ref: format!("artifact://room/deliveries/{message_id}.json"),
                    from_parent_thread_id: Some("thread_growth".to_string()),
                    via_concierge_thread_id: None,
                    to_parent_thread_id: "thread_risk".to_string(),
                    source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                    target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                    message_kind: "peer_bet".to_string(),
                    message_authority: "peer_debate".to_string(),
                    human_instruction: false,
                    response_contract: "peer_response_contract".to_string(),
                    client_user_message_id: Some(message_id.to_string()),
                    human_summary: format!("Incremental bet {message_id}."),
                    prompt: format!("Incremental bet {message_id}."),
                    metadata: serde_json::Map::from_iter([
                        (
                            "memythos_round_id".to_string(),
                            serde_json::Value::String("round-001".to_string()),
                        ),
                        (
                            "memythos_phase".to_string(),
                            serde_json::Value::String("bet".to_string()),
                        ),
                    ]),
                    output_schema: None,
                })
                .await
                .unwrap();
        }

        let initial_response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: None,
                since_cursor: None,
                after_cursor: None,
                limit: Some(1),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(initial_response) = initial_response
        else {
            panic!("expected MemythosRoomActivityList response");
        };
        assert_eq!(initial_response.turns.len(), 1);
        assert_eq!(initial_response.events.len(), 1);
        assert!(initial_response.has_more);
        assert_eq!(initial_response.returned_activity_scope, "initial");
        assert!(!initial_response.since_cursor_applied);
        let cursor = initial_response.cursor.expect("initial cursor");

        let delta_response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: None,
                since_cursor: Some(cursor.clone()),
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(delta_response) = delta_response else {
            panic!("expected MemythosRoomActivityList response");
        };

        assert!(!delta_response.events.is_empty());
        assert!(
            delta_response
                .events
                .iter()
                .all(|event| event.cursor != cursor)
        );
        assert_eq!(delta_response.returned_activity_scope, "delta");
        assert!(delta_response.since_cursor_applied);
        assert!(delta_response.blockers.is_empty());
        assert_ne!(delta_response.cursor, None);

        let stale_response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: None,
                since_cursor: Some("missing-cursor".to_string()),
                after_cursor: None,
                limit: Some(25),
                include_debug_refs: false,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRoomActivityList(stale_response) = stale_response else {
            panic!("expected MemythosRoomActivityList response");
        };
        assert!(stale_response.turns.is_empty());
        assert!(stale_response.events.is_empty());
        assert!(!stale_response.since_cursor_applied);
        assert!(
            stale_response
                .blockers
                .iter()
                .any(|blocker| blocker.contains("unknown or stale room activity cursor"))
        );
    }

    #[tokio::test]
    async fn creates_layer_and_arena_contract() {
        let processor = MemythosRequestProcessor::new();
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();

        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };

        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id.clone(),
                name: "Node contract debate".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Close the node contract.".to_string(),
                participant_ids: vec!["thread_a".to_string(), "thread_b".to_string()],
            })
            .await
            .unwrap();

        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };
        assert_eq!(arena_response.arena.layer_id, layer_response.layer.layer_id);
        assert_eq!(
            arena_response.arena.lifecycle_state,
            MemythosArenaLifecycleState::Draft
        );
    }

    #[tokio::test]
    async fn rejects_arena_for_unknown_layer() {
        let processor = MemythosRequestProcessor::new();
        let err = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: "missing".to_string(),
                name: "Invalid arena".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Should fail.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap_err();

        assert!(err.message.contains("unknown layer id"));
    }

    #[tokio::test]
    async fn reports_runtime_health_and_clean_close() {
        let processor = MemythosRequestProcessor::new();
        let health_response = processor
            .runtime_health(MemythosRuntimeHealthParams::default())
            .await
            .unwrap();

        let ClientResponsePayload::MemythosRuntimeHealth(health_response) = health_response else {
            panic!("expected MemythosRuntimeHealth response");
        };
        assert_eq!(
            health_response.lifecycle_state,
            MemythosRuntimeLifecycleState::Ready
        );
        assert_eq!(health_response.runtime_family, "app_server");
        assert_eq!(health_response.connection_mode, "stdio");
        assert_eq!(health_response.transport_owner, "app_server");
        assert_eq!(health_response.transport_id.as_deref(), Some("stdio"));
        assert!(!health_response.daemon_runtime_verified);
        assert!(
            health_response
                .capabilities
                .contains(&"memythos/thread/attach".to_string())
        );
        for capability in [
            "memythos/room/create",
            "memythos/room/list",
            "memythos/room/send",
            "memythos/room/timeline/get",
            "memythos/room/contract/emit",
            "memythos/room/contract/get",
        ] {
            assert!(
                health_response
                    .capabilities
                    .contains(&capability.to_string()),
                "missing Memythos room capability: {capability}"
            );
        }

        let close_response = processor
            .runtime_close(MemythosRuntimeCloseParams {
                force: false,
                reason: None,
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosRuntimeClose(close_response) = close_response else {
            panic!("expected MemythosRuntimeClose response");
        };
        assert!(close_response.closed_cleanly);
        assert_eq!(
            close_response.lifecycle_state,
            MemythosRuntimeLifecycleState::ClosedCleanly
        );
    }

    #[tokio::test]
    async fn reports_daemon_transport_when_runtime_is_websocket() {
        let processor =
            MemythosRequestProcessor::new_for_transport(AppServerRpcTransport::Websocket);
        let health_response = processor
            .runtime_health(MemythosRuntimeHealthParams::default())
            .await
            .unwrap();

        let ClientResponsePayload::MemythosRuntimeHealth(health_response) = health_response else {
            panic!("expected MemythosRuntimeHealth response");
        };

        assert_eq!(health_response.connection_mode, "daemon_websocket");
        assert_eq!(health_response.transport_owner, "app_server_daemon");
        assert_eq!(health_response.transport_id.as_deref(), Some("websocket"));
        assert!(health_response.daemon_runtime_verified);
    }

    #[tokio::test]
    async fn attaches_threads_and_filters_telemetry_refs() {
        let processor = MemythosRequestProcessor::new();
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id.clone(),
                name: "Node contract debate".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Close the node contract.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };

        let attach_response = processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: "thread_a".to_string(),
                role_id: Some("peer".to_string()),
                stance_id: Some("skeptic".to_string()),
                objective: Some("Challenge the implementation PID.".to_string()),
                contract_ref: Some("implementation-pid.md".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosThreadAttach(attach_response) = attach_response else {
            panic!("expected MemythosThreadAttach response");
        };
        assert_eq!(attach_response.attachment.thread_id, "thread_a");

        let list_response = processor
            .thread_list(MemythosThreadListParams {
                arena_id: arena_response.arena.arena_id.clone(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosThreadList(list_response) = list_response else {
            panic!("expected MemythosThreadList response");
        };
        assert_eq!(list_response.attachments.len(), 1);

        let telemetry_response = processor
            .telemetry_list(MemythosTelemetryListParams {
                layer_id: Some(layer_response.layer.layer_id),
                arena_id: Some(arena_response.arena.arena_id),
                thread_id: Some("thread_a".to_string()),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
        else {
            panic!("expected MemythosTelemetryList response");
        };
        assert_eq!(telemetry_response.telemetry_refs.len(), 1);
        assert_eq!(
            telemetry_response.telemetry_refs[0].kind,
            MemythosTelemetryRefKind::ThreadAttachment
        );
        assert_eq!(
            telemetry_response.telemetry_refs[0].source,
            MemythosTelemetrySource::MemythosRuntimeState
        );
        assert_eq!(telemetry_response.telemetry_refs[0].native_event_ref, None);
        assert_eq!(telemetry_response.telemetry_refs[0].detail_ref, None);
    }

    #[tokio::test]
    async fn native_telemetry_refs_are_source_tagged_and_compact() {
        let processor = MemythosRequestProcessor::new();
        let mut state = processor.state.lock().await;
        let long_summary = "tool payload ".repeat(80);

        processor.push_native_telemetry_ref_for_test(
            &mut state,
            MemythosTelemetryRefKind::RuntimeState,
            Some("mem_layer_1".to_string()),
            Some("mem_arena_1".to_string()),
            Some("thread_a".to_string()),
            "event:turn:42".to_string(),
            Some("app-server://events/turn/42".to_string()),
            MemythosEventChannel::TechnicalDetail,
            long_summary,
        );

        let telemetry_ref = state.telemetry_refs.last().expect("telemetry ref exists");
        assert_eq!(
            telemetry_ref.source,
            MemythosTelemetrySource::AppServerNative
        );
        assert_eq!(
            telemetry_ref.native_event_ref.as_deref(),
            Some("event:turn:42")
        );
        assert_eq!(
            telemetry_ref.detail_ref.as_deref(),
            Some("app-server://events/turn/42")
        );
        assert!(telemetry_ref.summary.chars().count() <= MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS);
        assert!(telemetry_ref.summary.ends_with('…'));
    }

    #[tokio::test]
    async fn records_native_thread_events_for_attached_threads_only() {
        let processor = MemythosRequestProcessor::new();
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id.clone(),
                name: "Node contract debate".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Close the node contract.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: "thread_a".to_string(),
                role_id: Some("peer".to_string()),
                stance_id: Some("skeptic".to_string()),
                objective: Some("Challenge the implementation PID.".to_string()),
                contract_ref: Some("implementation-pid.md".to_string()),
            })
            .await
            .unwrap();

        let unattached_recorded = processor
            .record_native_thread_event(
                "thread_missing",
                "thread:missing/turn:1/event:1".to_string(),
                None,
                MemythosEventChannel::TechnicalDetail,
                "Ignored unattached thread event.".to_string(),
            )
            .await;
        assert!(!unattached_recorded);

        let attached_recorded = processor
            .record_native_thread_event(
                "thread_a",
                "thread:thread_a/turn:1/event:2".to_string(),
                Some("app-server://threads/thread_a/turns/1/events/2".to_string()),
                MemythosEventChannel::HumanHighlight,
                "Thread reported a useful debate highlight.".to_string(),
            )
            .await;
        assert!(attached_recorded);

        let telemetry_response = processor
            .telemetry_list(MemythosTelemetryListParams {
                layer_id: Some(layer_response.layer.layer_id),
                arena_id: Some(arena_response.arena.arena_id),
                thread_id: Some("thread_a".to_string()),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
        else {
            panic!("expected MemythosTelemetryList response");
        };
        let native_refs: Vec<_> = telemetry_response
            .telemetry_refs
            .iter()
            .filter(|telemetry_ref| {
                telemetry_ref.source == MemythosTelemetrySource::AppServerNative
            })
            .collect();
        assert_eq!(native_refs.len(), 1);
        assert_eq!(
            native_refs[0].native_event_ref.as_deref(),
            Some("thread:thread_a/turn:1/event:2")
        );
        assert_eq!(
            native_refs[0].detail_ref.as_deref(),
            Some("app-server://threads/thread_a/turns/1/events/2")
        );
        assert_eq!(native_refs[0].channel, MemythosEventChannel::HumanHighlight);
    }

    #[tokio::test]
    async fn registers_arena_parents_and_delivers_parent_messages() {
        let processor = MemythosRequestProcessor::new();
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id.clone(),
                name: "Parent peer arena".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Let parent peers challenge ownership and routing.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };
        for thread_id in ["thread_growth", "thread_risk"] {
            processor
                .thread_attach(MemythosThreadAttachParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    role_id: Some("bettor".to_string()),
                    stance_id: Some(thread_id.to_string()),
                    objective: Some("Debate the BPM node contract.".to_string()),
                    contract_ref: Some("arena-contract.json".to_string()),
                })
                .await
                .unwrap();
        }

        for (thread_id, stance) in [("thread_growth", "growth"), ("thread_risk", "risk")] {
            let response = processor
                .arena_parent_register(MemythosArenaParentRegisterParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: stance.to_string(),
                    authority_scope: vec!["peer_debate".to_string()],
                })
                .await
                .unwrap();
            let ClientResponsePayload::MemythosArenaParentRegister(response) = response else {
                panic!("expected MemythosArenaParentRegister response");
            };
            assert_eq!(
                response.parent.lifecycle_state,
                MemythosArenaLifecycleState::Running
            );
        }

        let send_response = processor
            .arena_message_send(MemythosArenaMessageSendParams {
                message: MemythosArenaMessage {
                    message_id: "message-001".to_string(),
                    case_id: "case-001".to_string(),
                    arena_id: arena_response.arena.arena_id.clone(),
                    round_id: "round-001".to_string(),
                    from_parent_thread_id: "thread_growth".to_string(),
                    from_parent_role: "bettor".to_string(),
                    to_parent_thread_id: "thread_risk".to_string(),
                    to_parent_role: "bettor".to_string(),
                    message_kind: "peer_objection".to_string(),
                    human_summary: "Challenge ambiguous ownership before tactical execution."
                        .to_string(),
                    context_packet_ref: "artifact://context/minimal".to_string(),
                    artifact_refs: vec!["arena-contract.json".to_string()],
                    requires_response: true,
                    response_contract: Some("peer_objection_response".to_string()),
                },
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageSend(send_response) = send_response else {
            panic!("expected MemythosArenaMessageSend response");
        };
        assert_eq!(send_response.delivery.status, "recorded");
        assert_eq!(send_response.delivery.delivery_mechanism, "record_only");
        assert_eq!(send_response.delivery.receiver_turn_id, None);
        assert_eq!(send_response.delivery.receiver_response_event_ref, None);
        assert!(!send_response.delivery.delivered_as_human_instruction);
        assert_eq!(send_response.delivery.memory_replay_required, false);
        assert_eq!(send_response.delivery.receiver_thread_id, "thread_risk");

        let list_response = processor
            .arena_message_list(MemythosArenaMessageListParams {
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: Some("round-001".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageList(list_response) = list_response else {
            panic!("expected MemythosArenaMessageList response");
        };
        assert_eq!(list_response.deliveries.len(), 1);
        assert_eq!(list_response.deliveries[0].message_id, "message-001");

        let observation_response = processor
            .arena_message_observation_list(MemythosArenaMessageObservationListParams {
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: Some("round-001".to_string()),
                message_id: Some("message-001".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageObservationList(observation_response) =
            observation_response
        else {
            panic!("expected MemythosArenaMessageObservationList response");
        };
        assert_eq!(observation_response.observations.len(), 1);
        assert_eq!(
            observation_response.observations[0].observed_response_kind,
            MemythosParentPeerResponseKind::NoResponse
        );
        assert_eq!(
            observation_response.observations[0].semantic_alignment,
            MemythosSemanticAlignment::Invalid
        );

        let telemetry_response = processor
            .telemetry_list(MemythosTelemetryListParams {
                layer_id: Some(layer_response.layer.layer_id),
                arena_id: Some(arena_response.arena.arena_id),
                thread_id: Some("thread_risk".to_string()),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
        else {
            panic!("expected MemythosTelemetryList response");
        };
        assert!(
            telemetry_response
                .telemetry_refs
                .iter()
                .any(|telemetry_ref| {
                    telemetry_ref.kind == MemythosTelemetryRefKind::ArenaMessage
                        && telemetry_ref.channel == MemythosEventChannel::TechnicalDetail
                        && telemetry_ref.native_event_ref.is_some()
                })
        );
    }

    #[tokio::test]
    async fn arena_message_send_can_use_live_peer_parent_delivery_adapter() {
        let processor = MemythosRequestProcessor::new_for_transport_with_peer_delivery(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
        );
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id.clone(),
                name: "Parent peer arena".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Let parent peers challenge ownership and routing.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };

        for thread_id in ["thread_growth", "thread_risk"] {
            processor
                .thread_attach(MemythosThreadAttachParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    role_id: Some("bettor".to_string()),
                    stance_id: Some(thread_id.to_string()),
                    objective: Some("Debate the BPM node contract.".to_string()),
                    contract_ref: Some("arena-contract.json".to_string()),
                })
                .await
                .unwrap();
            processor
                .arena_parent_register(MemythosArenaParentRegisterParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: thread_id.to_string(),
                    authority_scope: vec!["peer_debate".to_string()],
                })
                .await
                .unwrap();
        }

        let send_response = processor
            .arena_message_send(MemythosArenaMessageSendParams {
                message: MemythosArenaMessage {
                    message_id: "message-live-001".to_string(),
                    case_id: "case-001".to_string(),
                    arena_id: arena_response.arena.arena_id.clone(),
                    round_id: "round-001".to_string(),
                    from_parent_thread_id: "thread_growth".to_string(),
                    from_parent_role: "bettor".to_string(),
                    to_parent_thread_id: "thread_risk".to_string(),
                    to_parent_role: "bettor".to_string(),
                    message_kind: "peer_objection".to_string(),
                    human_summary: "Challenge ambiguous ownership before tactical execution."
                        .to_string(),
                    context_packet_ref: "artifact://context/minimal".to_string(),
                    artifact_refs: vec!["arena-contract.json".to_string()],
                    requires_response: true,
                    response_contract: Some("peer_objection_response".to_string()),
                },
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageSend(send_response) = send_response else {
            panic!("expected MemythosArenaMessageSend response");
        };

        assert_eq!(send_response.delivery.status, "delivered_to_live_thread");
        assert_eq!(send_response.delivery.delivery_mechanism, "turn_start");
        assert_eq!(
            send_response.delivery.receiver_turn_id.as_deref(),
            Some("turn_for_thread_risk_message-live-001")
        );
        assert_eq!(send_response.delivery.receiver_response_event_ref, None);
        assert!(!send_response.delivery.delivered_as_human_instruction);
        assert!(
            send_response
                .delivery
                .event_refs
                .iter()
                .any(|event_ref| event_ref.contains("app-server://threads/thread_risk"))
        );

        let observation_response = processor
            .arena_message_observation_list(MemythosArenaMessageObservationListParams {
                arena_id: arena_response.arena.arena_id,
                round_id: Some("round-001".to_string()),
                message_id: Some("message-live-001".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageObservationList(observation_response) =
            observation_response
        else {
            panic!("expected MemythosArenaMessageObservationList response");
        };
        assert_eq!(observation_response.observations.len(), 1);
        assert_eq!(
            observation_response.observations[0].observed_response_kind,
            MemythosParentPeerResponseKind::PendingResponse
        );
        assert_eq!(
            observation_response.observations[0].semantic_alignment,
            MemythosSemanticAlignment::Pending
        );
        assert_eq!(
            observation_response.observations[0]
                .receiver_turn_id
                .as_deref(),
            Some("turn_for_thread_risk_message-live-001")
        );
        assert!(!observation_response.observations[0].treated_as_human_instruction);
    }

    #[tokio::test]
    async fn native_turn_completion_promotes_parent_peer_observation() {
        let processor = MemythosRequestProcessor::new_for_transport_with_peer_delivery(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
        );
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id.clone(),
                name: "Parent peer arena".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Let parent peers challenge ownership and routing.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };

        for thread_id in ["thread_growth", "thread_risk"] {
            processor
                .thread_attach(MemythosThreadAttachParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    role_id: Some("bettor".to_string()),
                    stance_id: Some(thread_id.to_string()),
                    objective: Some("Debate the BPM node contract.".to_string()),
                    contract_ref: Some("arena-contract.json".to_string()),
                })
                .await
                .unwrap();
            processor
                .arena_parent_register(MemythosArenaParentRegisterParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: thread_id.to_string(),
                    authority_scope: vec!["peer_debate".to_string()],
                })
                .await
                .unwrap();
        }

        processor
            .arena_message_send(MemythosArenaMessageSendParams {
                message: MemythosArenaMessage {
                    message_id: "message-live-002".to_string(),
                    case_id: "case-001".to_string(),
                    arena_id: arena_response.arena.arena_id.clone(),
                    round_id: "round-001".to_string(),
                    from_parent_thread_id: "thread_growth".to_string(),
                    from_parent_role: "bettor".to_string(),
                    to_parent_thread_id: "thread_risk".to_string(),
                    to_parent_role: "bettor".to_string(),
                    message_kind: "peer_objection".to_string(),
                    human_summary: "Challenge ambiguous ownership before tactical execution."
                        .to_string(),
                    context_packet_ref: "artifact://context/minimal".to_string(),
                    artifact_refs: vec!["arena-contract.json".to_string()],
                    requires_response: true,
                    response_contract: Some("peer_objection_response".to_string()),
                },
            })
            .await
            .unwrap();

        let matched = processor
            .record_native_turn_completed(
                "thread_risk",
                "turn_for_thread_risk_message-live-002",
                "completed",
                Some(1234),
                Some(2500),
            )
            .await;
        assert!(matched);

        let list_response = processor
            .arena_message_list(MemythosArenaMessageListParams {
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: Some("round-001".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageList(list_response) = list_response else {
            panic!("expected MemythosArenaMessageList response");
        };
        assert_eq!(list_response.deliveries.len(), 1);
        assert_eq!(
            list_response.deliveries[0].status,
            "receiver_turn_completed"
        );
        assert_eq!(
            list_response.deliveries[0]
                .receiver_response_event_ref
                .as_deref(),
            Some(
                "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/completed"
            )
        );

        let observation_response = processor
            .arena_message_observation_list(MemythosArenaMessageObservationListParams {
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: Some("round-001".to_string()),
                message_id: Some("message-live-002".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaMessageObservationList(observation_response) =
            observation_response
        else {
            panic!("expected MemythosArenaMessageObservationList response");
        };
        assert_eq!(
            observation_response.observations[0].observed_response_kind,
            MemythosParentPeerResponseKind::Ack
        );
        assert_eq!(
            observation_response.observations[0].semantic_alignment,
            MemythosSemanticAlignment::Acceptable
        );

        let telemetry_response = processor
            .telemetry_list(MemythosTelemetryListParams {
                layer_id: Some(layer_response.layer.layer_id),
                arena_id: Some(arena_response.arena.arena_id),
                thread_id: Some("thread_risk".to_string()),
                limit: Some(20),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
        else {
            panic!("expected MemythosTelemetryList response");
        };
        assert!(
            telemetry_response
                .telemetry_refs
                .iter()
                .any(|telemetry_ref| {
                    telemetry_ref.source == MemythosTelemetrySource::AppServerNative
                        && telemetry_ref.native_event_ref.as_deref()
                            == Some(
                                "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/completed"
                            )
                })
        );
    }

    #[tokio::test]
    async fn parent_continuity_tracks_multiple_receiver_turns() {
        let processor = MemythosRequestProcessor::new_for_transport_with_peer_delivery(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
        );
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id,
                name: "Parent peer arena".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Let parent peers challenge ownership and routing.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };

        for thread_id in ["thread_growth", "thread_risk"] {
            processor
                .thread_attach(MemythosThreadAttachParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    role_id: Some("bettor".to_string()),
                    stance_id: Some(thread_id.to_string()),
                    objective: Some("Debate the BPM node contract.".to_string()),
                    contract_ref: Some("arena-contract.json".to_string()),
                })
                .await
                .unwrap();
            processor
                .arena_parent_register(MemythosArenaParentRegisterParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: thread_id.to_string(),
                    authority_scope: vec!["peer_debate".to_string()],
                })
                .await
                .unwrap();
        }

        for message_id in ["message-live-001", "message-live-002"] {
            processor
                .arena_message_send(MemythosArenaMessageSendParams {
                    message: MemythosArenaMessage {
                        message_id: message_id.to_string(),
                        case_id: "case-001".to_string(),
                        arena_id: arena_response.arena.arena_id.clone(),
                        round_id: "round-001".to_string(),
                        from_parent_thread_id: "thread_growth".to_string(),
                        from_parent_role: "bettor".to_string(),
                        to_parent_thread_id: "thread_risk".to_string(),
                        to_parent_role: "bettor".to_string(),
                        message_kind: "peer_objection".to_string(),
                        human_summary: "Challenge ambiguous ownership before tactical execution."
                            .to_string(),
                        context_packet_ref: "artifact://context/minimal".to_string(),
                        artifact_refs: vec!["arena-contract.json".to_string()],
                        requires_response: true,
                        response_contract: Some("peer_objection_response".to_string()),
                    },
                })
                .await
                .unwrap();
        }

        let continuity_response = processor
            .parent_continuity_list(MemythosParentContinuityListParams {
                arena_id: arena_response.arena.arena_id,
                thread_id: Some("thread_risk".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosParentContinuityList(continuity_response) =
            continuity_response
        else {
            panic!("expected MemythosParentContinuityList response");
        };
        assert_eq!(continuity_response.continuities.len(), 1);
        let continuity = &continuity_response.continuities[0];
        assert_eq!(
            continuity.continuity_status,
            MemythosParentContinuityStatus::TurnContinuityObserved
        );
        assert_eq!(continuity.observed_turn_count, 2);
        assert_eq!(
            continuity.first_turn_id.as_deref(),
            Some("turn_for_thread_risk_message-live-001")
        );
        assert_eq!(
            continuity.latest_turn_id.as_deref(),
            Some("turn_for_thread_risk_message-live-002")
        );
        assert!(!continuity.memory_replay_required);
        assert!(!continuity.goal_snapshot_available);
    }

    #[tokio::test]
    async fn parent_continuity_is_verified_with_goal_snapshot_and_completed_turn() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
            Arc::new(RecordOnlyThreadConsolidationAdapter),
            Arc::new(RecordOnlyParentTurnResponseAdapter),
        );
        let layer_response = processor
            .layer_create(MemythosLayerCreateParams {
                name: "BPM E2E".to_string(),
                kind: MemythosLayerKind::BpmEndToEnd,
                parent_layer_id: None,
                objective: "Resolve an end-to-end process segment.".to_string(),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
            panic!("expected MemythosLayerCreate response");
        };
        let arena_response = processor
            .arena_create(MemythosArenaCreateParams {
                layer_id: layer_response.layer.layer_id,
                name: "Parent peer arena".to_string(),
                kind: MemythosArenaKind::Debate,
                objective: "Let parent peers challenge ownership and routing.".to_string(),
                participant_ids: vec![],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
            panic!("expected MemythosArenaCreate response");
        };

        for thread_id in ["thread_growth", "thread_risk"] {
            processor
                .thread_attach(MemythosThreadAttachParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    role_id: Some("bettor".to_string()),
                    stance_id: Some(thread_id.to_string()),
                    objective: Some("Debate the BPM node contract.".to_string()),
                    contract_ref: Some("arena-contract.json".to_string()),
                })
                .await
                .unwrap();
            processor
                .arena_parent_register(MemythosArenaParentRegisterParams {
                    arena_id: arena_response.arena.arena_id.clone(),
                    thread_id: thread_id.to_string(),
                    parent_role: "bettor".to_string(),
                    stance_profile: thread_id.to_string(),
                    authority_scope: vec!["peer_debate".to_string()],
                })
                .await
                .unwrap();
        }

        for message_id in ["message-live-001", "message-live-002"] {
            processor
                .arena_message_send(MemythosArenaMessageSendParams {
                    message: MemythosArenaMessage {
                        message_id: message_id.to_string(),
                        case_id: "case-001".to_string(),
                        arena_id: arena_response.arena.arena_id.clone(),
                        round_id: "round-001".to_string(),
                        from_parent_thread_id: "thread_growth".to_string(),
                        from_parent_role: "bettor".to_string(),
                        to_parent_thread_id: "thread_risk".to_string(),
                        to_parent_role: "bettor".to_string(),
                        message_kind: "peer_objection".to_string(),
                        human_summary: "Challenge ambiguous ownership before tactical execution."
                            .to_string(),
                        context_packet_ref: "artifact://context/minimal".to_string(),
                        artifact_refs: vec!["arena-contract.json".to_string()],
                        requires_response: true,
                        response_contract: Some("peer_objection_response".to_string()),
                    },
                })
                .await
                .unwrap();
        }
        processor
            .record_native_turn_completed(
                "thread_risk",
                "turn_for_thread_risk_message-live-002",
                "completed",
                Some(1234),
                Some(2500),
            )
            .await;
        processor
            .record_native_token_usage("thread_risk", "turn_for_thread_risk_message-live-002")
            .await;

        let continuity_response = processor
            .parent_continuity_list(MemythosParentContinuityListParams {
                arena_id: arena_response.arena.arena_id,
                thread_id: Some("thread_risk".to_string()),
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosParentContinuityList(continuity_response) =
            continuity_response
        else {
            panic!("expected MemythosParentContinuityList response");
        };

        assert_eq!(continuity_response.continuities.len(), 1);
        let continuity = &continuity_response.continuities[0];
        assert_eq!(
            continuity.continuity_status,
            MemythosParentContinuityStatus::Verified
        );
        assert!(continuity.goal_snapshot_available);
        assert_eq!(
            continuity.goal_snapshot_ref.as_deref(),
            Some("app-server://threads/thread_risk/goals/current")
        );
        assert_eq!(
            continuity.budget_state_ref.as_deref(),
            Some("app-server://threads/thread_risk/budget/current")
        );
        assert_eq!(continuity.goal_status, Some(ThreadGoalStatus::Active));
        assert_eq!(continuity.token_budget, Some(20_000));
        assert_eq!(continuity.tokens_used, Some(3_800));
        assert_eq!(continuity.time_used_seconds, Some(71));
        assert_eq!(
            continuity.latest_turn_completed_ref.as_deref(),
            Some(
                "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/completed"
            )
        );
        assert_eq!(
            continuity.token_usage_ref.as_deref(),
            Some(
                "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/token-usage"
            )
        );
    }
}
