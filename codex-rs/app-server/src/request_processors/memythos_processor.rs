use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

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
use codex_app_server_protocol::MemythosArenaMessageSendParams;
use codex_app_server_protocol::MemythosArenaMessageSendResponse;
use codex_app_server_protocol::MemythosArenaParent;
use codex_app_server_protocol::MemythosArenaParentRegisterParams;
use codex_app_server_protocol::MemythosArenaParentRegisterResponse;
use codex_app_server_protocol::MemythosEventChannel;
use codex_app_server_protocol::MemythosLayer;
use codex_app_server_protocol::MemythosLayerCreateParams;
use codex_app_server_protocol::MemythosLayerCreateResponse;
use codex_app_server_protocol::MemythosLayerListParams;
use codex_app_server_protocol::MemythosLayerListResponse;
use codex_app_server_protocol::MemythosParentContinuityListParams;
use codex_app_server_protocol::MemythosParentContinuityListResponse;
use codex_app_server_protocol::MemythosParentContinuityStatus;
use codex_app_server_protocol::MemythosParentPeerResponseKind;
use codex_app_server_protocol::MemythosParentPeerResponseObservation;
use codex_app_server_protocol::MemythosParentThreadContinuity;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomActivityCollab;
use codex_app_server_protocol::MemythosRoomActivityItem;
use codex_app_server_protocol::MemythosRoomActivityLifecycle;
use codex_app_server_protocol::MemythosRoomActivityListParams;
use codex_app_server_protocol::MemythosRoomActivityListResponse;
use codex_app_server_protocol::MemythosRoomActivityParticipant;
use codex_app_server_protocol::MemythosRoomActivitySubagents;
use codex_app_server_protocol::MemythosRoomActivityTurn;
use codex_app_server_protocol::MemythosRoomActivityUsage;
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
use codex_app_server_protocol::MemythosTelemetryListParams;
use codex_app_server_protocol::MemythosTelemetryListResponse;
use codex_app_server_protocol::MemythosTelemetryRef;
use codex_app_server_protocol::MemythosTelemetryRefKind;
use codex_app_server_protocol::MemythosTelemetrySource;
use codex_app_server_protocol::MemythosThreadAttachParams;
use codex_app_server_protocol::MemythosThreadAttachResponse;
use codex_app_server_protocol::MemythosThreadAttachment;
use codex_app_server_protocol::MemythosThreadListParams;
use codex_app_server_protocol::MemythosThreadListResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadGoalGetParams;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use tokio::sync::Mutex;

use crate::error_code::invalid_params;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::ThreadGoalRequestProcessor;
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
    native_token_usage_refs: HashMap<String, String>,
    telemetry_refs: Vec<MemythosTelemetryRef>,
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
            metadata.insert("memythos_peer_parent".to_string(), "true".to_string());
            metadata.insert("human_instruction".to_string(), "false".to_string());
            let mut additional_context = HashMap::new();
            additional_context.insert(
                "memythos.peer_parent".to_string(),
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
                        delivered_as_human_instruction: false,
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

#[derive(Clone)]
pub(crate) struct MemythosRequestProcessor {
    state: Arc<Mutex<MemythosRuntimeState>>,
    peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
    parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
    next_layer_id: Arc<AtomicU64>,
    next_arena_id: Arc<AtomicU64>,
    next_attachment_id: Arc<AtomicU64>,
    next_delivery_id: Arc<AtomicU64>,
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
        )
    }

    pub(crate) fn new_for_transport_with_adapters(
        rpc_transport: AppServerRpcTransport,
        peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
        parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
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
                native_token_usage_refs: HashMap::new(),
                telemetry_refs: Vec::new(),
            })),
            peer_parent_delivery_adapter,
            parent_goal_snapshot_adapter,
            next_layer_id: Arc::new(AtomicU64::default()),
            next_arena_id: Arc::new(AtomicU64::default()),
            next_attachment_id: Arc::new(AtomicU64::default()),
            next_delivery_id: Arc::new(AtomicU64::default()),
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
                "memythos/arena/message".to_string(),
                "memythos/arena/message/list".to_string(),
                "memythos/room/register".to_string(),
                "memythos/room/activity/list".to_string(),
                "memythos/room/sendInput".to_string(),
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

        Ok(MemythosRoomRegisterResponse {
            room,
            event_refs: vec![event_ref],
        }
        .into())
    }

    pub(crate) async fn room_send_input(
        &self,
        params: MemythosRoomSendInputParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        if params.human_instruction {
            return Err(invalid_params(
                "memythos/room/sendInput requires humanInstruction=false".to_string(),
            ));
        }
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
        let source = room_participant_by_thread(&room, &source_thread_id);
        if source.is_none()
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
            from_parent_thread_id: source_thread_id.clone(),
            from_parent_role: source
                .as_ref()
                .map(|participant| participant.parent_role.clone())
                .unwrap_or_else(|| "room_concierge".to_string()),
            to_parent_thread_id: params.to_parent_thread_id.clone(),
            to_parent_role: target.parent_role.clone(),
            message_kind: params.message_kind.clone(),
            human_summary: params.prompt.clone(),
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

        let delivery = MemythosArenaMessageDelivery {
            delivery_id,
            message_id: message.message_id.clone(),
            status: "delivered_to_live_thread".to_string(),
            sender_thread_id: source_thread_id,
            receiver_thread_id: params.to_parent_thread_id.clone(),
            arena_id: room.arena_id.clone(),
            round_id: message.round_id.clone(),
            phase: params
                .metadata
                .get("memythos_phase")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
                .or_else(|| phase_from_message_kind(&message.message_kind)),
            delivery_mechanism: "room_loopback_send_input".to_string(),
            receiver_turn_id: Some(target_turn_id.clone()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
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

        Ok(MemythosRoomSendInputResponse {
            delivery: MemythosRoomSendInputDelivery {
                thread_id: params.to_parent_thread_id,
                turn_id: target_turn_id,
                event_refs,
                room_id: params.room_id,
                room_message_ref: params.room_message_ref,
                delivery_ref: params.delivery_ref,
                delivery_mechanism: "room_loopback_send_input".to_string(),
                human_instruction: false,
                message_authority: params.message_authority,
            },
        }
        .into())
    }

    pub(crate) async fn room_activity_list(
        &self,
        params: MemythosRoomActivityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let room = state
            .rooms
            .get(&params.room_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown room id: {}", params.room_id)))?;
        let participant_thread_ids = room
            .participants
            .iter()
            .map(|participant| participant.thread_id.as_str())
            .collect::<HashSet<_>>();
        let mut deliveries = state
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
                params.phase.as_ref().map_or(true, |phase| {
                    delivery.phase.as_deref() == Some(phase.as_str())
                })
            })
            .filter(|delivery| {
                participant_thread_ids.contains(delivery.sender_thread_id.as_str())
                    || participant_thread_ids.contains(delivery.receiver_thread_id.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| left.delivery_id.cmp(&right.delivery_id));
        if let Some(limit) = params.limit {
            deliveries.truncate(limit);
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
        let participants = room
            .participants
            .iter()
            .map(|participant| {
                let participant_deliveries = deliveries
                    .iter()
                    .filter(|delivery| delivery.receiver_thread_id == participant.thread_id)
                    .collect::<Vec<_>>();
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
                }
            })
            .collect::<Vec<_>>();
        let turns = deliveries
            .iter()
            .filter_map(|delivery| {
                room_activity_turn_from_delivery(&room, delivery, params.include_debug_refs)
            })
            .collect::<Vec<_>>();
        let cursor = turns
            .last()
            .map(|turn| format!("{}::{}", turn.thread_id, turn.turn_id));
        Ok(MemythosRoomActivityListResponse {
            room_id: room.room_id,
            case_id: room.case_id,
            layer_id: room.layer_id,
            arena_id: room.arena_id,
            round_id: params.round_id,
            cursor,
            source_method: "memythos/room/activity/list".to_string(),
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
            blockers: Vec::new(),
        }
        .into())
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
            Some(layer_id),
            Some(arena_id),
            Some(thread_id.to_string()),
            Some(native_event_ref),
            detail_ref,
            MemythosEventChannel::StateTransition,
            summary,
        );

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
            Some(arena_id),
            Some(thread_id.to_string()),
            Some(native_event_ref),
            None,
            MemythosEventChannel::StateTransition,
            format!("Native token usage observed for thread {thread_id} turn {turn_id}."),
        );
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
    let event_ref = delivery
        .receiver_response_event_ref
        .clone()
        .unwrap_or_else(|| {
            format!(
                "app-server://threads/{}/turns/{}",
                delivery.receiver_thread_id, turn_id
            )
        });
    let parent_key = room
        .participants
        .iter()
        .find(|participant| participant.thread_id == delivery.receiver_thread_id)
        .map(|participant| participant.parent_key.clone())
        .unwrap_or_else(|| arena_parent_key(&delivery.arena_id, &delivery.receiver_thread_id));
    let phase = delivery.phase.clone();
    let human_highlight = phase.as_ref().map(|phase| {
        compact_summary(format!(
            "{} received {} for phase {phase}.",
            parent_key, delivery.message_id
        ))
    });
    let technical_summary = compact_summary(format!(
        "{} delivered {} from {} to {} as {}",
        delivery.delivery_mechanism,
        delivery.message_id,
        delivery.sender_thread_id,
        delivery.receiver_thread_id,
        delivery.status
    ));
    Some(MemythosRoomActivityTurn {
        parent_key,
        thread_id: delivery.receiver_thread_id.clone(),
        turn_id,
        round_id: Some(delivery.round_id.clone()),
        phase,
        status: status.to_string(),
        items: vec![MemythosRoomActivityItem {
            kind: "collab_call".to_string(),
            status: delivery.status.clone(),
            summary: technical_summary.clone(),
            text: human_highlight.clone(),
            human_highlight,
            technical_summary: Some(technical_summary),
            artifact_ref: Some(delivery.message_id.clone()),
            event_ref,
            refs: compact_event_refs(refs),
        }],
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

fn room_participant_by_thread<'a>(
    room: &'a MemythosRoom,
    thread_id: &str,
) -> Option<&'a MemythosRoomParticipant> {
    room.participants
        .iter()
        .find(|participant| participant.thread_id == thread_id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::MemythosArenaKind;
    use codex_app_server_protocol::MemythosArenaMessage;
    use codex_app_server_protocol::MemythosLayerKind;

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
    async fn room_send_input_uses_native_loopback_delivery() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
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
                prompt: "No soy humano; soy peer de arena. Objeta mi propuesta.".to_string(),
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
        );
        let mut params = room_register_params();
        params.participants.push(MemythosRoomParticipant {
            parent_key: "case/bpm_e2e/arena/observer/room_concierge".to_string(),
            thread_id: "thread_concierge".to_string(),
            parent_role: "observer".to_string(),
            stance_profile: "room_concierge".to_string(),
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
                source_parent_key: "case/bpm_e2e/arena/observer/room_concierge".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_proposal".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                client_user_message_id: Some("message-002".to_string()),
                prompt: "Concierge entrega un mensaje entre peers.".to_string(),
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
        assert_eq!(response.lifecycle.room_state, "round_closed");
        assert!(response.lifecycle.clean_close);
        assert_eq!(response.collab.send_input_count, 0);
    }

    #[tokio::test]
    async fn room_activity_list_summarizes_delivery_completion_and_usage() {
        let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
            AppServerRpcTransport::Websocket,
            Arc::new(FakeLivePeerParentDeliveryAdapter),
            Arc::new(FakeParentGoalSnapshotAdapter),
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
                prompt: "Apuesta y declara condiciones de ejecucion.".to_string(),
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
        assert!(
            response.turns[0].items[0]
                .refs
                .iter()
                .all(|event_ref| { !event_ref.contains("/events/") })
        );

        let bet_response = processor
            .room_activity_list(MemythosRoomActivityListParams {
                room_id: "room-001".to_string(),
                round_id: Some("round-001".to_string()),
                phase: Some("bet".to_string()),
                since_cursor: None,
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
