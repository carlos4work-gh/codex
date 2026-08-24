use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArena;
#[cfg(test)]
use codex_app_server_protocol::MemythosArenaAggregateContract;
use codex_app_server_protocol::MemythosArenaAggregateState;
use codex_app_server_protocol::MemythosArenaCheckpointState;
use codex_app_server_protocol::MemythosArenaCompositionLease;
use codex_app_server_protocol::MemythosArenaCompositionLifecycleState;
use codex_app_server_protocol::MemythosArenaCompositionProvisionParams;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaCompositionRevisionActionKind;
use codex_app_server_protocol::MemythosArenaCreateParams;
use codex_app_server_protocol::MemythosArenaCreateResponse;
#[cfg(test)]
use codex_app_server_protocol::MemythosArenaDecisionMethod;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
#[cfg(test)]
use codex_app_server_protocol::MemythosArenaLateArrivalPolicy;
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
use codex_app_server_protocol::MemythosArenaMessageReadParams;
use codex_app_server_protocol::MemythosArenaMessageReadResponse;
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
use codex_app_server_protocol::MemythosArenaRequestParams;
use codex_app_server_protocol::MemythosArenaRequestResponse;
use codex_app_server_protocol::MemythosArenaResumeAssessment;
use codex_app_server_protocol::MemythosArenaResumeDisposition;
use codex_app_server_protocol::MemythosArenaResumeExecutionMode;
use codex_app_server_protocol::MemythosArenaResumeExecutionPlan;
use codex_app_server_protocol::MemythosArenaRunParams;
use codex_app_server_protocol::MemythosArenaRunResponse;
use codex_app_server_protocol::MemythosArenaStateGetParams;
use codex_app_server_protocol::MemythosArenaStateGetResponse;
use codex_app_server_protocol::MemythosEventChannel;
use codex_app_server_protocol::MemythosLayerCreateParams;
use codex_app_server_protocol::MemythosLayerCreateResponse;
use codex_app_server_protocol::MemythosLayerListParams;
use codex_app_server_protocol::MemythosLayerListResponse;
use codex_app_server_protocol::MemythosMailboxHealthGetParams;
use codex_app_server_protocol::MemythosMailboxHealthGetResponse;
use codex_app_server_protocol::MemythosMailboxQuarantineGetParams;
use codex_app_server_protocol::MemythosMailboxQuarantineGetResponse;
use codex_app_server_protocol::MemythosMailboxQuarantineListParams;
use codex_app_server_protocol::MemythosMailboxQuarantineListResponse;
use codex_app_server_protocol::MemythosMailboxQuarantineRecord;
use codex_app_server_protocol::MemythosMailboxQuarantineResolutionAction;
use codex_app_server_protocol::MemythosMailboxQuarantineResolveParams;
use codex_app_server_protocol::MemythosMailboxQuarantineResolveResponse;
use codex_app_server_protocol::MemythosMailboxResolutionAuditRecord;
use codex_app_server_protocol::MemythosMailboxResolutionGetParams;
use codex_app_server_protocol::MemythosMailboxResolutionGetResponse;
use codex_app_server_protocol::MemythosMailboxResolutionListParams;
use codex_app_server_protocol::MemythosMailboxResolutionListResponse;
use codex_app_server_protocol::MemythosParentContinuityListParams;
use codex_app_server_protocol::MemythosParentContinuityListResponse;
#[cfg(test)]
use codex_app_server_protocol::MemythosParentContinuityStatus;
#[cfg(test)]
use codex_app_server_protocol::MemythosParentPeerResponseKind;
#[cfg(test)]
use codex_app_server_protocol::MemythosParentRole;
#[cfg(test)]
use codex_app_server_protocol::MemythosParentStance;
use codex_app_server_protocol::MemythosPromptLineagePart;
use codex_app_server_protocol::MemythosPromptOrigin;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomActivityCollab;
use codex_app_server_protocol::MemythosRoomActivityEvent;
use codex_app_server_protocol::MemythosRoomActivityLifecycle;
use codex_app_server_protocol::MemythosRoomActivityListParams;
use codex_app_server_protocol::MemythosRoomActivityListResponse;
use codex_app_server_protocol::MemythosRoomActivityParticipant;
use codex_app_server_protocol::MemythosRoomActivitySubagents;
use codex_app_server_protocol::MemythosRoomActivityUsage;
#[cfg(test)]
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
use codex_app_server_protocol::MemythosRuntimeHealthParams;
use codex_app_server_protocol::MemythosRuntimeLifecycleState;
#[cfg(test)]
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
use codex_app_server_protocol::MemythosThreadConsolidationAuthorityMode;
use codex_app_server_protocol::MemythosThreadConsolidationPurpose;
#[cfg(test)]
use codex_app_server_protocol::MemythosThreadConsolidationSourceRef;
use codex_app_server_protocol::MemythosThreadContractAssembleParams;
use codex_app_server_protocol::MemythosThreadContractAssembleResponse;
use codex_app_server_protocol::MemythosThreadContractListParams;
use codex_app_server_protocol::MemythosThreadContractListResponse;
use codex_app_server_protocol::MemythosThreadContractReadParams;
use codex_app_server_protocol::MemythosThreadContractReadResponse;
use codex_app_server_protocol::MemythosThreadListParams;
use codex_app_server_protocol::MemythosThreadListResponse;
use codex_app_server_protocol::MemythosTurnUsageAttribution;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::ThreadTokenUsage;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::AgentPath;
use codex_protocol::ResponseItemId;
#[cfg(test)]
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
#[cfg(test)]
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_rollout::state_db::StateDbHandle;
use codex_state::ArenaSnapshotRecord;
use codex_state::NativeMailboxCommunicationRecord;
use codex_state::NativeMailboxResolutionAction;
use codex_state::NativeMailboxResolutionAuditRecord;
use codex_state::NativeMailboxResolutionCommand;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tracing::warn;

use crate::error_code::internal_error;
use crate::error_code::invalid_params;
use crate::outgoing_message::ConnectionId;
use crate::request_processors::memythos_activity::*;
use crate::request_processors::memythos_arena_state::ArenaCommand;
use crate::request_processors::memythos_arena_state::ArenaEventKind;
use crate::request_processors::memythos_arena_state::NativeArenaState;
use crate::request_processors::memythos_checkpoint::*;
use crate::request_processors::memythos_closure::*;
use crate::request_processors::memythos_composition::*;
use crate::request_processors::memythos_composition_planning::*;
use crate::request_processors::memythos_contracts::*;
use crate::request_processors::memythos_delivery::*;
use crate::request_processors::memythos_judge::*;
use crate::request_processors::memythos_loopback::*;
use crate::request_processors::memythos_observability::*;
use crate::request_processors::memythos_parent_configuration::*;
use crate::request_processors::memythos_parent_goal::*;
use crate::request_processors::memythos_parent_provisioning::*;
use crate::request_processors::memythos_parent_response::*;
use crate::request_processors::memythos_peer_delivery::*;
use crate::request_processors::memythos_resume::*;
use crate::request_processors::memythos_room::*;
use crate::request_processors::memythos_room_routing::*;
use crate::request_processors::memythos_round::*;
use crate::request_processors::memythos_runtime::*;
use crate::request_processors::memythos_runtime_state::*;
use crate::request_processors::memythos_thread_consolidation::*;

#[path = "memythos_processor/activity_handlers.rs"]
mod activity_handlers;
#[path = "memythos_processor/event_handlers.rs"]
mod event_handlers;
#[path = "memythos_processor/mailbox_handlers.rs"]
mod mailbox_handlers;
#[path = "memythos_processor/query_handlers.rs"]
mod query_handlers;
#[path = "memythos_processor/room_handlers.rs"]
mod room_handlers;
#[path = "memythos_processor/thread_handlers.rs"]
mod thread_handlers;

#[derive(Debug, Clone)]
struct PreparedParentDeliveryGoal {
    active_goal: ThreadGoal,
    previous_goal: ThreadGoal,
    assigned_for_delivery: bool,
}

fn arena_snapshot_sha256(snapshot_json: &str) -> String {
    format!("{:x}", Sha256::digest(snapshot_json.as_bytes()))
}

fn generated_id_sequence(id: &str, prefix: &str) -> Option<u64> {
    id.strip_prefix(prefix)?.strip_prefix('_')?.parse().ok()
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

#[cfg(test)]
struct RecordOnlyArenaParentProvisioningAdapter;

#[cfg(test)]
impl ArenaParentProvisioningAdapter for RecordOnlyArenaParentProvisioningAdapter {
    fn provision_parent<'a>(
        &'a self,
        _params: &'a MemythosArenaCompositionProvisionParams,
        participant: &'a codex_app_server_protocol::MemythosArenaCompositionParticipant,
        _reusable_thread_id: Option<&'a str>,
        _connection_id: ConnectionId,
    ) -> ArenaParentProvisionFuture<'a> {
        Box::pin(async move {
            Err(invalid_params(format!(
                "native arena provisioning unavailable for participant {}",
                participant.participant_id
            )))
        })
    }

    fn transition_parent_goal<'a>(
        &'a self,
        thread_id: &'a str,
        objective: Option<&'a str>,
        status: ThreadGoalStatus,
        _arm_for_next_turn: bool,
    ) -> ArenaParentGoalTransitionFuture<'a> {
        Box::pin(async move {
            Ok(ThreadGoal {
                thread_id: thread_id.to_string(),
                objective: objective.unwrap_or_default().to_string(),
                status,
                token_budget: None,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at: 0,
                updated_at: 0,
            })
        })
    }

    fn read_parent_goal<'a>(&'a self, thread_id: &'a str) -> ArenaParentGoalReadFuture<'a> {
        Box::pin(async move {
            Ok(Some(ThreadGoal {
                thread_id: thread_id.to_string(),
                objective: String::new(),
                status: ThreadGoalStatus::Paused,
                token_budget: None,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at: 0,
                updated_at: 0,
            }))
        })
    }

    fn rollback_parent<'a>(&'a self, thread_id: &'a str) -> ArenaParentProvisionFuture<'a> {
        Box::pin(async move {
            Ok(ProvisionedArenaParent {
                participant_id: String::new(),
                thread_id: thread_id.to_string(),
                goal_ref: String::new(),
                lease_id: String::new(),
                lease_source: "rolled_back".to_string(),
                memory_scope: String::new(),
                goal: ThreadGoal {
                    thread_id: thread_id.to_string(),
                    objective: String::new(),
                    status: ThreadGoalStatus::Paused,
                    token_budget: None,
                    tokens_used: 0,
                    time_used_seconds: 0,
                    created_at: 0,
                    updated_at: 0,
                },
                newly_created: false,
            })
        })
    }
}

#[cfg(test)]
struct RecordOnlyArenaCompositionPlanningAdapter;

#[cfg(test)]
impl ArenaCompositionPlanningAdapter for RecordOnlyArenaCompositionPlanningAdapter {
    fn plan<'a>(
        &'a self,
        _params: &'a MemythosArenaRequestParams,
        _previous: Option<&'a MemythosArenaCompositionProvisionResponse>,
        _connection_id: ConnectionId,
    ) -> ArenaCompositionPlanningFuture<'a> {
        Box::pin(async {
            Err(invalid_params(
                "native arena composition planning is unavailable in record-only mode",
            ))
        })
    }
}

struct MemythosArenaPhaseUpdate {
    arena_id: String,
    round_id: String,
    phase: String,
    lifecycle_state: MemythosArenaLifecycleState,
    event_refs: Vec<String>,
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
pub(crate) struct MemythosRequestProcessor {
    state: Arc<Mutex<MemythosRuntimeState>>,
    arena_state_db: Option<StateDbHandle>,
    arena_restore_result: Arc<OnceCell<Result<(), String>>>,
    peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
    parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
    thread_consolidation_adapter: Arc<dyn ThreadConsolidationAdapter>,
    parent_turn_response_adapter: Arc<dyn ParentTurnResponseAdapter>,
    parent_configuration_adapter: Arc<dyn ParentConfigurationAdapter>,
    arena_parent_provisioning_adapter: Arc<dyn ArenaParentProvisioningAdapter>,
    arena_composition_planning_adapter: Arc<dyn ArenaCompositionPlanningAdapter>,
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
            Arc::new(RecordOnlyArenaParentProvisioningAdapter),
            Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_transport_with_native_adapters(
        rpc_transport: AppServerRpcTransport,
        peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
        parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
        thread_consolidation_adapter: Arc<dyn ThreadConsolidationAdapter>,
        parent_turn_response_adapter: Arc<dyn ParentTurnResponseAdapter>,
        parent_configuration_adapter: Arc<dyn ParentConfigurationAdapter>,
        arena_parent_provisioning_adapter: Arc<dyn ArenaParentProvisioningAdapter>,
        arena_composition_planning_adapter: Arc<dyn ArenaCompositionPlanningAdapter>,
    ) -> Self {
        Self::new_for_transport_with_native_adapters_and_state_db(
            rpc_transport,
            peer_parent_delivery_adapter,
            parent_goal_snapshot_adapter,
            thread_consolidation_adapter,
            parent_turn_response_adapter,
            parent_configuration_adapter,
            arena_parent_provisioning_adapter,
            arena_composition_planning_adapter,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_transport_with_native_adapters_and_state_db(
        rpc_transport: AppServerRpcTransport,
        peer_parent_delivery_adapter: Arc<dyn PeerParentDeliveryAdapter>,
        parent_goal_snapshot_adapter: Arc<dyn ParentGoalSnapshotAdapter>,
        thread_consolidation_adapter: Arc<dyn ThreadConsolidationAdapter>,
        parent_turn_response_adapter: Arc<dyn ParentTurnResponseAdapter>,
        parent_configuration_adapter: Arc<dyn ParentConfigurationAdapter>,
        arena_parent_provisioning_adapter: Arc<dyn ArenaParentProvisioningAdapter>,
        arena_composition_planning_adapter: Arc<dyn ArenaCompositionPlanningAdapter>,
        arena_state_db: Option<StateDbHandle>,
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
                arena_lifecycles: HashMap::new(),
                rooms: HashMap::new(),
                thread_attachments: HashMap::new(),
                arena_parents: HashMap::new(),
                arena_compositions: HashMap::new(),
                restored_coordination_snapshots: HashMap::new(),
                arena_message_deliveries: Vec::new(),
                arena_messages: HashMap::new(),
                arena_message_aggregates: HashMap::new(),
                arena_resume_execution_plans: HashMap::new(),
                room_activity_events: HashMap::new(),
                native_parent_turn_responses: HashMap::new(),
                structured_contracts: HashMap::new(),
                native_token_usage_refs: HashMap::new(),
                native_thread_usage_totals: HashMap::new(),
                native_turn_usage: HashMap::new(),
                telemetry_refs: Vec::new(),
            })),
            arena_state_db,
            arena_restore_result: Arc::new(OnceCell::new()),
            peer_parent_delivery_adapter,
            parent_goal_snapshot_adapter,
            thread_consolidation_adapter,
            parent_turn_response_adapter,
            parent_configuration_adapter,
            arena_parent_provisioning_adapter,
            arena_composition_planning_adapter,
            next_layer_id: Arc::new(AtomicU64::default()),
            next_arena_id: Arc::new(AtomicU64::default()),
            next_attachment_id: Arc::new(AtomicU64::default()),
            next_delivery_id: Arc::new(AtomicU64::default()),
            next_room_activity_id: Arc::new(AtomicU64::default()),
            next_contract_id: Arc::new(AtomicU64::default()),
            next_telemetry_ref_id: Arc::new(AtomicU64::default()),
        }
    }

    async fn ensure_arena_state_restored(&self) -> Result<(), JSONRPCErrorError> {
        let result = self
            .arena_restore_result
            .get_or_init(|| async {
                self.restore_arena_coordination_snapshots()
                    .await
                    .map_err(|error| error.message)
            })
            .await;
        result.clone().map_err(invalid_params)
    }

    async fn restore_arena_coordination_snapshots(&self) -> Result<(), JSONRPCErrorError> {
        let Some(state_db) = self.arena_state_db.as_ref() else {
            return Ok(());
        };
        let records = state_db.list_arena_snapshots().await.map_err(|error| {
            invalid_params(format!(
                "failed to load Arena snapshots from app-server state: {error}"
            ))
        })?;
        for record in records {
            if record.schema_version != i64::from(ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION) {
                return Err(invalid_params(format!(
                    "unsupported Arena coordination snapshot schema {} for {}",
                    record.schema_version, record.arena_id
                )));
            }
            let actual_hash = arena_snapshot_sha256(&record.snapshot_json);
            if actual_hash != record.last_event_hash {
                return Err(invalid_params(format!(
                    "Arena snapshot hash mismatch for {}",
                    record.arena_id
                )));
            }
            let snapshot: PersistedArenaCoordinationSnapshot =
                serde_json::from_str(&record.snapshot_json).map_err(|error| {
                    invalid_params(format!(
                        "invalid Arena coordination snapshot for {}: {error}",
                        record.arena_id
                    ))
                })?;
            if snapshot.schema_version != ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION
                || snapshot.protocol.arena_id != record.arena_id
                || snapshot.room.arena_id != record.arena_id
            {
                return Err(invalid_params(format!(
                    "Arena snapshot identity mismatch for {}",
                    record.arena_id
                )));
            }
            let lifecycle = NativeArenaState::restore_protocol_snapshot(snapshot.protocol.clone())
                .map_err(|error| invalid_params(error.to_string()))?;
            if i64::try_from(snapshot.protocol.sequence).ok() != Some(record.snapshot_sequence) {
                return Err(invalid_params(format!(
                    "Arena snapshot sequence mismatch for {}",
                    record.arena_id
                )));
            }
            let concierge = snapshot
                .room
                .participants
                .iter()
                .find(|participant| participant.parent_role == "room_concierge")
                .ok_or_else(|| {
                    invalid_params(format!(
                        "Arena {} snapshot has no OOTB Room Concierge",
                        record.arena_id
                    ))
                })?;
            if concierge.thread_id != record.concierge_thread_id || concierge.goal_ref.is_none() {
                return Err(invalid_params(format!(
                    "Arena {} Concierge reference is inconsistent",
                    record.arena_id
                )));
            }

            let mut restored_goals = HashMap::new();
            for participant in &snapshot.room.participants {
                let goal = self
                    .arena_parent_provisioning_adapter
                    .read_parent_goal(&participant.thread_id)
                    .await?
                    .ok_or_else(|| {
                        invalid_params(format!(
                            "Arena {} recovery paused: OOTB goal missing for parent {}",
                            record.arena_id, participant.thread_id
                        ))
                    })?;
                restored_goals.insert(participant.thread_id.clone(), goal);
            }
            let concierge_goal = restored_goals
                .get(&concierge.thread_id)
                .expect("Concierge goal was resolved above");
            let lifecycle_state = lifecycle.protocol_state();
            let arena = MemythosArena {
                arena_id: record.arena_id.clone(),
                layer_id: snapshot.layer_id.clone(),
                name: record.arena_id.clone(),
                kind: codex_app_server_protocol::MemythosArenaKind::Debate,
                lifecycle_state,
                objective: concierge_goal.objective.clone(),
                participant_ids: snapshot
                    .room
                    .participants
                    .iter()
                    .map(|participant| participant.thread_id.clone())
                    .collect(),
            };

            let mut state = self.state.lock().await;
            if let Some(max_delivery_id) = snapshot
                .deliveries
                .iter()
                .filter_map(|delivery| generated_id_sequence(&delivery.delivery_id, "mem_delivery"))
                .max()
            {
                self.next_delivery_id
                    .fetch_max(max_delivery_id, std::sync::atomic::Ordering::Relaxed);
            }
            state.arenas.insert(record.arena_id.clone(), arena);
            state
                .arena_lifecycles
                .insert(record.arena_id.clone(), lifecycle);
            state
                .rooms
                .insert(snapshot.room.room_id.clone(), snapshot.room.clone());
            for participant in &snapshot.room.participants {
                state.arena_parents.insert(
                    arena_parent_key(&record.arena_id, &participant.thread_id),
                    MemythosArenaParent {
                        arena_id: record.arena_id.clone(),
                        thread_id: participant.thread_id.clone(),
                        parent_role: participant.parent_role.clone(),
                        stance_profile: participant.stance_profile.clone(),
                        authority_scope: participant.authority_scope.clone(),
                        lifecycle_state,
                    },
                );
            }
            state.arena_message_deliveries.extend(
                snapshot
                    .deliveries
                    .clone()
                    .into_iter()
                    .map(|delivery| delivery.restore(&record.arena_id)),
            );
            for aggregate in snapshot.aggregates.clone() {
                let (key, aggregate) = aggregate.restore();
                state.arena_message_aggregates.insert(key, aggregate);
            }
            state
                .restored_coordination_snapshots
                .insert(record.arena_id, snapshot);
        }
        Ok(())
    }

    async fn persist_arena_coordination_snapshot(
        &self,
        arena_id: &str,
    ) -> Result<(), JSONRPCErrorError> {
        let Some(state_db) = self.arena_state_db.as_ref() else {
            return Ok(());
        };
        let snapshot = {
            let state = self.state.lock().await;
            let protocol = state
                .arena_lifecycles
                .get(arena_id)
                .ok_or_else(|| {
                    invalid_params(format!(
                        "Arena {arena_id} cannot persist without a canonical lifecycle"
                    ))
                })?
                .protocol_snapshot();
            let mut deliveries = state
                .arena_message_deliveries
                .iter()
                .filter(|delivery| delivery.arena_id == arena_id)
                .map(PersistedArenaDeliveryCheckpoint::capture)
                .collect::<Vec<_>>();
            deliveries.sort_by(|left, right| left.delivery_id.cmp(&right.delivery_id));
            let aggregate_prefix = format!("{arena_id}::");
            let mut aggregates = state
                .arena_message_aggregates
                .iter()
                .filter(|(key, _)| key.starts_with(&aggregate_prefix))
                .map(|(key, aggregate)| PersistedArenaAggregateCheckpoint::capture(key, aggregate))
                .collect::<Vec<_>>();
            aggregates.sort_by(|left, right| left.key.cmp(&right.key));
            if let Some(composition) = state.arena_compositions.get(arena_id) {
                PersistedArenaCoordinationSnapshot {
                    schema_version: ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION,
                    protocol,
                    layer_id: composition.room.layer_id.clone(),
                    room: composition.room.clone(),
                    contract_version: composition.contract.contract_version.clone(),
                    coordination: composition.contract.coordination.clone(),
                    composition_version: composition.composition_version,
                    composition_lifecycle_state: composition.lifecycle_state,
                    leases: composition.leases.clone(),
                    deliveries,
                    aggregates,
                }
            } else {
                let mut restored = state
                    .restored_coordination_snapshots
                    .get(arena_id)
                    .cloned()
                    .ok_or_else(|| {
                        invalid_params(format!(
                            "Arena {arena_id} cannot persist without a coordination checkpoint"
                        ))
                    })?;
                restored.protocol = protocol;
                restored.deliveries = deliveries;
                restored.aggregates = aggregates;
                restored
            }
        };
        let concierge = snapshot
            .room
            .participants
            .iter()
            .find(|participant| participant.parent_role == "room_concierge")
            .ok_or_else(|| invalid_params(format!("Arena {arena_id} has no Room Concierge")))?;
        if concierge.goal_ref.is_none() {
            return Err(invalid_params(format!(
                "Arena {arena_id} Concierge has no OOTB goal reference"
            )));
        }
        let snapshot_json = serde_json::to_string(&snapshot).map_err(|error| {
            invalid_params(format!("failed to serialize Arena {arena_id}: {error}"))
        })?;
        let snapshot_sequence = i64::try_from(snapshot.protocol.sequence)
            .map_err(|_| invalid_params(format!("Arena {arena_id} sequence exceeds i64")))?;
        state_db
            .upsert_arena_snapshot(&ArenaSnapshotRecord {
                arena_id: arena_id.to_string(),
                concierge_thread_id: concierge.thread_id.clone(),
                schema_version: i64::from(ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION),
                snapshot_sequence,
                last_event_hash: arena_snapshot_sha256(&snapshot_json),
                snapshot_json,
                updated_at_ms: Utc::now().timestamp_millis(),
            })
            .await
            .map_err(|error| {
                invalid_params(format!(
                    "failed to persist Arena {arena_id} in app-server state: {error}"
                ))
            })
    }

    async fn prepare_parent_goal_for_delivery(
        &self,
        message: &MemythosArenaMessage,
    ) -> Result<PreparedParentDeliveryGoal, JSONRPCErrorError> {
        let current_goal = self
            .arena_parent_provisioning_adapter
            .read_parent_goal(&message.to_parent_thread_id)
            .await?
            .ok_or_else(|| {
                invalid_params(format!(
                    "parent thread {} has no provisioned goal",
                    message.to_parent_thread_id
                ))
            })?;
        match room_delivery_goal_transition(&current_goal.status) {
            RoomDeliveryGoalTransition::AssignDeliveryGoal => {
                let active_goal = self
                    .arena_parent_provisioning_adapter
                    .transition_parent_goal(
                        &message.to_parent_thread_id,
                        Some(&room_delivery_goal_objective(message)),
                        ThreadGoalStatus::Active,
                        true,
                    )
                    .await?;
                Ok(PreparedParentDeliveryGoal {
                    active_goal,
                    previous_goal: current_goal,
                    assigned_for_delivery: true,
                })
            }
            RoomDeliveryGoalTransition::PreserveGoal => {
                validate_parent_goal_accepts_delivery(&current_goal)?;
                let active_goal = self
                    .arena_parent_provisioning_adapter
                    .transition_parent_goal(
                        &message.to_parent_thread_id,
                        Some(&room_delivery_goal_objective(message)),
                        ThreadGoalStatus::Active,
                        true,
                    )
                    .await?;
                Ok(PreparedParentDeliveryGoal {
                    active_goal,
                    previous_goal: current_goal,
                    assigned_for_delivery: true,
                })
            }
        }
    }

    async fn rollback_parent_goal_after_failed_delivery(
        &self,
        message: &MemythosArenaMessage,
        prepared: &PreparedParentDeliveryGoal,
    ) -> Option<String> {
        if !prepared.assigned_for_delivery {
            return None;
        }
        self.arena_parent_provisioning_adapter
            .transition_parent_goal(
                &message.to_parent_thread_id,
                Some(&prepared.previous_goal.objective),
                prepared.previous_goal.status.clone(),
                false,
            )
            .await
            .err()
            .map(|error| format!("delivery goal rollback also failed: {}", error.message))
    }

    async fn complete_parent_goal_after_successful_delivery(
        &self,
        thread_id: &str,
        message_ids: &[String],
    ) {
        let goal = match self
            .arena_parent_provisioning_adapter
            .read_parent_goal(thread_id)
            .await
        {
            Ok(Some(goal)) => goal,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    thread_id,
                    error = %error.message,
                    "failed to read bounded room-delivery goal after turn completion"
                );
                return;
            }
        };
        if !goal_matches_completed_room_delivery(&goal, message_ids) {
            return;
        }
        if let Err(error) = self
            .arena_parent_provisioning_adapter
            .transition_parent_goal(
                thread_id,
                Some(&goal.objective),
                ThreadGoalStatus::Complete,
                false,
            )
            .await
        {
            warn!(
                thread_id,
                error = %error.message,
                "failed to close bounded room-delivery goal after successful turn"
            );
        }
    }

    pub(crate) async fn runtime_health(
        &self,
        _params: MemythosRuntimeHealthParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        Ok(runtime_health_response(&state).into())
    }

    pub(crate) async fn runtime_close(
        &self,
        params: MemythosRuntimeCloseParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let response = close_runtime(&mut state, params);
        let lifecycle_state = response.lifecycle_state;
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
        Ok(response.into())
    }

    pub(crate) async fn layer_create(
        &self,
        params: MemythosLayerCreateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let layer_id = self.next_id("mem_layer", &self.next_layer_id);
        let layer = create_layer(&mut state, params, layer_id)?;
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
        Ok(MemythosLayerListResponse {
            layers: sorted_layers(&state),
        }
        .into())
    }

    pub(crate) async fn arena_create(
        &self,
        params: MemythosArenaCreateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let arena_id = self.next_id("mem_arena", &self.next_arena_id);
        let arena = create_arena(&mut state, params, arena_id)?;
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
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        Ok(MemythosArenaListResponse {
            arenas: sorted_arenas(&state, params.layer_id.as_deref()),
        }
        .into())
    }

    pub(crate) async fn arena_request(
        &self,
        params: MemythosArenaRequestParams,
        connection_id: ConnectionId,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        if params.case_id.trim().is_empty()
            || params.layer_id.trim().is_empty()
            || params.arena_id.trim().is_empty()
            || params.room_id.trim().is_empty()
            || params.request_origin.trim().is_empty()
            || params.case_brief.trim().is_empty()
            || params.layer_objective.trim().is_empty()
            || params.expected_deliverable.trim().is_empty()
            || params.completion_criteria.is_empty()
            || params.cost_goal.trim().is_empty()
        {
            return Err(invalid_params(
                "arena request requires semantic case, layer, arena, room, origin, objective, deliverable, completion criteria, and cost goal",
            ));
        }
        validate_arena_cost_context(params.cost_context.as_ref())?;
        let previous = {
            let state = self.state.lock().await;
            if state
                .restored_coordination_snapshots
                .contains_key(&params.arena_id)
                && !state.arena_compositions.contains_key(&params.arena_id)
            {
                return Err(invalid_params(format!(
                    "Arena {} was restored from its canonical checkpoint; resume through the same OOTB Room Concierge instead of replanning",
                    params.arena_id
                )));
            }
            state.arena_compositions.get(&params.arena_id).cloned()
        };
        let resume = if let Some(previous) = previous.as_ref() {
            Some(
                self.arena_composition_planning_adapter
                    .assess_resume(&params, previous, connection_id)
                    .await?,
            )
        } else {
            None
        };
        if let Some(resume) = resume.as_ref()
            && resume.assessment.disposition == MemythosArenaResumeDisposition::RetainDecision
        {
            let mut composition = previous
                .clone()
                .expect("retain decision requires an active composition");
            mark_composition_leases_reused(&mut composition);
            return Ok(MemythosArenaRequestResponse {
                request_id: self.next_id("mem_arena_request", &self.next_delivery_id),
                planner_thread_id: resume.planner_thread_id.clone(),
                planner_turn_id: resume.planner_turn_id.clone(),
                composition: composition.into(),
                resume_assessment: resume.assessment.clone(),
                initial_delivery: None,
            }
            .into());
        }
        let (composition, planner_thread_id, planner_turn_id) =
            if resume.as_ref().is_some_and(|resume| {
                resume.assessment.disposition == MemythosArenaResumeDisposition::PartialResume
            }) {
                let resume = resume
                    .as_ref()
                    .expect("partial resume branch requires a native assessment");
                (
                    previous
                        .clone()
                        .expect("partial resume requires an active composition"),
                    resume.planner_thread_id.clone(),
                    resume.planner_turn_id.clone(),
                )
            } else {
                let planned = self
                    .arena_composition_planning_adapter
                    .plan(&params, previous.as_ref(), connection_id)
                    .await?;
                if planned.contract.arena_id != params.arena_id {
                    return Err(invalid_params(format!(
                        "native planner returned arena id {} for requested arena {}",
                        planned.contract.arena_id, params.arena_id
                    )));
                }
                validate_planned_arena_cost_context(&params, &planned.contract)?;
                let mut revision_params = params.clone();
                if revision_params.composition_change_signal.is_none()
                    && let Some(resume) = resume.as_ref()
                {
                    revision_params.composition_change_signal =
                        Some(resume.assessment.rationale.clone());
                }
                let revision = previous
                    .as_ref()
                    .map(|previous| {
                        build_native_composition_revision(
                            &revision_params,
                            previous,
                            &planned.contract,
                        )
                    })
                    .transpose()?;
                let provision = self
                    .arena_composition_provision(
                        MemythosArenaCompositionProvisionParams {
                            case_id: params.case_id.clone(),
                            layer_id: params.layer_id.clone(),
                            room_id: params.room_id.clone(),
                            cwd: params.cwd.clone(),
                            upstream_authority_scope: params.available_authority.clone(),
                            contract: planned.contract,
                            revision,
                        },
                        connection_id,
                    )
                    .await?;
                let ClientResponsePayload::MemythosArenaCompositionProvision(composition) =
                    provision
                else {
                    return Err(invalid_params(
                        "native arena provisioning returned an unexpected response",
                    ));
                };
                (
                    composition,
                    planned.planner_thread_id,
                    planned.planner_turn_id,
                )
            };
        let target_participant_id = composition
            .contract
            .coordination
            .concierge_participant_id
            .as_ref()
            .ok_or_else(|| invalid_params("arena composition has no Room Concierge"))?;
        let target = composition
            .leases
            .iter()
            .find(|lease| &lease.participant_id == target_participant_id)
            .ok_or_else(|| {
                invalid_params(format!(
                    "arena composition has no live lease for intake target {}",
                    target_participant_id
                ))
            })?;
        let resume_assessment = resume.as_ref().map_or_else(
            || MemythosArenaResumeAssessment {
                disposition: MemythosArenaResumeDisposition::InitialRound,
                rationale: "No prior arena composition exists; run the initial round.".to_string(),
                affected_participant_ids: Vec::new(),
                cited_change_refs: Vec::new(),
                affected_decision_refs: Vec::new(),
                comparability_invalidated: false,
                avoided_full_round: false,
                resume_execution_plan: MemythosArenaResumeExecutionPlan {
                    mode: MemythosArenaResumeExecutionMode::InitialRound,
                    affected_participant_ids: Vec::new(),
                    source_round_id: None,
                    affected_decision_refs: Vec::new(),
                    cited_change_refs: Vec::new(),
                },
            },
            |resume| resume.assessment.clone(),
        );
        let request_id = self.next_id("mem_arena_request", &self.next_delivery_id);
        let round_id = if resume.as_ref().is_some_and(|resume| {
            resume.assessment.disposition == MemythosArenaResumeDisposition::PartialResume
        }) {
            format!("{}-resume-{request_id}", params.arena_id)
        } else {
            format!(
                "{}-round-{}",
                params.arena_id, composition.composition_version
            )
        };
        let room_message_ref = format!(
            "app-server://rooms/{}/human-intake/{}",
            params.room_id, request_id
        );
        let delivery_ref = format!("{room_message_ref}/delivery");
        let prompt = build_arena_intake_prompt(
            &params,
            &composition.contract,
            &resume_assessment.resume_execution_plan,
        );
        {
            let mut state = self.state.lock().await;
            state.arena_resume_execution_plans.insert(
                arena_round_key(&params.arena_id, &round_id),
                resume_assessment.resume_execution_plan.clone(),
            );
            if resume_assessment.disposition == MemythosArenaResumeDisposition::PartialResume {
                let lifecycle_state = state
                    .arena_lifecycles
                    .get_mut(&params.arena_id)
                    .ok_or_else(|| {
                        invalid_params(format!(
                            "arena {} has no canonical native lifecycle",
                            params.arena_id
                        ))
                    })?
                    .transition(ArenaCommand::Activate)
                    .map_err(|error| invalid_params(error.to_string()))
                    .map(|_| {
                        state
                            .arena_lifecycles
                            .get(&params.arena_id)
                            .expect("arena lifecycle exists after resume activation")
                            .protocol_state()
                    })?;
                if let Some(arena) = state.arenas.get_mut(&params.arena_id) {
                    arena.lifecycle_state = lifecycle_state;
                }
                if let Some(composition) = state.arena_compositions.get_mut(&params.arena_id) {
                    composition.lifecycle_state =
                        MemythosArenaCompositionLifecycleState::ActiveProposals;
                }
                for parent in state
                    .arena_parents
                    .values_mut()
                    .filter(|parent| parent.arena_id == params.arena_id)
                {
                    parent.lifecycle_state = MemythosArenaLifecycleState::Running;
                }
                for attachment in state
                    .thread_attachments
                    .values_mut()
                    .filter(|attachment| attachment.arena_id == params.arena_id)
                {
                    attachment.lifecycle_state = MemythosArenaLifecycleState::Running;
                }
                self.push_telemetry_ref(
                    &mut state,
                    MemythosTelemetryRefKind::ArenaState,
                    MemythosTelemetrySource::AppServerNative,
                    Some(params.layer_id.clone()),
                    Some(params.arena_id.clone()),
                    None,
                    Some(format!(
                        "app-server://memythos/arenas/{}/rounds/{round_id}/resumed",
                        params.arena_id
                    )),
                    None,
                    MemythosEventChannel::StateTransition,
                    format!(
                        "Arena {} reopened its existing native parent composition for partial resume round {round_id}.",
                        params.arena_id
                    ),
                );
            }
        }
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "memythos_phase".to_string(),
            serde_json::Value::String("arena_intake".to_string()),
        );
        metadata.insert(
            "memythos_request_origin".to_string(),
            serde_json::Value::String(params.request_origin.clone()),
        );
        metadata.insert(
            "memythos_round_id".to_string(),
            serde_json::Value::String(round_id),
        );
        let delivery = self
            .room_send_input_on_connection(
                MemythosRoomSendInputParams {
                    room_id: params.room_id.clone(),
                    room_message_ref,
                    delivery_ref,
                    from_parent_thread_id: None,
                    via_concierge_thread_id: None,
                    to_parent_thread_id: target.thread_id.clone(),
                    source_parent_key: format!("human:{}", params.request_origin),
                    target_parent_key: target.parent_key.clone(),
                    message_kind: "human_intake".to_string(),
                    message_authority: "human_delegated".to_string(),
                    human_instruction: true,
                    response_contract: params.expected_deliverable.clone(),
                    delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                    aggregate_contract: None,
                    client_user_message_id: Some(request_id.clone()),
                    human_summary: params.case_brief.clone(),
                    prompt,
                    metadata,
                    output_schema: None,
                },
                connection_id,
            )
            .await?;
        let ClientResponsePayload::MemythosRoomSendInput(delivery) = delivery else {
            return Err(invalid_params(
                "native arena intake returned an unexpected response",
            ));
        };
        let mut composition = {
            let state = self.state.lock().await;
            state
                .arena_compositions
                .get(&params.arena_id)
                .cloned()
                .unwrap_or(composition)
        };
        if resume.as_ref().is_some_and(|resume| {
            resume.assessment.disposition == MemythosArenaResumeDisposition::PartialResume
        }) {
            mark_composition_leases_reused(&mut composition);
        }
        Ok(MemythosArenaRequestResponse {
            request_id,
            planner_thread_id,
            planner_turn_id,
            composition: composition.into(),
            resume_assessment,
            initial_delivery: Some(delivery.delivery),
        }
        .into())
    }

    pub(crate) async fn arena_composition_provision(
        &self,
        params: MemythosArenaCompositionProvisionParams,
        connection_id: ConnectionId,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        validate_arena_composition_contract(&params)?;
        {
            let state = self.state.lock().await;
            if state
                .restored_coordination_snapshots
                .contains_key(&params.contract.arena_id)
                && !state
                    .arena_compositions
                    .contains_key(&params.contract.arena_id)
            {
                return Err(invalid_params(format!(
                    "Arena {} was restored from its canonical checkpoint; continue through the same OOTB Room Concierge instead of provisioning duplicate parents",
                    params.contract.arena_id
                )));
            }
        }
        for participant in &params.contract.participants {
            self.arena_parent_provisioning_adapter
                .validate_role_stance(&participant.agent_role, &participant.stance)?;
        }

        let previous_composition = {
            let state = self.state.lock().await;
            state
                .arena_compositions
                .get(&params.contract.arena_id)
                .cloned()
        };
        validate_arena_composition_revision(&params, previous_composition.as_ref())?;
        let composition_version = previous_composition
            .as_ref()
            .map_or(1, |previous| previous.composition_version + 1);

        let participant_by_id = params
            .contract
            .participants
            .iter()
            .map(|participant| (participant.participant_id.as_str(), participant))
            .collect::<HashMap<_, _>>();
        let reusable_threads = previous_composition
            .as_ref()
            .zip(params.revision.as_ref())
            .map(|(previous, revision)| {
                revision
                    .actions
                    .iter()
                    .filter(|action| {
                        action.action == MemythosArenaCompositionRevisionActionKind::Keep
                    })
                    .filter_map(|action| {
                        let lease = previous
                            .leases
                            .iter()
                            .find(|lease| lease.participant_id == action.participant_id)?;
                        Some((action.participant_id.clone(), lease.thread_id.clone()))
                    })
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let mut provisioned_parents = Vec::with_capacity(params.contract.participants.len());
        for participant in &params.contract.participants {
            let reusable_thread_id = reusable_threads
                .get(&participant.participant_id)
                .map(String::as_str);
            match self
                .arena_parent_provisioning_adapter
                .provision_parent(&params, participant, reusable_thread_id, connection_id)
                .await
            {
                Ok(parent) => provisioned_parents.push(parent),
                Err(error) => {
                    for parent in provisioned_parents
                        .iter()
                        .filter(|parent| parent.newly_created)
                    {
                        let _ = self
                            .arena_parent_provisioning_adapter
                            .rollback_parent(&parent.thread_id)
                            .await;
                    }
                    return Err(error);
                }
            }
        }

        let mut validated = Vec::with_capacity(provisioned_parents.len());
        let mut proposal_threads = HashSet::new();
        let mut proposal_stances = HashSet::new();
        for provisioned in &provisioned_parents {
            let participant = participant_by_id
                .get(provisioned.participant_id.as_str())
                .copied()
                .ok_or_else(|| {
                    invalid_params(format!(
                        "provisioned parent references unknown participant: {}",
                        provisioned.participant_id
                    ))
                })?;
            let snapshot = self
                .parent_configuration_adapter
                .read_configuration(&provisioned.thread_id)
                .await;
            if !snapshot.blockers.is_empty() {
                for parent in provisioned_parents
                    .iter()
                    .filter(|parent| parent.newly_created)
                {
                    let _ = self
                        .arena_parent_provisioning_adapter
                        .rollback_parent(&parent.thread_id)
                        .await;
                }
                return Err(invalid_params(format!(
                    "thread {} configuration is not valid: {}",
                    provisioned.thread_id,
                    snapshot.blockers.join("; ")
                )));
            }
            let Some(effective_agent_role) = snapshot.agent_role.clone() else {
                for parent in provisioned_parents
                    .iter()
                    .filter(|parent| parent.newly_created)
                {
                    let _ = self
                        .arena_parent_provisioning_adapter
                        .rollback_parent(&parent.thread_id)
                        .await;
                }
                return Err(invalid_params(format!(
                    "thread {} has no effective agent role",
                    provisioned.thread_id
                )));
            };
            if effective_agent_role != participant.agent_role {
                for parent in provisioned_parents
                    .iter()
                    .filter(|parent| parent.newly_created)
                {
                    let _ = self
                        .arena_parent_provisioning_adapter
                        .rollback_parent(&parent.thread_id)
                        .await;
                }
                return Err(invalid_params(format!(
                    "thread {} effective role {} does not match participant role {}",
                    provisioned.thread_id, effective_agent_role, participant.agent_role
                )));
            }
            if snapshot.proposal_bearing == Some(true) {
                proposal_threads.insert(provisioned.thread_id.as_str());
                proposal_stances.insert(participant.stance.as_str());
            }
            validated.push((participant, provisioned, effective_agent_role));
        }

        if is_competitive_method(params.contract.coordination.decision_method) {
            let minimum = params
                .contract
                .coordination
                .round_policy
                .as_ref()
                .map_or(2, |policy| policy.minimum_competing_positions.max(2))
                as usize;
            if proposal_threads.len() < minimum || proposal_stances.len() < minimum {
                for parent in provisioned_parents
                    .iter()
                    .filter(|parent| parent.newly_created)
                {
                    let _ = self
                        .arena_parent_provisioning_adapter
                        .rollback_parent(&parent.thread_id)
                        .await;
                }
                return Err(invalid_params(format!(
                    "competitive arena requires at least {minimum} proposal-bearing parents with independent threads and stances"
                )));
            }
        }

        let participants = validated
            .iter()
            .map(|(participant, provisioned, _)| MemythosRoomParticipant {
                parent_key: arena_parent_key(&params.contract.arena_id, &provisioned.thread_id),
                thread_id: provisioned.thread_id.clone(),
                parent_role: participant.agent_role.clone(),
                stance_profile: participant.stance.clone(),
                goal_ref: Some(provisioned.goal_ref.clone()),
                authority_scope: participant.authority_scope.clone(),
            })
            .collect::<Vec<_>>();
        let room = MemythosRoom {
            room_id: params.room_id.clone(),
            case_id: params.case_id.clone(),
            layer_id: params.layer_id.clone(),
            arena_id: params.contract.arena_id.clone(),
            topology: "parent_peer_room".to_string(),
            participants: participants.clone(),
        };
        let leases = validated
            .iter()
            .map(
                |(participant, provisioned, effective_agent_role)| MemythosArenaCompositionLease {
                    participant_id: participant.participant_id.clone(),
                    parent_key: arena_parent_key(&params.contract.arena_id, &provisioned.thread_id),
                    thread_id: provisioned.thread_id.clone(),
                    role: participant.agent_role.clone(),
                    effective_agent_role: effective_agent_role.clone(),
                    stance: participant.stance.clone(),
                    lease_id: provisioned.lease_id.clone(),
                    lease_source: provisioned.lease_source.clone(),
                    memory_scope: provisioned.memory_scope.clone(),
                    goal_ref: provisioned.goal_ref.clone(),
                    identity_context_version: native_arena_parent_identity_version(&params),
                    identity_context_sha256: native_arena_parent_identity_sha256(
                        &params,
                        participant,
                    ),
                    identity_bootstrap_ref: format!(
                        "app-server://threads/{}/root-developer-instructions",
                        provisioned.thread_id
                    ),
                    effort_intent: participant.effort_intent.clone(),
                    reasoning_effort: participant.reasoning_effort.clone(),
                    token_budget: provisioned.goal.token_budget,
                    goal_status: provisioned.goal.status,
                    status: "active".to_string(),
                },
            )
            .collect::<Vec<_>>();
        let planned_token_budget = if leases.iter().all(|lease| lease.token_budget.is_some()) {
            Some(leases.iter().filter_map(|lease| lease.token_budget).sum())
        } else {
            None
        };
        let event_refs = vec![
            format!(
                "memythos://arenas/{}/compositions/{composition_version}",
                params.contract.arena_id
            ),
            format!("memythos://rooms/{}/registered", params.room_id),
        ];

        // Commit the validated composition as one state mutation. No partial room is observable.
        let mut state = self.state.lock().await;
        let native_lifecycle = state
            .arena_lifecycles
            .entry(params.contract.arena_id.clone())
            .or_insert(
                NativeArenaState::new(params.contract.arena_id.clone()).map_err(|error| {
                    invalid_params(format!("failed to initialize native arena state: {error}"))
                })?,
            );
        let lifecycle_event = native_lifecycle
            .transition(ArenaCommand::Activate)
            .map_err(|error| invalid_params(error.to_string()))?;
        let arena_lifecycle_state = native_lifecycle.protocol_state();
        state
            .thread_attachments
            .retain(|_, attachment| attachment.arena_id != params.contract.arena_id);
        state
            .arena_parents
            .retain(|_, parent| parent.arena_id != params.contract.arena_id);
        let arena = MemythosArena {
            arena_id: params.contract.arena_id.clone(),
            layer_id: params.layer_id.clone(),
            name: params.contract.arena_id.clone(),
            kind: codex_app_server_protocol::MemythosArenaKind::Debate,
            lifecycle_state: arena_lifecycle_state,
            objective: params.contract.shared_objective.clone(),
            participant_ids: participants
                .iter()
                .map(|participant| participant.thread_id.clone())
                .collect(),
        };
        state.arenas.insert(arena.arena_id.clone(), arena);
        state.rooms.insert(room.room_id.clone(), room.clone());
        for ((participant, provisioned, _), room_participant) in
            validated.iter().zip(participants.iter())
        {
            let attachment_id = self.next_id("mem_attach", &self.next_attachment_id);
            state.thread_attachments.insert(
                attachment_id.clone(),
                MemythosThreadAttachment {
                    attachment_id,
                    arena_id: params.contract.arena_id.clone(),
                    thread_id: provisioned.thread_id.clone(),
                    role_id: Some(participant.agent_role.clone()),
                    stance_id: Some(participant.stance.clone()),
                    objective: Some(participant.role_objective.clone()),
                    contract_ref: Some(event_refs[0].clone()),
                    lifecycle_state: MemythosArenaLifecycleState::Running,
                },
            );
            state.arena_parents.insert(
                room_participant.parent_key.clone(),
                MemythosArenaParent {
                    arena_id: params.contract.arena_id.clone(),
                    thread_id: provisioned.thread_id.clone(),
                    parent_role: participant.agent_role.clone(),
                    stance_profile: participant.stance.clone(),
                    authority_scope: participant.authority_scope.clone(),
                    lifecycle_state: MemythosArenaLifecycleState::Running,
                },
            );
        }
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaState,
            MemythosTelemetrySource::AppServerNative,
            Some(params.layer_id.clone()),
            Some(params.contract.arena_id.clone()),
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!(
                "Arena composition {} provisioned atomically with {} native parents at transition {}.",
                params.contract.arena_id,
                participants.len(),
                lifecycle_event.sequence
            ),
        );

        let response = MemythosArenaCompositionProvisionResponse {
            contract: params.contract,
            composition_version,
            lifecycle_state: MemythosArenaCompositionLifecycleState::ActiveProposals,
            applied_revision: params.revision,
            room,
            leases,
            planned_token_budget,
            event_refs,
        };
        state
            .arena_compositions
            .insert(response.contract.arena_id.clone(), response.clone());
        let arena_id = response.contract.arena_id.clone();
        drop(state);
        self.persist_arena_coordination_snapshot(&arena_id).await?;

        Ok(response.into())
    }

    pub(crate) async fn thread_attach(
        &self,
        params: MemythosThreadAttachParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }
        let lifecycle_state = state
            .arena_lifecycles
            .get_mut(&params.arena_id)
            .ok_or_else(|| {
                invalid_params(format!(
                    "arena {} has no canonical native lifecycle",
                    params.arena_id
                ))
            })?
            .transition(ArenaCommand::Activate)
            .map_err(|error| invalid_params(error.to_string()))
            .map(|_| {
                state
                    .arena_lifecycles
                    .get(&params.arena_id)
                    .expect("arena lifecycle exists after activation")
                    .protocol_state()
            })?;
        let arena = state
            .arenas
            .get_mut(&params.arena_id)
            .expect("arena existence checked before lifecycle transition");

        if !arena.participant_ids.contains(&params.thread_id) {
            arena.participant_ids.push(params.thread_id.clone());
        }
        arena.lifecycle_state = lifecycle_state;
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
            .update_arena_phase(params.arena_id, params.round_id, params.phase, true)
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
        self.ensure_arena_state_restored().await?;
        let mut message = params.message;
        {
            let state = self.state.lock().await;
            if let Some(delivery) = state.arena_message_deliveries.iter().find(|delivery| {
                delivery.arena_id == message.arena_id && delivery.message_id == message.message_id
            }) {
                return Ok(MemythosArenaMessageSendResponse {
                    delivery: delivery.clone(),
                }
                .into());
            }
        }
        let (layer_id, aggregate_state, target_reasoning_effort) = {
            let mut state = self.state.lock().await;
            let Some(arena) = state.arenas.get(&message.arena_id) else {
                return Err(invalid_params(format!(
                    "unknown arena id: {}",
                    message.arena_id
                )));
            };
            let layer_id = arena.layer_id.clone();
            let sender_key = arena_parent_key(&message.arena_id, &message.from_parent_thread_id);
            let receiver_key = arena_parent_key(&message.arena_id, &message.to_parent_thread_id);
            if !state.arena_parents.contains_key(&sender_key) {
                return Err(invalid_params(format!(
                    "sender parent {} is not registered in arena {}",
                    message.from_parent_thread_id, message.arena_id
                )));
            }
            if !state.arena_parents.contains_key(&receiver_key) {
                return Err(invalid_params(format!(
                    "receiver parent {} is not registered in arena {}",
                    message.to_parent_thread_id, message.arena_id
                )));
            }

            let aggregate_state = prepare_native_aggregate_delivery(&mut state, &mut message)?;
            apply_native_checkpoint_execution_contract(&state, &mut message)?;
            state
                .arena_messages
                .insert(message.message_id.clone(), message.clone());
            let target_reasoning_effort = arena_parent_reasoning_effort(
                &state,
                &message.arena_id,
                &message.to_parent_thread_id,
            );
            (layer_id, aggregate_state, target_reasoning_effort)
        };

        let prepared_goal = if message.requires_response {
            Some(self.prepare_parent_goal_for_delivery(&message).await?)
        } else {
            None
        };

        let delivery_id = self.next_id("mem_delivery", &self.next_delivery_id);
        let delivery_attempt = self
            .peer_parent_delivery_adapter
            .deliver_peer_parent_message(&message, target_reasoning_effort.clone(), ConnectionId(0))
            .await;
        if delivery_attempt.rejection_reason.is_some()
            && let Some(prepared_goal) = prepared_goal.as_ref()
        {
            let rollback_detail = self
                .rollback_parent_goal_after_failed_delivery(&message, prepared_goal)
                .await;
            if let Some(detail) = rollback_detail {
                return Err(invalid_params(detail));
            }
        }
        let mut state = self.state.lock().await;
        let aggregate_state = finalize_native_aggregate_delivery(
            &mut state,
            &message,
            aggregate_state,
            delivery_attempt.rejection_reason.is_none(),
        );
        let (checkpoint_state, checkpoint_event_refs) =
            native_aggregate_checkpoint_projection(&state, &message);
        let telemetry_channel = delivery_attempt.telemetry_channel;
        let telemetry_summary = delivery_attempt.telemetry_summary.clone();
        let delivery_phase = if message.to_parent_role == "judge"
            && message.requires_response
            && matches!(
                message.delivery_policy,
                Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger)
            ) {
            Some("judge".to_string())
        } else {
            phase_from_message_kind(&message.message_kind)
        };
        let delivery = MemythosArenaMessageDelivery {
            delivery_id: delivery_id.clone(),
            message_id: message.message_id.clone(),
            human_summary: message.human_summary.clone(),
            status: delivery_attempt.status,
            sender_thread_id: message.from_parent_thread_id,
            receiver_thread_id: message.to_parent_thread_id,
            arena_id: message.arena_id,
            round_id: message.round_id,
            phase: delivery_phase,
            delivery_mechanism: delivery_attempt.delivery_mechanism,
            delivery_policy: message.delivery_policy,
            aggregate_id: message
                .aggregate_contract
                .as_ref()
                .map(|contract| contract.aggregate_id.clone()),
            aggregate_state,
            checkpoint_state,
            checkpoint_event_refs,
            receiver_turn_id: delivery_attempt.receiver_turn_id,
            receiver_response_event_ref: delivery_attempt.receiver_response_event_ref,
            delivered_as_human_instruction: delivery_attempt.delivered_as_human_instruction,
            memory_replay_required: delivery_attempt.memory_replay_required,
            event_refs: delivery_attempt.event_refs,
            rejection_reason: delivery_attempt.rejection_reason,
            failure_reason: None,
        };
        state.arena_message_deliveries.push(delivery.clone());
        if let Some(prepared_goal) = prepared_goal.as_ref()
            && let Some(composition) = state.arena_compositions.get_mut(&delivery.arena_id)
            && let Some(lease) = composition
                .leases
                .iter_mut()
                .find(|lease| lease.thread_id == delivery.receiver_thread_id)
        {
            lease.goal_status = prepared_goal.active_goal.status.clone();
        }
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

        drop(state);
        self.persist_arena_coordination_snapshot(&delivery.arena_id)
            .await?;

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

    async fn update_arena_phase(
        &self,
        arena_id: String,
        round_id: String,
        phase: String,
        start: bool,
    ) -> Result<MemythosArenaPhaseUpdate, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let mut state = self.state.lock().await;
        if !state.arenas.contains_key(&arena_id) {
            return Err(invalid_params(format!("unknown arena id: {}", arena_id)));
        }
        let command = if start {
            ArenaCommand::StartPhase {
                round_id: round_id.clone(),
                phase: phase.clone(),
            }
        } else {
            ArenaCommand::ClosePhase {
                round_id: round_id.clone(),
                phase: phase.clone(),
            }
        };
        let lifecycle = state.arena_lifecycles.get_mut(&arena_id).ok_or_else(|| {
            invalid_params(format!(
                "arena {arena_id} has no canonical native lifecycle"
            ))
        })?;
        let event = lifecycle
            .transition(command)
            .map_err(|error| invalid_params(error.to_string()))?;
        let lifecycle_state = lifecycle.protocol_state();
        let arena = state
            .arenas
            .get_mut(&arena_id)
            .expect("arena existence checked before native transition");
        arena.lifecycle_state = lifecycle_state;
        let layer_id = arena.layer_id.clone();
        let arena_id = arena.arena_id.clone();
        let event_ref = format!(
            "app-server://memythos/arenas/{arena_id}/rounds/{round_id}/phases/{phase}/{}?sequence={}",
            event.action(),
            event.sequence
        );
        if event.kind == ArenaEventKind::PhaseClosed {
            let aggregate_prefix = format!("{arena_id}::{round_id}::");
            for aggregate in state
                .arena_message_aggregates
                .iter_mut()
                .filter(|(key, aggregate)| {
                    key.starts_with(&aggregate_prefix) && aggregate.contract.phase_id == phase
                })
                .map(|(_, aggregate)| aggregate)
            {
                if matches!(
                    aggregate.state,
                    MemythosArenaAggregateState::Open
                        | MemythosArenaAggregateState::Collecting
                        | MemythosArenaAggregateState::ReadyByExpectedSources
                        | MemythosArenaAggregateState::ReadyByQuorum
                ) {
                    aggregate.state = MemythosArenaAggregateState::SealedIncomplete;
                    transition_native_checkpoint(
                        aggregate,
                        MemythosArenaCheckpointState::MaterialException,
                    );
                }
            }
        }
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
            format!(
                "Arena {arena_id} phase {phase} {} through canonical native transition {}.",
                event.action(),
                event.sequence
            ),
        );
        let update = MemythosArenaPhaseUpdate {
            arena_id,
            round_id,
            phase,
            lifecycle_state,
            event_refs: vec![event_ref],
        };
        drop(state);
        self.persist_arena_coordination_snapshot(&update.arena_id)
            .await?;
        Ok(update)
    }

    pub(crate) async fn arena_phase_close(
        &self,
        params: MemythosArenaPhaseCloseParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let update = self
            .update_arena_phase(params.arena_id, params.round_id, params.phase, false)
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
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        let arena = state
            .arenas
            .get(&params.arena_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown arena id: {}", params.arena_id)))?;
        let lifecycle_state = state
            .arena_lifecycles
            .get(&params.arena_id)
            .ok_or_else(|| {
                invalid_params(format!(
                    "arena {} has no canonical native lifecycle",
                    params.arena_id
                ))
            })?
            .protocol_state();
        Ok(MemythosArenaRunResponse {
            arena_id: params.arena_id,
            round_id: params.round_id,
            lifecycle_state,
            phase_state_source: "app_server_protocol".to_string(),
            local_ts_arena_state_used: false,
            event_refs: vec![format!(
                "app-server://memythos/arenas/{}/run/{}",
                arena.arena_id, lifecycle_state as u8
            )],
        }
        .into())
    }

    #[cfg_attr(not(test), allow(dead_code))]

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
        let activating_delivery = turn_id.as_deref().and_then(|turn_id| {
            state
                .arena_message_deliveries
                .iter()
                .rev()
                .find(|delivery| {
                    delivery.receiver_thread_id == thread_id
                        && delivery.receiver_turn_id.as_deref() == Some(turn_id)
                })
        });
        let participant_id = native_participant_id_for_thread(&state, &arena_id, &thread_id);
        let event = MemythosRoomActivityEvent {
            cursor: cursor.clone(),
            created_at: Utc::now().to_rfc3339(),
            iteration: 0,
            sequence,
            room_id: room_id.clone(),
            arena_id,
            thread_id,
            turn_id,
            round_id: round_id
                .or_else(|| activating_delivery.map(|delivery| delivery.round_id.clone())),
            phase: phase
                .or_else(|| activating_delivery.and_then(|delivery| delivery.phase.clone())),
            participant_id,
            activation_reason: activating_delivery.map(native_delivery_activation_reason),
            causation_id: activating_delivery.map(|delivery| delivery.message_id.clone()),
            correlation_id: activating_delivery.map(|delivery| delivery.delivery_id.clone()),
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

fn mark_composition_leases_reused(composition: &mut MemythosArenaCompositionProvisionResponse) {
    for lease in &mut composition.leases {
        lease.lease_source = "app_server_native_reused".to_string();
    }
}

fn arena_parent_reasoning_effort(
    state: &MemythosRuntimeState,
    arena_id: &str,
    thread_id: &str,
) -> Option<ReasoningEffort> {
    let (_, leases) = arena_coordination_and_leases(state, arena_id)?;
    leases
        .iter()
        .find(|lease| lease.thread_id == thread_id)
        .map(|lease| lease.reasoning_effort.clone())
}

#[cfg(test)]
#[path = "memythos_processor_tests.rs"]
mod tests;
