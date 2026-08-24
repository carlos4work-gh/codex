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
use crate::request_processors::memythos_port_error::ArenaPortError;
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
#[path = "memythos_processor/provisioning_handlers.rs"]
mod provisioning_handlers;
#[path = "memythos_processor/query_handlers.rs"]
mod query_handlers;
#[path = "memythos_processor/room_handlers.rs"]
mod room_handlers;
#[path = "memythos_processor/runtime_handlers.rs"]
mod runtime_handlers;
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
        let (_, lifecycle_state) = state
            .transition_arena_lifecycle(&params.arena_id, ArenaCommand::Activate)
            .map_err(|error| invalid_params(error.to_string()))?;
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
        validate_peer_parent_delivery_attempt(&message, &delivery_attempt)
            .map_err(|error| ArenaPortError::contract_rejected(error.to_string()))
            .map_err(|error| error.into_jsonrpc("peer_delivery"))?;
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
        let (event, lifecycle_state) = state
            .transition_arena_lifecycle(&arena_id, command)
            .map_err(|error| invalid_params(error.to_string()))?;
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
