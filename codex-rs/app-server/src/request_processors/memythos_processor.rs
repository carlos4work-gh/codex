use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::AdditionalContextEntry;
use codex_app_server_protocol::AdditionalContextKind;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArena;
use codex_app_server_protocol::MemythosArenaAggregateContract;
use codex_app_server_protocol::MemythosArenaAggregateState;
use codex_app_server_protocol::MemythosArenaCheckpointState;
use codex_app_server_protocol::MemythosArenaCompositionCoordination;
use codex_app_server_protocol::MemythosArenaCompositionLease;
use codex_app_server_protocol::MemythosArenaCompositionLifecycleState;
use codex_app_server_protocol::MemythosArenaCompositionProvisionParams;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaCompositionRevisionActionKind;
use codex_app_server_protocol::MemythosArenaCreateParams;
use codex_app_server_protocol::MemythosArenaCreateResponse;
use codex_app_server_protocol::MemythosArenaDecisionMethod;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
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
use codex_app_server_protocol::MemythosLayer;
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
use codex_app_server_protocol::MemythosTokenUsageBreakdown;
use codex_app_server_protocol::MemythosTurnUsageAttribution;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::ThreadTokenUsage;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_core::ThreadManager;
use codex_protocol::AgentPath;
use codex_protocol::ResponseItemId;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_rollout::state_db::StateDbHandle;
use codex_state::ArenaSnapshotRecord;
use codex_state::NativeMailboxCommunicationRecord;
use codex_state::NativeMailboxResolutionAction;
use codex_state::NativeMailboxResolutionAuditRecord;
use codex_state::NativeMailboxResolutionCommand;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tracing::warn;

use crate::error_code::internal_error;
use crate::error_code::invalid_params;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::TurnRequestProcessor;
use crate::request_processors::memythos_arena_state::ArenaCommand;
use crate::request_processors::memythos_arena_state::ArenaEventKind;
use crate::request_processors::memythos_arena_state::NativeArenaProtocolSnapshot;
use crate::request_processors::memythos_arena_state::NativeArenaState;
use crate::request_processors::memythos_composition::*;
use crate::request_processors::memythos_composition_planning::*;
use crate::request_processors::memythos_contracts::*;
use crate::request_processors::memythos_delivery::*;
use crate::request_processors::memythos_judge::*;
use crate::request_processors::memythos_observability::*;
use crate::request_processors::memythos_parent_configuration::*;
use crate::request_processors::memythos_parent_goal::*;
use crate::request_processors::memythos_parent_provisioning::*;
use crate::request_processors::memythos_parent_response::*;
use crate::request_processors::memythos_resume::*;
use crate::request_processors::memythos_thread_consolidation::*;

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
    arena_lifecycles: HashMap<String, NativeArenaState>,
    rooms: HashMap<String, MemythosRoom>,
    thread_attachments: HashMap<String, MemythosThreadAttachment>,
    arena_parents: HashMap<String, MemythosArenaParent>,
    arena_compositions: HashMap<String, MemythosArenaCompositionProvisionResponse>,
    restored_coordination_snapshots: HashMap<String, PersistedArenaCoordinationSnapshot>,
    arena_message_deliveries: Vec<MemythosArenaMessageDelivery>,
    arena_messages: HashMap<String, MemythosArenaMessage>,
    arena_message_aggregates: HashMap<String, NativeArenaMessageAggregate>,
    arena_resume_execution_plans: HashMap<String, MemythosArenaResumeExecutionPlan>,
    room_activity_events: HashMap<String, Vec<MemythosRoomActivityEvent>>,
    native_parent_turn_responses: HashMap<String, ParentTurnResponse>,
    structured_contracts: HashMap<String, MemythosStructuredContract>,
    native_token_usage_refs: HashMap<String, String>,
    native_thread_usage_totals: HashMap<String, MemythosTokenUsageBreakdown>,
    native_turn_usage: HashMap<String, MemythosTurnUsageAttribution>,
    telemetry_refs: Vec<MemythosTelemetryRef>,
}

const ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedArenaCoordinationSnapshot {
    schema_version: u32,
    protocol: NativeArenaProtocolSnapshot,
    layer_id: String,
    room: MemythosRoom,
    contract_version: String,
    coordination: MemythosArenaCompositionCoordination,
    composition_version: u32,
    composition_lifecycle_state: MemythosArenaCompositionLifecycleState,
    leases: Vec<MemythosArenaCompositionLease>,
    deliveries: Vec<PersistedArenaDeliveryCheckpoint>,
    aggregates: Vec<PersistedArenaAggregateCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedArenaDeliveryCheckpoint {
    delivery_id: String,
    message_id: String,
    status: String,
    sender_thread_id: String,
    receiver_thread_id: String,
    round_id: String,
    phase: Option<String>,
    delivery_mechanism: String,
    delivery_policy: Option<MemythosArenaDeliveryPolicy>,
    aggregate_id: Option<String>,
    aggregate_state: Option<MemythosArenaAggregateState>,
    checkpoint_state: Option<MemythosArenaCheckpointState>,
    checkpoint_event_refs: Vec<String>,
    receiver_turn_id: Option<String>,
    receiver_response_event_ref: Option<String>,
    event_refs: Vec<String>,
    rejection_reason: Option<String>,
    failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedArenaAggregateCheckpoint {
    key: String,
    contract: MemythosArenaAggregateContract,
    state: MemythosArenaAggregateState,
    received_source_thread_ids: Vec<String>,
    received_message_ids: Vec<String>,
    trigger_message_id: Option<String>,
    checkpoint_state: MemythosArenaCheckpointState,
    checkpoint_history: Vec<MemythosArenaCheckpointState>,
}

impl PersistedArenaDeliveryCheckpoint {
    fn capture(delivery: &MemythosArenaMessageDelivery) -> Self {
        Self {
            delivery_id: delivery.delivery_id.clone(),
            message_id: delivery.message_id.clone(),
            status: delivery.status.clone(),
            sender_thread_id: delivery.sender_thread_id.clone(),
            receiver_thread_id: delivery.receiver_thread_id.clone(),
            round_id: delivery.round_id.clone(),
            phase: delivery.phase.clone(),
            delivery_mechanism: delivery.delivery_mechanism.clone(),
            delivery_policy: delivery.delivery_policy,
            aggregate_id: delivery.aggregate_id.clone(),
            aggregate_state: delivery.aggregate_state,
            checkpoint_state: delivery.checkpoint_state,
            checkpoint_event_refs: delivery.checkpoint_event_refs.clone(),
            receiver_turn_id: delivery.receiver_turn_id.clone(),
            receiver_response_event_ref: delivery.receiver_response_event_ref.clone(),
            event_refs: delivery.event_refs.clone(),
            rejection_reason: delivery.rejection_reason.clone(),
            failure_reason: delivery.failure_reason.clone(),
        }
    }

    fn restore(self, arena_id: &str) -> MemythosArenaMessageDelivery {
        MemythosArenaMessageDelivery {
            delivery_id: self.delivery_id,
            message_id: self.message_id,
            human_summary: String::new(),
            status: self.status,
            sender_thread_id: self.sender_thread_id,
            receiver_thread_id: self.receiver_thread_id,
            arena_id: arena_id.to_string(),
            round_id: self.round_id,
            phase: self.phase,
            delivery_mechanism: self.delivery_mechanism,
            delivery_policy: self.delivery_policy,
            aggregate_id: self.aggregate_id,
            aggregate_state: self.aggregate_state,
            checkpoint_state: self.checkpoint_state,
            checkpoint_event_refs: self.checkpoint_event_refs,
            receiver_turn_id: self.receiver_turn_id,
            receiver_response_event_ref: self.receiver_response_event_ref,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: self.event_refs,
            rejection_reason: self.rejection_reason,
            failure_reason: self.failure_reason,
        }
    }
}

impl PersistedArenaAggregateCheckpoint {
    fn capture(key: &str, aggregate: &NativeArenaMessageAggregate) -> Self {
        let mut received_source_thread_ids = aggregate
            .received_source_thread_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        received_source_thread_ids.sort();
        let mut received_message_ids = aggregate
            .received_message_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        received_message_ids.sort();
        Self {
            key: key.to_string(),
            contract: aggregate.contract.clone(),
            state: aggregate.state,
            received_source_thread_ids,
            received_message_ids,
            trigger_message_id: aggregate.trigger_message_id.clone(),
            checkpoint_state: aggregate.checkpoint_state,
            checkpoint_history: aggregate.checkpoint_history.clone(),
        }
    }

    fn restore(self) -> (String, NativeArenaMessageAggregate) {
        (
            self.key,
            NativeArenaMessageAggregate {
                contract: self.contract,
                state: self.state,
                received_source_thread_ids: self.received_source_thread_ids.into_iter().collect(),
                received_message_ids: self.received_message_ids.into_iter().collect(),
                trigger_message_id: self.trigger_message_id,
                checkpoint_state: self.checkpoint_state,
                checkpoint_history: self.checkpoint_history,
            },
        )
    }
}

#[derive(Debug, Clone)]
struct NativeArenaMessageAggregate {
    contract: MemythosArenaAggregateContract,
    state: MemythosArenaAggregateState,
    received_source_thread_ids: HashSet<String>,
    received_message_ids: HashSet<String>,
    trigger_message_id: Option<String>,
    checkpoint_state: MemythosArenaCheckpointState,
    checkpoint_history: Vec<MemythosArenaCheckpointState>,
}

#[derive(Debug, Clone)]
struct ArenaClosureCandidate {
    arena_id: String,
    layer_id: String,
    parent_thread_ids: Vec<String>,
    outcome: ArenaTerminalOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArenaTerminalOutcome {
    Close,
    ParentRollup,
}

#[derive(Debug, Clone)]
struct PreparedParentDeliveryGoal {
    active_goal: ThreadGoal,
    previous_goal: ThreadGoal,
    assigned_for_delivery: bool,
}

fn native_phase_turn_refs(
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

fn native_arena_parent_task_contract(
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

fn append_native_arena_parent_task_contract(
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
pub(crate) type NativeMailboxReenqueueFuture<'a> =
    Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

pub(crate) trait PeerParentDeliveryAdapter: Send + Sync {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a>;

    fn reenqueue_native_mailbox_communication<'a>(
        &'a self,
        receiver_thread_id: &'a str,
        communication_id: &'a str,
    ) -> NativeMailboxReenqueueFuture<'a>;
}

#[derive(Debug)]
#[cfg(test)]
struct RecordOnlyPeerParentDeliveryAdapter;

#[cfg(test)]
impl PeerParentDeliveryAdapter for RecordOnlyPeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        _reasoning_effort: Option<ReasoningEffort>,
        _connection_id: ConnectionId,
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

    fn reenqueue_native_mailbox_communication<'a>(
        &'a self,
        _receiver_thread_id: &'a str,
        _communication_id: &'a str,
    ) -> NativeMailboxReenqueueFuture<'a> {
        Box::pin(async { Ok(false) })
    }
}

#[derive(Clone)]
pub(crate) struct NativeMailboxPeerParentDeliveryAdapter {
    turn_processor: TurnRequestProcessor,
    thread_manager: Arc<ThreadManager>,
    state_db: Option<StateDbHandle>,
}

impl NativeMailboxPeerParentDeliveryAdapter {
    pub(crate) fn new(
        turn_processor: TurnRequestProcessor,
        thread_manager: Arc<ThreadManager>,
        state_db: Option<StateDbHandle>,
    ) -> Self {
        Self {
            turn_processor,
            thread_manager,
            state_db,
        }
    }
}

impl PeerParentDeliveryAdapter for NativeMailboxPeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            if message.from_parent_role != "human" {
                return deliver_native_parent_mailbox_message(&self.thread_manager, message).await;
            }
            let request_id = ConnectionRequestId {
                connection_id,
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
                effort: reasoning_effort,
                summary: None,
                personality: None,
                output_schema: message.output_schema.clone(),
                collaboration_mode: None,
                multi_agent_mode: None,
            };

            match self
                .turn_processor
                .turn_start(request_id, params, Some("memythos".to_string()), None)
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

    fn reenqueue_native_mailbox_communication<'a>(
        &'a self,
        receiver_thread_id: &'a str,
        communication_id: &'a str,
    ) -> NativeMailboxReenqueueFuture<'a> {
        Box::pin(async move {
            let thread_id = ThreadId::from_string(receiver_thread_id)
                .map_err(|error| format!("invalid receiver thread id: {error}"))?;
            let state_db = self
                .state_db
                .as_ref()
                .ok_or_else(|| "native mailbox recovery requires sqlite state".to_string())?;
            let record = state_db
                .get_native_mailbox_communication(receiver_thread_id, communication_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "native mailbox communication not found".to_string())?;
            let communication =
                serde_json::from_str::<InterAgentCommunication>(&record.communication_json)
                    .map_err(|error| format!("invalid durable mailbox payload: {error}"))?;
            self.thread_manager
                .reenqueue_inter_agent_communication(thread_id, thread_id, communication)
                .await
                .map(|_| true)
                .map_err(|error| error.to_string())
        })
    }
}

async fn deliver_native_parent_mailbox_message(
    thread_manager: &ThreadManager,
    message: &MemythosArenaMessage,
) -> PeerParentDeliveryAttempt {
    let event_ref = format!(
        "memythos://arenas/{}/rounds/{}/messages/{}",
        message.arena_id, message.round_id, message.message_id
    );
    let target_thread_id = match ThreadId::from_string(&message.to_parent_thread_id) {
        Ok(thread_id) => thread_id,
        Err(error) => {
            return failed_native_mailbox_delivery_attempt(
                message,
                &format!("invalid target parent thread id: {error}"),
            );
        }
    };
    let target_thread = match thread_manager.get_thread(target_thread_id).await {
        Ok(thread) => thread,
        Err(error) => {
            return failed_native_mailbox_delivery_attempt(message, &error.to_string());
        }
    };
    let target_status = target_thread.agent_status().await;
    let trigger_turn = match native_mailbox_wake_policy(&target_status, message.requires_response) {
        Ok(trigger_turn) => trigger_turn,
        Err(reason) => return failed_native_mailbox_delivery_attempt(message, &reason),
    };
    let mut communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        build_peer_parent_envelope(message),
        trigger_turn,
    );
    communication.id = Some(ResponseItemId::from_server(message.message_id.clone()));
    let sender_thread_id = match ThreadId::from_string(&message.from_parent_thread_id) {
        Ok(thread_id) => thread_id,
        Err(error) => {
            return failed_native_mailbox_delivery_attempt(
                message,
                &format!("invalid source parent thread id: {error}"),
            );
        }
    };
    match thread_manager
        .send_inter_agent_communication(sender_thread_id, target_thread_id, communication)
        .await
    {
        Ok(submission_id) => {
            let mechanism = if trigger_turn {
                "native_mailbox_trigger_turn"
            } else {
                "native_mailbox_queue_only"
            };
            PeerParentDeliveryAttempt {
                status: if trigger_turn {
                    "delivered_to_native_mailbox_turn".to_string()
                } else {
                    "queued_in_native_mailbox".to_string()
                },
                delivery_mechanism: mechanism.to_string(),
                receiver_turn_id: trigger_turn.then_some(submission_id.clone()),
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: vec![
                    event_ref,
                    format!(
                        "app-server://threads/{}/mailbox/{}",
                        message.to_parent_thread_id, submission_id
                    ),
                ],
                rejection_reason: None,
                telemetry_channel: MemythosEventChannel::StateTransition,
                telemetry_summary: format!(
                    "Arena message {} delivered through the native app-server mailbox to parent thread {} (trigger_turn={trigger_turn}).",
                    message.message_id, message.to_parent_thread_id
                ),
            }
        }
        Err(error) => failed_native_mailbox_delivery_attempt(message, &error.to_string()),
    }
}

fn native_mailbox_wake_policy(
    target_status: &AgentStatus,
    requires_response: bool,
) -> Result<bool, String> {
    if !requires_response {
        return Ok(false);
    }
    match target_status {
        AgentStatus::Running
        | AgentStatus::PendingInit
        | AgentStatus::Interrupted
        | AgentStatus::Completed(_) => Ok(true),
        AgentStatus::Errored(reason) => Err(format!("target parent is errored: {reason}")),
        AgentStatus::Shutdown => Err("target parent is shutdown".to_string()),
        AgentStatus::NotFound => Err("target parent is not found".to_string()),
    }
}

fn canonical_native_judge_reassessment_contract(
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

fn canonical_native_concierge_refinement_contract(
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

fn apply_native_checkpoint_execution_contract(
    state: &MemythosRuntimeState,
    message: &mut MemythosArenaMessage,
) -> Result<(), JSONRPCErrorError> {
    if message.requires_response
        && message.to_parent_role == "bettor"
        && matches!(
            message.message_kind.as_str(),
            "peer_review_and_objection" | "peer_bet"
        )
    {
        let (_, leases) =
            arena_coordination_and_leases(state, &message.arena_id).ok_or_else(|| {
                invalid_params("mechanism contract requires an Arena coordination checkpoint")
            })?;
        let participant_id = leases
            .iter()
            .find(|lease| lease.thread_id == message.to_parent_thread_id)
            .map(|lease| lease.participant_id.as_str())
            .ok_or_else(|| invalid_params("mechanism contract requires a leased bettor parent"))?;
        let eligible_bettor_ids = leases
            .iter()
            .filter(|lease| lease.role == MemythosParentRole::Bettor.as_wire())
            .map(|lease| lease.participant_id.clone())
            .collect::<Vec<_>>();
        let bettor_thread_ids = leases
            .iter()
            .filter(|lease| eligible_bettor_ids.contains(&lease.participant_id))
            .map(|lease| lease.thread_id.as_str())
            .collect::<HashSet<_>>();
        let own_thread_ids = HashSet::from([message.to_parent_thread_id.as_str()]);
        let peer_thread_ids = bettor_thread_ids
            .iter()
            .copied()
            .filter(|thread_id| *thread_id != message.to_parent_thread_id)
            .collect::<HashSet<_>>();
        let proposal_refs = native_phase_turn_refs(
            state,
            &message.arena_id,
            &message.round_id,
            "proposal",
            &bettor_thread_ids,
        );
        let own_proposal_refs = native_phase_turn_refs(
            state,
            &message.arena_id,
            &message.round_id,
            "proposal",
            &own_thread_ids,
        );
        let peer_proposal_refs = native_phase_turn_refs(
            state,
            &message.arena_id,
            &message.round_id,
            "proposal",
            &peer_thread_ids,
        );
        if proposal_refs.len() != bettor_thread_ids.len()
            || own_proposal_refs.len() != 1
            || peer_proposal_refs.len() != peer_thread_ids.len()
        {
            return Err(invalid_params(
                "mechanism contract requires one native proposal turn ref per bettor",
            ));
        }
        message.execution_prompt = Some(native_bettor_checkpoint_prompt(
            &message.message_kind,
            &message.human_summary,
        ));
        message.response_contract = Some(
            match message.message_kind.as_str() {
                "peer_review_and_objection" => "mechanism_cross_read",
                "peer_bet" => "mechanism_bet",
                _ => unreachable!(),
            }
            .to_string(),
        );
        message.output_schema = Some(match message.message_kind.as_str() {
            "peer_review_and_objection" => native_mechanism_cross_read_output_schema(
                participant_id,
                &eligible_bettor_ids,
                &own_proposal_refs,
                &peer_proposal_refs,
            )?,
            "peer_bet" => {
                let own_cross_read_refs = native_phase_turn_refs(
                    state,
                    &message.arena_id,
                    &message.round_id,
                    "peer_review_and_objection",
                    &own_thread_ids,
                );
                if own_cross_read_refs.len() != 1 {
                    return Err(invalid_params(
                        "mechanism bet contract requires one native cross-read turn ref",
                    ));
                }
                native_mechanism_bet_output_schema(
                    participant_id,
                    &eligible_bettor_ids,
                    &proposal_refs,
                    &own_cross_read_refs,
                )?
            }
            _ => unreachable!(),
        });
        append_native_arena_parent_task_contract(state, message);
        return Ok(());
    }
    if message.requires_response && message.message_kind == "targeted_refinement" {
        let participant_id = state
            .arena_compositions
            .get(&message.arena_id)
            .and_then(|composition| {
                composition
                    .leases
                    .iter()
                    .find(|lease| lease.thread_id == message.to_parent_thread_id)
            })
            .map(|lease| lease.participant_id.as_str())
            .ok_or_else(|| invalid_params("targeted refinement requires a leased bettor parent"))?;
        message.execution_prompt = Some(native_bettor_checkpoint_prompt(
            &message.message_kind,
            &message.human_summary,
        ));
        message.response_contract = Some("refinement_delta".to_string());
        message.output_schema = Some(native_refinement_delta_output_schema(participant_id)?);
        append_native_arena_parent_task_contract(state, message);
        return Ok(());
    }
    if message.requires_response && message.message_kind == "final_verdict_request" {
        let eligible_winner_ids = state
            .arena_compositions
            .get(&message.arena_id)
            .map(|composition| {
                composition
                    .contract
                    .participants
                    .iter()
                    .filter(|participant| participant.agent_role == "bettor")
                    .map(|participant| participant.participant_id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        message.execution_prompt = Some(format!(
            "{}\n\nFinal refinement boundary: issue the definitive verdict from the original sealed bets and the targeted refinement packet. `next_action` must be `close` or `parent_rollup`; targeted_refinements must be empty. Do not start another refinement round.",
            native_judge_checkpoint_prompt(&message.human_summary, &eligible_winner_ids)
        ));
        message.response_contract = Some("final_judge_verdict".to_string());
        let mut schema = native_judge_verdict_output_schema(&eligible_winner_ids)?;
        if let Some(next_action) = schema.pointer_mut("/properties/next_action/enum") {
            *next_action = serde_json::json!(["close", "parent_rollup"]);
        }
        message.output_schema = Some(schema);
        append_native_arena_parent_task_contract(state, message);
        return Ok(());
    }
    if !message.requires_response
        || !matches!(
            message.delivery_policy,
            Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger)
        )
    {
        return Ok(());
    }

    match message.to_parent_role.as_str() {
        "room_concierge" => {
            message.execution_prompt = Some(native_concierge_checkpoint_prompt(
                &message.message_kind,
                &message.human_summary,
            ));
            message.response_contract = Some(format!("{}_checkpoint", message.message_kind));
        }
        "judge" => {
            let eligible_winner_ids = state
                .arena_compositions
                .get(&message.arena_id)
                .map(|composition| {
                    composition
                        .contract
                        .participants
                        .iter()
                        .filter(|participant| participant.agent_role == "bettor")
                        .map(|participant| participant.participant_id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if eligible_winner_ids.is_empty() {
                return Ok(());
            }
            message.execution_prompt = Some(native_judge_checkpoint_prompt(
                &message.human_summary,
                &eligible_winner_ids,
            ));
            message.response_contract = Some("judge_verdict".to_string());
            message.output_schema = Some(native_judge_verdict_output_schema(&eligible_winner_ids)?);
        }
        "bettor" => {
            message.execution_prompt = Some(native_bettor_checkpoint_prompt(
                &message.message_kind,
                &message.human_summary,
            ));
            message.response_contract = Some(format!("{}_response", message.message_kind));
        }
        _ => {}
    }
    append_native_arena_parent_task_contract(state, message);
    Ok(())
}

fn prepare_native_aggregate_delivery(
    state: &mut MemythosRuntimeState,
    message: &mut MemythosArenaMessage,
) -> Result<Option<MemythosArenaAggregateState>, JSONRPCErrorError> {
    let policy = message
        .delivery_policy
        .unwrap_or(if message.requires_response {
            MemythosArenaDeliveryPolicy::Immediate
        } else {
            MemythosArenaDeliveryPolicy::QueueOnly
        });
    message.delivery_policy = Some(policy);
    match policy {
        MemythosArenaDeliveryPolicy::Immediate => {
            message.requires_response = true;
            Ok(None)
        }
        MemythosArenaDeliveryPolicy::QueueOnly => {
            message.requires_response = false;
            Ok(None)
        }
        MemythosArenaDeliveryPolicy::AggregateThenTrigger => {
            let contract = message.aggregate_contract.clone().ok_or_else(|| {
                invalid_params("aggregate_then_trigger requires aggregateContract")
            })?;
            validate_native_aggregate_contract(message, &contract)?;
            let aggregate_key = format!(
                "{}::{}::{}",
                message.arena_id, message.round_id, contract.aggregate_id
            );
            let aggregate = state
                .arena_message_aggregates
                .entry(aggregate_key)
                .or_insert_with(|| NativeArenaMessageAggregate {
                    contract: contract.clone(),
                    state: MemythosArenaAggregateState::Open,
                    received_source_thread_ids: HashSet::new(),
                    received_message_ids: HashSet::new(),
                    trigger_message_id: None,
                    checkpoint_state: MemythosArenaCheckpointState::PhaseOpen,
                    checkpoint_history: vec![MemythosArenaCheckpointState::PhaseOpen],
                });
            if aggregate.contract != contract {
                return Err(invalid_params(format!(
                    "aggregate {} contract changed while collecting",
                    contract.aggregate_id
                )));
            }
            if matches!(
                aggregate.state,
                MemythosArenaAggregateState::RecipientTriggered
                    | MemythosArenaAggregateState::Consumed
                    | MemythosArenaAggregateState::Sealed
                    | MemythosArenaAggregateState::SealedIncomplete
                    | MemythosArenaAggregateState::ExceptionRouted
            ) {
                return match contract.late_arrival_policy {
                    MemythosArenaLateArrivalPolicy::Reject => Err(invalid_params(format!(
                        "aggregate {} is already sealed",
                        contract.aggregate_id
                    ))),
                    MemythosArenaLateArrivalPolicy::QueueWithoutRetrigger => {
                        message.requires_response = false;
                        Ok(Some(aggregate.state))
                    }
                };
            }
            if !aggregate
                .received_message_ids
                .insert(message.message_id.clone())
            {
                return Err(invalid_params(format!(
                    "aggregate {} already received message {}",
                    contract.aggregate_id, message.message_id
                )));
            }
            aggregate
                .received_source_thread_ids
                .insert(message.from_parent_thread_id.clone());
            let all_expected = contract
                .expected_source_thread_ids
                .iter()
                .all(|source| aggregate.received_source_thread_ids.contains(source));
            let quorum_reached =
                aggregate.received_source_thread_ids.len() >= contract.quorum as usize;
            aggregate.state = if all_expected {
                MemythosArenaAggregateState::ReadyByExpectedSources
            } else if quorum_reached {
                MemythosArenaAggregateState::ReadyByQuorum
            } else {
                MemythosArenaAggregateState::Collecting
            };
            if matches!(
                aggregate.state,
                MemythosArenaAggregateState::ReadyByExpectedSources
                    | MemythosArenaAggregateState::ReadyByQuorum
            ) {
                transition_native_checkpoint(
                    aggregate,
                    MemythosArenaCheckpointState::CheckpointReady,
                );
                transition_native_checkpoint(
                    aggregate,
                    MemythosArenaCheckpointState::CheckpointSealed,
                );
            } else {
                transition_native_checkpoint(
                    aggregate,
                    MemythosArenaCheckpointState::CollectingMailboxContributions,
                );
            }
            message.requires_response = matches!(
                aggregate.state,
                MemythosArenaAggregateState::ReadyByExpectedSources
                    | MemythosArenaAggregateState::ReadyByQuorum
            );
            if message.requires_response {
                aggregate.trigger_message_id = Some(message.message_id.clone());
            }
            Ok(Some(aggregate.state))
        }
    }
}

fn finalize_native_aggregate_delivery(
    state: &mut MemythosRuntimeState,
    message: &MemythosArenaMessage,
    prepared_state: Option<MemythosArenaAggregateState>,
    delivered: bool,
) -> Option<MemythosArenaAggregateState> {
    let contract = message.aggregate_contract.as_ref()?;
    let aggregate_key = format!(
        "{}::{}::{}",
        message.arena_id, message.round_id, contract.aggregate_id
    );
    let aggregate = state.arena_message_aggregates.get_mut(&aggregate_key)?;
    if !delivered {
        aggregate.state = MemythosArenaAggregateState::ExceptionRouted;
        transition_native_checkpoint(aggregate, MemythosArenaCheckpointState::MaterialException);
        transition_native_checkpoint(
            aggregate,
            MemythosArenaCheckpointState::ConciergeExceptionHandling,
        );
    } else if message.requires_response
        && matches!(
            prepared_state,
            Some(
                MemythosArenaAggregateState::ReadyByExpectedSources
                    | MemythosArenaAggregateState::ReadyByQuorum
            )
        )
    {
        aggregate.state = MemythosArenaAggregateState::RecipientTriggered;
        transition_native_checkpoint(
            aggregate,
            if message.to_parent_role == "room_concierge" {
                MemythosArenaCheckpointState::ConciergeSynthesis
            } else {
                MemythosArenaCheckpointState::NextPhaseDispatched
            },
        );
    }
    Some(aggregate.state)
}

fn transition_native_checkpoint(
    aggregate: &mut NativeArenaMessageAggregate,
    next: MemythosArenaCheckpointState,
) {
    if aggregate.checkpoint_state != next {
        aggregate.checkpoint_state = next;
        aggregate.checkpoint_history.push(next);
    }
}

fn native_aggregate_checkpoint_projection(
    state: &MemythosRuntimeState,
    message: &MemythosArenaMessage,
) -> (Option<MemythosArenaCheckpointState>, Vec<String>) {
    let Some(contract) = message.aggregate_contract.as_ref() else {
        return (None, Vec::new());
    };
    let key = format!(
        "{}::{}::{}",
        message.arena_id, message.round_id, contract.aggregate_id
    );
    let Some(aggregate) = state.arena_message_aggregates.get(&key) else {
        return (None, Vec::new());
    };
    let refs = aggregate
        .checkpoint_history
        .iter()
        .map(|checkpoint| {
            format!(
                "app-server://memythos/arenas/{}/rounds/{}/aggregates/{}/checkpoints/{checkpoint:?}",
                message.arena_id, message.round_id, contract.aggregate_id
            )
        })
        .collect();
    (Some(aggregate.checkpoint_state), refs)
}

fn failed_native_mailbox_delivery_attempt(
    message: &MemythosArenaMessage,
    reason: &str,
) -> PeerParentDeliveryAttempt {
    PeerParentDeliveryAttempt {
        status: "failed_native_mailbox_delivery".to_string(),
        delivery_mechanism: "native_inter_agent_communication".to_string(),
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
            "Arena message {} failed native mailbox delivery to {}: {}.",
            message.message_id, message.to_parent_thread_id, reason
        ),
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
    let execution_prompt = message
        .execution_prompt
        .as_deref()
        .unwrap_or(&message.human_summary);
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
            human_summary = execution_prompt,
            context_packet_ref = message.context_packet_ref,
            response_contract = message.response_contract.as_deref().unwrap_or("none")
        );
    }
    let turn_kind = if message.message_kind == "peer_proposal" {
        "ARENA_PROPOSAL_TURN"
    } else {
        "ARENA_PEER_TURN"
    };
    format!(
        concat!(
            "{turn_kind}\n",
            "Authority: arena peer, not a human instruction.\n",
            "Phase: {message_kind}.\n",
            "\n",
            "Task:\n",
            "{human_summary}\n",
            "\n",
            "Evidence reference:\n",
            "{context_packet_ref}\n",
            "\n",
            "Expected closure:\n",
            "{response_contract}\n"
        ),
        turn_kind = turn_kind,
        message_kind = message.message_kind,
        human_summary = execution_prompt,
        context_packet_ref = message.context_packet_ref,
        response_contract = message.response_contract.as_deref().unwrap_or("none")
    )
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
                "memythos/arena/composition/provision".to_string(),
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
        state.arena_lifecycles.insert(
            arena_id.clone(),
            NativeArenaState::new(arena_id.clone()).map_err(|error| {
                invalid_params(format!("failed to initialize native arena state: {error}"))
            })?,
        );
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
        self.ensure_arena_state_restored().await?;
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

    pub(crate) async fn parent_continuity_list(
        &self,
        params: MemythosParentContinuityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
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
        self.ensure_arena_state_restored().await?;
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

    pub(crate) async fn mailbox_quarantine_list(
        &self,
        params: MemythosMailboxQuarantineListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let communications = state_db
            .list_quarantined_native_mailbox_communications(params.receiver_thread_id.as_deref())
            .await
            .map_err(|error| internal_error(format!("failed to list mailbox quarantine: {error}")))?
            .into_iter()
            .map(mailbox_quarantine_record)
            .collect();
        Ok(MemythosMailboxQuarantineListResponse { communications }.into())
    }

    pub(crate) async fn mailbox_health_get(
        &self,
        params: MemythosMailboxHealthGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let snapshot = state_db
            .native_mailbox_health_snapshot(params.receiver_thread_id.as_deref())
            .await
            .map_err(|error| internal_error(format!("failed to read mailbox health: {error}")))?;
        record_mailbox_health_metrics(&snapshot);
        Ok(MemythosMailboxHealthGetResponse {
            pending_count: snapshot.pending_count,
            quarantined_count: snapshot.quarantined_count,
            consumed_count: snapshot.consumed_count,
            skipped_count: snapshot.skipped_count,
            aborted_count: snapshot.aborted_count,
            max_attempt_count: snapshot.max_attempt_count,
            oldest_pending_updated_at_ms: snapshot.oldest_pending_updated_at_ms,
            oldest_quarantined_updated_at_ms: snapshot.oldest_quarantined_updated_at_ms,
            resolution_count: snapshot.resolution_count,
        }
        .into())
    }

    pub(crate) async fn mailbox_quarantine_get(
        &self,
        params: MemythosMailboxQuarantineGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let communication = state_db
            .get_native_mailbox_communication(&params.receiver_thread_id, &params.communication_id)
            .await
            .map_err(|error| internal_error(format!("failed to read mailbox quarantine: {error}")))?
            .filter(|record| record.status == "quarantined")
            .ok_or_else(|| invalid_params("unknown quarantined mailbox communication"))?;
        Ok(MemythosMailboxQuarantineGetResponse {
            communication: mailbox_quarantine_record(communication),
        }
        .into())
    }

    pub(crate) async fn mailbox_quarantine_resolve(
        &self,
        params: MemythosMailboxQuarantineResolveParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let (action, action_label) = match params.action {
            MemythosMailboxQuarantineResolutionAction::Retry => {
                (NativeMailboxResolutionAction::Retry, "retry")
            }
            MemythosMailboxQuarantineResolutionAction::Skip => {
                (NativeMailboxResolutionAction::Skip, "skip")
            }
            MemythosMailboxQuarantineResolutionAction::Replace => {
                (NativeMailboxResolutionAction::Replace, "replace")
            }
            MemythosMailboxQuarantineResolutionAction::Abort => {
                (NativeMailboxResolutionAction::Abort, "abort")
            }
        };
        let now = chrono::Utc::now().timestamp_millis();
        let replacement = params
            .replacement_message
            .map(|message| {
                if message.to_parent_thread_id != params.receiver_thread_id {
                    return Err(invalid_params(
                        "replacement message must target the same receiver thread",
                    ));
                }
                let mut communication = InterAgentCommunication::new(
                    AgentPath::root(),
                    AgentPath::root(),
                    Vec::new(),
                    build_peer_parent_envelope(&message),
                    message.requires_response,
                );
                communication.id = Some(ResponseItemId::from_server(message.message_id.clone()));
                let communication_json =
                    serde_json::to_string(&communication).map_err(|error| {
                        internal_error(format!("failed to serialize replacement message: {error}"))
                    })?;
                Ok(NativeMailboxCommunicationRecord {
                    receiver_thread_id: params.receiver_thread_id.clone(),
                    communication_id: message.message_id.clone(),
                    source_call_id: Some(message.message_id),
                    submission_id: None,
                    payload_hash: format!(
                        "sha256:{:x}",
                        Sha256::digest(communication_json.as_bytes())
                    ),
                    communication_json,
                    status: "pending".to_string(),
                    attempt_count: 0,
                    failure_fingerprint: None,
                    last_progress_ref: None,
                    quarantine_reason: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                })
            })
            .transpose()?;
        let resolution_started = Instant::now();
        let outcome = match state_db
            .resolve_native_mailbox_quarantine(&NativeMailboxResolutionCommand {
                receiver_thread_id: params.receiver_thread_id,
                communication_id: params.communication_id,
                command_id: params.command_id,
                action,
                actor: params.actor,
                reason: params.reason,
                replacement,
                created_at_ms: now,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                record_mailbox_resolution_metrics(
                    action_label,
                    "operational_error",
                    "not_applicable",
                    resolution_started.elapsed(),
                );
                return Err(invalid_params(format!(
                    "failed to resolve quarantine: {error}"
                )));
            }
        };
        let live_reenqueue_status = if action == NativeMailboxResolutionAction::Retry {
            if outcome.conflict {
                "not_enqueued_conflict".to_string()
            } else if outcome.existing {
                "already_resolved".to_string()
            } else {
                match self
                    .peer_parent_delivery_adapter
                    .reenqueue_native_mailbox_communication(
                        &outcome.receiver_thread_id,
                        &outcome.communication_id,
                    )
                    .await
                {
                    Ok(true) => "enqueued".to_string(),
                    Ok(false) => "deferred_until_resume".to_string(),
                    Err(error) => {
                        warn!("live native mailbox retry deferred: {error}");
                        "deferred_until_resume".to_string()
                    }
                }
            }
        } else {
            "not_applicable".to_string()
        };
        let resolution_outcome = if outcome.conflict {
            "conflict"
        } else if outcome.existing {
            "existing"
        } else {
            "winner"
        };
        record_mailbox_resolution_metrics(
            action_label,
            resolution_outcome,
            &live_reenqueue_status,
            resolution_started.elapsed(),
        );
        Ok(MemythosMailboxQuarantineResolveResponse {
            receiver_thread_id: outcome.receiver_thread_id,
            communication_id: outcome.communication_id,
            command_id: outcome.command_id,
            action: outcome.action,
            resulting_status: outcome.resulting_status,
            replacement_communication_id: outcome.replacement_communication_id,
            existing: outcome.existing,
            live_reenqueue_status,
            conflict: outcome.conflict,
            winner_command_id: outcome.winner_command_id,
        }
        .into())
    }

    pub(crate) async fn mailbox_resolution_list(
        &self,
        params: MemythosMailboxResolutionListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let after_id = params
            .cursor
            .as_deref()
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|_| invalid_params("invalid resolution cursor"))?;
        let limit = params.limit.unwrap_or(50).clamp(1, 100) as usize;
        let mut records = state_db
            .list_native_mailbox_resolution_audit(
                params.receiver_thread_id.as_deref(),
                params.communication_id.as_deref(),
                after_id,
                (limit + 1) as i64,
            )
            .await
            .map_err(|error| internal_error(format!("failed to list resolution audit: {error}")))?;
        let has_more = records.len() > limit;
        records.truncate(limit);
        let next_cursor = has_more
            .then(|| records.last().map(|record| record.id.to_string()))
            .flatten();
        Ok(MemythosMailboxResolutionListResponse {
            resolutions: records
                .into_iter()
                .map(mailbox_resolution_audit_record)
                .collect(),
            next_cursor,
        }
        .into())
    }

    pub(crate) async fn mailbox_resolution_get(
        &self,
        params: MemythosMailboxResolutionGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let resolution = state_db
            .get_native_mailbox_resolution_audit(&params.receiver_thread_id, &params.command_id)
            .await
            .map_err(|error| internal_error(format!("failed to read resolution audit: {error}")))?
            .ok_or_else(|| invalid_params("unknown mailbox resolution command"))?;
        Ok(MemythosMailboxResolutionGetResponse {
            resolution: mailbox_resolution_audit_record(resolution),
        }
        .into())
    }

    pub(crate) async fn arena_message_read(
        &self,
        params: MemythosArenaMessageReadParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        let message = state
            .arena_messages
            .get(&params.message_id)
            .filter(|message| message.arena_id == params.arena_id)
            .cloned()
            .ok_or_else(|| {
                invalid_params(format!(
                    "unknown message {} in arena {}",
                    params.message_id, params.arena_id
                ))
            })?;
        let delivered_prompt = build_peer_parent_envelope(&message);
        Ok(MemythosArenaMessageReadResponse {
            message,
            delivered_prompt,
        }
        .into())
    }

    pub(crate) async fn arena_message_observation_list(
        &self,
        params: MemythosArenaMessageObservationListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
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
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        let mut arena = state
            .arenas
            .get(&params.arena_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown arena id: {}", params.arena_id)))?;
        let protocol_snapshot = state
            .arena_lifecycles
            .get(&params.arena_id)
            .ok_or_else(|| {
                invalid_params(format!(
                    "arena {} has no canonical native lifecycle",
                    params.arena_id
                ))
            })?
            .protocol_snapshot();
        arena.lifecycle_state = NativeArenaState::restore_protocol_snapshot(protocol_snapshot)
            .map_err(|error| invalid_params(error.to_string()))?
            .protocol_state();
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
                delivery_policy: None,
                aggregate_contract: None,
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
        let target_turn_id = delivery_response.delivery.turn_id.clone().ok_or_else(|| {
            invalid_params("cross-room delivery did not start a target turn".to_string())
        })?;
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
        mut args: MemythosRoomToolSendMessageArgs,
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

        let (room, source, target, inherited_round_id, inherited_phase, decision_method) = {
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
            let decision_method = state
                .arena_compositions
                .get(&room.arena_id)
                .map(|composition| composition.contract.coordination.decision_method.clone());
            let native_judge_bet = source.parent_role == "bettor"
                && args.message_kind == "peer_bet"
                && decision_method
                    .as_ref()
                    .is_some_and(|method| is_competitive_method(*method));
            let direct_aggregate_target = native_judge_bet
                || matches!(
                    args.delivery_policy,
                    Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger)
                ) && args.target_parent_key.is_some();
            let target = if native_judge_bet {
                room.participants
                    .iter()
                    .find(|participant| participant.parent_role == "judge")
                    .cloned()
                    .ok_or_else(|| {
                        invalid_params(format!(
                            "room {} has no judge parent for native bet aggregation",
                            room.room_id
                        ))
                    })?
            } else if source.parent_role == "room_concierge" || direct_aggregate_target {
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
            (
                room,
                source,
                target,
                inherited_round_id,
                inherited_phase,
                decision_method,
            )
        };
        validate_room_message_kind(decision_method.as_ref(), &args.message_kind)?;
        validate_room_message_route(
            decision_method.as_ref(),
            &args.message_kind,
            &source.parent_role,
            &target.parent_role,
        )?;
        let (eligible_winner_ids, existing_native_judge_turn, target_task_contract) = {
            let state = self.state.lock().await;
            let composition = state.arena_compositions.get(&room.arena_id);
            validate_resume_execution_message(
                &state,
                &room,
                &inherited_round_id,
                &args.message_kind,
                &source,
                &target,
            )?;
            validate_competitive_round_progress(
                decision_method.as_ref(),
                &args.message_kind,
                &room,
                composition,
                &state.arena_message_deliveries,
            )?;
            let eligible_winner_ids = composition
                .map(|composition| {
                    composition
                        .contract
                        .participants
                        .iter()
                        .filter(|participant| participant.agent_role == "bettor")
                        .map(|participant| participant.participant_id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let existing_native_judge_turn = (args.message_kind == "verdict_request")
                .then(|| {
                    state
                        .arena_message_deliveries
                        .iter()
                        .rev()
                        .find(|delivery| {
                            delivery.arena_id == room.arena_id
                                && delivery.receiver_thread_id == target.thread_id
                                && delivery.aggregate_id.is_some()
                                && delivery.receiver_turn_id.is_some()
                                && matches!(
                                    delivery.aggregate_state,
                                    Some(
                                        MemythosArenaAggregateState::RecipientTriggered
                                            | MemythosArenaAggregateState::Consumed
                                    )
                                )
                        })
                        .and_then(|delivery| delivery.receiver_turn_id.clone())
                })
                .flatten();
            let target_task_contract =
                native_arena_parent_task_contract(&state, &room.arena_id, &target.thread_id);
            (
                eligible_winner_ids,
                existing_native_judge_turn,
                target_task_contract,
            )
        };
        if target.thread_id == current_thread_id {
            return Err(invalid_params(
                "room message target must be a different parent thread".to_string(),
            ));
        }
        if let Some(target_turn_id) = existing_native_judge_turn {
            let (response_item_ref, response_text, event_refs) = self
                .await_parent_turn_response(&target.thread_id, &target_turn_id)
                .await?;
            return Ok(MemythosRoomToolResponse {
                room_id: room.room_id,
                target_parent_key: target.parent_key,
                target_thread_id: target.thread_id,
                target_turn_id,
                response_item_ref,
                response_text,
                event_refs,
            });
        }

        let activates_native_judge = source.parent_role == "bettor"
            && target.parent_role == "judge"
            && args.message_kind == "peer_bet"
            && decision_method
                .as_ref()
                .is_some_and(|method| is_competitive_method(*method));
        let activates_native_concierge_checkpoint = source.parent_role == "bettor"
            && target.parent_role == "room_concierge"
            && matches!(
                args.message_kind.as_str(),
                "peer_proposal" | "peer_review_and_objection"
            )
            && decision_method
                .as_ref()
                .is_some_and(|method| is_competitive_method(*method));
        let asynchronous_phase_dispatch = source.parent_role == "room_concierge"
            && target.parent_role == "bettor"
            && matches!(
                args.message_kind.as_str(),
                "peer_proposal" | "peer_review_and_objection" | "peer_bet"
            )
            && decision_method
                .as_ref()
                .is_some_and(|method| is_competitive_method(*method));
        if activates_native_judge {
            args.delivery_policy = Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger);
            args.aggregate_contract = Some(canonical_native_judge_bet_contract(
                &room,
                &inherited_round_id,
                &target,
            )?);
            args.response_contract = "judge_verdict".to_string();
        } else if activates_native_concierge_checkpoint {
            args.delivery_policy = Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger);
            args.aggregate_contract = Some(canonical_native_concierge_phase_contract(
                &room,
                &inherited_round_id,
                &target,
                &args.message_kind,
            )?);
            args.response_contract = format!("{}_checkpoint", args.message_kind);
        } else if asynchronous_phase_dispatch {
            // The concierge returns asynchronously, but each assigned parent must
            // still receive trigger-turn work. Core serializes it if a turn is active.
            args.delivery_policy = Some(MemythosArenaDeliveryPolicy::Immediate);
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
        let mut execution_prompt = if activates_native_concierge_checkpoint {
            native_concierge_checkpoint_prompt(&args.message_kind, &args.message)
        } else if (args.message_kind == "verdict_request" || activates_native_judge)
            && !eligible_winner_ids.is_empty()
        {
            native_judge_checkpoint_prompt(&args.message, &eligible_winner_ids)
        } else {
            args.message.clone()
        };
        if let Some(task_contract) = target_task_contract {
            execution_prompt.push_str("\n\n");
            execution_prompt.push_str(&task_contract);
        }
        let output_schema = if (args.message_kind == "verdict_request" || activates_native_judge)
            && !eligible_winner_ids.is_empty()
        {
            Some(native_judge_verdict_output_schema(&eligible_winner_ids)?)
        } else {
            None
        };
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
                delivery_policy: args.delivery_policy,
                aggregate_contract: args.aggregate_contract,
                client_user_message_id: Some(message_id),
                human_summary: args.message.clone(),
                prompt: execution_prompt,
                metadata,
                output_schema,
            })
            .await?;
        let ClientResponsePayload::MemythosRoomSendInput(delivery_response) = payload else {
            return Err(invalid_params(
                "native room tool received an unexpected delivery response".to_string(),
            ));
        };

        if matches!(
            delivery_response.delivery.delivery_policy,
            Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger)
        ) {
            return Ok(MemythosRoomToolResponse {
                room_id: room.room_id,
                target_parent_key: target.parent_key,
                target_thread_id: target.thread_id,
                target_turn_id: delivery_response
                    .delivery
                    .turn_id
                    .unwrap_or_else(|| "mailbox_queued".to_string()),
                response_item_ref: delivery_response.delivery.delivery_ref,
                response_text: format!(
                    "Contribution accepted by aggregate mailbox with status {}. The recipient will run once when the checkpoint is sealed.",
                    delivery_response.delivery.status
                ),
                event_refs: delivery_response.delivery.event_refs,
            });
        }
        let target_turn_id = delivery_response.delivery.turn_id.clone().ok_or_else(|| {
            invalid_params("room delivery did not start a target turn".to_string())
        })?;
        if asynchronous_phase_dispatch {
            return Ok(MemythosRoomToolResponse {
                room_id: room.room_id,
                target_parent_key: target.parent_key.clone(),
                target_thread_id: target.thread_id,
                target_turn_id,
                response_item_ref: delivery_response.delivery.delivery_ref,
                response_text: format!(
                    "Phase assignment dispatched asynchronously to {}. Its response will return through the native aggregate checkpoint; end this concierge turn without waiting.",
                    target.parent_key
                ),
                event_refs: delivery_response.delivery.event_refs,
            });
        }
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
        let mut completed_without_message_since = None;
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

            let mut native_failure_reason = None;
            {
                let state = self.state.lock().await;
                if let Some(delivery) = state.arena_message_deliveries.iter().find(|delivery| {
                    delivery.receiver_thread_id == target_thread_id
                        && delivery.receiver_turn_id.as_deref() == Some(target_turn_id)
                }) {
                    native_failure_reason = delivery.failure_reason.clone();
                }
            }

            match native_delivery_status.as_deref() {
                Some("receiver_turn_failed") => {
                    return Err(invalid_params(format!(
                        "parent turn {target_turn_id} failed{}",
                        native_failure_reason
                            .as_deref()
                            .map(|reason| format!(": {reason}"))
                            .unwrap_or_default()
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
                    return Err(invalid_params(format!(
                        "parent turn {target_turn_id} failed"
                    )));
                }
                Some(TurnStatus::Interrupted) => {
                    return Err(invalid_params(format!(
                        "parent turn {target_turn_id} was interrupted"
                    )));
                }
                Some(TurnStatus::InProgress) | None => {
                    completed_without_message_since = None;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub(crate) async fn room_send_input_on_connection(
        &self,
        params: MemythosRoomSendInputParams,
        connection_id: ConnectionId,
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
            execution_prompt: Some(params.prompt.clone()),
            context_packet_ref: params.room_message_ref.clone(),
            artifact_refs: vec![params.delivery_ref.clone()],
            requires_response: true,
            delivery_policy: params.delivery_policy,
            aggregate_contract: params.aggregate_contract.clone(),
            response_contract: Some(params.response_contract.clone()),
            output_schema: params.output_schema.clone(),
        };
        if !params.human_instruction
            && !matches!(
                message.delivery_policy,
                None | Some(MemythosArenaDeliveryPolicy::Immediate)
            )
        {
            let payload = self
                .arena_message_send(MemythosArenaMessageSendParams {
                    message: message.clone(),
                })
                .await?;
            let ClientResponsePayload::MemythosArenaMessageSend(response) = payload else {
                return Err(invalid_params(
                    "native mailbox delivery returned an unexpected response".to_string(),
                ));
            };
            return Ok(MemythosRoomSendInputResponse {
                delivery: MemythosRoomSendInputDelivery {
                    delivery_id: response.delivery.delivery_id,
                    thread_id: params.to_parent_thread_id,
                    turn_id: response.delivery.receiver_turn_id,
                    round_id: message.round_id,
                    event_refs: response.delivery.event_refs,
                    room_id: params.room_id,
                    room_message_ref: params.room_message_ref,
                    delivery_ref: params.delivery_ref,
                    delivery_mechanism: response.delivery.delivery_mechanism,
                    human_instruction: false,
                    message_authority: params.message_authority,
                    status: response.delivery.status,
                    delivery_policy: response.delivery.delivery_policy,
                    aggregate_state: response.delivery.aggregate_state,
                },
            }
            .into());
        }
        let target_reasoning_effort = {
            let state = self.state.lock().await;
            arena_parent_reasoning_effort(&state, &room.arena_id, &target.thread_id)
        };
        let delivery_id = self.next_id("mem_room_delivery", &self.next_delivery_id);
        let prepared_goal = self.prepare_parent_goal_for_delivery(&message).await?;
        let delivery_attempt = self
            .peer_parent_delivery_adapter
            .deliver_peer_parent_message(&message, target_reasoning_effort.clone(), connection_id)
            .await;
        let Some(target_turn_id) = delivery_attempt.receiver_turn_id.clone() else {
            let rollback_detail = self
                .rollback_parent_goal_after_failed_delivery(&message, &prepared_goal)
                .await
                .map(|detail| format!("; {detail}"))
                .unwrap_or_default();
            return Err(invalid_params(format!(
                "room sendInput failed to create target turn: {}{}",
                delivery_attempt
                    .rejection_reason
                    .clone()
                    .unwrap_or_else(|| "unknown delivery failure".to_string()),
                rollback_detail
            )));
        };
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
        // The explicit room act is the native phase source of truth. The caller's
        // inherited phase is only a fallback for non-debate message kinds.
        let delivery_phase = phase_from_message_kind(&message.message_kind).or_else(|| {
            params
                .metadata
                .get("memythos_phase")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
        });

        let delivery = MemythosArenaMessageDelivery {
            delivery_id: delivery_id.clone(),
            message_id: message.message_id.clone(),
            human_summary: message.human_summary.clone(),
            status: "delivered_to_live_thread".to_string(),
            sender_thread_id: source_thread_id,
            receiver_thread_id: params.to_parent_thread_id.clone(),
            arena_id: room.arena_id.clone(),
            round_id: message.round_id.clone(),
            phase: delivery_phase.clone(),
            delivery_mechanism: "room_loopback_send_input".to_string(),
            delivery_policy: message.delivery_policy,
            aggregate_id: None,
            aggregate_state: None,
            checkpoint_state: None,
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some(target_turn_id.clone()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: params.human_instruction,
            memory_replay_required: false,
            event_refs: event_refs.clone(),
            rejection_reason: None,
            failure_reason: None,
        };
        let mut state = self.state.lock().await;
        state
            .arena_messages
            .insert(message.message_id.clone(), message.clone());
        if let Some(composition) = state.arena_compositions.get_mut(&room.arena_id)
            && let Some(lease) = composition
                .leases
                .iter_mut()
                .find(|lease| lease.thread_id == params.to_parent_thread_id)
        {
            lease.goal_status = prepared_goal.active_goal.status.clone();
        }
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
            if params.human_instruction {
                "human_like"
            } else {
                "parent_mailbox"
            },
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
        drop(state);

        Ok(MemythosRoomSendInputResponse {
            delivery: MemythosRoomSendInputDelivery {
                delivery_id,
                thread_id: params.to_parent_thread_id,
                turn_id: Some(target_turn_id),
                round_id: message.round_id,
                event_refs,
                room_id: params.room_id,
                room_message_ref: params.room_message_ref,
                delivery_ref: params.delivery_ref,
                delivery_mechanism: "room_loopback_send_input".to_string(),
                human_instruction: params.human_instruction,
                message_authority: params.message_authority,
                status: "delivered_to_live_thread".to_string(),
                delivery_policy: message.delivery_policy,
                aggregate_state: None,
            },
        }
        .into())
    }

    pub(crate) async fn room_send_input(
        &self,
        params: MemythosRoomSendInputParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.room_send_input_on_connection(params, ConnectionId(0))
            .await
    }

    pub(crate) async fn room_send_on_connection(
        &self,
        params: MemythosRoomSendInputParams,
        connection_id: ConnectionId,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let payload = self
            .room_send_input_on_connection(params, connection_id)
            .await?;
        let ClientResponsePayload::MemythosRoomSendInput(mut response) = payload else {
            return Ok(payload);
        };
        response.delivery.delivery_mechanism = "room_loopback_send".to_string();
        Ok(ClientResponsePayload::MemythosRoomSendInput(response))
    }

    #[cfg(test)]
    pub(crate) async fn room_send(
        &self,
        params: MemythosRoomSendInputParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.room_send_on_connection(params, ConnectionId(0)).await
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

        let assembly = self
            .thread_consolidation_adapter
            .consolidate_threads(&MemythosThreadConsolidateParams {
                coordinator_thread_id: params.coordinator_thread_id.clone(),
                source_thread_ids: params.source_thread_ids.clone(),
                since_cursors: params.since_cursors.clone(),
                items_view: params.items_view.clone(),
                purpose: MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
                authority_mode: MemythosThreadConsolidationAuthorityMode::PeerCoordination,
                instructions: params.instructions.clone(),
                per_source_limit: params.per_source_limit,
                client_user_message_id: params.client_user_message_id.clone(),
                output_schema: params.output_schema.clone(),
            })
            .await;

        let contract_id = self.next_id("mem_contract", &self.next_contract_id);
        let producer_turn_id = assembly
            .consolidation_turn_id
            .clone()
            .unwrap_or_else(|| format!("unavailable-{contract_id}"));
        let contract_ref = format!(
            "app-server://threads/{}/turns/{}/contracts/{}",
            params.coordinator_thread_id, producer_turn_id, contract_id
        );
        let structured_output_ref = assembly.structured_output_ref.clone();
        let schema_ref = format!(
            "app-server://schemas/{}/v1",
            sanitize_contract_ref_segment(&params.contract_kind)
        );
        let source_refs = assembly.source_refs.clone();
        let technical_evidence_refs = compact_event_refs(
            vec![
                format!(
                    "app-server://threads/{}/memythos/contracts/{}/instructions",
                    params.coordinator_thread_id, contract_id
                ),
                format!(
                    "app-server://threads/{}/memythos/contracts/{}/schema",
                    params.coordinator_thread_id, contract_id
                ),
            ]
            .into_iter()
            .chain(assembly.technical_evidence_refs.clone())
            .collect(),
        );
        let source_evidence_refs = contract_source_evidence_refs(
            &source_refs,
            &technical_evidence_refs,
            assembly.agent_message_ref.as_deref(),
            structured_output_ref.as_deref(),
        );
        let payload = params.output_schema.as_ref().map(|schema| {
            serde_json::json!({
                "contract_kind": params.contract_kind,
                "schema_ref": schema_ref,
                "output_schema": schema,
                "structured_output_ref": structured_output_ref,
                "source_evidence_refs": source_evidence_refs,
                "assembly_status": if assembly.blockers.is_empty() { "running" } else { "blocked" }
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
            missing_evidence: if structured_output_ref.is_none() {
                vec!["structured_output_ref".to_string()]
            } else {
                Vec::new()
            },
            blockers: assembly.blockers.clone(),
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
            agent_message_ref: assembly.agent_message_ref,
            structured_output_ref,
            technical_evidence_refs,
            source_method: "memythos/thread/contract/assemble".to_string(),
            used_thread_turns_summary: assembly.used_thread_turns_summary,
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
        let (
            room,
            arena_lifecycle_state,
            mut deliveries,
            room_activity_events,
            token_usage_refs,
            turn_usage,
        ) = {
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
            let turn_usage = state
                .native_turn_usage
                .values()
                .filter(|usage| usage.arena_id == room.arena_id)
                .filter(|usage| participant_thread_ids.contains(usage.thread_id.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            let arena_lifecycle_state = state
                .arenas
                .get(&room.arena_id)
                .map(|arena| arena.lifecycle_state);
            (
                room,
                arena_lifecycle_state,
                deliveries,
                room_activity_events,
                token_usage_refs,
                turn_usage,
            )
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
        let clean_close = arena_lifecycle_state == Some(MemythosArenaLifecycleState::ClosedCleanly)
            && active_turns == 0
            && failed_turns == 0;
        let awaiting_parent = arena_lifecycle_state
            == Some(MemythosArenaLifecycleState::AwaitingParent)
            && active_turns == 0
            && failed_turns == 0;
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
                            || event.channel == "parent_mailbox"
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
        let recorded_native_turn_responses = {
            let state = self.state.lock().await;
            state.native_parent_turn_responses.clone()
        };
        let turns = deliveries
            .iter()
            .filter_map(|delivery| {
                let native_response = delivery.receiver_turn_id.as_ref().and_then(|turn_id| {
                    recorded_native_turn_responses
                        .get(&native_token_usage_key(
                            &delivery.receiver_thread_id,
                            turn_id,
                        ))
                        .or_else(|| {
                            native_turn_responses
                                .get(&(delivery.receiver_thread_id.clone(), turn_id.clone()))
                        })
                });
                if delivery.status == "receiver_turn_completed"
                    && delivery.receiver_turn_id.is_some()
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
                } else if awaiting_parent {
                    "awaiting_parent".to_string()
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
                total: sum_memythos_usage(turn_usage.iter().map(|usage| &usage.usage)),
                turns: turn_usage,
                cost_weighted_usage: None,
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
                    matches!(event.channel.as_str(), "human_like" | "parent_mailbox")
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
        failure_reason: Option<String>,
        last_agent_message: Option<String>,
    ) -> bool {
        if let Err(error) = self.ensure_arena_state_restored().await {
            warn!(error = %error.message, "failed to restore Arena state before turn completion");
            return false;
        }
        let (matched_delivery, arena_id, loopbacks, completed_delivery_message_ids) = {
            let mut state = self.state.lock().await;
            let Some((layer_id, arena_id)) = find_attachment_context(&state, thread_id) else {
                return false;
            };

            let native_event_ref =
                format!("app-server://threads/{thread_id}/turns/{turn_id}/completed");
            if let Some(text) = last_agent_message
                .as_ref()
                .filter(|text| !text.trim().is_empty())
            {
                let response = state
                    .native_parent_turn_responses
                    .entry(native_token_usage_key(thread_id, turn_id))
                    .or_insert_with(|| ParentTurnResponse {
                        status: None,
                        request_item_ref: None,
                        request_text: None,
                        item_ref: None,
                        text: None,
                    });
                response.status = Some(TurnStatus::Completed);
                response.text = Some(text.clone());
                response
                    .item_ref
                    .get_or_insert_with(|| native_event_ref.clone());
            }
            let mut matched_delivery = false;
            let mut completed_aggregates = Vec::new();
            let mut completed_delivery_message_ids = Vec::new();
            for delivery in state
                .arena_message_deliveries
                .iter_mut()
                .filter(|delivery| {
                    delivery.receiver_thread_id == thread_id
                        && delivery.receiver_turn_id.as_deref() == Some(turn_id)
                })
            {
                matched_delivery = true;
                if status == "completed" {
                    completed_delivery_message_ids.push(delivery.message_id.clone());
                }
                delivery.status = match status {
                    "completed" => "receiver_turn_completed".to_string(),
                    "failed" => "receiver_turn_failed".to_string(),
                    "interrupted" => "receiver_turn_interrupted".to_string(),
                    _ => format!("receiver_turn_{status}"),
                };
                delivery.receiver_response_event_ref = Some(native_event_ref.clone());
                delivery.failure_reason = failure_reason.clone();
                if status == "completed"
                    && let Some(aggregate_id) = delivery.aggregate_id.as_ref()
                {
                    completed_aggregates.push((
                        delivery.arena_id.clone(),
                        delivery.round_id.clone(),
                        aggregate_id.clone(),
                    ));
                    delivery.aggregate_state = Some(MemythosArenaAggregateState::Consumed);
                }
                if !delivery.event_refs.contains(&native_event_ref) {
                    delivery.event_refs.push(native_event_ref.clone());
                }
            }
            for (arena_id, round_id, aggregate_id) in completed_aggregates {
                let key = format!("{arena_id}::{round_id}::{aggregate_id}");
                if let Some(aggregate) = state.arena_message_aggregates.get_mut(&key) {
                    aggregate.state = MemythosArenaAggregateState::Consumed;
                    transition_native_checkpoint(
                        aggregate,
                        MemythosArenaCheckpointState::NextPhaseDispatched,
                    );
                }
                for delivery in state
                    .arena_message_deliveries
                    .iter_mut()
                    .filter(|delivery| {
                        delivery.arena_id == arena_id
                            && delivery.round_id == round_id
                            && delivery.aggregate_id.as_deref() == Some(aggregate_id.as_str())
                    })
                {
                    delivery.status = "receiver_turn_completed".to_string();
                    delivery.aggregate_state = Some(MemythosArenaAggregateState::Consumed);
                    delivery.receiver_response_event_ref = Some(native_event_ref.clone());
                }
            }

            let detail_ref = completed_at.map(|completed_at| {
                format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/completed_at/{completed_at}"
                )
            });
            let summary = match (duration_ms, failure_reason.as_deref()) {
                (Some(duration_ms), Some(reason)) => format!(
                    "Native turn {turn_id} for thread {thread_id} completed with status {status} in {duration_ms}ms: {reason}"
                ),
                (None, Some(reason)) => format!(
                    "Native turn {turn_id} for thread {thread_id} completed with status {status}: {reason}"
                ),
                (Some(duration_ms), None) => format!(
                    "Native turn {turn_id} for thread {thread_id} completed with status {status} in {duration_ms}ms."
                ),
                (None, None) => format!(
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

            let loopbacks = if status == "completed" {
                native_turn_loopback_candidates(&state, thread_id, turn_id, &native_event_ref)
            } else {
                Vec::new()
            };
            (
                matched_delivery,
                arena_id,
                loopbacks,
                completed_delivery_message_ids,
            )
        };

        if status == "completed" && !completed_delivery_message_ids.is_empty() {
            self.complete_parent_goal_after_successful_delivery(
                thread_id,
                &completed_delivery_message_ids,
            )
            .await;
        }

        for loopback_message in loopbacks {
            if let Err(error) = self
                .arena_message_send(MemythosArenaMessageSendParams {
                    message: loopback_message,
                })
                .await
            {
                warn!(
                    thread_id,
                    turn_id,
                    error = %error.message,
                    "failed to deliver native arena turn completion loopback"
                );
            }
        }

        if status == "completed" {
            // A completed turn can materialize the final queue-only loopback (for example the
            // judge verdict). Closure must observe that delivery, not the state from before it.
            let closure_candidate = {
                let state = self.state.lock().await;
                arena_closure_candidate(&state, &arena_id, thread_id)
            };
            if let Some(candidate) = closure_candidate {
                self.terminalize_arena_parent_goals(candidate).await;
            }
        }

        if matched_delivery
            && let Err(error) = self.persist_arena_coordination_snapshot(&arena_id).await
        {
            warn!(
                arena_id,
                error = %error.message,
                "failed to persist Arena state after turn completion"
            );
        }

        matched_delivery
    }

    pub(crate) async fn record_native_parent_agent_message(
        &self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text: String,
    ) -> bool {
        let mut state = self.state.lock().await;
        let matched_delivery = state.arena_message_deliveries.iter().any(|delivery| {
            delivery.receiver_thread_id == thread_id
                && delivery.receiver_turn_id.as_deref() == Some(turn_id)
        });
        if !matched_delivery {
            return false;
        }

        let item_ref = format!("app-server://threads/{thread_id}/turns/{turn_id}/items/{item_id}");
        state.native_parent_turn_responses.insert(
            native_token_usage_key(thread_id, turn_id),
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: None,
                request_text: None,
                item_ref: Some(item_ref.clone()),
                text: Some(text),
            },
        );
        for delivery in state
            .arena_message_deliveries
            .iter_mut()
            .filter(|delivery| {
                delivery.receiver_thread_id == thread_id
                    && delivery.receiver_turn_id.as_deref() == Some(turn_id)
            })
        {
            delivery.receiver_response_event_ref = Some(item_ref.clone());
            if !delivery.event_refs.contains(&item_ref) {
                delivery.event_refs.push(item_ref.clone());
            }
        }
        true
    }

    async fn terminalize_arena_parent_goals(&self, candidate: ArenaClosureCandidate) {
        let mut original_goals = Vec::with_capacity(candidate.parent_thread_ids.len());
        for parent_thread_id in &candidate.parent_thread_ids {
            let goal = match self
                .arena_parent_provisioning_adapter
                .read_parent_goal(parent_thread_id)
                .await
            {
                Ok(Some(goal)) => goal,
                Ok(None) => {
                    warn!(
                        arena_id = candidate.arena_id,
                        parent_thread_id,
                        "cannot terminalize native arena because a parent goal is missing"
                    );
                    return;
                }
                Err(error) => {
                    warn!(
                        arena_id = candidate.arena_id,
                        parent_thread_id,
                        error = %error.message,
                        "cannot terminalize native arena because a parent goal could not be read"
                    );
                    return;
                }
            };
            original_goals.push(goal);
        }

        let mut transitioned: Vec<ThreadGoal> = Vec::new();
        for goal in &original_goals {
            if goal.status != ThreadGoalStatus::Complete {
                if let Err(error) = self
                    .arena_parent_provisioning_adapter
                    .transition_parent_goal(
                        &goal.thread_id,
                        Some(&goal.objective),
                        ThreadGoalStatus::Complete,
                        false,
                    )
                    .await
                {
                    warn!(
                        arena_id = candidate.arena_id,
                        parent_thread_id = goal.thread_id,
                        error = %error.message,
                        "cannot terminalize native arena because a parent goal transition failed"
                    );
                    for transitioned_goal in transitioned.iter().rev() {
                        if let Err(rollback_error) = self
                            .arena_parent_provisioning_adapter
                            .transition_parent_goal(
                                &transitioned_goal.thread_id,
                                Some(&transitioned_goal.objective),
                                transitioned_goal.status.clone(),
                                false,
                            )
                            .await
                        {
                            warn!(
                                arena_id = candidate.arena_id,
                                parent_thread_id = transitioned_goal.thread_id,
                                error = %rollback_error.message,
                                "failed to roll back parent goal after native arena terminalization failure"
                            );
                        }
                    }
                    return;
                }
                transitioned.push(goal.clone());
            }
        }

        let mut state = self.state.lock().await;
        let (command, composition_state, state_ref, summary) = match candidate.outcome {
            ArenaTerminalOutcome::Close => (
                ArenaCommand::CloseCleanly,
                MemythosArenaCompositionLifecycleState::Closed,
                "closed-cleanly",
                format!(
                    "Arena {} closed cleanly after every native parent goal reached complete.",
                    candidate.arena_id
                ),
            ),
            ArenaTerminalOutcome::ParentRollup => (
                ArenaCommand::AwaitParent,
                MemythosArenaCompositionLifecycleState::BlockedAuthority,
                "awaiting-parent",
                format!(
                    "Arena {} completed its local round and awaits an authority contract from its parent layer.",
                    candidate.arena_id
                ),
            ),
        };
        let native_lifecycle = state
            .arena_lifecycles
            .get_mut(&candidate.arena_id)
            .expect("terminal arena candidate requires canonical native lifecycle");
        let lifecycle_event = match native_lifecycle.transition(command) {
            Ok(event) => event,
            Err(error) => {
                warn!(
                    arena_id = candidate.arena_id,
                    error = %error,
                    "cannot terminalize native arena because its canonical transition was rejected"
                );
                return;
            }
        };
        let arena_state = native_lifecycle.protocol_state();
        if let Some(arena) = state.arenas.get_mut(&candidate.arena_id) {
            arena.lifecycle_state = arena_state;
        }
        if let Some(composition) = state.arena_compositions.get_mut(&candidate.arena_id) {
            composition.lifecycle_state = composition_state;
        }
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaState,
            MemythosTelemetrySource::AppServerNative,
            Some(candidate.layer_id),
            Some(candidate.arena_id.clone()),
            None,
            Some(format!(
                "app-server://memythos/arenas/{}/{state_ref}?sequence={}",
                candidate.arena_id, lifecycle_event.sequence,
            )),
            None,
            MemythosEventChannel::StateTransition,
            summary,
        );
    }

    pub(crate) async fn record_native_token_usage(
        &self,
        thread_id: &str,
        turn_id: &str,
        token_usage: &ThreadTokenUsage,
    ) -> bool {
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
        let current_total = memythos_usage_breakdown(&token_usage.total);
        let previous_total = state
            .native_thread_usage_totals
            .insert(thread_id.to_string(), current_total.clone())
            .unwrap_or_default();
        let delta = subtract_memythos_usage(&current_total, &previous_total);
        let usage_key = native_token_usage_key(thread_id, turn_id);
        let activating_delivery = state
            .arena_message_deliveries
            .iter()
            .rev()
            .find(|delivery| {
                delivery.receiver_thread_id == thread_id
                    && delivery.receiver_turn_id.as_deref() == Some(turn_id)
            });
        let round_id = activating_delivery.map(|delivery| delivery.round_id.clone());
        let phase = activating_delivery.and_then(|delivery| delivery.phase.clone());
        let activation_reason = activating_delivery.map(native_delivery_activation_reason);
        let causation_id = activating_delivery.map(|delivery| delivery.message_id.clone());
        let correlation_id = activating_delivery.map(|delivery| delivery.delivery_id.clone());
        let parent = state
            .arena_parents
            .get(&arena_parent_key(&arena_id, thread_id));
        let parent_role = parent.map(|parent| parent.parent_role.clone());
        let stance_profile = parent.map(|parent| parent.stance_profile.clone());
        let goal_ref = state
            .rooms
            .values()
            .filter(|room| room.arena_id == arena_id)
            .flat_map(|room| room.participants.iter())
            .find(|participant| participant.thread_id == thread_id)
            .and_then(|participant| participant.goal_ref.clone());
        let participant_id = native_participant_id_for_thread(&state, &arena_id, thread_id);
        state
            .native_turn_usage
            .entry(usage_key)
            .and_modify(|usage| add_memythos_usage(&mut usage.usage, &delta))
            .or_insert_with(|| MemythosTurnUsageAttribution {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                arena_id: arena_id.clone(),
                round_id,
                phase,
                parent_role,
                stance_profile,
                goal_ref,
                activation_reason,
                participant_id,
                causation_id,
                correlation_id,
                usage: delta,
                cost_weighted_usage: None,
                evidence_outcome: "not_available".to_string(),
                event_ref: native_event_ref.clone(),
            });
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

fn mailbox_quarantine_record(
    record: NativeMailboxCommunicationRecord,
) -> MemythosMailboxQuarantineRecord {
    MemythosMailboxQuarantineRecord {
        receiver_thread_id: record.receiver_thread_id,
        communication_id: record.communication_id,
        source_call_id: record.source_call_id,
        payload_hash: record.payload_hash,
        status: record.status,
        attempt_count: record.attempt_count,
        failure_fingerprint: record.failure_fingerprint,
        last_progress_ref: record.last_progress_ref,
        quarantine_reason: record.quarantine_reason,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    }
}

fn mailbox_resolution_audit_record(
    record: NativeMailboxResolutionAuditRecord,
) -> MemythosMailboxResolutionAuditRecord {
    MemythosMailboxResolutionAuditRecord {
        id: record.id,
        receiver_thread_id: record.receiver_thread_id,
        communication_id: record.communication_id,
        command_id: record.command_id,
        resolution_generation: record.resolution_generation,
        action: record.action,
        actor: record.actor,
        reason: record.reason,
        pre_status: record.pre_status,
        pre_attempt_count: record.pre_attempt_count,
        pre_failure_fingerprint: record.pre_failure_fingerprint,
        pre_last_progress_ref: record.pre_last_progress_ref,
        pre_quarantine_reason: record.pre_quarantine_reason,
        pre_payload_hash: record.pre_payload_hash,
        resulting_status: record.resulting_status,
        replacement_communication_id: record.replacement_communication_id,
        created_at_ms: record.created_at_ms,
    }
}

fn native_delivery_activation_reason(delivery: &MemythosArenaMessageDelivery) -> String {
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

fn native_participant_id_for_thread(
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
        failure_reason: delivery.failure_reason.clone(),
        items,
    })
}

fn native_turn_loopback_candidate(
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

fn native_turn_loopback_candidates(
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

fn native_arena_intake_assignments(
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

fn validate_room_message_kind(
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

fn validate_room_message_route(
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

fn validate_competitive_round_progress(
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

fn validate_resume_execution_message(
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

fn arena_round_key(arena_id: &str, round_id: &str) -> String {
    format!("{arena_id}::{round_id}")
}

fn mark_composition_leases_reused(composition: &mut MemythosArenaCompositionProvisionResponse) {
    for lease in &mut composition.leases {
        lease.lease_source = "app_server_native_reused".to_string();
    }
}

fn arena_coordination_and_leases<'a>(
    state: &'a MemythosRuntimeState,
    arena_id: &str,
) -> Option<(
    &'a MemythosArenaCompositionCoordination,
    &'a [MemythosArenaCompositionLease],
)> {
    if let Some(composition) = state.arena_compositions.get(arena_id) {
        return Some((&composition.contract.coordination, &composition.leases));
    }
    state
        .restored_coordination_snapshots
        .get(arena_id)
        .map(|snapshot| (&snapshot.coordination, snapshot.leases.as_slice()))
}

fn arena_closure_candidate(
    state: &MemythosRuntimeState,
    arena_id: &str,
    completion_trigger_thread_id: &str,
) -> Option<ArenaClosureCandidate> {
    let arena = state.arenas.get(arena_id)?;
    if arena.lifecycle_state == MemythosArenaLifecycleState::ClosedCleanly {
        return None;
    }
    let (coordination, leases) = arena_coordination_and_leases(state, arena_id)?;
    let coordinator_id = coordination.concierge_participant_id.as_ref()?;
    let coordinator_thread_id = leases
        .iter()
        .find(|lease| &lease.participant_id == coordinator_id)?
        .thread_id
        .as_str();
    if !leases
        .iter()
        .any(|lease| lease.thread_id == completion_trigger_thread_id)
    {
        return None;
    }
    let active_round_id = state
        .arena_message_deliveries
        .iter()
        .rev()
        .find(|delivery| {
            delivery.arena_id == arena_id
                && delivery.receiver_thread_id == coordinator_thread_id
                && delivery.delivered_as_human_instruction
        })?
        .round_id
        .as_str();
    let deliveries = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| delivery.arena_id == arena_id && delivery.round_id == active_round_id)
        .collect::<Vec<_>>();
    if deliveries.is_empty()
        || deliveries
            .iter()
            .any(|delivery| delivery.rejection_reason.is_some())
        || deliveries.iter().any(|delivery| {
            delivery
                .receiver_turn_id
                .as_deref()
                .is_some_and(|turn_id| turn_id != "mailbox_queued")
                && delivery.status != "receiver_turn_completed"
        })
    {
        return None;
    }

    let mut terminal_outcome = ArenaTerminalOutcome::Close;
    if is_competitive_method(coordination.decision_method) {
        let policy = coordination.round_policy.as_ref()?;
        let minimum_positions = policy.minimum_competing_positions as usize;
        let distinct_completed_targets = |phase: &str| {
            deliveries
                .iter()
                .filter(|delivery| delivery.phase.as_deref() == Some(phase))
                .map(|delivery| delivery.receiver_thread_id.as_str())
                .collect::<HashSet<_>>()
                .len()
        };
        let judge_id = coordination.judge_participant_id.as_ref()?;
        let judge_thread_id = leases
            .iter()
            .find(|lease| &lease.participant_id == judge_id)?
            .thread_id
            .as_str();
        let eligible_winner_ids = leases
            .iter()
            .filter(|lease| lease.role == MemythosParentRole::Bettor.as_wire())
            .map(|lease| lease.participant_id.as_str())
            .collect::<HashSet<_>>();
        let execution_plan = state
            .arena_resume_execution_plans
            .get(&arena_round_key(arena_id, active_round_id));
        if execution_plan.is_some_and(|plan| {
            plan.mode == MemythosArenaResumeExecutionMode::ReassessAffectedPositions
        }) {
            let execution_plan = execution_plan.expect("partial resume plan was checked above");
            let affected_ids = execution_plan
                .affected_participant_ids
                .iter()
                .collect::<HashSet<_>>();
            let expected_affected_threads = leases
                .iter()
                .filter(|lease| {
                    lease.role == MemythosParentRole::Bettor.as_wire()
                        && affected_ids.contains(&lease.participant_id)
                })
                .map(|lease| lease.thread_id.as_str())
                .collect::<HashSet<_>>();
            let completed_affected_threads = deliveries
                .iter()
                .filter(|delivery| {
                    delivery.phase.as_deref() == Some("resume_reassessment")
                        && delivery.receiver_thread_id != judge_thread_id
                        && delivery.status == "receiver_turn_completed"
                })
                .map(|delivery| delivery.receiver_thread_id.as_str())
                .collect::<HashSet<_>>();
            if expected_affected_threads.is_empty()
                || completed_affected_threads != expected_affected_threads
            {
                return None;
            }
        } else {
            if distinct_completed_targets("proposal") < minimum_positions
                || distinct_completed_targets("peer_review_and_objection") < minimum_positions
                || distinct_completed_targets("bet") < minimum_positions
                || distinct_completed_targets("judge") < 1
            {
                return None;
            }
        }
        terminal_outcome = deliveries.iter().find_map(|delivery| {
            if delivery.receiver_thread_id != judge_thread_id
                || !matches!(
                    delivery.phase.as_deref(),
                    Some("judge") | Some("final_judge")
                )
                || delivery.status != "receiver_turn_completed"
            {
                return None;
            }
            let turn_id = delivery.receiver_turn_id.as_deref()?;
            let text = state
                .native_parent_turn_responses
                .get(&native_token_usage_key(judge_thread_id, turn_id))?
                .text
                .as_deref()?;
            match native_judge_next_action(text, &eligible_winner_ids).as_deref() {
                Some("close") => Some(ArenaTerminalOutcome::Close),
                Some("parent_rollup") => Some(ArenaTerminalOutcome::ParentRollup),
                _ => None,
            }
        })?;
    }

    Some(ArenaClosureCandidate {
        arena_id: arena_id.to_string(),
        layer_id: arena.layer_id.clone(),
        parent_thread_ids: leases.iter().map(|lease| lease.thread_id.clone()).collect(),
        outcome: terminal_outcome,
    })
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

#[cfg(test)]
#[path = "memythos_processor_tests.rs"]
mod tests;
