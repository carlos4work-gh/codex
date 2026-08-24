use std::collections::HashMap;
use std::collections::HashSet;

use codex_app_server_protocol::MemythosArena;
use codex_app_server_protocol::MemythosArenaAggregateContract;
use codex_app_server_protocol::MemythosArenaAggregateState;
use codex_app_server_protocol::MemythosArenaCheckpointState;
use codex_app_server_protocol::MemythosArenaCompositionCoordination;
use codex_app_server_protocol::MemythosArenaCompositionLease;
use codex_app_server_protocol::MemythosArenaCompositionLifecycleState;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosArenaMessageDelivery;
use codex_app_server_protocol::MemythosArenaParent;
use codex_app_server_protocol::MemythosArenaResumeExecutionPlan;
use codex_app_server_protocol::MemythosLayer;
use codex_app_server_protocol::MemythosRoom;
use codex_app_server_protocol::MemythosRoomActivityEvent;
use codex_app_server_protocol::MemythosRuntimeLifecycleState;
use codex_app_server_protocol::MemythosStructuredContract;
use codex_app_server_protocol::MemythosTelemetryRef;
use codex_app_server_protocol::MemythosThreadAttachment;
use codex_app_server_protocol::MemythosTokenUsageBreakdown;
use codex_app_server_protocol::MemythosTurnUsageAttribution;
use serde::Deserialize;
use serde::Serialize;

use crate::request_processors::memythos_arena_state::ArenaCommand;
use crate::request_processors::memythos_arena_state::ArenaDomainError;
use crate::request_processors::memythos_arena_state::ArenaEvent;
use crate::request_processors::memythos_arena_state::ArenaOperationIdentity;
use crate::request_processors::memythos_arena_state::NativeArenaProtocolSnapshot;
use crate::request_processors::memythos_arena_state::NativeArenaState;
use crate::request_processors::memythos_parent_response::ParentTurnResponse;

pub(super) struct MemythosRuntimeState {
    pub(super) runtime_id: String,
    pub(super) lifecycle_state: MemythosRuntimeLifecycleState,
    pub(super) runtime_family: String,
    pub(super) connection_mode: String,
    pub(super) transport_owner: String,
    pub(super) transport_id: Option<String>,
    pub(super) daemon_runtime_verified: bool,
    pub(super) degraded_reasons: Vec<String>,
    pub(super) layers: HashMap<String, MemythosLayer>,
    pub(super) arenas: HashMap<String, MemythosArena>,
    pub(super) arena_lifecycles: HashMap<String, NativeArenaState>,
    pub(super) rooms: HashMap<String, MemythosRoom>,
    pub(super) thread_attachments: HashMap<String, MemythosThreadAttachment>,
    pub(super) arena_parents: HashMap<String, MemythosArenaParent>,
    pub(super) arena_compositions: HashMap<String, MemythosArenaCompositionProvisionResponse>,
    pub(super) restored_coordination_snapshots: HashMap<String, PersistedArenaCoordinationSnapshot>,
    pub(super) arena_recovery_blockers: HashMap<String, Vec<PersistedArenaRecoveryBlocker>>,
    pub(super) arena_pending_effects: HashMap<String, PersistedArenaPendingEffect>,
    pub(super) arena_message_deliveries: Vec<MemythosArenaMessageDelivery>,
    pub(super) arena_messages: HashMap<String, MemythosArenaMessage>,
    pub(super) arena_message_aggregates: HashMap<String, NativeArenaMessageAggregate>,
    pub(super) arena_resume_execution_plans: HashMap<String, MemythosArenaResumeExecutionPlan>,
    pub(super) room_activity_events: HashMap<String, Vec<MemythosRoomActivityEvent>>,
    pub(super) native_parent_turn_responses: HashMap<String, ParentTurnResponse>,
    pub(super) structured_contracts: HashMap<String, MemythosStructuredContract>,
    pub(super) native_token_usage_refs: HashMap<String, String>,
    pub(super) native_thread_usage_totals: HashMap<String, MemythosTokenUsageBreakdown>,
    pub(super) native_turn_usage: HashMap<String, MemythosTurnUsageAttribution>,
    pub(super) telemetry_refs: Vec<MemythosTelemetryRef>,
}

impl MemythosRuntimeState {
    pub(super) fn transition_arena_lifecycle(
        &mut self,
        arena_id: &str,
        command: ArenaCommand,
    ) -> Result<(ArenaEvent, MemythosArenaLifecycleState), ArenaDomainError> {
        let lifecycle = self.arena_lifecycles.get_mut(arena_id).ok_or_else(|| {
            ArenaDomainError::new(format!(
                "arena {arena_id} has no canonical native lifecycle"
            ))
        })?;
        let event = lifecycle.transition(command)?;
        Ok((event, lifecycle.protocol_state()))
    }

    pub(super) fn transition_arena_lifecycle_with_operation(
        &mut self,
        arena_id: &str,
        identity: ArenaOperationIdentity,
        command: ArenaCommand,
    ) -> Result<(ArenaEvent, MemythosArenaLifecycleState), ArenaDomainError> {
        let lifecycle = self.arena_lifecycles.get_mut(arena_id).ok_or_else(|| {
            ArenaDomainError::new(format!(
                "arena {arena_id} has no canonical native lifecycle"
            ))
        })?;
        let event = lifecycle.transition_with_operation(identity, command)?;
        Ok((event, lifecycle.protocol_state()))
    }
}

pub(super) const LEGACY_ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub(super) const PREVIOUS_ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
pub(super) const ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedArenaCoordinationSnapshot {
    pub(super) schema_version: u32,
    pub(super) protocol: NativeArenaProtocolSnapshot,
    pub(super) layer_id: String,
    pub(super) room: MemythosRoom,
    pub(super) contract_version: String,
    pub(super) coordination: MemythosArenaCompositionCoordination,
    pub(super) composition_version: u32,
    pub(super) composition_lifecycle_state: MemythosArenaCompositionLifecycleState,
    pub(super) leases: Vec<MemythosArenaCompositionLease>,
    #[serde(default)]
    pub(super) pending_effects: Vec<PersistedArenaPendingEffect>,
    #[serde(default)]
    pub(super) recovery_blockers: Vec<PersistedArenaRecoveryBlocker>,
    pub(super) deliveries: Vec<PersistedArenaDeliveryCheckpoint>,
    pub(super) aggregates: Vec<PersistedArenaAggregateCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedArenaRecoveryBlocker {
    pub(super) event_ref: String,
    pub(super) communication_id: String,
    pub(super) receiver_thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedArenaPendingEffect {
    pub(super) arena_id: String,
    #[serde(default)]
    pub(super) delivery_id: String,
    pub(super) message_id: String,
    pub(super) communication_id: String,
    pub(super) source_call_id: String,
    pub(super) receiver_thread_id: String,
    pub(super) payload_hash: String,
    #[serde(default)]
    pub(super) sender_thread_id: String,
    #[serde(default)]
    pub(super) round_id: String,
    #[serde(default)]
    pub(super) message_kind: String,
    #[serde(default)]
    pub(super) to_parent_role: String,
    #[serde(default)]
    pub(super) requires_response: bool,
    #[serde(default)]
    pub(super) delivery_policy: Option<MemythosArenaDeliveryPolicy>,
    #[serde(default)]
    pub(super) aggregate_contract: Option<MemythosArenaAggregateContract>,
    #[serde(default)]
    pub(super) prepared_aggregate_state: Option<MemythosArenaAggregateState>,
}

pub(super) fn arena_pending_effect_key(arena_id: &str, message_id: &str) -> String {
    format!("{arena_id}::{message_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedArenaDeliveryCheckpoint {
    pub(super) delivery_id: String,
    pub(super) message_id: String,
    pub(super) status: String,
    pub(super) sender_thread_id: String,
    pub(super) receiver_thread_id: String,
    pub(super) round_id: String,
    pub(super) phase: Option<String>,
    pub(super) delivery_mechanism: String,
    pub(super) delivery_policy: Option<MemythosArenaDeliveryPolicy>,
    pub(super) aggregate_id: Option<String>,
    pub(super) aggregate_state: Option<MemythosArenaAggregateState>,
    pub(super) checkpoint_state: Option<MemythosArenaCheckpointState>,
    pub(super) checkpoint_event_refs: Vec<String>,
    pub(super) receiver_turn_id: Option<String>,
    pub(super) receiver_response_event_ref: Option<String>,
    pub(super) event_refs: Vec<String>,
    pub(super) rejection_reason: Option<String>,
    pub(super) failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedArenaAggregateCheckpoint {
    pub(super) key: String,
    pub(super) contract: MemythosArenaAggregateContract,
    pub(super) state: MemythosArenaAggregateState,
    pub(super) received_source_thread_ids: Vec<String>,
    pub(super) received_message_ids: Vec<String>,
    pub(super) trigger_message_id: Option<String>,
    pub(super) checkpoint_state: MemythosArenaCheckpointState,
    pub(super) checkpoint_history: Vec<MemythosArenaCheckpointState>,
}

impl PersistedArenaDeliveryCheckpoint {
    pub(super) fn capture(delivery: &MemythosArenaMessageDelivery) -> Self {
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

    pub(super) fn restore(self, arena_id: &str) -> MemythosArenaMessageDelivery {
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
    pub(super) fn capture(key: &str, aggregate: &NativeArenaMessageAggregate) -> Self {
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

    pub(super) fn restore(self) -> (String, NativeArenaMessageAggregate) {
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
pub(super) struct NativeArenaMessageAggregate {
    pub(super) contract: MemythosArenaAggregateContract,
    pub(super) state: MemythosArenaAggregateState,
    pub(super) received_source_thread_ids: HashSet<String>,
    pub(super) received_message_ids: HashSet<String>,
    pub(super) trigger_message_id: Option<String>,
    pub(super) checkpoint_state: MemythosArenaCheckpointState,
    pub(super) checkpoint_history: Vec<MemythosArenaCheckpointState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_checkpoint_capture_is_deterministic_and_round_trips() {
        let aggregate = NativeArenaMessageAggregate {
            contract: MemythosArenaAggregateContract {
                aggregate_id: "aggregate-a".to_string(),
                recipient_thread_id: "judge".to_string(),
                quorum: 2,
                expected_source_thread_ids: vec!["bettor-a".to_string(), "bettor-b".to_string()],
                phase_id: "bet".to_string(),
                deadline_ref: None,
                completion_criteria_ref: "app-server://checkpoint/all-bets".to_string(),
                late_arrival_policy:
                    codex_app_server_protocol::MemythosArenaLateArrivalPolicy::Reject,
            },
            state: MemythosArenaAggregateState::Collecting,
            received_source_thread_ids: HashSet::from([
                "bettor-b".to_string(),
                "bettor-a".to_string(),
            ]),
            received_message_ids: HashSet::from(["message-b".to_string(), "message-a".to_string()]),
            trigger_message_id: Some("message-b".to_string()),
            checkpoint_state: MemythosArenaCheckpointState::CollectingMailboxContributions,
            checkpoint_history: vec![MemythosArenaCheckpointState::CollectingMailboxContributions],
        };

        let checkpoint = PersistedArenaAggregateCheckpoint::capture("arena::round", &aggregate);
        assert_eq!(
            checkpoint.received_source_thread_ids,
            ["bettor-a", "bettor-b"]
        );
        assert_eq!(checkpoint.received_message_ids, ["message-a", "message-b"]);

        let (key, restored) = checkpoint.restore();
        assert_eq!(key, "arena::round");
        assert_eq!(restored.contract, aggregate.contract);
        assert_eq!(restored.state, aggregate.state);
        assert_eq!(
            restored.received_source_thread_ids,
            aggregate.received_source_thread_ids
        );
        assert_eq!(
            restored.received_message_ids,
            aggregate.received_message_ids
        );
        assert_eq!(restored.checkpoint_history, aggregate.checkpoint_history);
    }
}
