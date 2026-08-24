use std::collections::HashSet;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaAggregateState;
use codex_app_server_protocol::MemythosArenaCheckpointState;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
use codex_app_server_protocol::MemythosArenaLateArrivalPolicy;
use codex_app_server_protocol::MemythosArenaMessage;

use crate::error_code::invalid_params;
use crate::request_processors::memythos_delivery::validate_native_aggregate_contract;
use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;
use crate::request_processors::memythos_runtime_state::NativeArenaMessageAggregate;

pub(super) fn prepare_native_aggregate_delivery(
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

pub(super) fn finalize_native_aggregate_delivery(
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

pub(super) fn transition_native_checkpoint(
    aggregate: &mut NativeArenaMessageAggregate,
    next: MemythosArenaCheckpointState,
) {
    if aggregate.checkpoint_state != next {
        aggregate.checkpoint_state = next;
        aggregate.checkpoint_history.push(next);
    }
}

pub(super) fn native_aggregate_checkpoint_projection(
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
