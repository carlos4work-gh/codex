use super::*;

impl MemythosRequestProcessor {
    pub(super) async fn ensure_arena_state_restored(&self) -> Result<(), JSONRPCErrorError> {
        let result = self
            .arena_restore_result
            .get_or_init(|| async {
                self.restore_arena_coordination_snapshots()
                    .await
                    .map_err(|error| error.message)
            })
            .await;
        result.clone().map_err(invalid_params)?;
        let terminal_result = self
            .arena_terminal_recovery_result
            .get_or_init(|| async {
                self.reconcile_native_terminal_turns()
                    .await
                    .map_err(|error| error.message)
            })
            .await;
        terminal_result.clone().map_err(invalid_params)?;
        self.reconcile_mailbox_recovery_blockers().await
    }

    async fn reconcile_native_terminal_turns(&self) -> Result<(), JSONRPCErrorError> {
        let turns = {
            let state = self.state.lock().await;
            state
                .arena_message_deliveries
                .iter()
                .filter(|delivery| !delivery.status.starts_with("receiver_turn_"))
                .filter_map(|delivery| {
                    delivery
                        .receiver_turn_id
                        .as_ref()
                        .map(|turn_id| (delivery.receiver_thread_id.clone(), turn_id.clone()))
                })
                .collect::<Vec<_>>()
        };
        let responses = self
            .parent_turn_response_adapter
            .read_responses(turns)
            .await;
        for ((thread_id, turn_id), response) in responses {
            let Some(status) = response.status else {
                continue;
            };
            let status = match status {
                TurnStatus::Completed => "completed",
                TurnStatus::Failed => "failed",
                TurnStatus::Interrupted => "interrupted",
                TurnStatus::InProgress => continue,
            };
            self.record_native_turn_completed_restored(
                &thread_id,
                &turn_id,
                status,
                None,
                None,
                None,
                response.text,
            )
            .await;
        }
        Ok(())
    }

    pub(super) async fn restore_arena_coordination_snapshots(
        &self,
    ) -> Result<(), JSONRPCErrorError> {
        let Some(state_db) = self.arena_state_db.as_ref() else {
            return Ok(());
        };
        let records = state_db.list_arena_snapshots().await.map_err(|error| {
            invalid_params(format!(
                "failed to load Arena snapshots from app-server state: {error}"
            ))
        })?;
        let mut validated_snapshots = Vec::with_capacity(records.len());
        let mut pending_effect_keys = HashSet::new();
        for record in records {
            if !matches!(
                u32::try_from(record.schema_version).ok(),
                Some(
                    LEGACY_ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION
                        | PREVIOUS_ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION
                        | RECOVERY_BLOCKER_ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION
                        | ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION
                )
            ) {
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
            let mut snapshot: PersistedArenaCoordinationSnapshot =
                serde_json::from_str(&record.snapshot_json).map_err(|error| {
                    invalid_params(format!(
                        "invalid Arena coordination snapshot for {}: {error}",
                        record.arena_id
                    ))
                })?;
            let source_schema_version = snapshot.schema_version;
            if i64::from(snapshot.schema_version) != record.schema_version
                || snapshot.protocol.arena_id != record.arena_id
                || snapshot.room.arena_id != record.arena_id
            {
                return Err(invalid_params(format!(
                    "Arena snapshot identity mismatch for {}",
                    record.arena_id
                )));
            }
            snapshot.schema_version = ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION;
            for delivery in &mut snapshot.deliveries {
                delivery.migrate_from_schema(source_schema_version);
            }
            for pending_effect in &snapshot.pending_effects {
                let key = arena_pending_effect_key(&record.arena_id, &pending_effect.message_id);
                if pending_effect.arena_id != record.arena_id
                    || pending_effect.message_id.trim().is_empty()
                    || pending_effect.communication_id.trim().is_empty()
                    || pending_effect.source_call_id != pending_effect.communication_id
                    || pending_effect.receiver_thread_id.trim().is_empty()
                    || pending_effect.delivery_id.trim().is_empty()
                    || pending_effect.sender_thread_id.trim().is_empty()
                    || pending_effect.round_id.trim().is_empty()
                    || pending_effect.message_kind.trim().is_empty()
                    || pending_effect.to_parent_role.trim().is_empty()
                    || !pending_effect.payload_hash.starts_with("sha256:")
                    || pending_effect.payload_hash.len() <= "sha256:".len()
                    || !snapshot.room.participants.iter().any(|participant| {
                        participant.thread_id == pending_effect.receiver_thread_id
                    })
                    || snapshot
                        .deliveries
                        .iter()
                        .any(|delivery| delivery.message_id == pending_effect.message_id)
                    || !pending_effect_keys.insert(key)
                {
                    return Err(invalid_params(format!(
                        "Arena {} has an invalid or conflicting pending effect {}",
                        record.arena_id, pending_effect.message_id
                    )));
                }
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
                    .await
                    .map_err(ArenaPortError::classify_effect_failure)
                    .map_err(|error| error.into_jsonrpc("parent_runtime"))?
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
            validated_snapshots.push((record, snapshot, lifecycle, arena));
        }

        let quarantined = state_db
            .list_quarantined_native_mailbox_communications(None)
            .await
            .map_err(|error| {
                invalid_params(format!(
                    "failed to reconcile Arena mailbox quarantine: {error}"
                ))
            })?;
        let quarantined_keys = quarantined
            .iter()
            .map(|record| {
                (
                    record.receiver_thread_id.clone(),
                    record.communication_id.clone(),
                )
            })
            .collect::<HashSet<_>>();
        let mut reconciled_effects = Vec::new();
        for (_, snapshot, _, _) in &validated_snapshots {
            for pending in &snapshot.pending_effects {
                if quarantined_keys.contains(&(
                    pending.receiver_thread_id.clone(),
                    pending.communication_id.clone(),
                )) {
                    continue;
                }
                let message = pending_effect_message(pending);
                let effect = PeerParentStagedEffect {
                    communication_id: pending.communication_id.clone(),
                    source_call_id: pending.source_call_id.clone(),
                    receiver_thread_id: pending.receiver_thread_id.clone(),
                    payload_hash: pending.payload_hash.clone(),
                };
                let attempt = self
                    .peer_parent_delivery_adapter
                    .activate_staged_peer_parent_message(&message, &effect, None, ConnectionId(0))
                    .await;
                validate_peer_parent_delivery_attempt(&message, &attempt)
                    .map_err(|error| ArenaPortError::contract_rejected(error.to_string()))
                    .map_err(|error| error.into_jsonrpc("peer_delivery_recovery"))?;
                if let Some(reason) = attempt.rejection_reason.as_ref() {
                    return Err(ArenaPortError::outcome_unknown(reason.clone())
                        .into_jsonrpc("peer_delivery_recovery"));
                }
                reconciled_effects.push((pending.clone(), message, attempt));
            }
        }

        let mut state = self.state.lock().await;
        for (record, snapshot, lifecycle, arena) in validated_snapshots {
            let lifecycle_state = lifecycle.protocol_state();
            if let Some(max_delivery_id) = snapshot
                .deliveries
                .iter()
                .filter_map(|delivery| generated_id_sequence(&delivery.delivery_id, "mem_delivery"))
                .chain(snapshot.pending_effects.iter().filter_map(|effect| {
                    generated_id_sequence(&effect.delivery_id, "mem_delivery")
                }))
                .max()
            {
                self.next_delivery_id
                    .fetch_max(max_delivery_id, std::sync::atomic::Ordering::Relaxed);
            }
            let arena_id = record.arena_id.clone();
            let recovery_blockers = snapshot.recovery_blockers.clone();
            state.arenas.insert(arena_id.clone(), arena);
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
            for pending_effect in snapshot.pending_effects.clone() {
                state.arena_pending_effects.insert(
                    arena_pending_effect_key(&pending_effect.arena_id, &pending_effect.message_id),
                    pending_effect,
                );
            }
            for aggregate in snapshot.aggregates.clone() {
                let (key, aggregate) = aggregate.restore();
                state.arena_message_aggregates.insert(key, aggregate);
            }
            state
                .restored_coordination_snapshots
                .insert(arena_id.clone(), snapshot);
            state
                .arena_recovery_blockers
                .insert(arena_id, recovery_blockers);
        }
        let mut reconciled_arena_ids = HashSet::new();
        for (pending, message, attempt) in reconciled_effects {
            let aggregate_state = finalize_native_aggregate_delivery(
                &mut state,
                &message,
                pending.prepared_aggregate_state,
                true,
            );
            let (checkpoint_state, checkpoint_event_refs) =
                native_aggregate_checkpoint_projection(&state, &message);
            let delivery = MemythosArenaMessageDelivery {
                delivery_id: pending.delivery_id,
                message_id: pending.message_id.clone(),
                human_summary: String::new(),
                status: attempt.status,
                sender_thread_id: pending.sender_thread_id,
                receiver_thread_id: pending.receiver_thread_id,
                arena_id: pending.arena_id.clone(),
                round_id: pending.round_id,
                phase: phase_from_message_kind(&pending.message_kind),
                delivery_mechanism: attempt.delivery_mechanism,
                delivery_policy: pending.delivery_policy,
                aggregate_id: pending
                    .aggregate_contract
                    .as_ref()
                    .map(|contract| contract.aggregate_id.clone()),
                aggregate_state,
                checkpoint_state,
                checkpoint_event_refs,
                receiver_turn_id: attempt.receiver_turn_id,
                receiver_response_event_ref: attempt.receiver_response_event_ref,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: attempt.event_refs,
                rejection_reason: None,
                failure_reason: None,
            };
            state
                .arena_pending_effects
                .remove(&arena_pending_effect_key(
                    &pending.arena_id,
                    &pending.message_id,
                ));
            state.arena_message_deliveries.push(delivery);
            reconciled_arena_ids.insert(pending.arena_id);
        }
        drop(state);
        for arena_id in reconciled_arena_ids {
            self.persist_arena_coordination_snapshot(&arena_id).await?;
        }
        Ok(())
    }

    pub(super) async fn reconcile_mailbox_recovery_blockers(
        &self,
    ) -> Result<(), JSONRPCErrorError> {
        let Some(state_db) = self.arena_state_db.as_ref() else {
            return Ok(());
        };
        let quarantined = state_db
            .list_quarantined_native_mailbox_communications(None)
            .await
            .map_err(|error| {
                invalid_params(format!(
                    "failed to reconcile Arena mailbox quarantine: {error}"
                ))
            })?;
        let mut changed_arena_ids = Vec::new();
        let mut state = self.state.lock().await;
        let rooms = state.rooms.values().cloned().collect::<Vec<_>>();
        for room in rooms {
            let message_refs = state
                .arena_pending_effects
                .values()
                .filter(|effect| effect.arena_id == room.arena_id)
                .map(|effect| {
                    (
                        effect.receiver_thread_id.clone(),
                        effect.communication_id.clone(),
                    )
                })
                .chain(
                    state
                        .arena_message_deliveries
                        .iter()
                        .filter(|delivery| delivery.arena_id == room.arena_id)
                        .map(|delivery| {
                            (
                                delivery.receiver_thread_id.clone(),
                                delivery.message_id.clone(),
                            )
                        }),
                )
                .collect::<HashSet<_>>();
            let mut current = quarantined
                .iter()
                .filter(|record| {
                    message_refs.contains(&(
                        record.receiver_thread_id.clone(),
                        record.communication_id.clone(),
                    ))
                })
                .map(|record| PersistedArenaRecoveryBlocker {
                    event_ref: mailbox_quarantine_event_ref(
                        &record.receiver_thread_id,
                        &record.communication_id,
                    ),
                    communication_id: record.communication_id.clone(),
                    receiver_thread_id: record.receiver_thread_id.clone(),
                })
                .collect::<Vec<_>>();
            current.sort_by(|left, right| left.event_ref.cmp(&right.event_ref));
            let previous = state
                .arena_recovery_blockers
                .get(&room.arena_id)
                .cloned()
                .unwrap_or_default();
            if previous != current {
                state
                    .arena_recovery_blockers
                    .insert(room.arena_id.clone(), current.clone());
                changed_arena_ids.push(room.arena_id.clone());
            }
            let concierge = room
                .participants
                .iter()
                .find(|participant| participant.parent_role == "room_concierge")
                .cloned();
            for blocker in &current {
                let already_projected =
                    state
                        .room_activity_events
                        .get(&room.room_id)
                        .is_some_and(|events| {
                            events
                                .iter()
                                .any(|event| event.source_ref.as_ref() == Some(&blocker.event_ref))
                        });
                if already_projected {
                    continue;
                }
                let record = quarantined.iter().find(|record| {
                    record.receiver_thread_id == blocker.receiver_thread_id
                        && record.communication_id == blocker.communication_id
                });
                let reason = record
                    .and_then(|record| record.quarantine_reason.as_deref())
                    .unwrap_or("automatic recovery budget exhausted");
                let attempts = record.map_or(0, |record| record.attempt_count);
                let (thread_id, recipient) = concierge.as_ref().map_or_else(
                    || {
                        (
                            "room_concierge".to_string(),
                            runtime_room_concierge_actor_ref(),
                        )
                    },
                    |participant| {
                        (
                            participant.thread_id.clone(),
                            room_actor_ref_for_participant(participant),
                        )
                    },
                );
                self.push_room_activity_event(
                    &mut state,
                    room.room_id.clone(),
                    room.arena_id.clone(),
                    thread_id,
                    None,
                    None,
                    None,
                    "room_concierge".to_string(),
                    app_server_actor_ref(),
                    recipient,
                    "mailbox_recovery_incident".to_string(),
                    MemythosPromptOrigin::AppServerProtocol,
                    vec![MemythosPromptLineagePart {
                        origin: MemythosPromptOrigin::AppServerProtocol,
                        summary: "durable native mailbox quarantine projection".to_string(),
                        source_ref: Some(blocker.event_ref.clone()),
                    }],
                    "technical_detail",
                    "mailbox_quarantined",
                    "blocked",
                    format!(
                        "Room paused: {} to {}. Actions: retry, skip, replace, abort. Attempts: {}. Reason: {reason}.",
                        blocker.communication_id, blocker.receiver_thread_id, attempts,
                    ),
                    Some(blocker.event_ref.clone()),
                );
            }
        }
        drop(state);
        changed_arena_ids.sort();
        changed_arena_ids.dedup();
        for arena_id in changed_arena_ids {
            self.persist_arena_coordination_snapshot(&arena_id).await?;
        }
        Ok(())
    }

    pub(super) async fn persist_arena_coordination_snapshot(
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
            let mut pending_effects = state
                .arena_pending_effects
                .values()
                .filter(|effect| effect.arena_id == arena_id)
                .cloned()
                .collect::<Vec<_>>();
            pending_effects.sort_by(|left, right| left.message_id.cmp(&right.message_id));
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
                    pending_effects,
                    recovery_blockers: state
                        .arena_recovery_blockers
                        .get(arena_id)
                        .cloned()
                        .unwrap_or_default(),
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
                restored.schema_version = ARENA_COORDINATION_SNAPSHOT_SCHEMA_VERSION;
                restored.pending_effects = pending_effects;
                restored.recovery_blockers = state
                    .arena_recovery_blockers
                    .get(arena_id)
                    .cloned()
                    .unwrap_or_default();
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

    pub(super) async fn prepare_parent_goal_for_delivery(
        &self,
        message: &MemythosArenaMessage,
    ) -> Result<PreparedParentDeliveryGoal, JSONRPCErrorError> {
        let current_goal = self
            .arena_parent_provisioning_adapter
            .read_parent_goal(&message.to_parent_thread_id)
            .await
            .map_err(ArenaPortError::classify_effect_failure)
            .map_err(|error| error.into_jsonrpc("parent_runtime"))?
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
                    .await
                    .map_err(ArenaPortError::classify_effect_failure)
                    .map_err(|error| error.into_jsonrpc("parent_runtime"))?;
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
                    .await
                    .map_err(ArenaPortError::classify_effect_failure)
                    .map_err(|error| error.into_jsonrpc("parent_runtime"))?;
                Ok(PreparedParentDeliveryGoal {
                    active_goal,
                    previous_goal: current_goal,
                    assigned_for_delivery: true,
                })
            }
        }
    }

    pub(super) async fn rollback_parent_goal_after_failed_delivery(
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
            .map(ArenaPortError::classify_effect_failure)
            .map(|error| {
                error.into_jsonrpc("parent_runtime").message.replace(
                    "arena port parent_runtime ",
                    "delivery goal rollback also failed: ",
                )
            })
    }

    pub(super) async fn complete_parent_goal_after_successful_delivery(
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
                let error =
                    ArenaPortError::classify_effect_failure(error).into_jsonrpc("parent_runtime");
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
            let error =
                ArenaPortError::classify_effect_failure(error).into_jsonrpc("parent_runtime");
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
}

fn mailbox_quarantine_event_ref(receiver_thread_id: &str, communication_id: &str) -> String {
    format!("app-server://native-mailbox/{receiver_thread_id}/{communication_id}/quarantine")
}

fn pending_effect_message(pending: &PersistedArenaPendingEffect) -> MemythosArenaMessage {
    MemythosArenaMessage {
        message_id: pending.message_id.clone(),
        case_id: String::new(),
        arena_id: pending.arena_id.clone(),
        round_id: pending.round_id.clone(),
        from_parent_thread_id: pending.sender_thread_id.clone(),
        from_parent_role: "recovered_native_parent".to_string(),
        to_parent_thread_id: pending.receiver_thread_id.clone(),
        to_parent_role: pending.to_parent_role.clone(),
        message_kind: pending.message_kind.clone(),
        human_summary: String::new(),
        execution_prompt: None,
        context_packet_ref: String::new(),
        artifact_refs: Vec::new(),
        requires_response: pending.requires_response,
        delivery_policy: pending.delivery_policy,
        aggregate_contract: pending.aggregate_contract.clone(),
        response_contract: None,
        output_schema: None,
    }
}
