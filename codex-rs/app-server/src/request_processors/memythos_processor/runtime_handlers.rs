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
        result.clone().map_err(invalid_params)
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
            for pending_effect in &snapshot.pending_effects {
                let key = arena_pending_effect_key(&record.arena_id, &pending_effect.message_id);
                if pending_effect.arena_id != record.arena_id
                    || pending_effect.message_id.trim().is_empty()
                    || pending_effect.communication_id.trim().is_empty()
                    || pending_effect.source_call_id != pending_effect.communication_id
                    || pending_effect.receiver_thread_id.trim().is_empty()
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

        let mut state = self.state.lock().await;
        for (record, snapshot, lifecycle, arena) in validated_snapshots {
            let lifecycle_state = lifecycle.protocol_state();
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
                .insert(record.arena_id, snapshot);
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
