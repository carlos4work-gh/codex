use super::*;

impl MemythosRequestProcessor {
    #[cfg(test)]
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

    pub(super) async fn terminalize_arena_parent_goals(&self, candidate: ArenaClosureCandidate) {
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
}
