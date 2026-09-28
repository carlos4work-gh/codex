use super::*;

impl MemythosRequestProcessor {
    pub(crate) async fn room_activity_list(
        &self,
        params: MemythosRoomActivityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let (
            room,
            arena_lifecycle_state,
            recovery_blockers,
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
            let recovery_blockers = state
                .arena_recovery_blockers
                .get(&room.arena_id)
                .cloned()
                .unwrap_or_default();
            (
                room,
                arena_lifecycle_state,
                recovery_blockers,
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
        blockers.extend(
            recovery_blockers
                .iter()
                .map(|blocker| blocker.event_ref.clone()),
        );
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
        let mut native_turn_responses = self
            .parent_turn_response_adapter
            .read_responses(requested_turns)
            .await;
        validate_parent_turn_responses(&native_turn_responses).map_err(|error| {
            invalid_params(format!(
                "parent response adapter contract rejected: {error}"
            ))
        })?;
        let recorded_native_turn_responses = {
            let state = self.state.lock().await;
            state.native_parent_turn_responses.clone()
        };
        for delivery in &deliveries {
            let Some(turn_id) = delivery.receiver_turn_id.as_ref() else {
                continue;
            };
            let key = (delivery.receiver_thread_id.clone(), turn_id.clone());
            let recorded_response = recorded_native_turn_responses.get(&native_token_usage_key(
                &delivery.receiver_thread_id,
                turn_id,
            ));
            if let Some(recorded_response) = recorded_response {
                native_turn_responses
                    .entry(key.clone())
                    .and_modify(|native_response| {
                        *native_response =
                            merge_parent_turn_responses(recorded_response, native_response);
                    })
                    .or_insert_with(|| recorded_response.clone());
            }
            if let Some(response) = native_turn_responses.get(&key) {
                validate_parent_turn_response(&key.0, &key.1, response).map_err(|error| {
                    invalid_params(format!(
                        "effective parent response contract rejected: {error}"
                    ))
                })?;
            }
        }
        let turns = deliveries
            .iter()
            .filter_map(|delivery| {
                let native_response = delivery.receiver_turn_id.as_ref().and_then(|turn_id| {
                    native_turn_responses
                        .get(&(delivery.receiver_thread_id.clone(), turn_id.clone()))
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
                room_state: if !recovery_blockers.is_empty() {
                    "recoverable_pause".to_string()
                } else if clean_close {
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
            validate_parent_configuration_snapshot(&participant.thread_id, &snapshot).map_err(
                |error| {
                    invalid_params(format!(
                        "parent configuration adapter contract rejected: {error}"
                    ))
                },
            )?;
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
        deliveries.retain(|delivery| {
            input_events.iter().any(|event| {
                event.causation_id.as_deref() == Some(delivery.message_id.as_str())
                    || (event.causation_id.is_none()
                        && event.turn_id.as_ref() == delivery.receiver_turn_id.as_ref())
            })
        });
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
        validate_parent_turn_responses(&native_responses).map_err(|error| {
            invalid_params(format!(
                "parent response adapter contract rejected: {error}"
            ))
        })?;
        let participant_by_thread = room
            .participants
            .iter()
            .map(|participant| (participant.thread_id.as_str(), participant))
            .collect::<HashMap<_, _>>();
        let mut blockers = Vec::new();
        let mut entries = Vec::new();
        let mut projected_native_items = HashSet::new();
        for delivery in &deliveries {
            let Some(turn_id) = delivery.receiver_turn_id.as_ref() else {
                continue;
            };
            let Some(input_event) = input_events.iter().find(|event| {
                event.causation_id.as_deref() == Some(delivery.message_id.as_str())
                    || (event.causation_id.is_none() && event.turn_id.as_ref() == Some(turn_id))
            }) else {
                continue;
            };
            let native_response =
                native_responses.get(&(delivery.receiver_thread_id.clone(), turn_id.clone()));
            let request_item_ref = native_response
                .and_then(|response| response.request_item_ref.clone())
                .unwrap_or_else(|| {
                    format!(
                        "app-server://threads/{}/turns/{turn_id}/items/{}",
                        delivery.receiver_thread_id, delivery.message_id
                    )
                });
            if projected_native_items.insert((turn_id.clone(), request_item_ref.clone())) {
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
                    source_item_ref: request_item_ref,
                    causal_ref: delivery.message_id.clone(),
                });
            }
            if let Some(response) = native_response {
                match (response.item_ref.as_ref(), response.text.as_ref()) {
                    (Some(item_ref), Some(text)) => {
                        if projected_native_items.insert((turn_id.clone(), item_ref.clone())) {
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
    pub(super) fn push_telemetry_ref(
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
    pub(super) fn push_room_activity_event(
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
    pub(super) fn push_native_telemetry_ref_for_test(
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

    pub(super) fn next_id(&self, prefix: &str, counter: &AtomicU64) -> String {
        let next = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        format!("{prefix}_{next}")
    }
}
