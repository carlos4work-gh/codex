use super::*;

impl MemythosRequestProcessor {
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
        validate_peer_parent_delivery_attempt(&message, &delivery_attempt).map_err(|error| {
            invalid_params(format!("peer delivery adapter contract rejected: {error}"))
        })?;
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
}
