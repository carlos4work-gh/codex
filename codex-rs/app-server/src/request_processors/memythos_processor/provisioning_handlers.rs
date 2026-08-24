use super::*;

impl MemythosRequestProcessor {
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
            let planned = self
                .arena_composition_planning_adapter
                .assess_resume(&params, previous, connection_id)
                .await
                .map_err(ArenaPortError::classify_planning_failure)
                .map_err(|error| error.into_jsonrpc("composition_planning"))?;
            validate_planned_arena_resume(previous, &planned)
                .map_err(|error| ArenaPortError::contract_rejected(error.to_string()))
                .map_err(|error| error.into_jsonrpc("composition_planning"))?;
            Some(planned)
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
                    .await
                    .map_err(ArenaPortError::classify_planning_failure)
                    .map_err(|error| error.into_jsonrpc("composition_planning"))?;
                validate_planned_arena_composition(&params, &planned)
                    .map_err(|error| ArenaPortError::contract_rejected(error.to_string()))
                    .map_err(|error| error.into_jsonrpc("composition_planning"))?;
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
        let mut provisioned_parents: Vec<ProvisionedArenaParent> =
            Vec::with_capacity(params.contract.participants.len());
        for participant in &params.contract.participants {
            let reusable_thread_id = reusable_threads
                .get(&participant.participant_id)
                .map(String::as_str);
            match self
                .arena_parent_provisioning_adapter
                .provision_parent(&params, participant, reusable_thread_id, connection_id)
                .await
            {
                Ok(parent) => {
                    if let Err(contract_error) =
                        validate_provisioned_arena_parent(participant, reusable_thread_id, &parent)
                    {
                        if parent.newly_created && !parent.thread_id.is_empty() {
                            let _ = self
                                .arena_parent_provisioning_adapter
                                .rollback_parent(&parent.thread_id)
                                .await;
                        }
                        for previous in provisioned_parents
                            .iter()
                            .filter(|previous| previous.newly_created)
                        {
                            let _ = self
                                .arena_parent_provisioning_adapter
                                .rollback_parent(&previous.thread_id)
                                .await;
                        }
                        return Err(invalid_params(format!(
                            "parent provisioning adapter contract rejected: {contract_error}"
                        )));
                    }
                    provisioned_parents.push(parent);
                }
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
            validate_parent_configuration_snapshot(&provisioned.thread_id, &snapshot).map_err(
                |error| {
                    invalid_params(format!(
                        "parent configuration adapter contract rejected: {error}"
                    ))
                },
            )?;
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
}
