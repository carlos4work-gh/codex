use super::*;

#[test]
fn native_peer_bet_is_an_incremental_commitment_contract() {
    let prompt = native_bettor_checkpoint_prompt("peer_bet", "The sealed peer checkpoint follows.");

    assert!(prompt.contains("incremental final commitment"));
    assert!(prompt.contains("proposal_ref and cross_read_ref"));
    assert!(prompt.contains("distinct, conditioned, converged, or rollup_required"));
    assert!(prompt.contains("mechanism_delta and decision_effect"));
    assert!(prompt.contains("tradeoff and cost of error"));
    assert!(prompt.contains("reopening_signals"));
    assert!(prompt.contains("withdraws redundant competition without erasing attribution"));
    assert!(!prompt.contains("return one final bet"));
}

#[test]
fn native_cross_read_preserves_each_bettors_differential_responsibility() {
    let prompt = native_bettor_checkpoint_prompt(
        "peer_review_and_objection",
        "The sealed proposal checkpoint follows.",
    );

    assert!(prompt.contains("own differential responsibility"));
    assert!(prompt.contains("supported_mechanism"));
    assert!(prompt.contains("mechanism_delta"));
    assert!(prompt.contains("decision_effect"));
    assert!(prompt.contains("shared_ground"));
    assert!(prompt.contains("residual_dissent"));
    assert!(prompt.contains("yield_condition"));
    assert!(prompt.contains("Convergence supported by evidence is valid"));
    assert!(prompt.contains("instead of inventing opposition"));
}

#[test]
fn native_cross_read_requires_pre_bet_mechanism_separation() {
    let eligible = vec!["bettor-growth".to_string(), "bettor-risk".to_string()];
    let proposal_refs = vec!["app-server://threads/risk/turns/proposal".to_string()];
    let peer_refs = vec!["app-server://threads/growth/turns/proposal".to_string()];
    let schema = native_mechanism_cross_read_output_schema(
        "bettor-risk",
        &eligible,
        &proposal_refs,
        &peer_refs,
    )
    .expect("cross-read schema");
    let required = schema["required"].as_array().expect("required fields");
    for field in [
        "proposal_ref",
        "supported_mechanism",
        "mechanism_delta",
        "decision_effect",
        "shared_ground",
        "residual_dissent",
        "yield_condition",
    ] {
        assert!(required.iter().any(|value| value.as_str() == Some(field)));
    }
    assert_eq!(schema["additionalProperties"], serde_json::json!(false));
    assert_eq!(
        schema["properties"]["mechanism_state"]["enum"],
        serde_json::json!(["distinct", "converged", "rollup_required"])
    );
    assert_eq!(
        schema["properties"]["proposal_ref"]["enum"],
        serde_json::json!(proposal_refs)
    );
    assert_eq!(
        schema["properties"]["incorporated_peer_refs"]["items"]["enum"],
        serde_json::json!(peer_refs)
    );
}

#[test]
fn native_bet_resolves_mechanism_state_and_native_trace_refs() {
    let eligible = vec!["bettor-growth".to_string(), "bettor-risk".to_string()];
    let proposal_refs = vec![
        "app-server://threads/growth/turns/proposal".to_string(),
        "app-server://threads/risk/turns/proposal".to_string(),
    ];
    let cross_read_refs = vec!["app-server://threads/growth/turns/cross-read".to_string()];
    let schema = native_mechanism_bet_output_schema(
        "bettor-growth",
        &eligible,
        &proposal_refs,
        &cross_read_refs,
    )
    .expect("bet schema");
    assert_eq!(
        schema["properties"]["mechanism_state"]["enum"],
        serde_json::json!(["distinct", "conditioned", "converged", "rollup_required"])
    );
    let required = schema["required"].as_array().expect("required fields");
    for field in [
        "proposal_ref",
        "cross_read_ref",
        "accepted_tradeoff",
        "cost_of_error",
    ] {
        assert!(required.iter().any(|value| value.as_str() == Some(field)));
    }
    assert_eq!(
        schema["properties"]["proposal_ref"]["enum"],
        serde_json::json!(proposal_refs)
    );
    assert_eq!(
        schema["properties"]["cross_read_ref"]["enum"],
        serde_json::json!(cross_read_refs)
    );
}

#[tokio::test]
async fn native_bettor_turns_receive_mechanism_output_contracts() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let risk_thread_id = thread_for("bettor-risk");
    let growth_thread_id = thread_for("bettor-growth");
    let delivery = |id: &str, phase: &str, receiver: &str| MemythosArenaMessageDelivery {
        delivery_id: id.to_string(),
        message_id: id.to_string(),
        human_summary: phase.to_string(),
        status: "receiver_turn_completed".to_string(),
        sender_thread_id: concierge_thread_id.clone(),
        receiver_thread_id: receiver.to_string(),
        arena_id: response.room.arena_id.clone(),
        round_id: "round-1".to_string(),
        phase: Some(phase.to_string()),
        delivery_mechanism: "test".to_string(),
        delivery_policy: None,
        aggregate_id: None,
        aggregate_state: None,
        checkpoint_state: None,
        checkpoint_event_refs: Vec::new(),
        receiver_turn_id: Some(format!("turn-{phase}-{receiver}")),
        receiver_response_event_ref: None,
        delivered_as_human_instruction: false,
        memory_replay_required: false,
        event_refs: Vec::new(),
        rejection_reason: None,
        failure_reason: None,
    };
    let mut state = processor.state.lock().await;
    for phase in ["proposal", "peer_review_and_objection"] {
        state.arena_message_deliveries.push(delivery(
            &format!("{phase}-risk"),
            phase,
            &risk_thread_id,
        ));
        state.arena_message_deliveries.push(delivery(
            &format!("{phase}-growth"),
            phase,
            &growth_thread_id,
        ));
    }

    for (message_kind, expected_contract, required_ref) in [
        (
            "peer_review_and_objection",
            "mechanism_cross_read",
            "proposal_ref",
        ),
        ("peer_bet", "mechanism_bet", "cross_read_ref"),
    ] {
        let mut message = MemythosArenaMessage {
            message_id: format!("mechanism-{message_kind}"),
            case_id: "case-1".to_string(),
            arena_id: response.room.arena_id.clone(),
            round_id: "round-1".to_string(),
            from_parent_thread_id: concierge_thread_id.clone(),
            from_parent_role: "room_concierge".to_string(),
            to_parent_thread_id: risk_thread_id.clone(),
            to_parent_role: "bettor".to_string(),
            message_kind: message_kind.to_string(),
            human_summary: "sealed checkpoint".to_string(),
            execution_prompt: None,
            context_packet_ref: "context://round-1".to_string(),
            artifact_refs: Vec::new(),
            requires_response: true,
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            response_contract: None,
            output_schema: None,
        };
        apply_native_checkpoint_execution_contract(&state, &mut message)
            .expect("mechanism contract should apply");
        assert_eq!(
            message.response_contract.as_deref(),
            Some(expected_contract)
        );
        assert_eq!(
            message
                .output_schema
                .as_ref()
                .and_then(|schema| schema.pointer(&format!("/properties/{required_ref}/type")))
                .and_then(serde_json::Value::as_str),
            Some("string")
        );
        let allowed_refs = message
            .output_schema
            .as_ref()
            .and_then(|schema| schema.pointer(&format!("/properties/{required_ref}/enum")));
        assert!(
            allowed_refs.is_some_and(|refs| refs.as_array().is_some_and(|refs| {
                !refs.is_empty()
                    && refs.iter().all(|value| {
                        value
                            .as_str()
                            .is_some_and(|value| value.starts_with("app-server://threads/"))
                    })
            }))
        );
    }
}

#[test]
fn native_judge_attributes_each_parent_without_persistent_reputation() {
    let prompt = native_judge_checkpoint_prompt(
        "The sealed bet checkpoint follows.",
        &["bettor-growth".to_string(), "bettor-risk".to_string()],
    );

    assert!(prompt.contains("Attribute every eligible bettor exactly once"));
    assert!(prompt.contains("rank every other eligible participant exactly once"));
    assert!(prompt.contains("never include the winner"));
    assert!(prompt.contains("adopted, conditioned, rejected, or preserved_dissent"));
    assert!(prompt.contains("Credit useful evidence even when its parent did not win"));
    assert!(prompt.contains("do not reward persistence after refutation"));
    assert!(
        prompt.contains("do not turn this theoretical verdict into a persistent reputation score")
    );
}

#[test]
fn native_judge_targeted_refinement_contract_is_bounded_and_attributable() {
    let eligible = ["bettor-growth", "bettor-risk"]
        .into_iter()
        .collect::<HashSet<_>>();
    let verdict = serde_json::json!({
            "winner_participant_id": "bettor-growth",
            "ranked_alternatives": ["bettor-risk"],
            "winning_decision": "Advance with bounded growth.",
            "accepted_tradeoff": "Accept slower rollout for reversibility.",
            "next_action": "targeted_refinement",
            "contribution_attribution": [
                {"participant_id": "bettor-growth", "claim_refs": ["claim://growth"], "disposition": "adopted", "rationale": "Growth resolves the bounded objective."},
                {"participant_id": "bettor-risk", "claim_refs": ["claim://risk"], "disposition": "preserved_dissent", "rationale": "Risk owns the unresolved reversal threshold."}
            ],
            "dissent": "Retain the reversal threshold.",
            "preserved_dissent": ["Retain the reversal threshold."],
            "targeted_refinements": [{
                "participant_id": "bettor-risk",
                "tension": "The reversal threshold remains implicit.",
                "request": "Make the threshold observable.",
                "sufficiency_criterion": "A measurable threshold is stated or authority is escalated."
            }],
            "reopening_signals": ["The threshold is crossed."],
            "protected_decisions_status": "preserved",
            "reopened_decision_refs": [],
            "resume_scope_status": "not_applicable",
            "rationale": "Only one localized tension remains."
        })
        .to_string();

    assert!(is_valid_native_judge_verdict(&verdict, &eligible));
    assert_eq!(
        native_judge_next_action(&verdict, &eligible).as_deref(),
        Some("targeted_refinement")
    );

    let prompt =
        native_bettor_checkpoint_prompt("targeted_refinement", "Resolve the reversal threshold.");
    assert!(prompt.contains("answer only the Judge's targeted mandate"));
    assert!(prompt.contains("Do not restart proposal, cross-read, or bet"));
}

#[test]
fn native_judge_rejects_unbounded_or_mismatched_refinement_contracts() {
    let eligible = ["bettor-growth", "bettor-risk"]
        .into_iter()
        .collect::<HashSet<_>>();
    let base = serde_json::json!({
        "winner_participant_id": "bettor-growth",
        "ranked_alternatives": ["bettor-risk"],
        "winning_decision": "Advance with bounded growth.",
        "accepted_tradeoff": "Accept slower rollout for reversibility.",
        "next_action": "close",
        "contribution_attribution": [
            {"participant_id": "bettor-growth", "claim_refs": [], "disposition": "adopted", "rationale": "Useful."},
            {"participant_id": "bettor-risk", "claim_refs": [], "disposition": "rejected", "rationale": "Insufficient."}
        ],
        "dissent": "none",
        "preserved_dissent": [],
        "targeted_refinements": [{
            "participant_id": "bettor-risk",
            "tension": "Still open.",
            "request": "Try again.",
            "sufficiency_criterion": "Resolve it."
        }],
        "reopening_signals": [],
        "protected_decisions_status": "preserved",
        "reopened_decision_refs": [],
        "resume_scope_status": "not_applicable",
        "rationale": "Invalid close with refinement."
    });

    assert!(!is_valid_native_judge_verdict(&base.to_string(), &eligible));
    let mut unknown = base;
    unknown["next_action"] = serde_json::json!("targeted_refinement");
    unknown["targeted_refinements"][0]["participant_id"] = serde_json::json!("bettor-unknown");
    assert!(!is_valid_native_judge_verdict(
        &unknown.to_string(),
        &eligible
    ));
}
use codex_app_server_protocol::MemythosArenaKind;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosLayerKind;
use codex_app_server_protocol::TokenUsageBreakdown;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

fn native_usage(
    total_tokens: i64,
    input_tokens: i64,
    cached_input_tokens: i64,
) -> ThreadTokenUsage {
    let total = TokenUsageBreakdown {
        total_tokens,
        input_tokens,
        cached_input_tokens,
        output_tokens: total_tokens - input_tokens,
        reasoning_output_tokens: 0,
    };
    ThreadTokenUsage {
        total: total.clone(),
        last: total,
        model_context_window: Some(200_000),
    }
}

#[test]
fn native_mailbox_wake_policy_delegates_active_turn_serialization_to_core() {
    assert_eq!(
        native_mailbox_wake_policy(&AgentStatus::Running, true),
        Ok(true)
    );
    assert_eq!(
        native_mailbox_wake_policy(&AgentStatus::Completed(None), true),
        Ok(true)
    );
    assert_eq!(
        native_mailbox_wake_policy(&AgentStatus::Interrupted, true),
        Ok(true)
    );
    assert_eq!(
        native_mailbox_wake_policy(&AgentStatus::Completed(None), false),
        Ok(false)
    );
}

#[test]
fn native_mailbox_wake_policy_rejects_closed_parent_threads() {
    assert!(
        native_mailbox_wake_policy(&AgentStatus::Shutdown, true)
            .expect_err("shutdown parent must be rejected")
            .contains("shutdown")
    );
    assert!(
        native_mailbox_wake_policy(&AgentStatus::Errored("boom".to_string()), true)
            .expect_err("errored parent must be rejected")
            .contains("boom")
    );
}

#[test]
fn judge_verdict_schema_constrains_winner_to_native_participant_ids() {
    let eligible = vec!["p1-growth".to_string(), "p2-risk".to_string()];
    let schema = native_judge_verdict_output_schema(&eligible).expect("verdict schema");
    assert_eq!(
        schema["properties"]["winner_participant_id"]["enum"],
        serde_json::json!(["p1-growth", "p2-risk"])
    );
    assert_eq!(schema["additionalProperties"], serde_json::json!(false));
    assert_eq!(
        schema["properties"]["contribution_attribution"]["items"]["properties"]["participant_id"]["enum"],
        serde_json::json!(["p1-growth", "p2-risk"])
    );
    assert!(
        schema["properties"]["ranked_alternatives"]
            .get("uniqueItems")
            .is_none()
    );
    assert!(
        schema["properties"]["ranked_alternatives"]
            .get("minItems")
            .is_none()
    );
    assert!(
        schema["properties"]["ranked_alternatives"]
            .get("maxItems")
            .is_none()
    );
    assert!(
        schema["required"]
            .as_array()
            .expect("required fields")
            .contains(&serde_json::json!("protected_decisions_status"))
    );
    assert!(
        schema["required"]
            .as_array()
            .expect("required fields")
            .contains(&serde_json::json!("reopened_decision_refs"))
    );
    assert!(
        schema["required"]
            .as_array()
            .expect("required fields")
            .contains(&serde_json::json!("resume_scope_status"))
    );
}

#[test]
fn judge_bet_aggregation_is_canonical_and_runtime_owned() {
    let participant =
        |parent_key: &str, parent_role: &str, thread_id: &str, stance_profile: &str| {
            MemythosRoomParticipant {
                parent_key: parent_key.to_string(),
                thread_id: thread_id.to_string(),
                parent_role: parent_role.to_string(),
                stance_profile: stance_profile.to_string(),
                goal_ref: None,
                authority_scope: Vec::new(),
            }
        };
    let room = MemythosRoom {
        room_id: "room-1".to_string(),
        case_id: "case-1".to_string(),
        layer_id: "layer-1".to_string(),
        arena_id: "arena-1".to_string(),
        topology: "room_concierge".to_string(),
        participants: vec![
            participant(
                "concierge",
                "room_concierge",
                "concierge-thread",
                "coordination",
            ),
            participant("bettor-a", "bettor", "bettor-a-thread", "growth"),
            participant("bettor-b", "bettor", "bettor-b-thread", "risk"),
            participant("judge", "judge", "judge-thread", "business_fitness"),
        ],
    };
    let judge = room
        .participants
        .iter()
        .find(|participant| participant.parent_role == "judge")
        .expect("judge");

    let first =
        canonical_native_judge_bet_contract(&room, "round-1", judge).expect("canonical contract");
    let second = canonical_native_judge_bet_contract(&room, "round-1", judge)
        .expect("same canonical contract");

    assert_eq!(first, second);
    assert_eq!(first.recipient_thread_id, "judge-thread");
    assert_eq!(first.quorum, 2);
    assert_eq!(
        first.expected_source_thread_ids,
        vec!["bettor-a-thread".to_string(), "bettor-b-thread".to_string()]
    );
    assert_eq!(
        first.late_arrival_policy,
        MemythosArenaLateArrivalPolicy::Reject
    );
}

#[test]
fn arena_composition_schema_closes_every_object_for_strict_output() {
    fn assert_closed(value: &serde_json::Value, object_count: &mut usize) {
        match value {
            serde_json::Value::Object(object) => {
                if object.get("type").and_then(serde_json::Value::as_str) == Some("object") {
                    *object_count += 1;
                    assert_eq!(
                        object.get("additionalProperties"),
                        Some(&serde_json::Value::Bool(false))
                    );
                    if let Some(properties) = object
                        .get("properties")
                        .and_then(serde_json::Value::as_object)
                    {
                        let required = object
                            .get("required")
                            .and_then(serde_json::Value::as_array)
                            .expect("strict object must require every property");
                        let required = required
                            .iter()
                            .map(|value| {
                                value.as_str().expect("required property must be a string")
                            })
                            .collect::<std::collections::BTreeSet<_>>();
                        let properties = properties
                            .keys()
                            .map(String::as_str)
                            .collect::<std::collections::BTreeSet<_>>();
                        assert_eq!(required, properties);
                    }
                }
                for nested in object.values() {
                    assert_closed(nested, object_count);
                }
            }
            serde_json::Value::Array(values) => {
                for nested in values {
                    assert_closed(nested, object_count);
                }
            }
            _ => {}
        }
    }

    let schema = arena_composition_output_schema().expect("composition schema");
    let mut object_count = 0;
    assert_closed(&schema, &mut object_count);
    assert!(object_count >= 4, "expected nested composition objects");
}

#[test]
fn arena_composition_schema_uses_only_responses_compatible_reasoning_effort() {
    fn assert_no_all_of(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                assert!(!object.contains_key("allOf"), "allOf is not supported");
                for nested in object.values() {
                    assert_no_all_of(nested);
                }
            }
            serde_json::Value::Array(values) => {
                for nested in values {
                    assert_no_all_of(nested);
                }
            }
            _ => {}
        }
    }

    fn find_reasoning_effort(value: &serde_json::Value) -> Option<&serde_json::Value> {
        match value {
            serde_json::Value::Object(object) => object
                .get("properties")
                .and_then(serde_json::Value::as_object)
                .and_then(|properties| properties.get("reasoningEffort"))
                .or_else(|| object.values().find_map(find_reasoning_effort)),
            serde_json::Value::Array(values) => values.iter().find_map(find_reasoning_effort),
            _ => None,
        }
    }

    let schema = arena_composition_output_schema().expect("composition schema");
    assert_no_all_of(&schema);
    assert_eq!(
        find_reasoning_effort(&schema)
            .and_then(|value| value.get("enum"))
            .cloned(),
        Some(serde_json::json!(["low", "medium", "high", "xhigh"]))
    );
}

#[derive(Debug)]
struct FakeLivePeerParentDeliveryAdapter;

impl PeerParentDeliveryAdapter for FakeLivePeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        _reasoning_effort: Option<ReasoningEffort>,
        _connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            let receiver_turn_id = message.requires_response.then(|| {
                format!(
                    "turn_for_{}_{}",
                    message.to_parent_thread_id, message.message_id
                )
            });
            let mut event_refs = vec![format!(
                "memythos://arenas/{}/rounds/{}/messages/{}",
                message.arena_id, message.round_id, message.message_id
            )];
            if let Some(turn_id) = receiver_turn_id.as_deref() {
                event_refs.push(format!(
                    "app-server://threads/{}/turns/{turn_id}",
                    message.to_parent_thread_id
                ));
            } else {
                event_refs.push(format!(
                    "app-server://threads/{}/mailbox/{}",
                    message.to_parent_thread_id, message.message_id
                ));
            }
            PeerParentDeliveryAttempt {
                status: if message.requires_response {
                    "delivered_to_live_thread".to_string()
                } else {
                    "queued_in_native_mailbox".to_string()
                },
                delivery_mechanism: if message.requires_response {
                    "turn_start".to_string()
                } else {
                    "native_mailbox_queue_only".to_string()
                },
                receiver_turn_id,
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs,
                rejection_reason: None,
                telemetry_channel: MemythosEventChannel::StateTransition,
                telemetry_summary: format!(
                    "Arena message {} delivered to live parent thread {}.",
                    message.message_id, message.to_parent_thread_id
                ),
            }
        })
    }
}

#[derive(Debug)]
struct FakeParentGoalSnapshotAdapter;

#[derive(Debug)]
struct FakeParentConfigurationAdapter;

#[derive(Debug)]
struct CompositionParentConfigurationAdapter;

impl ParentConfigurationAdapter for CompositionParentConfigurationAdapter {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a> {
        Box::pin(async move {
            let role = thread_id.split("::").nth(1).map(str::to_string);
            ParentConfigurationSnapshot {
                proposal_bearing: role.as_deref().map(|role| role == "bettor"),
                agent_role: role,
                collaboration_mode: "default".to_string(),
                session_source: "app_server".to_string(),
                lifecycle_state: "loaded".to_string(),
                config_sources: vec![format!("app-server://threads/{thread_id}/config")],
                ..Default::default()
            }
        })
    }
}

#[derive(Default)]
struct FakeArenaParentProvisioningAdapter {
    fail_participant: Option<String>,
    rolled_back: Arc<Mutex<Vec<String>>>,
    goal_transitions: Arc<Mutex<Vec<(String, Option<String>, ThreadGoalStatus, bool)>>>,
    goals: Arc<Mutex<HashMap<String, ThreadGoal>>>,
}

struct FakeArenaCompositionPlanningAdapter {
    contract: MemythosArenaCompositionContract,
}

struct ExpandingArenaCompositionPlanningAdapter {
    initial_contract: MemythosArenaCompositionContract,
    expanded_contract: MemythosArenaCompositionContract,
}

impl ArenaCompositionPlanningAdapter for FakeArenaCompositionPlanningAdapter {
    fn plan<'a>(
        &'a self,
        _params: &'a MemythosArenaRequestParams,
        _previous: Option<&'a MemythosArenaCompositionProvisionResponse>,
        _connection_id: ConnectionId,
    ) -> ArenaCompositionPlanningFuture<'a> {
        let contract = self.contract.clone();
        Box::pin(async move {
            Ok(PlannedArenaComposition {
                planner_thread_id: "planner-thread".to_string(),
                planner_turn_id: "planner-turn".to_string(),
                contract,
            })
        })
    }

    fn assess_resume<'a>(
        &'a self,
        params: &'a MemythosArenaRequestParams,
        previous: &'a MemythosArenaCompositionProvisionResponse,
        _connection_id: ConnectionId,
    ) -> ArenaResumePlanningFuture<'a> {
        let all_participant_ids = previous
            .contract
            .participants
            .iter()
            .map(|participant| participant.participant_id.clone())
            .collect::<Vec<_>>();
        let candidate_change_refs = params
            .resume_context
            .as_ref()
            .map(|context| context.candidate_change_refs.clone())
            .unwrap_or_default();
        let has_change =
            params.composition_change_signal.is_some() || !candidate_change_refs.is_empty();
        let full_round = candidate_change_refs
            .iter()
            .any(|reference| reference == "evidence://global-invalidation");
        let partial_participant_ids = previous
            .contract
            .participants
            .iter()
            .find(|participant| participant.agent_role == "bettor")
            .map(|participant| vec![participant.participant_id.clone()])
            .unwrap_or_else(|| all_participant_ids.clone());
        let source_round_id = format!(
            "{}-round-{}",
            previous.contract.arena_id, previous.composition_version
        );
        Box::pin(async move {
            Ok(PlannedArenaResume {
                planner_thread_id: "novelty-thread".to_string(),
                planner_turn_id: "novelty-turn".to_string(),
                assessment: if full_round {
                    MemythosArenaResumeAssessment {
                        disposition: MemythosArenaResumeDisposition::FullRound,
                        rationale: "fixture evidence invalidates comparability across all bets"
                            .to_string(),
                        affected_participant_ids: all_participant_ids.clone(),
                        cited_change_refs: candidate_change_refs.clone(),
                        affected_decision_refs: vec!["decision://fixture".to_string()],
                        comparability_invalidated: true,
                        avoided_full_round: false,
                        resume_execution_plan: MemythosArenaResumeExecutionPlan {
                            mode: MemythosArenaResumeExecutionMode::FullRound,
                            affected_participant_ids: all_participant_ids.clone(),
                            source_round_id: Some(source_round_id.clone()),
                            affected_decision_refs: vec!["decision://fixture".to_string()],
                            cited_change_refs: candidate_change_refs.clone(),
                        },
                    }
                } else if has_change {
                    MemythosArenaResumeAssessment {
                        disposition: MemythosArenaResumeDisposition::PartialResume,
                        rationale: "fixture material change affects the selected live parents"
                            .to_string(),
                        affected_participant_ids: partial_participant_ids.clone(),
                        cited_change_refs: vec!["evidence://fixture-change".to_string()],
                        affected_decision_refs: vec!["decision://fixture".to_string()],
                        comparability_invalidated: false,
                        avoided_full_round: true,
                        resume_execution_plan: MemythosArenaResumeExecutionPlan {
                            mode: MemythosArenaResumeExecutionMode::ReassessAffectedPositions,
                            affected_participant_ids: partial_participant_ids.clone(),
                            source_round_id: Some(source_round_id.clone()),
                            affected_decision_refs: vec!["decision://fixture".to_string()],
                            cited_change_refs: vec!["evidence://fixture-change".to_string()],
                        },
                    }
                } else {
                    MemythosArenaResumeAssessment {
                        disposition: MemythosArenaResumeDisposition::RetainDecision,
                        rationale: "fixture has no material delta".to_string(),
                        affected_participant_ids: Vec::new(),
                        cited_change_refs: Vec::new(),
                        affected_decision_refs: Vec::new(),
                        comparability_invalidated: false,
                        avoided_full_round: true,
                        resume_execution_plan: MemythosArenaResumeExecutionPlan {
                            mode: MemythosArenaResumeExecutionMode::RetainDecision,
                            affected_participant_ids: Vec::new(),
                            source_round_id: Some(source_round_id.clone()),
                            affected_decision_refs: Vec::new(),
                            cited_change_refs: Vec::new(),
                        },
                    }
                },
            })
        })
    }
}

impl ArenaCompositionPlanningAdapter for ExpandingArenaCompositionPlanningAdapter {
    fn plan<'a>(
        &'a self,
        _params: &'a MemythosArenaRequestParams,
        previous: Option<&'a MemythosArenaCompositionProvisionResponse>,
        _connection_id: ConnectionId,
    ) -> ArenaCompositionPlanningFuture<'a> {
        let contract = if previous.is_some() {
            self.expanded_contract.clone()
        } else {
            self.initial_contract.clone()
        };
        Box::pin(async move {
            Ok(PlannedArenaComposition {
                planner_thread_id: "planner-thread".to_string(),
                planner_turn_id: "planner-turn".to_string(),
                contract,
            })
        })
    }

    fn assess_resume<'a>(
        &'a self,
        _params: &'a MemythosArenaRequestParams,
        previous: &'a MemythosArenaCompositionProvisionResponse,
        _connection_id: ConnectionId,
    ) -> ArenaResumePlanningFuture<'a> {
        let affected_participant_ids: Vec<String> = previous
            .contract
            .participants
            .iter()
            .map(|participant| participant.participant_id.clone())
            .collect();
        let source_round_id = format!(
            "{}-round-{}",
            previous.contract.arena_id, previous.composition_version
        );
        Box::pin(async move {
            Ok(PlannedArenaResume {
                    planner_thread_id: "novelty-thread".to_string(),
                    planner_turn_id: "novelty-turn".to_string(),
                    assessment: MemythosArenaResumeAssessment {
                        disposition: MemythosArenaResumeDisposition::FullRound,
                        rationale: "fixture evidence gap invalidates method comparability and requires one additional perspective"
                            .to_string(),
                        affected_participant_ids: affected_participant_ids.clone(),
                        cited_change_refs: vec!["evidence://fixture-gap".to_string()],
                        affected_decision_refs: vec!["decision://fixture".to_string()],
                        comparability_invalidated: true,
                        avoided_full_round: false,
                        resume_execution_plan: MemythosArenaResumeExecutionPlan {
                            mode: MemythosArenaResumeExecutionMode::FullRound,
                            affected_participant_ids: affected_participant_ids.clone(),
                            source_round_id: Some(source_round_id),
                            affected_decision_refs: vec!["decision://fixture".to_string()],
                            cited_change_refs: vec!["evidence://fixture-gap".to_string()],
                        },
                    },
                })
        })
    }
}

impl ArenaParentProvisioningAdapter for FakeArenaParentProvisioningAdapter {
    fn provision_parent<'a>(
        &'a self,
        params: &'a MemythosArenaCompositionProvisionParams,
        participant: &'a codex_app_server_protocol::MemythosArenaCompositionParticipant,
        reusable_thread_id: Option<&'a str>,
        _connection_id: ConnectionId,
    ) -> ArenaParentProvisionFuture<'a> {
        Box::pin(async move {
            if self.fail_participant.as_deref() == Some(&participant.participant_id) {
                return Err(invalid_params("injected provisioning failure"));
            }
            let newly_created = reusable_thread_id.is_none();
            let thread_id = reusable_thread_id.map(str::to_string).unwrap_or_else(|| {
                format!(
                    "test::{}::{}",
                    participant.agent_role, participant.participant_id
                )
            });
            let goal = ThreadGoal {
                thread_id: thread_id.clone(),
                objective: participant.role_objective.clone(),
                status: ThreadGoalStatus::Paused,
                token_budget: participant.token_budget,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at: 0,
                updated_at: 0,
            };
            self.goals
                .lock()
                .await
                .insert(thread_id.clone(), goal.clone());
            Ok(ProvisionedArenaParent {
                participant_id: participant.participant_id.clone(),
                goal_ref: format!("app-server://threads/{thread_id}/goals/test"),
                lease_id: format!("lease::{}", participant.participant_id),
                lease_source: if newly_created { "created" } else { "reused" }.to_string(),
                memory_scope: format!("arena:{}", params.contract.arena_id),
                goal,
                thread_id,
                newly_created,
            })
        })
    }

    fn transition_parent_goal<'a>(
        &'a self,
        thread_id: &'a str,
        objective: Option<&'a str>,
        status: ThreadGoalStatus,
        arm_for_next_turn: bool,
    ) -> ArenaParentGoalTransitionFuture<'a> {
        Box::pin(async move {
            self.goal_transitions.lock().await.push((
                thread_id.to_string(),
                objective.map(str::to_string),
                status.clone(),
                arm_for_next_turn,
            ));
            let mut goals = self.goals.lock().await;
            let goal = goals
                .get_mut(thread_id)
                .ok_or_else(|| invalid_params("test parent goal does not exist"))?;
            if let Some(objective) = objective {
                goal.objective = objective.to_string();
            }
            goal.status = status;
            let goal = goal.clone();
            Ok(goal)
        })
    }

    fn read_parent_goal<'a>(&'a self, thread_id: &'a str) -> ArenaParentGoalReadFuture<'a> {
        Box::pin(async move { Ok(self.goals.lock().await.get(thread_id).cloned()) })
    }

    fn rollback_parent<'a>(&'a self, thread_id: &'a str) -> ArenaParentProvisionFuture<'a> {
        Box::pin(async move {
            self.rolled_back.lock().await.push(thread_id.to_string());
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

impl ParentConfigurationAdapter for FakeParentConfigurationAdapter {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a> {
        Box::pin(async move {
            ParentConfigurationSnapshot {
                agent_role: Some(format!("native_role_for_{thread_id}")),
                proposal_bearing: None,
                personality: Some("pragmatic".to_string()),
                multi_agent_mode: Some("proactive".to_string()),
                parent_thread_id: None,
                collaboration_mode: "default".to_string(),
                session_source: "app_server".to_string(),
                config_sources: vec![format!("app-server://threads/{thread_id}/config")],
                lifecycle_state: "loaded".to_string(),
                blockers: Vec::new(),
            }
        })
    }
}

impl ParentGoalSnapshotAdapter for FakeParentGoalSnapshotAdapter {
    fn current_goal_snapshot<'a>(&'a self, thread_id: &'a str) -> ParentGoalSnapshotFuture<'a> {
        Box::pin(async move {
            ParentGoalSnapshot {
                goal_snapshot_ref: Some(format!("app-server://threads/{thread_id}/goals/current")),
                budget_state_ref: Some(format!("app-server://threads/{thread_id}/budget/current")),
                goal_status: Some(ThreadGoalStatus::Active),
                token_budget: Some(20_000),
                tokens_used: Some(3_800),
                time_used_seconds: Some(71),
                evidence_refs: vec![
                    format!("app-server://threads/{thread_id}/goals/current"),
                    format!("app-server://threads/{thread_id}/budget/current"),
                ],
                degraded_reason: None,
            }
        })
    }
}

fn competitive_composition_params() -> MemythosArenaCompositionProvisionParams {
    let participant = |participant_id: &str, agent_role: &str, stance: &str| {
        codex_app_server_protocol::MemythosArenaCompositionParticipant {
            participant_id: participant_id.to_string(),
            agent_role: agent_role.to_string(),
            stance: stance.to_string(),
            authority_scope: vec!["business_process".to_string()],
            role_objective: format!("Fulfil the {participant_id} responsibility"),
            expected_contribution: format!("Independent contribution from {participant_id}"),
            exit_condition: format!("{participant_id} has delivered its position"),
            effort_intent: "proportionate to uncertainty and decision impact".to_string(),
            reasoning_effort: ReasoningEffort::Low,
            token_budget: Some(20_000),
        }
    };
    MemythosArenaCompositionProvisionParams {
        case_id: "case-composition".to_string(),
        layer_id: "bpm_e2e".to_string(),
        room_id: "room-composition".to_string(),
        cwd: None,
        upstream_authority_scope: vec!["business_process".to_string()],
        revision: None,
        contract: codex_app_server_protocol::MemythosArenaCompositionContract {
            contract_version: "1.0".to_string(),
            arena_id: "arena-composition".to_string(),
            shared_objective: "Resolve the BPM decision with independent positions".to_string(),
            completion_criteria: vec!["Judge selects a supported position".to_string()],
            participants: vec![
                participant("concierge", "room_concierge", "coordination"),
                participant("bettor-growth", "bettor", "growth"),
                participant("bettor-risk", "bettor", "risk"),
                participant("judge", "judge", "business_fitness"),
            ],
            coordination: codex_app_server_protocol::MemythosArenaCompositionCoordination {
                coordinator_participant_id: None,
                concierge_participant_id: Some("concierge".to_string()),
                judge_participant_id: Some("judge".to_string()),
                decision_method: MemythosArenaDecisionMethod::BettingRound,
                round_policy: Some(codex_app_server_protocol::MemythosArenaRoundPolicy {
                    minimum_competing_positions: 2,
                    cross_read_required: true,
                    objection_required: true,
                    explicit_bet_required: true,
                }),
            },
            cost_envelope: codex_app_server_protocol::MemythosArenaCostEnvelope {
                mode: codex_app_server_protocol::MemythosArenaCostEnvelopeMode::ExplicitCap,
                rationale: "The fixture supplies a measured bounded allocation".to_string(),
                baseline_refs: Vec::new(),
                total_token_budget: Some(80_000),
                coordination_token_budget: Some(20_000),
                substantive_token_budget: Some(60_000),
                method_integrity_funded: true,
                exhaustion_policy:
                    codex_app_server_protocol::MemythosArenaCostExhaustionPolicy::WrapUpThenReplan,
            },
            effort_rationale: "Allocate bounded effort while preserving both independent positions"
                .to_string(),
            rationale: "Two independent bettors prevent a fake round".to_string(),
            unresolved_role_gap: None,
        },
    }
}

fn semantic_arena_request_params() -> MemythosArenaRequestParams {
    MemythosArenaRequestParams {
        case_id: "case-composition".to_string(),
        layer_id: "bpm_e2e".to_string(),
        arena_id: "arena-composition".to_string(),
        room_id: "room-composition".to_string(),
        cwd: None,
        request_origin: "human".to_string(),
        case_brief: "Decide whether the BPM node can move to tactical design".to_string(),
        layer_objective: "Protect end-to-end business authority".to_string(),
        expected_deliverable: "A supported arena decision".to_string(),
        completion_criteria: vec!["Judge selects a supported position".to_string()],
        closed_decisions: vec!["The BPM scope is already approved".to_string()],
        available_authority: vec!["business_process".to_string()],
        uncertainties: vec!["Operational ownership needs validation".to_string()],
        reality_evidence: vec!["Current process map".to_string()],
        cost_goal: "Use the smallest sufficient arena".to_string(),
        cost_context: Some(codex_app_server_protocol::MemythosArenaCostContext {
            explicit_token_cap: Some(80_000),
            comparable_evidence: Vec::new(),
        }),
        composition_change_signal: None,
        resume_context: None,
    }
}

fn initial_resume_execution_plan() -> MemythosArenaResumeExecutionPlan {
    MemythosArenaResumeExecutionPlan {
        mode: MemythosArenaResumeExecutionMode::InitialRound,
        affected_participant_ids: Vec::new(),
        source_round_id: None,
        affected_decision_refs: Vec::new(),
        cited_change_refs: Vec::new(),
    }
}

fn partial_resume_execution_plan(
    affected_participant_ids: Vec<String>,
) -> MemythosArenaResumeExecutionPlan {
    MemythosArenaResumeExecutionPlan {
        mode: MemythosArenaResumeExecutionMode::ReassessAffectedPositions,
        affected_participant_ids,
        source_round_id: Some("arena-composition-round-1".to_string()),
        affected_decision_refs: vec!["decision://fixture".to_string()],
        cited_change_refs: vec!["evidence://fixture-change".to_string()],
    }
}

#[test]
fn native_parent_setup_contains_only_stable_identity_context() {
    let mut params = competitive_composition_params();
    params.contract.completion_criteria = vec![
        "Preserve the accepted coordination-limbo dissent".to_string(),
        "State queueing, misrouting, and bot-overreach reopening signals".to_string(),
    ];
    let judge = params
        .contract
        .participants
        .iter()
        .find(|participant| participant.agent_role == "judge")
        .expect("judge participant");

    let instructions = native_arena_parent_developer_instructions(&params, judge);

    assert!(instructions.contains(&judge.agent_role));
    assert!(instructions.contains(&judge.stance));
    assert!(!instructions.contains(&params.contract.shared_objective));
    assert!(!instructions.contains("Preserve the accepted coordination-limbo dissent"));
    assert!(!instructions.contains(&judge.role_objective));
    assert!(instructions.contains(
        "Do not collapse a required dissent or reopening signal into an implementation refinement"
    ));
    assert_eq!(
        native_arena_parent_identity_sha256(&params, judge).len(),
        64
    );
    assert_eq!(
        native_arena_parent_identity_version(&params),
        format!("{}:parent-identity-v2", params.contract.contract_version)
    );

    let identity_before = native_arena_parent_identity_sha256(&params, judge);
    let mut revised = params.clone();
    revised.contract.shared_objective = "A revised bounded objective".to_string();
    revised.contract.completion_criteria = vec!["A revised completion criterion".to_string()];
    revised
        .contract
        .participants
        .iter_mut()
        .find(|participant| participant.participant_id == judge.participant_id)
        .expect("revised judge participant")
        .role_objective = "A revised task objective".to_string();
    let revised_judge = revised
        .contract
        .participants
        .iter()
        .find(|participant| participant.participant_id == judge.participant_id)
        .expect("revised judge participant");
    assert_eq!(
        identity_before,
        native_arena_parent_identity_sha256(&revised, revised_judge)
    );

    let mut changed_identity = revised.clone();
    changed_identity
        .contract
        .participants
        .iter_mut()
        .find(|participant| participant.participant_id == judge.participant_id)
        .expect("changed judge participant")
        .stance = "materially_different_stance".to_string();
    let changed_judge = changed_identity
        .contract
        .participants
        .iter()
        .find(|participant| participant.participant_id == judge.participant_id)
        .expect("changed judge participant");
    assert_ne!(
        identity_before,
        native_arena_parent_identity_sha256(&changed_identity, changed_judge)
    );
}

#[test]
fn reusable_parent_accepts_the_exact_native_identity_after_role_instructions() {
    let params = competitive_composition_params();
    let participant = params
        .contract
        .participants
        .iter()
        .find(|participant| participant.agent_role == "bettor")
        .expect("bettor participant");
    let identity = native_arena_parent_developer_instructions(&params, participant);
    let composed = format!("Native role instructions.\n\n{identity}");

    validate_reusable_parent_identity(Some(&composed), &params, participant)
        .expect("the live thread should retain its exact native identity suffix");
}

#[test]
fn reusable_parent_rejects_missing_or_stale_native_identity() {
    let params = competitive_composition_params();
    let participant = params
        .contract
        .participants
        .iter()
        .find(|participant| participant.agent_role == "bettor")
        .expect("bettor participant");

    let missing = validate_reusable_parent_identity(None, &params, participant)
        .expect_err("a parent without bootstrap identity cannot be reused");
    assert!(missing.message.contains("revise the arena composition"));

    let stale = validate_reusable_parent_identity(
        Some("Native role instructions.\n\nStale arena identity."),
        &params,
        participant,
    )
    .expect_err("a stale identity cannot silently enter the next round");
    assert!(stale.message.contains("parent-identity-v2"));
    assert!(stale.message.contains("sha256"));
}

#[test]
fn competitive_room_requires_explicit_semantic_message_kinds() {
    assert!(
        validate_room_message_kind(
            Some(&MemythosArenaDecisionMethod::CompetitiveDebate),
            "consultation",
        )
        .is_err()
    );
    assert!(
        validate_room_message_kind(
            Some(&MemythosArenaDecisionMethod::CompetitiveDebate),
            "peer_proposal",
        )
        .is_ok()
    );
    assert_eq!(
        phase_from_message_kind("verdict_request").as_deref(),
        Some("judge")
    );
    assert!(validate_room_message_kind(None, "consultation").is_ok());
}

#[test]
fn competitive_room_routes_phases_through_their_native_roles() {
    let method = Some(&MemythosArenaDecisionMethod::BettingRound);
    assert!(
        validate_room_message_route(
            method,
            "notify_coordinator",
            "process_steward",
            "room_concierge",
        )
        .is_ok()
    );
    assert!(validate_room_message_route(method, "peer_bet", "room_concierge", "bettor",).is_ok());
    assert!(
        validate_room_message_route(method, "verdict_request", "room_concierge", "judge",).is_ok()
    );
    assert!(
        validate_room_message_route(method, "verdict_request", "room_concierge", "bettor",)
            .is_err()
    );
    assert!(
        validate_room_message_route(method, "judge_verdict", "bettor", "room_concierge",).is_err()
    );
    assert!(
        validate_room_message_route(method, "targeted_refinement", "room_concierge", "bettor",)
            .is_ok()
    );
    assert!(
        validate_room_message_route(method, "refinement_delta", "bettor", "room_concierge",)
            .is_ok()
    );
    assert!(
        validate_room_message_route(method, "final_verdict_request", "room_concierge", "judge",)
            .is_ok()
    );
    assert!(
        validate_room_message_route(method, "final_judge_verdict", "judge", "room_concierge",)
            .is_ok()
    );
    assert!(validate_room_message_route(None, "consultation", "peer", "observer").is_ok());
}

#[test]
fn competitive_room_cannot_request_judge_before_plural_bets_complete() {
    let room = MemythosRoom {
        room_id: "room-1".to_string(),
        case_id: "case-1".to_string(),
        layer_id: "layer-1".to_string(),
        arena_id: "arena-1".to_string(),
        topology: "concierge_hub".to_string(),
        participants: Vec::new(),
    };
    let delivery = |id: &str, receiver: &str| MemythosArenaMessageDelivery {
        delivery_id: id.to_string(),
        message_id: id.to_string(),
        human_summary: "bet".to_string(),
        status: "receiver_turn_completed".to_string(),
        sender_thread_id: "concierge".to_string(),
        receiver_thread_id: receiver.to_string(),
        arena_id: room.arena_id.clone(),
        round_id: "round-1".to_string(),
        phase: Some("bet".to_string()),
        delivery_mechanism: "room_loopback_send_input".to_string(),
        delivery_policy: None,
        aggregate_id: None,
        aggregate_state: None,
        checkpoint_state: None,
        checkpoint_event_refs: Vec::new(),
        receiver_turn_id: Some(format!("turn-{id}")),
        receiver_response_event_ref: None,
        delivered_as_human_instruction: false,
        memory_replay_required: false,
        event_refs: Vec::new(),
        rejection_reason: None,
        failure_reason: None,
    };
    let method = Some(&MemythosArenaDecisionMethod::CompetitiveDebate);
    assert!(
        validate_competitive_round_progress(
            method,
            "verdict_request",
            &room,
            None,
            &[delivery("one", "bettor-a")],
        )
        .is_err()
    );
    assert!(
        validate_competitive_round_progress(
            method,
            "verdict_request",
            &room,
            None,
            &[delivery("one", "bettor-a"), delivery("two", "bettor-b"),],
        )
        .is_ok()
    );
}

#[tokio::test]
async fn judge_completion_atomically_closes_every_parent_goal() {
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter::default());
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(9))
        .await
        .expect("competitive composition should provision");

    let delivery =
        |id: &str, phase: &str, receiver: &str, turn_id: &str| MemythosArenaMessageDelivery {
            delivery_id: id.to_string(),
            message_id: id.to_string(),
            human_summary: phase.to_string(),
            status: if phase == "arena_intake" {
                "receiver_turn_running".to_string()
            } else {
                "receiver_turn_completed".to_string()
            },
            sender_thread_id: "test::room_concierge::concierge".to_string(),
            receiver_thread_id: receiver.to_string(),
            arena_id: "arena-composition".to_string(),
            round_id: "round-1".to_string(),
            phase: Some(phase.to_string()),
            delivery_mechanism: "room_loopback_send_input".to_string(),
            delivery_policy: None,
            aggregate_id: None,
            aggregate_state: None,
            checkpoint_state: None,
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some(turn_id.to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: phase == "arena_intake",
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        };
    {
        let mut state = processor.state.lock().await;
        let bettor_growth = "test::bettor::bettor-growth";
        let bettor_risk = "test::bettor::bettor-risk";
        for phase in ["proposal", "peer_review_and_objection", "bet"] {
            state.arena_message_deliveries.push(delivery(
                &format!("{phase}-growth"),
                phase,
                bettor_growth,
                &format!("turn-{phase}-growth"),
            ));
            state.arena_message_deliveries.push(delivery(
                &format!("{phase}-risk"),
                phase,
                bettor_risk,
                &format!("turn-{phase}-risk"),
            ));
        }
        state.arena_message_deliveries.push(delivery(
            "judge",
            "judge",
            "test::judge::judge",
            "turn-judge",
        ));
        state
            .arena_message_deliveries
            .last_mut()
            .expect("judge delivery")
            .status = "receiver_turn_running".to_string();
        state.arena_message_deliveries.push(delivery(
            "intake",
            "arena_intake",
            "test::room_concierge::concierge",
            "turn-concierge",
        ));
        state
            .arena_message_deliveries
            .last_mut()
            .expect("arena intake delivery")
            .status = "receiver_turn_completed".to_string();
    }

    assert!(
            processor
                .record_native_turn_completed(
                    "test::judge::judge",
                    "turn-judge",
                    "completed",
                    Some(1),
                    Some(100),
                    None,
                    Some(
                        serde_json::json!({
                            "winner_participant_id": "bettor-growth",
                            "ranked_alternatives": ["bettor-risk"],
                            "winning_decision": "Adopt bounded reversible growth.",
                            "accepted_tradeoff": "Trade speed for lower reversal cost.",
                            "next_action": "close",
                            "contribution_attribution": [
                                {"participant_id": "bettor-growth", "claim_refs": ["claim://growth/wedge"], "disposition": "adopted", "rationale": "The wedge resolves the bounded objective."},
                                {"participant_id": "bettor-risk", "claim_refs": ["claim://risk/reversibility"], "disposition": "conditioned", "rationale": "Reversibility constrains acceleration."}
                            ],
                            "dissent": "Retain the bounded risk posture.",
                            "preserved_dissent": ["Retain the bounded risk posture."],
                            "targeted_refinements": [],
                            "reopening_signals": ["Unit economics materially deteriorate."],
                            "protected_decisions_status": "preserved",
                            "reopened_decision_refs": [],
                            "resume_scope_status": "not_applicable",
                            "rationale": "The growth posture wins within the declared reversible boundary."
                        })
                        .to_string(),
                    ),
                )
                .await
        );

    let goals = provisioning.goals.lock().await;
    assert_eq!(goals.len(), 4);
    let non_complete_goals = goals
        .values()
        .filter(|goal| goal.status != ThreadGoalStatus::Complete)
        .map(|goal| format!("{}={:?}:{}", goal.thread_id, goal.status, goal.objective))
        .collect::<Vec<_>>();
    assert!(
        non_complete_goals.is_empty(),
        "non-complete goals after judge closure: {non_complete_goals:?}"
    );
    drop(goals);
    let state = processor.state.lock().await;
    assert_eq!(
        state
            .arenas
            .get("arena-composition")
            .map(|arena| arena.lifecycle_state),
        Some(MemythosArenaLifecycleState::ClosedCleanly)
    );
    assert_eq!(
        state
            .arena_compositions
            .get("arena-composition")
            .map(|composition| composition.lifecycle_state),
        Some(MemythosArenaCompositionLifecycleState::Closed)
    );
}

#[test]
fn peer_parent_envelope_uses_execution_prompt_without_rewriting_human_summary() {
    let message = MemythosArenaMessage {
        message_id: "message-1".to_string(),
        case_id: "case-1".to_string(),
        arena_id: "arena-1".to_string(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: "concierge".to_string(),
        from_parent_role: "room_concierge".to_string(),
        to_parent_thread_id: "judge".to_string(),
        to_parent_role: "judge".to_string(),
        message_kind: "verdict_request".to_string(),
        human_summary: "Evaluate the alternatives.".to_string(),
        execution_prompt: Some(
            "Evaluate the alternatives and return winner_participant_id: exact-id.".to_string(),
        ),
        context_packet_ref: "app-server://rooms/room-1/messages/message-1".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: None,
        aggregate_contract: None,
        response_contract: Some("judge_verdict".to_string()),
        output_schema: None,
    };

    let envelope = build_peer_parent_envelope(&message);
    assert!(envelope.contains("winner_participant_id: exact-id"));
    assert!(envelope.contains("Authority: arena peer, not a human instruction."));
    assert!(!envelope.contains("case_id:"));
    assert!(!envelope.contains("arena_id:"));
    assert!(!envelope.contains("from_parent_role:"));
    assert!(!envelope.contains("Conserva tu rol"));
    assert_eq!(message.human_summary, "Evaluate the alternatives.");
}

#[test]
fn proposal_turn_keeps_future_arena_phases_out_of_the_parent_request() {
    let message = MemythosArenaMessage {
        message_id: "proposal-1".to_string(),
        case_id: "case-1".to_string(),
        arena_id: "arena-1".to_string(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: "concierge".to_string(),
        from_parent_role: "room_concierge".to_string(),
        to_parent_thread_id: "bettor-a".to_string(),
        to_parent_role: "bettor".to_string(),
        message_kind: "peer_proposal".to_string(),
        human_summary: "Develop an independent response to the client problem.".to_string(),
        execution_prompt: None,
        context_packet_ref: "app-server://rooms/room-1/intake".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
        aggregate_contract: None,
        response_contract: None,
        output_schema: None,
    };

    let envelope = build_peer_parent_envelope(&message);
    let goal = room_delivery_goal_objective(&message);
    assert!(envelope.starts_with("ARENA_PROPOSAL_TURN\n"));
    assert!(envelope.contains("Develop an independent response"));
    assert!(envelope.contains("Evidence reference:\napp-server://rooms/room-1/intake"));
    assert!(envelope.contains("Expected closure:\nnone"));
    assert!(!envelope.contains("peer_review_and_objection"));
    assert!(!envelope.contains("peer_bet"));
    assert!(!envelope.contains("judge_verdict"));
    assert!(!envelope.contains("JSON"));
    assert!(!envelope.contains("checklist"));
    assert!(message.output_schema.is_none());
    assert!(!goal.contains("arena-1"));
    assert!(!goal.contains("round-1"));
    assert!(!goal.contains("Act as the persistent parent role"));
    assert!(!goal.contains("Develop an independent response"));
}

#[tokio::test]
async fn aggregate_then_trigger_queues_until_expected_sources_are_complete() {
    let processor = MemythosRequestProcessor::new();
    let contract = MemythosArenaAggregateContract {
        aggregate_id: "judge-round-1".to_string(),
        recipient_thread_id: "judge".to_string(),
        expected_source_thread_ids: vec!["bettor-a".to_string(), "bettor-b".to_string()],
        quorum: 2,
        phase_id: "bet".to_string(),
        deadline_ref: None,
        completion_criteria_ref: "criteria://all-bets".to_string(),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    };
    let message = |id: &str, source: &str| MemythosArenaMessage {
        message_id: id.to_string(),
        case_id: "case-1".to_string(),
        arena_id: "arena-1".to_string(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: source.to_string(),
        from_parent_role: "bettor".to_string(),
        to_parent_thread_id: "judge".to_string(),
        to_parent_role: "judge".to_string(),
        message_kind: "peer_bet".to_string(),
        human_summary: format!("Bet from {source}"),
        execution_prompt: None,
        context_packet_ref: "context://round-1".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
        aggregate_contract: Some(contract.clone()),
        response_contract: Some("judge_verdict".to_string()),
        output_schema: None,
    };
    let mut first = message("bet-a", "bettor-a");
    let mut second = message("bet-b", "bettor-b");
    let mut state = processor.state.lock().await;

    assert_eq!(
        prepare_native_aggregate_delivery(&mut state, &mut first).expect("first bet"),
        Some(MemythosArenaAggregateState::Collecting)
    );
    assert!(!first.requires_response);
    assert_eq!(
        prepare_native_aggregate_delivery(&mut state, &mut second).expect("second bet"),
        Some(MemythosArenaAggregateState::ReadyByExpectedSources)
    );
    assert!(second.requires_response);
    assert_eq!(
        finalize_native_aggregate_delivery(
            &mut state,
            &second,
            Some(MemythosArenaAggregateState::ReadyByExpectedSources),
            true,
        ),
        Some(MemythosArenaAggregateState::RecipientTriggered)
    );

    let mut late = message("bet-a-late", "bettor-a");
    assert!(prepare_native_aggregate_delivery(&mut state, &mut late).is_err());
}

#[tokio::test]
async fn autonomous_bet_aggregate_applies_the_native_judge_contract() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let growth_thread_id = thread_for("bettor-growth");
    let risk_thread_id = thread_for("bettor-risk");
    let judge_thread_id = thread_for("judge");
    let contract = MemythosArenaAggregateContract {
        aggregate_id: "judge-bets-round-1".to_string(),
        recipient_thread_id: judge_thread_id.clone(),
        expected_source_thread_ids: vec![growth_thread_id.clone(), risk_thread_id.clone()],
        quorum: 2,
        phase_id: "bet".to_string(),
        deadline_ref: None,
        completion_criteria_ref: "criteria://all-bets".to_string(),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    };
    let message = |id: &str, source: &str| MemythosArenaMessage {
        message_id: id.to_string(),
        case_id: "case-1".to_string(),
        arena_id: response.room.arena_id.clone(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: source.to_string(),
        from_parent_role: "bettor".to_string(),
        to_parent_thread_id: judge_thread_id.clone(),
        to_parent_role: "judge".to_string(),
        message_kind: "peer_bet".to_string(),
        human_summary: format!("Final bet from {source}"),
        execution_prompt: None,
        context_packet_ref: "context://round-1".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
        aggregate_contract: Some(contract.clone()),
        response_contract: None,
        output_schema: None,
    };
    let mut first = message("bet-growth", &growth_thread_id);
    let mut second = message("bet-risk", &risk_thread_id);
    let mut state = processor.state.lock().await;
    prepare_native_aggregate_delivery(&mut state, &mut first).expect("first bet");
    apply_native_checkpoint_execution_contract(&state, &mut first)
        .expect("open aggregate contract");
    assert!(first.execution_prompt.is_none());

    prepare_native_aggregate_delivery(&mut state, &mut second).expect("second bet");
    apply_native_checkpoint_execution_contract(&state, &mut second)
        .expect("sealed aggregate contract");
    let prompt = second
        .execution_prompt
        .as_deref()
        .expect("sealed aggregate must carry the judge contract");
    assert!(prompt.contains("Return only the JSON object required by the native output schema"));
    assert!(prompt.contains("measures only whether protected guardrails"));
    assert!(prompt.contains("separately describes the work scope"));
    assert!(prompt.contains("protected_decisions_status=preserved"));
    assert!(prompt.contains("reopened_decision_refs"));
    assert!(prompt.contains("resume_scope_status=partially_reopened"));
    assert!(prompt.contains(
        "does not by itself make that participant's hypothesis the global lead diagnosis"
    ));
    assert!(prompt.contains("explain only changed evidence"));
    assert!(prompt.contains("bettor-growth"));
    assert!(prompt.contains("bettor-risk"));
    assert!(prompt.contains("returned automatically to the Room Concierge"));
    assert_eq!(second.response_contract.as_deref(), Some("judge_verdict"));
    assert_eq!(
        second
            .output_schema
            .as_ref()
            .and_then(|schema| schema.pointer("/properties/winner_participant_id/type"))
            .and_then(serde_json::Value::as_str),
        Some("string")
    );
    assert_eq!(
        second
            .output_schema
            .as_ref()
            .and_then(|schema| schema.pointer("/properties/resume_scope_status/type"))
            .and_then(serde_json::Value::as_str),
        Some("string")
    );
}

#[tokio::test]
async fn completed_judge_turn_queues_verdict_for_concierge_continuity() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let judge_thread_id = thread_for("judge");
    let judge_turn_id = "judge-turn-1";
    let mut state = processor.state.lock().await;
    state
        .arena_message_deliveries
        .push(MemythosArenaMessageDelivery {
            delivery_id: "judge-wake".to_string(),
            message_id: "sealed-bets".to_string(),
            human_summary: "All bets sealed.".to_string(),
            status: "receiver_turn_completed".to_string(),
            sender_thread_id: thread_for("bettor-risk"),
            receiver_thread_id: judge_thread_id.clone(),
            arena_id: response.room.arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: Some("judge".to_string()),
            delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
            aggregate_id: Some("judge-bets-round-1".to_string()),
            aggregate_state: Some(MemythosArenaAggregateState::Consumed),
            checkpoint_state: Some(MemythosArenaCheckpointState::NextPhaseDispatched),
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some(judge_turn_id.to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        });
    state.native_parent_turn_responses.insert(
        native_token_usage_key(&judge_thread_id, judge_turn_id),
        ParentTurnResponse {
            status: Some(TurnStatus::Completed),
            request_item_ref: None,
            request_text: None,
            item_ref: Some("app-server://judge/verdict".to_string()),
            text: Some(
                "winner_participant_id: bettor-risk\nprotected_decisions_status: preserved"
                    .to_string(),
            ),
        },
    );

    let loopback = native_turn_loopback_candidate(
        &state,
        &judge_thread_id,
        judge_turn_id,
        "app-server://judge/completed",
    )
    .expect("judge completion must loop back through the native room");
    assert_eq!(loopback.to_parent_thread_id, concierge_thread_id);
    assert_eq!(loopback.message_kind, "judge_verdict");
    assert!(!loopback.requires_response);
    assert_eq!(
        loopback.delivery_policy,
        Some(MemythosArenaDeliveryPolicy::QueueOnly)
    );
    assert!(loopback.response_contract.is_none());
    assert!(loopback.execution_prompt.is_none());
}

#[tokio::test]
async fn closing_judge_verdict_queues_individual_learning_on_each_bettor_parent() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let growth_thread_id = thread_for("bettor-growth");
    let risk_thread_id = thread_for("bettor-risk");
    let judge_thread_id = thread_for("judge");
    let judge_turn_id = "judge-close-turn";
    let verdict = serde_json::json!({
            "winner_participant_id": "bettor-growth",
            "ranked_alternatives": ["bettor-risk"],
            "winning_decision": "Adopt the bounded growth wedge.",
            "accepted_tradeoff": "Keep the risk objection as an explicit reality check.",
            "next_action": "close",
            "contribution_attribution": [
                {"participant_id": "bettor-growth", "claim_refs": ["claim://growth/wedge"], "disposition": "adopted", "rationale": "It best resolves the bounded objective."},
                {"participant_id": "bettor-risk", "claim_refs": ["claim://risk/acceptance"], "disposition": "preserved_dissent", "rationale": "Its acceptance condition remains useful."}
            ],
            "dissent": "Acceptance remains a reality-check condition.",
            "preserved_dissent": ["Reopen if acceptance evidence fails."],
            "targeted_refinements": [],
            "reopening_signals": ["Acceptance evidence fails."],
            "protected_decisions_status": "preserved",
            "reopened_decision_refs": [],
            "resume_scope_status": "not_applicable",
            "rationale": "The sealed bets are sufficient to close."
        })
        .to_string();
    let mut state = processor.state.lock().await;
    state
        .arena_message_deliveries
        .push(MemythosArenaMessageDelivery {
            delivery_id: "judge-close-wake".to_string(),
            message_id: "sealed-bets-close".to_string(),
            human_summary: "All bets sealed.".to_string(),
            status: "receiver_turn_completed".to_string(),
            sender_thread_id: risk_thread_id.clone(),
            receiver_thread_id: judge_thread_id.clone(),
            arena_id: response.room.arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: Some("judge".to_string()),
            delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
            aggregate_id: Some("judge-bets-round-1".to_string()),
            aggregate_state: Some(MemythosArenaAggregateState::Consumed),
            checkpoint_state: Some(MemythosArenaCheckpointState::NextPhaseDispatched),
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some(judge_turn_id.to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        });
    state.native_parent_turn_responses.insert(
        native_token_usage_key(&judge_thread_id, judge_turn_id),
        ParentTurnResponse {
            status: Some(TurnStatus::Completed),
            request_item_ref: None,
            request_text: None,
            item_ref: Some("app-server://judge/close-verdict".to_string()),
            text: Some(verdict),
        },
    );

    let messages = native_turn_loopback_candidates(
        &state,
        &judge_thread_id,
        judge_turn_id,
        "app-server://judge/close-completed",
    );
    assert_eq!(messages.len(), 3);
    assert!(messages.iter().any(|message| {
        message.message_kind == "judge_verdict"
            && message.to_parent_thread_id == concierge_thread_id
    }));
    let learning = messages
        .iter()
        .filter(|message| message.message_kind == "judge_learning")
        .collect::<Vec<_>>();
    assert_eq!(learning.len(), 2);
    assert_eq!(
        learning
            .iter()
            .map(|message| message.to_parent_thread_id.as_str())
            .collect::<HashSet<_>>(),
        HashSet::from([growth_thread_id.as_str(), risk_thread_id.as_str()])
    );
    assert!(learning.iter().all(|message| {
        message.from_parent_thread_id == concierge_thread_id
            && !message.requires_response
            && message.execution_prompt.is_none()
            && message.delivery_policy == Some(MemythosArenaDeliveryPolicy::QueueOnly)
            && validate_room_message_route(
                Some(&MemythosArenaDecisionMethod::BettingRound),
                &message.message_kind,
                &message.from_parent_role,
                &message.to_parent_role,
            )
            .is_ok()
    }));
    let growth_learning = learning
        .iter()
        .find(|message| message.to_parent_thread_id == growth_thread_id)
        .expect("growth learning should exist");
    assert!(
        growth_learning
            .human_summary
            .contains("claim://growth/wedge")
    );
    assert!(
        !growth_learning
            .human_summary
            .contains("claim://risk/acceptance")
    );
}

#[tokio::test]
async fn targeted_judge_verdict_reuses_selected_parents_and_returns_to_the_same_judge() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let growth_thread_id = thread_for("bettor-growth");
    let risk_thread_id = thread_for("bettor-risk");
    let judge_thread_id = thread_for("judge");
    let judge_turn_id = "judge-targeted-turn";
    let verdict = serde_json::json!({
            "winner_participant_id": "bettor-growth",
            "ranked_alternatives": ["bettor-risk"],
            "winning_decision": "Retain the bounded growth wedge subject to two owner attestations.",
            "accepted_tradeoff": "One bounded clarification turn delays closure.",
            "next_action": "targeted_refinement",
            "contribution_attribution": [
                {"participant_id": "bettor-growth", "claim_refs": ["claim://growth/owner"], "disposition": "conditioned", "rationale": "The owner must attest the threshold."},
                {"participant_id": "bettor-risk", "claim_refs": ["claim://risk/acceptance"], "disposition": "preserved_dissent", "rationale": "Acceptance evidence must remain explicit."}
            ],
            "dissent": "The owner attestations are not yet in the sealed evidence.",
            "preserved_dissent": ["Do not infer owner attestations."],
            "targeted_refinements": [
                {"participant_id": "bettor-growth", "tension": "Threshold ownership", "request": "Attest the measurable threshold owned by your perspective.", "sufficiency_criterion": "One explicit threshold and owner."},
                {"participant_id": "bettor-risk", "tension": "Acceptance ownership", "request": "Attest the acceptance proof owned by your perspective.", "sufficiency_criterion": "One observable acceptance proof and owner."}
            ],
            "reopening_signals": ["Either owner rejects its attestation."],
            "protected_decisions_status": "preserved",
            "reopened_decision_refs": [],
            "resume_scope_status": "not_applicable",
            "rationale": "Only the named owners can close the localized tension."
        })
        .to_string();
    let mut state = processor.state.lock().await;
    state
        .arena_message_deliveries
        .push(MemythosArenaMessageDelivery {
            delivery_id: "judge-targeted-wake".to_string(),
            message_id: "sealed-bets-targeted".to_string(),
            human_summary: "All bets sealed.".to_string(),
            status: "receiver_turn_completed".to_string(),
            sender_thread_id: risk_thread_id.clone(),
            receiver_thread_id: judge_thread_id.clone(),
            arena_id: response.room.arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: Some("judge".to_string()),
            delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
            aggregate_id: Some("judge-bets-round-1".to_string()),
            aggregate_state: Some(MemythosArenaAggregateState::Consumed),
            checkpoint_state: Some(MemythosArenaCheckpointState::NextPhaseDispatched),
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some(judge_turn_id.to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        });
    state.native_parent_turn_responses.insert(
        native_token_usage_key(&judge_thread_id, judge_turn_id),
        ParentTurnResponse {
            status: Some(TurnStatus::Completed),
            request_item_ref: None,
            request_text: None,
            item_ref: Some("app-server://judge/targeted-verdict".to_string()),
            text: Some(verdict.clone()),
        },
    );

    let messages = native_turn_loopback_candidates(
        &state,
        &judge_thread_id,
        judge_turn_id,
        "app-server://judge/targeted-completed",
    );
    assert_eq!(messages.len(), 3);
    assert!(messages.iter().any(|message| {
        message.message_kind == "judge_verdict"
            && message.to_parent_thread_id == concierge_thread_id
            && !message.requires_response
    }));
    let targeted = messages
        .iter()
        .filter(|message| message.message_kind == "targeted_refinement")
        .collect::<Vec<_>>();
    assert_eq!(targeted.len(), 2);
    assert_eq!(
        targeted
            .iter()
            .map(|message| message.to_parent_thread_id.as_str())
            .collect::<HashSet<_>>(),
        HashSet::from([growth_thread_id.as_str(), risk_thread_id.as_str()])
    );
    assert!(targeted.iter().all(|message| {
        message.from_parent_thread_id == concierge_thread_id
            && message.requires_response
            && message.delivery_policy == Some(MemythosArenaDeliveryPolicy::Immediate)
    }));

    state
        .arena_message_deliveries
        .push(MemythosArenaMessageDelivery {
            delivery_id: "judge-verdict-loopback".to_string(),
            message_id: format!("turn-loopback-{judge_turn_id}"),
            human_summary: verdict,
            status: "queued_in_native_mailbox".to_string(),
            sender_thread_id: judge_thread_id.clone(),
            receiver_thread_id: concierge_thread_id.clone(),
            arena_id: response.room.arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: Some("judge".to_string()),
            delivery_mechanism: "native_mailbox".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::QueueOnly),
            aggregate_id: None,
            aggregate_state: None,
            checkpoint_state: None,
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: None,
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        });
    let contract = canonical_native_concierge_refinement_contract(
        &state,
        &response.room,
        "round-1",
        response
            .room
            .participants
            .iter()
            .find(|participant| participant.thread_id == concierge_thread_id)
            .expect("concierge participant"),
    )
    .expect("targeted deltas should share one native aggregate");
    assert_eq!(contract.quorum, 2);
    assert_eq!(
        contract
            .expected_source_thread_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>(),
        HashSet::from([growth_thread_id.as_str(), risk_thread_id.as_str()])
    );

    state
        .arena_message_deliveries
        .push(MemythosArenaMessageDelivery {
            delivery_id: "refinement-packet-wake".to_string(),
            message_id: "all-refinement-deltas".to_string(),
            human_summary: "Two attributed refinement deltas.".to_string(),
            status: "receiver_turn_completed".to_string(),
            sender_thread_id: thread_for("bettor-risk"),
            receiver_thread_id: concierge_thread_id.clone(),
            arena_id: response.room.arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: Some("targeted_refinement".to_string()),
            delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
            aggregate_id: Some(contract.aggregate_id),
            aggregate_state: Some(MemythosArenaAggregateState::Consumed),
            checkpoint_state: Some(MemythosArenaCheckpointState::ConciergeSynthesis),
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some("concierge-refinement-turn".to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        });
    state.native_parent_turn_responses.insert(
        native_token_usage_key(&concierge_thread_id, "concierge-refinement-turn"),
        ParentTurnResponse {
            status: Some(TurnStatus::Completed),
            request_item_ref: None,
            request_text: None,
            item_ref: Some("app-server://concierge/refinement-packet".to_string()),
            text: Some("Both attributed owner attestations are sealed.".to_string()),
        },
    );
    let final_request = native_turn_loopback_candidates(
        &state,
        &concierge_thread_id,
        "concierge-refinement-turn",
        "app-server://concierge/refinement-completed",
    );
    assert_eq!(final_request.len(), 1);
    assert_eq!(final_request[0].message_kind, "final_verdict_request");
    assert_eq!(final_request[0].to_parent_thread_id, judge_thread_id);
    assert!(final_request[0].requires_response);
}

#[tokio::test]
async fn aggregate_then_trigger_does_not_count_duplicate_messages() {
    let processor = MemythosRequestProcessor::new();
    let contract = MemythosArenaAggregateContract {
        aggregate_id: "planner-round-1".to_string(),
        recipient_thread_id: "planner".to_string(),
        expected_source_thread_ids: vec!["peer-a".to_string(), "peer-b".to_string()],
        quorum: 2,
        phase_id: "proposal".to_string(),
        deadline_ref: Some("deadline://round-1".to_string()),
        completion_criteria_ref: "criteria://two-proposals".to_string(),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::QueueWithoutRetrigger,
    };
    let mut message = MemythosArenaMessage {
        message_id: "proposal-a".to_string(),
        case_id: "case-1".to_string(),
        arena_id: "arena-1".to_string(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: "peer-a".to_string(),
        from_parent_role: "peer".to_string(),
        to_parent_thread_id: "planner".to_string(),
        to_parent_role: "peer".to_string(),
        message_kind: "peer_proposal".to_string(),
        human_summary: "Proposal A".to_string(),
        execution_prompt: None,
        context_packet_ref: "context://round-1".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
        aggregate_contract: Some(contract),
        response_contract: None,
        output_schema: None,
    };
    let mut state = processor.state.lock().await;
    prepare_native_aggregate_delivery(&mut state, &mut message).expect("first proposal");

    let mut duplicate = message.clone();
    assert!(prepare_native_aggregate_delivery(&mut state, &mut duplicate).is_err());
    let aggregate = state
        .arena_message_aggregates
        .values()
        .next()
        .expect("aggregate");
    assert_eq!(aggregate.received_source_thread_ids.len(), 1);
}

#[tokio::test]
async fn completed_bettor_phase_fans_out_through_native_aggregate_mailboxes() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let bettor_threads = response
        .room
        .participants
        .iter()
        .filter(|participant| participant.parent_role == "bettor")
        .map(|participant| participant.thread_id.clone())
        .collect::<Vec<_>>();
    assert!(bettor_threads.len() >= 2);
    let concierge_thread_id = response
        .leases
        .iter()
        .find(|lease| lease.participant_id == "concierge")
        .expect("concierge")
        .thread_id
        .clone();
    let mut state = processor.state.lock().await;
    for (index, bettor_thread_id) in bettor_threads.iter().enumerate() {
        let turn_id = format!("proposal-turn-{index}");
        state
            .arena_message_deliveries
            .push(MemythosArenaMessageDelivery {
                delivery_id: format!("proposal-assignment-{index}"),
                message_id: format!("proposal-request-{index}"),
                human_summary: "Develop an independent proposal.".to_string(),
                status: "receiver_turn_completed".to_string(),
                sender_thread_id: concierge_thread_id.clone(),
                receiver_thread_id: bettor_thread_id.clone(),
                arena_id: response.room.arena_id.clone(),
                round_id: "round-1".to_string(),
                phase: Some("proposal".to_string()),
                delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                aggregate_id: None,
                aggregate_state: None,
                checkpoint_state: None,
                checkpoint_event_refs: Vec::new(),
                receiver_turn_id: Some(turn_id.clone()),
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: Vec::new(),
                rejection_reason: None,
                failure_reason: None,
            });
        state.native_parent_turn_responses.insert(
            native_token_usage_key(bettor_thread_id, &turn_id),
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: None,
                request_text: None,
                item_ref: Some(format!("app-server://proposal-{index}")),
                text: Some(format!("Proposal {index} with explicit tradeoffs.")),
            },
        );
    }

    let source = bettor_threads.last().expect("last bettor");
    let turn_id = format!("proposal-turn-{}", bettor_threads.len() - 1);
    let fanout = native_turn_loopback_candidates(
        &state,
        source,
        &turn_id,
        "app-server://proposal-set/completed",
    );
    assert_eq!(
        fanout.len(),
        bettor_threads.len() * (bettor_threads.len() - 1)
    );
    assert!(fanout.iter().all(|message| {
        message.to_parent_role == "bettor"
            && message.to_parent_thread_id != message.from_parent_thread_id
            && message.message_kind == "peer_review_and_objection"
            && message.delivery_policy == Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger)
            && message.aggregate_contract.as_ref().is_some_and(|contract| {
                contract.phase_id == "proposal"
                    && contract.expected_source_thread_ids.len() == bettor_threads.len() - 1
                    && contract.quorum
                        == u32::try_from(bettor_threads.len() - 1).expect("bettor quorum fits u32")
            })
    }));
    for bettor_thread_id in &bettor_threads {
        assert_eq!(
            fanout
                .iter()
                .filter(|message| message.to_parent_thread_id == *bettor_thread_id)
                .count(),
            bettor_threads.len() - 1
        );
    }
}

#[test]
fn native_method_authority_does_not_consume_upstream_business_authority() {
    let mut params = competitive_composition_params();
    params.upstream_authority_scope = vec!["recommend".to_string()];
    for participant in &mut params.contract.participants {
        participant.authority_scope = match participant.agent_role.as_str() {
            "room_concierge" | "process_steward" => vec!["coordinate".to_string()],
            "judge" => vec!["judge".to_string()],
            _ => vec!["recommend".to_string()],
        };
    }

    validate_arena_composition_contract(&params)
        .expect("native coordination and judging authority belongs to the selected method");

    params
        .contract
        .participants
        .iter_mut()
        .find(|participant| participant.agent_role == "bettor")
        .expect("bettor")
        .authority_scope = vec!["decide_business_policy".to_string()];
    assert!(validate_arena_composition_contract(&params).is_err());
}

#[test]
fn downstream_delegate_is_native_room_concierge_method_authority() {
    let mut params = competitive_composition_params();
    params.upstream_authority_scope = vec!["recommend".to_string(), "delegate".to_string()];
    for participant in &mut params.contract.participants {
        participant.authority_scope = match participant.agent_role.as_str() {
            "room_concierge" => vec!["coordinate".to_string(), "delegate".to_string()],
            "judge" => vec!["judge".to_string()],
            _ => vec!["recommend".to_string()],
        };
    }

    validate_arena_composition_contract(&params)
        .expect("the coordinator may promote an approved contract within upstream authority");

    params.upstream_authority_scope = vec!["recommend".to_string()];
    validate_arena_composition_contract(&params)
        .expect("native delegation coordinates the method and does not consume business authority");
}

#[test]
fn ordinary_competitive_arena_rejects_an_unnecessary_process_steward() {
    let mut params = competitive_composition_params();
    let mut steward = params.contract.participants[0].clone();
    steward.participant_id = "process-steward".to_string();
    steward.agent_role = "process_steward".to_string();
    steward.stance = "end_to_end_integrity".to_string();
    steward.role_objective = "Coordinate the process".to_string();
    params.contract.participants.push(steward);
    params.contract.coordination.coordinator_participant_id = Some("process-steward".to_string());

    let error = validate_arena_composition_contract(&params)
        .expect_err("ordinary checkpoint coordination belongs to the Room Concierge");
    assert!(error.message.contains("exceptional-governance rationale"));

    params.contract.rationale =
        "Regulatory exception requires an independent process steward".to_string();
    params.contract.cost_envelope.total_token_budget = Some(100_000);
    params.contract.cost_envelope.coordination_token_budget = Some(40_000);
    validate_arena_composition_contract(&params)
        .expect("an explicit governance exception may add a process steward");
}

#[test]
fn arena_composition_participant_requires_native_reasoning_effort() {
    let participant = competitive_composition_params()
        .contract
        .participants
        .into_iter()
        .next()
        .expect("competitive composition has a participant");
    let mut value = serde_json::to_value(participant).expect("participant should serialize");
    value
        .as_object_mut()
        .expect("participant should be an object")
        .remove("reasoningEffort");

    let error = serde_json::from_value::<
        codex_app_server_protocol::MemythosArenaCompositionParticipant,
    >(value)
    .expect_err("reasoningEffort must not be omitted by the native planner");

    assert!(error.to_string().contains("reasoningEffort"));
}

#[test]
fn arena_composition_rejects_custom_reasoning_effort() {
    let mut params = competitive_composition_params();
    params.contract.participants[0].reasoning_effort =
        ReasoningEffort::Custom("future-effort".to_string());

    let error = validate_arena_composition_contract(&params)
        .expect_err("arena participants must use known app-server effort values");

    assert!(error.message.contains("reasoning effort"));
}

#[test]
fn arena_composition_rejects_effort_incompatible_with_active_parent_toolset() {
    for effort in [ReasoningEffort::None, ReasoningEffort::Minimal] {
        let mut params = competitive_composition_params();
        params.contract.participants[0].reasoning_effort = effort.clone();

        let error = validate_arena_composition_contract(&params)
            .expect_err("active arena parent tools require low or greater reasoning effort");

        assert!(error.message.contains(effort.as_str()));
        assert!(error.message.contains("active arena parent toolset"));
    }
}

#[tokio::test]
async fn arena_request_owns_planning_provisioning_and_initial_activation() {
    let contract = competitive_composition_params().contract;
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter::default());
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(FakeArenaCompositionPlanningAdapter { contract }),
    );

    let response = processor
        .arena_request(semantic_arena_request_params(), ConnectionId(7))
        .await
        .expect("semantic request should create and activate the arena internally");
    let ClientResponsePayload::MemythosArenaRequest(response) = response else {
        panic!("expected semantic arena request response");
    };

    assert_eq!(response.planner_thread_id, "planner-thread");
    assert_eq!(response.planner_turn_id, "planner-turn");
    assert_eq!(response.composition.leases.len(), 4);
    assert_eq!(response.composition.planned_token_budget, Some(80_000));
    assert!(
        response
            .composition
            .leases
            .iter()
            .all(|lease| lease.token_budget == Some(20_000))
    );
    assert_eq!(
        response
            .composition
            .leases
            .iter()
            .filter(|lease| lease.goal_status == ThreadGoalStatus::Active)
            .count(),
        1
    );
    assert!(response.composition.leases.iter().any(|lease| {
        lease.participant_id == "concierge" && lease.goal_status == ThreadGoalStatus::Active
    }));
    assert_eq!(
        response
            .composition
            .leases
            .iter()
            .filter(|lease| lease.role == "bettor")
            .count(),
        2
    );
    let initial_delivery = response
        .initial_delivery
        .as_ref()
        .expect("initial request should deliver intake");
    assert!(initial_delivery.human_instruction);
    assert_eq!(initial_delivery.round_id, "arena-composition-round-1");
    assert_eq!(
        initial_delivery.thread_id,
        "test::room_concierge::concierge"
    );
    provisioning
        .goals
        .lock()
        .await
        .get_mut("test::room_concierge::concierge")
        .expect("concierge goal should exist")
        .status = ThreadGoalStatus::Complete;
    processor
        .room_send_input(MemythosRoomSendInputParams {
            room_id: "room-composition".to_string(),
            room_message_ref: "app-server://rooms/room-composition/human-intake/follow-up"
                .to_string(),
            delivery_ref: "app-server://rooms/room-composition/human-intake/follow-up/delivery"
                .to_string(),
            from_parent_thread_id: None,
            via_concierge_thread_id: None,
            to_parent_thread_id: "test::room_concierge::concierge".to_string(),
            source_parent_key: "human:test".to_string(),
            target_parent_key: response
                .composition
                .leases
                .iter()
                .find(|lease| lease.participant_id == "concierge")
                .expect("concierge lease should exist")
                .parent_key
                .clone(),
            message_kind: "human_intake".to_string(),
            message_authority: "human_delegated".to_string(),
            human_instruction: true,
            response_contract: "Respond to the follow-up.".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("follow-up".to_string()),
            human_summary: "Clarify the arena outcome.".to_string(),
            prompt: "Clarify the arena outcome.".to_string(),
            metadata: serde_json::Map::new(),
            output_schema: None,
        })
        .await
        .expect("a later room turn should create a bounded assignment on the same parent");
    let transitions = provisioning.goal_transitions.lock().await;
    assert_eq!(transitions.len(), 2);
    assert!(transitions.iter().all(|transition| {
        transition.0 == "test::room_concierge::concierge"
            && transition.1.as_deref().is_some_and(|objective| {
                objective.contains("Complete only native room assignment")
                    && objective.contains("call update_goal with status complete")
                    && objective.contains("memythos_room_send_message")
                    && objective.contains("not materialized progress")
            })
            && transition.2 == ThreadGoalStatus::Active
            && transition.3
    }));
    assert!(
        transitions[1]
            .1
            .as_deref()
            .is_some_and(|objective| objective.contains("follow-up"))
    );
    let state = processor.state.lock().await;
    assert_eq!(state.arena_compositions.len(), 1);
    assert_eq!(state.arena_message_deliveries.len(), 2);
}

#[test]
fn arena_intake_makes_app_server_the_only_activation_authority() {
    let params = semantic_arena_request_params();
    let contract = competitive_composition_params().contract;

    let prompt = build_arena_intake_prompt(&params, &contract, &initial_resume_execution_plan());

    assert!(prompt.contains("one autonomous native arena run"));
    assert!(prompt.contains("the client will only observe"));
    assert!(prompt.contains("peer_proposal"));
    assert!(prompt.contains("fans the sealed proposal checkpoint out to every bettor"));
    assert!(prompt.contains("fans the sealed review checkpoint out for final bets"));
    assert!(prompt.contains("activates the Judge exactly once"));
    assert!(prompt.contains("bettor-growth"));
    assert!(prompt.contains("bettor-risk"));
    assert!(prompt.contains("Do not ask the client to activate phases"));
    assert!(prompt.contains("You are the Room Concierge and own the arena objective"));
    assert!(prompt.contains("mechanical mailbox transitions under the arena contract"));
    assert!(prompt.contains("app-server dispatches the phase assignments"));
    assert!(prompt.contains("Do not call tools to activate proposals"));
    assert!(prompt.contains("an ordinary checkpoint does not"));
    assert!(prompt.contains("Do not keep a concierge turn alive while peers work"));
    assert!(prompt.contains("Do not issue separate cross-read, bet, or verdict requests"));
    assert!(prompt.contains("Never bet or judge as Room Concierge"));
}

#[test]
fn resumed_arena_intake_preserves_three_distinct_semantic_scopes() {
    let mut params = semantic_arena_request_params();
    params.resume_context = Some(codex_app_server_protocol::MemythosArenaResumeContext {
        previous_decision_refs: vec!["decision://slowdown-observed".to_string()],
        previous_evidence_refs: vec!["evidence://baseline".to_string()],
        candidate_change_refs: vec!["evidence://instrumentation-defect".to_string()],
        protected_decisions: vec!["The slowdown was observed".to_string()],
        revisable_settlement: vec!["Causal hypothesis weights".to_string()],
        open_implementation_scope: vec!["Repair the attribution window".to_string()],
    });

    let prompt = build_arena_intake_prompt(
        &params,
        &competitive_composition_params().contract,
        &initial_resume_execution_plan(),
    );

    assert!(prompt.contains("Protected decisions:\n- The slowdown was observed"));
    assert!(prompt.contains("Revisable settlement:\n- Causal hypothesis weights"));
    assert!(prompt.contains("Open implementation scope:\n- Repair the attribution window"));
}

#[test]
fn partial_resume_intake_authorizes_only_one_combined_reassessment_per_affected_parent() {
    let params = semantic_arena_request_params();
    let plan = partial_resume_execution_plan(vec!["bettor-growth".to_string()]);

    let prompt =
        build_arena_intake_prompt(&params, &competitive_composition_params().contract, &plan);

    assert!(prompt.contains("bounded partial resume"));
    assert!(prompt.contains("resume_reassessment assignment"));
    assert!(prompt.contains("App-server will dispatch exactly one"));
    assert!(prompt.contains("Affected participant ids: bettor-growth"));
    assert!(prompt.contains("Source round: arena-composition-round-1"));
    assert!(prompt.contains("no proposal, cross-read, or separate bet assignment"));
    assert!(!prompt.contains("Dispatch exactly one independent peer_proposal"));
}

#[tokio::test]
async fn partial_resume_resolves_contract_participants_through_native_leases() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let provision = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("competitive composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(provision) = provision else {
        panic!("expected composition provision response");
    };
    let round_id = "arena-composition-round-2";
    let plan = partial_resume_execution_plan(vec![
        "concierge".to_string(),
        "bettor-growth".to_string(),
        "judge".to_string(),
    ]);
    let mut state = processor.state.lock().await;
    state
        .arena_resume_execution_plans
        .insert(arena_round_key(&provision.room.arena_id, round_id), plan);
    let room = provision.room;
    let concierge = room
        .participants
        .iter()
        .find(|participant| participant.parent_role == "room_concierge")
        .expect("concierge parent");
    let growth = room
        .participants
        .iter()
        .find(|participant| participant.thread_id.contains("bettor-growth"))
        .expect("growth parent");
    let risk = room
        .participants
        .iter()
        .find(|participant| participant.thread_id.contains("bettor-risk"))
        .expect("risk parent");
    let judge = room
        .participants
        .iter()
        .find(|participant| participant.parent_role == "judge")
        .expect("judge parent");

    validate_resume_execution_message(
        &state,
        &room,
        round_id,
        "resume_reassessment",
        concierge,
        growth,
    )
    .expect("affected parent should be authorized");
    assert!(
        validate_resume_execution_message(
            &state,
            &room,
            round_id,
            "resume_reassessment",
            concierge,
            risk,
        )
        .is_err(),
        "unaffected parent must not be scheduled"
    );
    let aggregate = canonical_native_judge_reassessment_contract(&state, &room, round_id, judge)
        .expect("partial judge aggregate should resolve live leased threads");
    assert_eq!(aggregate.quorum, 1);
    assert_eq!(
        aggregate.expected_source_thread_ids,
        vec![growth.thread_id.clone()]
    );
    assert_eq!(aggregate.phase_id, "resume_reassessment");
}

#[tokio::test]
async fn completed_partial_reassessments_trigger_exactly_one_native_judge_turn() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let provision = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("competitive composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(provision) = provision else {
        panic!("expected composition provision response");
    };
    let participant = |participant_id: &str| {
        provision
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
    };
    let concierge = participant("concierge");
    let growth = participant("bettor-growth");
    let risk = participant("bettor-risk");
    let judge = participant("judge");
    let round_id = "arena-composition-round-2";
    let assignment =
        |participant_id: &str, thread_id: &str, turn_id: &str| MemythosArenaMessageDelivery {
            delivery_id: format!("resume-assignment-{participant_id}"),
            message_id: format!("resume-request-{participant_id}"),
            human_summary: "Reassess only the affected position.".to_string(),
            status: "receiver_turn_running".to_string(),
            sender_thread_id: concierge.thread_id.clone(),
            receiver_thread_id: thread_id.to_string(),
            arena_id: provision.room.arena_id.clone(),
            round_id: round_id.to_string(),
            phase: Some("resume_reassessment".to_string()),
            delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_id: None,
            aggregate_state: None,
            checkpoint_state: None,
            checkpoint_event_refs: Vec::new(),
            receiver_turn_id: Some(turn_id.to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: Vec::new(),
            rejection_reason: None,
            failure_reason: None,
        };
    {
        let mut state = processor.state.lock().await;
        state.arena_resume_execution_plans.insert(
            arena_round_key(&provision.room.arena_id, round_id),
            partial_resume_execution_plan(vec![
                "concierge".to_string(),
                "bettor-growth".to_string(),
                "bettor-risk".to_string(),
                "judge".to_string(),
            ]),
        );
        state.arena_message_deliveries.push(assignment(
            "bettor-growth",
            &growth.thread_id,
            "turn-resume-growth",
        ));
        state.arena_message_deliveries.push(assignment(
            "bettor-risk",
            &risk.thread_id,
            "turn-resume-risk",
        ));
    }

    assert!(
        processor
            .record_native_turn_completed(
                &growth.thread_id,
                "turn-resume-growth",
                "completed",
                Some(1_000),
                Some(250),
                None,
                Some("Growth revises its position and bounded bet.".to_string()),
            )
            .await
    );
    {
        let state = processor.state.lock().await;
        let deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| {
                delivery
                    .aggregate_id
                    .as_deref()
                    .is_some_and(|id| id.ends_with("::judge_reassessment"))
            })
            .collect::<Vec<_>>();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(
            deliveries[0].aggregate_state,
            Some(MemythosArenaAggregateState::Collecting)
        );
        assert!(deliveries[0].receiver_turn_id.is_none());
    }

    assert!(
        processor
            .record_native_turn_completed(
                &risk.thread_id,
                "turn-resume-risk",
                "completed",
                Some(1_100),
                Some(275),
                None,
                Some("Risk revises its objections, bet, and breakpoints.".to_string()),
            )
            .await
    );
    let delivery_count = {
        let state = processor.state.lock().await;
        let deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| {
                delivery
                    .aggregate_id
                    .as_deref()
                    .is_some_and(|id| id.ends_with("::judge_reassessment"))
            })
            .collect::<Vec<_>>();
        assert_eq!(deliveries.len(), 2);
        assert_eq!(
            deliveries
                .iter()
                .filter(|delivery| delivery.receiver_turn_id.is_some())
                .count(),
            1,
            "the final affected reassessment must trigger one judge turn"
        );
        assert!(
            deliveries
                .iter()
                .all(|delivery| delivery.receiver_thread_id == judge.thread_id)
        );
        assert_eq!(
            deliveries
                .iter()
                .filter(|delivery| {
                    delivery.phase.as_deref() == Some("resume_reassessment")
                        && delivery.receiver_turn_id.is_none()
                })
                .count(),
            1,
            "the incomplete aggregate must remain a reassessment collection"
        );
        assert_eq!(
            deliveries
                .iter()
                .filter(|delivery| {
                    delivery.phase.as_deref() == Some("judge")
                        && delivery.receiver_turn_id.is_some()
                })
                .count(),
            1,
            "the sealed aggregate must become the single judge activation"
        );
        assert_eq!(
            deliveries[1].aggregate_state,
            Some(MemythosArenaAggregateState::RecipientTriggered)
        );
        state.arena_message_deliveries.len()
    };

    processor
        .record_native_turn_completed(
            &risk.thread_id,
            "turn-resume-risk",
            "completed",
            Some(1_100),
            Some(275),
            None,
            Some("Risk revises its objections, bet, and breakpoints.".to_string()),
        )
        .await;
    assert_eq!(
        processor.state.lock().await.arena_message_deliveries.len(),
        delivery_count,
        "turn completion replay must not duplicate a reassessment or judge wake"
    );
}

#[test]
fn competitive_method_consolidates_cross_read_and_objection_into_one_phase() {
    assert_eq!(
        phase_from_message_kind("peer_cross_read").as_deref(),
        Some("peer_review_and_objection")
    );
    assert_eq!(
        phase_from_message_kind("peer_objection").as_deref(),
        Some("peer_review_and_objection")
    );
}

#[test]
fn room_delivery_assigns_paused_or_completed_goals() {
    assert_eq!(
        room_delivery_goal_transition(&ThreadGoalStatus::Paused),
        RoomDeliveryGoalTransition::AssignDeliveryGoal
    );
    assert_eq!(
        room_delivery_goal_transition(&ThreadGoalStatus::Complete),
        RoomDeliveryGoalTransition::AssignDeliveryGoal
    );
    for status in [
        ThreadGoalStatus::Active,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::UsageLimited,
        ThreadGoalStatus::BudgetLimited,
    ] {
        assert_eq!(
            room_delivery_goal_transition(&status),
            RoomDeliveryGoalTransition::PreserveGoal,
            "room delivery must not override {status:?} goals"
        );
    }
}

#[test]
fn successful_turn_only_closes_the_goal_for_its_bounded_delivery() {
    let goal = ThreadGoal {
        thread_id: "bettor-a".to_string(),
        objective: concat!(
            "Complete only native room assignment review-1 for phase peer_cross_read. ",
            "Use the identity, stance, memory, and tools already installed on this parent."
        )
        .to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: Some(20_000),
        tokens_used: 3_000,
        time_used_seconds: 40,
        created_at: 0,
        updated_at: 0,
    };

    assert!(goal_matches_completed_room_delivery(
        &goal,
        &["review-1".to_string()]
    ));
    assert!(!goal_matches_completed_room_delivery(
        &goal,
        &["proposal-1".to_string()]
    ));

    let mut completed = goal;
    completed.status = ThreadGoalStatus::Complete;
    assert!(!goal_matches_completed_room_delivery(
        &completed,
        &["review-1".to_string()]
    ));
}

#[tokio::test]
async fn successful_bounded_delivery_turn_completes_active_goal_without_continuation() {
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter::default());
    provisioning.goals.lock().await.insert(
        "bettor-a".to_string(),
        ThreadGoal {
            thread_id: "bettor-a".to_string(),
            objective: room_delivery_goal_objective(&MemythosArenaMessage {
                message_id: "review-1".to_string(),
                case_id: "case-1".to_string(),
                arena_id: "arena-1".to_string(),
                round_id: "round-1".to_string(),
                from_parent_thread_id: "concierge".to_string(),
                from_parent_role: "room_concierge".to_string(),
                to_parent_thread_id: "bettor-a".to_string(),
                to_parent_role: "bettor".to_string(),
                message_kind: "peer_cross_read".to_string(),
                human_summary: "Review sealed proposals.".to_string(),
                execution_prompt: None,
                context_packet_ref: "app-server://rooms/room-1/checkpoints/proposals".to_string(),
                artifact_refs: Vec::new(),
                requires_response: true,
                delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                aggregate_contract: None,
                response_contract: None,
                output_schema: None,
            }),
            status: ThreadGoalStatus::Active,
            token_budget: Some(20_000),
            tokens_used: 3_000,
            time_used_seconds: 40,
            created_at: 0,
            updated_at: 0,
        },
    );
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(RecordOnlyParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );

    processor
        .complete_parent_goal_after_successful_delivery("bettor-a", &["review-1".to_string()])
        .await;

    let goal = provisioning
        .goals
        .lock()
        .await
        .get("bettor-a")
        .cloned()
        .expect("goal should remain materialized");
    assert_eq!(goal.status, ThreadGoalStatus::Complete);
    let transitions = provisioning.goal_transitions.lock().await;
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].2, ThreadGoalStatus::Complete);
    assert!(!transitions[0].3);
}

#[tokio::test]
async fn automatic_mailbox_delivery_rearms_a_completed_parent_for_the_current_phase() {
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter::default());
    provisioning.goals.lock().await.insert(
        "bettor-a".to_string(),
        ThreadGoal {
            thread_id: "bettor-a".to_string(),
            objective: "Complete proposal assignment proposal-1".to_string(),
            status: ThreadGoalStatus::Complete,
            token_budget: Some(20_000),
            tokens_used: 2_000,
            time_used_seconds: 10,
            created_at: 0,
            updated_at: 0,
        },
    );
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(RecordOnlyParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let message = MemythosArenaMessage {
        message_id: "bet-1".to_string(),
        case_id: "case-1".to_string(),
        arena_id: "arena-1".to_string(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: "concierge".to_string(),
        from_parent_role: "room_concierge".to_string(),
        to_parent_thread_id: "bettor-a".to_string(),
        to_parent_role: "bettor".to_string(),
        message_kind: "peer_bet".to_string(),
        human_summary: "Place the final bet from sealed evidence.".to_string(),
        execution_prompt: None,
        context_packet_ref: "app-server://rooms/room-1/checkpoints/review".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
        aggregate_contract: None,
        response_contract: None,
        output_schema: None,
    };

    let prepared = processor
        .prepare_parent_goal_for_delivery(&message)
        .await
        .expect("completed proposal goal should be replaced before the bet turn");

    assert!(prepared.assigned_for_delivery);
    assert_eq!(prepared.previous_goal.status, ThreadGoalStatus::Complete);
    assert_eq!(prepared.active_goal.status, ThreadGoalStatus::Active);
    assert!(prepared.active_goal.objective.contains("assignment bet-1"));
    assert!(prepared.active_goal.objective.contains("phase peer_bet"));
    assert!(!prepared.active_goal.objective.contains("proposal-1"));
    let transitions = provisioning.goal_transitions.lock().await;
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].0, "bettor-a");
    assert_eq!(transitions[0].2, ThreadGoalStatus::Active);
    assert!(transitions[0].3);
}

#[tokio::test]
async fn automatic_mailbox_delivery_rearms_an_active_parent_for_each_assignment() {
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter::default());
    provisioning.goals.lock().await.insert(
        "concierge".to_string(),
        ThreadGoal {
            thread_id: "concierge".to_string(),
            objective: "Complete initial room intake".to_string(),
            status: ThreadGoalStatus::Active,
            token_budget: Some(20_000),
            tokens_used: 2_000,
            time_used_seconds: 10,
            created_at: 0,
            updated_at: 0,
        },
    );
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(RecordOnlyParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let message = MemythosArenaMessage {
        message_id: "resume-1".to_string(),
        case_id: "case-1".to_string(),
        arena_id: "arena-1".to_string(),
        round_id: "round-2".to_string(),
        from_parent_thread_id: "human-intake".to_string(),
        from_parent_role: "human".to_string(),
        to_parent_thread_id: "concierge".to_string(),
        to_parent_role: "room_concierge".to_string(),
        message_kind: "human_intake".to_string(),
        human_summary: "Resume from the accepted parent contract.".to_string(),
        execution_prompt: None,
        context_packet_ref: "app-server://rooms/room-1/checkpoints/resume".to_string(),
        artifact_refs: Vec::new(),
        requires_response: true,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
        aggregate_contract: None,
        response_contract: None,
        output_schema: None,
    };

    let prepared = processor
        .prepare_parent_goal_for_delivery(&message)
        .await
        .expect("active parent should receive a fresh bounded assignment");

    assert!(prepared.assigned_for_delivery);
    assert_eq!(prepared.previous_goal.status, ThreadGoalStatus::Active);
    assert_eq!(prepared.active_goal.status, ThreadGoalStatus::Active);
    assert!(
        prepared
            .active_goal
            .objective
            .contains("assignment resume-1")
    );
    let transitions = provisioning.goal_transitions.lock().await;
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].0, "concierge");
    assert_eq!(transitions[0].2, ThreadGoalStatus::Active);
    assert!(transitions[0].3);
}

#[test]
fn open_cost_envelope_preserves_method_without_inventing_limits() {
    let mut contract = competitive_composition_params().contract;
    for participant in &mut contract.participants {
        participant.token_budget = None;
    }
    contract.cost_envelope = codex_app_server_protocol::MemythosArenaCostEnvelope {
        mode: codex_app_server_protocol::MemythosArenaCostEnvelopeMode::Open,
        rationale: "No accepted comparable run or explicit caller cap exists".to_string(),
        baseline_refs: Vec::new(),
        total_token_budget: None,
        coordination_token_budget: None,
        substantive_token_budget: None,
        method_integrity_funded: true,
        exhaustion_policy:
            codex_app_server_protocol::MemythosArenaCostExhaustionPolicy::WrapUpThenReplan,
    };

    validate_arena_cost_envelope(&contract).expect("open envelope should remain valid");
}

#[test]
fn bounded_cost_envelope_preserves_coordination_and_substantive_work() {
    let contract = competitive_composition_params().contract;

    validate_arena_cost_envelope(&contract).expect("bounded fixture should be valid");
    assert_eq!(
        contract.cost_envelope.coordination_token_budget,
        Some(20_000)
    );
    assert_eq!(
        contract.cost_envelope.substantive_token_budget,
        Some(60_000)
    );
    assert!(contract.cost_envelope.method_integrity_funded);
}

#[test]
fn calibrated_cost_envelope_requires_accepted_comparable_evidence() {
    let mut params = semantic_arena_request_params();
    params.cost_context = Some(codex_app_server_protocol::MemythosArenaCostContext {
        explicit_token_cap: None,
        comparable_evidence: vec![
            codex_app_server_protocol::MemythosArenaComparableCostEvidence {
                evidence_ref: "cost://prior-bpm-round".to_string(),
                tokens_used: 80_000,
                accepted_result: true,
                comparability_rationale: "Same decision method and uncertainty class".to_string(),
            },
        ],
    });
    let mut contract = competitive_composition_params().contract;
    contract.cost_envelope.mode =
        codex_app_server_protocol::MemythosArenaCostEnvelopeMode::Calibrated;
    contract.cost_envelope.baseline_refs = vec!["cost://prior-bpm-round".to_string()];

    validate_planned_arena_cost_context(&params, &contract)
        .expect("accepted comparable evidence should calibrate the envelope");
    contract.cost_envelope.baseline_refs = vec!["cost://unrelated-run".to_string()];
    assert!(validate_planned_arena_cost_context(&params, &contract).is_err());
}

#[test]
fn explicit_cost_cap_must_match_the_native_envelope() {
    let mut params = semantic_arena_request_params();
    params
        .cost_context
        .as_mut()
        .expect("fixture cost context")
        .explicit_token_cap = Some(70_000);
    let contract = competitive_composition_params().contract;

    assert!(validate_planned_arena_cost_context(&params, &contract).is_err());
}

#[test]
fn budget_limited_parent_requires_native_replan_instead_of_blind_delivery() {
    let goal = ThreadGoal {
        thread_id: "thread-risk".to_string(),
        objective: "Assess operational risk".to_string(),
        status: ThreadGoalStatus::BudgetLimited,
        token_budget: Some(20_000),
        tokens_used: 20_000,
        time_used_seconds: 42,
        created_at: 0,
        updated_at: 1,
    };

    let error = validate_parent_goal_accepts_delivery(&goal)
        .expect_err("budget-limited parents must not receive blind follow-up work");
    assert!(error.message.contains("preserve the completed work"));
    assert!(error.message.contains("memythos/arena/request"));
}

#[test]
fn competitive_envelope_rejects_underfunded_method_integrity() {
    let mut contract = competitive_composition_params().contract;
    contract.cost_envelope.method_integrity_funded = false;

    assert!(validate_arena_cost_envelope(&contract).is_err());
}

#[tokio::test]
async fn arena_request_partially_resumes_without_replanning_the_active_composition() {
    let contract = competitive_composition_params().contract;
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(FakeArenaCompositionPlanningAdapter { contract }),
    );

    let first = processor
        .arena_request(semantic_arena_request_params(), ConnectionId(7))
        .await
        .expect("initial semantic request should provision");
    let ClientResponsePayload::MemythosArenaRequest(first) = first else {
        panic!("expected initial semantic arena request response");
    };
    {
        let mut state = processor.state.lock().await;
        state
            .arenas
            .get_mut("arena-composition")
            .expect("provisioned arena")
            .lifecycle_state = MemythosArenaLifecycleState::ClosedCleanly;
        state
            .arena_compositions
            .get_mut("arena-composition")
            .expect("provisioned composition")
            .lifecycle_state = MemythosArenaCompositionLifecycleState::Closed;
        for parent in state.arena_parents.values_mut() {
            parent.lifecycle_state = MemythosArenaLifecycleState::ClosedCleanly;
        }
        for attachment in state.thread_attachments.values_mut() {
            attachment.lifecycle_state = MemythosArenaLifecycleState::ClosedCleanly;
        }
    }
    let mut update = semantic_arena_request_params();
    update.composition_change_signal = Some(
        "upstream contract changed; verify whether the current perspectives remain sufficient"
            .to_string(),
    );
    let response = processor
        .arena_request(update, ConnectionId(7))
        .await
        .expect("semantic change signal should resume inside app-server");
    let ClientResponsePayload::MemythosArenaRequest(response) = response else {
        panic!("expected semantic arena request response");
    };

    assert_eq!(response.composition.composition_version, 1);
    assert_eq!(
        response.resume_assessment.disposition,
        MemythosArenaResumeDisposition::PartialResume
    );
    assert_eq!(response.resume_assessment.affected_participant_ids.len(), 1);
    let first_delivery = first
        .initial_delivery
        .as_ref()
        .expect("initial request delivery");
    let resumed_delivery = response
        .initial_delivery
        .as_ref()
        .expect("material resume delivery");
    assert_eq!(first_delivery.round_id, "arena-composition-round-1");
    assert!(
        resumed_delivery
            .round_id
            .starts_with("arena-composition-resume-mem_arena_request"),
        "partial resume needs a distinct round id without pretending the composition changed"
    );
    assert_ne!(first_delivery.round_id, resumed_delivery.round_id);
    assert!(response.composition.applied_revision.is_none());
    assert!(
        response
            .composition
            .leases
            .iter()
            .all(|lease| lease.lease_source == "app_server_native_reused")
    );
    assert_eq!(
        first
            .composition
            .leases
            .iter()
            .map(|lease| (&lease.participant_id, &lease.thread_id))
            .collect::<Vec<_>>(),
        response
            .composition
            .leases
            .iter()
            .map(|lease| (&lease.participant_id, &lease.thread_id))
            .collect::<Vec<_>>(),
        "new evidence is a task delta and must not replace parent identity"
    );
    {
        let state = processor.state.lock().await;
        assert_eq!(
            state
                .arenas
                .get("arena-composition")
                .map(|arena| arena.lifecycle_state),
            Some(MemythosArenaLifecycleState::Running)
        );
        assert_eq!(
            state
                .arena_compositions
                .get("arena-composition")
                .map(|composition| composition.lifecycle_state),
            Some(MemythosArenaCompositionLifecycleState::ActiveProposals)
        );
        assert!(
            state
                .arena_parents
                .values()
                .all(|parent| { parent.lifecycle_state == MemythosArenaLifecycleState::Running })
        );
        assert!(state.thread_attachments.values().all(|attachment| {
            attachment.lifecycle_state == MemythosArenaLifecycleState::Running
        }));
    }

    let resumed_turn_id = resumed_delivery
        .turn_id
        .as_deref()
        .expect("partial resume concierge turn");
    assert!(
        processor
            .record_native_turn_completed(
                &resumed_delivery.thread_id,
                resumed_turn_id,
                "completed",
                Some(500),
                Some(100),
                None,
                Some("The bounded resume is framed for the affected position.".to_string()),
            )
            .await
    );
    let state = processor.state.lock().await;
    let reassessments = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| {
            delivery.round_id == resumed_delivery.round_id
                && delivery.phase.as_deref() == Some("resume_reassessment")
        })
        .collect::<Vec<_>>();
    assert_eq!(reassessments.len(), 1);
    assert_eq!(
        reassessments[0].sender_thread_id,
        resumed_delivery.thread_id
    );
    assert_ne!(
        reassessments[0].receiver_thread_id,
        resumed_delivery.thread_id
    );
    assert!(
        reassessments[0]
            .human_summary
            .contains("planner has already accepted these cited change refs as material novelty")
    );
    assert!(
        reassessments[0]
            .human_summary
            .contains("evidence://fixture-change")
    );
    assert!(
        reassessments[0]
            .human_summary
            .contains("do not claim it is absent or unverified")
    );
}

#[tokio::test]
async fn arena_request_retains_decision_without_provisioning_or_parent_wake() {
    let contract = competitive_composition_params().contract;
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter::default());
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(FakeArenaCompositionPlanningAdapter { contract }),
    );

    processor
        .arena_request(semantic_arena_request_params(), ConnectionId(7))
        .await
        .expect("initial semantic request should provision");
    let goal_transition_count = provisioning.goal_transitions.lock().await.len();
    let response = processor
        .arena_request(semantic_arena_request_params(), ConnectionId(7))
        .await
        .expect("identical request should retain the prior decision");
    let ClientResponsePayload::MemythosArenaRequest(response) = response else {
        panic!("expected retained semantic arena request response");
    };

    assert_eq!(response.composition.composition_version, 1);
    assert_eq!(
        response.resume_assessment.disposition,
        MemythosArenaResumeDisposition::RetainDecision
    );
    assert!(response.resume_assessment.avoided_full_round);
    assert!(response.initial_delivery.is_none());
    assert!(
        response
            .composition
            .leases
            .iter()
            .all(|lease| lease.lease_source == "app_server_native_reused")
    );
    assert_eq!(
        provisioning.goal_transitions.lock().await.len(),
        goal_transition_count,
        "retaining the decision must not provision or wake any parent"
    );
}

#[tokio::test]
async fn arena_request_requires_explicit_comparability_invalidation_for_full_round() {
    let contract = competitive_composition_params().contract;
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(FakeArenaCompositionPlanningAdapter { contract }),
    );
    processor
        .arena_request(semantic_arena_request_params(), ConnectionId(7))
        .await
        .expect("initial semantic request should provision");
    let mut update = semantic_arena_request_params();
    update.resume_context = Some(codex_app_server_protocol::MemythosArenaResumeContext {
        previous_decision_refs: vec!["decision://fixture".to_string()],
        previous_evidence_refs: vec!["evidence://baseline".to_string()],
        candidate_change_refs: vec!["evidence://global-invalidation".to_string()],
        protected_decisions: vec!["The observed slowdown remains a fact".to_string()],
        revisable_settlement: vec!["Causal hypothesis weights".to_string()],
        open_implementation_scope: vec!["Instrumentation repair".to_string()],
    });
    let response = processor
        .arena_request(update, ConnectionId(7))
        .await
        .expect("global invalidation should open a full round");
    let ClientResponsePayload::MemythosArenaRequest(response) = response else {
        panic!("expected full-round semantic arena request response");
    };

    assert_eq!(
        response.resume_assessment.disposition,
        MemythosArenaResumeDisposition::FullRound
    );
    assert!(response.resume_assessment.comparability_invalidated);
    assert!(!response.resume_assessment.affected_decision_refs.is_empty());
    assert!(response.initial_delivery.is_some());
}

#[tokio::test]
async fn arena_request_can_expand_after_a_budget_or_evidence_gap_without_restarting_parents() {
    let initial_contract = competitive_composition_params().contract;
    let mut expanded_contract = initial_contract.clone();
    let mut reality = expanded_contract
        .participants
        .iter()
        .find(|participant| participant.participant_id == "bettor-risk")
        .expect("risk participant fixture")
        .clone();
    reality.participant_id = "reality-observer".to_string();
    reality.agent_role = "observer".to_string();
    reality.stance = "reality_fit".to_string();
    reality.role_objective = "Test the decision against current reality".to_string();
    reality.expected_contribution = "Independent reality evidence".to_string();
    reality.effort_intent = "Focused validation of the material evidence gap".to_string();
    reality.token_budget = None;
    expanded_contract.participants.push(reality);
    for participant in &mut expanded_contract.participants {
        participant.token_budget = None;
    }
    expanded_contract.cost_envelope =
            codex_app_server_protocol::MemythosArenaCostEnvelope {
                mode: codex_app_server_protocol::MemythosArenaCostEnvelopeMode::Open,
                rationale: "A material evidence gap changes the method and has no accepted comparable cost baseline".to_string(),
                baseline_refs: Vec::new(),
                total_token_budget: None,
                coordination_token_budget: None,
                substantive_token_budget: None,
                method_integrity_funded: true,
                exhaustion_policy: codex_app_server_protocol::MemythosArenaCostExhaustionPolicy::WrapUpThenReplan,
            };
    expanded_contract.effort_rationale =
        "Keep the valid parents and add one open-budget reality check".to_string();

    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(ExpandingArenaCompositionPlanningAdapter {
            initial_contract,
            expanded_contract,
        }),
    );

    let first = processor
        .arena_request(semantic_arena_request_params(), ConnectionId(7))
        .await
        .expect("initial semantic request should provision");
    let ClientResponsePayload::MemythosArenaRequest(first) = first else {
        panic!("expected initial semantic arena request response");
    };
    let original_threads = first
        .composition
        .leases
        .iter()
        .map(|lease| (lease.participant_id.clone(), lease.thread_id.clone()))
        .collect::<HashMap<_, _>>();

    let mut update = semantic_arena_request_params();
    update.composition_change_signal = Some(
            "The current parents reached their bounded goal but exposed a material reality-evidence gap; preserve their conclusions and add only the missing perspective"
                .to_string(),
        );
    let second = processor
        .arena_request(update, ConnectionId(7))
        .await
        .expect("semantic evidence gap should expand the live arena");
    let ClientResponsePayload::MemythosArenaRequest(second) = second else {
        panic!("expected expanded semantic arena request response");
    };

    assert_eq!(second.composition.composition_version, 2);
    assert_eq!(second.composition.leases.len(), 5);
    assert_eq!(second.composition.planned_token_budget, None);
    assert!(second.composition.leases.iter().any(|lease| {
        lease.participant_id == "reality-observer"
            && lease.lease_source == "created"
            && lease.token_budget.is_none()
    }));
    assert!(
        second
            .composition
            .leases
            .iter()
            .filter(|lease| {
                original_threads
                    .get(&lease.participant_id)
                    .is_some_and(|thread_id| thread_id == &lease.thread_id)
            })
            .count()
            == 4
    );
}

#[tokio::test]
async fn arena_composition_provisions_plural_bettors_atomically() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };

    assert_eq!(response.leases.len(), 4);
    assert_eq!(
        response
            .leases
            .iter()
            .filter(|lease| lease.role == "bettor")
            .count(),
        2
    );
    assert_eq!(response.room.participants.len(), 4);
    assert_eq!(response.planned_token_budget, Some(80_000));
    assert!(response.leases.iter().all(|lease| {
        lease.token_budget == Some(20_000)
            && lease.goal_status == ThreadGoalStatus::Paused
            && !lease.effort_intent.is_empty()
    }));
    assert_eq!(response.composition_version, 1);
    assert_eq!(
        response.lifecycle_state,
        MemythosArenaCompositionLifecycleState::ActiveProposals
    );
    assert!(response.applied_revision.is_none());
    let state = processor.state.lock().await;
    assert_eq!(state.arena_parents.len(), 4);
    assert!(state.arenas.contains_key("arena-composition"));
    assert!(state.arena_compositions.contains_key("arena-composition"));
}

#[tokio::test]
async fn arena_terminalizes_after_native_judge_returns_a_structured_verdict() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let growth_thread_id = thread_for("bettor-growth");
    let risk_thread_id = thread_for("bettor-risk");
    let judge_thread_id = thread_for("judge");

    let mut state = processor.state.lock().await;
    for (index, sender, receiver, phase, human_instruction) in [
        (
            0,
            "human",
            concierge_thread_id.as_str(),
            "arena_intake",
            true,
        ),
        (
            1,
            concierge_thread_id.as_str(),
            growth_thread_id.as_str(),
            "proposal",
            false,
        ),
        (
            2,
            concierge_thread_id.as_str(),
            risk_thread_id.as_str(),
            "proposal",
            false,
        ),
        (
            3,
            concierge_thread_id.as_str(),
            growth_thread_id.as_str(),
            "peer_review_and_objection",
            false,
        ),
        (
            4,
            concierge_thread_id.as_str(),
            risk_thread_id.as_str(),
            "peer_review_and_objection",
            false,
        ),
        (
            5,
            concierge_thread_id.as_str(),
            growth_thread_id.as_str(),
            "bet",
            false,
        ),
        (
            6,
            concierge_thread_id.as_str(),
            risk_thread_id.as_str(),
            "bet",
            false,
        ),
        (
            7,
            concierge_thread_id.as_str(),
            judge_thread_id.as_str(),
            "judge",
            false,
        ),
        (
            8,
            judge_thread_id.as_str(),
            concierge_thread_id.as_str(),
            "judge",
            false,
        ),
    ] {
        state
            .arena_message_deliveries
            .push(MemythosArenaMessageDelivery {
                delivery_id: format!("delivery-{index}"),
                message_id: format!("message-{index}"),
                human_summary: format!("Completed {phase} delivery"),
                status: "receiver_turn_completed".to_string(),
                sender_thread_id: sender.to_string(),
                receiver_thread_id: receiver.to_string(),
                arena_id: response.room.arena_id.clone(),
                round_id: "round-1".to_string(),
                phase: Some(phase.to_string()),
                delivery_mechanism: "native_test".to_string(),
                delivery_policy: None,
                aggregate_id: None,
                aggregate_state: None,
                checkpoint_state: None,
                checkpoint_event_refs: Vec::new(),
                receiver_turn_id: Some(format!("turn-{index}")),
                receiver_response_event_ref: Some(format!("item-{index}")),
                delivered_as_human_instruction: human_instruction,
                memory_replay_required: false,
                event_refs: Vec::new(),
                rejection_reason: None,
                failure_reason: None,
            });
    }
    state.native_parent_turn_responses.insert(
            native_token_usage_key(&judge_thread_id, "turn-7"),
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: None,
                request_text: None,
                item_ref: Some("app-server://judge/verdict".to_string()),
                text: Some(serde_json::json!({
                    "winner_participant_id": "bettor-growth",
                    "ranked_alternatives": ["bettor-risk"],
                    "winning_decision": "Adopt bounded reversible growth.",
                    "accepted_tradeoff": "Trade speed for lower reversal cost.",
                    "next_action": "close",
                    "contribution_attribution": [
                        {"participant_id": "bettor-growth", "claim_refs": ["claim://growth/wedge"], "disposition": "adopted", "rationale": "The wedge resolves the bounded objective."},
                        {"participant_id": "bettor-risk", "claim_refs": ["claim://risk/reversibility"], "disposition": "conditioned", "rationale": "Reversibility constrains acceleration."}
                    ],
                    "dissent": "Retain the bounded risk posture.",
                    "preserved_dissent": ["Retain the bounded risk posture."],
                    "targeted_refinements": [],
                    "reopening_signals": ["Unit economics materially deteriorate."],
                    "protected_decisions_status": "preserved",
                    "reopened_decision_refs": [],
                    "resume_scope_status": "not_applicable",
                    "rationale": "The growth posture wins within the declared reversible boundary."
                }).to_string()),
            },
        );

    let candidate = arena_closure_candidate(&state, &response.room.arena_id, &judge_thread_id)
        .expect("the final judge completion should close a fully completed round");
    assert_eq!(candidate.outcome, ArenaTerminalOutcome::Close);
    assert_eq!(candidate.arena_id, response.room.arena_id);
    assert!(candidate.parent_thread_ids.contains(&concierge_thread_id));
    assert!(candidate.parent_thread_ids.contains(&judge_thread_id));

    let response_key = native_token_usage_key(&judge_thread_id, "turn-7");
    let judge_response = state
        .native_parent_turn_responses
        .get_mut(&response_key)
        .and_then(|response| response.text.as_mut())
        .expect("judge response should exist");
    let mut verdict: serde_json::Value =
        serde_json::from_str(judge_response).expect("judge verdict should parse");
    verdict["next_action"] = serde_json::json!("parent_rollup");
    verdict["rationale"] = serde_json::json!(
        "The bounded arena completed, but only the parent layer can assign the missing authority."
    );
    *judge_response = verdict.to_string();

    let rollup_candidate =
        arena_closure_candidate(&state, &response.room.arena_id, &judge_thread_id)
            .expect("parent rollup should terminalize the local arena round");
    assert_eq!(rollup_candidate.outcome, ArenaTerminalOutcome::ParentRollup);
    drop(state);

    processor
        .terminalize_arena_parent_goals(rollup_candidate)
        .await;
    let state = processor.state.lock().await;
    assert_eq!(
        state
            .arenas
            .get(&response.room.arena_id)
            .expect("arena should remain registered")
            .lifecycle_state,
        MemythosArenaLifecycleState::AwaitingParent
    );
    assert_eq!(
        state
            .arena_compositions
            .get(&response.room.arena_id)
            .expect("composition should remain registered")
            .lifecycle_state,
        MemythosArenaCompositionLifecycleState::BlockedAuthority
    );
}

#[tokio::test]
async fn partial_resume_closes_after_affected_reassessment_and_canonical_judge_verdict() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let growth_thread_id = thread_for("bettor-growth");
    let judge_thread_id = thread_for("judge");
    let round_id = "arena-composition-resume-1";

    let mut state = processor.state.lock().await;
    state.arena_resume_execution_plans.insert(
        arena_round_key(&response.room.arena_id, round_id),
        partial_resume_execution_plan(vec![
            "concierge".to_string(),
            "bettor-growth".to_string(),
            "judge".to_string(),
        ]),
    );
    for (index, sender, receiver, phase, human_instruction) in [
        (
            0,
            "human",
            concierge_thread_id.as_str(),
            "arena_intake",
            true,
        ),
        (
            1,
            concierge_thread_id.as_str(),
            growth_thread_id.as_str(),
            "resume_reassessment",
            false,
        ),
        (
            2,
            growth_thread_id.as_str(),
            judge_thread_id.as_str(),
            "judge",
            false,
        ),
    ] {
        state
            .arena_message_deliveries
            .push(MemythosArenaMessageDelivery {
                delivery_id: format!("resume-delivery-{index}"),
                message_id: format!("resume-message-{index}"),
                human_summary: format!("Completed {phase} delivery"),
                status: "receiver_turn_completed".to_string(),
                sender_thread_id: sender.to_string(),
                receiver_thread_id: receiver.to_string(),
                arena_id: response.room.arena_id.clone(),
                round_id: round_id.to_string(),
                phase: Some(phase.to_string()),
                delivery_mechanism: "native_test".to_string(),
                delivery_policy: None,
                aggregate_id: None,
                aggregate_state: None,
                checkpoint_state: None,
                checkpoint_event_refs: Vec::new(),
                receiver_turn_id: Some(format!("resume-turn-{index}")),
                receiver_response_event_ref: Some(format!("resume-item-{index}")),
                delivered_as_human_instruction: human_instruction,
                memory_replay_required: false,
                event_refs: Vec::new(),
                rejection_reason: None,
                failure_reason: None,
            });
    }
    state.native_parent_turn_responses.insert(
            native_token_usage_key(&judge_thread_id, "resume-turn-2"),
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: None,
                request_text: None,
                item_ref: Some("app-server://judge/resume-verdict".to_string()),
                text: Some(
                    serde_json::json!({
                        "winner_participant_id": "bettor-growth",
                        "ranked_alternatives": ["bettor-risk"],
                        "winning_decision": "Retain the bounded winner after reassessment.",
                        "accepted_tradeoff": "Reopen only affected hypothesis weight.",
                        "next_action": "close",
                        "contribution_attribution": [
                            {"participant_id": "bettor-growth", "claim_refs": ["claim://growth/wedge"], "disposition": "adopted", "rationale": "The affected evidence preserves this contribution."},
                            {"participant_id": "bettor-risk", "claim_refs": ["claim://risk/reversibility"], "disposition": "preserved_dissent", "rationale": "The risk boundary remains a reopening condition."}
                        ],
                        "dissent": "Retain the bounded risk posture.",
                        "preserved_dissent": ["Retain the bounded risk posture."],
                        "targeted_refinements": [],
                        "reopening_signals": ["A verified instrumentation break."],
                        "protected_decisions_status": "preserved",
                        "reopened_decision_refs": ["decision://forecast/winner"],
                        "resume_scope_status": "partially_reopened",
                        "rationale": "The affected position was reassessed without reopening unrelated decisions."
                    })
                    .to_string(),
                ),
            },
        );

    let candidate = arena_closure_candidate(&state, &response.room.arena_id, &judge_thread_id)
        .expect("canonical judge completion should close a bounded partial resume");
    assert_eq!(candidate.arena_id, response.room.arena_id);
}

#[test]
fn native_judge_verdict_rejects_the_legacy_text_contract() {
    let eligible = HashSet::from(["bettor-growth", "bettor-risk"]);
    assert!(!is_valid_native_judge_verdict(
        "winner_participant_id: bettor-growth\nprotected_decisions_status: preserved",
        &eligible,
    ));
}

#[test]
fn native_judge_verdict_rejects_an_ineligible_winner() {
    let eligible = HashSet::from(["bettor-growth", "bettor-risk"]);
    assert!(!is_valid_native_judge_verdict(
            &serde_json::json!({
                "winner_participant_id": "judge",
                "ranked_alternatives": ["bettor-growth"],
                "winning_decision": "Invalid winner.",
                "accepted_tradeoff": "None.",
                "next_action": "close",
                "contribution_attribution": [
                    {"participant_id": "bettor-growth", "claim_refs": [], "disposition": "adopted", "rationale": "Valid participant."},
                    {"participant_id": "bettor-risk", "claim_refs": [], "disposition": "rejected", "rationale": "Valid participant."}
                ],
                "dissent": "",
                "preserved_dissent": [],
                "targeted_refinements": [],
                "reopening_signals": [],
                "protected_decisions_status": "preserved",
                "reopened_decision_refs": [],
                "resume_scope_status": "not_applicable",
                "rationale": ""
            })
            .to_string(),
            &eligible,
        ));
}

#[test]
fn native_judge_verdict_requires_each_rejected_alternative_exactly_once() {
    let eligible = HashSet::from(["bettor-growth", "bettor-risk", "bettor-control"]);
    let verdict = |ranked_alternatives: serde_json::Value| {
        serde_json::json!({
                "winner_participant_id": "bettor-growth",
                "ranked_alternatives": ranked_alternatives,
                "winning_decision": "Adopt the bounded growth wedge.",
                "accepted_tradeoff": "Retain reversibility.",
                "next_action": "close",
                "contribution_attribution": [
                    {"participant_id": "bettor-growth", "claim_refs": [], "disposition": "adopted", "rationale": "Winner."},
                    {"participant_id": "bettor-risk", "claim_refs": [], "disposition": "preserved_dissent", "rationale": "Risk remains bounded dissent."},
                    {"participant_id": "bettor-control", "claim_refs": [], "disposition": "conditioned", "rationale": "Control contributes a condition."}
                ],
                "dissent": "Risk remains a reopening condition.",
                "preserved_dissent": ["Risk remains a reopening condition."],
                "targeted_refinements": [],
                "reopening_signals": ["Unit economics deteriorate."],
                "protected_decisions_status": "preserved",
                "reopened_decision_refs": [],
                "resume_scope_status": "not_applicable",
                "rationale": "The evidence supports the bounded winner."
            })
            .to_string()
    };

    assert!(is_valid_native_judge_verdict(
        &verdict(serde_json::json!(["bettor-risk", "bettor-control"])),
        &eligible,
    ));
    assert!(!is_valid_native_judge_verdict(
        &verdict(serde_json::json!(["bettor-growth", "bettor-risk"])),
        &eligible,
    ));
    assert!(!is_valid_native_judge_verdict(
        &verdict(serde_json::json!(["bettor-risk", "bettor-risk"])),
        &eligible,
    ));
    assert!(!is_valid_native_judge_verdict(
        &verdict(serde_json::json!(["bettor-risk"])),
        &eligible,
    ));
}

#[test]
fn native_judge_verdict_rejects_missing_or_duplicate_parent_attribution() {
    let eligible = HashSet::from(["bettor-growth", "bettor-risk"]);
    let verdict = |attribution: serde_json::Value| {
        serde_json::json!({
            "winner_participant_id": "bettor-growth",
            "ranked_alternatives": ["bettor-risk"],
            "winning_decision": "Adopt the bounded growth wedge.",
            "accepted_tradeoff": "Retain reversibility.",
            "next_action": "close",
            "contribution_attribution": attribution,
            "dissent": "Risk remains a reopening condition.",
            "preserved_dissent": ["Risk remains a reopening condition."],
            "targeted_refinements": [],
            "reopening_signals": ["Unit economics deteriorate."],
            "protected_decisions_status": "preserved",
            "reopened_decision_refs": [],
            "resume_scope_status": "not_applicable",
            "rationale": "The evidence supports the bounded winner."
        })
        .to_string()
    };
    assert!(!is_valid_native_judge_verdict(
        &verdict(serde_json::json!([
            {"participant_id": "bettor-growth", "claim_refs": [], "disposition": "adopted", "rationale": "Winner."}
        ])),
        &eligible,
    ));
    assert!(!is_valid_native_judge_verdict(
        &verdict(serde_json::json!([
            {"participant_id": "bettor-growth", "claim_refs": [], "disposition": "adopted", "rationale": "Winner."},
            {"participant_id": "bettor-growth", "claim_refs": [], "disposition": "conditioned", "rationale": "Duplicate."}
        ])),
        &eligible,
    ));
}

#[test]
fn native_judge_verdict_keeps_partial_resume_separate_from_closed_decisions() {
    let eligible = HashSet::from(["bettor-seasonal", "bettor-measurement"]);
    assert!(is_valid_native_judge_verdict(
            &serde_json::json!({
                "winner_participant_id": "bettor-measurement",
                "ranked_alternatives": ["bettor-seasonal"],
                "winning_decision": "Treat measurement contamination as the leading bounded explanation.",
                "accepted_tradeoff": "Delay a structural commitment until observability is reconciled.",
                "next_action": "close",
                "contribution_attribution": [
                    {"participant_id": "bettor-measurement", "claim_refs": ["claim://measurement/break"], "disposition": "adopted", "rationale": "The instrumentation break best explains uncertainty."},
                    {"participant_id": "bettor-seasonal", "claim_refs": ["claim://seasonal/timing"], "disposition": "preserved_dissent", "rationale": "Timing remains a bounded alternative."}
                ],
                "dissent": "Seasonality remains a bounded alternative.",
                "preserved_dissent": ["Seasonality remains a bounded alternative."],
                "targeted_refinements": [],
                "reopening_signals": ["A verified instrumentation break."],
                "protected_decisions_status": "preserved",
                "reopened_decision_refs": ["decision://forecast/winner-and-weights"],
                "resume_scope_status": "partially_reopened",
                "rationale": "Only affected hypothesis weights are reopened."
            })
            .to_string(),
            &eligible,
        ));
}

#[tokio::test]
async fn arena_does_not_close_without_a_structured_native_judge_verdict() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let response = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(response) = response else {
        panic!("expected arena composition provision response");
    };
    let thread_for = |participant_id: &str| {
        response
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
            .thread_id
            .clone()
    };
    let concierge_thread_id = thread_for("concierge");
    let growth_thread_id = thread_for("bettor-growth");
    let risk_thread_id = thread_for("bettor-risk");
    let judge_thread_id = thread_for("judge");

    let mut state = processor.state.lock().await;
    for (index, receiver, phase) in [
        (0, concierge_thread_id.as_str(), "arena_intake"),
        (1, growth_thread_id.as_str(), "proposal"),
        (2, risk_thread_id.as_str(), "proposal"),
        (3, growth_thread_id.as_str(), "peer_review_and_objection"),
        (4, risk_thread_id.as_str(), "peer_review_and_objection"),
        (5, growth_thread_id.as_str(), "bet"),
        (6, risk_thread_id.as_str(), "bet"),
        (7, judge_thread_id.as_str(), "judge"),
    ] {
        state
            .arena_message_deliveries
            .push(MemythosArenaMessageDelivery {
                delivery_id: format!("delivery-{index}"),
                message_id: format!("message-{index}"),
                human_summary: format!("Completed {phase} delivery"),
                status: "receiver_turn_completed".to_string(),
                sender_thread_id: if phase == "arena_intake" {
                    "human".to_string()
                } else {
                    concierge_thread_id.clone()
                },
                receiver_thread_id: receiver.to_string(),
                arena_id: response.room.arena_id.clone(),
                round_id: "round-1".to_string(),
                phase: Some(phase.to_string()),
                delivery_mechanism: "native_test".to_string(),
                delivery_policy: None,
                aggregate_id: None,
                aggregate_state: None,
                checkpoint_state: None,
                checkpoint_event_refs: Vec::new(),
                receiver_turn_id: Some(format!("turn-{index}")),
                receiver_response_event_ref: Some(format!("item-{index}")),
                delivered_as_human_instruction: phase == "arena_intake",
                memory_replay_required: false,
                event_refs: Vec::new(),
                rejection_reason: None,
                failure_reason: None,
            });
    }

    assert!(
        arena_closure_candidate(&state, &response.room.arena_id, &judge_thread_id).is_none(),
        "a judge turn without its required structured verdict is not a clean arena close"
    );
}

#[tokio::test]
async fn arena_composition_requires_explicit_revision_and_reuses_only_kept_identity() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let first = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("initial composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(first) = first else {
        panic!("expected initial composition response");
    };
    let initial_judge = first
        .leases
        .iter()
        .find(|lease| lease.participant_id == "judge")
        .expect("initial judge lease");
    let initial_task_contract = {
        let state = processor.state.lock().await;
        native_arena_parent_task_contract(&state, &first.room.arena_id, &initial_judge.thread_id)
            .expect("initial task contract")
    };
    assert!(initial_task_contract.contains("Mandatory completion criteria"));

    let implicit_error = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect_err("an active composition cannot be replaced implicitly");
    assert!(
        implicit_error
            .message
            .contains("explicit add/keep/retire revision")
    );

    let mut revised = competitive_composition_params();
    revised.revision = Some(
        codex_app_server_protocol::MemythosArenaCompositionRevision {
            revision_id: "revision-2".to_string(),
            previous_version: 1,
            next_version: 2,
            previous_contract_ref: first.event_refs[0].clone(),
            trigger: "upstream_contract_changed".to_string(),
            rationale: "Retain the valid team while applying the revised goal".to_string(),
            actions: first
                .leases
                .iter()
                .map(
                    |lease| codex_app_server_protocol::MemythosArenaCompositionRevisionAction {
                        action: MemythosArenaCompositionRevisionActionKind::Keep,
                        participant_id: lease.participant_id.clone(),
                        thread_id: Some(lease.thread_id.clone()),
                        reason: "Role and stance remain required".to_string(),
                    },
                )
                .collect(),
        },
    );
    let second = processor
        .arena_composition_provision(revised, ConnectionId(0))
        .await
        .expect("explicit revision should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(second) = second else {
        panic!("expected revised composition response");
    };
    assert_eq!(second.composition_version, 2);
    assert!(second.applied_revision.is_some());
    assert!(
        second
            .leases
            .iter()
            .all(|lease| lease.lease_source == "reused")
    );
    assert_eq!(
        first
            .leases
            .iter()
            .map(|lease| lease.thread_id.as_str())
            .collect::<Vec<_>>(),
        second
            .leases
            .iter()
            .map(|lease| lease.thread_id.as_str())
            .collect::<Vec<_>>()
    );
    let revised_judge = second
        .leases
        .iter()
        .find(|lease| lease.participant_id == "judge")
        .expect("revised judge lease");
    let revised_task_contract = {
        let state = processor.state.lock().await;
        native_arena_parent_task_contract(&state, &second.room.arena_id, &revised_judge.thread_id)
            .expect("revised task contract")
    };
    assert!(revised_task_contract.contains("Native current task delta"));
    assert!(!revised_task_contract.contains("Mandatory completion criteria"));
    assert!(revised_task_contract.contains("Do not restate, re-argue, or summarize"));
    assert!(revised_task_contract.contains("Final validation boundaries for this verdict"));
    assert!(revised_task_contract.contains("Judge selects a supported position"));
    assert!(revised_task_contract.contains("Preserve any exact predicate or invariant verbatim"));

    let revised_bettor = second
        .leases
        .iter()
        .find(|lease| lease.participant_id == "bettor-growth")
        .expect("revised bettor lease");
    let revised_bettor_task_contract = {
        let state = processor.state.lock().await;
        native_arena_parent_task_contract(&state, &second.room.arena_id, &revised_bettor.thread_id)
            .expect("revised bettor task contract")
    };
    assert!(revised_bettor_task_contract.contains("Native current task delta"));
    assert!(!revised_bettor_task_contract.contains("Final validation boundaries"));
    assert!(!revised_bettor_task_contract.contains("Judge selects a supported position"));
}

#[tokio::test]
async fn arena_composition_rolls_back_new_parents_before_state_commit() {
    let provisioning = Arc::new(FakeArenaParentProvisioningAdapter {
        fail_participant: Some("bettor-risk".to_string()),
        ..Default::default()
    });
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        provisioning.clone(),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );

    let error = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect_err("injected failure must reject the composition");
    assert!(error.message.contains("injected provisioning failure"));
    assert_eq!(provisioning.rolled_back.lock().await.len(), 2);
    let state = processor.state.lock().await;
    assert!(state.arenas.is_empty());
    assert!(state.rooms.is_empty());
    assert!(state.arena_parents.is_empty());
}

#[tokio::test]
async fn competitive_composition_rejects_a_single_bettor_position() {
    let mut params = competitive_composition_params();
    params
        .contract
        .participants
        .retain(|participant| participant.participant_id != "bettor-risk");
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let error = processor
        .arena_composition_provision(params, ConnectionId(0))
        .await
        .expect_err("one proposal-bearing parent is not a competitive round");
    assert!(
        error
            .message
            .contains("at least 2 proposal-bearing parents")
    );
}

#[tokio::test]
async fn arena_composition_rejects_a_non_positive_planner_budget() {
    let mut params = competitive_composition_params();
    params.contract.participants[0].token_budget = Some(0);
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let error = processor
        .arena_composition_provision(params, ConnectionId(0))
        .await
        .expect_err("a non-positive planner budget must not reach thread/goal/set");
    assert!(error.message.contains("token budget must be positive"));
}

#[derive(Debug)]
struct FakeParentTurnResponseAdapter;

impl ParentTurnResponseAdapter for FakeParentTurnResponseAdapter {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a> {
        Box::pin(async move {
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: Some(format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/items/user-message"
                )),
                request_text: Some(format!(
                    "Pedido conversacional OOTB para {thread_id} en {turn_id}."
                )),
                item_ref: Some(format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/items/final-agent-message"
                )),
                text: Some(format!(
                    "Cierre conversacional OOTB de {thread_id} para {turn_id}."
                )),
            }
        })
    }
}

#[derive(Debug, Default)]
struct ListenerGatedParentTurnResponseAdapter {
    reads: AtomicUsize,
}

impl ParentTurnResponseAdapter for ListenerGatedParentTurnResponseAdapter {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if read == 0 {
                return ParentTurnResponse {
                    status: None,
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            }
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: Some(format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/items/user-message"
                )),
                request_text: Some(format!(
                    "Pedido conversacional OOTB para {thread_id} en {turn_id}."
                )),
                item_ref: Some(format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/items/final-agent-message"
                )),
                text: Some(format!(
                    "Cierre conversacional OOTB de {thread_id} para {turn_id}."
                )),
            }
        })
    }
}

#[derive(Debug, Default)]
struct DelayedParentTurnResponseAdapter {
    reads: AtomicUsize,
}

impl ParentTurnResponseAdapter for DelayedParentTurnResponseAdapter {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if read < 3 {
                return ParentTurnResponse {
                    status: Some(TurnStatus::InProgress),
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            }
            ParentTurnResponse {
                status: Some(TurnStatus::Completed),
                request_item_ref: Some(format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/items/user-message"
                )),
                request_text: Some("Native request".to_string()),
                item_ref: Some(format!(
                    "app-server://threads/{thread_id}/turns/{turn_id}/items/final-agent-message"
                )),
                text: Some("Native parent completed after remaining in progress.".to_string()),
            }
        })
    }
}

#[derive(Debug)]
struct FakeThreadConsolidationAdapter;

impl ThreadConsolidationAdapter for FakeThreadConsolidationAdapter {
    fn consolidate_threads<'a>(
        &'a self,
        params: &'a MemythosThreadConsolidateParams,
    ) -> ThreadConsolidationFuture<'a> {
        Box::pin(async move {
            let source_refs = params
                .source_thread_ids
                .iter()
                .map(|thread_id| MemythosThreadConsolidationSourceRef {
                    thread_id: thread_id.clone(),
                    turn_refs: vec![format!("app-server://threads/{thread_id}/turns/turn-1")],
                    items_view: "summary".to_string(),
                    cursor: params.since_cursors.get(thread_id).cloned(),
                    next_cursor: Some(format!("cursor-after-{thread_id}")),
                    latest_agent_message_ref: Some(format!(
                        "app-server://threads/{thread_id}/turns/turn-1/items/msg-1"
                    )),
                    latest_agent_message_text: Some(format!(
                        "{thread_id} propone avanzar con una definicion concreta."
                    )),
                    technical_evidence_refs: vec![format!(
                        "app-server://threads/{thread_id}/turns/turn-1/items/collab-1"
                    )],
                })
                .collect::<Vec<_>>();
            ThreadConsolidationAttempt {
                    consolidation_turn_id: Some("turn-consolidation-001".to_string()),
                    source_refs,
                    agent_message_ref: Some(
                        "app-server://threads/thread_concierge/turns/turn-consolidation-001/items/msg-1"
                            .to_string(),
                    ),
                    structured_output_ref: Some(
                        "app-server://threads/thread_concierge/turns/turn-consolidation-001/output-schema"
                            .to_string(),
                    ),
                    technical_evidence_refs: vec![
                        "app-server://threads/thread_a/turns/turn-1/items/collab-1".to_string(),
                    ],
                    source_method: "thread/turns/list".to_string(),
                    used_thread_turns_summary: true,
                    blockers: Vec::new(),
                }
        })
    }
}

fn room_register_params() -> MemythosRoomRegisterParams {
    MemythosRoomRegisterParams {
        room_id: "room-001".to_string(),
        case_id: "case-001".to_string(),
        layer_id: "bpm_e2e".to_string(),
        arena_id: "arena-room-001".to_string(),
        topology: "cross_parent_room".to_string(),
        participants: vec![
            MemythosRoomParticipant {
                parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                thread_id: "thread_growth".to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: "growth".to_string(),
                goal_ref: Some("app-server://threads/thread_growth/goals/current".to_string()),
                authority_scope: vec!["peer_debate".to_string()],
            },
            MemythosRoomParticipant {
                parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                thread_id: "thread_risk".to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: "risk".to_string(),
                goal_ref: Some("app-server://threads/thread_risk/goals/current".to_string()),
                authority_scope: vec!["peer_debate".to_string()],
            },
        ],
    }
}

fn room_register_params_with_concierge() -> MemythosRoomRegisterParams {
    let mut params = room_register_params();
    params.participants.push(MemythosRoomParticipant {
        parent_key: "case/bpm_e2e/arena/room_concierge".to_string(),
        thread_id: "thread_concierge".to_string(),
        parent_role: "room_concierge".to_string(),
        stance_profile: "coordination".to_string(),
        goal_ref: Some("app-server://threads/thread_concierge/goals/current".to_string()),
        authority_scope: vec!["room_coordination".to_string()],
    });
    params
}

fn room_register_params_with_concierge_and_judge() -> MemythosRoomRegisterParams {
    let mut params = room_register_params_with_concierge();
    params.participants.push(MemythosRoomParticipant {
        parent_key: "case/bpm_e2e/arena/judge/fitness".to_string(),
        thread_id: "thread_judge".to_string(),
        parent_role: "judge".to_string(),
        stance_profile: "business_fitness".to_string(),
        goal_ref: Some("app-server://threads/thread_judge/goals/current".to_string()),
        authority_scope: vec!["judge".to_string()],
    });
    params
}

async fn register_room_with_native_arena(
    processor: &MemythosRequestProcessor,
    mut params: MemythosRoomRegisterParams,
) {
    let layer = processor
        .layer_create(MemythosLayerCreateParams {
            name: "Room test layer".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Exercise native room delivery.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer) = layer else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer.layer.layer_id.clone(),
            name: "Room test arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Exercise native room delivery.".to_string(),
            participant_ids: Vec::new(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena) = arena else {
        panic!("expected MemythosArenaCreate response");
    };
    params.layer_id = layer.layer.layer_id;
    params.arena_id = arena.arena.arena_id.clone();
    for participant in &params.participants {
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: params.arena_id.clone(),
                thread_id: participant.thread_id.clone(),
                role_id: Some(participant.parent_role.clone()),
                stance_id: Some(participant.stance_profile.clone()),
                objective: Some("Exercise native room delivery.".to_string()),
                contract_ref: None,
            })
            .await
            .unwrap();
        processor
            .arena_parent_register(MemythosArenaParentRegisterParams {
                arena_id: params.arena_id.clone(),
                thread_id: participant.thread_id.clone(),
                parent_role: participant.parent_role.clone(),
                stance_profile: participant.stance_profile.clone(),
                authority_scope: participant.authority_scope.clone(),
            })
            .await
            .unwrap();
    }
    processor.room_register(params).await.unwrap();
}

fn second_room_register_params_with_concierge() -> MemythosRoomRegisterParams {
    MemythosRoomRegisterParams {
        room_id: "room-002".to_string(),
        case_id: "case-001".to_string(),
        layer_id: "tactical_operational".to_string(),
        arena_id: "arena-room-002".to_string(),
        topology: "cross_parent_room".to_string(),
        participants: vec![
            MemythosRoomParticipant {
                parent_key: "case/tactical/arena/room_concierge".to_string(),
                thread_id: "thread_tactical_concierge".to_string(),
                parent_role: "room_concierge".to_string(),
                stance_profile: "coordination".to_string(),
                goal_ref: Some(
                    "app-server://threads/thread_tactical_concierge/goals/current".to_string(),
                ),
                authority_scope: vec!["room_coordination".to_string()],
            },
            MemythosRoomParticipant {
                parent_key: "case/tactical/arena/bettor/design".to_string(),
                thread_id: "thread_tactical_bettor".to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: "customer_flow".to_string(),
                goal_ref: Some(
                    "app-server://threads/thread_tactical_bettor/goals/current".to_string(),
                ),
                authority_scope: vec!["peer_debate".to_string()],
            },
        ],
    }
}

#[tokio::test]
async fn room_registration_rejects_duplicate_threads() {
    let processor = MemythosRequestProcessor::new();
    let mut params = room_register_params();
    params.participants[1].thread_id = params.participants[0].thread_id.clone();

    let error = processor.room_register(params).await.unwrap_err();

    assert!(
        error.message.contains("duplicate room participant thread"),
        "unexpected error: {}",
        error.message
    );
}

#[tokio::test]
async fn room_registration_rejects_legacy_observer_concierge_encoding() {
    let processor = MemythosRequestProcessor::new();
    let mut params = room_register_params();
    params.participants.push(MemythosRoomParticipant {
        parent_key: "case/bpm_e2e/arena/observer/room_concierge".to_string(),
        thread_id: "thread_concierge".to_string(),
        parent_role: "observer".to_string(),
        stance_profile: "room_concierge".to_string(),
        goal_ref: Some("app-server://threads/thread_concierge/goals/current".to_string()),
        authority_scope: vec!["room_coordination".to_string()],
    });

    let error = processor.room_register(params).await.unwrap_err();

    assert!(
        error
            .message
            .contains("legacy observer + room_concierge encoding"),
        "unexpected error: {}",
        error.message
    );
}

#[tokio::test]
async fn room_list_returns_registered_rooms_from_native_state() {
    let processor = MemythosRequestProcessor::new();
    processor
        .room_register(room_register_params())
        .await
        .unwrap();
    let mut second_room = room_register_params();
    second_room.room_id = "room-002".to_string();
    second_room.case_id = "case-002".to_string();
    processor.room_register(second_room).await.unwrap();

    let response = processor
        .room_list(MemythosRoomListParams {
            case_id: Some("case-001".to_string()),
            layer_id: None,
            arena_id: None,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomList(response) = response else {
        panic!("expected MemythosRoomList response");
    };

    assert_eq!(response.source_method, "memythos/room/list");
    assert_eq!(response.rooms.len(), 1);
    assert_eq!(response.rooms[0].room_id, "room-001");
    assert_eq!(response.rooms[0].participants.len(), 2);
}

#[tokio::test]
async fn room_tool_lists_registered_parent_threads() {
    let processor = MemythosRequestProcessor::new();
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();

    let participants = processor
        .room_tool_list_participants("thread_growth")
        .await
        .unwrap();

    assert_eq!(participants.len(), 3);
    assert_eq!(participants[0].parent_role, "bettor");
    assert!(participants[0].is_current_parent);
    assert_eq!(participants[2].parent_role, "room_concierge");
}

#[tokio::test]
async fn room_tool_delegates_and_resumes_the_same_cross_room_concierge_thread() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();
    processor
        .room_register(second_room_register_params_with_concierge())
        .await
        .unwrap();

    let rooms = processor
        .room_tool_list_rooms("thread_concierge")
        .await
        .unwrap();
    assert_eq!(rooms.len(), 2);
    assert_eq!(rooms.iter().filter(|room| room.is_current_room).count(), 1);

    let delegate = processor
        .room_tool_send_to_room(
            "thread_concierge",
            MemythosRoomToolSendToRoomArgs {
                target_room_id: "room-002".to_string(),
                message: "Produce una bajada tactica y devuelve un rollup.".to_string(),
                authority: "peer".to_string(),
                message_kind: "delegate_to_tactical".to_string(),
                response_contract: "Devuelve un unico rollup.".to_string(),
            },
        )
        .await
        .unwrap();
    let resume = processor
        .room_tool_send_to_room(
            "thread_concierge",
            MemythosRoomToolSendToRoomArgs {
                target_room_id: "room-002".to_string(),
                message: "Reanuda con esta definicion resuelta.".to_string(),
                authority: "peer".to_string(),
                message_kind: "resume_tactical_after_rollup".to_string(),
                response_contract: "Devuelve cierre tactico final.".to_string(),
            },
        )
        .await
        .unwrap();

    assert_eq!(delegate.target_thread_id, "thread_tactical_concierge");
    assert_eq!(resume.target_thread_id, delegate.target_thread_id);
    assert!(delegate.response_text.contains("thread_tactical_concierge"));
    assert!(resume.response_text.contains("thread_tactical_concierge"));
    let state = processor.state.lock().await;
    let cross_room_deliveries = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| delivery.receiver_thread_id == "thread_tactical_concierge")
        .collect::<Vec<_>>();
    assert_eq!(cross_room_deliveries.len(), 2);
    assert_eq!(
        cross_room_deliveries[0].phase.as_deref(),
        Some("delegate_to_tactical")
    );
    assert_eq!(
        cross_room_deliveries[1].phase.as_deref(),
        Some("resume_tactical_after_rollup")
    );
}

#[tokio::test]
async fn room_tool_reconciles_parent_completion_that_precedes_delivery_registration() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();

    let response = processor
        .room_tool_send_message(
            "thread_concierge",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                message: "Evalua esta alternativa desde riesgo.".to_string(),
                authority: "peer".to_string(),
                message_kind: "consultation".to_string(),
                response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                delivery_policy: None,
                aggregate_contract: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(response.target_thread_id, "thread_risk");
    assert!(
        response
            .response_text
            .contains("Cierre conversacional OOTB de thread_risk")
    );
    let state = processor.state.lock().await;
    let delivery = state
        .arena_message_deliveries
        .last()
        .expect("room tool should retain its native delivery");
    assert_eq!(delivery.status, "receiver_turn_completed");
    assert!(
        delivery
            .receiver_response_event_ref
            .as_deref()
            .is_some_and(|value| value.ends_with("/items/final-agent-message"))
    );
}

#[tokio::test]
async fn room_tool_waits_for_the_native_parent_terminal_state() {
    let response_adapter = Arc::new(DelayedParentTurnResponseAdapter::default());
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        response_adapter.clone(),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();

    let response = processor
        .room_tool_send_message(
            "thread_concierge",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                message: "Continue until the native parent closes.".to_string(),
                authority: "peer".to_string(),
                message_kind: "consultation".to_string(),
                response_contract: "Return the native final AgentMessage.".to_string(),
                delivery_policy: None,
                aggregate_contract: None,
            },
        )
        .await
        .unwrap();

    assert!(response_adapter.reads.load(Ordering::SeqCst) >= 4);
    assert_eq!(
        response.response_text,
        "Native parent completed after remaining in progress."
    );
}

#[tokio::test]
async fn room_tool_waits_until_native_listener_observes_terminal_turn() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(ListenerGatedParentTurnResponseAdapter::default()),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();

    let response = processor
        .room_tool_send_message(
            "thread_concierge",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                message: "Evalua esta alternativa desde riesgo.".to_string(),
                authority: "peer".to_string(),
                message_kind: "consultation".to_string(),
                response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                delivery_policy: None,
                aggregate_contract: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(response.target_thread_id, "thread_risk");
    assert!(
        response
            .response_text
            .contains("Cierre conversacional OOTB de thread_risk")
    );
}

#[tokio::test]
async fn room_tool_inherits_round_and_phase_from_parent_inbound_delivery() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();
    {
        let mut state = processor.state.lock().await;
        state
            .arena_message_deliveries
            .push(MemythosArenaMessageDelivery {
                delivery_id: "human-intake-delivery".to_string(),
                message_id: "human-intake-message".to_string(),
                human_summary: "Evalua el pedido humano de la arena.".to_string(),
                status: "receiver_turn_completed".to_string(),
                sender_thread_id: "human".to_string(),
                receiver_thread_id: "thread_concierge".to_string(),
                arena_id: "arena-room-001".to_string(),
                round_id: "intake-001".to_string(),
                phase: Some("human_intake".to_string()),
                delivery_mechanism: "room_loopback_send_input".to_string(),
                delivery_policy: None,
                aggregate_id: None,
                aggregate_state: None,
                checkpoint_state: None,
                checkpoint_event_refs: Vec::new(),
                receiver_turn_id: Some("human-intake-turn".to_string()),
                receiver_response_event_ref: None,
                delivered_as_human_instruction: true,
                memory_replay_required: false,
                event_refs: Vec::new(),
                rejection_reason: None,
                failure_reason: None,
            });
    }

    processor
        .room_tool_send_message(
            "thread_concierge",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                message: "Evalua esta alternativa desde riesgo.".to_string(),
                authority: "peer".to_string(),
                message_kind: "consultation".to_string(),
                response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                delivery_policy: None,
                aggregate_contract: None,
            },
        )
        .await
        .unwrap();

    let state = processor.state.lock().await;
    let delivery = state
        .arena_message_deliveries
        .last()
        .expect("room tool should retain its native delivery");
    assert_eq!(delivery.round_id, "intake-001");
    assert_eq!(delivery.phase.as_deref(), Some("human_intake"));
}

#[tokio::test]
async fn room_tool_routes_peer_to_concierge_and_returns_native_closure() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();

    let running = {
        let processor = processor.clone();
        tokio::spawn(async move {
            processor
                .room_tool_send_message(
                    "thread_growth",
                    MemythosRoomToolSendMessageArgs {
                        target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                        message: "Necesito que el room coordine esta objecion.".to_string(),
                        authority: "peer".to_string(),
                        message_kind: "objection".to_string(),
                        response_contract: "Devuelve la decision coordinada.".to_string(),
                        delivery_policy: None,
                        aggregate_contract: None,
                    },
                )
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(20)).await;
    let target_turn_id = {
        let state = processor.state.lock().await;
        let delivery = state
            .arena_message_deliveries
            .last()
            .expect("room tool should create a delivery");
        assert_eq!(delivery.sender_thread_id, "thread_growth");
        assert_eq!(delivery.receiver_thread_id, "thread_concierge");
        delivery
            .receiver_turn_id
            .clone()
            .expect("delivery should have a native turn")
    };
    assert!(
        processor
            .record_native_turn_completed(
                "thread_concierge",
                &target_turn_id,
                "completed",
                Some(1_000),
                Some(250),
                None,
                None,
            )
            .await
    );

    let response = running.await.unwrap().unwrap();
    assert_eq!(
        response.target_parent_key,
        "case/bpm_e2e/arena/room_concierge"
    );
    assert_eq!(response.target_thread_id, "thread_concierge");
    assert_eq!(response.target_turn_id, target_turn_id);
    assert!(
        response
            .response_text
            .contains("Cierre conversacional OOTB de thread_concierge")
    );
}

#[tokio::test]
async fn room_tool_routes_concierge_to_selected_parent() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params_with_concierge())
        .await
        .unwrap();

    let running = {
        let processor = processor.clone();
        tokio::spawn(async move {
            processor
                .room_tool_send_message(
                    "thread_concierge",
                    MemythosRoomToolSendMessageArgs {
                        target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                        message: "Evalua esta alternativa desde riesgo.".to_string(),
                        authority: "peer".to_string(),
                        message_kind: "consultation".to_string(),
                        response_contract: "Devuelve tesis, limites y proximo paso.".to_string(),
                        delivery_policy: None,
                        aggregate_contract: None,
                    },
                )
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(20)).await;
    let target_turn_id = {
        let state = processor.state.lock().await;
        let delivery = state
            .arena_message_deliveries
            .last()
            .expect("room tool should create a delivery");
        assert_eq!(delivery.sender_thread_id, "thread_concierge");
        assert_eq!(delivery.receiver_thread_id, "thread_risk");
        delivery
            .receiver_turn_id
            .clone()
            .expect("delivery should have a native turn")
    };
    assert!(
        processor
            .record_native_turn_completed(
                "thread_risk",
                &target_turn_id,
                "completed",
                Some(1_000),
                Some(250),
                None,
                None,
            )
            .await
    );

    let response = running.await.unwrap().unwrap();
    assert_eq!(response.target_parent_key, "case/bpm_e2e/arena/bettor/risk");
    assert_eq!(response.target_thread_id, "thread_risk");
    assert!(
        response
            .response_text
            .contains("Cierre conversacional OOTB de thread_risk")
    );
}

#[tokio::test]
async fn completed_bettor_proposals_fan_out_and_trigger_each_bettor_once() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    register_room_with_native_arena(&processor, room_register_params_with_concierge_and_judge())
        .await;
    let arena_id = processor
        .state
        .lock()
        .await
        .rooms
        .get("room-001")
        .expect("registered room")
        .arena_id
        .clone();

    let assignment = |id: &str, receiver: &str| MemythosArenaMessageDelivery {
        delivery_id: format!("delivery-{id}"),
        message_id: format!("message-{id}"),
        human_summary: "Produce one independent proposal.".to_string(),
        status: "receiver_turn_running".to_string(),
        sender_thread_id: "thread_concierge".to_string(),
        receiver_thread_id: receiver.to_string(),
        arena_id: arena_id.clone(),
        round_id: "round-1".to_string(),
        phase: Some("proposal".to_string()),
        delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
        delivery_policy: None,
        aggregate_id: None,
        aggregate_state: None,
        checkpoint_state: None,
        checkpoint_event_refs: Vec::new(),
        receiver_turn_id: Some(format!("turn-{id}")),
        receiver_response_event_ref: None,
        delivered_as_human_instruction: false,
        memory_replay_required: false,
        event_refs: Vec::new(),
        rejection_reason: None,
        failure_reason: None,
    };
    {
        let mut state = processor.state.lock().await;
        state
            .arena_message_deliveries
            .push(assignment("growth", "thread_growth"));
        state
            .arena_message_deliveries
            .push(assignment("risk", "thread_risk"));
    }

    assert!(
        processor
            .record_native_turn_completed(
                "thread_growth",
                "turn-growth",
                "completed",
                Some(1_000),
                Some(250),
                None,
                Some("Growth proposal with bounded upside.".to_string()),
            )
            .await
    );
    {
        let state = processor.state.lock().await;
        let responses = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| {
                delivery.sender_thread_id == "thread_growth"
                    && delivery.phase.as_deref() == Some("peer_review_and_objection")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            responses.len(),
            0,
            "peer fanout must wait until the proposal set is sealed"
        );
    }

    assert!(
        processor
            .record_native_turn_completed(
                "thread_risk",
                "turn-risk",
                "completed",
                Some(1_100),
                Some(275),
                None,
                Some("Risk proposal with explicit downside limits.".to_string()),
            )
            .await
    );
    let delivery_count = {
        let state = processor.state.lock().await;
        let responses = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| {
                matches!(
                    delivery.sender_thread_id.as_str(),
                    "thread_growth" | "thread_risk"
                ) && delivery.phase.as_deref() == Some("peer_review_and_objection")
            })
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert!(
            responses
                .iter()
                .all(|delivery| { delivery.sender_thread_id != delivery.receiver_thread_id })
        );
        assert_eq!(
            responses
                .iter()
                .filter(|delivery| {
                    delivery.aggregate_state
                        == Some(MemythosArenaAggregateState::RecipientTriggered)
                })
                .count(),
            2,
            "the last proposal must trigger exactly one review turn per bettor"
        );
        assert_eq!(
            responses
                .iter()
                .filter_map(|delivery| delivery.receiver_turn_id.as_deref())
                .filter(|turn_id| *turn_id != "mailbox_queued")
                .count(),
            2
        );
        state.arena_message_deliveries.len()
    };

    processor
        .record_native_turn_completed(
            "thread_risk",
            "turn-risk",
            "completed",
            Some(1_100),
            Some(275),
            None,
            Some("Risk proposal with explicit downside limits.".to_string()),
        )
        .await;
    assert_eq!(
        processor.state.lock().await.arena_message_deliveries.len(),
        delivery_count,
        "turn completion replay must not duplicate the native room response"
    );
}

#[tokio::test]
async fn room_tool_aggregates_bets_and_starts_exactly_one_judge_turn() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    register_room_with_native_arena(&processor, room_register_params_with_concierge_and_judge())
        .await;
    let contract = MemythosArenaAggregateContract {
        aggregate_id: "judge-bets-round-1".to_string(),
        recipient_thread_id: "thread_judge".to_string(),
        expected_source_thread_ids: vec!["thread_growth".to_string(), "thread_risk".to_string()],
        quorum: 2,
        phase_id: "bet".to_string(),
        deadline_ref: None,
        completion_criteria_ref: "criteria://all-bettors-committed".to_string(),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    };

    let first = processor
        .room_tool_send_message(
            "thread_growth",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/judge/fitness".to_string()),
                message: "Growth commits to option A.".to_string(),
                authority: "peer".to_string(),
                message_kind: "peer_bet".to_string(),
                response_contract: "Judge the complete bet set once sealed.".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                aggregate_contract: Some(contract.clone()),
            },
        )
        .await
        .unwrap();
    let second = processor
        .room_tool_send_message(
            "thread_risk",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/judge/fitness".to_string()),
                message: "Risk commits to option B.".to_string(),
                authority: "peer".to_string(),
                message_kind: "peer_bet".to_string(),
                response_contract: "Judge the complete bet set once sealed.".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                aggregate_contract: Some(contract),
            },
        )
        .await
        .unwrap();

    assert_eq!(first.target_turn_id, "mailbox_queued");
    assert!(second.target_turn_id.starts_with("turn_for_thread_judge_"));
    let state = processor.state.lock().await;
    let deliveries = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| delivery.aggregate_id.as_deref() == Some("judge-bets-round-1"))
        .collect::<Vec<_>>();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(
        deliveries
            .iter()
            .filter(|delivery| delivery.receiver_turn_id.is_some())
            .count(),
        1
    );
    assert_eq!(
        deliveries[1].aggregate_state,
        Some(MemythosArenaAggregateState::RecipientTriggered)
    );
}

#[tokio::test]
async fn competitive_concierge_dispatches_without_waiting_and_resumes_once_per_checkpoint() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(CompositionParentConfigurationAdapter),
        Arc::new(FakeArenaParentProvisioningAdapter::default()),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    let provision = processor
        .arena_composition_provision(competitive_composition_params(), ConnectionId(0))
        .await
        .expect("composition should provision");
    let ClientResponsePayload::MemythosArenaCompositionProvision(provision) = provision else {
        panic!("expected arena composition provision response");
    };
    let participant = |participant_id: &str| {
        provision
            .leases
            .iter()
            .find(|lease| lease.participant_id == participant_id)
            .expect("participant lease should exist")
    };
    let concierge = participant("concierge");
    let growth = participant("bettor-growth");
    let risk = participant("bettor-risk");

    let dispatch = processor
            .room_tool_send_message(
                &concierge.thread_id,
                MemythosRoomToolSendMessageArgs {
                    target_parent_key: Some(growth.parent_key.clone()),
                    message: "Produce an independent proposal, then return it to the Concierge with peer_proposal.".to_string(),
                    authority: "peer".to_string(),
                    message_kind: "peer_proposal".to_string(),
                    response_contract: "Return one bounded proposal.".to_string(),
                    delivery_policy: Some(MemythosArenaDeliveryPolicy::QueueOnly),
                    aggregate_contract: None,
                },
            )
            .await
            .expect("concierge dispatch should not wait for bettor completion");
    assert!(dispatch.target_turn_id.starts_with("turn_for_"));
    assert!(dispatch.response_text.contains("dispatched asynchronously"));
    let state = processor.state.lock().await;
    let dispatch_delivery = state
        .arena_message_deliveries
        .iter()
        .find(|delivery| {
            delivery.receiver_turn_id.as_deref() == Some(dispatch.target_turn_id.as_str())
        })
        .expect("competitive concierge dispatch delivery");
    assert_eq!(
        dispatch_delivery.delivery_policy,
        Some(MemythosArenaDeliveryPolicy::Immediate)
    );
    drop(state);

    let first = processor
        .room_tool_send_message(
            &growth.thread_id,
            MemythosRoomToolSendMessageArgs {
                target_parent_key: None,
                message: "Growth proposal.".to_string(),
                authority: "peer".to_string(),
                message_kind: "peer_proposal".to_string(),
                response_contract: String::new(),
                delivery_policy: None,
                aggregate_contract: None,
            },
        )
        .await
        .expect("first proposal should enter the aggregate mailbox");
    let second = processor
        .room_tool_send_message(
            &risk.thread_id,
            MemythosRoomToolSendMessageArgs {
                target_parent_key: None,
                message: "Risk proposal.".to_string(),
                authority: "peer".to_string(),
                message_kind: "peer_proposal".to_string(),
                response_contract: String::new(),
                delivery_policy: None,
                aggregate_contract: None,
            },
        )
        .await
        .expect("last proposal should seal and trigger the concierge checkpoint");

    assert_eq!(first.target_turn_id, "mailbox_queued");
    assert!(second.target_turn_id.starts_with("turn_for_"));
    let state = processor.state.lock().await;
    let proposal_deliveries = state
        .arena_message_deliveries
        .iter()
        .filter(|delivery| {
            delivery
                .aggregate_id
                .as_deref()
                .is_some_and(|aggregate_id| aggregate_id.ends_with("::concierge_proposal"))
        })
        .collect::<Vec<_>>();
    assert_eq!(proposal_deliveries.len(), 2);
    assert_eq!(
        proposal_deliveries
            .iter()
            .filter(|delivery| delivery.receiver_turn_id.is_some())
            .count(),
        1
    );
    assert_eq!(
        proposal_deliveries[1].checkpoint_state,
        Some(MemythosArenaCheckpointState::ConciergeSynthesis)
    );
}

#[test]
fn canonical_concierge_checkpoint_requires_every_bettor() {
    let room = MemythosRoom {
        room_id: "room-001".to_string(),
        case_id: "case-001".to_string(),
        layer_id: "bpm_e2e".to_string(),
        arena_id: "arena-001".to_string(),
        topology: "cross_parent_room".to_string(),
        participants: room_register_params_with_concierge().participants,
    };
    let concierge = room
        .participants
        .iter()
        .find(|participant| participant.parent_role == "room_concierge")
        .expect("concierge participant");
    let contract = canonical_native_concierge_phase_contract(
        &room,
        "round-1",
        concierge,
        "peer_review_and_objection",
    )
    .expect("canonical review checkpoint");

    assert_eq!(contract.quorum, 2);
    assert_eq!(contract.expected_source_thread_ids.len(), 2);
    assert_eq!(contract.recipient_thread_id, "thread_concierge");
    assert_eq!(contract.phase_id, "peer_review_and_objection");
}

#[tokio::test]
async fn room_tool_aggregate_policy_is_generic_for_non_judge_consumers() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    register_room_with_native_arena(&processor, room_register_params_with_concierge()).await;
    let contract = MemythosArenaAggregateContract {
        aggregate_id: "planner-inputs-round-1".to_string(),
        recipient_thread_id: "thread_risk".to_string(),
        expected_source_thread_ids: vec![
            "thread_growth".to_string(),
            "thread_concierge".to_string(),
        ],
        quorum: 2,
        phase_id: "planning".to_string(),
        deadline_ref: None,
        completion_criteria_ref: "criteria://all-planning-inputs".to_string(),
        late_arrival_policy: MemythosArenaLateArrivalPolicy::Reject,
    };

    let first = processor
        .room_tool_send_message(
            "thread_growth",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                message: "Provide the first planning input.".to_string(),
                authority: "peer".to_string(),
                message_kind: "planning_input".to_string(),
                response_contract: "Plan once all inputs are available.".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                aggregate_contract: Some(contract.clone()),
            },
        )
        .await
        .unwrap();
    let second = processor
        .room_tool_send_message(
            "thread_concierge",
            MemythosRoomToolSendMessageArgs {
                target_parent_key: Some("case/bpm_e2e/arena/bettor/risk".to_string()),
                message: "Provide the final planning constraint.".to_string(),
                authority: "peer".to_string(),
                message_kind: "planning_input".to_string(),
                response_contract: "Plan once all inputs are available.".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                aggregate_contract: Some(contract),
            },
        )
        .await
        .unwrap();

    assert_eq!(first.target_turn_id, "mailbox_queued");
    assert!(second.target_turn_id.starts_with("turn_for_thread_risk_"));
    let state = processor.state.lock().await;
    assert_eq!(
        state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| {
                delivery.aggregate_id.as_deref() == Some("planner-inputs-round-1")
                    && delivery.receiver_turn_id.is_some()
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn thread_consolidate_rejects_empty_sources() {
    let processor = MemythosRequestProcessor::new();
    let error = processor
            .thread_consolidate(MemythosThreadConsolidateParams {
                coordinator_thread_id: "thread_concierge".to_string(),
                source_thread_ids: Vec::new(),
                since_cursors: HashMap::new(),
                items_view: Some("summary".to_string()),
                purpose:
                    codex_app_server_protocol::MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
                authority_mode:
                    codex_app_server_protocol::MemythosThreadConsolidationAuthorityMode::PeerCoordination,
                instructions: "Consolidate.".to_string(),
                per_source_limit: Some(2),
                client_user_message_id: Some("consolidation-001".to_string()),
                output_schema: None,
            })
            .await
            .unwrap_err();

    assert!(
        error
            .message
            .contains("sourceThreadIds must contain at least one thread"),
        "unexpected error: {}",
        error.message
    );
}

#[tokio::test]
async fn thread_consolidate_uses_native_summary_and_coordinator_turn() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(FakeThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );

    let response = processor
            .thread_consolidate(MemythosThreadConsolidateParams {
                coordinator_thread_id: "thread_concierge".to_string(),
                source_thread_ids: vec!["thread_a".to_string(), "thread_b".to_string()],
                since_cursors: HashMap::from([(
                    "thread_a".to_string(),
                    "cursor-a".to_string(),
                )]),
                items_view: Some("summary".to_string()),
                purpose:
                    codex_app_server_protocol::MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
                authority_mode:
                    codex_app_server_protocol::MemythosThreadConsolidationAuthorityMode::PeerCoordination,
                instructions: "Consolidate the round for the judge.".to_string(),
                per_source_limit: Some(2),
                client_user_message_id: Some("consolidation-001".to_string()),
                output_schema: Some(serde_json::json!({"type": "object"})),
            })
            .await
            .unwrap();
    let ClientResponsePayload::MemythosThreadConsolidate(response) = response else {
        panic!("expected MemythosThreadConsolidate response");
    };

    assert_eq!(response.consolidation_turn_id, "turn-consolidation-001");
    assert_eq!(response.coordinator_thread_id, "thread_concierge");
    assert_eq!(response.source_method, "thread/turns/list");
    assert!(response.used_thread_turns_summary);
    assert!(response.blockers.is_empty());
    assert_eq!(response.source_refs.len(), 2);
    assert_eq!(response.source_refs[0].cursor.as_deref(), Some("cursor-a"));
    assert!(response.agent_message_ref.is_some());
    assert!(response.structured_output_ref.is_some());
    assert_eq!(response.technical_evidence_refs.len(), 1);

    let telemetry = processor
        .telemetry_list(MemythosTelemetryListParams {
            layer_id: None,
            arena_id: None,
            thread_id: Some("thread_concierge".to_string()),
            limit: Some(10),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosTelemetryList(telemetry) = telemetry else {
        panic!("expected MemythosTelemetryList response");
    };
    assert!(telemetry.telemetry_refs.iter().any(|telemetry_ref| {
        telemetry_ref.kind == MemythosTelemetryRefKind::ThreadConsolidation
            && telemetry_ref.source == MemythosTelemetrySource::AppServerNative
    }));
}

#[tokio::test]
async fn thread_contract_assemble_registers_native_contract_ref() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(FakeThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );

    let response = processor
        .thread_contract_assemble(MemythosThreadContractAssembleParams {
            coordinator_thread_id: "thread_concierge".to_string(),
            source_thread_ids: vec!["thread_a".to_string(), "thread_b".to_string()],
            since_cursors: HashMap::new(),
            items_view: Some("summary".to_string()),
            contract_kind: "resume_contract".to_string(),
            instructions: "Assemble a resume contract from closed evidence.".to_string(),
            per_source_limit: Some(2),
            client_user_message_id: Some("contract-001".to_string()),
            output_schema: Some(serde_json::json!({"type": "object"})),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosThreadContractAssemble(response) = response else {
        panic!("expected MemythosThreadContractAssemble response");
    };

    assert_eq!(response.contract.contract_kind, "resume_contract");
    assert_eq!(
        response.contract.storage_kind,
        "app_server_native_contract_message"
    );
    assert!(response.contract.contract_ref.contains("/contracts/"));
    assert_eq!(response.contract.producer_turn_id, "turn-consolidation-001");
    assert!(response.contract.missing_evidence.is_empty());
    assert!(response.contract.blockers.is_empty());
    assert!(response.contract.payload.is_some());
    assert!(response.agent_message_ref.is_some());
    assert!(response.structured_output_ref.is_some());
    assert!(response.used_thread_turns_summary);
    assert_eq!(response.source_refs.len(), 2);

    let read = processor
        .thread_contract_read(MemythosThreadContractReadParams {
            contract_ref: response.contract.contract_ref.clone(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosThreadContractRead(read) = read else {
        panic!("expected MemythosThreadContractRead response");
    };
    assert_eq!(read.contract.contract_ref, response.contract.contract_ref);

    let list = processor
        .thread_contract_list(MemythosThreadContractListParams {
            thread_id: Some("thread_concierge".to_string()),
            contract_kind: Some("resume_contract".to_string()),
            limit: Some(10),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosThreadContractList(list) = list else {
        panic!("expected MemythosThreadContractList response");
    };
    assert_eq!(list.contracts.len(), 1);
    assert_eq!(list.contracts[0].contract_kind, "resume_contract");
}

#[tokio::test]
async fn thread_contract_assemble_rejects_missing_output_schema() {
    let processor = MemythosRequestProcessor::new();
    let error = processor
        .thread_contract_assemble(MemythosThreadContractAssembleParams {
            coordinator_thread_id: "thread_concierge".to_string(),
            source_thread_ids: vec!["thread_a".to_string()],
            since_cursors: HashMap::new(),
            items_view: Some("summary".to_string()),
            contract_kind: "resume_contract".to_string(),
            instructions: "Assemble.".to_string(),
            per_source_limit: Some(2),
            client_user_message_id: Some("contract-001".to_string()),
            output_schema: None,
        })
        .await
        .unwrap_err();

    assert!(
        error.message.contains("outputSchema is required"),
        "unexpected error: {}",
        error.message
    );
}

#[tokio::test]
async fn room_send_input_uses_native_loopback_delivery() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    let register_response = processor
        .room_register(room_register_params())
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomRegister(register_response) = register_response else {
        panic!("expected MemythosRoomRegister response");
    };
    assert_eq!(register_response.event_refs.len(), 1);

    let send_response = processor
        .room_send_input(MemythosRoomSendInputParams {
            room_id: "room-001".to_string(),
            room_message_ref: "artifact://room/messages/message-001.json".to_string(),
            delivery_ref: "artifact://room/deliveries/delivery-001.json".to_string(),
            from_parent_thread_id: Some("thread_growth".to_string()),
            via_concierge_thread_id: None,
            to_parent_thread_id: "thread_risk".to_string(),
            source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
            target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
            message_kind: "peer_objection".to_string(),
            message_authority: "peer_debate".to_string(),
            human_instruction: false,
            response_contract: "peer_response_contract".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("message-001".to_string()),
            human_summary: "Challenge my proposal as an arena peer.".to_string(),
            prompt: "I am not a human; I am an arena peer. Challenge my proposal.".to_string(),
            metadata: serde_json::Map::from_iter([
                (
                    "memythos_round_id".to_string(),
                    serde_json::Value::String("round-001".to_string()),
                ),
                (
                    "memythos_phase".to_string(),
                    serde_json::Value::String("bet".to_string()),
                ),
            ]),
            output_schema: None,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
        panic!("expected MemythosRoomSendInput response");
    };

    assert_eq!(
        send_response.delivery.delivery_mechanism,
        "room_loopback_send_input"
    );
    assert!(!send_response.delivery.human_instruction);
    assert_eq!(send_response.delivery.thread_id, "thread_risk");
    assert_eq!(
        send_response.delivery.turn_id.as_deref(),
        Some("turn_for_thread_risk_message-001")
    );
    assert!(send_response.delivery.event_refs.iter().any(|event_ref| {
        event_ref == "app-server://rooms/room-001/messages/message-001/delivered"
    }));

    let read_response = processor
        .arena_message_read(MemythosArenaMessageReadParams {
            arena_id: "arena-room-001".to_string(),
            message_id: "message-001".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageRead(read_response) = read_response else {
        panic!("expected MemythosArenaMessageRead response");
    };
    assert_eq!(read_response.message.to_parent_thread_id, "thread_risk");
    assert!(
        read_response
            .delivered_prompt
            .contains("I am not a human; I am an arena peer")
    );
}

#[tokio::test]
async fn room_send_input_prefers_concierge_source_when_present() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    let mut params = room_register_params();
    params.participants.push(MemythosRoomParticipant {
        parent_key: "case/bpm_e2e/arena/room_concierge/coordination".to_string(),
        thread_id: "thread_concierge".to_string(),
        parent_role: "room_concierge".to_string(),
        stance_profile: "coordination".to_string(),
        goal_ref: Some("app-server://threads/thread_concierge/goals/current".to_string()),
        authority_scope: vec!["room_coordination".to_string()],
    });
    processor.room_register(params).await.unwrap();

    let send_response = processor
        .room_send_input(MemythosRoomSendInputParams {
            room_id: "room-001".to_string(),
            room_message_ref: "artifact://room/messages/message-002.json".to_string(),
            delivery_ref: "artifact://room/deliveries/delivery-002.json".to_string(),
            from_parent_thread_id: Some("thread_growth".to_string()),
            via_concierge_thread_id: Some("thread_concierge".to_string()),
            to_parent_thread_id: "thread_risk".to_string(),
            source_parent_key: "case/bpm_e2e/arena/room_concierge/coordination".to_string(),
            target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
            message_kind: "peer_proposal".to_string(),
            message_authority: "peer_debate".to_string(),
            human_instruction: false,
            response_contract: "peer_response_contract".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("message-002".to_string()),
            human_summary: "The concierge delivers a message between peers.".to_string(),
            prompt: "The concierge delivers a message between peers.".to_string(),
            metadata: serde_json::Map::from_iter([
                (
                    "memythos_round_id".to_string(),
                    serde_json::Value::String("round-001".to_string()),
                ),
                (
                    "memythos_phase".to_string(),
                    serde_json::Value::String("bet".to_string()),
                ),
            ]),
            output_schema: None,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
        panic!("expected MemythosRoomSendInput response");
    };

    assert_eq!(send_response.delivery.thread_id, "thread_risk");
    assert_eq!(
        send_response.delivery.delivery_mechanism,
        "room_loopback_send_input"
    );
    let timeline = processor
        .room_timeline_get(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: Some("proposal".to_string()),
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(timeline) = timeline else {
        panic!("expected MemythosRoomActivityList response");
    };
    let delivered = timeline
        .events
        .iter()
        .find(|event| event.event_kind == "input_delivered")
        .expect("expected delivered input event");
    assert_eq!(
        delivered.sender.role,
        Some(MemythosParentRole::RoomConcierge)
    );
    assert_eq!(
        delivered.sender.stance,
        Some(MemythosParentStance::Coordination)
    );
    assert_eq!(delivered.recipient.role, Some(MemythosParentRole::Bettor));
    assert_eq!(delivered.authority, "peer_debate");
    assert_eq!(delivered.channel, "parent_mailbox");
    assert_eq!(delivered.phase.as_deref(), Some("proposal"));
    assert_eq!(
        delivered.prompt_origin,
        MemythosPromptOrigin::AgentToAgentPrompt
    );
    assert_eq!(delivered.prompt_lineage.len(), 1);
}

#[tokio::test]
async fn room_send_input_accepts_human_intake_without_registered_source_parent() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();

    let send_response = processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: "app-server://rooms/room-001/messages/human-intake-001"
                    .to_string(),
                delivery_ref: "app-server://rooms/room-001/deliveries/human-intake-001".to_string(),
                from_parent_thread_id: None,
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_growth".to_string(),
                source_parent_key: "human".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                message_kind: "initial_human_request".to_string(),
                message_authority: "human_intake".to_string(),
                human_instruction: true,
                response_contract: "respond conversationally and ask for missing context if needed"
                    .to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                aggregate_contract: None,
                client_user_message_id: Some("human-intake-001".to_string()),
                human_summary:
                    "Assess whether the BPM node can become a tactical PID without reopening business decisions."
                        .to_string(),
                prompt:
                    "Assess whether the BPM node can become a tactical PID without reopening business decisions."
                        .to_string(),
                metadata: serde_json::Map::from_iter([
                    (
                        "memythos_round_id".to_string(),
                        serde_json::Value::String("round-human-001".to_string()),
                    ),
                    (
                        "memythos_phase".to_string(),
                        serde_json::Value::String("intake".to_string()),
                    ),
                ]),
                output_schema: None,
            })
            .await
            .unwrap();
    let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
        panic!("expected MemythosRoomSendInput response");
    };
    assert!(send_response.delivery.human_instruction);
    assert_eq!(send_response.delivery.thread_id, "thread_growth");

    let timeline = processor
        .room_timeline_get(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-human-001".to_string()),
            phase: Some("intake".to_string()),
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(timeline) = timeline else {
        panic!("expected MemythosRoomActivityList response");
    };
    let delivered = timeline
        .events
        .iter()
        .find(|event| event.event_kind == "human_intake_delivered")
        .expect("expected human intake delivery event");
    assert_eq!(delivered.sender.kind, MemythosRoomActorKind::Human);
    assert_eq!(delivered.sender.label.as_deref(), Some("human"));
    assert_eq!(delivered.recipient.role, Some(MemythosParentRole::Bettor));
    assert_eq!(delivered.authority, "human_intake");
    assert_eq!(
        delivered.prompt_origin,
        MemythosPromptOrigin::HumanPromptInjection
    );
    assert_eq!(delivered.prompt_lineage.len(), 1);
    assert_eq!(
        delivered.prompt_lineage[0].origin,
        MemythosPromptOrigin::HumanPromptInjection
    );
}

#[tokio::test]
async fn room_send_reports_final_native_delivery_mechanism() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();
    let response = processor
        .room_send(MemythosRoomSendInputParams {
            room_id: "room-001".to_string(),
            room_message_ref: "app-server://rooms/room-001/messages/message-003".to_string(),
            delivery_ref: "app-server://rooms/room-001/deliveries/delivery-003".to_string(),
            from_parent_thread_id: Some("thread_growth".to_string()),
            via_concierge_thread_id: None,
            to_parent_thread_id: "thread_risk".to_string(),
            source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
            target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
            message_kind: "peer_objection".to_string(),
            message_authority: "peer_debate".to_string(),
            human_instruction: false,
            response_contract: "peer_response_contract".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("message-003".to_string()),
            human_summary: "Challenge my proposal as an arena peer.".to_string(),
            prompt: "I am not a human; I am an arena peer. Challenge my proposal.".to_string(),
            metadata: serde_json::Map::new(),
            output_schema: None,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomSendInput(response) = response else {
        panic!("expected MemythosRoomSendInput response");
    };

    assert_eq!(response.delivery.delivery_mechanism, "room_loopback_send");
}

#[tokio::test]
async fn room_activity_list_returns_compact_initial_view() {
    let processor = MemythosRequestProcessor::new();
    processor
        .room_register(room_register_params())
        .await
        .unwrap();

    let response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert_eq!(response.source_method, "memythos/room/activity/list");
    assert_eq!(response.participants.len(), 2);
    assert!(response.turns.is_empty());
    assert!(response.events.is_empty());
    assert_eq!(response.lifecycle.room_state, "running");
    assert!(!response.lifecycle.clean_close);
    assert_eq!(response.collab.send_input_count, 0);
}

#[tokio::test]
async fn room_timeline_get_reports_final_native_source_method() {
    let processor = MemythosRequestProcessor::new();
    processor
        .room_register(room_register_params())
        .await
        .unwrap();

    let response = processor
        .room_timeline_get(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: None,
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert_eq!(response.source_method, "memythos/room/timeline/get");
}

#[tokio::test]
async fn room_activity_list_exposes_native_registration_cursor_events() {
    let processor = MemythosRequestProcessor::new_for_transport_with_native_adapters(
        AppServerRpcTransport::InProcess,
        Arc::new(RecordOnlyPeerParentDeliveryAdapter),
        Arc::new(RecordOnlyParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
        Arc::new(FakeParentConfigurationAdapter),
        Arc::new(RecordOnlyArenaParentProvisioningAdapter),
        Arc::new(RecordOnlyArenaCompositionPlanningAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();

    let response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: None,
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert_eq!(response.source_method, "memythos/room/activity/list");
    assert_eq!(response.events.len(), 3);
    assert_eq!(response.events[0].event_kind, "room_registered");
    assert_eq!(response.events[0].sequence, 1);
    assert_eq!(
        response.events[0].sender.kind,
        MemythosRoomActorKind::AppServer
    );
    assert_eq!(
        response.events[0].recipient.role,
        Some(MemythosParentRole::RoomConcierge)
    );
    assert_eq!(
        response.events[0].prompt_origin,
        MemythosPromptOrigin::MemythosRuntimeSetup
    );
    assert_eq!(response.events[1].event_kind, "participant_attached");
    assert_eq!(response.events[1].sequence, 2);
    assert_eq!(
        response.events[1].recipient.role,
        Some(MemythosParentRole::Bettor)
    );
    assert_eq!(
        response.events[1].recipient.stance,
        Some(MemythosParentStance::Growth)
    );
    assert_eq!(response.events[1].prompt_lineage.len(), 1);
    assert_eq!(response.events[2].event_kind, "participant_attached");
    assert_eq!(response.events[2].sequence, 3);
    assert_eq!(response.next_cursor, response.cursor);
    assert!(!response.has_more);

    let configuration = processor
        .room_parent_configuration_list(MemythosRoomParentConfigurationListParams {
            room_id: "room-001".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomParentConfigurationList(configuration) = configuration
    else {
        panic!("expected MemythosRoomParentConfigurationList response");
    };
    assert_eq!(
        configuration.source_method,
        "memythos/room/parent-configuration/list"
    );
    assert_eq!(configuration.configurations.len(), 2);
    assert_eq!(
        configuration.configurations[0].registered_role,
        MemythosParentRole::Bettor
    );
    assert_eq!(
        configuration.configurations[0].stance,
        MemythosParentStance::Growth
    );
    assert_eq!(
        configuration.configurations[0]
            .effective_agent_role
            .as_deref(),
        Some("native_role_for_thread_growth")
    );
    assert_eq!(
        configuration.configurations[0].personality.as_deref(),
        Some("pragmatic")
    );
    assert!(configuration.configurations[0].blockers.is_empty());
    assert!(configuration.blockers.is_empty());
}

#[tokio::test]
async fn room_activity_list_summarizes_delivery_without_closing_arena() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(FakeParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();
    let send_response = processor
        .room_send_input(MemythosRoomSendInputParams {
            room_id: "room-001".to_string(),
            room_message_ref: "artifact://room/messages/message-003.json".to_string(),
            delivery_ref: "artifact://room/deliveries/delivery-003.json".to_string(),
            from_parent_thread_id: Some("thread_growth".to_string()),
            via_concierge_thread_id: None,
            to_parent_thread_id: "thread_risk".to_string(),
            source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
            target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
            message_kind: "peer_bet".to_string(),
            message_authority: "peer_debate".to_string(),
            human_instruction: false,
            response_contract: "peer_response_contract".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("message-003".to_string()),
            human_summary: "Place a bet and state execution conditions.".to_string(),
            prompt: "Place a bet and state execution conditions.".to_string(),
            metadata: serde_json::Map::from_iter([
                (
                    "memythos_round_id".to_string(),
                    serde_json::Value::String("round-001".to_string()),
                ),
                (
                    "memythos_phase".to_string(),
                    serde_json::Value::String("bet".to_string()),
                ),
            ]),
            output_schema: None,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomSendInput(send_response) = send_response else {
        panic!("expected MemythosRoomSendInput response");
    };
    let expected_correlation_id = send_response.delivery.delivery_id;
    assert!(
        processor
            .record_native_turn_completed(
                "thread_risk",
                "turn_for_thread_risk_message-003",
                "completed",
                Some(1_000),
                Some(250),
                None,
                None,
            )
            .await
    );
    assert!(
        processor
            .record_native_token_usage(
                "thread_risk",
                "turn_for_thread_risk_message-003",
                &native_usage(1_200, 1_000, 600),
            )
            .await
    );
    assert!(
        processor
            .record_native_token_usage(
                "thread_risk",
                "turn_for_thread_risk_message-003",
                &native_usage(1_200, 1_000, 600),
            )
            .await
    );

    let response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert_eq!(response.turns.len(), 1);
    assert_eq!(response.turns[0].status, "completed");
    assert_eq!(
        response.turns[0].parent_key,
        "case/bpm_e2e/arena/bettor/risk"
    );
    assert_eq!(response.turns[0].round_id.as_deref(), Some("round-001"));
    assert_eq!(response.turns[0].phase.as_deref(), Some("bet"));
    assert_eq!(response.lifecycle.completed_turns, 1);
    assert_eq!(response.lifecycle.failed_turns, 0);
    assert_eq!(response.lifecycle.room_state, "running");
    assert!(!response.lifecycle.clean_close);
    assert_eq!(response.collab.completed_send_input_count, 1);
    assert_eq!(response.usage.token_usage_events, 1);
    assert_eq!(response.usage.total.total_tokens, 1_200);
    assert_eq!(response.usage.total.cached_input_tokens, 600);
    assert_eq!(response.usage.total.non_cached_input_tokens, 400);
    assert_eq!(response.usage.turns.len(), 1);
    assert_eq!(
        response.usage.turns[0].round_id.as_deref(),
        Some("round-001")
    );
    assert_eq!(response.usage.turns[0].phase.as_deref(), Some("bet"));
    assert_eq!(
        response.usage.turns[0].activation_reason.as_deref(),
        Some("room_loopback_delivery")
    );
    assert_eq!(
        response.usage.turns[0].participant_id.as_deref(),
        Some("case/bpm_e2e/arena/bettor/risk")
    );
    assert_eq!(
        response.usage.turns[0].causation_id.as_deref(),
        Some("message-003")
    );
    let correlation_id = response.usage.turns[0]
        .correlation_id
        .as_deref()
        .expect("native delivery correlation id");
    assert_eq!(correlation_id, expected_correlation_id);
    let usage_event = response
        .events
        .iter()
        .find(|event| event.event_kind == "token_usage_observed")
        .expect("token usage room activity event");
    assert_eq!(usage_event.phase.as_deref(), Some("bet"));
    assert_eq!(usage_event.correlation_id.as_deref(), Some(correlation_id));
    assert_eq!(usage_event.causation_id.as_deref(), Some("message-003"));
    assert_eq!(
        usage_event.activation_reason.as_deref(),
        Some("room_loopback_delivery")
    );
    assert_eq!(response.usage.turns[0].usage.total_tokens, 1_200);
    assert!(response.usage.cost_weighted_usage.is_none());
    assert!(response.blockers.is_empty());
    assert_eq!(response.turns[0].items.len(), 3);
    let delivery_item = &response.turns[0].items[0];
    assert_eq!(delivery_item.kind, "collab_call");
    assert!(delivery_item.text.is_none());
    assert!(delivery_item.human_highlight.is_none());
    let request_item = &response.turns[0].items[1];
    assert_eq!(request_item.kind, "user_message");
    assert_eq!(request_item.item_type.as_deref(), Some("userMessage"));
    assert_eq!(
        request_item.text.as_deref(),
        Some("Place a bet and state execution conditions.")
    );
    assert_eq!(request_item.human_highlight, request_item.text);
    assert!(
        !request_item
            .human_highlight
            .as_deref()
            .unwrap_or_default()
            .contains("MEMYTHOS_PEER_PARENT_MESSAGE")
    );
    assert_eq!(
        request_item.event_ref,
        "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-003/items/user-message"
    );
    let response_item = &response.turns[0].items[2];
    assert_eq!(response_item.kind, "agent_message");
    assert_eq!(response_item.item_type.as_deref(), Some("agentMessage"));
    assert_eq!(
        response_item.text.as_deref(),
        Some("Cierre conversacional OOTB de thread_risk para turn_for_thread_risk_message-003.")
    );
    assert_eq!(response_item.human_highlight, response_item.text);
    assert_eq!(
        response_item.event_ref,
        "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-003/items/final-agent-message"
    );
    assert!(
        !response_item
            .text
            .as_deref()
            .unwrap()
            .contains(" received ")
    );
    assert!(
        delivery_item
            .refs
            .iter()
            .all(|event_ref| { !event_ref.contains("/events/") })
    );

    let dialogue = processor
        .room_dialogue_list(MemythosRoomDialogueListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: Some("bet".to_string()),
            after_cursor: None,
            limit: Some(25),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomDialogueList(dialogue) = dialogue else {
        panic!("expected MemythosRoomDialogueList response");
    };
    assert_eq!(dialogue.source_method, "memythos/room/dialogue/list");
    assert_eq!(dialogue.entries.len(), 2);
    assert!(dialogue.blockers.is_empty());
    assert_eq!(dialogue.entries[0].kind, "request");
    assert_eq!(
        dialogue.entries[0].text,
        "Place a bet and state execution conditions."
    );
    assert!(
        dialogue.entries[0]
            .source_item_ref
            .ends_with("/user-message")
    );
    assert_eq!(dialogue.entries[1].kind, "response");
    assert_eq!(
        dialogue.entries[1].text,
        "Cierre conversacional OOTB de thread_risk para turn_for_thread_risk_message-003."
    );
    assert!(
        dialogue.entries[1]
            .source_item_ref
            .ends_with("/final-agent-message")
    );
    assert!(
        dialogue
            .entries
            .iter()
            .all(|entry| !entry.text.contains("Participant "))
    );

    let bet_response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: Some("bet".to_string()),
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(bet_response) = bet_response else {
        panic!("expected MemythosRoomActivityList response");
    };
    assert_eq!(bet_response.turns.len(), 1);

    let judge_response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: Some("judge".to_string()),
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(judge_response) = judge_response else {
        panic!("expected MemythosRoomActivityList response");
    };
    assert!(judge_response.turns.is_empty());
}

#[tokio::test]
async fn room_activity_list_blocks_completed_turn_without_native_agent_message() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();
    processor
        .room_send_input(MemythosRoomSendInputParams {
            room_id: "room-001".to_string(),
            room_message_ref: "app-server://rooms/room-001/messages/message-missing".to_string(),
            delivery_ref: "app-server://rooms/room-001/deliveries/delivery-missing".to_string(),
            from_parent_thread_id: Some("thread_growth".to_string()),
            via_concierge_thread_id: None,
            to_parent_thread_id: "thread_risk".to_string(),
            source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
            target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
            message_kind: "peer_bet".to_string(),
            message_authority: "peer_debate".to_string(),
            human_instruction: false,
            response_contract: "peer_response_contract".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("message-missing".to_string()),
            human_summary: "Respond with your conversational close.".to_string(),
            prompt: "Respond with your conversational close.".to_string(),
            metadata: serde_json::Map::new(),
            output_schema: None,
        })
        .await
        .unwrap();
    assert!(
        processor
            .record_native_turn_completed(
                "thread_risk",
                "turn_for_thread_risk_message-missing",
                "completed",
                None,
                None,
                None,
                None,
            )
            .await
    );

    let response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: None,
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert_eq!(response.turns.len(), 1);
    assert_eq!(response.turns[0].items.len(), 1);
    assert_eq!(response.turns[0].items[0].kind, "collab_call");
    assert!(response.turns[0].items[0].human_highlight.is_none());
    assert!(
        response
            .blockers
            .iter()
            .any(|blocker| { blocker.contains("has no readable native AgentMessage") })
    );
}

#[tokio::test]
async fn room_activity_list_does_not_require_agent_message_for_consumed_aggregate_component() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();
    {
        let mut state = processor.state.lock().await;
        state
            .arena_message_deliveries
            .push(MemythosArenaMessageDelivery {
                delivery_id: "aggregate-component-delivery".to_string(),
                message_id: "aggregate-component-message".to_string(),
                human_summary: "One sealed aggregate component.".to_string(),
                status: "receiver_turn_completed".to_string(),
                sender_thread_id: "thread_growth".to_string(),
                receiver_thread_id: "thread_risk".to_string(),
                arena_id: "arena-001".to_string(),
                round_id: "round-001".to_string(),
                phase: Some("bet".to_string()),
                delivery_mechanism: "native_aggregate_mailbox".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::AggregateThenTrigger),
                aggregate_id: Some("aggregate-001".to_string()),
                aggregate_state: Some(MemythosArenaAggregateState::Consumed),
                checkpoint_state: Some(MemythosArenaCheckpointState::NextPhaseDispatched),
                checkpoint_event_refs: Vec::new(),
                receiver_turn_id: None,
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: Vec::new(),
                rejection_reason: None,
                failure_reason: None,
            });
    }

    let response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: None,
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert!(response.blockers.is_empty());
    assert!(response.turns.is_empty());
}

#[tokio::test]
async fn room_activity_list_uses_native_agent_message_completion_projection() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();
    processor
        .room_send_input(MemythosRoomSendInputParams {
            room_id: "room-001".to_string(),
            room_message_ref: "app-server://rooms/room-001/messages/message-native".to_string(),
            delivery_ref: "app-server://rooms/room-001/deliveries/delivery-native".to_string(),
            from_parent_thread_id: Some("thread_growth".to_string()),
            via_concierge_thread_id: None,
            to_parent_thread_id: "thread_risk".to_string(),
            source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
            target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
            message_kind: "peer_bet".to_string(),
            message_authority: "peer_debate".to_string(),
            human_instruction: false,
            response_contract: "peer_response_contract".to_string(),
            delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
            aggregate_contract: None,
            client_user_message_id: Some("message-native".to_string()),
            human_summary: "Respond with your conversational close.".to_string(),
            prompt: "Respond with your conversational close.".to_string(),
            metadata: serde_json::Map::new(),
            output_schema: None,
        })
        .await
        .unwrap();
    let turn_id = "turn_for_thread_risk_message-native";
    assert!(
        processor
            .record_native_parent_agent_message(
                "thread_risk",
                turn_id,
                "agent-message-native",
                "Native parent response.".to_string(),
            )
            .await
    );
    assert!(
        processor
            .record_native_turn_completed(
                "thread_risk",
                turn_id,
                "completed",
                None,
                None,
                None,
                None,
            )
            .await
    );

    let response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: None,
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(response) = response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert!(response.blockers.is_empty());
    assert_eq!(response.turns.len(), 1);
    assert!(response.turns[0].items.iter().any(|item| {
        item.kind == "agent_message" && item.text.as_deref() == Some("Native parent response.")
    }));
}

#[tokio::test]
async fn room_activity_list_applies_since_cursor_as_delta_boundary() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    processor
        .room_register(room_register_params())
        .await
        .unwrap();

    for message_id in ["message-004", "message-005"] {
        processor
            .room_send_input(MemythosRoomSendInputParams {
                room_id: "room-001".to_string(),
                room_message_ref: format!("artifact://room/messages/{message_id}.json"),
                delivery_ref: format!("artifact://room/deliveries/{message_id}.json"),
                from_parent_thread_id: Some("thread_growth".to_string()),
                via_concierge_thread_id: None,
                to_parent_thread_id: "thread_risk".to_string(),
                source_parent_key: "case/bpm_e2e/arena/bettor/growth".to_string(),
                target_parent_key: "case/bpm_e2e/arena/bettor/risk".to_string(),
                message_kind: "peer_bet".to_string(),
                message_authority: "peer_debate".to_string(),
                human_instruction: false,
                response_contract: "peer_response_contract".to_string(),
                delivery_policy: Some(MemythosArenaDeliveryPolicy::Immediate),
                aggregate_contract: None,
                client_user_message_id: Some(message_id.to_string()),
                human_summary: format!("Incremental bet {message_id}."),
                prompt: format!("Incremental bet {message_id}."),
                metadata: serde_json::Map::from_iter([
                    (
                        "memythos_round_id".to_string(),
                        serde_json::Value::String("round-001".to_string()),
                    ),
                    (
                        "memythos_phase".to_string(),
                        serde_json::Value::String("bet".to_string()),
                    ),
                ]),
                output_schema: None,
            })
            .await
            .unwrap();
    }

    let initial_response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: None,
            since_cursor: None,
            after_cursor: None,
            limit: Some(1),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(initial_response) = initial_response else {
        panic!("expected MemythosRoomActivityList response");
    };
    assert_eq!(initial_response.turns.len(), 1);
    assert_eq!(initial_response.events.len(), 1);
    assert!(initial_response.has_more);
    assert_eq!(initial_response.returned_activity_scope, "initial");
    assert!(!initial_response.since_cursor_applied);
    let cursor = initial_response.cursor.expect("initial cursor");

    let delta_response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: None,
            since_cursor: Some(cursor.clone()),
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(delta_response) = delta_response else {
        panic!("expected MemythosRoomActivityList response");
    };

    assert!(!delta_response.events.is_empty());
    assert!(
        delta_response
            .events
            .iter()
            .all(|event| event.cursor != cursor)
    );
    assert_eq!(delta_response.returned_activity_scope, "delta");
    assert!(delta_response.since_cursor_applied);
    assert!(delta_response.blockers.is_empty());
    assert_ne!(delta_response.cursor, None);

    let stale_response = processor
        .room_activity_list(MemythosRoomActivityListParams {
            room_id: "room-001".to_string(),
            round_id: Some("round-001".to_string()),
            phase: None,
            since_cursor: Some("missing-cursor".to_string()),
            after_cursor: None,
            limit: Some(25),
            include_debug_refs: false,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRoomActivityList(stale_response) = stale_response else {
        panic!("expected MemythosRoomActivityList response");
    };
    assert!(stale_response.turns.is_empty());
    assert!(stale_response.events.is_empty());
    assert!(!stale_response.since_cursor_applied);
    assert!(
        stale_response
            .blockers
            .iter()
            .any(|blocker| blocker.contains("unknown or stale room activity cursor"))
    );
}

#[tokio::test]
async fn creates_layer_and_arena_contract() {
    let processor = MemythosRequestProcessor::new();
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();

    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };

    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id.clone(),
            name: "Node contract debate".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Close the node contract.".to_string(),
            participant_ids: vec!["thread_a".to_string(), "thread_b".to_string()],
        })
        .await
        .unwrap();

    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };
    assert_eq!(arena_response.arena.layer_id, layer_response.layer.layer_id);
    assert_eq!(
        arena_response.arena.lifecycle_state,
        MemythosArenaLifecycleState::Draft
    );
}

#[tokio::test]
async fn canonical_arena_lifecycle_is_owned_by_rust_across_rpc_calls() {
    let processor = MemythosRequestProcessor::new();
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "Native lifecycle".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Exercise canonical Arena transitions.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id,
            name: "Canonical lifecycle".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Keep lifecycle authority in Rust.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };
    let arena_id = arena_response.arena.arena_id;
    let start = MemythosArenaPhaseStartParams {
        arena_id: arena_id.clone(),
        round_id: "round-1".to_string(),
        phase: "proposal".to_string(),
    };

    let first_start = processor.arena_phase_start(start.clone()).await.unwrap();
    let ClientResponsePayload::MemythosArenaPhaseStart(first_start) = first_start else {
        panic!("expected MemythosArenaPhaseStart response");
    };
    assert_eq!(
        first_start.lifecycle_state,
        MemythosArenaLifecycleState::Running
    );
    assert!(first_start.event_refs[0].contains("started?sequence=1"));

    let retained_start = processor.arena_phase_start(start).await.unwrap();
    let ClientResponsePayload::MemythosArenaPhaseStart(retained_start) = retained_start else {
        panic!("expected MemythosArenaPhaseStart response");
    };
    assert!(
        retained_start.event_refs[0].contains("start-retained?sequence=2"),
        "repeating the RPC must retain the canonical phase"
    );

    let concurrent = processor
        .arena_phase_start(MemythosArenaPhaseStartParams {
            arena_id: arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: "bet".to_string(),
        })
        .await
        .unwrap_err();
    assert!(concurrent.message.contains("proposal is active"));

    let state_response = processor
        .arena_state_get(MemythosArenaStateGetParams {
            arena_id: arena_id.clone(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaStateGet(state_response) = state_response else {
        panic!("expected MemythosArenaStateGet response");
    };
    assert!(!state_response.local_ts_arena_state_used);
    assert_eq!(
        state_response.arena.lifecycle_state,
        MemythosArenaLifecycleState::Running
    );

    let close = MemythosArenaPhaseCloseParams {
        arena_id: arena_id.clone(),
        round_id: "round-1".to_string(),
        phase: "proposal".to_string(),
    };
    processor.arena_phase_close(close.clone()).await.unwrap();
    let retained_close = processor.arena_phase_close(close).await.unwrap();
    let ClientResponsePayload::MemythosArenaPhaseClose(retained_close) = retained_close else {
        panic!("expected MemythosArenaPhaseClose response");
    };
    assert!(
        retained_close.event_refs[0].contains("close-retained?sequence=4"),
        "repeating close must not duplicate the transition"
    );

    processor
        .arena_phase_start(MemythosArenaPhaseStartParams {
            arena_id: arena_id.clone(),
            round_id: "round-1".to_string(),
            phase: "bet".to_string(),
        })
        .await
        .unwrap();
    let run_response = processor
        .arena_run(MemythosArenaRunParams {
            arena_id,
            round_id: "round-1".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaRun(run_response) = run_response else {
        panic!("expected MemythosArenaRun response");
    };
    assert!(!run_response.local_ts_arena_state_used);
    assert_eq!(
        run_response.lifecycle_state,
        MemythosArenaLifecycleState::Running
    );
}

#[tokio::test]
async fn rejects_arena_for_unknown_layer() {
    let processor = MemythosRequestProcessor::new();
    let err = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: "missing".to_string(),
            name: "Invalid arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Should fail.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap_err();

    assert!(err.message.contains("unknown layer id"));
}

#[tokio::test]
async fn reports_runtime_health_and_clean_close() {
    let processor = MemythosRequestProcessor::new();
    let health_response = processor
        .runtime_health(MemythosRuntimeHealthParams::default())
        .await
        .unwrap();

    let ClientResponsePayload::MemythosRuntimeHealth(health_response) = health_response else {
        panic!("expected MemythosRuntimeHealth response");
    };
    assert_eq!(
        health_response.lifecycle_state,
        MemythosRuntimeLifecycleState::Ready
    );
    assert_eq!(health_response.runtime_family, "app_server");
    assert_eq!(health_response.connection_mode, "stdio");
    assert_eq!(health_response.transport_owner, "app_server");
    assert_eq!(health_response.transport_id.as_deref(), Some("stdio"));
    assert!(!health_response.daemon_runtime_verified);
    assert!(
        health_response
            .capabilities
            .contains(&"memythos/thread/attach".to_string())
    );
    for capability in [
        "memythos/room/create",
        "memythos/room/list",
        "memythos/room/send",
        "memythos/room/timeline/get",
        "memythos/room/contract/emit",
        "memythos/room/contract/get",
    ] {
        assert!(
            health_response
                .capabilities
                .contains(&capability.to_string()),
            "missing Memythos room capability: {capability}"
        );
    }

    let close_response = processor
        .runtime_close(MemythosRuntimeCloseParams {
            force: false,
            reason: None,
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosRuntimeClose(close_response) = close_response else {
        panic!("expected MemythosRuntimeClose response");
    };
    assert!(close_response.closed_cleanly);
    assert_eq!(
        close_response.lifecycle_state,
        MemythosRuntimeLifecycleState::ClosedCleanly
    );
}

#[tokio::test]
async fn reports_daemon_transport_when_runtime_is_websocket() {
    let processor = MemythosRequestProcessor::new_for_transport(AppServerRpcTransport::Websocket);
    let health_response = processor
        .runtime_health(MemythosRuntimeHealthParams::default())
        .await
        .unwrap();

    let ClientResponsePayload::MemythosRuntimeHealth(health_response) = health_response else {
        panic!("expected MemythosRuntimeHealth response");
    };

    assert_eq!(health_response.connection_mode, "daemon_websocket");
    assert_eq!(health_response.transport_owner, "app_server_daemon");
    assert_eq!(health_response.transport_id.as_deref(), Some("websocket"));
    assert!(health_response.daemon_runtime_verified);
}

#[tokio::test]
async fn attaches_threads_and_filters_telemetry_refs() {
    let processor = MemythosRequestProcessor::new();
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id.clone(),
            name: "Node contract debate".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Close the node contract.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };

    let attach_response = processor
        .thread_attach(MemythosThreadAttachParams {
            arena_id: arena_response.arena.arena_id.clone(),
            thread_id: "thread_a".to_string(),
            role_id: Some("peer".to_string()),
            stance_id: Some("skeptic".to_string()),
            objective: Some("Challenge the implementation PID.".to_string()),
            contract_ref: Some("implementation-pid.md".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosThreadAttach(attach_response) = attach_response else {
        panic!("expected MemythosThreadAttach response");
    };
    assert_eq!(attach_response.attachment.thread_id, "thread_a");

    let list_response = processor
        .thread_list(MemythosThreadListParams {
            arena_id: arena_response.arena.arena_id.clone(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosThreadList(list_response) = list_response else {
        panic!("expected MemythosThreadList response");
    };
    assert_eq!(list_response.attachments.len(), 1);

    let telemetry_response = processor
        .telemetry_list(MemythosTelemetryListParams {
            layer_id: Some(layer_response.layer.layer_id),
            arena_id: Some(arena_response.arena.arena_id),
            thread_id: Some("thread_a".to_string()),
            limit: Some(10),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
    else {
        panic!("expected MemythosTelemetryList response");
    };
    assert_eq!(telemetry_response.telemetry_refs.len(), 1);
    assert_eq!(
        telemetry_response.telemetry_refs[0].kind,
        MemythosTelemetryRefKind::ThreadAttachment
    );
    assert_eq!(
        telemetry_response.telemetry_refs[0].source,
        MemythosTelemetrySource::MemythosRuntimeState
    );
    assert_eq!(telemetry_response.telemetry_refs[0].native_event_ref, None);
    assert_eq!(telemetry_response.telemetry_refs[0].detail_ref, None);
}

#[tokio::test]
async fn native_telemetry_refs_are_source_tagged_and_compact() {
    let processor = MemythosRequestProcessor::new();
    let mut state = processor.state.lock().await;
    let long_summary = "tool payload ".repeat(80);

    processor.push_native_telemetry_ref_for_test(
        &mut state,
        MemythosTelemetryRefKind::RuntimeState,
        Some("mem_layer_1".to_string()),
        Some("mem_arena_1".to_string()),
        Some("thread_a".to_string()),
        "event:turn:42".to_string(),
        Some("app-server://events/turn/42".to_string()),
        MemythosEventChannel::TechnicalDetail,
        long_summary,
    );

    let telemetry_ref = state.telemetry_refs.last().expect("telemetry ref exists");
    assert_eq!(
        telemetry_ref.source,
        MemythosTelemetrySource::AppServerNative
    );
    assert_eq!(
        telemetry_ref.native_event_ref.as_deref(),
        Some("event:turn:42")
    );
    assert_eq!(
        telemetry_ref.detail_ref.as_deref(),
        Some("app-server://events/turn/42")
    );
    assert!(telemetry_ref.summary.chars().count() <= MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS);
    assert!(telemetry_ref.summary.ends_with('…'));
}

#[tokio::test]
async fn records_native_thread_events_for_attached_threads_only() {
    let processor = MemythosRequestProcessor::new();
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id.clone(),
            name: "Node contract debate".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Close the node contract.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };
    processor
        .thread_attach(MemythosThreadAttachParams {
            arena_id: arena_response.arena.arena_id.clone(),
            thread_id: "thread_a".to_string(),
            role_id: Some("peer".to_string()),
            stance_id: Some("skeptic".to_string()),
            objective: Some("Challenge the implementation PID.".to_string()),
            contract_ref: Some("implementation-pid.md".to_string()),
        })
        .await
        .unwrap();

    let unattached_recorded = processor
        .record_native_thread_event(
            "thread_missing",
            "thread:missing/turn:1/event:1".to_string(),
            None,
            MemythosEventChannel::TechnicalDetail,
            "Ignored unattached thread event.".to_string(),
        )
        .await;
    assert!(!unattached_recorded);

    let attached_recorded = processor
        .record_native_thread_event(
            "thread_a",
            "thread:thread_a/turn:1/event:2".to_string(),
            Some("app-server://threads/thread_a/turns/1/events/2".to_string()),
            MemythosEventChannel::HumanHighlight,
            "Thread reported a useful debate highlight.".to_string(),
        )
        .await;
    assert!(attached_recorded);

    let telemetry_response = processor
        .telemetry_list(MemythosTelemetryListParams {
            layer_id: Some(layer_response.layer.layer_id),
            arena_id: Some(arena_response.arena.arena_id),
            thread_id: Some("thread_a".to_string()),
            limit: Some(10),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
    else {
        panic!("expected MemythosTelemetryList response");
    };
    let native_refs: Vec<_> = telemetry_response
        .telemetry_refs
        .iter()
        .filter(|telemetry_ref| telemetry_ref.source == MemythosTelemetrySource::AppServerNative)
        .collect();
    assert_eq!(native_refs.len(), 1);
    assert_eq!(
        native_refs[0].native_event_ref.as_deref(),
        Some("thread:thread_a/turn:1/event:2")
    );
    assert_eq!(
        native_refs[0].detail_ref.as_deref(),
        Some("app-server://threads/thread_a/turns/1/events/2")
    );
    assert_eq!(native_refs[0].channel, MemythosEventChannel::HumanHighlight);
}

#[tokio::test]
async fn registers_arena_parents_and_delivers_parent_messages() {
    let processor = MemythosRequestProcessor::new();
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id.clone(),
            name: "Parent peer arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Let parent peers challenge ownership and routing.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };
    for thread_id in ["thread_growth", "thread_risk"] {
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                role_id: Some("bettor".to_string()),
                stance_id: Some(thread_id.to_string()),
                objective: Some("Debate the BPM node contract.".to_string()),
                contract_ref: Some("arena-contract.json".to_string()),
            })
            .await
            .unwrap();
    }

    for (thread_id, stance) in [("thread_growth", "growth"), ("thread_risk", "risk")] {
        let response = processor
            .arena_parent_register(MemythosArenaParentRegisterParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: stance.to_string(),
                authority_scope: vec!["peer_debate".to_string()],
            })
            .await
            .unwrap();
        let ClientResponsePayload::MemythosArenaParentRegister(response) = response else {
            panic!("expected MemythosArenaParentRegister response");
        };
        assert_eq!(
            response.parent.lifecycle_state,
            MemythosArenaLifecycleState::Running
        );
    }

    let send_response = processor
        .arena_message_send(MemythosArenaMessageSendParams {
            message: MemythosArenaMessage {
                message_id: "message-001".to_string(),
                case_id: "case-001".to_string(),
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: "round-001".to_string(),
                from_parent_thread_id: "thread_growth".to_string(),
                from_parent_role: "bettor".to_string(),
                to_parent_thread_id: "thread_risk".to_string(),
                to_parent_role: "bettor".to_string(),
                message_kind: "peer_objection".to_string(),
                human_summary: "Challenge ambiguous ownership before tactical execution."
                    .to_string(),
                execution_prompt: None,
                context_packet_ref: "artifact://context/minimal".to_string(),
                artifact_refs: vec!["arena-contract.json".to_string()],
                requires_response: true,
                delivery_policy: None,
                aggregate_contract: None,
                response_contract: Some("peer_objection_response".to_string()),
                output_schema: None,
            },
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageSend(send_response) = send_response else {
        panic!("expected MemythosArenaMessageSend response");
    };
    assert_eq!(send_response.delivery.status, "recorded");
    assert_eq!(send_response.delivery.delivery_mechanism, "record_only");
    assert_eq!(send_response.delivery.receiver_turn_id, None);
    assert_eq!(send_response.delivery.receiver_response_event_ref, None);
    assert!(!send_response.delivery.delivered_as_human_instruction);
    assert_eq!(send_response.delivery.memory_replay_required, false);
    assert_eq!(send_response.delivery.receiver_thread_id, "thread_risk");

    let list_response = processor
        .arena_message_list(MemythosArenaMessageListParams {
            arena_id: arena_response.arena.arena_id.clone(),
            round_id: Some("round-001".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageList(list_response) = list_response else {
        panic!("expected MemythosArenaMessageList response");
    };
    assert_eq!(list_response.deliveries.len(), 1);
    assert_eq!(list_response.deliveries[0].message_id, "message-001");

    let observation_response = processor
        .arena_message_observation_list(MemythosArenaMessageObservationListParams {
            arena_id: arena_response.arena.arena_id.clone(),
            round_id: Some("round-001".to_string()),
            message_id: Some("message-001".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageObservationList(observation_response) =
        observation_response
    else {
        panic!("expected MemythosArenaMessageObservationList response");
    };
    assert_eq!(observation_response.observations.len(), 1);
    assert_eq!(
        observation_response.observations[0].observed_response_kind,
        MemythosParentPeerResponseKind::NoResponse
    );
    assert_eq!(
        observation_response.observations[0].semantic_alignment,
        MemythosSemanticAlignment::Invalid
    );

    let telemetry_response = processor
        .telemetry_list(MemythosTelemetryListParams {
            layer_id: Some(layer_response.layer.layer_id),
            arena_id: Some(arena_response.arena.arena_id),
            thread_id: Some("thread_risk".to_string()),
            limit: Some(10),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
    else {
        panic!("expected MemythosTelemetryList response");
    };
    assert!(
        telemetry_response
            .telemetry_refs
            .iter()
            .any(|telemetry_ref| {
                telemetry_ref.kind == MemythosTelemetryRefKind::ArenaMessage
                    && telemetry_ref.channel == MemythosEventChannel::TechnicalDetail
                    && telemetry_ref.native_event_ref.is_some()
            })
    );
}

#[tokio::test]
async fn arena_message_send_can_use_live_peer_parent_delivery_adapter() {
    let processor = MemythosRequestProcessor::new_for_transport_with_peer_delivery(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
    );
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id.clone(),
            name: "Parent peer arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Let parent peers challenge ownership and routing.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };

    for thread_id in ["thread_growth", "thread_risk"] {
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                role_id: Some("bettor".to_string()),
                stance_id: Some(thread_id.to_string()),
                objective: Some("Debate the BPM node contract.".to_string()),
                contract_ref: Some("arena-contract.json".to_string()),
            })
            .await
            .unwrap();
        processor
            .arena_parent_register(MemythosArenaParentRegisterParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: thread_id.to_string(),
                authority_scope: vec!["peer_debate".to_string()],
            })
            .await
            .unwrap();
    }

    let send_response = processor
        .arena_message_send(MemythosArenaMessageSendParams {
            message: MemythosArenaMessage {
                message_id: "message-live-001".to_string(),
                case_id: "case-001".to_string(),
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: "round-001".to_string(),
                from_parent_thread_id: "thread_growth".to_string(),
                from_parent_role: "bettor".to_string(),
                to_parent_thread_id: "thread_risk".to_string(),
                to_parent_role: "bettor".to_string(),
                message_kind: "peer_objection".to_string(),
                human_summary: "Challenge ambiguous ownership before tactical execution."
                    .to_string(),
                execution_prompt: None,
                context_packet_ref: "artifact://context/minimal".to_string(),
                artifact_refs: vec!["arena-contract.json".to_string()],
                requires_response: true,
                delivery_policy: None,
                aggregate_contract: None,
                response_contract: Some("peer_objection_response".to_string()),
                output_schema: None,
            },
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageSend(send_response) = send_response else {
        panic!("expected MemythosArenaMessageSend response");
    };

    assert_eq!(send_response.delivery.status, "delivered_to_live_thread");
    assert_eq!(send_response.delivery.delivery_mechanism, "turn_start");
    assert_eq!(
        send_response.delivery.receiver_turn_id.as_deref(),
        Some("turn_for_thread_risk_message-live-001")
    );
    assert_eq!(send_response.delivery.receiver_response_event_ref, None);
    assert!(!send_response.delivery.delivered_as_human_instruction);
    assert!(
        send_response
            .delivery
            .event_refs
            .iter()
            .any(|event_ref| event_ref.contains("app-server://threads/thread_risk"))
    );

    let observation_response = processor
        .arena_message_observation_list(MemythosArenaMessageObservationListParams {
            arena_id: arena_response.arena.arena_id,
            round_id: Some("round-001".to_string()),
            message_id: Some("message-live-001".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageObservationList(observation_response) =
        observation_response
    else {
        panic!("expected MemythosArenaMessageObservationList response");
    };
    assert_eq!(observation_response.observations.len(), 1);
    assert_eq!(
        observation_response.observations[0].observed_response_kind,
        MemythosParentPeerResponseKind::PendingResponse
    );
    assert_eq!(
        observation_response.observations[0].semantic_alignment,
        MemythosSemanticAlignment::Pending
    );
    assert_eq!(
        observation_response.observations[0]
            .receiver_turn_id
            .as_deref(),
        Some("turn_for_thread_risk_message-live-001")
    );
    assert!(!observation_response.observations[0].treated_as_human_instruction);
}

#[tokio::test]
async fn native_turn_completion_promotes_parent_peer_observation() {
    let processor = MemythosRequestProcessor::new_for_transport_with_peer_delivery(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
    );
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id.clone(),
            name: "Parent peer arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Let parent peers challenge ownership and routing.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };

    for thread_id in ["thread_growth", "thread_risk"] {
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                role_id: Some("bettor".to_string()),
                stance_id: Some(thread_id.to_string()),
                objective: Some("Debate the BPM node contract.".to_string()),
                contract_ref: Some("arena-contract.json".to_string()),
            })
            .await
            .unwrap();
        processor
            .arena_parent_register(MemythosArenaParentRegisterParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: thread_id.to_string(),
                authority_scope: vec!["peer_debate".to_string()],
            })
            .await
            .unwrap();
    }

    processor
        .arena_message_send(MemythosArenaMessageSendParams {
            message: MemythosArenaMessage {
                message_id: "message-live-002".to_string(),
                case_id: "case-001".to_string(),
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: "round-001".to_string(),
                from_parent_thread_id: "thread_growth".to_string(),
                from_parent_role: "bettor".to_string(),
                to_parent_thread_id: "thread_risk".to_string(),
                to_parent_role: "bettor".to_string(),
                message_kind: "peer_objection".to_string(),
                human_summary: "Challenge ambiguous ownership before tactical execution."
                    .to_string(),
                execution_prompt: None,
                context_packet_ref: "artifact://context/minimal".to_string(),
                artifact_refs: vec!["arena-contract.json".to_string()],
                requires_response: true,
                delivery_policy: None,
                aggregate_contract: None,
                response_contract: Some("peer_objection_response".to_string()),
                output_schema: None,
            },
        })
        .await
        .unwrap();

    let matched = processor
        .record_native_turn_completed(
            "thread_risk",
            "turn_for_thread_risk_message-live-002",
            "completed",
            Some(1234),
            Some(2500),
            None,
            None,
        )
        .await;
    assert!(matched);

    let list_response = processor
        .arena_message_list(MemythosArenaMessageListParams {
            arena_id: arena_response.arena.arena_id.clone(),
            round_id: Some("round-001".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageList(list_response) = list_response else {
        panic!("expected MemythosArenaMessageList response");
    };
    assert_eq!(list_response.deliveries.len(), 1);
    assert_eq!(
        list_response.deliveries[0].status,
        "receiver_turn_completed"
    );
    assert_eq!(
        list_response.deliveries[0]
            .receiver_response_event_ref
            .as_deref(),
        Some(
            "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/completed"
        )
    );

    let observation_response = processor
        .arena_message_observation_list(MemythosArenaMessageObservationListParams {
            arena_id: arena_response.arena.arena_id.clone(),
            round_id: Some("round-001".to_string()),
            message_id: Some("message-live-002".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageObservationList(observation_response) =
        observation_response
    else {
        panic!("expected MemythosArenaMessageObservationList response");
    };
    assert_eq!(
        observation_response.observations[0].observed_response_kind,
        MemythosParentPeerResponseKind::Ack
    );
    assert_eq!(
        observation_response.observations[0].semantic_alignment,
        MemythosSemanticAlignment::Acceptable
    );

    let telemetry_response = processor
        .telemetry_list(MemythosTelemetryListParams {
            layer_id: Some(layer_response.layer.layer_id.clone()),
            arena_id: Some(arena_response.arena.arena_id.clone()),
            thread_id: Some("thread_risk".to_string()),
            limit: Some(20),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
    else {
        panic!("expected MemythosTelemetryList response");
    };
    assert!(
            telemetry_response
                .telemetry_refs
                .iter()
                .any(|telemetry_ref| {
                    telemetry_ref.source == MemythosTelemetrySource::AppServerNative
                        && telemetry_ref.native_event_ref.as_deref()
                            == Some(
                                "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/completed"
                            )
                })
        );

    processor
        .arena_message_send(MemythosArenaMessageSendParams {
            message: MemythosArenaMessage {
                message_id: "message-live-003".to_string(),
                case_id: "case-001".to_string(),
                arena_id: arena_response.arena.arena_id.clone(),
                round_id: "round-001".to_string(),
                from_parent_thread_id: "thread_growth".to_string(),
                from_parent_role: "bettor".to_string(),
                to_parent_thread_id: "thread_risk".to_string(),
                to_parent_role: "bettor".to_string(),
                message_kind: "peer_objection".to_string(),
                human_summary: "Exercise native failure evidence preservation.".to_string(),
                execution_prompt: None,
                context_packet_ref: "artifact://context/minimal".to_string(),
                artifact_refs: vec![],
                requires_response: true,
                delivery_policy: None,
                aggregate_contract: None,
                response_contract: Some("peer_objection_response".to_string()),
                output_schema: None,
            },
        })
        .await
        .unwrap();

    let failure_reason = "Model backend ended the turn before producing a response.";
    let matched = processor
        .record_native_turn_completed(
            "thread_risk",
            "turn_for_thread_risk_message-live-003",
            "failed",
            Some(5678),
            Some(667),
            Some(failure_reason.to_string()),
            None,
        )
        .await;
    assert!(matched);

    let list_response = processor
        .arena_message_list(MemythosArenaMessageListParams {
            arena_id: arena_response.arena.arena_id.clone(),
            round_id: Some("round-001".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaMessageList(list_response) = list_response else {
        panic!("expected MemythosArenaMessageList response");
    };
    let failed_delivery = list_response
        .deliveries
        .iter()
        .find(|delivery| delivery.message_id == "message-live-003")
        .expect("failed native delivery should remain observable");
    assert_eq!(failed_delivery.status, "receiver_turn_failed");
    assert_eq!(
        failed_delivery.failure_reason.as_deref(),
        Some(failure_reason)
    );

    let telemetry_response = processor
        .telemetry_list(MemythosTelemetryListParams {
            layer_id: Some(layer_response.layer.layer_id),
            arena_id: Some(arena_response.arena.arena_id),
            thread_id: Some("thread_risk".to_string()),
            limit: Some(20),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosTelemetryList(telemetry_response) = telemetry_response
    else {
        panic!("expected MemythosTelemetryList response");
    };
    assert!(
        telemetry_response
            .telemetry_refs
            .iter()
            .any(|telemetry_ref| telemetry_ref.summary.contains(failure_reason))
    );
}

#[tokio::test]
async fn parent_continuity_tracks_multiple_receiver_turns() {
    let processor = MemythosRequestProcessor::new_for_transport_with_peer_delivery(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
    );
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id,
            name: "Parent peer arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Let parent peers challenge ownership and routing.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };

    for thread_id in ["thread_growth", "thread_risk"] {
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                role_id: Some("bettor".to_string()),
                stance_id: Some(thread_id.to_string()),
                objective: Some("Debate the BPM node contract.".to_string()),
                contract_ref: Some("arena-contract.json".to_string()),
            })
            .await
            .unwrap();
        processor
            .arena_parent_register(MemythosArenaParentRegisterParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: thread_id.to_string(),
                authority_scope: vec!["peer_debate".to_string()],
            })
            .await
            .unwrap();
    }

    for message_id in ["message-live-001", "message-live-002"] {
        processor
            .arena_message_send(MemythosArenaMessageSendParams {
                message: MemythosArenaMessage {
                    message_id: message_id.to_string(),
                    case_id: "case-001".to_string(),
                    arena_id: arena_response.arena.arena_id.clone(),
                    round_id: "round-001".to_string(),
                    from_parent_thread_id: "thread_growth".to_string(),
                    from_parent_role: "bettor".to_string(),
                    to_parent_thread_id: "thread_risk".to_string(),
                    to_parent_role: "bettor".to_string(),
                    message_kind: "peer_objection".to_string(),
                    human_summary: "Challenge ambiguous ownership before tactical execution."
                        .to_string(),
                    execution_prompt: None,
                    context_packet_ref: "artifact://context/minimal".to_string(),
                    artifact_refs: vec!["arena-contract.json".to_string()],
                    requires_response: true,
                    delivery_policy: None,
                    aggregate_contract: None,
                    response_contract: Some("peer_objection_response".to_string()),
                    output_schema: None,
                },
            })
            .await
            .unwrap();
    }

    let continuity_response = processor
        .parent_continuity_list(MemythosParentContinuityListParams {
            arena_id: arena_response.arena.arena_id,
            thread_id: Some("thread_risk".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosParentContinuityList(continuity_response) =
        continuity_response
    else {
        panic!("expected MemythosParentContinuityList response");
    };
    assert_eq!(continuity_response.continuities.len(), 1);
    let continuity = &continuity_response.continuities[0];
    assert_eq!(
        continuity.continuity_status,
        MemythosParentContinuityStatus::TurnContinuityObserved
    );
    assert_eq!(continuity.observed_turn_count, 2);
    assert_eq!(
        continuity.first_turn_id.as_deref(),
        Some("turn_for_thread_risk_message-live-001")
    );
    assert_eq!(
        continuity.latest_turn_id.as_deref(),
        Some("turn_for_thread_risk_message-live-002")
    );
    assert!(!continuity.memory_replay_required);
    assert!(!continuity.goal_snapshot_available);
}

#[tokio::test]
async fn parent_continuity_is_verified_with_goal_snapshot_and_completed_turn() {
    let processor = MemythosRequestProcessor::new_for_transport_with_adapters(
        AppServerRpcTransport::Websocket,
        Arc::new(FakeLivePeerParentDeliveryAdapter),
        Arc::new(FakeParentGoalSnapshotAdapter),
        Arc::new(RecordOnlyThreadConsolidationAdapter),
        Arc::new(RecordOnlyParentTurnResponseAdapter),
    );
    let layer_response = processor
        .layer_create(MemythosLayerCreateParams {
            name: "BPM E2E".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: None,
            objective: "Resolve an end-to-end process segment.".to_string(),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosLayerCreate(layer_response) = layer_response else {
        panic!("expected MemythosLayerCreate response");
    };
    let arena_response = processor
        .arena_create(MemythosArenaCreateParams {
            layer_id: layer_response.layer.layer_id,
            name: "Parent peer arena".to_string(),
            kind: MemythosArenaKind::Debate,
            objective: "Let parent peers challenge ownership and routing.".to_string(),
            participant_ids: vec![],
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosArenaCreate(arena_response) = arena_response else {
        panic!("expected MemythosArenaCreate response");
    };

    for thread_id in ["thread_growth", "thread_risk"] {
        processor
            .thread_attach(MemythosThreadAttachParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                role_id: Some("bettor".to_string()),
                stance_id: Some(thread_id.to_string()),
                objective: Some("Debate the BPM node contract.".to_string()),
                contract_ref: Some("arena-contract.json".to_string()),
            })
            .await
            .unwrap();
        processor
            .arena_parent_register(MemythosArenaParentRegisterParams {
                arena_id: arena_response.arena.arena_id.clone(),
                thread_id: thread_id.to_string(),
                parent_role: "bettor".to_string(),
                stance_profile: thread_id.to_string(),
                authority_scope: vec!["peer_debate".to_string()],
            })
            .await
            .unwrap();
    }

    for message_id in ["message-live-001", "message-live-002"] {
        processor
            .arena_message_send(MemythosArenaMessageSendParams {
                message: MemythosArenaMessage {
                    message_id: message_id.to_string(),
                    case_id: "case-001".to_string(),
                    arena_id: arena_response.arena.arena_id.clone(),
                    round_id: "round-001".to_string(),
                    from_parent_thread_id: "thread_growth".to_string(),
                    from_parent_role: "bettor".to_string(),
                    to_parent_thread_id: "thread_risk".to_string(),
                    to_parent_role: "bettor".to_string(),
                    message_kind: "peer_objection".to_string(),
                    human_summary: "Challenge ambiguous ownership before tactical execution."
                        .to_string(),
                    execution_prompt: None,
                    context_packet_ref: "artifact://context/minimal".to_string(),
                    artifact_refs: vec!["arena-contract.json".to_string()],
                    requires_response: true,
                    delivery_policy: None,
                    aggregate_contract: None,
                    response_contract: Some("peer_objection_response".to_string()),
                    output_schema: None,
                },
            })
            .await
            .unwrap();
    }
    processor
        .record_native_turn_completed(
            "thread_risk",
            "turn_for_thread_risk_message-live-002",
            "completed",
            Some(1234),
            Some(2500),
            None,
            None,
        )
        .await;
    processor
        .record_native_token_usage(
            "thread_risk",
            "turn_for_thread_risk_message-live-002",
            &native_usage(900, 700, 500),
        )
        .await;

    let continuity_response = processor
        .parent_continuity_list(MemythosParentContinuityListParams {
            arena_id: arena_response.arena.arena_id,
            thread_id: Some("thread_risk".to_string()),
        })
        .await
        .unwrap();
    let ClientResponsePayload::MemythosParentContinuityList(continuity_response) =
        continuity_response
    else {
        panic!("expected MemythosParentContinuityList response");
    };

    assert_eq!(continuity_response.continuities.len(), 1);
    let continuity = &continuity_response.continuities[0];
    assert_eq!(
        continuity.continuity_status,
        MemythosParentContinuityStatus::Verified
    );
    assert!(continuity.goal_snapshot_available);
    assert_eq!(
        continuity.goal_snapshot_ref.as_deref(),
        Some("app-server://threads/thread_risk/goals/current")
    );
    assert_eq!(
        continuity.budget_state_ref.as_deref(),
        Some("app-server://threads/thread_risk/budget/current")
    );
    assert_eq!(continuity.goal_status, Some(ThreadGoalStatus::Active));
    assert_eq!(continuity.token_budget, Some(20_000));
    assert_eq!(continuity.tokens_used, Some(3_800));
    assert_eq!(continuity.time_used_seconds, Some(71));
    assert_eq!(
        continuity.latest_turn_completed_ref.as_deref(),
        Some(
            "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/completed"
        )
    );
    assert_eq!(
        continuity.token_usage_ref.as_deref(),
        Some(
            "app-server://threads/thread_risk/turns/turn_for_thread_risk_message-live-002/token-usage"
        )
    );
}
