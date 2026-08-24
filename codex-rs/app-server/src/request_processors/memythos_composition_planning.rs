use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaCompositionContract;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaRequestParams;
use codex_app_server_protocol::MemythosArenaResumeAssessment;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_core::StartThreadOptions;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_utils_absolute_path::AbsolutePathBuf;

use crate::error_code::invalid_params;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::ThreadRequestProcessor;
use crate::request_processors::TurnRequestProcessor;
use crate::request_processors::memythos_contracts::validate_responses_output_schema;
use crate::request_processors::memythos_resume::validate_native_resume_assessment;

#[derive(Debug, Clone)]
pub(crate) struct PlannedArenaComposition {
    pub(super) planner_thread_id: String,
    pub(super) planner_turn_id: String,
    pub(super) contract: MemythosArenaCompositionContract,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedArenaResume {
    pub(super) planner_thread_id: String,
    pub(super) planner_turn_id: String,
    pub(super) assessment: MemythosArenaResumeAssessment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArenaCompositionPlanningContractError {
    MissingPlannerRef(&'static str),
    ArenaMismatch,
    InvalidResumeAssessment(String),
}

impl std::fmt::Display for ArenaCompositionPlanningContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPlannerRef(reference) => {
                write!(formatter, "planning result is missing {reference}")
            }
            Self::ArenaMismatch => {
                formatter.write_str("planned contract does not belong to requested arena")
            }
            Self::InvalidResumeAssessment(message) => {
                write!(formatter, "resume assessment is invalid: {message}")
            }
        }
    }
}

fn validate_planner_refs(
    planner_thread_id: &str,
    planner_turn_id: &str,
) -> Result<(), ArenaCompositionPlanningContractError> {
    if planner_thread_id.trim().is_empty() {
        return Err(ArenaCompositionPlanningContractError::MissingPlannerRef(
            "planner thread id",
        ));
    }
    if planner_turn_id.trim().is_empty() {
        return Err(ArenaCompositionPlanningContractError::MissingPlannerRef(
            "planner turn id",
        ));
    }
    Ok(())
}

pub(crate) fn validate_planned_arena_composition(
    params: &MemythosArenaRequestParams,
    planned: &PlannedArenaComposition,
) -> Result<(), ArenaCompositionPlanningContractError> {
    validate_planner_refs(&planned.planner_thread_id, &planned.planner_turn_id)?;
    if planned.contract.arena_id != params.arena_id {
        return Err(ArenaCompositionPlanningContractError::ArenaMismatch);
    }
    Ok(())
}

pub(crate) fn validate_planned_arena_resume(
    previous: &MemythosArenaCompositionProvisionResponse,
    planned: &PlannedArenaResume,
) -> Result<(), ArenaCompositionPlanningContractError> {
    validate_planner_refs(&planned.planner_thread_id, &planned.planner_turn_id)?;
    validate_native_resume_assessment(&planned.assessment, previous).map_err(|error| {
        ArenaCompositionPlanningContractError::InvalidResumeAssessment(error.message)
    })
}

pub(crate) type ArenaCompositionPlanningFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PlannedArenaComposition, JSONRPCErrorError>> + Send + 'a>>;
pub(crate) type ArenaResumePlanningFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PlannedArenaResume, JSONRPCErrorError>> + Send + 'a>>;

pub(crate) trait ArenaCompositionPlanningAdapter: Send + Sync {
    fn plan<'a>(
        &'a self,
        params: &'a MemythosArenaRequestParams,
        previous: Option<&'a MemythosArenaCompositionProvisionResponse>,
        connection_id: ConnectionId,
    ) -> ArenaCompositionPlanningFuture<'a>;

    fn assess_resume<'a>(
        &'a self,
        _params: &'a MemythosArenaRequestParams,
        _previous: &'a MemythosArenaCompositionProvisionResponse,
        _connection_id: ConnectionId,
    ) -> ArenaResumePlanningFuture<'a> {
        Box::pin(async {
            Err(invalid_params(
                "native material-novelty assessment is unavailable for this planner",
            ))
        })
    }
}

#[derive(Clone)]
pub(crate) struct NativeArenaCompositionPlanningAdapter {
    thread_manager: Arc<ThreadManager>,
    config: Arc<Config>,
    thread_processor: ThreadRequestProcessor,
    turn_processor: TurnRequestProcessor,
}

const ARENA_COMPOSITION_PLANNER_ROLE: &str = "arena_composition_planner";

pub(super) fn arena_composition_output_schema() -> Result<serde_json::Value, JSONRPCErrorError> {
    let mut schema = protocol_definition_output_schema("MemythosArenaCompositionContract")?;
    normalize_arena_composition_output_schema(&mut schema);
    close_json_schema_objects(&mut schema);
    validate_responses_output_schema(&schema)?;
    Ok(schema)
}

fn arena_resume_output_schema() -> Result<serde_json::Value, JSONRPCErrorError> {
    let mut schema = protocol_definition_output_schema("MemythosArenaResumeAssessment")?;
    close_json_schema_objects(&mut schema);
    validate_responses_output_schema(&schema)?;
    Ok(schema)
}

fn protocol_definition_output_schema(
    definition_name: &str,
) -> Result<serde_json::Value, JSONRPCErrorError> {
    let bundle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../app-server-protocol/schema/json/ClientRequest.json"
    )))
    .map_err(|err| invalid_params(format!("failed to load protocol schema bundle: {err}")))?;
    let definitions = bundle
        .get("definitions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| invalid_params("protocol schema bundle has no definitions"))?;
    let mut schema = definitions
        .get(definition_name)
        .cloned()
        .ok_or_else(|| invalid_params(format!("protocol schema has no {definition_name}")))?;
    let mut pending = Vec::new();
    collect_protocol_definition_refs(&schema, &mut pending);
    let mut reachable = serde_json::Map::new();
    while let Some(name) = pending.pop() {
        if reachable.contains_key(&name) {
            continue;
        }
        let definition = definitions
            .get(&name)
            .cloned()
            .ok_or_else(|| invalid_params(format!("protocol schema has no {name}")))?;
        collect_protocol_definition_refs(&definition, &mut pending);
        reachable.insert(name, definition);
    }
    schema
        .as_object_mut()
        .ok_or_else(|| {
            invalid_params(format!(
                "protocol definition {definition_name} is not an object"
            ))
        })?
        .insert(
            "definitions".to_string(),
            serde_json::Value::Object(reachable),
        );
    Ok(schema)
}

fn collect_protocol_definition_refs(value: &serde_json::Value, refs: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(name) = object
                .get("$ref")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.strip_prefix("#/definitions/"))
            {
                refs.push(name.to_string());
            }
            for value in object.values() {
                collect_protocol_definition_refs(value, refs);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_protocol_definition_refs(value, refs);
            }
        }
        _ => {}
    }
}

pub(super) fn native_mechanism_cross_read_output_schema(
    participant_id: &str,
    eligible_bettor_ids: &[String],
    proposal_refs: &[String],
    peer_proposal_refs: &[String],
) -> Result<serde_json::Value, JSONRPCErrorError> {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "participant_id": { "type": "string", "enum": [participant_id] },
            "proposal_ref": { "type": "string", "enum": proposal_refs },
            "supported_proposal_participant_id": { "type": "string", "enum": eligible_bettor_ids },
            "mechanism_state": { "type": "string", "enum": ["distinct", "converged", "rollup_required"] },
            "supported_mechanism": { "type": "string" },
            "mechanism_delta": { "type": "string" },
            "decision_effect": { "type": "string" },
            "shared_ground": { "type": "array", "items": { "type": "string" } },
            "incorporated_peer_refs": {
                "type": "array",
                "items": { "type": "string", "enum": peer_proposal_refs }
            },
            "residual_dissent": { "type": "string" },
            "yield_condition": { "type": "string" },
            "parent_rollup_question": { "type": "string" }
        },
        "required": [
            "participant_id", "proposal_ref", "supported_proposal_participant_id",
            "mechanism_state", "supported_mechanism", "mechanism_delta",
            "decision_effect", "shared_ground", "incorporated_peer_refs",
            "residual_dissent", "yield_condition", "parent_rollup_question"
        ],
        "additionalProperties": false
    });
    validate_responses_output_schema(&schema)?;
    Ok(schema)
}

pub(super) fn native_mechanism_bet_output_schema(
    participant_id: &str,
    eligible_bettor_ids: &[String],
    proposal_refs: &[String],
    cross_read_refs: &[String],
) -> Result<serde_json::Value, JSONRPCErrorError> {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "participant_id": { "type": "string", "enum": [participant_id] },
            "proposal_ref": { "type": "string", "enum": proposal_refs },
            "cross_read_ref": { "type": "string", "enum": cross_read_refs },
            "supported_proposal_participant_id": { "type": "string", "enum": eligible_bettor_ids },
            "mechanism_state": { "type": "string", "enum": ["distinct", "conditioned", "converged", "rollup_required"] },
            "supported_mechanism": { "type": "string" },
            "mechanism_delta": { "type": "string" },
            "decision_effect": { "type": "string" },
            "shared_ground": { "type": "array", "items": { "type": "string" } },
            "residual_dissent": { "type": "string" },
            "yield_condition": { "type": "string" },
            "accepted_tradeoff": { "type": "string" },
            "cost_of_error": { "type": "string" },
            "reopening_signals": { "type": "array", "items": { "type": "string" } },
            "parent_rollup_question": { "type": "string" }
        },
        "required": [
            "participant_id", "proposal_ref", "cross_read_ref",
            "supported_proposal_participant_id", "mechanism_state",
            "supported_mechanism", "mechanism_delta", "decision_effect",
            "shared_ground", "residual_dissent", "yield_condition",
            "accepted_tradeoff", "cost_of_error", "reopening_signals",
            "parent_rollup_question"
        ],
        "additionalProperties": false
    });
    validate_responses_output_schema(&schema)?;
    Ok(schema)
}

fn normalize_arena_composition_output_schema(schema: &mut serde_json::Value) {
    match schema {
        serde_json::Value::Object(object) => {
            if let Some(reasoning_effort) = object
                .get_mut("properties")
                .and_then(serde_json::Value::as_object_mut)
                .and_then(|properties| properties.get_mut("reasoningEffort"))
            {
                *reasoning_effort = serde_json::json!({
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh"],
                    "description": "Native app-server reasoning effort for this arena parent. The active arena parent toolset is incompatible with none/minimal."
                });
            }
            for value in object.values_mut() {
                normalize_arena_composition_output_schema(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_arena_composition_output_schema(value);
            }
        }
        _ => {}
    }
}

fn close_json_schema_objects(schema: &mut serde_json::Value) {
    match schema {
        serde_json::Value::Object(object) => {
            if object.get("type").and_then(serde_json::Value::as_str) == Some("object") {
                object
                    .entry("additionalProperties")
                    .or_insert(serde_json::Value::Bool(false));
                if let Some(properties) = object
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                {
                    object.insert(
                        "required".to_string(),
                        serde_json::Value::Array(
                            properties
                                .keys()
                                .cloned()
                                .map(serde_json::Value::String)
                                .collect(),
                        ),
                    );
                }
            }
            for value in object.values_mut() {
                close_json_schema_objects(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                close_json_schema_objects(value);
            }
        }
        _ => {}
    }
}

impl NativeArenaCompositionPlanningAdapter {
    pub(crate) fn new(
        thread_manager: Arc<ThreadManager>,
        config: Arc<Config>,
        thread_processor: ThreadRequestProcessor,
        turn_processor: TurnRequestProcessor,
    ) -> Self {
        Self {
            thread_manager,
            config,
            thread_processor,
            turn_processor,
        }
    }

    async fn read_planner_turn(
        &self,
        planner_thread_id: &str,
        planner_turn_id: &str,
    ) -> Result<Option<codex_app_server_protocol::Turn>, JSONRPCErrorError> {
        let response = self
            .thread_processor
            .thread_turns_list(ThreadTurnsListParams {
                thread_id: planner_thread_id.to_string(),
                cursor: None,
                limit: Some(10),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            })
            .await?;
        let Some(ClientResponsePayload::ThreadTurnsList(response)) = response else {
            return Ok(None);
        };
        Ok(response
            .data
            .into_iter()
            .find(|turn| turn.id == planner_turn_id))
    }

    fn planner_context(
        &self,
        params: &MemythosArenaRequestParams,
        previous: Option<&MemythosArenaCompositionProvisionResponse>,
    ) -> serde_json::Value {
        let roles = codex_core::effective_role_catalog(&self.config)
            .into_iter()
            .map(|role| {
                let capabilities = role.config.planner_capabilities.as_ref();
                serde_json::json!({
                    "id": role.id,
                    "description": role.config.description,
                    "allowedStances": capabilities.map(|value| value.allowed_stances.clone()).unwrap_or_default(),
                    "authorityScopes": capabilities.map(|value| value.authority_scopes.clone()).unwrap_or_default(),
                    "participantKinds": capabilities.map(|value| value.participant_kinds.clone()).unwrap_or_default(),
                    "requiredCompanions": capabilities.map(|value| value.required_companions.clone()).unwrap_or_default(),
                    "incompatibleRoles": capabilities.map(|value| value.incompatible_roles.clone()).unwrap_or_default(),
                    "supportsMultipleStances": capabilities.map(|value| value.supports_multiple_stances).unwrap_or(false),
                    "proposalBearing": capabilities.map(|value| value.proposal_bearing).unwrap_or(false),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "caseId": params.case_id,
            "layerId": params.layer_id,
            "arenaId": params.arena_id,
            "roomId": params.room_id,
            "requestOrigin": params.request_origin,
            "caseBrief": params.case_brief,
            "layerObjective": params.layer_objective,
            "expectedDeliverable": params.expected_deliverable,
            "completionCriteria": params.completion_criteria,
            "closedDecisions": params.closed_decisions,
            "availableAuthority": params.available_authority,
            "uncertainties": params.uncertainties,
            "realityEvidence": params.reality_evidence,
            "costGoal": params.cost_goal,
            "costContext": params.cost_context,
            "compositionChangeSignal": params.composition_change_signal,
            "resumeContext": params.resume_context,
            "previousComposition": previous,
            "nativeRoleCatalog": roles,
        })
    }

    async fn assess_resume_native(
        &self,
        params: &MemythosArenaRequestParams,
        previous: &MemythosArenaCompositionProvisionResponse,
        connection_id: ConnectionId,
    ) -> Result<PlannedArenaResume, JSONRPCErrorError> {
        let mut config = (*self.config).clone();
        if let Some(cwd) = params.cwd.as_ref() {
            config.cwd = AbsolutePathBuf::try_from(PathBuf::from(cwd)).map_err(|err| {
                invalid_params(format!("arena request cwd must be absolute: {err}"))
            })?;
        }
        config.developer_instructions = Some(
                    concat!(
                        "You are the native Memythos material-novelty assessor. Decide whether a closed arena decision must be resumed. ",
                        "Material novelty requires new reality evidence, a new human or upstream definition, a contradiction with the current decision, ",
                        "a reached breakpoint, a material objective/restriction/authority change, or a later fact that invalidates a bet. ",
                        "Elapsed time, inactivity, repeated wording, or a generic desire to validate again are not material novelty. ",
                        "Use retain_decision when the prior result remains comparable, partial_resume when only named participants or perspectives must work again, ",
                        "and full_round only when cited change evidence invalidates comparability across the prior competitive result. ",
                        "For retain_decision, return no affected participants, comparabilityInvalidated=false, and avoidedFullRound=true. ",
                        "For partial_resume, return at least one active affected participant and at least one supplied candidateChangeRef as a citedChangeRef, ",
                        "with comparabilityInvalidated=false and avoidedFullRound=true. For full_round, cite at least one supplied candidateChangeRef and one affected decision ref, ",
                        "with comparabilityInvalidated=true and avoidedFullRound=false. If no supplied candidateChangeRef supports partial_resume or full_round, use retain_decision. ",
                        "Always return a resumeExecutionPlan matching the disposition: retain_decision uses retain_decision, partial_resume uses reassess_affected_positions, and full_round uses full_round. ",
                        "The plan must repeat the exact affected participant ids, affected decision refs, and cited change refs from the assessment. Use the supplied sourceRoundId exactly; never invent it. ",
                        "Never invent refs. Preserve closed decisions that are unaffected, and return only the requested structured assessment.",
                        "Treat protectedDecisions as authoritative unless a cited change materially invalidates one. ",
                        "RevisableSettlement contains hypotheses, weights, and interpretations that may change without reopening protected decisions. ",
                        "OpenImplementationScope contains downstream questions that may be resolved without reopening either business decisions or the whole settlement."
                    )
                    .to_string(),
        );
        codex_core::apply_role_to_config_for_multi_agent_v2(
            &mut config,
            Some(ARENA_COMPOSITION_PLANNER_ROLE),
        )
        .await
        .map_err(invalid_params)?;
        let environments = self
            .thread_manager
            .default_environment_selections(&config.cwd, &config.workspace_roots);
        let mut start_options = StartThreadOptions::new(config);
        start_options.dynamic_tools = Vec::new();
        start_options.metrics_service_name = Some("memythos_arena_novelty_assessor".to_string());
        start_options.environments = Some(environments);
        let planner = self
            .thread_manager
            .start_thread(start_options)
            .await
            .map_err(|err| {
                invalid_params(format!("failed to start native novelty assessor: {err}"))
            })?;
        self.thread_processor
            .try_attach_thread_listener(planner.thread_id, vec![connection_id])
            .await;
        let planner_thread_id = planner.thread_id.to_string();
        let context = serde_json::to_string_pretty(&serde_json::json!({
            "request": self.planner_context(params, Some(previous)),
            "resumeContext": params.resume_context,
            "activeParticipantIds": previous.contract.participants.iter().map(|participant| participant.participant_id.as_str()).collect::<Vec<_>>(),
            "previousCompositionVersion": previous.composition_version,
            "previousContractRefs": previous.event_refs,
            "sourceRoundId": format!("{}-round-{}", params.arena_id, previous.composition_version),
        }))
        .map_err(|err| invalid_params(format!("failed to serialize novelty context: {err}")))?;
        let turn = self
            .turn_processor
            .turn_start(
                ConnectionRequestId {
                    connection_id,
                    request_id: RequestId::String(format!(
                        "memythos-arena-novelty:{}",
                        params.arena_id
                    )),
                },
                TurnStartParams {
                    thread_id: planner_thread_id.clone(),
                    client_user_message_id: Some(format!(
                        "arena-novelty:{}:{}",
                        params.arena_id, previous.composition_version
                    )),
                    input: vec![UserInput::Text {
                        text: format!(
                            "Assess material novelty and select the smallest valid resume scope. Do not re-plan the composition yet.\n\n{context}"
                        ),
                        text_elements: vec![],
                    }],
                    responsesapi_client_metadata: None,
                    additional_context: None,
                    environments: None,
                    cwd: None,
                    runtime_workspace_roots: None,
                    approval_policy: None,
                    approvals_reviewer: None,
                    sandbox_policy: None,
                    permissions: None,
                    model: None,
                    service_tier: None,
                    effort: None,
                    summary: None,
                    personality: None,
                    output_schema: Some(arena_resume_output_schema()?),
                    collaboration_mode: None,
                    multi_agent_mode: None,
                },
                Some("memythos".to_string()),
                None,
            )
            .await?;
        let Some(ClientResponsePayload::TurnStart(turn)) = turn else {
            return Err(invalid_params(
                "native novelty assessor did not start a turn",
            ));
        };
        let planner_turn_id = turn.turn.id;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        loop {
            if !self
                .thread_processor
                .turn_terminal_observed(&planner_thread_id, &planner_turn_id)
                .await?
            {
                if tokio::time::Instant::now() >= deadline {
                    return Err(invalid_params(format!(
                        "native novelty assessor turn {planner_turn_id} did not reach an OOTB terminal event before timeout"
                    )));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            let planner_turn = self
                .read_planner_turn(&planner_thread_id, &planner_turn_id)
                .await?;
            match planner_turn.as_ref().map(|turn| &turn.status) {
                Some(TurnStatus::Completed) => {
                    let text = planner_turn
                        .as_ref()
                        .and_then(|turn| {
                            turn.items.iter().rev().find_map(|item| match item {
                                ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
                                _ => None,
                            })
                        })
                        .ok_or_else(|| {
                            invalid_params(format!(
                                "native novelty assessor turn {planner_turn_id} completed without an OOTB final AgentMessage"
                            ))
                        })?;
                    let assessment = serde_json::from_str::<MemythosArenaResumeAssessment>(text)
                        .map_err(|err| {
                            invalid_params(format!(
                                "native novelty assessor returned invalid JSON: {err}"
                            ))
                        })?;
                    validate_native_resume_assessment(&assessment, previous)?;
                    return Ok(PlannedArenaResume {
                        planner_thread_id,
                        planner_turn_id,
                        assessment,
                    });
                }
                Some(TurnStatus::Failed) => {
                    let error = planner_turn
                        .as_ref()
                        .and_then(|turn| turn.error.as_ref())
                        .map(|error| error.message.as_str())
                        .unwrap_or("unknown app-server turn failure");
                    return Err(invalid_params(format!(
                        "native novelty assessor turn {planner_turn_id} failed: {error}"
                    )));
                }
                Some(TurnStatus::Interrupted) => {
                    return Err(invalid_params(format!(
                        "native novelty assessor turn {planner_turn_id} was interrupted"
                    )));
                }
                _ => {
                    return Err(invalid_params(format!(
                        "native novelty assessor turn {planner_turn_id} ended without a terminal status"
                    )));
                }
            }
        }
    }
}

impl ArenaCompositionPlanningAdapter for NativeArenaCompositionPlanningAdapter {
    fn plan<'a>(
        &'a self,
        params: &'a MemythosArenaRequestParams,
        previous: Option<&'a MemythosArenaCompositionProvisionResponse>,
        connection_id: ConnectionId,
    ) -> ArenaCompositionPlanningFuture<'a> {
        Box::pin(async move {
            let mut config = (*self.config).clone();
            if let Some(cwd) = params.cwd.as_ref() {
                config.cwd = AbsolutePathBuf::try_from(PathBuf::from(cwd)).map_err(|err| {
                    invalid_params(format!("arena request cwd must be absolute: {err}"))
                })?;
            }
            config.developer_instructions = Some(
                        "You are the native Memythos arena composition planner. Select parent roles and distinct stances exclusively from the supplied native role catalog. Do not solve the business case. Express domain-specific perspectives through stance and roleObjective; generic native roles are intentionally reusable across domains. For every proposal-bearing bettor, make roleObjective a case-specific differential mandate that states the question this perspective protects, evidence it must seek, risk no other selected perspective represents equally, authority it does not possess, and conditions under which it must yield or request rollup. Do not encode a fixed business-role catalog. Set unresolvedRoleGap to null whenever the catalog can express the required capability through a generic role and stance, and use a non-null gap only when the catalog structurally lacks a necessary coordination or decision capability. If you select competitive_debate, betting_round, or ranked_selection, method integrity requires at least two proposal-bearing bettors with materially different stances plus one room_concierge and one judge. The Room Concierge owns technical coordination, checkpoints, dependencies, exception routing, and communication; it is not a proposer or business authority. coordinatorParticipantId must be null for an ordinary arena. Select an additional coordinator/process steward only for an explicit regulatory, method-conflict, or exceptional-governance requirement and explain that exception in rationale. Native method authorities such as coordinate, delegate, and judge are granted internally by the selected arena method; they do not require matching business authority from availableAuthority. When availableAuthority includes delegate and the arena may promote an approved contract downstream after the judge verdict, assign delegate to the room_concierge; downstream promotion is native arena lifecycle work, not a missing proposal-bearing business role. Proposal-bearing authority must remain inside availableAuthority. Optimize team size only after preserving this invariant. Propose an effort intent and select a native reasoningEffort for every participant. The active arena parent toolset requires reasoningEffort low, medium, high, or xhigh; none and minimal are invalid for this runtime. Within that compatible range, choose effort proportionate to uncertainty and decision impact; routine room coordination and concise phase responses normally need less effort than final judgment of material uncertainty. tokenBudget is a cumulative hard limit over the complete parent objective, including every arena phase and all input/output tokens. Produce a costEnvelope before runtime. Use mode open and null budgets when costContext has neither an explicit numeric cap nor accepted comparable evidence. Use calibrated only from cited accepted comparable evidence, and explicit_cap only from costContext.explicitTokenCap. For calibrated or explicit_cap, assign every participant a positive tokenBudget, make their sum equal totalTokenBudget, and separately report the concierge coordination budget and all other substantive budgets. Funding must preserve the selected method, completion criteria, cross-read, objections, bets, and judge. If the available explicit cap cannot fund method integrity, do not pretend it can: select change_method with an honest compatible method or request_expansion while preserving the competitive composition. A qualitative request for efficiency, a small team, brevity, speed, or lower cost is not an explicit numeric hard limit. Never invent a numeric cap from qualitative cost language. The exhaustion policy is an agentic plan consumed through OOTB goals: exhaustion means wrap-up/replan, justified expansion, or explicit method change, never blind kill. Return only the requested structured contract."
                            .to_string(),
            );
            codex_core::apply_role_to_config_for_multi_agent_v2(
                &mut config,
                Some(ARENA_COMPOSITION_PLANNER_ROLE),
            )
            .await
            .map_err(invalid_params)?;
            let environments = self
                .thread_manager
                .default_environment_selections(&config.cwd, &config.workspace_roots);
            let planner = self
                .thread_manager
                .start_thread(StartThreadOptions {
                    agent_role: Some(ARENA_COMPOSITION_PLANNER_ROLE.to_string()),
                    dynamic_tools: Vec::new(),
                    metrics_service_name: Some("memythos_arena_composition_planner".to_string()),
                    environments: Some(environments),
                    ..StartThreadOptions::new(config)
                })
                .await
                .map_err(|err| {
                    invalid_params(format!("failed to start native arena planner: {err}"))
                })?;
            self.thread_processor
                .try_attach_thread_listener(planner.thread_id, vec![connection_id])
                .await;
            let planner_thread_id = planner.thread_id.to_string();
            let request_id = ConnectionRequestId {
                connection_id,
                request_id: RequestId::String(format!("memythos-arena-plan:{}", params.arena_id)),
            };
            let context = serde_json::to_string_pretty(&self.planner_context(params, previous))
                .map_err(|err| {
                    invalid_params(format!("failed to serialize arena planning context: {err}"))
                })?;
            let turn = self
                .turn_processor
                .turn_start(
                    request_id,
                    TurnStartParams {
                        thread_id: planner_thread_id.clone(),
                        client_user_message_id: Some(format!("arena-plan:{}", params.arena_id)),
                        input: vec![UserInput::Text {
                            text: format!("Plan the parent composition for this arena request. The client supplied semantic intent only; all runtime composition decisions belong here. If previousComposition exists, preserve participant IDs only when role and stance remain identical; use new IDs for replacements and explain the change in rationale.\n\n{context}"),
                            text_elements: vec![],
                        }],
                        responsesapi_client_metadata: None,
                        additional_context: None,
                        environments: None,
                        cwd: None,
                        runtime_workspace_roots: None,
                        approval_policy: None,
                        approvals_reviewer: None,
                        sandbox_policy: None,
                        permissions: None,
                        model: None,
                        service_tier: None,
                        effort: None,
                        summary: None,
                        personality: None,
                        output_schema: Some(arena_composition_output_schema()?),
                        collaboration_mode: None,
                        multi_agent_mode: None,
                    },
                    Some("memythos".to_string()),
                    None,
                )
                .await?;
            let Some(ClientResponsePayload::TurnStart(turn)) = turn else {
                return Err(invalid_params("native arena planner did not start a turn"));
            };
            let mut planner_turn_id = turn.turn.id;
            let mut role_gap_repair_attempts = 0_u8;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
            loop {
                if !self
                    .thread_processor
                    .turn_terminal_observed(&planner_thread_id, &planner_turn_id)
                    .await?
                {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(invalid_params(format!(
                            "native arena planner turn {planner_turn_id} did not reach an OOTB terminal event before timeout"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                let planner_turn = self
                    .read_planner_turn(&planner_thread_id, &planner_turn_id)
                    .await?;
                match planner_turn.as_ref().map(|turn| &turn.status) {
                    Some(TurnStatus::Completed) => {
                        let text = planner_turn
                            .as_ref()
                            .and_then(|turn| {
                                turn.items.iter().rev().find_map(|item| match item {
                                    ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
                                    _ => None,
                                })
                            })
                            .ok_or_else(|| {
                                invalid_params(format!(
                                    "native arena planner turn {planner_turn_id} completed without an OOTB final AgentMessage"
                                ))
                            })?;
                        let contract =
                            serde_json::from_str::<MemythosArenaCompositionContract>(&text)
                                .map_err(|err| {
                                    invalid_params(format!(
                                        "native arena planner returned invalid contract JSON: {err}"
                                    ))
                                })?;
                        if let Some(role_gap) = contract.unresolved_role_gap.as_deref() {
                            if role_gap_repair_attempts >= 2 {
                                return Err(invalid_params(format!(
                                    "native arena planner retained unresolvedRoleGap after {role_gap_repair_attempts} same-thread reviews: {role_gap}"
                                )));
                            }
                            role_gap_repair_attempts += 1;
                            let repair = self
                                .turn_processor
                                .turn_start(
                                    ConnectionRequestId {
                                        connection_id,
                                        request_id: RequestId::String(format!(
                                            "memythos-arena-plan-repair:{}",
                                            format!("{}:{role_gap_repair_attempts}", params.arena_id)
                                        )),
                                    },
                                    TurnStartParams {
                                        thread_id: planner_thread_id.clone(),
                                        client_user_message_id: Some(format!(
                                            "arena-plan-repair:{}",
                                            format!("{}:{role_gap_repair_attempts}", params.arena_id)
                                        )),
                                        input: vec![UserInput::Text {
                                            text: format!(
                                                "Native contract validation rejected unresolvedRoleGap: {role_gap}. Review attempt {role_gap_repair_attempts} of 2 on this same planner thread. Re-read nativeRoleCatalog in the original planning context below. Generic roles are intentionally domain-independent: express business specialization through stance and roleObjective, not a new role. A competitive arena already has native coordination and decision authority through room_concierge and judge. The Room Concierge owns ordinary checkpoint coordination and downstream promotion; do not add a process_steward unless an explicit exceptional-governance rationale requires one. Unless the catalog truly lacks one of those structural capabilities, return unresolvedRoleGap as null. Preserve method integrity and emit the complete corrected contract only.\n\nOriginal planning context:\n{context}"
                                            ),
                                            text_elements: vec![],
                                        }],
                                        responsesapi_client_metadata: None,
                                        additional_context: None,
                                        environments: None,
                                        cwd: None,
                                        runtime_workspace_roots: None,
                                        approval_policy: None,
                                        approvals_reviewer: None,
                                        sandbox_policy: None,
                                        permissions: None,
                                        model: None,
                                        service_tier: None,
                                        effort: None,
                                        summary: None,
                                        personality: None,
                                        output_schema: Some(arena_composition_output_schema()?),
                                        collaboration_mode: None,
                                        multi_agent_mode: None,
                                    },
                                    Some("memythos".to_string()),
                                    None,
                                )
                                .await?;
                            let Some(ClientResponsePayload::TurnStart(repair)) = repair else {
                                return Err(invalid_params(
                                    "native arena planner role-gap repair did not start a turn",
                                ));
                            };
                            planner_turn_id = repair.turn.id;
                            continue;
                        }
                        return Ok(PlannedArenaComposition {
                            planner_thread_id,
                            planner_turn_id,
                            contract,
                        });
                    }
                    Some(TurnStatus::Failed) => {
                        let error = planner_turn
                            .as_ref()
                            .and_then(|turn| turn.error.as_ref())
                            .map(|error| error.message.as_str())
                            .unwrap_or("unknown app-server turn failure");
                        return Err(invalid_params(format!(
                            "native arena planner turn {planner_turn_id} failed: {error}"
                        )));
                    }
                    Some(TurnStatus::Interrupted) => {
                        return Err(invalid_params(format!(
                            "native arena planner turn {planner_turn_id} was interrupted"
                        )));
                    }
                    Some(TurnStatus::InProgress) | None => {}
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(invalid_params(format!(
                        "timed out waiting for native arena planner turn {}",
                        planner_turn_id
                    )));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    }

    fn assess_resume<'a>(
        &'a self,
        params: &'a MemythosArenaRequestParams,
        previous: &'a MemythosArenaCompositionProvisionResponse,
        connection_id: ConnectionId,
    ) -> ArenaResumePlanningFuture<'a> {
        Box::pin(async move {
            self.assess_resume_native(params, previous, connection_id)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_evidence_contract_requires_both_ootb_refs() {
        assert_eq!(validate_planner_refs("thread-1", "turn-1"), Ok(()));
        assert_eq!(
            validate_planner_refs("", "turn-1"),
            Err(ArenaCompositionPlanningContractError::MissingPlannerRef(
                "planner thread id"
            ))
        );
        assert_eq!(
            validate_planner_refs("thread-1", "  "),
            Err(ArenaCompositionPlanningContractError::MissingPlannerRef(
                "planner turn id"
            ))
        );
    }
}
