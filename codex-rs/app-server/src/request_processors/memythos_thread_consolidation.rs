use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use codex_app_server_protocol::AdditionalContextEntry;
use codex_app_server_protocol::AdditionalContextKind;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::MemythosThreadConsolidateParams;
use codex_app_server_protocol::MemythosThreadConsolidationSourceRef;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;

use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::ThreadRequestProcessor;
use crate::request_processors::TurnRequestProcessor;
use crate::request_processors::memythos_contracts::build_thread_consolidation_prompt;
use crate::request_processors::memythos_contracts::compact_event_refs;
use crate::request_processors::memythos_contracts::empty_consolidation_source_ref;
use crate::request_processors::memythos_contracts::normalize_consolidation_items_view;

#[derive(Debug, Clone)]
pub(crate) struct ThreadConsolidationAttempt {
    pub(super) consolidation_turn_id: Option<String>,
    pub(super) source_refs: Vec<MemythosThreadConsolidationSourceRef>,
    pub(super) agent_message_ref: Option<String>,
    pub(super) structured_output_ref: Option<String>,
    pub(super) technical_evidence_refs: Vec<String>,
    pub(super) source_method: String,
    pub(super) used_thread_turns_summary: bool,
    pub(super) blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ThreadConsolidationContractError {
    SourceSetMismatch,
    SourceProjectionMismatch(String),
    ForeignEvidenceRef(String),
    InvalidMethodDisposition,
    MissingConsolidationTurn,
    ForeignCoordinatorRef(&'static str),
}

impl std::fmt::Display for ThreadConsolidationContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceSetMismatch => formatter
                .write_str("consolidation source refs do not exactly match requested threads"),
            Self::SourceProjectionMismatch(thread_id) => write!(
                formatter,
                "consolidation source projection does not match request for thread {thread_id}"
            ),
            Self::ForeignEvidenceRef(reference) => {
                write!(
                    formatter,
                    "consolidation returned foreign evidence ref {reference}"
                )
            }
            Self::InvalidMethodDisposition => formatter.write_str(
                "consolidation source method, native summary flag, and blockers are incoherent",
            ),
            Self::MissingConsolidationTurn => {
                formatter.write_str("successful native consolidation is missing its producer turn")
            }
            Self::ForeignCoordinatorRef(reference) => write!(
                formatter,
                "consolidation {reference} does not belong to its coordinator turn"
            ),
        }
    }
}

pub(crate) fn validate_thread_consolidation_attempt(
    params: &MemythosThreadConsolidateParams,
    attempt: &ThreadConsolidationAttempt,
) -> Result<(), ThreadConsolidationContractError> {
    let requested_sources = params
        .source_thread_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let returned_sources = attempt
        .source_refs
        .iter()
        .map(|source| source.thread_id.as_str())
        .collect::<HashSet<_>>();
    if requested_sources.len() != params.source_thread_ids.len()
        || returned_sources.len() != attempt.source_refs.len()
        || requested_sources != returned_sources
    {
        return Err(ThreadConsolidationContractError::SourceSetMismatch);
    }

    let expected_items_view = normalize_consolidation_items_view(params.items_view.as_deref());
    for source in &attempt.source_refs {
        if source.items_view != expected_items_view
            || source.cursor != params.since_cursors.get(&source.thread_id).cloned()
        {
            return Err(ThreadConsolidationContractError::SourceProjectionMismatch(
                source.thread_id.clone(),
            ));
        }
        let source_prefix = format!("app-server://threads/{}/turns/", source.thread_id);
        for reference in source
            .turn_refs
            .iter()
            .chain(source.technical_evidence_refs.iter())
            .chain(source.latest_agent_message_ref.iter())
        {
            if !reference.starts_with(&source_prefix) {
                return Err(ThreadConsolidationContractError::ForeignEvidenceRef(
                    reference.clone(),
                ));
            }
        }
        if source.latest_agent_message_ref.is_some() != source.latest_agent_message_text.is_some() {
            return Err(ThreadConsolidationContractError::SourceProjectionMismatch(
                source.thread_id.clone(),
            ));
        }
    }

    for reference in &attempt.technical_evidence_refs {
        if !params.source_thread_ids.iter().any(|thread_id| {
            reference.starts_with(&format!("app-server://threads/{thread_id}/turns/"))
        }) {
            return Err(ThreadConsolidationContractError::ForeignEvidenceRef(
                reference.clone(),
            ));
        }
    }

    match (
        attempt.source_method.as_str(),
        attempt.used_thread_turns_summary,
    ) {
        ("thread/turns/list", true) => {}
        ("record_only", false) if !attempt.blockers.is_empty() => {}
        _ => return Err(ThreadConsolidationContractError::InvalidMethodDisposition),
    }

    let Some(turn_id) = attempt.consolidation_turn_id.as_deref() else {
        if attempt.blockers.is_empty() {
            return Err(ThreadConsolidationContractError::MissingConsolidationTurn);
        }
        if attempt.agent_message_ref.is_some() || attempt.structured_output_ref.is_some() {
            return Err(ThreadConsolidationContractError::ForeignCoordinatorRef(
                "output ref",
            ));
        }
        return Ok(());
    };
    let coordinator_prefix = format!(
        "app-server://threads/{}/turns/{turn_id}/",
        params.coordinator_thread_id
    );
    for (name, reference) in [
        ("agent message ref", attempt.agent_message_ref.as_deref()),
        (
            "structured output ref",
            attempt.structured_output_ref.as_deref(),
        ),
    ] {
        if reference.is_some_and(|reference| !reference.starts_with(&coordinator_prefix)) {
            return Err(ThreadConsolidationContractError::ForeignCoordinatorRef(
                name,
            ));
        }
    }
    Ok(())
}

pub(crate) type ThreadConsolidationFuture<'a> =
    Pin<Box<dyn Future<Output = ThreadConsolidationAttempt> + Send + 'a>>;

pub(crate) trait ThreadConsolidationAdapter: Send + Sync {
    fn consolidate_threads<'a>(
        &'a self,
        params: &'a MemythosThreadConsolidateParams,
    ) -> ThreadConsolidationFuture<'a>;
}

#[derive(Clone)]
pub(crate) struct TurnStartThreadConsolidationAdapter {
    thread_processor: ThreadRequestProcessor,
    turn_processor: TurnRequestProcessor,
}

impl TurnStartThreadConsolidationAdapter {
    pub(crate) fn new(
        thread_processor: ThreadRequestProcessor,
        turn_processor: TurnRequestProcessor,
    ) -> Self {
        Self {
            thread_processor,
            turn_processor,
        }
    }
}

fn native_consolidation_items_view(items_view: &str) -> TurnItemsView {
    match items_view {
        "full" => TurnItemsView::Full,
        "notLoaded" => TurnItemsView::NotLoaded,
        _ => TurnItemsView::Summary,
    }
}

impl ThreadConsolidationAdapter for TurnStartThreadConsolidationAdapter {
    fn consolidate_threads<'a>(
        &'a self,
        params: &'a MemythosThreadConsolidateParams,
    ) -> ThreadConsolidationFuture<'a> {
        Box::pin(async move {
            let mut source_refs = Vec::with_capacity(params.source_thread_ids.len());
            let mut technical_evidence_refs = Vec::new();
            let mut blockers = Vec::new();
            let items_view = normalize_consolidation_items_view(params.items_view.as_deref());
            let native_items_view = native_consolidation_items_view(items_view);
            let per_source_limit = params.per_source_limit.unwrap_or(3).clamp(1, 10);

            for source_thread_id in &params.source_thread_ids {
                let cursor = params.since_cursors.get(source_thread_id).cloned();
                match self
                    .thread_processor
                    .thread_turns_list(ThreadTurnsListParams {
                        thread_id: source_thread_id.clone(),
                        cursor: cursor.clone(),
                        limit: Some(per_source_limit),
                        sort_direction: Some(SortDirection::Desc),
                        items_view: Some(native_items_view.clone()),
                    })
                    .await
                {
                    Ok(Some(ClientResponsePayload::ThreadTurnsList(response))) => {
                        let mut turn_refs = Vec::new();
                        let mut source_technical_evidence_refs = Vec::new();
                        let mut latest_agent_message_ref = None;
                        let mut latest_agent_message_text = None;
                        for turn in &response.data {
                            let turn_ref = format!(
                                "app-server://threads/{}/turns/{}",
                                source_thread_id, turn.id
                            );
                            turn_refs.push(turn_ref);
                            for item in &turn.items {
                                match item {
                                    ThreadItem::AgentMessage { id, text, .. } => {
                                        latest_agent_message_ref = Some(format!(
                                            "app-server://threads/{}/turns/{}/items/{}",
                                            source_thread_id, turn.id, id
                                        ));
                                        latest_agent_message_text = Some(text.clone());
                                    }
                                    ThreadItem::CollabAgentToolCall { id, .. }
                                    | ThreadItem::SubAgentActivity { id, .. } => {
                                        let evidence_ref = format!(
                                            "app-server://threads/{}/turns/{}/items/{}",
                                            source_thread_id, turn.id, id
                                        );
                                        source_technical_evidence_refs.push(evidence_ref.clone());
                                        technical_evidence_refs.push(evidence_ref);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        source_refs.push(MemythosThreadConsolidationSourceRef {
                            thread_id: source_thread_id.clone(),
                            turn_refs,
                            items_view: items_view.to_string(),
                            cursor,
                            next_cursor: response.next_cursor.or(response.backwards_cursor),
                            latest_agent_message_ref,
                            latest_agent_message_text,
                            technical_evidence_refs: compact_event_refs(
                                source_technical_evidence_refs,
                            ),
                        });
                    }
                    Ok(_) => {
                        blockers.push(format!(
                            "thread/turns/list returned no turns payload for {}",
                            source_thread_id
                        ));
                        source_refs.push(empty_consolidation_source_ref(
                            source_thread_id,
                            cursor,
                            items_view,
                        ));
                    }
                    Err(error) => {
                        blockers.push(format!(
                            "thread/turns/list failed for {}: {}",
                            source_thread_id, error.message
                        ));
                        source_refs.push(empty_consolidation_source_ref(
                            source_thread_id,
                            cursor,
                            items_view,
                        ));
                    }
                }
            }

            let context_payload = serde_json::json!({
                "purpose": params.purpose,
                "authorityMode": params.authority_mode,
                "sourceRefs": &source_refs,
                "technicalEvidenceRefs": &technical_evidence_refs,
                "instructions": params.instructions,
                "humanInstruction": false,
                "sourceMethod": "thread/turns/list",
                "itemsView": items_view
            });
            let context_text =
                serde_json::to_string_pretty(&context_payload).unwrap_or_else(|_| "{}".to_string());
            let prompt = build_thread_consolidation_prompt(params);
            let mut additional_context = HashMap::new();
            additional_context.insert(
                "memythos.thread_consolidation".to_string(),
                AdditionalContextEntry {
                    value: context_text,
                    kind: AdditionalContextKind::Application,
                },
            );
            let request_id = ConnectionRequestId {
                connection_id: ConnectionId(0),
                request_id: RequestId::String(format!(
                    "memythos-thread-consolidate:{}",
                    params
                        .client_user_message_id
                        .clone()
                        .unwrap_or_else(|| params.coordinator_thread_id.clone())
                )),
            };
            let mut metadata = HashMap::new();
            metadata.insert(
                "memythos_thread_consolidation".to_string(),
                "true".to_string(),
            );
            metadata.insert(
                "memythos_purpose".to_string(),
                format!("{:?}", params.purpose),
            );
            let turn_params = TurnStartParams {
                thread_id: params.coordinator_thread_id.clone(),
                client_user_message_id: params.client_user_message_id.clone(),
                input: vec![UserInput::Text {
                    text: prompt,
                    text_elements: vec![],
                }],
                responsesapi_client_metadata: Some(metadata),
                additional_context: Some(additional_context),
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
                output_schema: params.output_schema.clone(),
                collaboration_mode: None,
                multi_agent_mode: None,
            };

            let (consolidation_turn_id, agent_message_ref, structured_output_ref) = match self
                .turn_processor
                .turn_start(request_id, turn_params, Some("memythos".to_string()), None)
                .await
            {
                Ok(Some(ClientResponsePayload::TurnStart(response))) => {
                    let turn_id = response.turn.id;
                    let agent_message_ref =
                        response
                            .turn
                            .items
                            .iter()
                            .rev()
                            .find_map(|item| match item {
                                ThreadItem::AgentMessage { id, .. } => Some(format!(
                                    "app-server://threads/{}/turns/{}/items/{}",
                                    params.coordinator_thread_id, turn_id, id
                                )),
                                _ => None,
                            });
                    let structured_output_ref = params.output_schema.as_ref().map(|_| {
                        format!(
                            "app-server://threads/{}/turns/{}/output-schema",
                            params.coordinator_thread_id, turn_id
                        )
                    });
                    (Some(turn_id), agent_message_ref, structured_output_ref)
                }
                Ok(_) => {
                    blockers.push(
                        "turn/start returned no turn response for thread consolidation".to_string(),
                    );
                    (None, None, None)
                }
                Err(error) => {
                    blockers.push(format!(
                        "turn/start failed for thread consolidation: {}",
                        error.message
                    ));
                    (None, None, None)
                }
            };

            ThreadConsolidationAttempt {
                consolidation_turn_id,
                source_refs,
                agent_message_ref,
                structured_output_ref,
                technical_evidence_refs: compact_event_refs(technical_evidence_refs),
                source_method: "thread/turns/list".to_string(),
                used_thread_turns_summary: true,
                blockers,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::MemythosThreadConsolidationAuthorityMode;
    use codex_app_server_protocol::MemythosThreadConsolidationPurpose;

    fn params() -> MemythosThreadConsolidateParams {
        MemythosThreadConsolidateParams {
            coordinator_thread_id: "coordinator".to_string(),
            source_thread_ids: vec!["source-a".to_string()],
            since_cursors: HashMap::new(),
            items_view: Some("summary".to_string()),
            purpose: MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
            authority_mode: MemythosThreadConsolidationAuthorityMode::PeerCoordination,
            instructions: "Consolidate.".to_string(),
            per_source_limit: Some(2),
            client_user_message_id: None,
            output_schema: None,
        }
    }

    fn attempt() -> ThreadConsolidationAttempt {
        ThreadConsolidationAttempt {
            consolidation_turn_id: Some("turn-1".to_string()),
            source_refs: vec![MemythosThreadConsolidationSourceRef {
                thread_id: "source-a".to_string(),
                turn_refs: vec!["app-server://threads/source-a/turns/turn-1".to_string()],
                items_view: "summary".to_string(),
                cursor: None,
                next_cursor: Some("next".to_string()),
                latest_agent_message_ref: None,
                latest_agent_message_text: None,
                technical_evidence_refs: Vec::new(),
            }],
            agent_message_ref: Some(
                "app-server://threads/coordinator/turns/turn-1/items/message-1".to_string(),
            ),
            structured_output_ref: None,
            technical_evidence_refs: Vec::new(),
            source_method: "thread/turns/list".to_string(),
            used_thread_turns_summary: true,
            blockers: Vec::new(),
        }
    }

    #[test]
    fn contract_accepts_native_and_explicit_degraded_attempts() {
        assert_eq!(
            validate_thread_consolidation_attempt(&params(), &attempt()),
            Ok(())
        );

        let mut degraded = attempt();
        degraded.consolidation_turn_id = None;
        degraded.agent_message_ref = None;
        degraded.source_method = "record_only".to_string();
        degraded.used_thread_turns_summary = false;
        degraded.blockers = vec!["adapter unavailable".to_string()];
        assert_eq!(
            validate_thread_consolidation_attempt(&params(), &degraded),
            Ok(())
        );
    }

    #[test]
    fn contract_rejects_foreign_source_evidence() {
        let mut invalid = attempt();
        invalid.source_refs[0].turn_refs =
            vec!["app-server://threads/source-b/turns/turn-1".to_string()];
        assert!(matches!(
            validate_thread_consolidation_attempt(&params(), &invalid),
            Err(ThreadConsolidationContractError::ForeignEvidenceRef(_))
        ));
    }

    #[test]
    fn requested_items_view_maps_to_the_same_native_thread_projection() {
        assert_eq!(
            native_consolidation_items_view("summary"),
            TurnItemsView::Summary
        );
        assert_eq!(native_consolidation_items_view("full"), TurnItemsView::Full);
        assert_eq!(
            native_consolidation_items_view("notLoaded"),
            TurnItemsView::NotLoaded
        );
    }
}
