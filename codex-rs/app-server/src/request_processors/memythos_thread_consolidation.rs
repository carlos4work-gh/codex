use std::collections::HashMap;
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
                        items_view: Some(TurnItemsView::Summary),
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
