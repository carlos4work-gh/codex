use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;

use crate::request_processors::ThreadRequestProcessor;

#[derive(Debug, Clone)]
pub(crate) struct ParentTurnResponse {
    pub(crate) status: Option<TurnStatus>,
    pub(crate) request_item_ref: Option<String>,
    pub(crate) request_text: Option<String>,
    pub(crate) item_ref: Option<String>,
    pub(crate) text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParentTurnResponseContractError {
    PartialRequest,
    PartialResponse,
    ForeignItemRef(String),
}

impl std::fmt::Display for ParentTurnResponseContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartialRequest => {
                formatter.write_str("request ref and text must be present together")
            }
            Self::PartialResponse => {
                formatter.write_str("response ref and text must be present together")
            }
            Self::ForeignItemRef(item_ref) => {
                write!(
                    formatter,
                    "item ref does not belong to requested turn: {item_ref}"
                )
            }
        }
    }
}

pub(crate) fn validate_parent_turn_response(
    thread_id: &str,
    turn_id: &str,
    response: &ParentTurnResponse,
) -> Result<(), ParentTurnResponseContractError> {
    if response.request_item_ref.is_some() != response.request_text.is_some() {
        return Err(ParentTurnResponseContractError::PartialRequest);
    }
    if response.item_ref.is_some() != response.text.is_some() {
        return Err(ParentTurnResponseContractError::PartialResponse);
    }
    let prefix = format!("app-server://threads/{thread_id}/turns/{turn_id}/items/");
    for item_ref in [
        response.request_item_ref.as_ref(),
        response.item_ref.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if !item_ref.starts_with(&prefix) {
            return Err(ParentTurnResponseContractError::ForeignItemRef(
                item_ref.clone(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_parent_turn_responses(
    responses: &HashMap<(String, String), ParentTurnResponse>,
) -> Result<(), ParentTurnResponseContractError> {
    for ((thread_id, turn_id), response) in responses {
        validate_parent_turn_response(thread_id, turn_id, response)?;
    }
    Ok(())
}

fn parent_turn_response(
    thread_id: &str,
    turn: &codex_app_server_protocol::Turn,
) -> ParentTurnResponse {
    let request = turn.items.iter().find_map(|item| match item {
        ThreadItem::UserMessage { id, content, .. } => {
            let text = content
                .iter()
                .filter_map(|input| match input {
                    UserInput::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then(|| {
                (
                    format!(
                        "app-server://threads/{thread_id}/turns/{}/items/{id}",
                        turn.id
                    ),
                    text,
                )
            })
        }
        _ => None,
    });
    let response = turn.items.iter().rev().find_map(|item| match item {
        ThreadItem::AgentMessage { id, text, .. } => Some((
            format!(
                "app-server://threads/{thread_id}/turns/{}/items/{id}",
                turn.id
            ),
            text.clone(),
        )),
        _ => None,
    });
    ParentTurnResponse {
        status: Some(turn.status.clone()),
        request_item_ref: request.as_ref().map(|(item_ref, _)| item_ref.clone()),
        request_text: request.map(|(_, text)| text),
        item_ref: response.as_ref().map(|(item_ref, _)| item_ref.clone()),
        text: response.map(|(_, text)| text),
    }
}

pub(crate) type ParentTurnResponseFuture<'a> =
    Pin<Box<dyn Future<Output = ParentTurnResponse> + Send + 'a>>;
pub(crate) type ParentTurnResponsesFuture<'a> =
    Pin<Box<dyn Future<Output = HashMap<(String, String), ParentTurnResponse>> + Send + 'a>>;

pub(crate) trait ParentTurnResponseAdapter: Send + Sync {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a>;

    fn read_responses<'a>(&'a self, turns: Vec<(String, String)>) -> ParentTurnResponsesFuture<'a> {
        Box::pin(async move {
            let mut responses = HashMap::new();
            for (thread_id, turn_id) in turns {
                let response = self.read_response(&thread_id, &turn_id).await;
                responses.insert((thread_id, turn_id), response);
            }
            responses
        })
    }
}
#[derive(Clone)]
pub(crate) struct ThreadTurnsParentResponseAdapter {
    thread_processor: ThreadRequestProcessor,
}

impl ThreadTurnsParentResponseAdapter {
    pub(crate) fn new(thread_processor: ThreadRequestProcessor) -> Self {
        Self { thread_processor }
    }
}

impl ParentTurnResponseAdapter for ThreadTurnsParentResponseAdapter {
    fn read_response<'a>(
        &'a self,
        thread_id: &'a str,
        turn_id: &'a str,
    ) -> ParentTurnResponseFuture<'a> {
        Box::pin(async move {
            let Ok(true) = self
                .thread_processor
                .turn_terminal_observed(thread_id, turn_id)
                .await
            else {
                return ParentTurnResponse {
                    status: None,
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            };
            let response = self
                .thread_processor
                .thread_turns_list(ThreadTurnsListParams {
                    thread_id: thread_id.to_string(),
                    cursor: None,
                    limit: Some(10),
                    sort_direction: Some(SortDirection::Desc),
                    items_view: Some(TurnItemsView::Full),
                })
                .await;
            let Ok(Some(ClientResponsePayload::ThreadTurnsList(response))) = response else {
                return ParentTurnResponse {
                    status: None,
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            };
            let Some(turn) = response.data.iter().find(|turn| turn.id == turn_id) else {
                return ParentTurnResponse {
                    status: None,
                    request_item_ref: None,
                    request_text: None,
                    item_ref: None,
                    text: None,
                };
            };
            parent_turn_response(thread_id, turn)
        })
    }

    fn read_responses<'a>(&'a self, turns: Vec<(String, String)>) -> ParentTurnResponsesFuture<'a> {
        Box::pin(async move {
            let mut requested_by_thread = HashMap::<String, HashSet<String>>::new();
            for (thread_id, turn_id) in turns {
                requested_by_thread
                    .entry(thread_id)
                    .or_default()
                    .insert(turn_id);
            }

            let mut responses = HashMap::new();
            for (thread_id, mut requested_turn_ids) in requested_by_thread {
                let mut cursor = None;
                while !requested_turn_ids.is_empty() {
                    let response = self
                        .thread_processor
                        .thread_turns_list(ThreadTurnsListParams {
                            thread_id: thread_id.clone(),
                            cursor: cursor.clone(),
                            limit: Some(100),
                            sort_direction: Some(SortDirection::Desc),
                            items_view: Some(TurnItemsView::Full),
                        })
                        .await;
                    let Ok(Some(ClientResponsePayload::ThreadTurnsList(page))) = response else {
                        break;
                    };

                    for turn in page.data {
                        if !requested_turn_ids.remove(&turn.id) {
                            continue;
                        }
                        let response = parent_turn_response(&thread_id, &turn);
                        responses.insert((thread_id.clone(), turn.id), response);
                    }

                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    if cursor.as_deref() == Some(next_cursor.as_str()) {
                        break;
                    }
                    cursor = Some(next_cursor);
                }
            }
            responses
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoResponseAdapter;

    impl ParentTurnResponseAdapter for EchoResponseAdapter {
        fn read_response<'a>(
            &'a self,
            thread_id: &'a str,
            turn_id: &'a str,
        ) -> ParentTurnResponseFuture<'a> {
            Box::pin(async move {
                ParentTurnResponse {
                    status: None,
                    request_item_ref: Some(format!(
                        "app-server://threads/{thread_id}/turns/{turn_id}/items/request"
                    )),
                    request_text: Some(format!("{thread_id}:{turn_id}")),
                    item_ref: None,
                    text: None,
                }
            })
        }
    }

    #[tokio::test]
    async fn default_batch_reader_preserves_requested_turn_keys() {
        let responses = EchoResponseAdapter
            .read_responses(vec![("thread-a".to_string(), "turn-1".to_string())])
            .await;

        assert_eq!(
            responses
                .get(&("thread-a".to_string(), "turn-1".to_string()))
                .and_then(|response| response.request_text.as_deref()),
            Some("thread-a:turn-1")
        );
        assert_eq!(validate_parent_turn_responses(&responses), Ok(()));
    }

    #[test]
    fn contract_rejects_partial_or_foreign_response_evidence() {
        let partial = ParentTurnResponse {
            status: Some(TurnStatus::Completed),
            request_item_ref: None,
            request_text: None,
            item_ref: None,
            text: Some("answer".to_string()),
        };
        assert_eq!(
            validate_parent_turn_response("thread-a", "turn-1", &partial),
            Err(ParentTurnResponseContractError::PartialResponse)
        );

        let foreign = ParentTurnResponse {
            item_ref: Some("app-server://threads/thread-b/turns/turn-2/items/answer".to_string()),
            text: Some("answer".to_string()),
            ..partial
        };
        assert!(matches!(
            validate_parent_turn_response("thread-a", "turn-1", &foreign),
            Err(ParentTurnResponseContractError::ForeignItemRef(_))
        ));
    }
}
