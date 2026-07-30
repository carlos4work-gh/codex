use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_core::CodexThread;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem as CoreDynamicToolCallOutputContentItem;
use codex_protocol::dynamic_tools::DynamicToolResponse as CoreDynamicToolResponse;
use codex_protocol::protocol::Op;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::error;

use crate::outgoing_message::ClientRequestResult;
use crate::request_processors::MemythosRequestProcessor;
use crate::request_processors::MemythosRoomToolSendMessageArgs;
use crate::server_request_error::is_turn_transition_server_request_error;

pub(crate) async fn on_call_response(
    call_id: String,
    receiver: oneshot::Receiver<ClientRequestResult>,
    conversation: Arc<CodexThread>,
) {
    let response = receiver.await;
    let (response, _error) = match response {
        Ok(Ok(value)) => decode_response(value),
        Ok(Err(err)) if is_turn_transition_server_request_error(&err) => return,
        Ok(Err(err)) => {
            error!("request failed with client error: {err:?}");
            fallback_response("dynamic tool request failed")
        }
        Err(err) => {
            error!("request failed: {err:?}");
            fallback_response("dynamic tool request failed")
        }
    };

    submit_response(call_id, response, conversation).await;
}

pub(crate) async fn on_memythos_room_call(
    call_id: String,
    current_thread_id: String,
    tool: String,
    arguments: serde_json::Value,
    processor: MemythosRequestProcessor,
    conversation: Arc<CodexThread>,
) {
    let result = match tool.as_str() {
        "list_participants" => processor
            .room_tool_list_participants(&current_thread_id)
            .await
            .and_then(|participants| {
                serde_json::to_string(&participants)
                    .map_err(|error| crate::error_code::invalid_params(error.to_string()))
            }),
        "send_message" => {
            match serde_json::from_value::<MemythosRoomToolSendMessageArgs>(arguments) {
                Ok(args) => processor
                    .room_tool_send_message(&current_thread_id, args)
                    .await
                    .and_then(|response| {
                        serde_json::to_string(&response)
                            .map_err(|error| crate::error_code::invalid_params(error.to_string()))
                    }),
                Err(error) => Err(crate::error_code::invalid_params(format!(
                    "invalid memythos_room.send_message arguments: {error}"
                ))),
            }
        }
        _ => Err(crate::error_code::invalid_params(format!(
            "unknown memythos_room tool: {tool}"
        ))),
    };
    let response = match result {
        Ok(text) => DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText { text }],
            success: true,
        },
        Err(error) => DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: error.message,
            }],
            success: false,
        },
    };
    submit_response(call_id, response, conversation).await;
}

async fn submit_response(
    call_id: String,
    response: DynamicToolCallResponse,
    conversation: Arc<CodexThread>,
) {
    let DynamicToolCallResponse {
        content_items,
        success,
    } = response;
    let core_response = CoreDynamicToolResponse {
        content_items: content_items
            .into_iter()
            .map(CoreDynamicToolCallOutputContentItem::from)
            .collect(),
        success,
    };
    if let Err(err) = conversation
        .submit(Op::DynamicToolResponse {
            id: call_id.clone(),
            response: core_response,
        })
        .await
    {
        error!("failed to submit DynamicToolResponse: {err}");
    }
}

fn decode_response(value: serde_json::Value) -> (DynamicToolCallResponse, Option<String>) {
    match serde_json::from_value::<DynamicToolCallResponse>(value) {
        Ok(response) => (response, None),
        Err(err) => {
            error!("failed to deserialize DynamicToolCallResponse: {err}");
            fallback_response("dynamic tool response was invalid")
        }
    }
}

fn fallback_response(message: &str) -> (DynamicToolCallResponse, Option<String>) {
    (
        DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: message.to_string(),
            }],
            success: false,
        },
        Some(message.to_string()),
    )
}
