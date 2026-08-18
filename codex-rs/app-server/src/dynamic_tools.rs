use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_core::CodexThread;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem as CoreDynamicToolCallOutputContentItem;
use codex_protocol::dynamic_tools::DynamicToolResponse as CoreDynamicToolResponse;
use codex_protocol::protocol::Op;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::error;

use crate::image_url::REMOTE_IMAGE_URL_ERROR;
use crate::image_url::is_remote_image_url;
use crate::outgoing_message::ClientRequestResult;
use crate::request_processors::MemythosRequestProcessor;
use crate::request_processors::MemythosRoomToolSendMessageArgs;
use crate::request_processors::MemythosRoomToolSendToRoomArgs;
use crate::server_request_error::is_turn_transition_server_request_error;

const INVALID_AUDIO_URL_ERROR: &str = "audio URLs must use an inline data URL";

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
        "list_rooms" => processor
            .room_tool_list_rooms(&current_thread_id)
            .await
            .and_then(|rooms| {
                serde_json::to_string(&rooms)
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
        "send_to_room" => {
            match serde_json::from_value::<MemythosRoomToolSendToRoomArgs>(arguments) {
                Ok(args) => processor
                    .room_tool_send_to_room(&current_thread_id, args)
                    .await
                    .and_then(|response| {
                        serde_json::to_string(&response)
                            .map_err(|error| crate::error_code::invalid_params(error.to_string()))
                    }),
                Err(error) => Err(crate::error_code::invalid_params(format!(
                    "invalid memythos_room.send_to_room arguments: {error}"
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
        Ok(response)
            if response.content_items.iter().any(|item| {
                matches!(
                    item,
                    DynamicToolCallOutputContentItem::InputImage { image_url }
                        if is_remote_image_url(image_url)
                )
            }) =>
        {
            error!(
                message = REMOTE_IMAGE_URL_ERROR,
                "dynamic tool response was invalid"
            );
            fallback_response(REMOTE_IMAGE_URL_ERROR)
        }
        Ok(response)
            if response.content_items.iter().any(|item| {
                matches!(
                    item,
                    DynamicToolCallOutputContentItem::InputAudio { audio_url }
                        if !audio_url
                            .get(.."data:".len())
                            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
                )
            }) =>
        {
            error!(
                message = INVALID_AUDIO_URL_ERROR,
                "dynamic tool response was invalid"
            );
            fallback_response(INVALID_AUDIO_URL_ERROR)
        }
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
