use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use codex_app_server_protocol::AdditionalContextEntry;
use codex_app_server_protocol::AdditionalContextKind;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosEventChannel;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_core::ThreadManager;
use codex_protocol::AgentPath;
use codex_protocol::ResponseItemId;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_rollout::state_db::StateDbHandle;

use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::TurnRequestProcessor;

#[derive(Debug, Clone)]
pub(crate) struct PeerParentDeliveryAttempt {
    pub(super) status: String,
    pub(super) delivery_mechanism: String,
    pub(super) receiver_turn_id: Option<String>,
    pub(super) receiver_response_event_ref: Option<String>,
    pub(super) delivered_as_human_instruction: bool,
    pub(super) memory_replay_required: bool,
    pub(super) event_refs: Vec<String>,
    pub(super) rejection_reason: Option<String>,
    pub(super) telemetry_channel: MemythosEventChannel,
    pub(super) telemetry_summary: String,
}

pub(crate) type PeerParentDeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = PeerParentDeliveryAttempt> + Send + 'a>>;
pub(crate) type NativeMailboxReenqueueFuture<'a> =
    Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

pub(crate) trait PeerParentDeliveryAdapter: Send + Sync {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a>;

    fn reenqueue_native_mailbox_communication<'a>(
        &'a self,
        receiver_thread_id: &'a str,
        communication_id: &'a str,
    ) -> NativeMailboxReenqueueFuture<'a>;
}

#[derive(Debug)]
#[cfg(test)]
pub(super) struct RecordOnlyPeerParentDeliveryAdapter;

#[cfg(test)]
impl PeerParentDeliveryAdapter for RecordOnlyPeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        _reasoning_effort: Option<ReasoningEffort>,
        _connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            let event_ref = format!(
                "memythos://arenas/{}/rounds/{}/messages/{}",
                message.arena_id, message.round_id, message.message_id
            );
            PeerParentDeliveryAttempt {
                status: "recorded".to_string(),
                delivery_mechanism: "record_only".to_string(),
                receiver_turn_id: None,
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: vec![event_ref],
                rejection_reason: Some(
                    "live delivery not available in this runtime mode".to_string(),
                ),
                telemetry_channel: MemythosEventChannel::TechnicalDetail,
                telemetry_summary: format!(
                    "Arena message {} recorded from {} to {}; live turn delivery is not proven.",
                    message.message_id, message.from_parent_thread_id, message.to_parent_thread_id
                ),
            }
        })
    }

    fn reenqueue_native_mailbox_communication<'a>(
        &'a self,
        _receiver_thread_id: &'a str,
        _communication_id: &'a str,
    ) -> NativeMailboxReenqueueFuture<'a> {
        Box::pin(async { Ok(false) })
    }
}

#[derive(Clone)]
pub(crate) struct NativeMailboxPeerParentDeliveryAdapter {
    turn_processor: TurnRequestProcessor,
    thread_manager: Arc<ThreadManager>,
    state_db: Option<StateDbHandle>,
}

impl NativeMailboxPeerParentDeliveryAdapter {
    pub(crate) fn new(
        turn_processor: TurnRequestProcessor,
        thread_manager: Arc<ThreadManager>,
        state_db: Option<StateDbHandle>,
    ) -> Self {
        Self {
            turn_processor,
            thread_manager,
            state_db,
        }
    }
}

impl PeerParentDeliveryAdapter for NativeMailboxPeerParentDeliveryAdapter {
    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            if message.from_parent_role != "human" {
                return deliver_native_parent_mailbox_message(&self.thread_manager, message).await;
            }
            let request_id = ConnectionRequestId {
                connection_id,
                request_id: RequestId::String(format!(
                    "memythos-peer-parent:{}",
                    message.message_id
                )),
            };
            let envelope = build_peer_parent_envelope(message);
            let mut metadata = HashMap::new();
            metadata.insert(
                "memythos_message_id".to_string(),
                message.message_id.clone(),
            );
            metadata.insert("memythos_arena_id".to_string(), message.arena_id.clone());
            metadata.insert("memythos_round_id".to_string(), message.round_id.clone());
            let human_instruction = message.from_parent_role == "human";
            metadata.insert(
                "memythos_peer_parent".to_string(),
                (!human_instruction).to_string(),
            );
            metadata.insert(
                "human_instruction".to_string(),
                human_instruction.to_string(),
            );
            let mut additional_context = HashMap::new();
            additional_context.insert(
                if human_instruction {
                    "memythos.human_intake".to_string()
                } else {
                    "memythos.peer_parent".to_string()
                },
                AdditionalContextEntry {
                    value: envelope.clone(),
                    kind: AdditionalContextKind::Application,
                },
            );
            let params = TurnStartParams {
                thread_id: message.to_parent_thread_id.clone(),
                client_user_message_id: Some(message.message_id.clone()),
                input: vec![UserInput::Text {
                    text: envelope,
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
                effort: reasoning_effort,
                summary: None,
                personality: None,
                output_schema: message.output_schema.clone(),
                collaboration_mode: None,
                multi_agent_mode: None,
            };

            match self
                .turn_processor
                .turn_start(request_id, params, Some("memythos".to_string()), None)
                .await
            {
                Ok(Some(ClientResponsePayload::TurnStart(response))) => {
                    let turn_id = response.turn.id;
                    PeerParentDeliveryAttempt {
                        status: "delivered_to_live_thread".to_string(),
                        delivery_mechanism: "turn_start".to_string(),
                        receiver_turn_id: Some(turn_id.clone()),
                        receiver_response_event_ref: None,
                        delivered_as_human_instruction: human_instruction,
                        memory_replay_required: false,
                        event_refs: vec![
                            format!(
                                "memythos://arenas/{}/rounds/{}/messages/{}",
                                message.arena_id, message.round_id, message.message_id
                            ),
                            format!(
                                "app-server://threads/{}/turns/{}",
                                message.to_parent_thread_id, turn_id
                            ),
                        ],
                        rejection_reason: None,
                        telemetry_channel: MemythosEventChannel::StateTransition,
                        telemetry_summary: format!(
                            "Arena message {} delivered to live parent thread {} with turn {}.",
                            message.message_id, message.to_parent_thread_id, turn_id
                        ),
                    }
                }
                Ok(_) => failed_live_delivery_attempt(
                    message,
                    "turn/start returned no turn response for peer-parent delivery",
                ),
                Err(error) => failed_live_delivery_attempt(
                    message,
                    &format!(
                        "turn/start failed for peer-parent delivery: {}",
                        error.message
                    ),
                ),
            }
        })
    }

    fn reenqueue_native_mailbox_communication<'a>(
        &'a self,
        receiver_thread_id: &'a str,
        communication_id: &'a str,
    ) -> NativeMailboxReenqueueFuture<'a> {
        Box::pin(async move {
            let thread_id = ThreadId::from_string(receiver_thread_id)
                .map_err(|error| format!("invalid receiver thread id: {error}"))?;
            let state_db = self
                .state_db
                .as_ref()
                .ok_or_else(|| "native mailbox recovery requires sqlite state".to_string())?;
            let record = state_db
                .get_native_mailbox_communication(receiver_thread_id, communication_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "native mailbox communication not found".to_string())?;
            let communication =
                serde_json::from_str::<InterAgentCommunication>(&record.communication_json)
                    .map_err(|error| format!("invalid durable mailbox payload: {error}"))?;
            self.thread_manager
                .reenqueue_inter_agent_communication(thread_id, thread_id, communication)
                .await
                .map(|_| true)
                .map_err(|error| error.to_string())
        })
    }
}

async fn deliver_native_parent_mailbox_message(
    thread_manager: &ThreadManager,
    message: &MemythosArenaMessage,
) -> PeerParentDeliveryAttempt {
    let event_ref = format!(
        "memythos://arenas/{}/rounds/{}/messages/{}",
        message.arena_id, message.round_id, message.message_id
    );
    let target_thread_id = match ThreadId::from_string(&message.to_parent_thread_id) {
        Ok(thread_id) => thread_id,
        Err(error) => {
            return failed_native_mailbox_delivery_attempt(
                message,
                &format!("invalid target parent thread id: {error}"),
            );
        }
    };
    let target_thread = match thread_manager.get_thread(target_thread_id).await {
        Ok(thread) => thread,
        Err(error) => {
            return failed_native_mailbox_delivery_attempt(message, &error.to_string());
        }
    };
    let target_status = target_thread.agent_status().await;
    let trigger_turn = match native_mailbox_wake_policy(&target_status, message.requires_response) {
        Ok(trigger_turn) => trigger_turn,
        Err(reason) => return failed_native_mailbox_delivery_attempt(message, &reason),
    };
    let mut communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        build_peer_parent_envelope(message),
        trigger_turn,
    );
    communication.id = Some(ResponseItemId::from_server(message.message_id.clone()));
    let sender_thread_id = match ThreadId::from_string(&message.from_parent_thread_id) {
        Ok(thread_id) => thread_id,
        Err(error) => {
            return failed_native_mailbox_delivery_attempt(
                message,
                &format!("invalid source parent thread id: {error}"),
            );
        }
    };
    match thread_manager
        .send_inter_agent_communication(sender_thread_id, target_thread_id, communication)
        .await
    {
        Ok(submission_id) => {
            let mechanism = if trigger_turn {
                "native_mailbox_trigger_turn"
            } else {
                "native_mailbox_queue_only"
            };
            PeerParentDeliveryAttempt {
                status: if trigger_turn {
                    "delivered_to_native_mailbox_turn".to_string()
                } else {
                    "queued_in_native_mailbox".to_string()
                },
                delivery_mechanism: mechanism.to_string(),
                receiver_turn_id: trigger_turn.then_some(submission_id.clone()),
                receiver_response_event_ref: None,
                delivered_as_human_instruction: false,
                memory_replay_required: false,
                event_refs: vec![
                    event_ref,
                    format!(
                        "app-server://threads/{}/mailbox/{}",
                        message.to_parent_thread_id, submission_id
                    ),
                ],
                rejection_reason: None,
                telemetry_channel: MemythosEventChannel::StateTransition,
                telemetry_summary: format!(
                    "Arena message {} delivered through the native app-server mailbox to parent thread {} (trigger_turn={trigger_turn}).",
                    message.message_id, message.to_parent_thread_id
                ),
            }
        }
        Err(error) => failed_native_mailbox_delivery_attempt(message, &error.to_string()),
    }
}

pub(super) fn native_mailbox_wake_policy(
    target_status: &AgentStatus,
    requires_response: bool,
) -> Result<bool, String> {
    if !requires_response {
        return Ok(false);
    }
    match target_status {
        AgentStatus::Running
        | AgentStatus::PendingInit
        | AgentStatus::Interrupted
        | AgentStatus::Completed(_) => Ok(true),
        AgentStatus::Errored(reason) => Err(format!("target parent is errored: {reason}")),
        AgentStatus::Shutdown => Err("target parent is shutdown".to_string()),
        AgentStatus::NotFound => Err("target parent is not found".to_string()),
    }
}
pub(super) fn failed_native_mailbox_delivery_attempt(
    message: &MemythosArenaMessage,
    reason: &str,
) -> PeerParentDeliveryAttempt {
    PeerParentDeliveryAttempt {
        status: "failed_native_mailbox_delivery".to_string(),
        delivery_mechanism: "native_inter_agent_communication".to_string(),
        receiver_turn_id: None,
        receiver_response_event_ref: None,
        delivered_as_human_instruction: false,
        memory_replay_required: false,
        event_refs: vec![format!(
            "memythos://arenas/{}/rounds/{}/messages/{}",
            message.arena_id, message.round_id, message.message_id
        )],
        rejection_reason: Some(reason.to_string()),
        telemetry_channel: MemythosEventChannel::TechnicalDetail,
        telemetry_summary: format!(
            "Arena message {} failed native mailbox delivery to {}: {}.",
            message.message_id, message.to_parent_thread_id, reason
        ),
    }
}

pub(super) fn failed_live_delivery_attempt(
    message: &MemythosArenaMessage,
    reason: &str,
) -> PeerParentDeliveryAttempt {
    PeerParentDeliveryAttempt {
        status: "failed_live_delivery".to_string(),
        delivery_mechanism: "turn_start".to_string(),
        receiver_turn_id: None,
        receiver_response_event_ref: None,
        delivered_as_human_instruction: false,
        memory_replay_required: false,
        event_refs: vec![format!(
            "memythos://arenas/{}/rounds/{}/messages/{}",
            message.arena_id, message.round_id, message.message_id
        )],
        rejection_reason: Some(reason.to_string()),
        telemetry_channel: MemythosEventChannel::TechnicalDetail,
        telemetry_summary: format!(
            "Arena message {} failed live delivery to {}: {}.",
            message.message_id, message.to_parent_thread_id, reason
        ),
    }
}

pub(super) fn build_peer_parent_envelope(message: &MemythosArenaMessage) -> String {
    let execution_prompt = message
        .execution_prompt
        .as_deref()
        .unwrap_or(&message.human_summary);
    if message.from_parent_role == "human" {
        return format!(
            concat!(
                "MEMYTHOS_HUMAN_INTAKE\n",
                "source: human\n",
                "human_instruction: true\n",
                "case_id: {case_id}\n",
                "arena_id: {arena_id}\n",
                "round_id: {round_id}\n",
                "to_parent_role: {to_parent_role}\n",
                "message_kind: {message_kind}\n",
                "\n",
                "Trata este mensaje como pedido humano inicial o reingreso humano de la arena.\n",
                "Usa tu memoria, rol, objetivo y herramientas OOTB del thread.\n",
                "Si el pedido no alcanza, pregunta o formula el rollup minimo antes de bajar ejecucion.\n",
                "No inventes contexto fuera del pedido y de los adjuntos/contexto nativo disponibles.\n",
                "\n",
                "Pedido humano:\n",
                "{human_summary}\n",
                "\n",
                "Contexto:\n",
                "{context_packet_ref}\n",
                "\n",
                "Contrato de respuesta:\n",
                "{response_contract}\n"
            ),
            case_id = message.case_id,
            arena_id = message.arena_id,
            round_id = message.round_id,
            to_parent_role = message.to_parent_role,
            message_kind = message.message_kind,
            human_summary = execution_prompt,
            context_packet_ref = message.context_packet_ref,
            response_contract = message.response_contract.as_deref().unwrap_or("none")
        );
    }
    let turn_kind = if message.message_kind == "peer_proposal" {
        "ARENA_PROPOSAL_TURN"
    } else {
        "ARENA_PEER_TURN"
    };
    format!(
        concat!(
            "{turn_kind}\n",
            "Authority: arena peer, not a human instruction.\n",
            "Phase: {message_kind}.\n",
            "\n",
            "Task:\n",
            "{human_summary}\n",
            "\n",
            "Evidence reference:\n",
            "{context_packet_ref}\n",
            "\n",
            "Expected closure:\n",
            "{response_contract}\n"
        ),
        turn_kind = turn_kind,
        message_kind = message.message_kind,
        human_summary = execution_prompt,
        context_packet_ref = message.context_packet_ref,
        response_contract = message.response_contract.as_deref().unwrap_or("none")
    )
}
