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
use codex_app_server_protocol::ThreadResumeParams;
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
use sha2::Digest;
use sha2::Sha256;

use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::request_processors::ThreadRequestProcessor;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerParentDeliveryContractError {
    MissingField(&'static str),
    RejectedWithReceiverTurn,
    AcceptedResponseWithoutReceiverTurn,
    ResponseWithoutReceiverTurn,
}

impl std::fmt::Display for PeerParentDeliveryContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(formatter, "delivery attempt is missing {field}"),
            Self::RejectedWithReceiverTurn => {
                formatter.write_str("rejected delivery attempt cannot expose a receiver turn")
            }
            Self::AcceptedResponseWithoutReceiverTurn => formatter.write_str(
                "accepted response-required delivery attempt must expose a receiver turn",
            ),
            Self::ResponseWithoutReceiverTurn => formatter.write_str(
                "delivery attempt cannot expose a response event without a receiver turn",
            ),
        }
    }
}

pub(crate) fn validate_peer_parent_delivery_attempt(
    message: &MemythosArenaMessage,
    attempt: &PeerParentDeliveryAttempt,
) -> Result<(), PeerParentDeliveryContractError> {
    for (field, value) in [
        ("status", attempt.status.as_str()),
        ("delivery mechanism", attempt.delivery_mechanism.as_str()),
        ("telemetry summary", attempt.telemetry_summary.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(PeerParentDeliveryContractError::MissingField(field));
        }
    }
    if attempt.event_refs.is_empty() {
        return Err(PeerParentDeliveryContractError::MissingField(
            "evidence reference",
        ));
    }
    if attempt.rejection_reason.is_some() && attempt.receiver_turn_id.is_some() {
        return Err(PeerParentDeliveryContractError::RejectedWithReceiverTurn);
    }
    if attempt.rejection_reason.is_none()
        && message.requires_response
        && attempt.receiver_turn_id.is_none()
    {
        return Err(PeerParentDeliveryContractError::AcceptedResponseWithoutReceiverTurn);
    }
    if attempt.receiver_response_event_ref.is_some() && attempt.receiver_turn_id.is_none() {
        return Err(PeerParentDeliveryContractError::ResponseWithoutReceiverTurn);
    }
    Ok(())
}

pub(crate) type PeerParentDeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = PeerParentDeliveryAttempt> + Send + 'a>>;
pub(crate) type NativeMailboxReenqueueFuture<'a> =
    Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;
pub(crate) type PeerParentStageFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<PeerParentStagedEffect>, String>> + Send + 'a>>;

#[derive(Debug, Clone)]
pub(crate) struct PeerParentStagedEffect {
    pub(super) communication_id: String,
    pub(super) source_call_id: String,
    pub(super) receiver_thread_id: String,
    pub(super) payload_hash: String,
}

pub(crate) trait PeerParentDeliveryAdapter: Send + Sync {
    fn stage_peer_parent_message<'a>(
        &'a self,
        _message: &'a MemythosArenaMessage,
    ) -> PeerParentStageFuture<'a> {
        Box::pin(async { Ok(None) })
    }

    fn deliver_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a>;

    fn activate_staged_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        _effect: &'a PeerParentStagedEffect,
        reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a> {
        self.deliver_peer_parent_message(message, reasoning_effort, connection_id)
    }

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
    thread_processor: ThreadRequestProcessor,
    turn_processor: TurnRequestProcessor,
    thread_manager: Arc<ThreadManager>,
    state_db: Option<StateDbHandle>,
}

impl NativeMailboxPeerParentDeliveryAdapter {
    pub(crate) fn new(
        thread_processor: ThreadRequestProcessor,
        turn_processor: TurnRequestProcessor,
        thread_manager: Arc<ThreadManager>,
        state_db: Option<StateDbHandle>,
    ) -> Self {
        Self {
            thread_processor,
            turn_processor,
            thread_manager,
            state_db,
        }
    }
}

impl PeerParentDeliveryAdapter for NativeMailboxPeerParentDeliveryAdapter {
    fn stage_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
    ) -> PeerParentStageFuture<'a> {
        Box::pin(async move {
            if message.from_parent_role == "human" {
                return Ok(None);
            }
            let (sender_thread_id, target_thread_id, communication) =
                prepare_native_parent_mailbox_message(&self.thread_manager, message).await?;
            let communication_json =
                serde_json::to_string(&communication).map_err(|error| error.to_string())?;
            self.thread_manager
                .stage_inter_agent_communication(target_thread_id, &communication)
                .await
                .map_err(|error| error.to_string())?;
            let communication_id = communication
                .id
                .as_ref()
                .expect("native Arena communication has a stable id")
                .to_string();
            let _ = sender_thread_id;
            Ok(Some(PeerParentStagedEffect {
                source_call_id: communication_id.clone(),
                communication_id,
                receiver_thread_id: target_thread_id.to_string(),
                payload_hash: format!("sha256:{:x}", Sha256::digest(communication_json.as_bytes())),
            }))
        })
    }

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

    fn activate_staged_peer_parent_message<'a>(
        &'a self,
        message: &'a MemythosArenaMessage,
        effect: &'a PeerParentStagedEffect,
        _reasoning_effort: Option<ReasoningEffort>,
        connection_id: ConnectionId,
    ) -> PeerParentDeliveryFuture<'a> {
        Box::pin(async move {
            let sender_thread_id = match ThreadId::from_string(&message.from_parent_thread_id) {
                Ok(thread_id) => thread_id,
                Err(error) => {
                    return failed_native_mailbox_delivery_attempt(
                        message,
                        &format!("invalid source parent thread id: {error}"),
                    );
                }
            };
            let target_thread_id = match ThreadId::from_string(&effect.receiver_thread_id) {
                Ok(thread_id) => thread_id,
                Err(error) => {
                    return failed_native_mailbox_delivery_attempt(
                        message,
                        &format!("invalid target parent thread id: {error}"),
                    );
                }
            };
            if self
                .thread_manager
                .get_thread(target_thread_id)
                .await
                .is_err()
            {
                let request_id = ConnectionRequestId {
                    connection_id,
                    request_id: RequestId::String(format!(
                        "memythos-mailbox-recovery:{}",
                        effect.communication_id
                    )),
                };
                if let Err(error) = self
                    .thread_processor
                    .thread_resume(
                        request_id,
                        ThreadResumeParams {
                            thread_id: effect.receiver_thread_id.clone(),
                            history: None,
                            path: None,
                            model: None,
                            model_provider: None,
                            service_tier: None,
                            cwd: None,
                            runtime_workspace_roots: None,
                            approval_policy: None,
                            approvals_reviewer: None,
                            sandbox: None,
                            permissions: None,
                            config: None,
                            base_instructions: None,
                            developer_instructions: None,
                            personality: None,
                            exclude_turns: true,
                            initial_turns_page: None,
                        },
                        Some("memythos-recovery".to_string()),
                        None,
                        Default::default(),
                    )
                    .await
                {
                    return failed_native_mailbox_delivery_attempt(
                        message,
                        &format!("failed to resume native receiver thread: {}", error.message),
                    );
                }
            }
            match self
                .thread_manager
                .activate_staged_inter_agent_communication_by_id(
                    sender_thread_id,
                    target_thread_id,
                    &effect.communication_id,
                )
                .await
            {
                Ok((submission_id, communication)) => successful_native_mailbox_delivery_attempt(
                    message,
                    submission_id,
                    communication.trigger_turn,
                ),
                Err(error) => failed_native_mailbox_delivery_attempt(message, &error.to_string()),
            }
        })
    }
}

async fn deliver_native_parent_mailbox_message(
    thread_manager: &ThreadManager,
    message: &MemythosArenaMessage,
) -> PeerParentDeliveryAttempt {
    let (sender_thread_id, target_thread_id, communication) =
        match prepare_native_parent_mailbox_message(thread_manager, message).await {
            Ok(prepared) => prepared,
            Err(error) => return failed_native_mailbox_delivery_attempt(message, &error),
        };
    let trigger_turn = communication.trigger_turn;
    match thread_manager
        .send_inter_agent_communication(sender_thread_id, target_thread_id, communication)
        .await
    {
        Ok(submission_id) => {
            successful_native_mailbox_delivery_attempt(message, submission_id, trigger_turn)
        }
        Err(error) => failed_native_mailbox_delivery_attempt(message, &error.to_string()),
    }
}

async fn prepare_native_parent_mailbox_message(
    thread_manager: &ThreadManager,
    message: &MemythosArenaMessage,
) -> Result<(ThreadId, ThreadId, InterAgentCommunication), String> {
    let target_thread_id = ThreadId::from_string(&message.to_parent_thread_id)
        .map_err(|error| format!("invalid target parent thread id: {error}"))?;
    let target_thread = thread_manager
        .get_thread(target_thread_id)
        .await
        .map_err(|error| error.to_string())?;
    let target_status = target_thread.agent_status().await;
    let trigger_turn = match native_mailbox_wake_policy(&target_status, message.requires_response) {
        Ok(trigger_turn) => trigger_turn,
        Err(reason) => return Err(reason),
    };
    let mut communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        build_peer_parent_envelope(message),
        trigger_turn,
    );
    communication.id = Some(ResponseItemId::from_server(message.message_id.clone()));
    let sender_thread_id = ThreadId::from_string(&message.from_parent_thread_id)
        .map_err(|error| format!("invalid source parent thread id: {error}"))?;
    Ok((sender_thread_id, target_thread_id, communication))
}

fn successful_native_mailbox_delivery_attempt(
    message: &MemythosArenaMessage,
    submission_id: String,
    trigger_turn: bool,
) -> PeerParentDeliveryAttempt {
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
            format!(
                "memythos://arenas/{}/rounds/{}/messages/{}",
                message.arena_id, message.round_id, message.message_id
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::MemythosArenaDeliveryPolicy;

    fn message(requires_response: bool) -> MemythosArenaMessage {
        MemythosArenaMessage {
            message_id: "message-1".to_string(),
            case_id: "case-1".to_string(),
            arena_id: "arena-1".to_string(),
            round_id: "round-1".to_string(),
            from_parent_thread_id: "sender".to_string(),
            from_parent_role: "bettor".to_string(),
            to_parent_thread_id: "receiver".to_string(),
            to_parent_role: "judge".to_string(),
            message_kind: "peer_bet".to_string(),
            human_summary: "bet".to_string(),
            execution_prompt: None,
            context_packet_ref: "context://round-1".to_string(),
            artifact_refs: Vec::new(),
            requires_response,
            delivery_policy: Some(if requires_response {
                MemythosArenaDeliveryPolicy::Immediate
            } else {
                MemythosArenaDeliveryPolicy::QueueOnly
            }),
            aggregate_contract: None,
            response_contract: None,
            output_schema: None,
        }
    }

    #[tokio::test]
    async fn record_only_adapter_satisfies_rejected_delivery_contract() {
        let message = message(true);
        let attempt = RecordOnlyPeerParentDeliveryAdapter
            .deliver_peer_parent_message(&message, None, ConnectionId(0))
            .await;

        assert_eq!(
            validate_peer_parent_delivery_attempt(&message, &attempt),
            Ok(())
        );
    }

    #[test]
    fn native_delivery_shapes_satisfy_the_shared_contract() {
        let response_message = message(true);
        let accepted = PeerParentDeliveryAttempt {
            status: "delivered_to_native_mailbox_turn".to_string(),
            delivery_mechanism: "native_mailbox_trigger_turn".to_string(),
            receiver_turn_id: Some("turn-1".to_string()),
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: vec!["app-server://threads/receiver/mailbox/turn-1".to_string()],
            rejection_reason: None,
            telemetry_channel: MemythosEventChannel::StateTransition,
            telemetry_summary: "native delivery accepted".to_string(),
        };
        assert_eq!(
            validate_peer_parent_delivery_attempt(&response_message, &accepted),
            Ok(())
        );

        let queue_message = message(false);
        let queued = PeerParentDeliveryAttempt {
            receiver_turn_id: None,
            status: "queued_in_native_mailbox".to_string(),
            delivery_mechanism: "native_mailbox_queue_only".to_string(),
            telemetry_summary: "native delivery queued".to_string(),
            ..accepted
        };
        assert_eq!(
            validate_peer_parent_delivery_attempt(&queue_message, &queued),
            Ok(())
        );
    }

    #[test]
    fn shared_contract_rejects_accepted_response_without_turn() {
        let message = message(true);
        let attempt = PeerParentDeliveryAttempt {
            status: "delivered".to_string(),
            delivery_mechanism: "invalid_adapter".to_string(),
            receiver_turn_id: None,
            receiver_response_event_ref: None,
            delivered_as_human_instruction: false,
            memory_replay_required: false,
            event_refs: vec!["app-server://delivery/1".to_string()],
            rejection_reason: None,
            telemetry_channel: MemythosEventChannel::TechnicalDetail,
            telemetry_summary: "invalid accepted delivery".to_string(),
        };

        assert_eq!(
            validate_peer_parent_delivery_attempt(&message, &attempt),
            Err(PeerParentDeliveryContractError::AcceptedResponseWithoutReceiverTurn)
        );
    }
}
