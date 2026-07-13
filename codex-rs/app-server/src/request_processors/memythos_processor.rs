use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArena;
use codex_app_server_protocol::MemythosArenaCreateParams;
use codex_app_server_protocol::MemythosArenaCreateResponse;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosArenaListParams;
use codex_app_server_protocol::MemythosArenaListResponse;
use codex_app_server_protocol::MemythosEventChannel;
use codex_app_server_protocol::MemythosLayer;
use codex_app_server_protocol::MemythosLayerCreateParams;
use codex_app_server_protocol::MemythosLayerCreateResponse;
use codex_app_server_protocol::MemythosLayerListParams;
use codex_app_server_protocol::MemythosLayerListResponse;
use codex_app_server_protocol::MemythosRuntimeCloseParams;
use codex_app_server_protocol::MemythosRuntimeCloseResponse;
use codex_app_server_protocol::MemythosRuntimeHealthParams;
use codex_app_server_protocol::MemythosRuntimeHealthResponse;
use codex_app_server_protocol::MemythosRuntimeLifecycleState;
use codex_app_server_protocol::MemythosTelemetryListParams;
use codex_app_server_protocol::MemythosTelemetryListResponse;
use codex_app_server_protocol::MemythosTelemetryRef;
use codex_app_server_protocol::MemythosTelemetryRefKind;
use codex_app_server_protocol::MemythosTelemetrySource;
use codex_app_server_protocol::MemythosThreadAttachParams;
use codex_app_server_protocol::MemythosThreadAttachResponse;
use codex_app_server_protocol::MemythosThreadAttachment;
use codex_app_server_protocol::MemythosThreadListParams;
use codex_app_server_protocol::MemythosThreadListResponse;
use tokio::sync::Mutex;

use crate::error_code::invalid_params;

struct MemythosRuntimeState {
    runtime_id: String,
    lifecycle_state: MemythosRuntimeLifecycleState,
    runtime_family: String,
    connection_mode: String,
    transport_owner: String,
    transport_id: Option<String>,
    daemon_runtime_verified: bool,
    degraded_reasons: Vec<String>,
    layers: HashMap<String, MemythosLayer>,
    arenas: HashMap<String, MemythosArena>,
    thread_attachments: HashMap<String, MemythosThreadAttachment>,
    telemetry_refs: Vec<MemythosTelemetryRef>,
}

#[derive(Clone)]
pub(crate) struct MemythosRequestProcessor {
    state: Arc<Mutex<MemythosRuntimeState>>,
    next_layer_id: Arc<AtomicU64>,
    next_arena_id: Arc<AtomicU64>,
    next_attachment_id: Arc<AtomicU64>,
    next_telemetry_ref_id: Arc<AtomicU64>,
}

impl MemythosRequestProcessor {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::new_for_transport(AppServerRpcTransport::Stdio)
    }

    pub(crate) fn new_for_transport(rpc_transport: AppServerRpcTransport) -> Self {
        let (connection_mode, transport_owner, transport_id, daemon_runtime_verified) =
            match rpc_transport {
                AppServerRpcTransport::Stdio => ("stdio", "app_server", Some("stdio"), false),
                AppServerRpcTransport::Websocket => (
                    "daemon_websocket",
                    "app_server_daemon",
                    Some("websocket"),
                    true,
                ),
                AppServerRpcTransport::InProcess => (
                    "in_process",
                    "app_server_embedded",
                    Some("in_process"),
                    false,
                ),
            };

        Self {
            state: Arc::new(Mutex::new(MemythosRuntimeState {
                runtime_id: "memythos_app_server_runtime".to_string(),
                lifecycle_state: MemythosRuntimeLifecycleState::Ready,
                runtime_family: "app_server".to_string(),
                connection_mode: connection_mode.to_string(),
                transport_owner: transport_owner.to_string(),
                transport_id: transport_id.map(str::to_string),
                daemon_runtime_verified,
                degraded_reasons: Vec::new(),
                layers: HashMap::new(),
                arenas: HashMap::new(),
                thread_attachments: HashMap::new(),
                telemetry_refs: Vec::new(),
            })),
            next_layer_id: Arc::new(AtomicU64::default()),
            next_arena_id: Arc::new(AtomicU64::default()),
            next_attachment_id: Arc::new(AtomicU64::default()),
            next_telemetry_ref_id: Arc::new(AtomicU64::default()),
        }
    }

    pub(crate) async fn runtime_health(
        &self,
        _params: MemythosRuntimeHealthParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;

        Ok(MemythosRuntimeHealthResponse {
            runtime_id: state.runtime_id.clone(),
            protocol_version: "memythos.experimental.v1".to_string(),
            lifecycle_state: state.lifecycle_state,
            runtime_family: state.runtime_family.clone(),
            connection_mode: state.connection_mode.clone(),
            transport_owner: state.transport_owner.clone(),
            transport_id: state.transport_id.clone(),
            daemon_runtime_verified: state.daemon_runtime_verified,
            capabilities: vec![
                "memythos/runtime/health".to_string(),
                "memythos/runtime/close".to_string(),
                "memythos/layer/create".to_string(),
                "memythos/layer/list".to_string(),
                "memythos/arena/create".to_string(),
                "memythos/arena/list".to_string(),
                "memythos/thread/attach".to_string(),
                "memythos/thread/list".to_string(),
                "memythos/telemetry/list".to_string(),
            ],
            active_layers: state.layers.len(),
            active_arenas: state.arenas.len(),
            active_thread_attachments: state.thread_attachments.len(),
            telemetry_ref_count: state.telemetry_refs.len(),
            degraded_reasons: state.degraded_reasons.clone(),
        }
        .into())
    }

    pub(crate) async fn runtime_close(
        &self,
        params: MemythosRuntimeCloseParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if params.force {
            state.lifecycle_state = MemythosRuntimeLifecycleState::ClosedDegraded;
            state.degraded_reasons.push(
                params
                    .reason
                    .unwrap_or_else(|| "runtime was force closed by request".to_string()),
            );
        } else {
            state.lifecycle_state = MemythosRuntimeLifecycleState::ClosedCleanly;
        }
        let runtime_id = state.runtime_id.clone();
        let lifecycle_state = state.lifecycle_state;
        let degraded_reasons = state.degraded_reasons.clone();
        let closed_cleanly = lifecycle_state == MemythosRuntimeLifecycleState::ClosedCleanly;
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::RuntimeState,
            MemythosTelemetrySource::MemythosRuntimeState,
            None,
            None,
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!("Runtime closed with state {lifecycle_state:?}."),
        );

        Ok(MemythosRuntimeCloseResponse {
            runtime_id,
            lifecycle_state,
            closed_cleanly,
            degraded_reasons,
        }
        .into())
    }

    pub(crate) async fn layer_create(
        &self,
        params: MemythosLayerCreateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if let Some(parent_layer_id) = params.parent_layer_id.as_deref() {
            if !state.layers.contains_key(parent_layer_id) {
                return Err(invalid_params(format!(
                    "unknown parent layer id: {parent_layer_id}"
                )));
            }
        }

        let layer_id = self.next_id("mem_layer", &self.next_layer_id);
        let layer = MemythosLayer {
            layer_id: layer_id.clone(),
            name: params.name,
            kind: params.kind,
            parent_layer_id: params.parent_layer_id,
            objective: params.objective,
        };
        state.layers.insert(layer_id, layer.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::LayerState,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer.layer_id.clone()),
            None,
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!("Layer {} created.", layer.layer_id),
        );

        Ok(MemythosLayerCreateResponse { layer }.into())
    }

    pub(crate) async fn layer_list(
        &self,
        _params: MemythosLayerListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let mut layers: Vec<_> = state.layers.values().cloned().collect();
        layers.sort_by(|a, b| a.layer_id.cmp(&b.layer_id));

        Ok(MemythosLayerListResponse { layers }.into())
    }

    pub(crate) async fn arena_create(
        &self,
        params: MemythosArenaCreateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        if !state.layers.contains_key(&params.layer_id) {
            return Err(invalid_params(format!(
                "unknown layer id: {}",
                params.layer_id
            )));
        }

        let arena_id = self.next_id("mem_arena", &self.next_arena_id);
        let arena = MemythosArena {
            arena_id: arena_id.clone(),
            layer_id: params.layer_id,
            name: params.name,
            kind: params.kind,
            lifecycle_state: MemythosArenaLifecycleState::Draft,
            objective: params.objective,
            participant_ids: params.participant_ids,
        };
        state.arenas.insert(arena_id, arena.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ArenaState,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(arena.layer_id.clone()),
            Some(arena.arena_id.clone()),
            None,
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!("Arena {} created.", arena.arena_id),
        );

        Ok(MemythosArenaCreateResponse { arena }.into())
    }

    pub(crate) async fn arena_list(
        &self,
        params: MemythosArenaListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let mut arenas: Vec<_> = state
            .arenas
            .values()
            .filter(|arena| {
                params
                    .layer_id
                    .as_ref()
                    .map_or(true, |layer_id| &arena.layer_id == layer_id)
            })
            .cloned()
            .collect();
        arenas.sort_by(|a, b| a.arena_id.cmp(&b.arena_id));

        Ok(MemythosArenaListResponse { arenas }.into())
    }

    pub(crate) async fn thread_attach(
        &self,
        params: MemythosThreadAttachParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        let Some(arena) = state.arenas.get_mut(&params.arena_id) else {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        };

        if !arena.participant_ids.contains(&params.thread_id) {
            arena.participant_ids.push(params.thread_id.clone());
        }
        if arena.lifecycle_state == MemythosArenaLifecycleState::Draft {
            arena.lifecycle_state = MemythosArenaLifecycleState::Running;
        }
        let layer_id = arena.layer_id.clone();

        let attachment_id = self.next_id("mem_attach", &self.next_attachment_id);
        let attachment = MemythosThreadAttachment {
            attachment_id: attachment_id.clone(),
            arena_id: params.arena_id,
            thread_id: params.thread_id,
            role_id: params.role_id,
            stance_id: params.stance_id,
            objective: params.objective,
            contract_ref: params.contract_ref,
            lifecycle_state: MemythosArenaLifecycleState::Running,
        };
        state
            .thread_attachments
            .insert(attachment_id, attachment.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ThreadAttachment,
            MemythosTelemetrySource::MemythosRuntimeState,
            Some(layer_id),
            Some(attachment.arena_id.clone()),
            Some(attachment.thread_id.clone()),
            None,
            None,
            MemythosEventChannel::StateTransition,
            format!(
                "Thread {} attached to arena {}.",
                attachment.thread_id, attachment.arena_id
            ),
        );

        Ok(MemythosThreadAttachResponse { attachment }.into())
    }

    pub(crate) async fn thread_list(
        &self,
        params: MemythosThreadListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }
        let mut attachments: Vec<_> = state
            .thread_attachments
            .values()
            .filter(|attachment| attachment.arena_id == params.arena_id)
            .cloned()
            .collect();
        attachments.sort_by(|a, b| a.attachment_id.cmp(&b.attachment_id));

        Ok(MemythosThreadListResponse { attachments }.into())
    }

    pub(crate) async fn telemetry_list(
        &self,
        params: MemythosTelemetryListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let limit = params.limit.unwrap_or(100);
        let telemetry_refs = state
            .telemetry_refs
            .iter()
            .filter(|telemetry_ref| {
                params.layer_id.as_ref().map_or(true, |layer_id| {
                    telemetry_ref.layer_id.as_ref() == Some(layer_id)
                })
            })
            .filter(|telemetry_ref| {
                params.arena_id.as_ref().map_or(true, |arena_id| {
                    telemetry_ref.arena_id.as_ref() == Some(arena_id)
                })
            })
            .filter(|telemetry_ref| {
                params.thread_id.as_ref().map_or(true, |thread_id| {
                    telemetry_ref.thread_id.as_ref() == Some(thread_id)
                })
            })
            .take(limit)
            .cloned()
            .collect();

        Ok(MemythosTelemetryListResponse { telemetry_refs }.into())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn record_native_thread_event(
        &self,
        thread_id: &str,
        native_event_ref: String,
        detail_ref: Option<String>,
        channel: MemythosEventChannel,
        summary: String,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some((layer_id, arena_id)) = find_attachment_context(&state, thread_id) else {
            return false;
        };

        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::RuntimeState,
            MemythosTelemetrySource::AppServerNative,
            Some(layer_id),
            Some(arena_id),
            Some(thread_id.to_string()),
            Some(native_event_ref),
            detail_ref,
            channel,
            summary,
        );
        true
    }

    fn push_telemetry_ref(
        &self,
        state: &mut MemythosRuntimeState,
        kind: MemythosTelemetryRefKind,
        source: MemythosTelemetrySource,
        layer_id: Option<String>,
        arena_id: Option<String>,
        thread_id: Option<String>,
        native_event_ref: Option<String>,
        detail_ref: Option<String>,
        channel: MemythosEventChannel,
        summary: String,
    ) {
        let telemetry_ref_id = self.next_id("mem_tel", &self.next_telemetry_ref_id);
        state.telemetry_refs.push(MemythosTelemetryRef {
            telemetry_ref_id,
            kind,
            source,
            layer_id,
            arena_id,
            thread_id,
            native_event_ref,
            detail_ref,
            channel,
            summary: compact_summary(summary),
        });
    }

    #[cfg(test)]
    fn push_native_telemetry_ref_for_test(
        &self,
        state: &mut MemythosRuntimeState,
        kind: MemythosTelemetryRefKind,
        layer_id: Option<String>,
        arena_id: Option<String>,
        thread_id: Option<String>,
        native_event_ref: String,
        detail_ref: Option<String>,
        channel: MemythosEventChannel,
        summary: String,
    ) {
        self.push_telemetry_ref(
            state,
            kind,
            MemythosTelemetrySource::AppServerNative,
            layer_id,
            arena_id,
            thread_id,
            Some(native_event_ref),
            detail_ref,
            channel,
            summary,
        );
    }

    fn next_id(&self, prefix: &str, counter: &AtomicU64) -> String {
        let next = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        format!("{prefix}_{next}")
    }
}

const MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS: usize = 240;

fn find_attachment_context(
    state: &MemythosRuntimeState,
    thread_id: &str,
) -> Option<(String, String)> {
    let attachment = state
        .thread_attachments
        .values()
        .find(|attachment| attachment.thread_id == thread_id)?;
    let arena = state.arenas.get(&attachment.arena_id)?;
    Some((arena.layer_id.clone(), attachment.arena_id.clone()))
}

fn compact_summary(summary: String) -> String {
    let normalized = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS {
        return normalized;
    }

    let mut compacted: String = normalized
        .chars()
        .take(MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS.saturating_sub(1))
        .collect();
    compacted.push('…');
    compacted
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::MemythosArenaKind;
    use codex_app_server_protocol::MemythosLayerKind;

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
        let processor =
            MemythosRequestProcessor::new_for_transport(AppServerRpcTransport::Websocket);
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
            .filter(|telemetry_ref| {
                telemetry_ref.source == MemythosTelemetrySource::AppServerNative
            })
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
}
