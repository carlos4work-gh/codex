use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArena;
use codex_app_server_protocol::MemythosArenaCreateParams;
use codex_app_server_protocol::MemythosArenaCreateResponse;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosArenaListParams;
use codex_app_server_protocol::MemythosArenaListResponse;
use codex_app_server_protocol::MemythosLayer;
use codex_app_server_protocol::MemythosLayerCreateParams;
use codex_app_server_protocol::MemythosLayerCreateResponse;
use codex_app_server_protocol::MemythosLayerListParams;
use codex_app_server_protocol::MemythosLayerListResponse;
use tokio::sync::Mutex;

use crate::error_code::invalid_params;

#[derive(Default)]
struct MemythosRuntimeState {
    layers: HashMap<String, MemythosLayer>,
    arenas: HashMap<String, MemythosArena>,
}

#[derive(Clone, Default)]
pub(crate) struct MemythosRequestProcessor {
    state: Arc<Mutex<MemythosRuntimeState>>,
    next_layer_id: Arc<AtomicU64>,
    next_arena_id: Arc<AtomicU64>,
}

impl MemythosRequestProcessor {
    pub(crate) fn new() -> Self {
        Self::default()
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

    fn next_id(&self, prefix: &str, counter: &AtomicU64) -> String {
        let next = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        format!("{prefix}_{next}")
    }
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
}
