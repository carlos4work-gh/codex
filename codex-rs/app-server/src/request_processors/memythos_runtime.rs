use codex_app_server_protocol::MemythosArena;
use codex_app_server_protocol::MemythosArenaCreateParams;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosLayer;
use codex_app_server_protocol::MemythosLayerCreateParams;
use codex_app_server_protocol::MemythosRuntimeCloseParams;
use codex_app_server_protocol::MemythosRuntimeCloseResponse;
use codex_app_server_protocol::MemythosRuntimeHealthResponse;
use codex_app_server_protocol::MemythosRuntimeLifecycleState;

use crate::error_code::invalid_params;
use crate::request_processors::memythos_arena_state::NativeArenaState;
use crate::request_processors::memythos_runtime_state::MemythosRuntimeState;
use codex_app_server_protocol::JSONRPCErrorError;

const RUNTIME_CAPABILITIES: &[&str] = &[
    "memythos/runtime/health",
    "memythos/runtime/close",
    "memythos/layer/create",
    "memythos/layer/list",
    "memythos/arena/create",
    "memythos/arena/composition/provision",
    "memythos/arena/list",
    "memythos/thread/attach",
    "memythos/thread/list",
    "memythos/arena/parent/register",
    "memythos/arena/participant/register",
    "memythos/arena/phase/start",
    "memythos/arena/message",
    "memythos/arena/message/send",
    "memythos/arena/message/list",
    "memythos/arena/message/observe",
    "memythos/room/register",
    "memythos/room/create",
    "memythos/room/list",
    "memythos/room/activity/list",
    "memythos/room/timeline/get",
    "memythos/room/sendInput",
    "memythos/room/send",
    "memythos/thread/consolidate",
    "memythos/thread/contract/assemble",
    "memythos/room/contract/emit",
    "memythos/thread/contract/read",
    "memythos/room/contract/get",
    "memythos/thread/contract/list",
    "memythos/arena/state/get",
    "memythos/arena/phase/close",
    "memythos/arena/run",
    "memythos/telemetry/list",
];

pub(super) fn runtime_health_response(
    state: &MemythosRuntimeState,
) -> MemythosRuntimeHealthResponse {
    MemythosRuntimeHealthResponse {
        runtime_id: state.runtime_id.clone(),
        protocol_version: "memythos.experimental.v1".to_string(),
        lifecycle_state: state.lifecycle_state,
        runtime_family: state.runtime_family.clone(),
        connection_mode: state.connection_mode.clone(),
        transport_owner: state.transport_owner.clone(),
        transport_id: state.transport_id.clone(),
        daemon_runtime_verified: state.daemon_runtime_verified,
        capabilities: RUNTIME_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_string())
            .collect(),
        active_layers: state.layers.len(),
        active_arenas: state.arenas.len(),
        active_thread_attachments: state.thread_attachments.len(),
        telemetry_ref_count: state.telemetry_refs.len(),
        degraded_reasons: state.degraded_reasons.clone(),
    }
}

pub(super) fn close_runtime(
    state: &mut MemythosRuntimeState,
    params: MemythosRuntimeCloseParams,
) -> MemythosRuntimeCloseResponse {
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
    let lifecycle_state = state.lifecycle_state;
    MemythosRuntimeCloseResponse {
        runtime_id: state.runtime_id.clone(),
        lifecycle_state,
        closed_cleanly: lifecycle_state == MemythosRuntimeLifecycleState::ClosedCleanly,
        degraded_reasons: state.degraded_reasons.clone(),
    }
}

pub(super) fn create_layer(
    state: &mut MemythosRuntimeState,
    params: MemythosLayerCreateParams,
    layer_id: String,
) -> Result<MemythosLayer, JSONRPCErrorError> {
    if let Some(parent_layer_id) = params.parent_layer_id.as_deref()
        && !state.layers.contains_key(parent_layer_id)
    {
        return Err(invalid_params(format!(
            "unknown parent layer id: {parent_layer_id}"
        )));
    }
    let layer = MemythosLayer {
        layer_id: layer_id.clone(),
        name: params.name,
        kind: params.kind,
        parent_layer_id: params.parent_layer_id,
        objective: params.objective,
    };
    state.layers.insert(layer_id, layer.clone());
    Ok(layer)
}

pub(super) fn sorted_layers(state: &MemythosRuntimeState) -> Vec<MemythosLayer> {
    let mut layers: Vec<_> = state.layers.values().cloned().collect();
    layers.sort_by(|a, b| a.layer_id.cmp(&b.layer_id));
    layers
}

pub(super) fn create_arena(
    state: &mut MemythosRuntimeState,
    params: MemythosArenaCreateParams,
    arena_id: String,
) -> Result<MemythosArena, JSONRPCErrorError> {
    if !state.layers.contains_key(&params.layer_id) {
        return Err(invalid_params(format!(
            "unknown layer id: {}",
            params.layer_id
        )));
    }
    let arena = MemythosArena {
        arena_id: arena_id.clone(),
        layer_id: params.layer_id,
        name: params.name,
        kind: params.kind,
        lifecycle_state: MemythosArenaLifecycleState::Draft,
        objective: params.objective,
        participant_ids: params.participant_ids,
    };
    state.arena_lifecycles.insert(
        arena_id.clone(),
        NativeArenaState::new(arena_id.clone()).map_err(|error| {
            invalid_params(format!("failed to initialize native arena state: {error}"))
        })?,
    );
    state.arenas.insert(arena_id, arena.clone());
    Ok(arena)
}

pub(super) fn sorted_arenas(
    state: &MemythosRuntimeState,
    layer_id: Option<&str>,
) -> Vec<MemythosArena> {
    let mut arenas: Vec<_> = state
        .arenas
        .values()
        .filter(|arena| layer_id.is_none_or(|layer_id| arena.layer_id == layer_id))
        .cloned()
        .collect();
    arenas.sort_by(|a, b| a.arena_id.cmp(&b.arena_id));
    arenas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_capabilities_keep_native_control_surface() {
        assert!(RUNTIME_CAPABILITIES.contains(&"memythos/runtime/health"));
        assert!(RUNTIME_CAPABILITIES.contains(&"memythos/room/sendInput"));
        assert!(RUNTIME_CAPABILITIES.contains(&"memythos/arena/state/get"));
        assert_eq!(RUNTIME_CAPABILITIES.len(), 33);
    }
}
