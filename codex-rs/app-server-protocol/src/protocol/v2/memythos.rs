use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosLayerKind {
    Strategic,
    BpmEndToEnd,
    TacticalOperational,
    ImplementationReality,
    HumanSettlement,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosArenaKind {
    Discovery,
    Debate,
    ParentRollup,
    HumanDiscovery,
    Settlement,
    Implementation,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosArenaLifecycleState {
    Draft,
    Running,
    AwaitingParent,
    AwaitingHuman,
    ArtifactComplete,
    CloseRequested,
    ClosedCleanly,
    ClosedDegraded,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosRuntimeLifecycleState {
    Ready,
    Draining,
    ClosedCleanly,
    ClosedDegraded,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosEventChannel {
    HumanHighlight,
    TechnicalDetail,
    ArtifactPayload,
    StateTransition,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosTelemetryRefKind {
    RuntimeState,
    LayerState,
    ArenaState,
    ThreadAttachment,
    ArenaParent,
    ArenaMessage,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosTelemetrySource {
    AppServerNative,
    MemythosRuntimeState,
    SyntheticFixture,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosLayer {
    pub layer_id: String,
    pub name: String,
    pub kind: MemythosLayerKind,
    #[ts(optional = nullable)]
    pub parent_layer_id: Option<String>,
    pub objective: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArena {
    pub arena_id: String,
    pub layer_id: String,
    pub name: String,
    pub kind: MemythosArenaKind,
    pub lifecycle_state: MemythosArenaLifecycleState,
    pub objective: String,
    #[serde(default)]
    pub participant_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosThreadAttachment {
    pub attachment_id: String,
    pub arena_id: String,
    pub thread_id: String,
    #[ts(optional = nullable)]
    pub role_id: Option<String>,
    #[ts(optional = nullable)]
    pub stance_id: Option<String>,
    #[ts(optional = nullable)]
    pub objective: Option<String>,
    #[ts(optional = nullable)]
    pub contract_ref: Option<String>,
    pub lifecycle_state: MemythosArenaLifecycleState,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosTelemetryRef {
    pub telemetry_ref_id: String,
    pub kind: MemythosTelemetryRefKind,
    pub source: MemythosTelemetrySource,
    #[ts(optional = nullable)]
    pub layer_id: Option<String>,
    #[ts(optional = nullable)]
    pub arena_id: Option<String>,
    #[ts(optional = nullable)]
    pub thread_id: Option<String>,
    #[ts(optional = nullable)]
    pub native_event_ref: Option<String>,
    #[ts(optional = nullable)]
    pub detail_ref: Option<String>,
    pub channel: MemythosEventChannel,
    pub summary: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRuntimeHealthResponse {
    pub runtime_id: String,
    pub protocol_version: String,
    pub lifecycle_state: MemythosRuntimeLifecycleState,
    pub runtime_family: String,
    pub connection_mode: String,
    pub transport_owner: String,
    #[ts(optional = nullable)]
    pub transport_id: Option<String>,
    pub daemon_runtime_verified: bool,
    pub capabilities: Vec<String>,
    pub active_layers: usize,
    pub active_arenas: usize,
    pub active_thread_attachments: usize,
    pub telemetry_ref_count: usize,
    pub degraded_reasons: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRuntimeHealthParams {}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRuntimeCloseParams {
    #[serde(default)]
    pub force: bool,
    #[ts(optional = nullable)]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRuntimeCloseResponse {
    pub runtime_id: String,
    pub lifecycle_state: MemythosRuntimeLifecycleState,
    pub closed_cleanly: bool,
    pub degraded_reasons: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosLayerCreateParams {
    pub name: String,
    pub kind: MemythosLayerKind,
    #[ts(optional = nullable)]
    pub parent_layer_id: Option<String>,
    pub objective: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosLayerCreateResponse {
    pub layer: MemythosLayer,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosLayerListParams {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosLayerListResponse {
    pub layers: Vec<MemythosLayer>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaCreateParams {
    pub layer_id: String,
    pub name: String,
    pub kind: MemythosArenaKind,
    pub objective: String,
    #[serde(default)]
    pub participant_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaCreateResponse {
    pub arena: MemythosArena,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaListParams {
    #[ts(optional = nullable)]
    pub layer_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaListResponse {
    pub arenas: Vec<MemythosArena>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosThreadAttachParams {
    pub arena_id: String,
    pub thread_id: String,
    #[ts(optional = nullable)]
    pub role_id: Option<String>,
    #[ts(optional = nullable)]
    pub stance_id: Option<String>,
    #[ts(optional = nullable)]
    pub objective: Option<String>,
    #[ts(optional = nullable)]
    pub contract_ref: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosThreadAttachResponse {
    pub attachment: MemythosThreadAttachment,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosThreadListParams {
    pub arena_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosThreadListResponse {
    pub attachments: Vec<MemythosThreadAttachment>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaParent {
    pub arena_id: String,
    pub thread_id: String,
    pub parent_role: String,
    pub stance_profile: String,
    pub authority_scope: Vec<String>,
    pub lifecycle_state: MemythosArenaLifecycleState,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaParentRegisterParams {
    pub arena_id: String,
    pub thread_id: String,
    pub parent_role: String,
    pub stance_profile: String,
    #[serde(default)]
    pub authority_scope: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaParentRegisterResponse {
    pub parent: MemythosArenaParent,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessage {
    pub message_id: String,
    pub case_id: String,
    pub arena_id: String,
    pub round_id: String,
    pub from_parent_thread_id: String,
    pub from_parent_role: String,
    pub to_parent_thread_id: String,
    pub to_parent_role: String,
    pub message_kind: String,
    pub human_summary: String,
    pub context_packet_ref: String,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    pub requires_response: bool,
    #[ts(optional = nullable)]
    pub response_contract: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageDelivery {
    pub delivery_id: String,
    pub message_id: String,
    pub status: String,
    pub sender_thread_id: String,
    pub receiver_thread_id: String,
    pub arena_id: String,
    pub round_id: String,
    pub memory_replay_required: bool,
    pub event_refs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageSendParams {
    pub message: MemythosArenaMessage,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageSendResponse {
    pub delivery: MemythosArenaMessageDelivery,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageListParams {
    pub arena_id: String,
    #[ts(optional = nullable)]
    pub round_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageListResponse {
    pub deliveries: Vec<MemythosArenaMessageDelivery>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosTelemetryListParams {
    #[ts(optional = nullable)]
    pub layer_id: Option<String>,
    #[ts(optional = nullable)]
    pub arena_id: Option<String>,
    #[ts(optional = nullable)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosTelemetryListResponse {
    pub telemetry_refs: Vec<MemythosTelemetryRef>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn layer_create_params_use_camel_case_contract() {
        let params = MemythosLayerCreateParams {
            name: "BPM end-to-end".to_string(),
            kind: MemythosLayerKind::BpmEndToEnd,
            parent_layer_id: Some("strategic".to_string()),
            objective: "Protect the commercial flow end to end.".to_string(),
        };

        assert_eq!(
            serde_json::to_value(params).unwrap(),
            json!({
                "name": "BPM end-to-end",
                "kind": "bpmEndToEnd",
                "parentLayerId": "strategic",
                "objective": "Protect the commercial flow end to end."
            })
        );
    }

    #[test]
    fn arena_starts_as_runtime_lifecycle_state() {
        let arena = MemythosArena {
            arena_id: "mem_arena_1".to_string(),
            layer_id: "mem_layer_1".to_string(),
            name: "Node debate".to_string(),
            kind: MemythosArenaKind::Debate,
            lifecycle_state: MemythosArenaLifecycleState::Draft,
            objective: "Resolve the node contract.".to_string(),
            participant_ids: vec!["participant_a".to_string()],
        };

        assert_eq!(arena.lifecycle_state, MemythosArenaLifecycleState::Draft);
    }

    #[test]
    fn thread_attachment_params_use_camel_case_contract() {
        let params = MemythosThreadAttachParams {
            arena_id: "arena_1".to_string(),
            thread_id: "thread_1".to_string(),
            role_id: Some("peer".to_string()),
            stance_id: Some("skeptic".to_string()),
            objective: Some("Challenge the implementation PID.".to_string()),
            contract_ref: Some("implementation-pid.md".to_string()),
        };

        assert_eq!(
            serde_json::to_value(params).unwrap(),
            json!({
                "arenaId": "arena_1",
                "threadId": "thread_1",
                "roleId": "peer",
                "stanceId": "skeptic",
                "objective": "Challenge the implementation PID.",
                "contractRef": "implementation-pid.md"
            })
        );
    }
}
