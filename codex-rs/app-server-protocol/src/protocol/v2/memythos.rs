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
pub enum MemythosEventChannel {
    HumanHighlight,
    TechnicalDetail,
    ArtifactPayload,
    StateTransition,
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
}
