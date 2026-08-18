use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentRoleListParams {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum AgentRoleOrigin {
    BuiltIn,
    UserConfigured,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentRolePlannerCapabilities {
    pub work_modes: Vec<String>,
    pub problem_classes: Vec<String>,
    pub authority_scopes: Vec<String>,
    pub participant_kinds: Vec<String>,
    pub required_companions: Vec<String>,
    pub incompatible_roles: Vec<String>,
    pub allowed_stances: Vec<String>,
    pub relative_cost: Option<String>,
    pub relative_tool_use: Option<String>,
    pub supports_multiple_stances: bool,
    pub proposal_bearing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentRoleListEntry {
    pub id: String,
    pub description: Option<String>,
    pub origin: AgentRoleOrigin,
    pub has_locked_runtime_settings: bool,
    pub planner_capabilities: Option<AgentRolePlannerCapabilities>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentRoleListResponse {
    pub roles: Vec<AgentRoleListEntry>,
}
