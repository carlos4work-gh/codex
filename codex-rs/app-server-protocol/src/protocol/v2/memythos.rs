use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

use super::ThreadGoalStatus;

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

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosParentPeerResponseKind {
    PendingResponse,
    Ack,
    Question,
    Objection,
    Bet,
    RollupRequest,
    OffTopic,
    NoResponse,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosSemanticAlignment {
    Pending,
    Acceptable,
    Strong,
    Weak,
    Invalid,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum MemythosParentContinuityStatus {
    NoTurns,
    SingleTurnObserved,
    TurnContinuityObserved,
    Verified,
    Degraded,
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
    pub delivery_mechanism: String,
    #[ts(optional = nullable)]
    pub receiver_turn_id: Option<String>,
    #[ts(optional = nullable)]
    pub receiver_response_event_ref: Option<String>,
    pub delivered_as_human_instruction: bool,
    pub memory_replay_required: bool,
    pub event_refs: Vec<String>,
    #[ts(optional = nullable)]
    pub rejection_reason: Option<String>,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosParentPeerResponseObservation {
    pub observation_id: String,
    pub message_id: String,
    pub receiver_thread_id: String,
    #[ts(optional = nullable)]
    pub receiver_turn_id: Option<String>,
    #[ts(optional = nullable)]
    pub response_event_ref: Option<String>,
    pub observed_response_kind: MemythosParentPeerResponseKind,
    pub role_preserved: bool,
    pub treated_as_human_instruction: bool,
    pub semantic_alignment: MemythosSemanticAlignment,
    #[ts(optional = nullable)]
    pub actionable_next_step: Option<String>,
    pub evidence_refs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageObservationListParams {
    pub arena_id: String,
    #[ts(optional = nullable)]
    pub round_id: Option<String>,
    #[ts(optional = nullable)]
    pub message_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosArenaMessageObservationListResponse {
    pub observations: Vec<MemythosParentPeerResponseObservation>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomParticipant {
    pub parent_key: String,
    pub thread_id: String,
    pub parent_role: String,
    pub stance_profile: String,
    #[ts(optional = nullable)]
    pub goal_ref: Option<String>,
    #[serde(default)]
    pub authority_scope: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoom {
    pub room_id: String,
    pub case_id: String,
    pub layer_id: String,
    pub arena_id: String,
    pub topology: String,
    pub participants: Vec<MemythosRoomParticipant>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomRegisterParams {
    pub room_id: String,
    pub case_id: String,
    pub layer_id: String,
    pub arena_id: String,
    pub topology: String,
    #[serde(default)]
    pub participants: Vec<MemythosRoomParticipant>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomRegisterResponse {
    pub room: MemythosRoom,
    pub event_refs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityListParams {
    pub room_id: String,
    #[ts(optional = nullable)]
    pub round_id: Option<String>,
    #[ts(optional = nullable)]
    pub since_cursor: Option<String>,
    #[serde(default)]
    #[ts(optional = nullable)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub include_debug_refs: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityParticipant {
    pub parent_key: String,
    pub thread_id: String,
    pub parent_role: String,
    pub stance_profile: String,
    pub status: String,
    #[ts(optional = nullable)]
    pub goal_ref: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityItem {
    pub kind: String,
    pub status: String,
    pub summary: String,
    #[ts(optional = nullable)]
    pub text: Option<String>,
    pub event_ref: String,
    #[serde(default)]
    pub refs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityTurn {
    pub thread_id: String,
    pub turn_id: String,
    pub status: String,
    pub items: Vec<MemythosRoomActivityItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityLifecycle {
    pub room_state: String,
    pub active_turns: usize,
    pub completed_turns: usize,
    pub failed_turns: usize,
    pub clean_close: bool,
    pub force_closed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityCollab {
    pub send_input_count: usize,
    pub completed_send_input_count: usize,
    pub failed_send_input_count: usize,
    pub wait_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivitySubagents {
    pub activity_count: usize,
    pub started_count: usize,
    pub interacted_count: usize,
    pub interrupted_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityUsage {
    pub token_usage_events: usize,
    pub refs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomActivityListResponse {
    pub room_id: String,
    pub case_id: String,
    pub layer_id: String,
    pub arena_id: String,
    #[ts(optional = nullable)]
    pub round_id: Option<String>,
    #[ts(optional = nullable)]
    pub cursor: Option<String>,
    pub source_method: String,
    pub participants: Vec<MemythosRoomActivityParticipant>,
    pub turns: Vec<MemythosRoomActivityTurn>,
    pub lifecycle: MemythosRoomActivityLifecycle,
    pub collab: MemythosRoomActivityCollab,
    pub subagents: MemythosRoomActivitySubagents,
    pub usage: MemythosRoomActivityUsage,
    pub blockers: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomSendInputParams {
    pub room_id: String,
    pub room_message_ref: String,
    pub delivery_ref: String,
    #[ts(optional = nullable)]
    pub from_parent_thread_id: Option<String>,
    #[ts(optional = nullable)]
    pub via_concierge_thread_id: Option<String>,
    pub to_parent_thread_id: String,
    pub source_parent_key: String,
    pub target_parent_key: String,
    pub message_kind: String,
    pub message_authority: String,
    pub human_instruction: bool,
    pub response_contract: String,
    #[ts(optional = nullable)]
    pub client_user_message_id: Option<String>,
    pub prompt: String,
    #[serde(default)]
    #[ts(type = "Record<string, unknown>")]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    #[ts(type = "unknown")]
    pub output_schema: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomSendInputDelivery {
    pub thread_id: String,
    pub turn_id: String,
    pub event_refs: Vec<String>,
    pub room_id: String,
    pub room_message_ref: String,
    pub delivery_ref: String,
    pub delivery_mechanism: String,
    pub human_instruction: bool,
    pub message_authority: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosRoomSendInputResponse {
    pub delivery: MemythosRoomSendInputDelivery,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosParentThreadContinuity {
    pub arena_id: String,
    pub thread_id: String,
    pub parent_role: String,
    pub stance_profile: String,
    pub continuity_status: MemythosParentContinuityStatus,
    pub first_turn_id: Option<String>,
    pub latest_turn_id: Option<String>,
    pub observed_turn_count: usize,
    pub memory_replay_required: bool,
    pub goal_snapshot_available: bool,
    #[ts(optional = nullable)]
    pub goal_snapshot_ref: Option<String>,
    #[ts(optional = nullable)]
    pub budget_state_ref: Option<String>,
    #[ts(optional = nullable)]
    pub goal_status: Option<ThreadGoalStatus>,
    #[ts(optional = nullable)]
    pub token_budget: Option<i64>,
    #[ts(optional = nullable)]
    pub tokens_used: Option<i64>,
    #[ts(optional = nullable)]
    pub time_used_seconds: Option<i64>,
    #[ts(optional = nullable)]
    pub latest_turn_completed_ref: Option<String>,
    #[ts(optional = nullable)]
    pub token_usage_ref: Option<String>,
    pub evidence_refs: Vec<String>,
    pub degraded_reasons: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosParentContinuityListParams {
    pub arena_id: String,
    #[ts(optional = nullable)]
    pub thread_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct MemythosParentContinuityListResponse {
    pub continuities: Vec<MemythosParentThreadContinuity>,
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
