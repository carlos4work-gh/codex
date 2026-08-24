use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

use codex_app_server_protocol::MemythosArenaLifecycleState;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

pub(crate) const ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
const LEGACY_ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const ARENA_PROTOCOL_V1_TO_V2_MIGRATION_ID: &str = "arena_protocol_v1_to_v2_operations";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NativeArenaStatus {
    Draft,
    Active,
    PhaseComplete,
    AwaitingParent,
    ClosedCleanly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeArenaProtocolSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) arena_id: String,
    pub(crate) status: NativeArenaStatus,
    pub(crate) active_round_id: Option<String>,
    pub(crate) active_phase: Option<String>,
    pub(crate) completed_phases: Vec<NativeArenaCompletedPhaseSnapshot>,
    pub(crate) sequence: u64,
    #[serde(default)]
    pub(crate) operations: Vec<NativeArenaOperationSnapshot>,
    #[serde(default)]
    pub(crate) migration: Option<NativeArenaMigrationSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeArenaMigrationSnapshot {
    pub(crate) source_schema_version: u32,
    pub(crate) target_schema_version: u32,
    pub(crate) migration_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeArenaOperationSnapshot {
    pub(crate) operation_id: String,
    pub(crate) causation_id: Option<String>,
    pub(crate) composition_version: u32,
    pub(crate) command_kind: String,
    pub(crate) round_id: Option<String>,
    pub(crate) intent_sha256: String,
    pub(crate) event_sequence: u64,
    pub(crate) event_kind: ArenaEventKind,
    pub(crate) event_round_id: Option<String>,
    pub(crate) event_phase: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeArenaCompletedPhaseSnapshot {
    pub(crate) round_id: String,
    pub(crate) phase: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeArenaState {
    arena_id: String,
    status: NativeArenaStatus,
    active_round_id: Option<String>,
    active_phase: Option<String>,
    completed_phases: HashSet<(String, String)>,
    sequence: u64,
    operations: HashMap<String, NativeArenaOperationSnapshot>,
    migration: Option<NativeArenaMigrationSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArenaOperationIdentity {
    pub(crate) operation_id: String,
    pub(crate) causation_id: Option<String>,
    pub(crate) composition_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArenaCommand {
    Activate,
    StartPhase { round_id: String, phase: String },
    ClosePhase { round_id: String, phase: String },
    AwaitParent,
    CloseCleanly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArenaEventKind {
    Activated,
    ActivationRetained,
    PhaseStarted,
    PhaseStartRetained,
    PhaseClosed,
    PhaseCloseRetained,
    AwaitingParent,
    ClosedCleanly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArenaEvent {
    pub(crate) sequence: u64,
    pub(crate) kind: ArenaEventKind,
    pub(crate) arena_id: String,
    pub(crate) round_id: Option<String>,
    pub(crate) phase: Option<String>,
}

impl ArenaEvent {
    pub(crate) fn action(&self) -> &'static str {
        match self.kind {
            ArenaEventKind::Activated => "activated",
            ArenaEventKind::ActivationRetained => "activation-retained",
            ArenaEventKind::PhaseStarted => "started",
            ArenaEventKind::PhaseStartRetained => "start-retained",
            ArenaEventKind::PhaseClosed => "closed",
            ArenaEventKind::PhaseCloseRetained => "close-retained",
            ArenaEventKind::AwaitingParent => "awaiting-parent",
            ArenaEventKind::ClosedCleanly => "closed-cleanly",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArenaDomainError {
    message: String,
}

impl ArenaDomainError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ArenaDomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ArenaDomainError {}

impl NativeArenaState {
    pub(crate) fn new(arena_id: impl Into<String>) -> Result<Self, ArenaDomainError> {
        let arena_id = arena_id.into();
        if arena_id.trim().is_empty() {
            return Err(ArenaDomainError::new("arena state requires arena_id"));
        }
        Ok(Self {
            arena_id,
            status: NativeArenaStatus::Draft,
            active_round_id: None,
            active_phase: None,
            completed_phases: HashSet::new(),
            sequence: 0,
            operations: HashMap::new(),
            migration: None,
        })
    }

    pub(crate) fn transition_with_operation(
        &mut self,
        identity: ArenaOperationIdentity,
        command: ArenaCommand,
    ) -> Result<ArenaEvent, ArenaDomainError> {
        validate_operation_identity(&identity)?;
        let intent_sha256 = command_intent_sha256(&self.arena_id, &identity, &command);
        if let Some(recorded) = self.operations.get(&identity.operation_id) {
            if recorded.intent_sha256 != intent_sha256 {
                return Err(ArenaDomainError::new(format!(
                    "operation {} conflicts with its recorded arena command intent",
                    identity.operation_id
                )));
            }
            return Ok(ArenaEvent {
                sequence: recorded.event_sequence,
                kind: recorded.event_kind,
                arena_id: self.arena_id.clone(),
                round_id: recorded.event_round_id.clone(),
                phase: recorded.event_phase.clone(),
            });
        }

        let mut next = self.clone();
        let event = next.transition(command.clone())?;
        let (command_kind, round_id) = command_identity(&command);
        next.operations.insert(
            identity.operation_id.clone(),
            NativeArenaOperationSnapshot {
                operation_id: identity.operation_id,
                causation_id: identity.causation_id,
                composition_version: identity.composition_version,
                command_kind: command_kind.to_string(),
                round_id,
                intent_sha256,
                event_sequence: event.sequence,
                event_kind: event.kind,
                event_round_id: event.round_id.clone(),
                event_phase: event.phase.clone(),
            },
        );
        next.validate_invariants()?;
        *self = next;
        Ok(event)
    }

    pub(crate) fn transition(
        &mut self,
        command: ArenaCommand,
    ) -> Result<ArenaEvent, ArenaDomainError> {
        let (kind, round_id, phase) = self.decide(&command)?;
        let mut next = self.clone();
        next.apply(kind, round_id.as_deref(), phase.as_deref());
        next.sequence += 1;
        next.validate_invariants()?;
        let event = ArenaEvent {
            sequence: next.sequence,
            kind,
            arena_id: next.arena_id.clone(),
            round_id,
            phase,
        };
        *self = next;
        Ok(event)
    }

    pub(crate) fn protocol_state(&self) -> MemythosArenaLifecycleState {
        match self.status {
            NativeArenaStatus::Draft => MemythosArenaLifecycleState::Draft,
            NativeArenaStatus::Active => MemythosArenaLifecycleState::Running,
            NativeArenaStatus::PhaseComplete => MemythosArenaLifecycleState::ArtifactComplete,
            NativeArenaStatus::AwaitingParent => MemythosArenaLifecycleState::AwaitingParent,
            NativeArenaStatus::ClosedCleanly => MemythosArenaLifecycleState::ClosedCleanly,
        }
    }

    pub(crate) fn protocol_snapshot(&self) -> NativeArenaProtocolSnapshot {
        let mut completed_phases = self
            .completed_phases
            .iter()
            .map(|(round_id, phase)| NativeArenaCompletedPhaseSnapshot {
                round_id: round_id.clone(),
                phase: phase.clone(),
            })
            .collect::<Vec<_>>();
        completed_phases.sort();
        let mut operations = self.operations.values().cloned().collect::<Vec<_>>();
        operations.sort();
        NativeArenaProtocolSnapshot {
            schema_version: ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION,
            arena_id: self.arena_id.clone(),
            status: self.status,
            active_round_id: self.active_round_id.clone(),
            active_phase: self.active_phase.clone(),
            completed_phases,
            sequence: self.sequence,
            operations,
            migration: self.migration.clone(),
        }
    }

    pub(crate) fn restore_protocol_snapshot(
        snapshot: NativeArenaProtocolSnapshot,
    ) -> Result<Self, ArenaDomainError> {
        if !matches!(
            snapshot.schema_version,
            LEGACY_ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION | ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION
        ) {
            return Err(ArenaDomainError::new(format!(
                "unsupported arena protocol snapshot schema {}; expected {}",
                snapshot.schema_version, ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION
            )));
        }
        if snapshot.arena_id.trim().is_empty() {
            return Err(ArenaDomainError::new(
                "arena protocol snapshot requires arena_id",
            ));
        }
        match (&snapshot.active_round_id, &snapshot.active_phase) {
            (Some(round_id), Some(phase)) => validate_round_and_phase(round_id, phase)?,
            (None, None) => {}
            _ => {
                return Err(ArenaDomainError::new(
                    "arena protocol snapshot requires active round and phase together",
                ));
            }
        }
        if matches!(
            snapshot.status,
            NativeArenaStatus::AwaitingParent | NativeArenaStatus::ClosedCleanly
        ) && snapshot.active_round_id.is_some()
        {
            return Err(ArenaDomainError::new(
                "terminal or awaiting-parent arena snapshot cannot retain an active phase",
            ));
        }

        let mut completed_phases = HashSet::new();
        for completed_phase in snapshot.completed_phases {
            validate_round_and_phase(&completed_phase.round_id, &completed_phase.phase)?;
            let completed = (completed_phase.round_id, completed_phase.phase);
            if snapshot.active_round_id.as_deref() == Some(completed.0.as_str())
                && snapshot.active_phase.as_deref() == Some(completed.1.as_str())
            {
                return Err(ArenaDomainError::new(
                    "arena protocol snapshot cannot mark its active phase complete",
                ));
            }
            if !completed_phases.insert(completed) {
                return Err(ArenaDomainError::new(
                    "arena protocol snapshot contains a duplicate completed phase",
                ));
            }
        }

        let mut operations = HashMap::new();
        for operation in snapshot.operations {
            validate_operation_snapshot(&operation, snapshot.sequence)?;
            if operations
                .insert(operation.operation_id.clone(), operation)
                .is_some()
            {
                return Err(ArenaDomainError::new(
                    "arena protocol snapshot contains a duplicate operation id",
                ));
            }
        }

        let migration = if snapshot.schema_version == LEGACY_ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION
        {
            if snapshot.migration.is_some() {
                return Err(ArenaDomainError::new(
                    "legacy arena protocol snapshot cannot contain migration evidence",
                ));
            }
            Some(NativeArenaMigrationSnapshot {
                source_schema_version: LEGACY_ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION,
                target_schema_version: ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION,
                migration_id: ARENA_PROTOCOL_V1_TO_V2_MIGRATION_ID.to_string(),
            })
        } else {
            validate_migration_snapshot(snapshot.migration.as_ref())?;
            snapshot.migration
        };

        let restored = Self {
            arena_id: snapshot.arena_id,
            status: snapshot.status,
            active_round_id: snapshot.active_round_id,
            active_phase: snapshot.active_phase,
            completed_phases,
            sequence: snapshot.sequence,
            operations,
            migration,
        };
        restored.validate_invariants()?;
        Ok(restored)
    }

    #[cfg(test)]
    fn active_round_id(&self) -> Option<&str> {
        self.active_round_id.as_deref()
    }

    #[cfg(test)]
    fn active_phase(&self) -> Option<&str> {
        self.active_phase.as_deref()
    }

    fn decide(
        &self,
        command: &ArenaCommand,
    ) -> Result<(ArenaEventKind, Option<String>, Option<String>), ArenaDomainError> {
        if matches!(self.status, NativeArenaStatus::ClosedCleanly) {
            return Err(ArenaDomainError::new(format!(
                "arena {} is terminal and rejects further commands",
                self.arena_id
            )));
        }
        match command {
            ArenaCommand::Activate => {
                let kind = if self.status == NativeArenaStatus::Active {
                    ArenaEventKind::ActivationRetained
                } else {
                    ArenaEventKind::Activated
                };
                Ok((kind, None, None))
            }
            ArenaCommand::StartPhase { round_id, phase } => {
                validate_round_and_phase(round_id, phase)?;
                if self.active_round_id.as_deref() == Some(round_id)
                    && self.active_phase.as_deref() == Some(phase)
                {
                    return Ok((
                        ArenaEventKind::PhaseStartRetained,
                        Some(round_id.clone()),
                        Some(phase.clone()),
                    ));
                }
                if let (Some(active_round), Some(active_phase)) =
                    (&self.active_round_id, &self.active_phase)
                {
                    return Err(ArenaDomainError::new(format!(
                        "arena {} cannot start {round_id}/{phase}; phase {active_round}/{active_phase} is active",
                        self.arena_id
                    )));
                }
                Ok((
                    ArenaEventKind::PhaseStarted,
                    Some(round_id.clone()),
                    Some(phase.clone()),
                ))
            }
            ArenaCommand::ClosePhase { round_id, phase } => {
                validate_round_and_phase(round_id, phase)?;
                if self
                    .completed_phases
                    .contains(&(round_id.clone(), phase.clone()))
                {
                    return Ok((
                        ArenaEventKind::PhaseCloseRetained,
                        Some(round_id.clone()),
                        Some(phase.clone()),
                    ));
                }
                if self.active_round_id.as_deref() != Some(round_id)
                    || self.active_phase.as_deref() != Some(phase)
                {
                    return Err(ArenaDomainError::new(format!(
                        "arena {} cannot close inactive phase {round_id}/{phase}",
                        self.arena_id
                    )));
                }
                Ok((
                    ArenaEventKind::PhaseClosed,
                    Some(round_id.clone()),
                    Some(phase.clone()),
                ))
            }
            ArenaCommand::CloseCleanly => Ok((ArenaEventKind::ClosedCleanly, None, None)),
            ArenaCommand::AwaitParent => Ok((ArenaEventKind::AwaitingParent, None, None)),
        }
    }

    fn validate_invariants(&self) -> Result<(), ArenaDomainError> {
        let has_active_phase = self.active_round_id.is_some() || self.active_phase.is_some();
        if has_active_phase && !matches!(self.status, NativeArenaStatus::Active) {
            return Err(ArenaDomainError::new(format!(
                "arena {} status {:?} cannot retain an active phase",
                self.arena_id, self.status
            )));
        }
        if self.active_round_id.is_some() != self.active_phase.is_some() {
            return Err(ArenaDomainError::new(format!(
                "arena {} requires active round and phase together",
                self.arena_id
            )));
        }
        if let (Some(round_id), Some(phase)) = (&self.active_round_id, &self.active_phase)
            && self
                .completed_phases
                .contains(&(round_id.clone(), phase.clone()))
        {
            return Err(ArenaDomainError::new(format!(
                "arena {} cannot keep completed phase {round_id}/{phase} active",
                self.arena_id
            )));
        }
        Ok(())
    }

    fn apply(&mut self, kind: ArenaEventKind, round_id: Option<&str>, phase: Option<&str>) {
        match kind {
            ArenaEventKind::Activated => self.status = NativeArenaStatus::Active,
            ArenaEventKind::ActivationRetained | ArenaEventKind::PhaseStartRetained => {}
            ArenaEventKind::PhaseStarted => {
                self.status = NativeArenaStatus::Active;
                self.active_round_id = round_id.map(str::to_string);
                self.active_phase = phase.map(str::to_string);
            }
            ArenaEventKind::PhaseClosed => {
                if let (Some(round_id), Some(phase)) = (round_id, phase) {
                    self.completed_phases
                        .insert((round_id.to_string(), phase.to_string()));
                }
                self.status = NativeArenaStatus::PhaseComplete;
                self.active_round_id = None;
                self.active_phase = None;
            }
            ArenaEventKind::PhaseCloseRetained => {}
            ArenaEventKind::AwaitingParent => {
                self.status = NativeArenaStatus::AwaitingParent;
                self.active_round_id = None;
                self.active_phase = None;
            }
            ArenaEventKind::ClosedCleanly => {
                self.status = NativeArenaStatus::ClosedCleanly;
                self.active_round_id = None;
                self.active_phase = None;
            }
        }
    }
}

fn validate_operation_identity(identity: &ArenaOperationIdentity) -> Result<(), ArenaDomainError> {
    if identity.operation_id.trim().is_empty() {
        return Err(ArenaDomainError::new(
            "arena operation identity requires operation_id",
        ));
    }
    if identity
        .causation_id
        .as_ref()
        .is_some_and(|causation_id| causation_id.trim().is_empty())
    {
        return Err(ArenaDomainError::new(
            "arena operation identity rejects an empty causation_id",
        ));
    }
    Ok(())
}

fn validate_migration_snapshot(
    migration: Option<&NativeArenaMigrationSnapshot>,
) -> Result<(), ArenaDomainError> {
    let Some(migration) = migration else {
        return Ok(());
    };
    if migration.source_schema_version != LEGACY_ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION
        || migration.target_schema_version != ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION
        || migration.migration_id != ARENA_PROTOCOL_V1_TO_V2_MIGRATION_ID
    {
        return Err(ArenaDomainError::new(
            "arena protocol snapshot contains unsupported migration evidence",
        ));
    }
    Ok(())
}

fn validate_operation_snapshot(
    operation: &NativeArenaOperationSnapshot,
    sequence: u64,
) -> Result<(), ArenaDomainError> {
    validate_operation_identity(&ArenaOperationIdentity {
        operation_id: operation.operation_id.clone(),
        causation_id: operation.causation_id.clone(),
        composition_version: operation.composition_version,
    })?;
    if operation.command_kind.trim().is_empty()
        || operation.intent_sha256.len() != 64
        || !operation
            .intent_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ArenaDomainError::new(format!(
            "arena operation {} has an invalid command identity",
            operation.operation_id
        )));
    }
    if operation.event_sequence == 0 || operation.event_sequence > sequence {
        return Err(ArenaDomainError::new(format!(
            "arena operation {} references an invalid event sequence",
            operation.operation_id
        )));
    }
    Ok(())
}

fn command_identity(command: &ArenaCommand) -> (&'static str, Option<String>) {
    match command {
        ArenaCommand::Activate => ("activate", None),
        ArenaCommand::StartPhase { round_id, .. } => ("start_phase", Some(round_id.clone())),
        ArenaCommand::ClosePhase { round_id, .. } => ("close_phase", Some(round_id.clone())),
        ArenaCommand::AwaitParent => ("await_parent", None),
        ArenaCommand::CloseCleanly => ("close_cleanly", None),
    }
}

fn command_intent_sha256(
    arena_id: &str,
    identity: &ArenaOperationIdentity,
    command: &ArenaCommand,
) -> String {
    let (command_kind, round_id) = command_identity(command);
    let phase = match command {
        ArenaCommand::StartPhase { phase, .. } | ArenaCommand::ClosePhase { phase, .. } => {
            Some(phase.as_str())
        }
        _ => None,
    };
    let canonical = format!(
        "arena_id={arena_id}\ncomposition_version={}\ncommand_kind={command_kind}\nround_id={}\nphase={}\ncausation_id={}",
        identity.composition_version,
        round_id.as_deref().unwrap_or(""),
        phase.unwrap_or(""),
        identity.causation_id.as_deref().unwrap_or("")
    );
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

fn validate_round_and_phase(round_id: &str, phase: &str) -> Result<(), ArenaDomainError> {
    if round_id.trim().is_empty() || phase.trim().is_empty() {
        return Err(ArenaDomainError::new(
            "arena phase command requires round_id and phase",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(operation_id: &str) -> ArenaOperationIdentity {
        ArenaOperationIdentity {
            operation_id: operation_id.to_string(),
            causation_id: Some("request-1".to_string()),
            composition_version: 3,
        }
    }

    #[test]
    fn operation_retry_returns_the_confirmed_event_without_mutation() {
        let mut state = NativeArenaState::new("arena-616").unwrap();
        let command = ArenaCommand::StartPhase {
            round_id: "round-1".to_string(),
            phase: "proposal".to_string(),
        };

        let first = state
            .transition_with_operation(operation("op-start-proposal"), command.clone())
            .unwrap();
        let snapshot_after_first = state.protocol_snapshot();
        let retry = state
            .transition_with_operation(operation("op-start-proposal"), command)
            .unwrap();

        assert_eq!(retry, first);
        assert_eq!(state.protocol_snapshot(), snapshot_after_first);
        assert_eq!(state.sequence, 1);
        assert_eq!(state.operations.len(), 1);
    }

    #[test]
    fn operation_id_reuse_with_a_different_intent_is_an_atomic_conflict() {
        let mut state = NativeArenaState::new("arena-616").unwrap();
        state
            .transition_with_operation(
                operation("op-phase"),
                ArenaCommand::StartPhase {
                    round_id: "round-1".to_string(),
                    phase: "proposal".to_string(),
                },
            )
            .unwrap();
        let before = state.clone();

        let error = state
            .transition_with_operation(
                operation("op-phase"),
                ArenaCommand::ClosePhase {
                    round_id: "round-1".to_string(),
                    phase: "proposal".to_string(),
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("conflicts"));
        assert_eq!(state, before);
    }

    #[test]
    fn operation_outcome_survives_snapshot_restore() {
        let mut state = NativeArenaState::new("arena-616").unwrap();
        let command = ArenaCommand::Activate;
        let first = state
            .transition_with_operation(operation("op-activate"), command.clone())
            .unwrap();
        let serialized = serde_json::to_string(&state.protocol_snapshot()).unwrap();
        assert!(!serialized.contains("request payload"));

        let snapshot = serde_json::from_str(&serialized).unwrap();
        let mut restored = NativeArenaState::restore_protocol_snapshot(snapshot).unwrap();
        let retry = restored
            .transition_with_operation(operation("op-activate"), command)
            .unwrap();

        assert_eq!(retry, first);
        assert_eq!(restored, state);
    }

    #[test]
    fn canonical_state_rejects_concurrent_phases() {
        let mut state = NativeArenaState::new("arena-1").unwrap();
        state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();

        let error = state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "bet".to_string(),
            })
            .unwrap_err();

        assert!(error.to_string().contains("proposal is active"));
        assert_eq!(state.active_phase(), Some("proposal"));
    }

    #[test]
    fn protocol_snapshot_round_trips_deterministically_without_semantic_state() {
        let mut state = NativeArenaState::new("arena-620").unwrap();
        state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-2".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();
        state
            .transition(ArenaCommand::ClosePhase {
                round_id: "round-2".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();
        state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "review".to_string(),
            })
            .unwrap();
        state
            .transition(ArenaCommand::ClosePhase {
                round_id: "round-1".to_string(),
                phase: "review".to_string(),
            })
            .unwrap();

        let snapshot = state.protocol_snapshot();
        let serialized = serde_json::to_string(&snapshot).unwrap();
        assert!(!serialized.contains("next_action"));
        assert!(!serialized.contains("summary"));
        assert!(!serialized.contains("agenda"));
        assert!(serialized.find("round-1").unwrap() < serialized.find("round-2").unwrap());

        let restored = NativeArenaState::restore_protocol_snapshot(snapshot).unwrap();
        assert_eq!(restored, state);
        assert_eq!(
            serde_json::to_string(&restored.protocol_snapshot()).unwrap(),
            serialized
        );
    }

    #[test]
    fn protocol_snapshot_rejects_future_schema_and_invalid_active_phase() {
        let state = NativeArenaState::new("arena-620").unwrap();
        let mut future = state.protocol_snapshot();
        future.schema_version += 1;
        assert!(
            NativeArenaState::restore_protocol_snapshot(future)
                .unwrap_err()
                .to_string()
                .contains("unsupported arena protocol snapshot schema")
        );

        let mut invalid = state.protocol_snapshot();
        invalid.active_round_id = Some("round-1".to_string());
        assert!(
            NativeArenaState::restore_protocol_snapshot(invalid)
                .unwrap_err()
                .to_string()
                .contains("active round and phase together")
        );
    }

    #[test]
    fn legacy_protocol_snapshot_restores_with_an_empty_operation_ledger() {
        let legacy_snapshot = serde_json::from_str(include_str!(
            "../../tests/fixtures/memythos/arena_protocol_snapshot_v1.json"
        ))
        .unwrap();
        let restored = NativeArenaState::restore_protocol_snapshot(legacy_snapshot).unwrap();

        assert!(restored.operations.is_empty());
        let current = restored.protocol_snapshot();
        assert_eq!(current.schema_version, 2);
        assert_eq!(
            current.migration,
            Some(NativeArenaMigrationSnapshot {
                source_schema_version: 1,
                target_schema_version: 2,
                migration_id: ARENA_PROTOCOL_V1_TO_V2_MIGRATION_ID.to_string(),
            })
        );

        let restored_again = NativeArenaState::restore_protocol_snapshot(current.clone()).unwrap();
        assert_eq!(restored_again.protocol_snapshot(), current);
    }

    #[test]
    fn phase_start_and_close_are_idempotent() {
        let mut state = NativeArenaState::new("arena-1").unwrap();
        let command = ArenaCommand::StartPhase {
            round_id: "round-1".to_string(),
            phase: "proposal".to_string(),
        };
        assert_eq!(
            state.transition(command.clone()).unwrap().kind,
            ArenaEventKind::PhaseStarted
        );
        assert_eq!(
            state.transition(command).unwrap().kind,
            ArenaEventKind::PhaseStartRetained
        );

        let command = ArenaCommand::ClosePhase {
            round_id: "round-1".to_string(),
            phase: "proposal".to_string(),
        };
        assert_eq!(
            state.transition(command.clone()).unwrap().kind,
            ArenaEventKind::PhaseClosed
        );
        assert_eq!(
            state.transition(command).unwrap().kind,
            ArenaEventKind::PhaseCloseRetained
        );
        assert_eq!(
            state.protocol_state(),
            MemythosArenaLifecycleState::ArtifactComplete
        );
    }

    #[test]
    fn closing_the_wrong_round_is_rejected_without_mutation() {
        let mut state = NativeArenaState::new("arena-1").unwrap();
        state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();

        let error = state
            .transition(ArenaCommand::ClosePhase {
                round_id: "round-2".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap_err();

        assert!(error.to_string().contains("inactive phase"));
        assert_eq!(state.active_round_id(), Some("round-1"));
        assert_eq!(state.active_phase(), Some("proposal"));
    }

    #[test]
    fn terminal_state_rejects_future_work() {
        let mut state = NativeArenaState::new("arena-1").unwrap();
        state.transition(ArenaCommand::CloseCleanly).unwrap();

        let error = state.transition(ArenaCommand::Activate).unwrap_err();

        assert!(error.to_string().contains("terminal"));
        assert_eq!(
            state.protocol_state(),
            MemythosArenaLifecycleState::ClosedCleanly
        );
    }

    #[test]
    fn accepted_command_sequences_remain_valid_and_rejected_commands_are_atomic() {
        let commands = vec![
            ArenaCommand::Activate,
            ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            },
            ArenaCommand::ClosePhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            },
            ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "bet".to_string(),
            },
            ArenaCommand::ClosePhase {
                round_id: "round-1".to_string(),
                phase: "bet".to_string(),
            },
            ArenaCommand::AwaitParent,
            ArenaCommand::CloseCleanly,
        ];
        let mut frontier = vec![NativeArenaState::new("arena-property").unwrap()];

        for _ in 0..6 {
            let mut next_frontier = Vec::new();
            let mut seen_snapshots = HashSet::new();
            for state in frontier {
                for command in &commands {
                    let mut candidate = state.clone();
                    let before = candidate.clone();
                    match candidate.transition(command.clone()) {
                        Ok(event) => {
                            assert_eq!(event.sequence, before.sequence + 1);
                            assert_eq!(event.arena_id, "arena-property");
                            let restored = NativeArenaState::restore_protocol_snapshot(
                                candidate.protocol_snapshot(),
                            )
                            .expect("every accepted state must restore");
                            assert_eq!(restored, candidate);
                            let snapshot_key =
                                serde_json::to_string(&candidate.protocol_snapshot()).unwrap();
                            if seen_snapshots.insert(snapshot_key) {
                                next_frontier.push(candidate);
                            }
                        }
                        Err(_) => assert_eq!(candidate, before),
                    }
                }
            }
            frontier = next_frontier;
        }
    }

    #[test]
    fn retained_close_does_not_corrupt_a_new_active_phase() {
        let mut state = NativeArenaState::new("arena-1").unwrap();
        state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();
        state
            .transition(ArenaCommand::ClosePhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();
        state
            .transition(ArenaCommand::StartPhase {
                round_id: "round-1".to_string(),
                phase: "bet".to_string(),
            })
            .unwrap();

        let event = state
            .transition(ArenaCommand::ClosePhase {
                round_id: "round-1".to_string(),
                phase: "proposal".to_string(),
            })
            .unwrap();

        assert_eq!(event.kind, ArenaEventKind::PhaseCloseRetained);
        assert_eq!(state.status, NativeArenaStatus::Active);
        assert_eq!(state.active_phase(), Some("bet"));
    }
}
