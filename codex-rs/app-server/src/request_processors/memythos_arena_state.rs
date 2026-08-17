use std::collections::HashSet;
use std::fmt;

use codex_app_server_protocol::MemythosArenaLifecycleState;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArenaCommand {
    Activate,
    StartPhase { round_id: String, phase: String },
    ClosePhase { round_id: String, phase: String },
    AwaitParent,
    CloseCleanly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    fn new(message: impl Into<String>) -> Self {
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
        })
    }

    pub(crate) fn transition(
        &mut self,
        command: ArenaCommand,
    ) -> Result<ArenaEvent, ArenaDomainError> {
        let (kind, round_id, phase) = self.decide(&command)?;
        self.apply(kind, round_id.as_deref(), phase.as_deref());
        self.sequence += 1;
        Ok(ArenaEvent {
            sequence: self.sequence,
            kind,
            arena_id: self.arena_id.clone(),
            round_id,
            phase,
        })
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
        NativeArenaProtocolSnapshot {
            schema_version: ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION,
            arena_id: self.arena_id.clone(),
            status: self.status,
            active_round_id: self.active_round_id.clone(),
            active_phase: self.active_phase.clone(),
            completed_phases,
            sequence: self.sequence,
        }
    }

    pub(crate) fn restore_protocol_snapshot(
        snapshot: NativeArenaProtocolSnapshot,
    ) -> Result<Self, ArenaDomainError> {
        if snapshot.schema_version != ARENA_PROTOCOL_SNAPSHOT_SCHEMA_VERSION {
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

        Ok(Self {
            arena_id: snapshot.arena_id,
            status: snapshot.status,
            active_round_id: snapshot.active_round_id,
            active_phase: snapshot.active_phase,
            completed_phases,
            sequence: snapshot.sequence,
        })
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
            ArenaEventKind::PhaseCloseRetained => {
                self.status = NativeArenaStatus::PhaseComplete;
            }
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
}
