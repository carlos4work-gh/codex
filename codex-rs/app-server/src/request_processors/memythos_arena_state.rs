use std::collections::HashSet;
use std::fmt;

use codex_app_server_protocol::MemythosArenaLifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeArenaStatus {
    Draft,
    Active,
    PhaseComplete,
    ClosedCleanly,
    ClosedDegraded,
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
    CloseCleanly,
    CloseDegraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArenaEventKind {
    Activated,
    ActivationRetained,
    PhaseStarted,
    PhaseStartRetained,
    PhaseClosed,
    PhaseCloseRetained,
    ClosedCleanly,
    ClosedDegraded,
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
            ArenaEventKind::ClosedCleanly => "closed-cleanly",
            ArenaEventKind::ClosedDegraded => "closed-degraded",
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
            NativeArenaStatus::ClosedCleanly => MemythosArenaLifecycleState::ClosedCleanly,
            NativeArenaStatus::ClosedDegraded => MemythosArenaLifecycleState::ClosedDegraded,
        }
    }

    pub(crate) fn active_round_id(&self) -> Option<&str> {
        self.active_round_id.as_deref()
    }

    pub(crate) fn active_phase(&self) -> Option<&str> {
        self.active_phase.as_deref()
    }

    fn decide(
        &self,
        command: &ArenaCommand,
    ) -> Result<(ArenaEventKind, Option<String>, Option<String>), ArenaDomainError> {
        if matches!(
            self.status,
            NativeArenaStatus::ClosedCleanly | NativeArenaStatus::ClosedDegraded
        ) {
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
            ArenaCommand::CloseDegraded => Ok((ArenaEventKind::ClosedDegraded, None, None)),
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
            ArenaEventKind::ClosedCleanly => {
                self.status = NativeArenaStatus::ClosedCleanly;
                self.active_round_id = None;
                self.active_phase = None;
            }
            ArenaEventKind::ClosedDegraded => {
                self.status = NativeArenaStatus::ClosedDegraded;
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
    fn degraded_close_projects_protocol_state() {
        let mut state = NativeArenaState::new("arena-1").unwrap();
        let event = state.transition(ArenaCommand::CloseDegraded).unwrap();

        assert_eq!(event.kind, ArenaEventKind::ClosedDegraded);
        assert_eq!(event.action(), "closed-degraded");
        assert_eq!(
            state.protocol_state(),
            MemythosArenaLifecycleState::ClosedDegraded
        );
    }
}
