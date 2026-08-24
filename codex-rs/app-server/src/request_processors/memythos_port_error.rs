use codex_app_server_protocol::JSONRPCErrorError;

use crate::error_code::invalid_params;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArenaPortFailureKind {
    ContractRejected,
    TransientFailure,
    OutcomeUnknown,
    PermanentFailure,
}

impl ArenaPortFailureKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ContractRejected => "contract_rejected",
            Self::TransientFailure => "transient_failure",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::PermanentFailure => "permanent_failure",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ArenaPortError {
    pub(crate) kind: ArenaPortFailureKind,
    message: String,
}

impl ArenaPortError {
    pub(crate) fn contract_rejected(message: impl Into<String>) -> Self {
        Self {
            kind: ArenaPortFailureKind::ContractRejected,
            message: message.into(),
        }
    }

    pub(crate) fn classify_planning_failure(error: JSONRPCErrorError) -> Self {
        let message = error.message;
        let normalized = message.to_ascii_lowercase();
        let kind = if normalized.contains("timeout") || normalized.contains("timed out") {
            ArenaPortFailureKind::OutcomeUnknown
        } else if normalized.contains("temporarily unavailable")
            || normalized.contains("connection closed")
            || normalized.contains("failed to start native")
        {
            ArenaPortFailureKind::TransientFailure
        } else {
            ArenaPortFailureKind::PermanentFailure
        };
        Self { kind, message }
    }

    pub(crate) fn into_jsonrpc(self, port: &str) -> JSONRPCErrorError {
        invalid_params(format!(
            "arena port {port} {}: {}",
            self.kind.as_str(),
            self.message
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_failures_distinguish_unknown_transient_and_permanent_results() {
        let unknown = ArenaPortError::classify_planning_failure(invalid_params(
            "timed out waiting for native arena planner turn turn-1",
        ));
        assert_eq!(unknown.kind, ArenaPortFailureKind::OutcomeUnknown);

        let transient = ArenaPortError::classify_planning_failure(invalid_params(
            "failed to start native arena planner: connection closed",
        ));
        assert_eq!(transient.kind, ArenaPortFailureKind::TransientFailure);

        let permanent = ArenaPortError::classify_planning_failure(invalid_params(
            "native arena planner returned invalid contract JSON",
        ));
        assert_eq!(permanent.kind, ArenaPortFailureKind::PermanentFailure);
    }

    #[test]
    fn contract_rejection_remains_explicit_at_the_rpc_boundary() {
        let error = ArenaPortError::contract_rejected("foreign planner ref")
            .into_jsonrpc("composition_planning");
        assert!(error.message.contains("contract_rejected"));
        assert!(error.message.contains("foreign planner ref"));
    }
}
