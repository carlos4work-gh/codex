use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::InterAgentCommunication;
use codex_rollout::state_db::StateDbHandle;
use codex_state::NativeMailboxCommunicationRecord;
use codex_state::NativeMailboxInsertOutcome;
use codex_state::NativeMailboxRecoveryOutcome;
use sha2::Digest;
use sha2::Sha256;

const MAX_RECOVERY_ATTEMPTS: i64 = 3;

pub(crate) enum PersistOutcome {
    ReadyToSend,
    Existing { submission_id: String },
}

pub(crate) struct RestoredCommunications {
    pub communications: Vec<InterAgentCommunication>,
    pub warnings: Vec<String>,
}

pub(crate) struct DurableInterAgentMailbox {
    state_db: Option<StateDbHandle>,
}

impl DurableInterAgentMailbox {
    pub(crate) fn new(state_db: Option<StateDbHandle>) -> Self {
        Self { state_db }
    }

    pub(crate) async fn persist_before_send(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
        communication: &InterAgentCommunication,
    ) -> CodexResult<PersistOutcome> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(PersistOutcome::ReadyToSend);
        };
        let communication_json = serde_json::to_string(communication).map_err(|error| {
            CodexErr::Fatal(format!(
                "failed to serialize native mailbox message: {error}"
            ))
        })?;
        let now = chrono::Utc::now().timestamp_millis();
        let record = NativeMailboxCommunicationRecord {
            receiver_thread_id: receiver_thread_id.to_string(),
            communication_id: communication_id.to_string(),
            source_call_id: Some(communication_id.to_string()),
            submission_id: None,
            payload_hash: format!("sha256:{:x}", Sha256::digest(communication_json.as_bytes())),
            communication_json,
            status: "pending".to_string(),
            attempt_count: 0,
            failure_fingerprint: None,
            last_progress_ref: None,
            quarantine_reason: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let inserted = state_db
            .insert_pending_native_mailbox_communication(&record)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to persist native mailbox message: {error}"))
            })?;
        if inserted == NativeMailboxInsertOutcome::Inserted {
            return Ok(PersistOutcome::ReadyToSend);
        }
        let existing = state_db
            .get_native_mailbox_communication(receiver_thread_id, communication_id)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to read native mailbox message: {error}"))
            })?
            .ok_or_else(|| CodexErr::Fatal("native mailbox message disappeared".to_string()))?;
        Ok(PersistOutcome::Existing {
            submission_id: existing
                .submission_id
                .unwrap_or_else(|| communication_id.to_string()),
        })
    }

    pub(crate) async fn bind_submission(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
        submission_id: &str,
    ) -> CodexResult<()> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(());
        };
        state_db
            .set_native_mailbox_submission_id(
                receiver_thread_id,
                communication_id,
                submission_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to bind native mailbox submission: {error}"))
            })
    }

    pub(crate) async fn mark_consumed(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
    ) -> CodexResult<()> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(());
        };
        state_db
            .mark_native_mailbox_communication_consumed(
                receiver_thread_id,
                communication_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to consume native mailbox message: {error}"))
            })
            .map(|_| ())
    }

    pub(crate) async fn restore(
        &self,
        receiver_thread_id: &str,
    ) -> CodexResult<RestoredCommunications> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(RestoredCommunications {
                communications: Vec::new(),
                warnings: Vec::new(),
            });
        };
        let pending = state_db
            .list_pending_native_mailbox_communications(receiver_thread_id)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to restore native mailbox: {error}"))
            })?;
        let mut communications = Vec::new();
        let mut warnings = Vec::new();
        for record in pending {
            let recovery = state_db
                .claim_native_mailbox_communication_for_recovery(
                    &record.receiver_thread_id,
                    &record.communication_id,
                    MAX_RECOVERY_ATTEMPTS,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
                .map_err(|error| {
                    CodexErr::Fatal(format!("failed to claim native mailbox message: {error}"))
                })?;
            let record = match recovery {
                Some(NativeMailboxRecoveryOutcome::Claimed(record)) => record,
                Some(NativeMailboxRecoveryOutcome::Quarantined(record)) => {
                    warnings.push(format!(
                        "Native mailbox communication {} was quarantined after {} recovery attempts without durable progress; automatic recovery stopped.",
                        record.communication_id, record.attempt_count
                    ));
                    continue;
                }
                None => continue,
            };
            communications.push(serde_json::from_str(&record.communication_json).map_err(
                |error| {
                    CodexErr::Fatal(format!(
                        "failed to decode native mailbox message {}: {error}",
                        record.communication_id
                    ))
                },
            )?);
        }
        Ok(RestoredCommunications {
            communications,
            warnings,
        })
    }
}
