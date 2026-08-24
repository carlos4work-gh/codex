use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::InterAgentCommunication;
use codex_rollout::state_db::StateDbHandle;
use codex_state::NativeMailboxCommunicationRecord;
use codex_state::NativeMailboxInsertOutcome;
use codex_state::NativeMailboxRecoveryOutcome;
use codex_state::NativeMailboxSubmissionState;
use sha2::Digest;
use sha2::Sha256;

const MAX_RECOVERY_ATTEMPTS: i64 = 3;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PersistOutcome {
    ReadyToSend,
    Existing { submission_id: Option<String> },
}

pub(crate) struct RestoredCommunications {
    pub communications: Vec<InterAgentCommunication>,
    pub warnings: Vec<String>,
}

pub(crate) struct ActivatedCommunication {
    pub communication: InterAgentCommunication,
    pub submission_id: Option<String>,
    pub was_staged: bool,
}

pub(crate) struct DurableInterAgentMailbox {
    state_db: Option<StateDbHandle>,
}

impl DurableInterAgentMailbox {
    pub(crate) fn new(state_db: Option<StateDbHandle>) -> Self {
        Self { state_db }
    }

    pub(crate) async fn stage_before_send(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
        communication: &InterAgentCommunication,
    ) -> CodexResult<PersistOutcome> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(PersistOutcome::ReadyToSend);
        };
        let record = native_mailbox_record(
            receiver_thread_id,
            communication_id,
            communication,
            "staged",
        )?;
        let inserted = state_db
            .insert_staged_native_mailbox_communication(&record)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to stage native mailbox message: {error}"))
            })?;
        if inserted == NativeMailboxInsertOutcome::Inserted {
            return Ok(PersistOutcome::ReadyToSend);
        }
        let submission_id = state_db
            .get_native_mailbox_communication(receiver_thread_id, communication_id)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to read native mailbox message: {error}"))
            })?
            .and_then(|record| record.submission_id);
        Ok(PersistOutcome::Existing { submission_id })
    }

    pub(crate) async fn activate_before_send(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
    ) -> CodexResult<PersistOutcome> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(PersistOutcome::ReadyToSend);
        };
        let activated = state_db
            .activate_staged_native_mailbox_communication(
                receiver_thread_id,
                communication_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!(
                    "failed to activate native mailbox message: {error}"
                ))
            })?;
        if activated == NativeMailboxInsertOutcome::Inserted {
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
            submission_id: existing.submission_id,
        })
    }

    pub(crate) async fn activate_by_id(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
    ) -> CodexResult<ActivatedCommunication> {
        let state_db = self.state_db.as_ref().ok_or_else(|| {
            CodexErr::Fatal("staged mailbox activation requires sqlite state".to_string())
        })?;
        let staged = state_db
            .get_staged_native_mailbox_communication(receiver_thread_id, communication_id)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to read staged mailbox message: {error}"))
            })?;
        let outcome = self
            .activate_before_send(receiver_thread_id, communication_id)
            .await?;
        let active = state_db
            .get_native_mailbox_communication(receiver_thread_id, communication_id)
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!("failed to read active mailbox message: {error}"))
            })?
            .ok_or_else(|| CodexErr::Fatal("native mailbox message disappeared".to_string()))?;
        if let Some(staged) = staged.as_ref() {
            if staged.payload_hash != active.payload_hash
                || staged.communication_json != active.communication_json
            {
                return Err(CodexErr::Fatal(
                    "staged mailbox payload changed during activation".to_string(),
                ));
            }
        }
        let communication = serde_json::from_str(&active.communication_json).map_err(|error| {
            CodexErr::Fatal(format!(
                "failed to decode activated mailbox message {communication_id}: {error}"
            ))
        })?;
        let submission_id = match outcome {
            PersistOutcome::Existing { submission_id } => submission_id,
            PersistOutcome::ReadyToSend => None,
        };
        Ok(ActivatedCommunication {
            communication,
            submission_id,
            was_staged: staged.is_some(),
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

    pub(crate) async fn reserve_submission(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
        planned_submission_id: &str,
    ) -> CodexResult<NativeMailboxSubmissionState> {
        let state_db = self.state_db.as_ref().ok_or_else(|| {
            CodexErr::Fatal(
                "native mailbox submission reservation requires sqlite state".to_string(),
            )
        })?;
        state_db
            .reserve_native_mailbox_submission_id(
                receiver_thread_id,
                communication_id,
                planned_submission_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|error| {
                CodexErr::Fatal(format!(
                    "failed to reserve native mailbox submission: {error}"
                ))
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

fn native_mailbox_record(
    receiver_thread_id: &str,
    communication_id: &str,
    communication: &InterAgentCommunication,
    status: &str,
) -> CodexResult<NativeMailboxCommunicationRecord> {
    let communication_json = serde_json::to_string(communication).map_err(|error| {
        CodexErr::Fatal(format!(
            "failed to serialize native mailbox message: {error}"
        ))
    })?;
    let now = chrono::Utc::now().timestamp_millis();
    Ok(NativeMailboxCommunicationRecord {
        receiver_thread_id: receiver_thread_id.to_string(),
        communication_id: communication_id.to_string(),
        source_call_id: Some(communication_id.to_string()),
        submission_id: None,
        payload_hash: format!("sha256:{:x}", Sha256::digest(communication_json.as_bytes())),
        communication_json,
        status: status.to_string(),
        attempt_count: 0,
        failure_fingerprint: None,
        last_progress_ref: None,
        quarantine_reason: None,
        created_at_ms: now,
        updated_at_ms: now,
    })
}

pub(crate) fn native_mailbox_submission_id(
    receiver_thread_id: &str,
    communication_id: &str,
) -> String {
    let digest = Sha256::digest(format!("{receiver_thread_id}\0{communication_id}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}
