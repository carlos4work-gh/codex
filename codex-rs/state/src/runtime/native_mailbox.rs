use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMailboxCommunicationRecord {
    pub receiver_thread_id: String,
    pub communication_id: String,
    pub source_call_id: Option<String>,
    pub submission_id: Option<String>,
    pub communication_json: String,
    pub payload_hash: String,
    pub status: String,
    pub attempt_count: i64,
    pub failure_fingerprint: Option<String>,
    pub last_progress_ref: Option<String>,
    pub quarantine_reason: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeMailboxInsertOutcome {
    Inserted,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeMailboxRecoveryOutcome {
    Claimed(NativeMailboxCommunicationRecord),
    Quarantined(NativeMailboxCommunicationRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeMailboxResolutionAction {
    Retry,
    Skip,
    Replace,
    Abort,
}

impl NativeMailboxResolutionAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::Skip => "skip",
            Self::Replace => "replace",
            Self::Abort => "abort",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMailboxResolutionCommand {
    pub receiver_thread_id: String,
    pub communication_id: String,
    pub command_id: String,
    pub action: NativeMailboxResolutionAction,
    pub actor: String,
    pub reason: String,
    pub replacement: Option<NativeMailboxCommunicationRecord>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMailboxResolutionOutcome {
    pub receiver_thread_id: String,
    pub communication_id: String,
    pub command_id: String,
    pub action: String,
    pub resulting_status: String,
    pub replacement_communication_id: Option<String>,
    pub existing: bool,
    pub conflict: bool,
    pub winner_command_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMailboxResolutionAuditRecord {
    pub id: i64,
    pub receiver_thread_id: String,
    pub communication_id: String,
    pub command_id: String,
    pub resolution_generation: i64,
    pub action: String,
    pub actor: String,
    pub reason: String,
    pub pre_status: String,
    pub pre_attempt_count: i64,
    pub pre_failure_fingerprint: Option<String>,
    pub pre_last_progress_ref: Option<String>,
    pub pre_quarantine_reason: Option<String>,
    pub pre_payload_hash: String,
    pub resulting_status: String,
    pub replacement_communication_id: Option<String>,
    pub created_at_ms: i64,
}

impl StateRuntime {
    pub async fn insert_pending_native_mailbox_communication(
        &self,
        record: &NativeMailboxCommunicationRecord,
    ) -> anyhow::Result<NativeMailboxInsertOutcome> {
        let result = sqlx::query(
            r#"
INSERT INTO native_mailbox_communications (
    receiver_thread_id,
    communication_id,
    source_call_id,
    submission_id,
    communication_json,
    payload_hash,
    status,
    attempt_count,
    failure_fingerprint,
    last_progress_ref,
    quarantine_reason,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, NULL, NULL, NULL, ?, ?)
ON CONFLICT(receiver_thread_id, communication_id) DO NOTHING
            "#,
        )
        .bind(&record.receiver_thread_id)
        .bind(&record.communication_id)
        .bind(&record.source_call_id)
        .bind(&record.submission_id)
        .bind(&record.communication_json)
        .bind(&record.payload_hash)
        .bind(record.created_at_ms)
        .bind(record.updated_at_ms)
        .execute(self.pool.as_ref())
        .await?;

        if result.rows_affected() == 1 {
            return Ok(NativeMailboxInsertOutcome::Inserted);
        }

        let existing = self
            .get_native_mailbox_communication(&record.receiver_thread_id, &record.communication_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("native mailbox conflict row disappeared"))?;
        anyhow::ensure!(
            existing.source_call_id == record.source_call_id
                && existing.communication_json == record.communication_json
                && existing.payload_hash == record.payload_hash,
            "native mailbox communication identity conflicts with a different payload"
        );
        Ok(NativeMailboxInsertOutcome::Existing)
    }

    pub async fn get_native_mailbox_communication(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
    ) -> anyhow::Result<Option<NativeMailboxCommunicationRecord>> {
        let row = sqlx::query(
            r#"
SELECT receiver_thread_id, communication_id, source_call_id, submission_id,
    communication_json, payload_hash, status, attempt_count,
    failure_fingerprint, last_progress_ref, quarantine_reason,
    created_at_ms, updated_at_ms
FROM native_mailbox_communications
WHERE receiver_thread_id = ? AND communication_id = ?
            "#,
        )
        .bind(receiver_thread_id)
        .bind(communication_id)
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(native_mailbox_record_from_row).transpose()
    }

    pub async fn list_pending_native_mailbox_communications(
        &self,
        receiver_thread_id: &str,
    ) -> anyhow::Result<Vec<NativeMailboxCommunicationRecord>> {
        let rows = sqlx::query(
            r#"
SELECT receiver_thread_id, communication_id, source_call_id, submission_id,
    communication_json, payload_hash, status, attempt_count,
    failure_fingerprint, last_progress_ref, quarantine_reason,
    created_at_ms, updated_at_ms
FROM native_mailbox_communications
WHERE receiver_thread_id = ? AND status = 'pending'
ORDER BY id
            "#,
        )
        .bind(receiver_thread_id)
        .fetch_all(self.pool.as_ref())
        .await?;

        rows.into_iter()
            .map(native_mailbox_record_from_row)
            .collect()
    }

    pub async fn list_quarantined_native_mailbox_communications(
        &self,
        receiver_thread_id: Option<&str>,
    ) -> anyhow::Result<Vec<NativeMailboxCommunicationRecord>> {
        let rows = if let Some(receiver_thread_id) = receiver_thread_id {
            sqlx::query(
                r#"SELECT receiver_thread_id, communication_id, source_call_id, submission_id,
                    communication_json, payload_hash, status, attempt_count,
                    failure_fingerprint, last_progress_ref, quarantine_reason,
                    created_at_ms, updated_at_ms
                   FROM native_mailbox_communications
                   WHERE status = 'quarantined' AND receiver_thread_id = ? ORDER BY id"#,
            )
            .bind(receiver_thread_id)
            .fetch_all(self.pool.as_ref())
            .await?
        } else {
            sqlx::query(
                r#"SELECT receiver_thread_id, communication_id, source_call_id, submission_id,
                    communication_json, payload_hash, status, attempt_count,
                    failure_fingerprint, last_progress_ref, quarantine_reason,
                    created_at_ms, updated_at_ms
                   FROM native_mailbox_communications
                   WHERE status = 'quarantined' ORDER BY id"#,
            )
            .fetch_all(self.pool.as_ref())
            .await?
        };
        rows.into_iter()
            .map(native_mailbox_record_from_row)
            .collect()
    }

    pub async fn resolve_native_mailbox_quarantine(
        &self,
        command: &NativeMailboxResolutionCommand,
    ) -> anyhow::Result<NativeMailboxResolutionOutcome> {
        anyhow::ensure!(
            !command.command_id.trim().is_empty(),
            "command id is required"
        );
        anyhow::ensure!(!command.actor.trim().is_empty(), "actor is required");
        anyhow::ensure!(!command.reason.trim().is_empty(), "reason is required");
        anyhow::ensure!(
            matches!(command.action, NativeMailboxResolutionAction::Replace)
                == command.replacement.is_some(),
            "replacement payload is required only for replace"
        );
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) = sqlx::query(
            r#"SELECT communication_id, command_id, action, resulting_status,
                      replacement_communication_id
               FROM native_mailbox_resolution_commands
               WHERE receiver_thread_id = ? AND command_id = ?"#,
        )
        .bind(&command.receiver_thread_id)
        .bind(&command.command_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            anyhow::ensure!(
                row.try_get::<String, _>("communication_id")? == command.communication_id
                    && row.try_get::<String, _>("action")? == command.action.as_str(),
                "resolution command id conflicts with a different operation"
            );
            return Ok(NativeMailboxResolutionOutcome {
                receiver_thread_id: command.receiver_thread_id.clone(),
                communication_id: command.communication_id.clone(),
                command_id: command.command_id.clone(),
                action: command.action.as_str().to_string(),
                resulting_status: row.try_get("resulting_status")?,
                replacement_communication_id: row.try_get("replacement_communication_id")?,
                existing: true,
                conflict: false,
                winner_command_id: None,
            });
        }

        let pre_resolution_row = sqlx::query(
            r#"SELECT receiver_thread_id, communication_id, source_call_id, submission_id,
                      communication_json, payload_hash, status, attempt_count,
                      failure_fingerprint, last_progress_ref, quarantine_reason,
                      created_at_ms, updated_at_ms
               FROM native_mailbox_communications
               WHERE receiver_thread_id = ? AND communication_id = ?"#,
        )
        .bind(&command.receiver_thread_id)
        .bind(&command.communication_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("communication not found"))?;
        let pre_resolution = native_mailbox_record_from_row(pre_resolution_row)?;
        if pre_resolution.status != "quarantined" {
            if let Some(winner) = sqlx::query(
                r#"SELECT command_id, resulting_status, replacement_communication_id
                   FROM native_mailbox_resolution_commands
                   WHERE receiver_thread_id = ? AND communication_id = ?
                   ORDER BY id DESC LIMIT 1"#,
            )
            .bind(&command.receiver_thread_id)
            .bind(&command.communication_id)
            .fetch_optional(&mut *tx)
            .await?
            {
                return Ok(NativeMailboxResolutionOutcome {
                    receiver_thread_id: command.receiver_thread_id.clone(),
                    communication_id: command.communication_id.clone(),
                    command_id: command.command_id.clone(),
                    action: command.action.as_str().to_string(),
                    resulting_status: winner.try_get("resulting_status")?,
                    replacement_communication_id: winner.try_get("replacement_communication_id")?,
                    existing: false,
                    conflict: true,
                    winner_command_id: Some(winner.try_get("command_id")?),
                });
            }
            anyhow::bail!("communication is not quarantined");
        }
        let resolution_generation: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) + 1 FROM native_mailbox_resolution_commands
               WHERE receiver_thread_id = ? AND communication_id = ?"#,
        )
        .bind(&command.receiver_thread_id)
        .bind(&command.communication_id)
        .fetch_one(&mut *tx)
        .await?;

        let (resulting_status, replacement_id) = match command.action {
            NativeMailboxResolutionAction::Retry => ("pending", None),
            NativeMailboxResolutionAction::Skip => ("skipped", None),
            NativeMailboxResolutionAction::Abort => ("aborted", None),
            NativeMailboxResolutionAction::Replace => {
                let replacement = command.replacement.as_ref().expect("validated replacement");
                anyhow::ensure!(
                    replacement.receiver_thread_id == command.receiver_thread_id
                        && replacement.communication_id != command.communication_id,
                    "replacement must target the same receiver with a new identity"
                );
                sqlx::query(
                    r#"INSERT INTO native_mailbox_communications (
                        receiver_thread_id, communication_id, source_call_id, submission_id,
                        communication_json, payload_hash, status, attempt_count,
                        created_at_ms, updated_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)"#,
                )
                .bind(&replacement.receiver_thread_id)
                .bind(&replacement.communication_id)
                .bind(&replacement.source_call_id)
                .bind(&replacement.submission_id)
                .bind(&replacement.communication_json)
                .bind(&replacement.payload_hash)
                .bind(replacement.created_at_ms)
                .bind(replacement.updated_at_ms)
                .execute(&mut *tx)
                .await?;
                ("aborted", Some(replacement.communication_id.clone()))
            }
        };
        let result = sqlx::query(
            r#"UPDATE native_mailbox_communications
               SET status = ?, attempt_count = CASE WHEN ? = 'pending' THEN 0 ELSE attempt_count END,
                   failure_fingerprint = CASE WHEN ? = 'pending' THEN NULL ELSE failure_fingerprint END,
                   quarantine_reason = CASE WHEN ? = 'pending' THEN NULL ELSE quarantine_reason END,
                   updated_at_ms = ?
               WHERE receiver_thread_id = ? AND communication_id = ? AND status = 'quarantined'"#,
        )
        .bind(resulting_status)
        .bind(resulting_status)
        .bind(resulting_status)
        .bind(resulting_status)
        .bind(command.created_at_ms)
        .bind(&command.receiver_thread_id)
        .bind(&command.communication_id)
        .execute(&mut *tx)
        .await?;
        anyhow::ensure!(
            result.rows_affected() == 1,
            "communication is not quarantined"
        );
        sqlx::query(
            r#"INSERT INTO native_mailbox_resolution_commands (
                receiver_thread_id, communication_id, command_id, action, actor, reason,
                replacement_communication_id, resulting_status, created_at_ms,
                resolution_generation, pre_status, pre_attempt_count,
                pre_failure_fingerprint, pre_last_progress_ref, pre_quarantine_reason,
                pre_payload_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&command.receiver_thread_id)
        .bind(&command.communication_id)
        .bind(&command.command_id)
        .bind(command.action.as_str())
        .bind(&command.actor)
        .bind(&command.reason)
        .bind(&replacement_id)
        .bind(resulting_status)
        .bind(command.created_at_ms)
        .bind(resolution_generation)
        .bind(&pre_resolution.status)
        .bind(pre_resolution.attempt_count)
        .bind(&pre_resolution.failure_fingerprint)
        .bind(&pre_resolution.last_progress_ref)
        .bind(&pre_resolution.quarantine_reason)
        .bind(&pre_resolution.payload_hash)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(NativeMailboxResolutionOutcome {
            receiver_thread_id: command.receiver_thread_id.clone(),
            communication_id: command.communication_id.clone(),
            command_id: command.command_id.clone(),
            action: command.action.as_str().to_string(),
            resulting_status: resulting_status.to_string(),
            replacement_communication_id: replacement_id,
            existing: false,
            conflict: false,
            winner_command_id: None,
        })
    }

    pub async fn list_native_mailbox_resolution_audit(
        &self,
        receiver_thread_id: Option<&str>,
        communication_id: Option<&str>,
        after_id: Option<i64>,
        limit: i64,
    ) -> anyhow::Result<Vec<NativeMailboxResolutionAuditRecord>> {
        anyhow::ensure!(
            (1..=101).contains(&limit),
            "limit must be between 1 and 101"
        );
        let rows = sqlx::query(
            r#"SELECT id, receiver_thread_id, communication_id, command_id,
                      resolution_generation, action, actor, reason, pre_status,
                      pre_attempt_count, pre_failure_fingerprint, pre_last_progress_ref,
                      pre_quarantine_reason, pre_payload_hash, resulting_status,
                      replacement_communication_id, created_at_ms
               FROM native_mailbox_resolution_commands
               WHERE (? IS NULL OR receiver_thread_id = ?)
                 AND (? IS NULL OR communication_id = ?)
                 AND id > ?
               ORDER BY id LIMIT ?"#,
        )
        .bind(receiver_thread_id)
        .bind(receiver_thread_id)
        .bind(communication_id)
        .bind(communication_id)
        .bind(after_id.unwrap_or(0))
        .bind(limit)
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(NativeMailboxResolutionAuditRecord {
                    id: row.try_get("id")?,
                    receiver_thread_id: row.try_get("receiver_thread_id")?,
                    communication_id: row.try_get("communication_id")?,
                    command_id: row.try_get("command_id")?,
                    resolution_generation: row.try_get("resolution_generation")?,
                    action: row.try_get("action")?,
                    actor: row.try_get("actor")?,
                    reason: row.try_get("reason")?,
                    pre_status: row.try_get("pre_status")?,
                    pre_attempt_count: row.try_get("pre_attempt_count")?,
                    pre_failure_fingerprint: row.try_get("pre_failure_fingerprint")?,
                    pre_last_progress_ref: row.try_get("pre_last_progress_ref")?,
                    pre_quarantine_reason: row.try_get("pre_quarantine_reason")?,
                    pre_payload_hash: row.try_get("pre_payload_hash")?,
                    resulting_status: row.try_get("resulting_status")?,
                    replacement_communication_id: row.try_get("replacement_communication_id")?,
                    created_at_ms: row.try_get("created_at_ms")?,
                })
            })
            .collect()
    }

    pub async fn get_native_mailbox_resolution_audit(
        &self,
        receiver_thread_id: &str,
        command_id: &str,
    ) -> anyhow::Result<Option<NativeMailboxResolutionAuditRecord>> {
        let row = sqlx::query(
            r#"SELECT id, receiver_thread_id, communication_id, command_id,
                      resolution_generation, action, actor, reason, pre_status,
                      pre_attempt_count, pre_failure_fingerprint, pre_last_progress_ref,
                      pre_quarantine_reason, pre_payload_hash, resulting_status,
                      replacement_communication_id, created_at_ms
               FROM native_mailbox_resolution_commands
               WHERE receiver_thread_id = ? AND command_id = ?"#,
        )
        .bind(receiver_thread_id)
        .bind(command_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.map(native_mailbox_resolution_audit_from_row)
            .transpose()
    }

    pub async fn mark_native_mailbox_communication_consumed(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
        updated_at_ms: i64,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
UPDATE native_mailbox_communications
SET status = 'consumed', updated_at_ms = ?
WHERE receiver_thread_id = ? AND communication_id = ? AND status = 'pending'
            "#,
        )
        .bind(updated_at_ms)
        .bind(receiver_thread_id)
        .bind(communication_id)
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn claim_native_mailbox_communication_for_recovery(
        &self,
        receiver_thread_id: &str,
        communication_id: &str,
        max_recovery_attempts: i64,
        updated_at_ms: i64,
    ) -> anyhow::Result<Option<NativeMailboxRecoveryOutcome>> {
        anyhow::ensure!(
            max_recovery_attempts > 0,
            "native mailbox recovery budget must be positive"
        );
        let quarantine_reason = format!(
            "native mailbox recovery budget exhausted after {max_recovery_attempts} attempts without durable progress"
        );
        let row = sqlx::query(
            r#"
UPDATE native_mailbox_communications
SET attempt_count = attempt_count + 1,
    status = CASE
        WHEN attempt_count + 1 > ? THEN 'quarantined'
        ELSE status
    END,
    failure_fingerprint = CASE
        WHEN attempt_count + 1 > ? THEN 'native_mailbox_recovery_without_progress'
        ELSE failure_fingerprint
    END,
    quarantine_reason = CASE
        WHEN attempt_count + 1 > ? THEN ?
        ELSE quarantine_reason
    END,
    updated_at_ms = ?
WHERE receiver_thread_id = ? AND communication_id = ? AND status = 'pending'
RETURNING receiver_thread_id, communication_id, source_call_id, submission_id,
    communication_json, payload_hash, status, attempt_count,
    failure_fingerprint, last_progress_ref, quarantine_reason,
    created_at_ms, updated_at_ms
            "#,
        )
        .bind(max_recovery_attempts)
        .bind(max_recovery_attempts)
        .bind(max_recovery_attempts)
        .bind(quarantine_reason)
        .bind(updated_at_ms)
        .bind(receiver_thread_id)
        .bind(communication_id)
        .fetch_optional(self.pool.as_ref())
        .await?;

        let Some(record) = row.map(native_mailbox_record_from_row).transpose()? else {
            return Ok(None);
        };
        if record.status == "quarantined" {
            Ok(Some(NativeMailboxRecoveryOutcome::Quarantined(record)))
        } else {
            Ok(Some(NativeMailboxRecoveryOutcome::Claimed(record)))
        }
    }
}

fn native_mailbox_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> anyhow::Result<NativeMailboxCommunicationRecord> {
    Ok(NativeMailboxCommunicationRecord {
        receiver_thread_id: row.try_get("receiver_thread_id")?,
        communication_id: row.try_get("communication_id")?,
        source_call_id: row.try_get("source_call_id")?,
        submission_id: row.try_get("submission_id")?,
        communication_json: row.try_get("communication_json")?,
        payload_hash: row.try_get("payload_hash")?,
        status: row.try_get("status")?,
        attempt_count: row.try_get("attempt_count")?,
        failure_fingerprint: row.try_get("failure_fingerprint")?,
        last_progress_ref: row.try_get("last_progress_ref")?,
        quarantine_reason: row.try_get("quarantine_reason")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn native_mailbox_resolution_audit_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> anyhow::Result<NativeMailboxResolutionAuditRecord> {
    Ok(NativeMailboxResolutionAuditRecord {
        id: row.try_get("id")?,
        receiver_thread_id: row.try_get("receiver_thread_id")?,
        communication_id: row.try_get("communication_id")?,
        command_id: row.try_get("command_id")?,
        resolution_generation: row.try_get("resolution_generation")?,
        action: row.try_get("action")?,
        actor: row.try_get("actor")?,
        reason: row.try_get("reason")?,
        pre_status: row.try_get("pre_status")?,
        pre_attempt_count: row.try_get("pre_attempt_count")?,
        pre_failure_fingerprint: row.try_get("pre_failure_fingerprint")?,
        pre_last_progress_ref: row.try_get("pre_last_progress_ref")?,
        pre_quarantine_reason: row.try_get("pre_quarantine_reason")?,
        pre_payload_hash: row.try_get("pre_payload_hash")?,
        resulting_status: row.try_get("resulting_status")?,
        replacement_communication_id: row.try_get("replacement_communication_id")?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}

#[cfg(test)]
mod tests {
    use super::test_support::unique_temp_dir;
    use super::*;
    use crate::ThreadMetadataBuilder;
    use chrono::Utc;
    use codex_protocol::ThreadId;
    use codex_protocol::protocol::SessionSource;

    #[tokio::test]
    async fn native_mailbox_pending_record_is_idempotent_and_consumable() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
            .await
            .expect("initialize runtime");
        let thread_id =
            ThreadId::from_string("00000000-0000-4000-8000-000000000629").expect("valid thread id");
        let mut builder = ThreadMetadataBuilder::new(
            thread_id,
            codex_home.join("sessions").join("receiver.jsonl"),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.cwd = codex_home.clone();
        runtime
            .upsert_thread(&builder.build("test-provider"))
            .await
            .expect("persist receiver thread");

        let now = Utc::now().timestamp_millis();
        let record = NativeMailboxCommunicationRecord {
            receiver_thread_id: thread_id.to_string(),
            communication_id: "communication-1".to_string(),
            source_call_id: Some("source-call-1".to_string()),
            submission_id: Some("submission-1".to_string()),
            communication_json: r#"{"content":"durable"}"#.to_string(),
            payload_hash: "sha256:durable".to_string(),
            status: "pending".to_string(),
            attempt_count: 0,
            failure_fingerprint: None,
            last_progress_ref: None,
            quarantine_reason: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        assert_eq!(
            runtime
                .insert_pending_native_mailbox_communication(&record)
                .await
                .expect("insert pending communication"),
            NativeMailboxInsertOutcome::Inserted
        );
        assert_eq!(
            runtime
                .insert_pending_native_mailbox_communication(&record)
                .await
                .expect("retry identical communication"),
            NativeMailboxInsertOutcome::Existing
        );
        assert_eq!(
            runtime
                .list_pending_native_mailbox_communications(&thread_id.to_string())
                .await
                .expect("list pending communications"),
            vec![record.clone()]
        );

        let mut conflicting = record.clone();
        conflicting.communication_json = r#"{"content":"different"}"#.to_string();
        assert!(
            runtime
                .insert_pending_native_mailbox_communication(&conflicting)
                .await
                .expect_err("reject conflicting retry")
                .to_string()
                .contains("different payload")
        );

        assert!(
            runtime
                .mark_native_mailbox_communication_consumed(
                    &thread_id.to_string(),
                    &record.communication_id,
                    now + 1,
                )
                .await
                .expect("consume communication")
        );
        assert!(
            runtime
                .list_pending_native_mailbox_communications(&thread_id.to_string())
                .await
                .expect("list after consumption")
                .is_empty()
        );
        assert_eq!(
            runtime
                .get_native_mailbox_communication(&thread_id.to_string(), &record.communication_id,)
                .await
                .expect("read consumed communication")
                .expect("communication remains auditable")
                .status,
            "consumed"
        );

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn native_mailbox_recovery_budget_quarantines_without_reenqueue() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
            .await
            .expect("initialize runtime");
        let thread_id =
            ThreadId::from_string("00000000-0000-4000-8000-000000000630").expect("valid thread id");
        let mut builder = ThreadMetadataBuilder::new(
            thread_id,
            codex_home.join("sessions").join("receiver.jsonl"),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.cwd = codex_home.clone();
        runtime
            .upsert_thread(&builder.build("test-provider"))
            .await
            .expect("persist receiver thread");

        let now = Utc::now().timestamp_millis();
        let record = NativeMailboxCommunicationRecord {
            receiver_thread_id: thread_id.to_string(),
            communication_id: "poison-communication".to_string(),
            source_call_id: Some("poison-communication".to_string()),
            submission_id: Some("submission-poison".to_string()),
            communication_json: r#"{"content":"poison"}"#.to_string(),
            payload_hash: "sha256:poison".to_string(),
            status: "pending".to_string(),
            attempt_count: 0,
            failure_fingerprint: None,
            last_progress_ref: None,
            quarantine_reason: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        runtime
            .insert_pending_native_mailbox_communication(&record)
            .await
            .expect("insert poison communication");

        for expected_attempt in 1..=3 {
            let outcome = runtime
                .claim_native_mailbox_communication_for_recovery(
                    &thread_id.to_string(),
                    &record.communication_id,
                    3,
                    now + expected_attempt,
                )
                .await
                .expect("claim recovery")
                .expect("pending communication must be claimable");
            let NativeMailboxRecoveryOutcome::Claimed(claimed) = outcome else {
                panic!("attempt {expected_attempt} must remain recoverable");
            };
            assert_eq!(claimed.attempt_count, expected_attempt);
        }

        let outcome = runtime
            .claim_native_mailbox_communication_for_recovery(
                &thread_id.to_string(),
                &record.communication_id,
                3,
                now + 4,
            )
            .await
            .expect("exhaust recovery budget")
            .expect("pending communication must transition to quarantine");
        let NativeMailboxRecoveryOutcome::Quarantined(quarantined) = outcome else {
            panic!("fourth recovery must quarantine the communication");
        };
        assert_eq!(quarantined.attempt_count, 4);
        assert_eq!(
            quarantined.failure_fingerprint.as_deref(),
            Some("native_mailbox_recovery_without_progress")
        );
        assert!(
            quarantined
                .quarantine_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("after 3 attempts"))
        );
        assert!(
            runtime
                .list_pending_native_mailbox_communications(&thread_id.to_string())
                .await
                .expect("list pending after quarantine")
                .is_empty()
        );
        assert!(
            runtime
                .claim_native_mailbox_communication_for_recovery(
                    &thread_id.to_string(),
                    &record.communication_id,
                    3,
                    now + 5,
                )
                .await
                .expect("quarantined row is not claimable")
                .is_none()
        );

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn native_mailbox_quarantine_resolutions_are_atomic_and_idempotent() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
            .await
            .expect("initialize runtime");
        let thread_id =
            ThreadId::from_string("00000000-0000-4000-8000-000000000631").expect("valid thread id");
        let mut builder = ThreadMetadataBuilder::new(
            thread_id,
            codex_home.join("sessions").join("receiver.jsonl"),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.cwd = codex_home.clone();
        runtime
            .upsert_thread(&builder.build("test-provider"))
            .await
            .expect("persist receiver thread");
        let now = Utc::now().timestamp_millis();

        for (index, action) in [
            NativeMailboxResolutionAction::Retry,
            NativeMailboxResolutionAction::Skip,
            NativeMailboxResolutionAction::Abort,
            NativeMailboxResolutionAction::Replace,
        ]
        .into_iter()
        .enumerate()
        {
            let communication_id = format!("quarantined-{index}");
            let record = NativeMailboxCommunicationRecord {
                receiver_thread_id: thread_id.to_string(),
                communication_id: communication_id.clone(),
                source_call_id: Some(communication_id.clone()),
                submission_id: Some(format!("submission-{index}")),
                communication_json: format!(r#"{{"content":"payload-{index}"}}"#),
                payload_hash: format!("sha256:{index}"),
                status: "pending".to_string(),
                attempt_count: 0,
                failure_fingerprint: None,
                last_progress_ref: None,
                quarantine_reason: None,
                created_at_ms: now,
                updated_at_ms: now,
            };
            runtime
                .insert_pending_native_mailbox_communication(&record)
                .await
                .expect("insert communication");
            for attempt in 1..=4 {
                runtime
                    .claim_native_mailbox_communication_for_recovery(
                        &thread_id.to_string(),
                        &communication_id,
                        3,
                        now + attempt,
                    )
                    .await
                    .expect("advance recovery budget");
            }
            let replacement = matches!(action, NativeMailboxResolutionAction::Replace).then(|| {
                let replacement_id = format!("replacement-{index}");
                NativeMailboxCommunicationRecord {
                    receiver_thread_id: thread_id.to_string(),
                    communication_id: replacement_id.clone(),
                    source_call_id: Some(replacement_id),
                    submission_id: Some(format!("replacement-submission-{index}")),
                    communication_json: format!(r#"{{"content":"replacement-{index}"}}"#),
                    payload_hash: format!("sha256:replacement-{index}"),
                    status: "pending".to_string(),
                    attempt_count: 0,
                    failure_fingerprint: None,
                    last_progress_ref: None,
                    quarantine_reason: None,
                    created_at_ms: now + 10,
                    updated_at_ms: now + 10,
                }
            });
            let command = NativeMailboxResolutionCommand {
                receiver_thread_id: thread_id.to_string(),
                communication_id: communication_id.clone(),
                command_id: format!("command-{index}"),
                action,
                actor: "operator:test".to_string(),
                reason: "deterministic resolution".to_string(),
                replacement,
                created_at_ms: now + 20,
            };
            let outcome = runtime
                .resolve_native_mailbox_quarantine(&command)
                .await
                .expect("resolve quarantine");
            assert!(!outcome.existing);
            let replay = runtime
                .resolve_native_mailbox_quarantine(&command)
                .await
                .expect("repeat resolution command");
            assert!(replay.existing);
            assert_eq!(replay.resulting_status, outcome.resulting_status);

            let resolved = runtime
                .get_native_mailbox_communication(&thread_id.to_string(), &communication_id)
                .await
                .expect("read resolved communication")
                .expect("resolved communication remains auditable");
            match action {
                NativeMailboxResolutionAction::Retry => {
                    assert_eq!(resolved.status, "pending");
                    assert_eq!(resolved.attempt_count, 0);
                }
                NativeMailboxResolutionAction::Skip => assert_eq!(resolved.status, "skipped"),
                NativeMailboxResolutionAction::Abort | NativeMailboxResolutionAction::Replace => {
                    assert_eq!(resolved.status, "aborted")
                }
            }
            if let Some(replacement_id) = outcome.replacement_communication_id {
                assert_eq!(
                    runtime
                        .get_native_mailbox_communication(&thread_id.to_string(), &replacement_id)
                        .await
                        .expect("read replacement")
                        .expect("replacement must exist")
                        .status,
                    "pending"
                );
            }
        }
        assert!(
            runtime
                .list_quarantined_native_mailbox_communications(Some(&thread_id.to_string()))
                .await
                .expect("list quarantines after resolution")
                .is_empty()
        );
        let first_page = runtime
            .list_native_mailbox_resolution_audit(Some(&thread_id.to_string()), None, None, 2)
            .await
            .expect("read first audit page");
        assert_eq!(first_page.len(), 2);
        assert!(first_page.iter().all(|record| {
            record.pre_status == "quarantined"
                && record.pre_attempt_count == 4
                && record.pre_failure_fingerprint.as_deref()
                    == Some("native_mailbox_recovery_without_progress")
        }));
        let second_page = runtime
            .list_native_mailbox_resolution_audit(
                Some(&thread_id.to_string()),
                None,
                first_page.last().map(|record| record.id),
                2,
            )
            .await
            .expect("read second audit page");
        assert_eq!(second_page.len(), 2);
        assert!(second_page[0].id > first_page[1].id);
        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn native_mailbox_ten_concurrent_replacements_return_one_stable_winner() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
            .await
            .expect("initialize runtime");
        let thread_id =
            ThreadId::from_string("00000000-0000-4000-8000-000000000635").expect("valid thread id");
        let mut builder = ThreadMetadataBuilder::new(
            thread_id,
            codex_home.join("sessions").join("receiver.jsonl"),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.cwd = codex_home.clone();
        runtime
            .upsert_thread(&builder.build("test-provider"))
            .await
            .expect("persist receiver thread");

        let now = Utc::now().timestamp_millis();
        let communication_id = "concurrent-resolution";
        runtime
            .insert_pending_native_mailbox_communication(&NativeMailboxCommunicationRecord {
                receiver_thread_id: thread_id.to_string(),
                communication_id: communication_id.to_string(),
                source_call_id: Some(communication_id.to_string()),
                submission_id: Some("concurrent-submission".to_string()),
                communication_json: r#"{"content":"concurrent"}"#.to_string(),
                payload_hash: "sha256:concurrent".to_string(),
                status: "pending".to_string(),
                attempt_count: 0,
                failure_fingerprint: None,
                last_progress_ref: None,
                quarantine_reason: None,
                created_at_ms: now,
                updated_at_ms: now,
            })
            .await
            .expect("insert communication");
        for attempt in 1..=4 {
            runtime
                .claim_native_mailbox_communication_for_recovery(
                    &thread_id.to_string(),
                    communication_id,
                    3,
                    now + attempt,
                )
                .await
                .expect("advance recovery budget");
        }

        let mut runtimes = vec![runtime.clone()];
        for _ in 1..10 {
            runtimes.push(
                StateRuntime::init(codex_home.clone(), "test-provider".to_string())
                    .await
                    .expect("open independent runtime"),
            );
        }
        let commands = (0..10)
            .map(|index| NativeMailboxResolutionCommand {
                receiver_thread_id: thread_id.to_string(),
                communication_id: communication_id.to_string(),
                command_id: format!("command-concurrent-{index}"),
                action: NativeMailboxResolutionAction::Replace,
                actor: "operator:test".to_string(),
                reason: "concurrent replacement".to_string(),
                replacement: Some(NativeMailboxCommunicationRecord {
                    receiver_thread_id: thread_id.to_string(),
                    communication_id: format!("concurrent-replacement-{index}"),
                    source_call_id: Some(format!("concurrent-replacement-{index}")),
                    submission_id: Some(format!("replacement-submission-{index}")),
                    communication_json: format!(r#"{{"content":"replacement-{index}"}}"#),
                    payload_hash: format!("sha256:replacement-{index}"),
                    status: "pending".to_string(),
                    attempt_count: 0,
                    failure_fingerprint: None,
                    last_progress_ref: None,
                    quarantine_reason: None,
                    created_at_ms: now + 10,
                    updated_at_ms: now + 10,
                }),
                created_at_ms: now + 10,
            })
            .collect::<Vec<_>>();
        let mut tasks = tokio::task::JoinSet::new();
        for (runtime, command) in runtimes.into_iter().zip(commands.iter().cloned()) {
            tasks.spawn(async move { runtime.resolve_native_mailbox_quarantine(&command).await });
        }
        let mut outcomes = Vec::new();
        while let Some(result) = tasks.join_next().await {
            outcomes.push(
                result
                    .expect("resolution task completes")
                    .expect("resolution returns a business outcome"),
            );
        }
        assert_eq!(
            outcomes.iter().filter(|outcome| !outcome.conflict).count(),
            1
        );
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.conflict).count(),
            9
        );
        let winner = outcomes
            .iter()
            .find(|outcome| !outcome.conflict)
            .expect("one command wins");
        assert!(
            outcomes
                .iter()
                .filter(|outcome| outcome.conflict)
                .all(|loser| {
                    loser.winner_command_id.as_deref() == Some(winner.command_id.as_str())
                        && loser.replacement_communication_id == winner.replacement_communication_id
                })
        );
        let mut replacement_count = 0;
        for index in 0..10 {
            if runtime
                .get_native_mailbox_communication(
                    &thread_id.to_string(),
                    &format!("concurrent-replacement-{index}"),
                )
                .await
                .expect("read replacement")
                .is_some()
            {
                replacement_count += 1;
            }
        }
        assert_eq!(replacement_count, 1);

        let loser = outcomes
            .iter()
            .find(|outcome| outcome.conflict)
            .expect("one command conflicts");
        drop(runtime);
        let reopened = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
            .await
            .expect("reopen runtime after contention");
        let losing_command = commands
            .iter()
            .find(|command| command.command_id == loser.command_id)
            .expect("find losing command");
        let repeated_loser = reopened
            .resolve_native_mailbox_quarantine(losing_command)
            .await
            .expect("repeat losing command");
        assert!(repeated_loser.conflict);
        assert_eq!(repeated_loser.winner_command_id, loser.winner_command_id);
        assert_eq!(
            reopened
                .list_native_mailbox_resolution_audit(
                    Some(&thread_id.to_string()),
                    Some(communication_id),
                    None,
                    10,
                )
                .await
                .expect("list winning audit")
                .len(),
            1
        );

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn native_mailbox_failed_audit_rolls_back_replacement_and_original_update() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
            .await
            .expect("initialize runtime");
        let thread_id =
            ThreadId::from_string("00000000-0000-4000-8000-000000000636").expect("valid thread id");
        let mut builder = ThreadMetadataBuilder::new(
            thread_id,
            codex_home.join("sessions").join("receiver.jsonl"),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.cwd = codex_home.clone();
        runtime
            .upsert_thread(&builder.build("test-provider"))
            .await
            .expect("persist receiver thread");
        let now = Utc::now().timestamp_millis();
        let original_id = "faulted-resolution";
        let replacement_id = "faulted-resolution-replacement";
        runtime
            .insert_pending_native_mailbox_communication(&NativeMailboxCommunicationRecord {
                receiver_thread_id: thread_id.to_string(),
                communication_id: original_id.to_string(),
                source_call_id: Some(original_id.to_string()),
                submission_id: Some("faulted-submission".to_string()),
                communication_json: r#"{"content":"original"}"#.to_string(),
                payload_hash: "sha256:original".to_string(),
                status: "pending".to_string(),
                attempt_count: 0,
                failure_fingerprint: None,
                last_progress_ref: None,
                quarantine_reason: None,
                created_at_ms: now,
                updated_at_ms: now,
            })
            .await
            .expect("insert original");
        for attempt in 1..=4 {
            runtime
                .claim_native_mailbox_communication_for_recovery(
                    &thread_id.to_string(),
                    original_id,
                    3,
                    now + attempt,
                )
                .await
                .expect("advance recovery budget");
        }
        sqlx::query(
            r#"CREATE TRIGGER fail_native_mailbox_resolution_audit
               BEFORE INSERT ON native_mailbox_resolution_commands
               BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END"#,
        )
        .execute(runtime.pool.as_ref())
        .await
        .expect("install transaction fault");
        let command = NativeMailboxResolutionCommand {
            receiver_thread_id: thread_id.to_string(),
            communication_id: original_id.to_string(),
            command_id: "command-faulted-replace".to_string(),
            action: NativeMailboxResolutionAction::Replace,
            actor: "operator:test".to_string(),
            reason: "test rollback".to_string(),
            replacement: Some(NativeMailboxCommunicationRecord {
                receiver_thread_id: thread_id.to_string(),
                communication_id: replacement_id.to_string(),
                source_call_id: Some(replacement_id.to_string()),
                submission_id: Some("faulted-replacement-submission".to_string()),
                communication_json: r#"{"content":"replacement"}"#.to_string(),
                payload_hash: "sha256:replacement".to_string(),
                status: "pending".to_string(),
                attempt_count: 0,
                failure_fingerprint: None,
                last_progress_ref: None,
                quarantine_reason: None,
                created_at_ms: now + 10,
                updated_at_ms: now + 10,
            }),
            created_at_ms: now + 10,
        };
        assert!(
            runtime
                .resolve_native_mailbox_quarantine(&command)
                .await
                .expect_err("audit fault aborts resolution")
                .to_string()
                .contains("injected audit failure")
        );
        assert_eq!(
            runtime
                .get_native_mailbox_communication(&thread_id.to_string(), original_id)
                .await
                .expect("read original")
                .expect("original remains")
                .status,
            "quarantined"
        );
        assert!(
            runtime
                .get_native_mailbox_communication(&thread_id.to_string(), replacement_id)
                .await
                .expect("read replacement")
                .is_none()
        );
        assert!(
            runtime
                .list_native_mailbox_resolution_audit(
                    Some(&thread_id.to_string()),
                    Some(original_id),
                    None,
                    10,
                )
                .await
                .expect("read audit")
                .is_empty()
        );

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }
}
