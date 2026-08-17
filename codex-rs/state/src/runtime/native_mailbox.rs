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
}
