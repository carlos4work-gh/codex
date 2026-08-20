use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArenaSnapshotRecord {
    pub arena_id: String,
    pub concierge_thread_id: String,
    pub schema_version: i64,
    pub snapshot_sequence: i64,
    pub last_event_hash: String,
    pub snapshot_json: String,
    pub updated_at_ms: i64,
}

impl StateRuntime {
    pub async fn get_arena_snapshot(
        &self,
        arena_id: &str,
    ) -> anyhow::Result<Option<ArenaSnapshotRecord>> {
        let row = sqlx::query(
            r#"
SELECT arena_id, concierge_thread_id, schema_version, snapshot_sequence,
    last_event_hash, snapshot_json, updated_at_ms
FROM arena_snapshots
WHERE arena_id = ?
            "#,
        )
        .bind(arena_id)
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| {
            Ok(ArenaSnapshotRecord {
                arena_id: row.try_get("arena_id")?,
                concierge_thread_id: row.try_get("concierge_thread_id")?,
                schema_version: row.try_get("schema_version")?,
                snapshot_sequence: row.try_get("snapshot_sequence")?,
                last_event_hash: row.try_get("last_event_hash")?,
                snapshot_json: row.try_get("snapshot_json")?,
                updated_at_ms: row.try_get("updated_at_ms")?,
            })
        })
        .transpose()
    }

    pub async fn list_arena_snapshots(&self) -> anyhow::Result<Vec<ArenaSnapshotRecord>> {
        let rows = sqlx::query(
            r#"
SELECT arena_id, concierge_thread_id, schema_version, snapshot_sequence,
    last_event_hash, snapshot_json, updated_at_ms
FROM arena_snapshots
ORDER BY arena_id
            "#,
        )
        .fetch_all(self.pool.as_ref())
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ArenaSnapshotRecord {
                    arena_id: row.try_get("arena_id")?,
                    concierge_thread_id: row.try_get("concierge_thread_id")?,
                    schema_version: row.try_get("schema_version")?,
                    snapshot_sequence: row.try_get("snapshot_sequence")?,
                    last_event_hash: row.try_get("last_event_hash")?,
                    snapshot_json: row.try_get("snapshot_json")?,
                    updated_at_ms: row.try_get("updated_at_ms")?,
                })
            })
            .collect()
    }

    pub async fn upsert_arena_snapshot(
        &self,
        snapshot: &ArenaSnapshotRecord,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO arena_snapshots (
    arena_id,
    concierge_thread_id,
    schema_version,
    snapshot_sequence,
    last_event_hash,
    snapshot_json,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(arena_id) DO UPDATE SET
    concierge_thread_id = excluded.concierge_thread_id,
    schema_version = excluded.schema_version,
    snapshot_sequence = excluded.snapshot_sequence,
    last_event_hash = excluded.last_event_hash,
    snapshot_json = excluded.snapshot_json,
    updated_at_ms = excluded.updated_at_ms
WHERE excluded.snapshot_sequence >= arena_snapshots.snapshot_sequence
            "#,
        )
        .bind(&snapshot.arena_id)
        .bind(&snapshot.concierge_thread_id)
        .bind(snapshot.schema_version)
        .bind(snapshot.snapshot_sequence)
        .bind(&snapshot.last_event_hash)
        .bind(&snapshot.snapshot_json)
        .bind(snapshot.updated_at_ms)
        .execute(self.pool.as_ref())
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ArenaSnapshotRecord;
    use super::StateRuntime;
    use super::test_support::unique_temp_dir;
    use crate::ThreadMetadataBuilder;
    use chrono::Utc;
    use codex_protocol::ThreadId;
    use codex_protocol::protocol::SessionSource;
    use codex_utils_absolute_path::test_support::PathExt;

    #[tokio::test]
    async fn arena_snapshot_round_trips_without_semantic_concierge_state() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(
            crate::SqliteConfig::new_for_testing(codex_home.as_path().abs()),
            "test-provider".to_string(),
        )
        .await
        .expect("initialize runtime");
        let concierge_thread_id = ThreadId::from_string("00000000-0000-4000-8000-000000000620")
            .expect("valid concierge thread id");
        let mut builder = ThreadMetadataBuilder::new(
            concierge_thread_id,
            codex_home.join("sessions").join("concierge.jsonl"),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.cwd = codex_home.clone();
        runtime
            .upsert_thread(&builder.build("test-provider"))
            .await
            .expect("persist concierge thread");

        let snapshot = ArenaSnapshotRecord {
            arena_id: "arena-620".to_string(),
            concierge_thread_id: concierge_thread_id.to_string(),
            schema_version: 1,
            snapshot_sequence: 7,
            last_event_hash: "sha256:event-7".to_string(),
            snapshot_json: r#"{"arena_id":"arena-620","round_id":"round-1","phase":"proposal"}"#
                .to_string(),
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        runtime
            .upsert_arena_snapshot(&snapshot)
            .await
            .expect("persist arena snapshot");

        assert_eq!(
            runtime
                .get_arena_snapshot("arena-620")
                .await
                .expect("read arena snapshot"),
            Some(snapshot.clone())
        );
        assert_eq!(
            runtime
                .list_arena_snapshots()
                .await
                .expect("list arena snapshots"),
            vec![snapshot.clone()]
        );

        let mut newer_snapshot = snapshot.clone();
        newer_snapshot.snapshot_sequence = 8;
        newer_snapshot.last_event_hash = "sha256:event-8".to_string();
        runtime
            .upsert_arena_snapshot(&newer_snapshot)
            .await
            .expect("advance arena snapshot");
        runtime
            .upsert_arena_snapshot(&snapshot)
            .await
            .expect("ignore stale arena snapshot replay");
        assert_eq!(
            runtime
                .get_arena_snapshot("arena-620")
                .await
                .expect("read advanced arena snapshot"),
            Some(newer_snapshot)
        );

        runtime
            .delete_thread(concierge_thread_id)
            .await
            .expect("delete OOTB concierge thread");
        assert_eq!(
            runtime
                .get_arena_snapshot("arena-620")
                .await
                .expect("read cascaded arena snapshot"),
            None
        );

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }
}
