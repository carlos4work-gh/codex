CREATE TABLE arena_snapshots (
    arena_id TEXT PRIMARY KEY NOT NULL,
    concierge_thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    schema_version INTEGER NOT NULL CHECK(schema_version > 0),
    snapshot_sequence INTEGER NOT NULL CHECK(snapshot_sequence >= 0),
    last_event_hash TEXT NOT NULL,
    snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
    updated_at_ms INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_arena_snapshots_concierge_thread
    ON arena_snapshots(concierge_thread_id);
