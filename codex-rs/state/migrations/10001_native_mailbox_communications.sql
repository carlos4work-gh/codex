CREATE TABLE native_mailbox_communications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receiver_thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    communication_id TEXT NOT NULL,
    source_call_id TEXT,
    submission_id TEXT,
    communication_json TEXT NOT NULL CHECK(json_valid(communication_json)),
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'consumed', 'quarantined', 'skipped', 'aborted')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    failure_fingerprint TEXT,
    last_progress_ref TEXT,
    quarantine_reason TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(receiver_thread_id, communication_id)
);

CREATE INDEX idx_native_mailbox_receiver_status
    ON native_mailbox_communications(receiver_thread_id, status, id);

CREATE UNIQUE INDEX idx_native_mailbox_receiver_source_call
    ON native_mailbox_communications(receiver_thread_id, source_call_id)
    WHERE source_call_id IS NOT NULL;
