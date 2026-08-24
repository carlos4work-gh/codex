CREATE TABLE native_mailbox_staged_communications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receiver_thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    communication_id TEXT NOT NULL,
    source_call_id TEXT,
    submission_id TEXT,
    communication_json TEXT NOT NULL CHECK(json_valid(communication_json)),
    payload_hash TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(receiver_thread_id, communication_id)
);

CREATE UNIQUE INDEX idx_native_mailbox_staged_receiver_source_call
    ON native_mailbox_staged_communications(receiver_thread_id, source_call_id)
    WHERE source_call_id IS NOT NULL;
