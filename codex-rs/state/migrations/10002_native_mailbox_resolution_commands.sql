CREATE TABLE native_mailbox_resolution_commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receiver_thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    communication_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('retry', 'skip', 'replace', 'abort')),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    replacement_communication_id TEXT,
    resulting_status TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(receiver_thread_id, command_id),
    FOREIGN KEY(receiver_thread_id, communication_id)
        REFERENCES native_mailbox_communications(receiver_thread_id, communication_id)
);

CREATE INDEX idx_native_mailbox_resolution_communication
    ON native_mailbox_resolution_commands(receiver_thread_id, communication_id, id);
