ALTER TABLE native_mailbox_resolution_commands ADD COLUMN resolution_generation INTEGER;
ALTER TABLE native_mailbox_resolution_commands ADD COLUMN pre_status TEXT;
ALTER TABLE native_mailbox_resolution_commands ADD COLUMN pre_attempt_count INTEGER;
ALTER TABLE native_mailbox_resolution_commands ADD COLUMN pre_failure_fingerprint TEXT;
ALTER TABLE native_mailbox_resolution_commands ADD COLUMN pre_last_progress_ref TEXT;
ALTER TABLE native_mailbox_resolution_commands ADD COLUMN pre_quarantine_reason TEXT;
ALTER TABLE native_mailbox_resolution_commands ADD COLUMN pre_payload_hash TEXT;

CREATE INDEX idx_native_mailbox_resolution_page
    ON native_mailbox_resolution_commands(receiver_thread_id, communication_id, id);
