use super::*;

impl MemythosRequestProcessor {
    pub(crate) async fn mailbox_quarantine_list(
        &self,
        params: MemythosMailboxQuarantineListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let communications = state_db
            .list_quarantined_native_mailbox_communications(params.receiver_thread_id.as_deref())
            .await
            .map_err(|error| internal_error(format!("failed to list mailbox quarantine: {error}")))?
            .into_iter()
            .map(mailbox_quarantine_record)
            .collect();
        Ok(MemythosMailboxQuarantineListResponse { communications }.into())
    }

    pub(crate) async fn mailbox_health_get(
        &self,
        params: MemythosMailboxHealthGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let snapshot = state_db
            .native_mailbox_health_snapshot(params.receiver_thread_id.as_deref())
            .await
            .map_err(|error| internal_error(format!("failed to read mailbox health: {error}")))?;
        record_mailbox_health_metrics(&snapshot);
        Ok(MemythosMailboxHealthGetResponse {
            pending_count: snapshot.pending_count,
            quarantined_count: snapshot.quarantined_count,
            consumed_count: snapshot.consumed_count,
            skipped_count: snapshot.skipped_count,
            aborted_count: snapshot.aborted_count,
            max_attempt_count: snapshot.max_attempt_count,
            oldest_pending_updated_at_ms: snapshot.oldest_pending_updated_at_ms,
            oldest_quarantined_updated_at_ms: snapshot.oldest_quarantined_updated_at_ms,
            resolution_count: snapshot.resolution_count,
        }
        .into())
    }

    pub(crate) async fn mailbox_quarantine_get(
        &self,
        params: MemythosMailboxQuarantineGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let communication = state_db
            .get_native_mailbox_communication(&params.receiver_thread_id, &params.communication_id)
            .await
            .map_err(|error| internal_error(format!("failed to read mailbox quarantine: {error}")))?
            .filter(|record| record.status == "quarantined")
            .ok_or_else(|| invalid_params("unknown quarantined mailbox communication"))?;
        Ok(MemythosMailboxQuarantineGetResponse {
            communication: mailbox_quarantine_record(communication),
        }
        .into())
    }

    pub(crate) async fn mailbox_quarantine_resolve(
        &self,
        params: MemythosMailboxQuarantineResolveParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let (action, action_label) = match params.action {
            MemythosMailboxQuarantineResolutionAction::Retry => {
                (NativeMailboxResolutionAction::Retry, "retry")
            }
            MemythosMailboxQuarantineResolutionAction::Skip => {
                (NativeMailboxResolutionAction::Skip, "skip")
            }
            MemythosMailboxQuarantineResolutionAction::Replace => {
                (NativeMailboxResolutionAction::Replace, "replace")
            }
            MemythosMailboxQuarantineResolutionAction::Abort => {
                (NativeMailboxResolutionAction::Abort, "abort")
            }
        };
        let now = chrono::Utc::now().timestamp_millis();
        let replacement = params
            .replacement_message
            .map(|message| {
                if message.to_parent_thread_id != params.receiver_thread_id {
                    return Err(invalid_params(
                        "replacement message must target the same receiver thread",
                    ));
                }
                let mut communication = InterAgentCommunication::new(
                    AgentPath::root(),
                    AgentPath::root(),
                    Vec::new(),
                    build_peer_parent_envelope(&message),
                    message.requires_response,
                );
                communication.id = Some(ResponseItemId::from_server(message.message_id.clone()));
                let communication_json =
                    serde_json::to_string(&communication).map_err(|error| {
                        internal_error(format!("failed to serialize replacement message: {error}"))
                    })?;
                Ok(NativeMailboxCommunicationRecord {
                    receiver_thread_id: params.receiver_thread_id.clone(),
                    communication_id: message.message_id.clone(),
                    source_call_id: Some(message.message_id),
                    submission_id: None,
                    payload_hash: format!(
                        "sha256:{:x}",
                        Sha256::digest(communication_json.as_bytes())
                    ),
                    communication_json,
                    status: "pending".to_string(),
                    attempt_count: 0,
                    failure_fingerprint: None,
                    last_progress_ref: None,
                    quarantine_reason: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                })
            })
            .transpose()?;
        let resolution_started = Instant::now();
        let outcome = match state_db
            .resolve_native_mailbox_quarantine(&NativeMailboxResolutionCommand {
                receiver_thread_id: params.receiver_thread_id,
                communication_id: params.communication_id,
                command_id: params.command_id,
                action,
                actor: params.actor,
                reason: params.reason,
                replacement,
                created_at_ms: now,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                record_mailbox_resolution_metrics(
                    action_label,
                    "operational_error",
                    "not_applicable",
                    resolution_started.elapsed(),
                );
                return Err(invalid_params(format!(
                    "failed to resolve quarantine: {error}"
                )));
            }
        };
        let live_reenqueue_status = if action == NativeMailboxResolutionAction::Retry {
            if outcome.conflict {
                "not_enqueued_conflict".to_string()
            } else if outcome.existing {
                "already_resolved".to_string()
            } else {
                match self
                    .peer_parent_delivery_adapter
                    .reenqueue_native_mailbox_communication(
                        &outcome.receiver_thread_id,
                        &outcome.communication_id,
                    )
                    .await
                {
                    Ok(true) => "enqueued".to_string(),
                    Ok(false) => "deferred_until_resume".to_string(),
                    Err(error) => {
                        warn!("live native mailbox retry deferred: {error}");
                        "deferred_until_resume".to_string()
                    }
                }
            }
        } else {
            "not_applicable".to_string()
        };
        let resolution_outcome = if outcome.conflict {
            "conflict"
        } else if outcome.existing {
            "existing"
        } else {
            "winner"
        };
        record_mailbox_resolution_metrics(
            action_label,
            resolution_outcome,
            &live_reenqueue_status,
            resolution_started.elapsed(),
        );
        Ok(MemythosMailboxQuarantineResolveResponse {
            receiver_thread_id: outcome.receiver_thread_id,
            communication_id: outcome.communication_id,
            command_id: outcome.command_id,
            action: outcome.action,
            resulting_status: outcome.resulting_status,
            replacement_communication_id: outcome.replacement_communication_id,
            existing: outcome.existing,
            live_reenqueue_status,
            conflict: outcome.conflict,
            winner_command_id: outcome.winner_command_id,
        }
        .into())
    }

    pub(crate) async fn mailbox_resolution_list(
        &self,
        params: MemythosMailboxResolutionListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let after_id = params
            .cursor
            .as_deref()
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|_| invalid_params("invalid resolution cursor"))?;
        let limit = params.limit.unwrap_or(50).clamp(1, 100) as usize;
        let mut records = state_db
            .list_native_mailbox_resolution_audit(
                params.receiver_thread_id.as_deref(),
                params.communication_id.as_deref(),
                after_id,
                (limit + 1) as i64,
            )
            .await
            .map_err(|error| internal_error(format!("failed to list resolution audit: {error}")))?;
        let has_more = records.len() > limit;
        records.truncate(limit);
        let next_cursor = has_more
            .then(|| records.last().map(|record| record.id.to_string()))
            .flatten();
        Ok(MemythosMailboxResolutionListResponse {
            resolutions: records
                .into_iter()
                .map(mailbox_resolution_audit_record)
                .collect(),
            next_cursor,
        }
        .into())
    }

    pub(crate) async fn mailbox_resolution_get(
        &self,
        params: MemythosMailboxResolutionGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state_db = self
            .arena_state_db
            .as_ref()
            .ok_or_else(|| internal_error("state DB is unavailable"))?;
        let resolution = state_db
            .get_native_mailbox_resolution_audit(&params.receiver_thread_id, &params.command_id)
            .await
            .map_err(|error| internal_error(format!("failed to read resolution audit: {error}")))?
            .ok_or_else(|| invalid_params("unknown mailbox resolution command"))?;
        Ok(MemythosMailboxResolutionGetResponse {
            resolution: mailbox_resolution_audit_record(resolution),
        }
        .into())
    }
}

fn mailbox_quarantine_record(
    record: NativeMailboxCommunicationRecord,
) -> MemythosMailboxQuarantineRecord {
    MemythosMailboxQuarantineRecord {
        receiver_thread_id: record.receiver_thread_id,
        communication_id: record.communication_id,
        source_call_id: record.source_call_id,
        payload_hash: record.payload_hash,
        status: record.status,
        attempt_count: record.attempt_count,
        failure_fingerprint: record.failure_fingerprint,
        last_progress_ref: record.last_progress_ref,
        quarantine_reason: record.quarantine_reason,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    }
}

fn mailbox_resolution_audit_record(
    record: NativeMailboxResolutionAuditRecord,
) -> MemythosMailboxResolutionAuditRecord {
    MemythosMailboxResolutionAuditRecord {
        id: record.id,
        receiver_thread_id: record.receiver_thread_id,
        communication_id: record.communication_id,
        command_id: record.command_id,
        resolution_generation: record.resolution_generation,
        action: record.action,
        actor: record.actor,
        reason: record.reason,
        pre_status: record.pre_status,
        pre_attempt_count: record.pre_attempt_count,
        pre_failure_fingerprint: record.pre_failure_fingerprint,
        pre_last_progress_ref: record.pre_last_progress_ref,
        pre_quarantine_reason: record.pre_quarantine_reason,
        pre_payload_hash: record.pre_payload_hash,
        resulting_status: record.resulting_status,
        replacement_communication_id: record.replacement_communication_id,
        created_at_ms: record.created_at_ms,
    }
}
