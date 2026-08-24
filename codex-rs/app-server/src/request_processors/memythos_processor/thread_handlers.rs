use super::*;

impl MemythosRequestProcessor {
    pub(crate) async fn thread_consolidate(
        &self,
        params: MemythosThreadConsolidateParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        validate_thread_consolidation_request(&params)?;
        let attempt = self
            .thread_consolidation_adapter
            .consolidate_threads(&params)
            .await;
        let consolidation_turn_id = attempt
            .consolidation_turn_id
            .clone()
            .unwrap_or_else(|| "unavailable".to_string());
        let event_ref = format!(
            "app-server://threads/{}/turns/{}/memythos-thread-consolidation",
            params.coordinator_thread_id, consolidation_turn_id
        );
        let mut state = self.state.lock().await;
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ThreadConsolidation,
            if attempt.used_thread_turns_summary {
                MemythosTelemetrySource::AppServerNative
            } else {
                MemythosTelemetrySource::MemythosRuntimeState
            },
            None,
            None,
            Some(params.coordinator_thread_id.clone()),
            Some(event_ref),
            attempt.agent_message_ref.clone(),
            if attempt.blockers.is_empty() {
                MemythosEventChannel::HumanHighlight
            } else {
                MemythosEventChannel::TechnicalDetail
            },
            format!(
                "Thread consolidation for coordinator {} used {} source thread(s).",
                params.coordinator_thread_id,
                params.source_thread_ids.len()
            ),
        );

        Ok(MemythosThreadConsolidateResponse {
            consolidation_turn_id,
            coordinator_thread_id: params.coordinator_thread_id,
            source_refs: attempt.source_refs,
            agent_message_ref: attempt.agent_message_ref,
            structured_output_ref: attempt.structured_output_ref,
            technical_evidence_refs: attempt.technical_evidence_refs,
            source_method: attempt.source_method,
            used_thread_turns_summary: attempt.used_thread_turns_summary,
            blockers: attempt.blockers,
        }
        .into())
    }

    pub(crate) async fn thread_contract_assemble(
        &self,
        params: MemythosThreadContractAssembleParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        validate_thread_contract_assemble_request(&params)?;

        let assembly = self
            .thread_consolidation_adapter
            .consolidate_threads(&MemythosThreadConsolidateParams {
                coordinator_thread_id: params.coordinator_thread_id.clone(),
                source_thread_ids: params.source_thread_ids.clone(),
                since_cursors: params.since_cursors.clone(),
                items_view: params.items_view.clone(),
                purpose: MemythosThreadConsolidationPurpose::ArenaRoundConsolidation,
                authority_mode: MemythosThreadConsolidationAuthorityMode::PeerCoordination,
                instructions: params.instructions.clone(),
                per_source_limit: params.per_source_limit,
                client_user_message_id: params.client_user_message_id.clone(),
                output_schema: params.output_schema.clone(),
            })
            .await;

        let contract_id = self.next_id("mem_contract", &self.next_contract_id);
        let producer_turn_id = assembly
            .consolidation_turn_id
            .clone()
            .unwrap_or_else(|| format!("unavailable-{contract_id}"));
        let contract_ref = format!(
            "app-server://threads/{}/turns/{}/contracts/{}",
            params.coordinator_thread_id, producer_turn_id, contract_id
        );
        let structured_output_ref = assembly.structured_output_ref.clone();
        let schema_ref = format!(
            "app-server://schemas/{}/v1",
            sanitize_contract_ref_segment(&params.contract_kind)
        );
        let source_refs = assembly.source_refs.clone();
        let technical_evidence_refs = compact_event_refs(
            vec![
                format!(
                    "app-server://threads/{}/memythos/contracts/{}/instructions",
                    params.coordinator_thread_id, contract_id
                ),
                format!(
                    "app-server://threads/{}/memythos/contracts/{}/schema",
                    params.coordinator_thread_id, contract_id
                ),
            ]
            .into_iter()
            .chain(assembly.technical_evidence_refs.clone())
            .collect(),
        );
        let source_evidence_refs = contract_source_evidence_refs(
            &source_refs,
            &technical_evidence_refs,
            assembly.agent_message_ref.as_deref(),
            structured_output_ref.as_deref(),
        );
        let payload = params.output_schema.as_ref().map(|schema| {
            serde_json::json!({
                "contract_kind": params.contract_kind,
                "schema_ref": schema_ref,
                "output_schema": schema,
                "structured_output_ref": structured_output_ref,
                "source_evidence_refs": source_evidence_refs,
                "assembly_status": if assembly.blockers.is_empty() { "running" } else { "blocked" }
            })
        });
        let contract = MemythosStructuredContract {
            contract_ref: contract_ref.clone(),
            contract_kind: params.contract_kind.clone(),
            schema_ref,
            producer_thread_id: params.coordinator_thread_id.clone(),
            producer_turn_id: producer_turn_id.clone(),
            source_evidence_refs: source_evidence_refs.clone(),
            storage_kind: "app_server_native_contract_message".to_string(),
            created_at: Utc::now().to_rfc3339(),
            payload,
            missing_evidence: if structured_output_ref.is_none() {
                vec!["structured_output_ref".to_string()]
            } else {
                Vec::new()
            },
            blockers: assembly.blockers.clone(),
        };

        let event_ref = contract.contract_ref.clone();
        let mut state = self.state.lock().await;
        state
            .structured_contracts
            .insert(contract_ref.clone(), contract.clone());
        self.push_telemetry_ref(
            &mut state,
            MemythosTelemetryRefKind::ThreadConsolidation,
            MemythosTelemetrySource::AppServerNative,
            None,
            None,
            Some(params.coordinator_thread_id.clone()),
            Some(event_ref),
            structured_output_ref.clone(),
            if contract.blockers.is_empty() && contract.missing_evidence.is_empty() {
                MemythosEventChannel::ArtifactPayload
            } else {
                MemythosEventChannel::TechnicalDetail
            },
            format!(
                "Structured contract {} assembled for coordinator {}.",
                contract.contract_kind, contract.producer_thread_id
            ),
        );

        Ok(MemythosThreadContractAssembleResponse {
            contract,
            source_refs,
            agent_message_ref: assembly.agent_message_ref,
            structured_output_ref,
            technical_evidence_refs,
            source_method: "memythos/thread/contract/assemble".to_string(),
            used_thread_turns_summary: assembly.used_thread_turns_summary,
        }
        .into())
    }

    pub(crate) async fn thread_contract_read(
        &self,
        params: MemythosThreadContractReadParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let state = self.state.lock().await;
        let Some(contract) = state.structured_contracts.get(&params.contract_ref) else {
            return Err(invalid_params(format!(
                "unknown contract ref: {}",
                params.contract_ref
            )));
        };

        Ok(MemythosThreadContractReadResponse {
            contract: contract.clone(),
        }
        .into())
    }

    pub(crate) async fn thread_contract_list(
        &self,
        params: MemythosThreadContractListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        let limit = params.limit.unwrap_or(50).clamp(1, 200);
        let state = self.state.lock().await;
        let mut contracts = state
            .structured_contracts
            .values()
            .filter(|contract| {
                params
                    .thread_id
                    .as_ref()
                    .map_or(true, |thread_id| &contract.producer_thread_id == thread_id)
            })
            .filter(|contract| {
                params
                    .contract_kind
                    .as_ref()
                    .map_or(true, |kind| &contract.contract_kind == kind)
            })
            .cloned()
            .collect::<Vec<_>>();
        contracts.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        contracts.truncate(limit);

        Ok(MemythosThreadContractListResponse { contracts }.into())
    }
}
