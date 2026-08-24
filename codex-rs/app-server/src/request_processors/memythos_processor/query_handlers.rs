use super::*;

impl MemythosRequestProcessor {
    pub(crate) async fn parent_continuity_list(
        &self,
        params: MemythosParentContinuityListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }

        let parents = state
            .arena_parents
            .values()
            .filter(|parent| parent.arena_id == params.arena_id)
            .filter(|parent| {
                params
                    .thread_id
                    .as_ref()
                    .map_or(true, |thread_id| &parent.thread_id == thread_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let deliveries = state.arena_message_deliveries.clone();
        let native_token_usage_refs = state.native_token_usage_refs.clone();
        drop(state);

        let mut continuities = Vec::with_capacity(parents.len());
        for parent in parents {
            let goal_snapshot = self
                .parent_goal_snapshot_adapter
                .current_goal_snapshot(&parent.thread_id)
                .await;
            validate_parent_goal_snapshot(&parent.thread_id, &goal_snapshot).map_err(|error| {
                invalid_params(format!("parent goal adapter contract rejected: {error}"))
            })?;
            continuities.push(build_parent_thread_continuity(
                &parent,
                &deliveries,
                &native_token_usage_refs,
                goal_snapshot,
            ));
        }

        Ok(MemythosParentContinuityListResponse { continuities }.into())
    }

    pub(crate) async fn arena_message_list(
        &self,
        params: MemythosArenaMessageListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }
        let deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.arena_id == params.arena_id)
            .filter(|delivery| {
                params
                    .round_id
                    .as_ref()
                    .map_or(true, |round_id| &delivery.round_id == round_id)
            })
            .cloned()
            .collect();

        Ok(MemythosArenaMessageListResponse { deliveries }.into())
    }

    pub(crate) async fn arena_message_read(
        &self,
        params: MemythosArenaMessageReadParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        let message = state
            .arena_messages
            .get(&params.message_id)
            .filter(|message| message.arena_id == params.arena_id)
            .cloned()
            .ok_or_else(|| {
                invalid_params(format!(
                    "unknown message {} in arena {}",
                    params.message_id, params.arena_id
                ))
            })?;
        let delivered_prompt = build_peer_parent_envelope(&message);
        Ok(MemythosArenaMessageReadResponse {
            message,
            delivered_prompt,
        }
        .into())
    }

    pub(crate) async fn arena_message_observation_list(
        &self,
        params: MemythosArenaMessageObservationListParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        if !state.arenas.contains_key(&params.arena_id) {
            return Err(invalid_params(format!(
                "unknown arena id: {}",
                params.arena_id
            )));
        }

        let observations = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.arena_id == params.arena_id)
            .filter(|delivery| {
                params
                    .round_id
                    .as_ref()
                    .map_or(true, |round_id| &delivery.round_id == round_id)
            })
            .filter(|delivery| {
                params
                    .message_id
                    .as_ref()
                    .map_or(true, |message_id| &delivery.message_id == message_id)
            })
            .map(build_parent_peer_response_observation)
            .collect();

        Ok(MemythosArenaMessageObservationListResponse { observations }.into())
    }
    pub(crate) async fn arena_state_get(
        &self,
        params: MemythosArenaStateGetParams,
    ) -> Result<ClientResponsePayload, JSONRPCErrorError> {
        self.ensure_arena_state_restored().await?;
        let state = self.state.lock().await;
        let mut arena = state
            .arenas
            .get(&params.arena_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown arena id: {}", params.arena_id)))?;
        let protocol_snapshot = state
            .arena_lifecycles
            .get(&params.arena_id)
            .ok_or_else(|| {
                invalid_params(format!(
                    "arena {} has no canonical native lifecycle",
                    params.arena_id
                ))
            })?
            .protocol_snapshot();
        arena.lifecycle_state = NativeArenaState::restore_protocol_snapshot(protocol_snapshot)
            .map_err(|error| invalid_params(error.to_string()))?
            .protocol_state();
        let mut parents = state
            .arena_parents
            .values()
            .filter(|parent| parent.arena_id == params.arena_id)
            .cloned()
            .collect::<Vec<_>>();
        parents.sort_by(|left, right| left.thread_id.cmp(&right.thread_id));
        let mut deliveries = state
            .arena_message_deliveries
            .iter()
            .filter(|delivery| delivery.arena_id == params.arena_id)
            .cloned()
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| left.delivery_id.cmp(&right.delivery_id));
        Ok(MemythosArenaStateGetResponse {
            arena,
            parents,
            deliveries,
            phase_state_source: "app_server_protocol".to_string(),
            local_ts_arena_state_used: false,
        }
        .into())
    }
}
