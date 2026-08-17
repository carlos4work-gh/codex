use std::collections::BTreeMap;
use std::collections::HashSet;
use std::os::unix::process::ExitStatusExt;
use std::time::Duration;

use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::to_response;
use app_test_support::write_mock_responses_config_toml;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::MemythosArenaCompositionContract;
use codex_app_server_protocol::MemythosArenaCompositionCoordination;
use codex_app_server_protocol::MemythosArenaCompositionParticipant;
use codex_app_server_protocol::MemythosArenaCompositionProvisionParams;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaCostEnvelope;
use codex_app_server_protocol::MemythosArenaCostEnvelopeMode;
use codex_app_server_protocol::MemythosArenaCostExhaustionPolicy;
use codex_app_server_protocol::MemythosArenaDecisionMethod;
use codex_app_server_protocol::MemythosArenaDeliveryPolicy;
use codex_app_server_protocol::MemythosArenaLifecycleState;
use codex_app_server_protocol::MemythosArenaMessage;
use codex_app_server_protocol::MemythosArenaMessageDelivery;
use codex_app_server_protocol::MemythosArenaMessageSendParams;
use codex_app_server_protocol::MemythosArenaMessageSendResponse;
use codex_app_server_protocol::MemythosArenaPhaseStartParams;
use codex_app_server_protocol::MemythosArenaRoundPolicy;
use codex_app_server_protocol::MemythosArenaRunParams;
use codex_app_server_protocol::MemythosArenaRunResponse;
use codex_app_server_protocol::MemythosArenaStateGetParams;
use codex_app_server_protocol::MemythosArenaStateGetResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_protocol::openai_models::ReasoningEffort;
use tempfile::TempDir;
use tokio::time::timeout;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn arena_provision_checkpoint_survives_sigkill_without_duplicate_parents() -> Result<()> {
    let model_server = create_mock_responses_server_repeating_assistant("unused").await;
    let codex_home = TempDir::new()?;
    write_mock_responses_config_toml(
        codex_home.path(),
        &model_server.uri(),
        &BTreeMap::new(),
        /* auto_compact_limit */ 1024,
        /* requires_openai_auth */ None,
        "mock_provider",
        "compact",
    )?;
    write_arena_role_catalog(&codex_home)?;
    let provision_params = competitive_composition_params();

    let mut first_process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, first_process.initialize()).await??;
    let provision_id = first_process
        .send_memythos_arena_composition_provision_request(provision_params.clone())
        .await?;
    let provision_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        first_process.read_stream_until_response_message(RequestId::Integer(provision_id)),
    )
    .await??;
    let provisioned = to_response::<MemythosArenaCompositionProvisionResponse>(provision_response)?;
    let original_thread_ids = provisioned
        .leases
        .iter()
        .map(|lease| lease.thread_id.clone())
        .collect::<HashSet<_>>();
    assert_eq!(original_thread_ids.len(), 4);

    let phase_id = first_process
        .send_memythos_arena_phase_start_request(MemythosArenaPhaseStartParams {
            arena_id: "arena-sigkill".to_string(),
            round_id: "round-1".to_string(),
            phase: "proposal".to_string(),
        })
        .await?;
    timeout(
        RESPONSE_TIMEOUT,
        first_process.read_stream_until_response_message(RequestId::Integer(phase_id)),
    )
    .await??;

    let killed = first_process.sigkill().await?;
    assert_eq!(killed.signal(), Some(9), "app-server must exit by SIGKILL");
    drop(first_process);

    let mut restarted_process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, restarted_process.initialize()).await??;
    let run_id = restarted_process
        .send_memythos_arena_run_request(MemythosArenaRunParams {
            arena_id: "arena-sigkill".to_string(),
            round_id: "round-1".to_string(),
        })
        .await?;
    let run_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        restarted_process.read_stream_until_response_message(RequestId::Integer(run_id)),
    )
    .await??;
    let restored_run = to_response::<MemythosArenaRunResponse>(run_response)?;
    assert_eq!(
        restored_run.lifecycle_state,
        MemythosArenaLifecycleState::Running
    );

    let restored_thread_ids = read_arena_thread_ids(&mut restarted_process).await?;
    assert_eq!(restored_thread_ids, original_thread_ids);
    assert_eq!(
        read_native_thread_ids(&mut restarted_process).await?,
        original_thread_ids
    );

    let duplicate_id = restarted_process
        .send_memythos_arena_composition_provision_request(provision_params)
        .await?;
    let duplicate_error = timeout(
        RESPONSE_TIMEOUT,
        restarted_process.read_stream_until_error_message(RequestId::Integer(duplicate_id)),
    )
    .await??;
    assert!(
        duplicate_error
            .error
            .message
            .contains("instead of provisioning duplicate parents")
    );
    assert_eq!(
        read_arena_thread_ids(&mut restarted_process).await?,
        original_thread_ids
    );
    assert_eq!(
        read_native_thread_ids(&mut restarted_process).await?,
        original_thread_ids
    );

    Ok(())
}

#[tokio::test]
async fn arena_mailbox_delivery_survives_sigkill_and_retries_idempotently() -> Result<()> {
    let model_server = create_mock_responses_server_repeating_assistant("unused").await;
    let codex_home = TempDir::new()?;
    write_mock_responses_config_toml(
        codex_home.path(),
        &model_server.uri(),
        &BTreeMap::new(),
        /* auto_compact_limit */ 1024,
        /* requires_openai_auth */ None,
        "mock_provider",
        "compact",
    )?;
    write_arena_role_catalog(&codex_home)?;

    let mut first_process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, first_process.initialize()).await??;
    let provisioned = provision_arena(&mut first_process).await?;
    start_proposal_phase(&mut first_process).await?;
    let concierge = provisioned
        .leases
        .iter()
        .find(|lease| lease.role == "room_concierge")
        .expect("fixture must provision a Concierge");
    let bettors = provisioned
        .leases
        .iter()
        .filter(|lease| lease.role == "bettor")
        .collect::<Vec<_>>();
    assert_eq!(bettors.len(), 2);

    let first_message = queued_proposal_message(
        "message-before-sigkill",
        &concierge.thread_id,
        &bettors[0].thread_id,
    );
    let first_delivery = send_arena_message(&mut first_process, first_message.clone()).await?;
    assert_eq!(first_delivery.status, "queued_in_native_mailbox");

    let killed = first_process.sigkill().await?;
    assert_eq!(killed.signal(), Some(9), "app-server must exit by SIGKILL");
    drop(first_process);

    let mut restarted_process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, restarted_process.initialize()).await??;
    let restored = read_arena_state(&mut restarted_process).await?;
    assert_eq!(restored.deliveries.len(), 1);
    assert_eq!(
        restored.deliveries[0].delivery_id,
        first_delivery.delivery_id
    );

    let replayed = send_arena_message(&mut restarted_process, first_message).await?;
    assert_eq!(replayed.delivery_id, first_delivery.delivery_id);
    assert_eq!(
        read_arena_state(&mut restarted_process)
            .await?
            .deliveries
            .len(),
        1
    );

    resume_native_thread(&mut restarted_process, &bettors[1].thread_id).await?;
    let second_delivery = send_arena_message(
        &mut restarted_process,
        queued_proposal_message(
            "message-after-sigkill",
            &concierge.thread_id,
            &bettors[1].thread_id,
        ),
    )
    .await?;
    assert_ne!(second_delivery.delivery_id, first_delivery.delivery_id);
    assert_eq!(second_delivery.status, "queued_in_native_mailbox");
    assert!(second_delivery.rejection_reason.is_none());
    let final_state = read_arena_state(&mut restarted_process).await?;
    assert_eq!(final_state.deliveries.len(), 2);
    assert_eq!(
        final_state
            .deliveries
            .iter()
            .map(|delivery| delivery.delivery_id.as_str())
            .collect::<HashSet<_>>()
            .len(),
        2
    );

    Ok(())
}

async fn provision_arena(
    server: &mut TestAppServer,
) -> Result<MemythosArenaCompositionProvisionResponse> {
    let provision_id = server
        .send_memythos_arena_composition_provision_request(competitive_composition_params())
        .await?;
    let response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(provision_id)),
    )
    .await??;
    to_response(response)
}

async fn start_proposal_phase(server: &mut TestAppServer) -> Result<()> {
    let phase_id = server
        .send_memythos_arena_phase_start_request(MemythosArenaPhaseStartParams {
            arena_id: "arena-sigkill".to_string(),
            round_id: "round-1".to_string(),
            phase: "proposal".to_string(),
        })
        .await?;
    timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(phase_id)),
    )
    .await??;
    Ok(())
}

fn queued_proposal_message(
    message_id: &str,
    concierge_thread_id: &str,
    bettor_thread_id: &str,
) -> MemythosArenaMessage {
    MemythosArenaMessage {
        message_id: message_id.to_string(),
        case_id: "case-sigkill".to_string(),
        arena_id: "arena-sigkill".to_string(),
        round_id: "round-1".to_string(),
        from_parent_thread_id: concierge_thread_id.to_string(),
        from_parent_role: "room_concierge".to_string(),
        to_parent_thread_id: bettor_thread_id.to_string(),
        to_parent_role: "bettor".to_string(),
        message_kind: "peer_proposal".to_string(),
        human_summary: "Prepare one independent proposal.".to_string(),
        execution_prompt: None,
        context_packet_ref: "app-server://arena-sigkill/context/proposal".to_string(),
        artifact_refs: Vec::new(),
        requires_response: false,
        delivery_policy: Some(MemythosArenaDeliveryPolicy::QueueOnly),
        aggregate_contract: None,
        response_contract: None,
        output_schema: None,
    }
}

async fn send_arena_message(
    server: &mut TestAppServer,
    message: MemythosArenaMessage,
) -> Result<MemythosArenaMessageDelivery> {
    let message_id = server
        .send_memythos_arena_message_request(MemythosArenaMessageSendParams { message })
        .await?;
    let response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(message_id)),
    )
    .await??;
    Ok(to_response::<MemythosArenaMessageSendResponse>(response)?.delivery)
}

async fn resume_native_thread(server: &mut TestAppServer, thread_id: &str) -> Result<()> {
    let resume_id = server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(resume_id)),
    )
    .await??;
    Ok(())
}

async fn read_native_thread_ids(server: &mut TestAppServer) -> Result<HashSet<String>> {
    let list_id = server
        .send_thread_list_request(ThreadListParams {
            cursor: None,
            limit: Some(50),
            sort_key: None,
            sort_direction: None,
            model_providers: None,
            source_kinds: None,
            archived: None,
            cwd: None,
            use_state_db_only: true,
            search_term: None,
            parent_thread_id: None,
        })
        .await?;
    let list_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    Ok(to_response::<ThreadListResponse>(list_response)?
        .data
        .into_iter()
        .map(|thread| thread.id)
        .collect())
}

fn write_arena_role_catalog(codex_home: &TempDir) -> Result<()> {
    let roles_dir = codex_home.path().join("agents");
    std::fs::create_dir_all(&roles_dir)?;
    std::fs::write(
        roles_dir.join("room_concierge.toml"),
        r#"
name = "room_concierge"
description = "Coordinates the Arena without owning business semantics"
developer_instructions = "Coordinate checkpoints through native app-server primitives."

[planner_capabilities]
allowed_stances = ["coordination"]
proposal_bearing = false
"#,
    )?;
    std::fs::write(
        roles_dir.join("bettor.toml"),
        r#"
name = "bettor"
description = "Contributes an independent proposal and explicit bet"
developer_instructions = "Preserve independent evidence and yield when refuted."

[planner_capabilities]
allowed_stances = ["growth", "risk"]
proposal_bearing = true
supports_multiple_stances = true
"#,
    )?;
    std::fs::write(
        roles_dir.join("judge.toml"),
        r#"
name = "judge"
description = "Selects the bounded Arena outcome"
developer_instructions = "Judge only the active bounded objective and preserve dissent."

[planner_capabilities]
allowed_stances = ["business_fitness"]
proposal_bearing = false
"#,
    )?;
    Ok(())
}

async fn read_arena_thread_ids(server: &mut TestAppServer) -> Result<HashSet<String>> {
    Ok(read_arena_state(server)
        .await?
        .parents
        .into_iter()
        .map(|parent| parent.thread_id)
        .collect())
}

async fn read_arena_state(server: &mut TestAppServer) -> Result<MemythosArenaStateGetResponse> {
    let state_id = server
        .send_memythos_arena_state_get_request(MemythosArenaStateGetParams {
            arena_id: "arena-sigkill".to_string(),
        })
        .await?;
    let state_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(state_id)),
    )
    .await??;
    to_response(state_response)
}

fn competitive_composition_params() -> MemythosArenaCompositionProvisionParams {
    let participant = |participant_id: &str, agent_role: &str, stance: &str| {
        MemythosArenaCompositionParticipant {
            participant_id: participant_id.to_string(),
            agent_role: agent_role.to_string(),
            stance: stance.to_string(),
            authority_scope: vec!["business_process".to_string()],
            role_objective: format!("Fulfil the {participant_id} responsibility"),
            expected_contribution: format!("Independent contribution from {participant_id}"),
            exit_condition: format!("{participant_id} has delivered its position"),
            effort_intent: "proportionate to uncertainty and decision impact".to_string(),
            reasoning_effort: ReasoningEffort::Low,
            token_budget: Some(20_000),
        }
    };
    MemythosArenaCompositionProvisionParams {
        case_id: "case-sigkill".to_string(),
        layer_id: "bpm_e2e".to_string(),
        room_id: "room-sigkill".to_string(),
        cwd: None,
        upstream_authority_scope: vec!["business_process".to_string()],
        revision: None,
        contract: MemythosArenaCompositionContract {
            contract_version: "1.0".to_string(),
            arena_id: "arena-sigkill".to_string(),
            shared_objective: "Resolve the BPM decision with independent positions".to_string(),
            completion_criteria: vec!["Judge selects a supported position".to_string()],
            participants: vec![
                participant("concierge", "room_concierge", "coordination"),
                participant("bettor-growth", "bettor", "growth"),
                participant("bettor-risk", "bettor", "risk"),
                participant("judge", "judge", "business_fitness"),
            ],
            coordination: MemythosArenaCompositionCoordination {
                coordinator_participant_id: None,
                concierge_participant_id: Some("concierge".to_string()),
                judge_participant_id: Some("judge".to_string()),
                decision_method: MemythosArenaDecisionMethod::BettingRound,
                round_policy: Some(MemythosArenaRoundPolicy {
                    minimum_competing_positions: 2,
                    cross_read_required: true,
                    objection_required: true,
                    explicit_bet_required: true,
                }),
            },
            cost_envelope: MemythosArenaCostEnvelope {
                mode: MemythosArenaCostEnvelopeMode::ExplicitCap,
                rationale: "The fixture supplies a measured bounded allocation".to_string(),
                baseline_refs: Vec::new(),
                total_token_budget: Some(80_000),
                coordination_token_budget: Some(20_000),
                substantive_token_budget: Some(60_000),
                method_integrity_funded: true,
                exhaustion_policy: MemythosArenaCostExhaustionPolicy::WrapUpThenReplan,
            },
            effort_rationale: "Allocate bounded effort while preserving both independent positions"
                .to_string(),
            rationale: "Two independent bettors prevent a fake round".to_string(),
            unresolved_role_gap: None,
        },
    }
}
