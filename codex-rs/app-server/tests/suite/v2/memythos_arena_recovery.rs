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
use codex_app_server_protocol::MemythosMailboxQuarantineGetParams;
use codex_app_server_protocol::MemythosMailboxQuarantineGetResponse;
use codex_app_server_protocol::MemythosMailboxQuarantineListParams;
use codex_app_server_protocol::MemythosMailboxQuarantineListResponse;
use codex_app_server_protocol::MemythosMailboxQuarantineResolutionAction;
use codex_app_server_protocol::MemythosMailboxQuarantineResolveParams;
use codex_app_server_protocol::MemythosMailboxQuarantineResolveResponse;
use codex_app_server_protocol::MemythosMailboxResolutionGetParams;
use codex_app_server_protocol::MemythosMailboxResolutionGetResponse;
use codex_app_server_protocol::MemythosMailboxResolutionListParams;
use codex_app_server_protocol::MemythosMailboxResolutionListResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::WarningNotification;
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
async fn arena_mailbox_payload_rehydrates_after_sigkill_and_is_consumed_once() -> Result<()> {
    const DURABLE_PAYLOAD_MARKER: &str = "DURABLE_MAILBOX_PAYLOAD_BEFORE_SIGKILL";
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

    let mut first_message = queued_proposal_message(
        "message-before-sigkill",
        &concierge.thread_id,
        &bettors[0].thread_id,
    );
    first_message.execution_prompt = Some(DURABLE_PAYLOAD_MARKER.to_string());
    let first_delivery = send_arena_message(&mut first_process, first_message.clone()).await?;
    assert_eq!(first_delivery.status, "queued_in_native_mailbox");

    {
        let state_db = codex_state::StateRuntime::init(
            codex_home.path().to_path_buf(),
            "mock_provider".to_string(),
        )
        .await?;
        assert_eq!(
            state_db
                .get_native_mailbox_communication(&bettors[0].thread_id, "message-before-sigkill",)
                .await?
                .expect("queue-only communication must be durable before SIGKILL")
                .status,
            "pending"
        );
    }

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

    resume_native_thread(&mut restarted_process, &bettors[0].thread_id).await?;
    {
        let state_db = codex_state::StateRuntime::init(
            codex_home.path().to_path_buf(),
            "mock_provider".to_string(),
        )
        .await?;
        assert_eq!(
            state_db
                .get_native_mailbox_communication(&bettors[0].thread_id, "message-before-sigkill",)
                .await?
                .expect("communication must remain pending until rollout persistence")
                .status,
            "pending"
        );
    }
    let wake_delivery = send_arena_message(
        &mut restarted_process,
        triggered_proposal_message(
            "message-wake-rehydrated-mailbox",
            &concierge.thread_id,
            &bettors[0].thread_id,
        ),
    )
    .await?;
    assert_eq!(wake_delivery.status, "delivered_to_native_mailbox_turn");
    let completed = wait_for_turn_completed(&mut restarted_process).await?;
    assert_eq!(completed.thread_id, bettors[0].thread_id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);

    let requests = model_server
        .received_requests()
        .await
        .expect("mock model server must record the resumed turn");
    assert_eq!(requests.len(), 1, "rehydrated mailbox must start one turn");
    let request_body = requests[0].body_json::<serde_json::Value>()?.to_string();
    assert!(
        request_body.contains(DURABLE_PAYLOAD_MARKER),
        "the model request must contain the queue-only payload accepted before SIGKILL"
    );

    let state_db = codex_state::StateRuntime::init(
        codex_home.path().to_path_buf(),
        "mock_provider".to_string(),
    )
    .await?;
    assert_eq!(
        state_db
            .get_native_mailbox_communication(&bettors[0].thread_id, "message-before-sigkill",)
            .await?
            .expect("durable queue-only communication must remain auditable")
            .status,
        "consumed"
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
    assert_eq!(final_state.deliveries.len(), 3);
    assert_eq!(
        final_state
            .deliveries
            .iter()
            .map(|delivery| delivery.delivery_id.as_str())
            .collect::<HashSet<_>>()
            .len(),
        3
    );

    Ok(())
}

#[tokio::test]
async fn arena_mailbox_crash_loop_quarantines_poison_payload_and_warns() -> Result<()> {
    const POISON_PAYLOAD_MARKER: &str = "POISON_MAILBOX_PAYLOAD_MUST_BE_QUARANTINED";
    const HEALTHY_WAKE_MARKER: &str = "HEALTHY_WAKE_AFTER_QUARANTINE";
    const RETRY_WAKE_MARKER: &str = "HEALTHY_WAKE_AFTER_AUTHORIZED_RETRY";

    let model_server = create_mock_responses_server_repeating_assistant("healthy recovery").await;
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
    let bettor = provisioned
        .leases
        .iter()
        .find(|lease| lease.role == "bettor")
        .expect("fixture must provision a bettor");

    let mut poison_message = queued_proposal_message(
        "message-poison-crash-loop",
        &concierge.thread_id,
        &bettor.thread_id,
    );
    poison_message.execution_prompt = Some(POISON_PAYLOAD_MARKER.to_string());
    let delivery = send_arena_message(&mut first_process, poison_message).await?;
    assert_eq!(delivery.status, "queued_in_native_mailbox");
    let killed = first_process.sigkill().await?;
    assert_eq!(killed.signal(), Some(9));
    drop(first_process);

    for expected_attempt in 1..=3 {
        let mut recovery_process = TestAppServer::new(codex_home.path()).await?;
        timeout(RESPONSE_TIMEOUT, recovery_process.initialize()).await??;
        resume_native_thread(&mut recovery_process, &bettor.thread_id).await?;

        let state_db = codex_state::StateRuntime::init(
            codex_home.path().to_path_buf(),
            "mock_provider".to_string(),
        )
        .await?;
        let record = state_db
            .get_native_mailbox_communication(&bettor.thread_id, "message-poison-crash-loop")
            .await?
            .expect("poison communication must remain auditable");
        assert_eq!(record.status, "pending");
        assert_eq!(record.attempt_count, expected_attempt);
        drop(state_db);

        let killed = recovery_process.sigkill().await?;
        assert_eq!(killed.signal(), Some(9));
        drop(recovery_process);
    }

    let mut quarantining_process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, quarantining_process.initialize()).await??;
    resume_native_thread(&mut quarantining_process, &bettor.thread_id).await?;
    let warning_notification = timeout(
        RESPONSE_TIMEOUT,
        quarantining_process.read_stream_until_notification_message("warning"),
    )
    .await??;
    let warning: WarningNotification = serde_json::from_value(
        warning_notification
            .params
            .expect("quarantine warning params must be present"),
    )?;
    assert_eq!(
        warning.thread_id.as_deref(),
        Some(bettor.thread_id.as_str())
    );
    assert!(warning.message.contains("message-poison-crash-loop"));
    assert!(warning.message.contains("automatic recovery stopped"));

    let list_id = quarantining_process
        .send_memythos_mailbox_quarantine_list_request(MemythosMailboxQuarantineListParams {
            receiver_thread_id: Some(bettor.thread_id.clone()),
        })
        .await?;
    let list_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        quarantining_process.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    let listed = to_response::<MemythosMailboxQuarantineListResponse>(list_response)?;
    assert_eq!(listed.communications.len(), 1);
    assert_eq!(
        listed.communications[0].communication_id,
        "message-poison-crash-loop"
    );

    let get_id = quarantining_process
        .send_memythos_mailbox_quarantine_get_request(MemythosMailboxQuarantineGetParams {
            receiver_thread_id: bettor.thread_id.clone(),
            communication_id: "message-poison-crash-loop".to_string(),
        })
        .await?;
    let get_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        quarantining_process.read_stream_until_response_message(RequestId::Integer(get_id)),
    )
    .await??;
    let inspected = to_response::<MemythosMailboxQuarantineGetResponse>(get_response)?;
    assert_eq!(inspected.communication.status, "quarantined");
    assert_eq!(inspected.communication.attempt_count, 4);

    let state_db = codex_state::StateRuntime::init(
        codex_home.path().to_path_buf(),
        "mock_provider".to_string(),
    )
    .await?;
    let quarantined = state_db
        .get_native_mailbox_communication(&bettor.thread_id, "message-poison-crash-loop")
        .await?
        .expect("quarantined communication must remain auditable");
    assert_eq!(quarantined.status, "quarantined");
    assert_eq!(quarantined.attempt_count, 4);
    assert_eq!(
        quarantined.failure_fingerprint.as_deref(),
        Some("native_mailbox_recovery_without_progress")
    );
    drop(state_db);

    let mut healthy_wake = triggered_proposal_message(
        "message-healthy-after-quarantine",
        &concierge.thread_id,
        &bettor.thread_id,
    );
    healthy_wake.execution_prompt = Some(HEALTHY_WAKE_MARKER.to_string());
    let healthy_delivery = send_arena_message(&mut quarantining_process, healthy_wake).await?;
    assert_eq!(healthy_delivery.status, "delivered_to_native_mailbox_turn");
    let completed = wait_for_turn_completed(&mut quarantining_process).await?;
    assert_eq!(completed.thread_id, bettor.thread_id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);

    let requests = model_server
        .received_requests()
        .await
        .expect("mock model server must record the healthy turn");
    assert_eq!(requests.len(), 1);
    let request_body = requests[0].body_json::<serde_json::Value>()?.to_string();
    assert!(request_body.contains(HEALTHY_WAKE_MARKER));
    assert!(!request_body.contains(POISON_PAYLOAD_MARKER));

    let resolve_params = MemythosMailboxQuarantineResolveParams {
        receiver_thread_id: bettor.thread_id.clone(),
        communication_id: "message-poison-crash-loop".to_string(),
        command_id: "retry-poison-once".to_string(),
        action: MemythosMailboxQuarantineResolutionAction::Retry,
        actor: "operator:e2e".to_string(),
        reason: "payload fixed externally".to_string(),
        replacement_message: None,
    };
    let resolve_id = quarantining_process
        .send_memythos_mailbox_quarantine_resolve_request(resolve_params.clone())
        .await?;
    let resolve_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        quarantining_process.read_stream_until_response_message(RequestId::Integer(resolve_id)),
    )
    .await??;
    let resolved = to_response::<MemythosMailboxQuarantineResolveResponse>(resolve_response)?;
    assert_eq!(resolved.resulting_status, "pending");
    assert!(!resolved.existing);
    assert_eq!(resolved.live_reenqueue_status, "enqueued");

    let replay_id = quarantining_process
        .send_memythos_mailbox_quarantine_resolve_request(resolve_params)
        .await?;
    let replay_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        quarantining_process.read_stream_until_response_message(RequestId::Integer(replay_id)),
    )
    .await??;
    let replayed = to_response::<MemythosMailboxQuarantineResolveResponse>(replay_response)?;
    assert!(
        replayed.existing,
        "repeating a resolution command must be idempotent"
    );
    assert_eq!(replayed.live_reenqueue_status, "already_resolved");

    let mut retry_wake = triggered_proposal_message(
        "message-wake-authorized-retry",
        &concierge.thread_id,
        &bettor.thread_id,
    );
    retry_wake.execution_prompt = Some(RETRY_WAKE_MARKER.to_string());
    send_arena_message(&mut quarantining_process, retry_wake).await?;
    wait_for_turn_completed(&mut quarantining_process).await?;
    let requests = model_server
        .received_requests()
        .await
        .expect("mock model server must record authorized retry");
    assert_eq!(requests.len(), 2);
    let retry_body = requests[1].body_json::<serde_json::Value>()?.to_string();
    assert!(retry_body.contains(POISON_PAYLOAD_MARKER));
    assert!(retry_body.contains(RETRY_WAKE_MARKER));

    let state_db = codex_state::StateRuntime::init(
        codex_home.path().to_path_buf(),
        "mock_provider".to_string(),
    )
    .await?;
    assert_eq!(
        state_db
            .get_native_mailbox_communication(&bettor.thread_id, "message-poison-crash-loop")
            .await?
            .expect("retried communication remains auditable")
            .status,
        "consumed"
    );

    Ok(())
}

#[tokio::test]
async fn arena_mailbox_terminal_resolutions_and_replace_survive_restart() -> Result<()> {
    const SKIP_MARKER: &str = "QUARANTINED_SKIP_MUST_NOT_RUN";
    const ABORT_MARKER: &str = "QUARANTINED_ABORT_MUST_NOT_RUN";
    const REPLACED_MARKER: &str = "QUARANTINED_REPLACED_ORIGINAL_MUST_NOT_RUN";
    const RACE_MARKER: &str = "QUARANTINED_CONCURRENT_RESOLUTION_MUST_NOT_RUN";
    const CONTENTION_MARKER: &str = "QUARANTINED_CONTENTION_MUST_NOT_RUN";
    const REPLACEMENT_MARKER: &str = "AUTHORIZED_REPLACEMENT_MUST_RUN";

    let model_server = create_mock_responses_server_repeating_assistant("resolved").await;
    let codex_home = TempDir::new()?;
    write_mock_responses_config_toml(
        codex_home.path(),
        &model_server.uri(),
        &BTreeMap::new(),
        1024,
        None,
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
    let bettor = provisioned
        .leases
        .iter()
        .find(|lease| lease.role == "bettor")
        .expect("fixture must provision a bettor");

    for (message_id, marker) in [
        ("message-terminal-skip", SKIP_MARKER),
        ("message-terminal-abort", ABORT_MARKER),
        ("message-terminal-replace", REPLACED_MARKER),
        ("message-terminal-race", RACE_MARKER),
        ("message-terminal-contention", CONTENTION_MARKER),
    ] {
        let mut message =
            queued_proposal_message(message_id, &concierge.thread_id, &bettor.thread_id);
        message.execution_prompt = Some(marker.to_string());
        assert_eq!(
            send_arena_message(&mut first_process, message)
                .await?
                .status,
            "queued_in_native_mailbox"
        );
    }
    assert_eq!(first_process.sigkill().await?.signal(), Some(9));
    drop(first_process);

    let state_db = codex_state::StateRuntime::init(
        codex_home.path().to_path_buf(),
        "mock_provider".to_string(),
    )
    .await?;
    let now = chrono::Utc::now().timestamp_millis();
    for message_id in [
        "message-terminal-skip",
        "message-terminal-abort",
        "message-terminal-replace",
        "message-terminal-race",
        "message-terminal-contention",
    ] {
        for attempt in 1..=4 {
            state_db
                .claim_native_mailbox_communication_for_recovery(
                    &bettor.thread_id,
                    message_id,
                    3,
                    now + attempt,
                )
                .await?;
        }
    }
    drop(state_db);

    let mut process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, process.initialize()).await??;
    let skip = resolve_quarantine(
        &mut process,
        MemythosMailboxQuarantineResolveParams {
            receiver_thread_id: bettor.thread_id.clone(),
            communication_id: "message-terminal-skip".to_string(),
            command_id: "command-terminal-skip".to_string(),
            action: MemythosMailboxQuarantineResolutionAction::Skip,
            actor: "operator:e2e".to_string(),
            reason: "not required".to_string(),
            replacement_message: None,
        },
    )
    .await?;
    assert_eq!(skip.resulting_status, "skipped");
    let abort = resolve_quarantine(
        &mut process,
        MemythosMailboxQuarantineResolveParams {
            receiver_thread_id: bettor.thread_id.clone(),
            communication_id: "message-terminal-abort".to_string(),
            command_id: "command-terminal-abort".to_string(),
            action: MemythosMailboxQuarantineResolutionAction::Abort,
            actor: "operator:e2e".to_string(),
            reason: "unsafe payload".to_string(),
            replacement_message: None,
        },
    )
    .await?;
    assert_eq!(abort.resulting_status, "aborted");
    let mut replacement = queued_proposal_message(
        "message-terminal-replacement",
        &concierge.thread_id,
        &bettor.thread_id,
    );
    replacement.execution_prompt = Some(REPLACEMENT_MARKER.to_string());
    let replaced = resolve_quarantine(
        &mut process,
        MemythosMailboxQuarantineResolveParams {
            receiver_thread_id: bettor.thread_id.clone(),
            communication_id: "message-terminal-replace".to_string(),
            command_id: "command-terminal-replace".to_string(),
            action: MemythosMailboxQuarantineResolutionAction::Replace,
            actor: "operator:e2e".to_string(),
            reason: "corrected payload".to_string(),
            replacement_message: Some(replacement),
        },
    )
    .await?;
    assert_eq!(replaced.resulting_status, "aborted");
    assert_eq!(
        replaced.replacement_communication_id.as_deref(),
        Some("message-terminal-replacement")
    );

    let race_skip_id = process
        .send_memythos_mailbox_quarantine_resolve_request(MemythosMailboxQuarantineResolveParams {
            receiver_thread_id: bettor.thread_id.clone(),
            communication_id: "message-terminal-race".to_string(),
            command_id: "command-terminal-race-skip".to_string(),
            action: MemythosMailboxQuarantineResolutionAction::Skip,
            actor: "operator:e2e".to_string(),
            reason: "concurrent skip".to_string(),
            replacement_message: None,
        })
        .await?;
    let race_abort_id = process
        .send_memythos_mailbox_quarantine_resolve_request(MemythosMailboxQuarantineResolveParams {
            receiver_thread_id: bettor.thread_id.clone(),
            communication_id: "message-terminal-race".to_string(),
            command_id: "command-terminal-race-abort".to_string(),
            action: MemythosMailboxQuarantineResolutionAction::Abort,
            actor: "operator:e2e".to_string(),
            reason: "concurrent abort".to_string(),
            replacement_message: None,
        })
        .await?;
    let race_skip = to_response::<MemythosMailboxQuarantineResolveResponse>(
        timeout(
            RESPONSE_TIMEOUT,
            process.read_stream_until_response_message(RequestId::Integer(race_skip_id)),
        )
        .await??,
    )?;
    let race_abort = to_response::<MemythosMailboxQuarantineResolveResponse>(
        timeout(
            RESPONSE_TIMEOUT,
            process.read_stream_until_response_message(RequestId::Integer(race_abort_id)),
        )
        .await??,
    )?;
    let race_outcomes = [&race_skip, &race_abort];
    let race_winner = race_outcomes
        .iter()
        .find(|outcome| !outcome.conflict)
        .expect("one concurrent command wins");
    let race_loser = race_outcomes
        .iter()
        .find(|outcome| outcome.conflict)
        .expect("one concurrent command conflicts");
    assert_eq!(
        race_loser.winner_command_id.as_deref(),
        Some(race_winner.command_id.as_str())
    );
    assert_eq!(race_loser.live_reenqueue_status, "not_applicable");

    let mut contention_requests = Vec::new();
    for index in 0..10 {
        let mut replacement = queued_proposal_message(
            &format!("message-terminal-contention-replacement-{index}"),
            &concierge.thread_id,
            &bettor.thread_id,
        );
        replacement.execution_prompt = Some(format!("AUTHORIZED_CONTENTION_REPLACEMENT_{index}"));
        let request_id = process
            .send_memythos_mailbox_quarantine_resolve_request(
                MemythosMailboxQuarantineResolveParams {
                    receiver_thread_id: bettor.thread_id.clone(),
                    communication_id: "message-terminal-contention".to_string(),
                    command_id: format!("command-terminal-contention-{index}"),
                    action: MemythosMailboxQuarantineResolutionAction::Replace,
                    actor: "operator:e2e".to_string(),
                    reason: "contention hardening".to_string(),
                    replacement_message: Some(replacement),
                },
            )
            .await?;
        contention_requests.push(request_id);
    }
    let mut contention_outcomes = Vec::new();
    for request_id in contention_requests {
        let response: JSONRPCResponse = timeout(
            RESPONSE_TIMEOUT,
            process.read_stream_until_response_message(RequestId::Integer(request_id)),
        )
        .await??;
        contention_outcomes.push(to_response::<MemythosMailboxQuarantineResolveResponse>(
            response,
        )?);
    }
    assert_eq!(
        contention_outcomes
            .iter()
            .filter(|outcome| !outcome.conflict)
            .count(),
        1
    );
    assert_eq!(
        contention_outcomes
            .iter()
            .filter(|outcome| outcome.conflict)
            .count(),
        9
    );
    let contention_winner = contention_outcomes
        .iter()
        .find(|outcome| !outcome.conflict)
        .expect("one contention command wins");
    assert!(
        contention_outcomes
            .iter()
            .filter(|outcome| outcome.conflict)
            .all(|outcome| {
                outcome.winner_command_id.as_deref() == Some(contention_winner.command_id.as_str())
                    && outcome.replacement_communication_id
                        == contention_winner.replacement_communication_id
            })
    );

    let first_page_id = process
        .send_memythos_mailbox_resolution_list_request(MemythosMailboxResolutionListParams {
            receiver_thread_id: Some(bettor.thread_id.clone()),
            communication_id: None,
            cursor: None,
            limit: Some(2),
        })
        .await?;
    let first_page_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        process.read_stream_until_response_message(RequestId::Integer(first_page_id)),
    )
    .await??;
    let first_page = to_response::<MemythosMailboxResolutionListResponse>(first_page_response)?;
    assert_eq!(first_page.resolutions.len(), 2);
    let second_page_id = process
        .send_memythos_mailbox_resolution_list_request(MemythosMailboxResolutionListParams {
            receiver_thread_id: Some(bettor.thread_id.clone()),
            communication_id: None,
            cursor: first_page.next_cursor,
            limit: Some(2),
        })
        .await?;
    let second_page_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        process.read_stream_until_response_message(RequestId::Integer(second_page_id)),
    )
    .await??;
    let second_page = to_response::<MemythosMailboxResolutionListResponse>(second_page_response)?;
    assert_eq!(second_page.resolutions.len(), 2);
    let third_page_id = process
        .send_memythos_mailbox_resolution_list_request(MemythosMailboxResolutionListParams {
            receiver_thread_id: Some(bettor.thread_id.clone()),
            communication_id: None,
            cursor: second_page.next_cursor,
            limit: Some(2),
        })
        .await?;
    let third_page_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        process.read_stream_until_response_message(RequestId::Integer(third_page_id)),
    )
    .await??;
    let third_page = to_response::<MemythosMailboxResolutionListResponse>(third_page_response)?;
    assert_eq!(third_page.resolutions.len(), 1);
    assert!(third_page.next_cursor.is_none());

    let audit_get_id = process
        .send_memythos_mailbox_resolution_get_request(MemythosMailboxResolutionGetParams {
            receiver_thread_id: bettor.thread_id.clone(),
            command_id: "command-terminal-replace".to_string(),
        })
        .await?;
    let audit_get_response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        process.read_stream_until_response_message(RequestId::Integer(audit_get_id)),
    )
    .await??;
    let audit = to_response::<MemythosMailboxResolutionGetResponse>(audit_get_response)?.resolution;
    assert_eq!(audit.pre_status, "quarantined");
    assert_eq!(audit.pre_attempt_count, 4);
    assert_eq!(
        audit.pre_failure_fingerprint.as_deref(),
        Some("native_mailbox_recovery_without_progress")
    );
    assert_eq!(audit.resulting_status, "aborted");
    assert_eq!(
        audit.replacement_communication_id.as_deref(),
        Some("message-terminal-replacement")
    );

    resume_native_thread(&mut process, &bettor.thread_id).await?;
    let mut wake = triggered_proposal_message(
        "message-terminal-resolution-wake",
        &concierge.thread_id,
        &bettor.thread_id,
    );
    wake.execution_prompt = Some("TERMINAL_RESOLUTION_WAKE".to_string());
    send_arena_message(&mut process, wake).await?;
    wait_for_turn_completed(&mut process).await?;
    let requests = model_server
        .received_requests()
        .await
        .expect("replacement turn must reach model");
    assert_eq!(requests.len(), 1);
    let body = requests[0].body_json::<serde_json::Value>()?.to_string();
    assert!(body.contains(REPLACEMENT_MARKER));
    assert!(!body.contains(SKIP_MARKER));
    assert!(!body.contains(ABORT_MARKER));
    assert!(!body.contains(REPLACED_MARKER));
    assert!(!body.contains(RACE_MARKER));
    assert!(!body.contains(CONTENTION_MARKER));
    assert_eq!(
        (0..10)
            .filter(|index| body.contains(&format!("AUTHORIZED_CONTENTION_REPLACEMENT_{index}")))
            .count(),
        1
    );

    Ok(())
}

#[tokio::test]
async fn arena_completed_turn_ack_survives_sigkill_without_duplicate_turn() -> Result<()> {
    let model_server = create_mock_responses_server_repeating_assistant("proposal complete").await;
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
    let bettor = provisioned
        .leases
        .iter()
        .find(|lease| lease.role == "bettor")
        .expect("fixture must provision a bettor");
    let message = triggered_proposal_message(
        "message-completed-before-sigkill",
        &concierge.thread_id,
        &bettor.thread_id,
    );

    let delivered = send_arena_message(&mut first_process, message.clone()).await?;
    assert_eq!(delivered.status, "delivered_to_native_mailbox_turn");
    let receiver_turn_id = delivered
        .receiver_turn_id
        .clone()
        .expect("triggered mailbox delivery must return a turn id");
    let completed = wait_for_turn_completed(&mut first_process).await?;
    assert_eq!(completed.thread_id, bettor.thread_id);
    assert_eq!(completed.turn.id, receiver_turn_id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);

    let acknowledged = read_arena_state(&mut first_process).await?;
    let acknowledged_delivery = acknowledged
        .deliveries
        .iter()
        .find(|delivery| delivery.message_id == message.message_id)
        .expect("completed delivery must remain in Arena state");
    assert_eq!(acknowledged_delivery.status, "receiver_turn_completed");
    assert!(acknowledged_delivery.receiver_response_event_ref.is_some());
    assert_eq!(
        read_native_turn_ids(&mut first_process, &bettor.thread_id).await?,
        vec![receiver_turn_id.clone()]
    );

    let killed = first_process.sigkill().await?;
    assert_eq!(killed.signal(), Some(9), "app-server must exit by SIGKILL");
    drop(first_process);

    let mut restarted_process = TestAppServer::new(codex_home.path()).await?;
    timeout(RESPONSE_TIMEOUT, restarted_process.initialize()).await??;
    let restored = read_arena_state(&mut restarted_process).await?;
    let restored_delivery = restored
        .deliveries
        .iter()
        .find(|delivery| delivery.message_id == message.message_id)
        .expect("completed delivery checkpoint must be restored");
    assert_eq!(restored_delivery.status, "receiver_turn_completed");
    assert_eq!(
        restored_delivery.receiver_turn_id.as_deref(),
        Some(receiver_turn_id.as_str())
    );
    assert!(restored_delivery.receiver_response_event_ref.is_some());

    let replayed = send_arena_message(&mut restarted_process, message).await?;
    assert_eq!(replayed.delivery_id, delivered.delivery_id);
    assert_eq!(replayed.status, "receiver_turn_completed");
    assert_eq!(
        replayed.receiver_turn_id.as_deref(),
        Some(receiver_turn_id.as_str())
    );
    assert_eq!(
        read_native_turn_ids(&mut restarted_process, &bettor.thread_id).await?,
        vec![receiver_turn_id]
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

fn triggered_proposal_message(
    message_id: &str,
    concierge_thread_id: &str,
    bettor_thread_id: &str,
) -> MemythosArenaMessage {
    let mut message = queued_proposal_message(message_id, concierge_thread_id, bettor_thread_id);
    message.requires_response = true;
    message.delivery_policy = Some(MemythosArenaDeliveryPolicy::Immediate);
    message
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

async fn resolve_quarantine(
    server: &mut TestAppServer,
    params: MemythosMailboxQuarantineResolveParams,
) -> Result<MemythosMailboxQuarantineResolveResponse> {
    let request_id = server
        .send_memythos_mailbox_quarantine_resolve_request(params)
        .await?;
    let response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    Ok(to_response(response)?)
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

async fn wait_for_turn_completed(server: &mut TestAppServer) -> Result<TurnCompletedNotification> {
    let notification = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    Ok(serde_json::from_value(
        notification
            .params
            .expect("turn/completed params must be present"),
    )?)
}

async fn read_native_turn_ids(server: &mut TestAppServer, thread_id: &str) -> Result<Vec<String>> {
    let request_id = server
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: None,
            limit: Some(10),
            sort_direction: Some(SortDirection::Asc),
            items_view: None,
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        RESPONSE_TIMEOUT,
        server.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    Ok(to_response::<ThreadTurnsListResponse>(response)?
        .data
        .into_iter()
        .map(|turn| turn.id)
        .collect())
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
