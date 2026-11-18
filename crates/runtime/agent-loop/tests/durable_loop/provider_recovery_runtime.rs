use super::*;

#[tokio::test]
async fn cancellation_after_partial_flush_carries_the_durable_observation() {
    let fixture = Fixture::new();
    let usage = ModelUsage {
        input_tokens_uncached: 7,
        output_tokens: 2,
        total_tokens: Some(9),
        ..ModelUsage::default()
    };
    let response_ref = provider_ref("response-before-cancel");
    let provider = Arc::new(ScriptedProvider::new(vec![Script::EventsThenPending(
        vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: response_ref.clone(),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("partial before cancel"),
            }),
            Ok(ModelStreamEvent::Usage(usage)),
        ],
    )]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );
    let supervisor = InvocationSupervisor::new();
    let cancellation = supervisor.clone();
    let cancel_after_flush = async {
        tokio::time::timeout(Duration::from_secs(1), async {
            // Reader and Journal now advance concurrently. Cancel after the
            // exact usage prefix this test asserts has actually committed.
            while !journal
                .batches()
                .iter()
                .flat_map(|batch| &batch.events)
                .any(|event| matches!(event, ModelStreamEvent::Usage(value) if *value == usage))
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("partial frame must be flushed before cancellation");
        cancellation.cancel();
    };
    let (exit, ()) = tokio::join!(
        runtime.execute(fixture.attempt.clone(), fixture.telemetry(), supervisor),
        cancel_after_flush
    );

    assert!(matches!(exit, AgentLoopExit::Cancelled { .. }));
    assert_eq!(journal.snapshot().phase, LoopPhase::Cancelled);
    let observations = journal.cancellation_observations();
    let observation = observations
        .last()
        .and_then(Option::as_ref)
        .expect("in-flight cancellation observation");
    assert_eq!(
        observation.provider_response_ref.as_ref(),
        Some(&response_ref)
    );
    assert_eq!(observation.response_model_key.as_ref(), Some(&model_key()));
    assert_eq!(observation.usage, Some(usage));
    assert!(observation.public_output_observed);
}

#[tokio::test]
async fn incomplete_tool_lifecycle_stays_private_and_allows_sealed_fallback() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("partial-tool-before-disconnect");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("reasoning-only-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id,
                name: wire_name("visible_tool"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Err(ProviderError {
                kind: ProviderErrorKind::Transport,
                safe_code: ProviderErrorCode::TransportFailed,
                retryable: true,
                retry_after_ms: None,
                diagnostic_ref: None,
            }),
        ]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("fallback-after-reasoning"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("recovered on fallback route"),
            }),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime_with_route_count(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
        non_zero_u32(2),
    );

    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(journal.snapshot().active_model_route_ordinal, 1);
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
    assert!(
        journal
            .batches()
            .iter()
            .flat_map(|batch| &batch.events)
            .all(|event| !matches!(event, ModelStreamEvent::ToolCallStarted { .. })),
        "a disconnected incomplete Tool call must not become public output"
    );
}

#[tokio::test]
async fn local_validator_failure_flushes_only_the_last_fully_staged_state() {
    let fixture = Fixture::new();
    let first_ref = provider_ref("accepted-response");
    let usage = ModelUsage {
        input_tokens_uncached: 5,
        output_tokens: 1,
        total_tokens: Some(6),
        ..ModelUsage::default()
    };
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: first_ref.clone(),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text("accepted partial"),
        }),
        Ok(ModelStreamEvent::Usage(usage)),
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("rejected-duplicate-response"),
            response_model_key: model_key(),
        }),
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );

    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::InternalInvariantViolated,
        false,
    );
    let events = journal
        .batches()
        .into_iter()
        .flat_map(|batch| batch.events)
        .collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(event,
        ModelStreamEvent::TextDelta { delta, .. } if delta.as_str() == "accepted partial")));
    assert!(events.iter().any(|event| matches!(event,
        ModelStreamEvent::Usage(observed) if observed == &usage)));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ModelStreamEvent::ResponseStarted { .. }))
            .count(),
        1
    );
    assert!(
        matches!(events.first(), Some(ModelStreamEvent::ResponseStarted {
        response_ref, ..
    }) if response_ref == &first_ref)
    );
}

#[tokio::test]
async fn unexposed_provider_tool_is_durably_rejected_and_the_same_run_recovers() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("unexposed-tool-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("checking before rejected action"),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: tool_call_id("unexposed-tool-call"),
                name: wire_name("not_in_request_catalog"),
                provider_item_ref: None,
                provider_replay: None,
            }),
        ]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("corrected-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("recovered"),
            }),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime_with_route_count(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
        non_zero_u32(2),
    );

    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.requests().len(), 2);
    assert!(journal.failures().is_empty());
    let rejections = journal.action_rejections();
    assert_eq!(rejections.len(), 1);
    assert!(matches!(
        &rejections[0].rejection,
        musteragent_loop::ModelActionRejection::ToolNotExposed { attempted_name }
            if attempted_name.as_str() == "not_in_request_catalog"
    ));
    assert!(rejections[0].observation.public_output_observed);
    assert!(
        journal
            .batches()
            .iter()
            .flat_map(|batch| &batch.events)
            .all(|event| !matches!(event, ModelStreamEvent::ToolCallStarted { .. })),
        "an unexposed Tool call must never become public durable output"
    );
}

#[tokio::test]
async fn terminal_context_and_policy_stops_use_the_atomic_invocation_close_boundary() {
    for (stop_reason, code) in [
        (
            StopReason::ContextWindowExceeded,
            ErrorCode::ModelContextExceeded,
        ),
        (StopReason::ContentFilter, ErrorCode::PolicyDenied),
    ] {
        let fixture = Fixture::new();
        let usage = ModelUsage {
            input_tokens_uncached: 5,
            output_tokens: 1,
            total_tokens: Some(6),
            ..ModelUsage::default()
        };
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("terminal-policy-stop"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ReasoningSummaryDelta {
                output_index: 0,
                delta: model_text("public summary before terminal rejection"),
            }),
            Ok(ModelStreamEvent::Usage(usage)),
            Ok(ModelStreamEvent::ResponseCompleted { stop_reason }),
        ])]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = loop_runtime_with_route_count(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            Arc::clone(&journal),
            Duration::from_secs(1),
            non_zero_u32(2),
        );

        assert_durable_failure(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopFailureStage::ModelExecution,
            code,
            false,
        );
        assert_eq!(provider.requests().len(), 1, "{stop_reason:?}");
        assert_eq!(journal.snapshot().phase, LoopPhase::Failed);
        assert_eq!(journal.failures(), vec![(code, false)]);
        assert!(
            journal
                .batches()
                .iter()
                .flat_map(|batch| &batch.events)
                .any(
                    |event| matches!(event, ModelStreamEvent::Usage(observed) if observed == &usage)
                )
        );
    }
}

#[tokio::test]
async fn retryable_flag_cannot_expand_the_default_new_attempt_code_table() {
    let cases = [
        (
            ProviderErrorKind::Quota,
            ProviderErrorCode::QuotaExceeded,
            ErrorCode::QuotaExceeded,
        ),
        (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::AuthorizationFailed,
            ErrorCode::PolicyDenied,
        ),
        (
            ProviderErrorKind::Authentication,
            ProviderErrorCode::AuthenticationFailed,
            ErrorCode::Unauthenticated,
        ),
        (
            ProviderErrorKind::Protocol,
            ProviderErrorCode::ProtocolViolation,
            ErrorCode::ModelResponseInvalid,
        ),
    ];
    for (kind, safe_code, expected) in cases {
        let fixture = Fixture::new();
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
            ProviderError {
                kind,
                safe_code,
                retryable: true,
                retry_after_ms: None,
                diagnostic_ref: None,
            },
        )])]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = loop_runtime_with_route_count(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            Arc::clone(&journal),
            Duration::from_secs(1),
            NonZeroU32::MIN, // Exercise replacement attempts after the sealed chain is exhausted.
        );
        assert_durable_failure(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopFailureStage::ModelExecution,
            expected,
            false,
        );
        assert_eq!(provider.requests().len(), 1, "{safe_code:?}");
        assert_eq!(journal.snapshot().phase, LoopPhase::Failed, "{safe_code:?}");
    }
}

#[tokio::test]
async fn reasoning_summary_is_preserved_and_allows_sealed_fallback() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("partial-tool-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ReasoningSummaryDelta {
                output_index: 0,
                delta: model_text("Preparing an analysis"),
            }),
            Err(ProviderError {
                kind: ProviderErrorKind::Transport,
                safe_code: ProviderErrorCode::TransportFailed,
                retryable: true,
                retry_after_ms: None,
                diagnostic_ref: None,
            }),
        ]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("fallback-after-incomplete-tool"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("recovered on fallback route"),
            }),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime_with_route_count(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
        non_zero_u32(2),
    );

    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(journal.snapshot().active_model_route_ordinal, 1);
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
    assert!(
        journal
            .batches()
            .iter()
            .flat_map(|batch| &batch.events)
            .any(|event| matches!(event, ModelStreamEvent::ReasoningSummaryDelta { .. })),
        "failed reasoning remains a separate durable output"
    );
    assert_ne!(
        provider.requests()[0].invocation.invocation_id,
        provider.requests()[1].invocation.invocation_id
    );
}
