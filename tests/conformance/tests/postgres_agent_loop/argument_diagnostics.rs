use super::*;
use musteragent_loop::ToolCallPlanningFailure;
use musteragent_runtime_model::{ArgumentDiagnostic, ArgumentSource};

#[tokio::test]
async fn argument_diagnostics_survive_intent_and_pending_call_recovery() {
    for stop in [
        StopReason::ToolCalls,
        StopReason::MaxOutputTokens,
        StopReason::TokenLimit,
    ] {
        check_argument_recovery(stop).await;
    }
}

async fn check_argument_recovery(stop: StopReason) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let open = || {
        audited_loop_journal(
            &pool,
            content.clone(),
            Arc::new(ProtobufLedgerPayloadCodec),
            keyring(),
            running_loop::recovery_keyring(),
        )
    };
    let journal = open();
    let request = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media"),
            bytes: b"{}".to_vec(),
        })
        .await
        .expect("request");
    let mut iteration = model_iteration(fixture, attempt.clone(), request, 1);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut iteration,
    )
    .await;
    journal
        .begin_model(
            &attempt,
            &iteration,
            ProviderRecoveryCapability::NonResumable,
        )
        .await
        .expect("begin");
    journal
        .mark_model_dispatched(&attempt, iteration.invocation_id)
        .await
        .expect("dispatch");
    let mut diagnostic =
        ArgumentDiagnostic::observed("{\"private\":", ArgumentSource::ArgumentsDone, None);
    diagnostic.finish(stop, 16384, 1048576, None);
    let call = PlannedToolCall {
        call_id: ToolCallId::new("malformed").expect("call"),
        name: WireName::new("presentation_author").expect("name"),
        arguments: JsonValue::Object(BTreeMap::new()),
        provider_replay: None,
        planning_failure: Some(if stop.is_token_limited() {
            ToolCallPlanningFailure::Truncated
        } else {
            ToolCallPlanningFailure::MalformedJson
        }),
        argument_diagnostic: Some(diagnostic),
    };
    journal
        .append_model_events(
            &attempt,
            ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: 1,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("argument-evidence"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::ToolCallStarted {
                        output_index: 0,
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                        provider_item_ref: None,
                        provider_replay: None,
                    },
                    ModelStreamEvent::ToolCallCompleted {
                        output_index: 0,
                        call_id: call.call_id.clone(),
                        arguments: call.arguments.clone(),
                    },
                    ModelStreamEvent::ResponseCompleted { stop_reason: stop },
                ],
            },
        )
        .await
        .expect("events");
    let completion = ModelCompletionCommit {
        invocation_id: iteration.invocation_id,
        kind: ModelCompletionKind::ToolCalls,
        tool_calls: vec![call.clone()],
        reasoning_replay: None,
        usage: None,
        provider_response_ref: provider_ref("argument-evidence"),
        response_model_key: response_model_key(),
        continuation_ref: None,
    };
    let mut contradictory = completion.clone();
    contradictory.tool_calls[0].planning_failure = Some(ToolCallPlanningFailure::Truncated);
    contradictory.tool_calls[0]
        .argument_diagnostic
        .as_mut()
        .expect("evidence")
        .generation_limit = Some(if stop == StopReason::MaxOutputTokens {
        musteragent_runtime_model::GenerationLimit::OutputOrContextWindow
    } else {
        musteragent_runtime_model::GenerationLimit::OutputTokens
    });
    assert!(
        journal
            .stage_model_completion(&attempt, contradictory)
            .await
            .is_err(),
        "terminal evidence is authoritative"
    );
    journal
        .stage_model_completion(&attempt, completion)
        .await
        .expect("intent");
    drop(journal);
    for _ in 0..2 {
        let journal = open();
        let checkpoint = journal.load_checkpoint(&attempt).await.expect("checkpoint");
        let state = journal
            .recover_checkpoint(
                &attempt,
                checkpoint,
                ModelRecoveryConstraints {
                    recovery_policy: std::num::NonZeroU32::new(8).expect("policy").into(),
                },
            )
            .await
            .expect("recover");
        assert_eq!(state.pending_tool_calls, vec![call.clone()]);
        let feedback = call
            .planning_failure
            .expect("failure")
            .diagnostic_json(&state.pending_tool_calls[0])
            .expect("feedback");
        assert_eq!(
            feedback["diagnostic"]["generation_limit"],
            serde_json::to_value(stop.generation_limit()).unwrap()
        );
        assert_eq!(feedback["execution"], "not_executed");
        assert!(!feedback.to_string().contains("private"));
    }
    assert!(
        sqlx::query(
            "UPDATE runtime.pending_tool_calls SET argument_diagnostic_json=NULL WHERE run_id=$1"
        )
        .bind(fixture.run_id.into_uuid())
        .execute(&pool)
        .await
        .is_err(),
        "original diagnostics are immutable"
    );
}
