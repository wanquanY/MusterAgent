use super::*;
use musteragent_loop::{ModelFailure, ModelFailureCommit, ModelFailureResolution};

#[tokio::test]
async fn durable_stop_arbitrates_admission_failure_and_retry_in_the_commit_lane() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let journal = audited_loop_journal(
        &pool,
        content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
        running_loop::recovery_keyring(),
    );
    for (boundary, stop) in [
        ("begin", true),
        ("model", true),
        ("model", false),
        ("model-retry", true),
        ("model-partial", true),
        ("port", true),
        ("port-retry", true),
    ] {
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let request = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("application/x-musteragent-model-request").unwrap(),
                bytes: b"stop race fixture".to_vec(),
            })
            .await
            .unwrap();
        let mut iteration = model_iteration(fixture, attempt.clone(), request, 1);
        musteragent_conformance::fixtures::model_journal::freeze_model_request(
            content.as_ref(),
            &mut iteration,
        )
        .await;
        if boundary.starts_with("model") {
            journal
                .begin_model(
                    &attempt,
                    &iteration,
                    ProviderRecoveryCapability::NonResumable,
                )
                .await
                .unwrap();
        }
        if boundary == "model-partial" {
            journal
                .mark_model_dispatched(&attempt, iteration.invocation_id)
                .await
                .unwrap();
            journal
                .append_model_events(
                    &attempt,
                    ModelEventBatch {
                        invocation_id: iteration.invocation_id,
                        iteration: 1,
                        batch_index: 0,
                        events: vec![
                            ModelStreamEvent::ResponseStarted {
                                response_ref: provider_ref("stop-race"),
                                response_model_key: response_model_key(),
                            },
                            ModelStreamEvent::TextDelta {
                                output_index: 0,
                                delta: ModelText::new("retained response").unwrap(),
                            },
                            ModelStreamEvent::Usage(ModelUsage {
                                input_tokens_uncached: 20,
                                total_tokens: Some(20),
                                ..Default::default()
                            }),
                        ],
                    },
                )
                .await
                .unwrap();
        }
        if boundary.starts_with("model") {
            musteragent_conformance::fixtures::model_journal::confirm_model_stop(
                &pool,
                &attempt,
                iteration.invocation_id,
            )
            .await;
        }
        if stop {
            sqlx::query(
                "UPDATE runtime.runs SET status=4, cancel_requested_at_ms=1 WHERE run_id=$1",
            )
            .bind(fixture.run_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
        }
        let retry = boundary.ends_with("retry");
        let code = if retry {
            ErrorCode::DependencyUnavailable
        } else {
            ErrorCode::PolicyDenied
        };
        let policy = musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default();
        let checkpoint = if boundary == "begin" {
            journal
                .begin_model(
                    &attempt,
                    &iteration,
                    ProviderRecoveryCapability::NonResumable,
                )
                .await
                .unwrap()
        } else if boundary.starts_with("model") {
            let observation = journal
                .load_model_observation(&attempt, iteration.invocation_id)
                .await
                .unwrap();
            let failure = ModelFailure {
                code,
                retryable: retry,
            };
            journal
                .finish_failed_model(
                    &attempt,
                    ModelFailureCommit {
                        observation,
                        provider_failure: failure,
                        retry_after_ms: None,
                        recovery_policy: policy,
                        resolution: if retry {
                            ModelFailureResolution::RetryNewAttempt
                        } else {
                            ModelFailureResolution::FailRun { failure }
                        },
                    },
                )
                .await
                .unwrap()
        } else {
            journal
                .fail(&attempt, code, retry, None, policy)
                .await
                .unwrap()
        };
        assert_eq!(
            checkpoint.phase,
            if stop {
                LoopPhase::Cancelled
            } else {
                LoopPhase::Failed
            },
            "{boundary}"
        );
        let events: Vec<Vec<u8>> = sqlx::query_scalar("SELECT event.encoded_event FROM event_store.ledger_records record JOIN event_store.domain_events event USING (record_id) WHERE record.run_id=$1 ORDER BY record.conversation_seq")
            .bind(fixture.run_id.into_uuid()).fetch_all(&pool).await.unwrap();
        let events: Vec<_> = events
            .iter()
            .map(|b| {
                ProtobufLedgerPayloadCodec
                    .decode(LedgerRecordKind::DomainEvent, b)
                    .unwrap()
            })
            .collect();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(
                    e,
                    LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Cancelled { .. }))
                ))
                .count(),
            usize::from(stop)
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(
                    e,
                    LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Failed { .. }))
                ))
                .count(),
            usize::from(!stop)
        );
        assert!(!events.iter().any(|e| matches!(
            e,
            LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::RetryScheduled { .. }))
        )));
        if boundary == "model-partial" {
            assert!(events.iter().any(|event| matches!(event,
                LedgerPayload::DomainEvent(DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
                    reason: musteragent_conversation::PartialMessageReason::Cancelled,
                    content_bytes, ..
                })) if *content_bytes == "retained response".len() as u64
            )));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        LedgerPayload::DomainEvent(DomainEvent::Model(
                            musteragent_runtime_model::ModelEvent::UsageRecorded(_)
                        ))
                    ))
                    .count(),
                1,
                "stop records observed usage once"
            );
        }
        if boundary == "begin" {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM runtime.model_invocations WHERE run_id=$1",
            )
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, 0);
        }
    }
    pool.close().await;
}
