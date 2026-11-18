use super::*;
use musteragent_loop::{ModelFailure, ModelFailureCommit, ModelFailureResolution};
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

#[tokio::test]
async fn empty_generation_limit_preserves_usage_and_releases_attempt_until_backoff() {
    for stop in [StopReason::MaxOutputTokens, StopReason::TokenLimit] {
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
                media_type: MediaType::new("application/json").unwrap(),
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
        let usage = ModelUsage {
            input_tokens_uncached: 10,
            output_tokens: 256,
            reasoning_tokens: 256,
            total_tokens: Some(266),
            ..ModelUsage::default()
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
                            response_ref: provider_ref("empty-limit"),
                            response_model_key: response_model_key(),
                        },
                        ModelStreamEvent::Usage(usage),
                        ModelStreamEvent::ResponseCompleted { stop_reason: stop },
                    ],
                },
            )
            .await
            .expect("durable terminal and usage without visible output");
        let observation = journal
            .load_model_observation(&attempt, iteration.invocation_id)
            .await
            .expect("observation");
        assert_eq!(observation.usage, Some(usage));
        assert!(!observation.public_output_observed);
        journal
            .finish_failed_model(
                &attempt,
                ModelFailureCommit {
                    observation,
                    provider_failure: ModelFailure {
                        code: ErrorCode::ModelResponseInvalid,
                        retryable: true,
                    },
                    recovery_policy: RecoveryPolicySnapshot::production_default(),
                    retry_after_ms: None,
                    resolution: ModelFailureResolution::RetryNewAttempt,
                },
            )
            .await
            .expect("atomic retry and accounting");
        drop(journal);
        let facts: (i16, i16, String, i64, i64) = sqlx::query_as(
            "SELECT invocation.status, invocation.terminal_stop_reason, usage.total_tokens::text,
                    (SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id=$1),
                    (SELECT count(*) FROM event_store.outbox WHERE topic='musteragent.model-usage.v1')
             FROM runtime.model_invocations invocation JOIN runtime.model_usage_facts usage USING (invocation_id)
             WHERE invocation.invocation_id=$1")
            .bind(iteration.invocation_id.into_uuid()).fetch_one(&pool).await.expect("retained facts");
        assert_eq!(
            facts,
            (
                3,
                if stop == StopReason::TokenLimit { 7 } else { 3 },
                "266".into(),
                1,
                1
            )
        );
        assert_persisted_backoff(&pool, &attempt).await;

        // Exercise both sides of the dispatch boundary in the isolated database.
        let future: i64 = sqlx::query_scalar(
            "UPDATE runtime.runs
             SET not_before_ms = (extract(epoch FROM clock_timestamp() + interval '1 day') * 1000)::bigint
             WHERE run_id=$1 RETURNING not_before_ms",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("place retry in the future");
        let scheduler =
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        let worker = WorkerId::new("generation-recovery").unwrap();
        assert!(
            matches!(
                scheduler
                    .claim_one(&worker, Duration::from_secs(120))
                    .await
                    .unwrap(),
                ClaimOutcome::Idle { next_due_at: Some(next), database_now }
                    if next.unix_millis() == future && database_now < next
            ),
            "no hot retry"
        );
        sqlx::query("UPDATE runtime.runs SET not_before_ms=0 WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .execute(&pool)
            .await
            .expect("advance scheduler clock");
        let ClaimOutcome::Claimed(receipt) = scheduler
            .claim_one(&worker, Duration::from_secs(120))
            .await
            .unwrap()
        else {
            panic!("due retry");
        };
        assert_eq!(receipt.fence.run_id, attempt.run_id);
        assert_eq!(receipt.fence.attempt_number.get(), 2);
        let resumed = AttemptContext {
            attempt_id: receipt.fence.attempt_id,
            run_id: receipt.fence.run_id,
            attempt_number: receipt.fence.attempt_number,
            worker_id: receipt.fence.worker_id,
            lease_id: receipt.fence.lease_id,
            fencing_token: receipt.fence.fencing_token,
            lease_expires_at: receipt.lease_expires_at,
        };
        let reopened = open();
        let checkpoint = reopened
            .load_checkpoint(&resumed)
            .await
            .expect("reopened journal");
        let checkpoint = reopened
            .recover_checkpoint(
                &resumed,
                checkpoint,
                ModelRecoveryConstraints {
                    recovery_policy: std::num::NonZeroU32::new(8).unwrap().into(),
                },
            )
            .await
            .expect("same Run recovery");
        assert_eq!(checkpoint.checkpoint.phase, LoopPhase::ReadyForModel);
        let terminal_error: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT terminal_error FROM runtime.runs WHERE run_id=$1")
                .bind(fixture.run_id.into_uuid())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            terminal_error.is_none(),
            "no fabricated quota or context terminal"
        );
    }
}

/// Compare the scheduler deadline with the original RetryScheduled event. Later
/// ledger entries and the final lease release can have different timestamps.
async fn assert_persisted_backoff(pool: &sqlx::PgPool, attempt: &AttemptContext) {
    let rows: Vec<(i64, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT r.not_before_ms, l.occurred_at_ms, e.encoded_event
         FROM runtime.runs r JOIN event_store.ledger_records l USING (run_id)
         JOIN event_store.domain_events e USING (record_id)
         WHERE r.run_id=$1 AND l.attempt_id=$2",
    )
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_all(pool)
    .await
    .expect("read committed retry facts");
    let mut retries = 0;
    for (due, occurred, bytes) in rows {
        let payload = ProtobufLedgerPayloadCodec
            .decode(LedgerRecordKind::DomainEvent, &bytes)
            .expect("decode durable event");
        if let LedgerPayload::DomainEvent(DomainEvent::Run(
            musteragent_run::RunEvent::RetryScheduled {
                failed_attempt_id,
                not_before,
                ..
            },
        )) = payload
        {
            retries += 1;
            assert_eq!(failed_attempt_id, attempt.attempt_id);
            assert_eq!(due, not_before.unix_millis());
            assert!(due > occurred, "retry must have a positive backoff");
            assert_eq!(
                not_before,
                musteragent_loop::replacement_attempt_not_before(
                    UtcTimestamp::from_unix_millis(occurred),
                    attempt,
                    std::num::NonZeroU32::MIN,
                    None,
                    RecoveryPolicySnapshot::production_default()
                )
            );
        }
    }
    assert_eq!(
        retries, 1,
        "the failed attempt must schedule exactly one retry"
    );
}
