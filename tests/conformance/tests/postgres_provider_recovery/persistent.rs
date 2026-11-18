use super::*;
use musteragent_loop::LoopPhase;
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

#[tokio::test]
async fn persistent_recovery_preserves_partial_output_and_cancel_after_repeated_failures() {
    persistent_recovery_case(false).await;
}

#[tokio::test]
async fn persistent_semantic_repair_stops_after_three_invalid_responses_across_restarts() {
    persistent_recovery_case(true).await;
}

async fn persistent_recovery_case(semantic_repair: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("valid recovery test fixture");
    let pool = postgres.pool().await.expect("valid recovery test fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("valid recovery test fixture");
    let (_, content) = journal(&pool);
    let (fixture, mut attempt) = seed_running_loop(&pool).await;
    let request = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request")
                .expect("valid recovery test fixture"),
            bytes: b"durable persistent recovery request".to_vec(),
        })
        .await
        .expect("valid recovery test fixture");
    let worker = WorkerId::new("persistent-recovery-test").expect("valid recovery test fixture");
    let dispatch =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let mut partial_count = 0_i64;
    for index in 0..24 {
        let active = reopen_journal(&pool, content.clone());
        // Semantic response repair has a separate finite budget; this test
        // exercises only dependencies that can recover without user changes.
        let modes = [
            recovery_budget_cases::FailedStart::Provider,
            recovery_budget_cases::FailedStart::Preflight,
            recovery_budget_cases::FailedStart::PortAfterBegin,
            recovery_budget_cases::FailedStart::PortAfterDispatch,
            recovery_budget_cases::FailedStart::ProcessRestart,
        ];
        let mode = if semantic_repair {
            recovery_budget_cases::FailedStart::EmptyResponse
        } else {
            modes[index as usize % modes.len()]
        };
        let partial = matches!(
            mode,
            recovery_budget_cases::FailedStart::Provider
                | recovery_budget_cases::FailedStart::PortAfterDispatch
                | recovery_budget_cases::FailedStart::ProcessRestart
        );
        partial_count += i64::from(partial);
        let mut iteration = model_iteration(fixture, attempt.clone(), request.clone(), index + 1);
        musteragent_conformance::fixtures::model_journal::freeze_model_request(
            content.as_ref(),
            &mut iteration,
        )
        .await;
        recovery_budget_cases::exercise_failed_start_with_policy(
            &pool,
            &active,
            &iteration,
            index,
            mode,
            RecoveryPolicySnapshot::production_default(),
            partial,
        )
        .await;
        if semantic_repair && index == 2 {
            assert!(matches!(
                dispatch
                    .claim_one(&worker, Duration::from_secs(120))
                    .await
                    .expect("terminal scheduler"),
                ClaimOutcome::Idle { .. }
            ));
            return;
        }
        assert!(matches!(
            dispatch
                .claim_one(&worker, Duration::from_secs(120))
                .await
                .expect("valid recovery test fixture"),
            ClaimOutcome::Idle { .. }
        ));
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM conversation.messages WHERE run_id=$1 AND role=2",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("valid recovery test fixture");
        assert_eq!(
            count, partial_count,
            "each interrupted response is sealed once"
        );
        attempt = claim_budget_retry(&pool, fixture).await;
    }
    let active = reopen_journal(&pool, content);
    assert_eq!(
        active
            .acknowledge_cancellation(&attempt, None)
            .await
            .expect("valid recovery test fixture")
            .phase,
        LoopPhase::Cancelled
    );
    assert!(matches!(
        dispatch
            .claim_one(&worker, Duration::from_secs(120))
            .await
            .expect("valid recovery test fixture"),
        ClaimOutcome::Idle { .. }
    ));
}

#[tokio::test]
async fn permanent_failure_is_terminal_and_cannot_be_resumed() {
    use musteragent_identity::{Actor, CorrelationId, HomeEpoch, PrincipalKind};
    use musteragent_postgres::PostgresRuntimeApplicationStore;
    use musteragent_runtime_session::RuntimeApplicationStore;
    use musteragent_wire::ProtobufSnapshotCodec;
    for with_model in [false, true] {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("postgres");
        let pool = postgres.pool().await.expect("pool");
        musteragent_conformance::migrate(&pool)
            .await
            .expect("migrate");
        let (active, content) = journal(&pool);
        let (fixture, attempt) = seed_running_loop(&pool).await;
        if with_model {
            let request = content
                .put(PutContent {
                    tenant_id: fixture.tenant_id,
                    media_type: MediaType::new("application/x-musteragent-model-request")
                        .expect("media"),
                    bytes: b"repair model".to_vec(),
                })
                .await
                .expect("request");
            let mut iteration = model_iteration(fixture, attempt.clone(), request, 1);
            musteragent_conformance::fixtures::model_journal::freeze_model_request(
                content.as_ref(),
                &mut iteration,
            )
            .await;
            active
                .begin_model(
                    &attempt,
                    &iteration,
                    ProviderRecoveryCapability::NonResumable,
                )
                .await
                .expect("begin");
            active
                .mark_model_dispatched(&attempt, iteration.invocation_id)
                .await
                .expect("dispatch");
            active
                .append_model_events(
                    &attempt,
                    ModelEventBatch {
                        invocation_id: iteration.invocation_id,
                        iteration: iteration.iteration,
                        batch_index: 0,
                        events: vec![
                            ModelStreamEvent::ResponseStarted {
                                response_ref: ProviderRequestRef::from_opaque(
                                    OpaqueExternalRef::new("repair-partial").expect("response"),
                                ),
                                response_model_key: ModelKey::new("test-model").expect("model"),
                            },
                            ModelStreamEvent::TextDelta {
                                output_index: 0,
                                delta: musteragent_runtime_model::ModelText::new(
                                    "Already produced progress.",
                                )
                                .expect("text"),
                            },
                        ],
                    },
                )
                .await
                .expect("partial");
            musteragent_conformance::fixtures::model_journal::confirm_model_stop(
                &pool,
                &attempt,
                iteration.invocation_id,
            )
            .await;
        }
        assert_eq!(
            active
                .fail(
                    &attempt,
                    ErrorCode::CreditsExhausted,
                    false,
                    None,
                    RecoveryPolicySnapshot::production_default()
                )
                .await
                .expect("terminal failure")
                .phase,
            LoopPhase::Failed
        );
        let actor = Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        };
        let worker = WorkerId::new("repair-test").expect("worker");
        let dispatch =
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        assert!(matches!(
            dispatch
                .claim_one(&worker, Duration::from_secs(120))
                .await
                .expect("claim"),
            ClaimOutcome::Idle { .. }
        ));
        let app =
            PostgresRuntimeApplicationStore::new(pool.clone(), Arc::new(ProtobufSnapshotCodec));
        let authority = app
            .authorize_run_control(
                actor,
                fixture.run_id,
                fixture.requester_id,
                HomeEpoch::new(1).expect("epoch"),
                CorrelationId::generate(),
            )
            .await;
        assert!(
            authority.is_err(),
            "terminal Runs reject in-place recovery authority"
        );
        assert!(
            active
                .fail(
                    &attempt,
                    ErrorCode::CreditsExhausted,
                    false,
                    None,
                    RecoveryPolicySnapshot::production_default()
                )
                .await
                .is_err()
        );
        let partials: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM conversation.messages WHERE run_id=$1 AND role=2",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("partials");
        assert_eq!(partials, i64::from(with_model));
    }
}

#[tokio::test]
async fn provider_round_restores_a_recoverable_primary_or_waits_for_repair() {
    let db = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("provider round restores a recoverable primary or waits for repair must succeed");
    let pool = db
        .pool()
        .await
        .expect("provider round restores a recoverable primary or waits for repair must succeed");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("provider round restores a recoverable primary or waits for repair must succeed");
    for permanent in [true, false] {
        let (active, content) = journal(&pool);
        let (fixture, attempt) = super::running_loop::seed_running_loop_with_snapshot(
            &pool,
            (
                content.clone(),
                super::support::model_route(),
                super::exhausted_round::seal,
            ),
        )
        .await;
        let request = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("application/x-musteragent-model-request").expect("provider round restores a recoverable primary or waits for repair must succeed"),
                bytes: b"model round".to_vec(),
            })
            .await
            .expect("provider round restores a recoverable primary or waits for repair must succeed");
        let mut primary = model_iteration(fixture, attempt.clone(), request.clone(), 1);
        musteragent_conformance::fixtures::model_journal::freeze_model_request(
            content.as_ref(),
            &mut primary,
        )
        .await;
        let mut backup = model_iteration(fixture, attempt.clone(), request, 2);
        musteragent_conformance::fixtures::model_journal::freeze_model_request(
            content.as_ref(),
            &mut backup,
        )
        .await;
        super::exhausted_round::exercise(
            &pool,
            &active,
            &attempt,
            [primary, backup],
            content.as_ref(),
            permanent,
        )
        .await;
        let reason: i16 = sqlx::query_scalar(
            "SELECT wait_reason FROM runtime.runs WHERE run_id=$1",
        )
        .bind(attempt.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("provider round restores a recoverable primary or waits for repair must succeed");
        assert_eq!(reason, if permanent { 10 } else { 4 });
        if permanent {
            let app = musteragent_postgres::PostgresRuntimeApplicationStore::new(
                pool.clone(),
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            );
            let dispatch =
                PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
            let actor = musteragent_identity::Actor {
                tenant_id: fixture.tenant_id,
                principal_id: fixture.principal_id,
                principal_kind: musteragent_identity::PrincipalKind::User,
            };
            super::exhausted_round::resume_after_repair(
                &app,
                &dispatch,
                &attempt,
                actor,
                fixture.requester_id,
            )
            .await;
        }
    }
}
