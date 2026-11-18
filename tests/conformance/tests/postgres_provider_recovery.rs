use std::sync::Arc;
use std::time::Duration;

#[path = "support/exhausted_model_round.rs"]
mod exhausted_round;
#[path = "postgres_provider_recovery/persistent.rs"]
mod persistent;
#[path = "support/runtime_recovery_budget.rs"]
mod recovery_budget_cases;
#[path = "postgres_provider_recovery/retention.rs"]
mod retention;
#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "postgres_provider_recovery/support.rs"]
mod support;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::MediaType;
use musteragent_conversation::ConversationEvent;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{ToolCallId, VersionedKey};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_loop::{
    AgentLoopJournal, ModelEventBatch, ModelFailure, ModelFailureCommit, ModelFailureResolution,
    ModelInvocationObservation, ModelRecoveryConstraints,
};
use musteragent_model::ModelKey;
use musteragent_postgres::{PostgresAgentLoopJournal, PostgresContentStore, PostgresDispatchStore};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef, RetryDisposition};
use musteragent_runtime_model::{
    ModelEvent, ModelStreamEvent, ModelText, ModelUsage, ProviderRecoveryCapability,
    ProviderRequestRef,
};
use musteragent_scheduler::{ClaimOutcome, DispatchStore};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::WireName;
use musteragent_wire::ProtobufMeteringPublicationEncoder;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};
use sqlx::PgPool;

use running_loop::{Fixture, database_now, keyring, seed_running_loop};
use support::model_iteration;

#[tokio::test]
async fn loop_sealed_fallback_boundary() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (journal, content) = journal(&pool);
    let codec = ProtobufLedgerPayloadCodec;
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"primary sealed route request".to_vec(),
        })
        .await
        .expect("store request");
    let mut primary = model_iteration(fixture, attempt.clone(), request_ref, 1);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut primary,
    )
    .await;
    journal
        .begin_model(&attempt, &primary, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("begin primary route");
    journal
        .mark_model_dispatched(&attempt, primary.invocation_id)
        .await
        .expect("mark primary dispatched");
    let switched = musteragent_conformance::fixtures::model_journal::finish_failed_model(
        &pool,
        &journal,
        &attempt,
        ModelFailureCommit {
            recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
            observation: ModelInvocationObservation {
                invocation_id: primary.invocation_id,
                provider_response_ref: None,
                response_model_key: None,
                usage: None,
                public_output_observed: false,
                replay_barrier_observed: false,
            },
            provider_failure: ModelFailure {
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
            },
            retry_after_ms: None,
            resolution: ModelFailureResolution::RetryFallbackRoute {
                next_route_ordinal: 1,
            },
        },
    )
    .await
    .expect("switch to the next sealed route");
    assert_eq!(switched.phase, musteragent_loop::LoopPhase::ReadyForModel);
    assert_eq!(switched.active_model_route_ordinal, 1);

    let audit_payloads: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event USING (record_id)
         WHERE record.run_id = $1 ORDER BY record.run_seq",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read fallback audit");
    let fallback_failures = audit_payloads
        .iter()
        .filter(|payload| {
            matches!(
                codec.decode(LedgerRecordKind::DomainEvent, payload),
                Ok(LedgerPayload::DomainEvent(DomainEvent::Model(
                    ModelEvent::InvocationFailed {
                        invocation_id,
                        retry_disposition: RetryDisposition::RetryFallbackRoute,
                        ..
                    }
                ))) if invocation_id == primary.invocation_id
            )
        })
        .count();
    assert_eq!(
        fallback_failures, 1,
        "exactly one audited fallback boundary"
    );

    let fallback_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"fallback sealed route request".to_vec(),
        })
        .await
        .expect("store fallback request");
    let mut fallback = model_iteration(fixture, attempt.clone(), fallback_ref, 2);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut fallback,
    )
    .await;
    fallback.request.route.route_ref =
        VersionedKey::new("model.route.fallback@1").expect("fallback route");
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut fallback,
    )
    .await;
    journal
        .begin_model(
            &attempt,
            &fallback,
            ProviderRecoveryCapability::NonResumable,
        )
        .await
        .expect("begin fallback route");
    journal
        .mark_model_dispatched(&attempt, fallback.invocation_id)
        .await
        .expect("mark fallback dispatched");
    assert!(
        musteragent_conformance::fixtures::model_journal::finish_failed_model(
            &pool,
            &journal,
            &attempt,
            ModelFailureCommit {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
                observation: ModelInvocationObservation {
                    invocation_id: fallback.invocation_id,
                    provider_response_ref: None,
                    response_model_key: None,
                    usage: None,
                    public_output_observed: false,
                    replay_barrier_observed: false,
                },
                provider_failure: ModelFailure {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                },
                retry_after_ms: None,
                resolution: ModelFailureResolution::RetryFallbackRoute {
                    next_route_ordinal: 3,
                },
            },
        )
        .await
        .is_err(),
        "the Journal must reject skipping beyond the next sealed route ordinal"
    );
    let unchanged: (i32, i16, String) = sqlx::query_as(
        "SELECT checkpoint.active_model_route_ordinal, invocation.status,
                run.last_run_sequence::text
         FROM runtime.loop_checkpoints AS checkpoint
         JOIN runtime.runs AS run USING (run_id)
         JOIN runtime.model_invocations AS invocation
           ON invocation.invocation_id = checkpoint.model_invocation_id
         WHERE checkpoint.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rejected fallback state");
    assert_eq!(
        unchanged,
        (1, 1, "10".into()),
        "rejected route skip preserves both invocation and liveness facts"
    );
    pool.close().await;
}

#[tokio::test]
async fn journal_rejects_caller_false_when_durable_text_or_tool_output_exists() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (journal, content) = journal(&pool);
    let rollout_default: String = sqlx::query_scalar(
        "SELECT column_default
         FROM information_schema.columns
         WHERE table_schema = 'runtime'
           AND table_name = 'model_invocations'
           AND column_name = 'replay_barrier_observed'",
    )
    .fetch_one(&pool)
    .await
    .expect("read rolling-upgrade default");
    assert_eq!(rollout_default, "true");

    for visible in [VisibleOutput::Text, VisibleOutput::Tool] {
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let iteration =
            begin_iteration(&pool, &journal, &content, fixture, &attempt, visible).await;
        let response_ref = provider_ref(match visible {
            VisibleOutput::Text => "response-text-visible",
            VisibleOutput::Tool => "response-tool-visible",
        });
        let mut events = vec![ModelStreamEvent::ResponseStarted {
            response_ref: response_ref.clone(),
            response_model_key: response_model_key(),
        }];
        match visible {
            VisibleOutput::Text => events.push(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new("durable partial").expect("text"),
            }),
            VisibleOutput::Tool => events.push(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: ToolCallId::new("durable-visible-tool").expect("call"),
                name: WireName::new("visible_tool").expect("tool"),
                provider_item_ref: None,
                provider_replay: None,
            }),
        }
        journal
            .append_model_events(
                &attempt,
                ModelEventBatch {
                    invocation_id: iteration.invocation_id,
                    iteration: 1,
                    batch_index: 0,
                    events,
                },
            )
            .await
            .expect("persist visible batch");

        let rejected = musteragent_conformance::fixtures::model_journal::finish_failed_model(
            &pool,
            &journal,
            &attempt,
            ModelFailureCommit {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
                observation: ModelInvocationObservation {
                    invocation_id: iteration.invocation_id,
                    provider_response_ref: Some(response_ref.clone()),
                    response_model_key: Some(response_model_key()),
                    usage: None,
                    public_output_observed: false,
                    replay_barrier_observed: false,
                },
                provider_failure: ModelFailure {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                },
                retry_after_ms: None,
                resolution: ModelFailureResolution::RetryFallbackRoute {
                    next_route_ordinal: 1,
                },
            },
        )
        .await;
        assert!(
            rejected.is_err(),
            "{visible:?}: caller visibility was trusted"
        );
        let rejected_retry = musteragent_conformance::fixtures::model_journal::finish_failed_model(
            &pool,
            &journal,
            &attempt,
            ModelFailureCommit {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
                observation: ModelInvocationObservation {
                    invocation_id: iteration.invocation_id,
                    provider_response_ref: Some(response_ref),
                    response_model_key: Some(response_model_key()),
                    usage: None,
                    public_output_observed: true,
                    replay_barrier_observed: true,
                },
                provider_failure: ModelFailure {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                },
                retry_after_ms: None,
                resolution: ModelFailureResolution::RetryNewAttempt,
            },
        )
        .await;
        assert!(
            rejected_retry.is_err(),
            "{visible:?}: durable public output permitted a new Attempt"
        );
        let unchanged: (i16, i32, i16, bool) = sqlx::query_as(
            "SELECT invocation.status, checkpoint.active_model_route_ordinal,
                    checkpoint.phase, invocation.public_output_observed
             FROM runtime.model_invocations AS invocation
             JOIN runtime.loop_checkpoints AS checkpoint
               ON checkpoint.model_invocation_id = invocation.invocation_id
             WHERE invocation.invocation_id = $1",
        )
        .bind(iteration.invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read rejected state");
        assert_eq!(unchanged, (1, 0, 2, true), "{visible:?}");
        let rollback_counts: (i64, i64, i64, i16) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM runtime.model_usage_facts
                    WHERE invocation_id = $1),
                 (SELECT count(*) FROM conversation.messages
                    WHERE run_id = $2 AND lifecycle = 2),
                 (SELECT count(*) FROM runtime.leases WHERE run_id = $2),
                 (SELECT status FROM runtime.runs WHERE run_id = $2)",
        )
        .bind(iteration.invocation_id.into_uuid())
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read rolled-back close facts");
        assert_eq!(rollback_counts, (0, 0, 1, 2), "{visible:?}");
    }
    pool.close().await;
}

#[tokio::test]
async fn retry_cancel_and_terminal_failure_close_every_owner_row_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (journal, content) = journal(&pool);

    for transition in [
        AtomicTransition::RetryNewAttempt,
        AtomicTransition::FailRun,
        AtomicTransition::CancelRun,
    ] {
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let iteration = begin_iteration(
            &pool,
            &journal,
            &content,
            fixture,
            &attempt,
            VisibleOutput::Text,
        )
        .await;
        let response_ref = provider_ref(match transition {
            AtomicTransition::RetryNewAttempt => "atomic-retry-response",
            AtomicTransition::FailRun => "atomic-fail-response",
            AtomicTransition::CancelRun => "atomic-cancel-response",
        });
        let usage = ModelUsage {
            input_tokens_uncached: 6,
            output_tokens: 2,
            total_tokens: Some(8),
            ..ModelUsage::default()
        };
        let partial = match transition {
            AtomicTransition::RetryNewAttempt => ModelStreamEvent::ReasoningSummaryDelta {
                output_index: 0,
                delta: ModelText::new("private atomic partial").expect("reasoning"),
            },
            AtomicTransition::FailRun | AtomicTransition::CancelRun => {
                ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("atomic partial").expect("text"),
                }
            }
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
                            response_ref: response_ref.clone(),
                            response_model_key: response_model_key(),
                        },
                        partial,
                        ModelStreamEvent::Usage(usage),
                    ],
                },
            )
            .await
            .expect("persist atomic prefix");
        let observation = ModelInvocationObservation {
            invocation_id: iteration.invocation_id,
            provider_response_ref: Some(response_ref),
            response_model_key: Some(response_model_key()),
            usage: Some(usage),
            public_output_observed: true,
            replay_barrier_observed: !matches!(transition, AtomicTransition::RetryNewAttempt),
        };
        let closed = match transition {
            AtomicTransition::RetryNewAttempt => {
                musteragent_conformance::fixtures::model_journal::finish_failed_model(
                    &pool,
                    &journal,
                    &attempt,
                    ModelFailureCommit {
                        recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
                        observation,
                        provider_failure: ModelFailure {
                            code: ErrorCode::DependencyTimeout,
                            retryable: true,
                        },
                        retry_after_ms: None,
                        resolution: ModelFailureResolution::RetryNewAttempt,
                    },
                )
                .await
            }
            AtomicTransition::FailRun => {
                musteragent_conformance::fixtures::model_journal::finish_failed_model(
                    &pool,
                    &journal,
                    &attempt,
                    ModelFailureCommit {
                        recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
                        observation,
                        provider_failure: ModelFailure {
                            code: ErrorCode::QuotaExceeded,
                            retryable: true,
                        },
                        retry_after_ms: None,
                        resolution: ModelFailureResolution::FailRun {
                            failure: ModelFailure {
                                code: ErrorCode::QuotaExceeded,
                                retryable: false,
                            },
                        },
                    },
                )
                .await
            }
            AtomicTransition::CancelRun => {
                musteragent_conformance::fixtures::model_journal::confirm_model_stop(
                    &pool,
                    &attempt,
                    iteration.invocation_id,
                )
                .await;
                journal
                    .acknowledge_cancellation(&attempt, Some(observation))
                    .await
            }
        }
        .expect("atomic model close");
        assert_eq!(closed.phase, transition.expected_phase());

        let state: (i16, i16, i16, i64, i64, i64, bool) = sqlx::query_as(
            "SELECT invocation.status, attempt.status, run.status,
                    (SELECT count(*) FROM runtime.leases WHERE run_id = $2),
                    (SELECT count(*) FROM runtime.model_usage_facts
                       WHERE invocation_id = $1),
                    (SELECT count(*) FROM conversation.messages
                       WHERE run_id = $2 AND lifecycle = 2),
                    run.not_before_ms IS NOT NULL
             FROM runtime.model_invocations AS invocation
             JOIN runtime.attempts AS attempt
               ON attempt.attempt_id = invocation.attempt_id
             JOIN runtime.runs AS run ON run.run_id = invocation.run_id
             WHERE invocation.invocation_id = $1",
        )
        .bind(iteration.invocation_id.into_uuid())
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read atomic owner rows");
        assert_eq!(state, transition.expected_rows());
        let frame_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM event_store.output_frames AS frame
             JOIN event_store.ledger_records AS record USING (record_id)
             WHERE record.run_id = $1",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read atomic partial frames");
        assert_eq!(
            frame_count, 2,
            "{transition:?}: interrupted output must be sealed, not erased"
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn terminal_owner_write_failure_rolls_back_invocation_usage_partial_and_run_together() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (journal, content) = journal(&pool);
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let iteration = begin_iteration(
        &pool,
        &journal,
        &content,
        fixture,
        &attempt,
        VisibleOutput::Text,
    )
    .await;
    let response_ref = provider_ref("rollback-response");
    let usage = ModelUsage {
        input_tokens_uncached: 4,
        output_tokens: 1,
        total_tokens: Some(5),
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
                        response_ref: response_ref.clone(),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new("must roll back").expect("text"),
                    },
                    ModelStreamEvent::Usage(usage),
                ],
            },
        )
        .await
        .expect("persist rollback prefix");
    sqlx::query(
        "CREATE FUNCTION runtime.reject_cap016_run_terminal() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.status IN (6, 7) THEN
                 RAISE EXCEPTION 'CAP-016 injected terminal owner failure';
             END IF;
             RETURN NEW;
         END
         $$",
    )
    .execute(&pool)
    .await
    .expect("install rollback function");
    sqlx::query(
        "CREATE TRIGGER cap016_reject_terminal
         BEFORE UPDATE ON runtime.runs
         FOR EACH ROW EXECUTE FUNCTION runtime.reject_cap016_run_terminal()",
    )
    .execute(&pool)
    .await
    .expect("install rollback trigger");

    let rejected = musteragent_conformance::fixtures::model_journal::finish_failed_model(
        &pool,
        &journal,
        &attempt,
        ModelFailureCommit {
            recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
            observation: ModelInvocationObservation {
                invocation_id: iteration.invocation_id,
                provider_response_ref: Some(response_ref),
                response_model_key: Some(response_model_key()),
                usage: Some(usage),
                public_output_observed: true,
                replay_barrier_observed: true,
            },
            provider_failure: ModelFailure {
                code: ErrorCode::QuotaExceeded,
                retryable: false,
            },
            retry_after_ms: None,
            resolution: ModelFailureResolution::FailRun {
                failure: ModelFailure {
                    code: ErrorCode::QuotaExceeded,
                    retryable: false,
                },
            },
        },
    )
    .await;
    assert!(rejected.is_err());
    let rolled_back: (i16, i16, i16, i16, i64, i64, i64) = sqlx::query_as(
        "SELECT invocation.status, attempt.status, run.status, checkpoint.phase,
                (SELECT count(*) FROM runtime.leases WHERE run_id = $2),
                (SELECT count(*) FROM runtime.model_usage_facts
                   WHERE invocation_id = $1),
                (SELECT count(*) FROM conversation.messages
                   WHERE run_id = $2 AND lifecycle = 2)
         FROM runtime.model_invocations AS invocation
         JOIN runtime.attempts AS attempt
           ON attempt.attempt_id = invocation.attempt_id
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         JOIN runtime.loop_checkpoints AS checkpoint ON checkpoint.run_id = run.run_id
         WHERE invocation.invocation_id = $1",
    )
    .bind(iteration.invocation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rolled-back terminal owner rows");
    assert_eq!(rolled_back, (1, 1, 2, 2, 1, 0, 0));
    pool.close().await;
}

#[tokio::test]
async fn journal_rejects_retry_new_attempt_for_retryable_but_disallowed_codes() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (journal, content) = journal(&pool);

    for code in [
        ErrorCode::QuotaExceeded,
        ErrorCode::PolicyDenied,
        ErrorCode::Unauthenticated,
        ErrorCode::InvalidCommand,
        ErrorCode::ModelRouteUnavailable,
    ] {
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let iteration = begin_iteration(
            &pool,
            &journal,
            &content,
            fixture,
            &attempt,
            VisibleOutput::Text,
        )
        .await;
        let rejected = musteragent_conformance::fixtures::model_journal::finish_failed_model(
            &pool,
            &journal,
            &attempt,
            ModelFailureCommit {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
                observation: ModelInvocationObservation {
                    invocation_id: iteration.invocation_id,
                    provider_response_ref: None,
                    response_model_key: None,
                    usage: None,
                    public_output_observed: false,
                    replay_barrier_observed: false,
                },
                provider_failure: ModelFailure {
                    code,
                    retryable: true,
                },
                retry_after_ms: None,
                resolution: ModelFailureResolution::RetryNewAttempt,
            },
        )
        .await;
        assert!(rejected.is_err(), "{code:?}");
        let unchanged: (i16, i16, i64) = sqlx::query_as(
            "SELECT invocation.status, run.status,
                    (SELECT count(*) FROM runtime.leases WHERE run_id = $2)
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run USING (run_id)
             WHERE invocation.invocation_id = $1",
        )
        .bind(iteration.invocation_id.into_uuid())
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read rejected retry state");
        assert_eq!(unchanged, (1, 2, 1), "{code:?}");
    }
    pool.close().await;
}

#[tokio::test]
async fn replacement_terminalizes_nonresumable_visible_partial_and_usage_exactly_once() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (journal, content) = journal(&pool);
    let (fixture, stale_attempt) = seed_running_loop(&pool).await;
    let iteration = begin_iteration(
        &pool,
        &journal,
        &content,
        fixture,
        &stale_attempt,
        VisibleOutput::Text,
    )
    .await;
    let usage = ModelUsage {
        input_tokens_uncached: 10,
        output_tokens: 3,
        reasoning_tokens: 1,
        total_tokens: Some(13),
        ..ModelUsage::default()
    };
    journal
        .append_model_events(
            &stale_attempt,
            ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: 1,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("provider-response-before-lease-loss"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new("durable before lease loss").expect("text"),
                    },
                    ModelStreamEvent::Usage(usage),
                ],
            },
        )
        .await
        .expect("persist interruption observations");

    expire_attempt(&pool, &stale_attempt).await;
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let claimed = scheduler
        .claim_one(
            &WorkerId::new("cap016-recovery-worker").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim replacement Attempt");
    let ClaimOutcome::Claimed(claimed) = claimed else {
        panic!("expired model Attempt must be recovered");
    };
    assert_eq!(claimed.fence.attempt_number.get(), 2);
    let replacement = AttemptContext {
        attempt_id: claimed.fence.attempt_id,
        run_id: claimed.fence.run_id,
        attempt_number: claimed.fence.attempt_number,
        worker_id: claimed.fence.worker_id,
        lease_id: claimed.fence.lease_id,
        fencing_token: claimed.fence.fencing_token,
        lease_expires_at: claimed.lease_expires_at,
    };
    let loaded = journal
        .load_checkpoint(&replacement)
        .await
        .expect("load replacement checkpoint");
    let recovered = journal
        .recover_checkpoint(
            &replacement,
            loaded,
            ModelRecoveryConstraints {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
            },
        )
        .await
        .expect("seal abandoned invocation from durable observations");
    assert!(matches!(
        recovered.checkpoint.phase,
        musteragent_loop::LoopPhase::Failed
    ));
    assert!(recovered.checkpoint.continuation_ref.is_none());
    assert!(
        recovered
            .checkpoint
            .continuation_source_invocation_id
            .is_none()
    );

    let facts: (
        i16,
        i64,
        i64,
        i64,
        bool,
        String,
        String,
        Vec<u8>,
        uuid::Uuid,
    ) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id = $1),
                (SELECT count(*) FROM event_store.outbox
                   WHERE topic = 'musteragent.model-usage.v1'),
                (SELECT count(*) FROM conversation.messages
                   WHERE run_id = $2 AND lifecycle = 2),
                usage.cancelled,
                usage.requested_model_key,
                usage.response_model_key,
                invocation.provider_request_ref,
                usage.attempt_id
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_usage_facts AS usage USING (invocation_id)
         WHERE invocation.invocation_id = $1",
    )
    .bind(iteration.invocation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovered facts");
    assert_eq!(facts.0, 3);
    assert_eq!((facts.1, facts.2, facts.3), (1, 1, 1));
    assert!(!facts.4);
    assert_eq!(facts.5, "requested-model");
    assert_eq!(facts.6, "provider-response-model");
    assert_eq!(facts.8, stale_attempt.attempt_id.into_uuid());
    assert!(
        !facts
            .7
            .windows("provider-response-before-lease-loss".len())
            .any(|window| window == b"provider-response-before-lease-loss")
    );

    let projected: (i16, uuid::Uuid, uuid::Uuid) = sqlx::query_as(
        "SELECT lifecycle, content_id, message_id FROM conversation.messages
         WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3 AND role=2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("recovered partial message");
    assert_eq!(projected.0, 2, "interrupted output remains visibly partial");
    // committed_sequence is the atomic batch boundary, not the partial event's position.
    let events: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event FROM event_store.domain_events AS event
         JOIN event_store.ledger_records AS record ON record.record_id=event.record_id
         WHERE record.tenant_id=$1 AND record.conversation_id=$2 AND record.run_id=$3",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("recovery ledger events");
    let partials = events.iter().filter(|bytes| matches!(
        ProtobufLedgerPayloadCodec.decode(LedgerRecordKind::DomainEvent, bytes),
        Ok(LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessagePartiallyCommitted { message_id, content_ref, .. }
        ))) if content_ref.content_id.into_uuid() == projected.1 && message_id.into_uuid() == projected.2
    )).count();
    assert_eq!(partials, 1, "one canonical partial event owns the message");

    let repeated = journal
        .recover_checkpoint(
            &replacement,
            recovered.checkpoint.clone(),
            ModelRecoveryConstraints {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
            },
        )
        .await
        .expect_err("terminal recovery releases the replacement fence");
    assert_eq!(repeated.code, ErrorCode::AttemptLeaseStale);
    let dedup: (i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id = $1),
             (SELECT count(*) FROM event_store.outbox
                WHERE topic = 'musteragent.model-usage.v1')",
    )
    .bind(iteration.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read dedup counts");
    assert_eq!(dedup, (1, 1));
    pool.close().await;
}

#[derive(Debug, Copy, Clone)]
enum VisibleOutput {
    Text,
    Tool,
}

#[derive(Debug, Copy, Clone)]
enum AtomicTransition {
    RetryNewAttempt,
    FailRun,
    CancelRun,
}

impl AtomicTransition {
    fn expected_phase(self) -> musteragent_loop::LoopPhase {
        match self {
            Self::RetryNewAttempt => musteragent_loop::LoopPhase::ReadyForModel,
            Self::FailRun => musteragent_loop::LoopPhase::Failed,
            Self::CancelRun => musteragent_loop::LoopPhase::Cancelled,
        }
    }

    const fn expected_rows(self) -> (i16, i16, i16, i64, i64, i64, bool) {
        match self {
            Self::RetryNewAttempt => (3, 4, 3, 0, 1, 0, true),
            Self::FailRun => (3, 4, 6, 0, 1, 1, false),
            Self::CancelRun => (4, 5, 7, 0, 1, 1, false),
        }
    }
}

fn journal(pool: &PgPool) -> (PostgresAgentLoopJournal, Arc<dyn ContentStore>) {
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let journal = reopen_journal(pool, content.clone());
    (journal, content)
}

fn reopen_journal(pool: &PgPool, content: Arc<dyn ContentStore>) -> PostgresAgentLoopJournal {
    let keys = keyring();
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    musteragent_conformance::fixtures::model_journal::supervise(PostgresAgentLoopJournal::new(
        pool.clone(),
        content.clone(),
        codec.clone(),
        codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
    ))
}

async fn begin_iteration(
    _pool: &PgPool,
    journal: &PostgresAgentLoopJournal,
    content: &Arc<dyn ContentStore>,
    fixture: Fixture,
    attempt: &AttemptContext,
    visible: VisibleOutput,
) -> musteragent_loop::ModelIteration {
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: format!("CAP-016 {visible:?}").into_bytes(),
        })
        .await
        .expect("store request");
    let mut iteration = model_iteration(fixture, attempt.clone(), request_ref, 1);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut iteration,
    )
    .await;
    journal
        .begin_model(
            attempt,
            &iteration,
            ProviderRecoveryCapability::NonResumable,
        )
        .await
        .expect("begin model");
    journal
        .mark_model_dispatched(attempt, iteration.invocation_id)
        .await
        .expect("mark model dispatched");
    iteration
}

async fn expire_attempt(pool: &PgPool, attempt: &AttemptContext) {
    let expired_at = database_now(pool).await.unix_millis().saturating_sub(1);
    sqlx::query("UPDATE runtime.leases SET expires_at_ms = $2 WHERE run_id = $1")
        .bind(attempt.run_id.into_uuid())
        .bind(expired_at)
        .execute(pool)
        .await
        .expect("expire lease");
    sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms = $2 WHERE attempt_id = $1")
        .bind(attempt.attempt_id.into_uuid())
        .bind(expired_at)
        .execute(pool)
        .await
        .expect("expire Attempt");
}

fn provider_ref(value: &str) -> ProviderRequestRef {
    ProviderRequestRef::from_opaque(OpaqueExternalRef::new(value).expect("provider ref"))
}

fn response_model_key() -> ModelKey {
    ModelKey::new("provider-response-model").expect("response model")
}

#[tokio::test]
async fn provider_recovery_budget_is_durable_fenced_and_exhausts_after_eight_starts() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    for mode in recovery_budget_cases::FAILED_STARTS {
        let (_, content) = journal(&pool);
        let (fixture, mut attempt) = seed_running_loop(&pool).await;
        let request_ref = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("application/x-musteragent-model-request")
                    .expect("media"),
                bytes: b"durable recovery budget request".to_vec(),
            })
            .await
            .expect("store request");
        for failure_index in 0..8 {
            let journal = reopen_journal(&pool, content.clone());
            let mut iteration = model_iteration(
                fixture,
                attempt.clone(),
                request_ref.clone(),
                failure_index + 1,
            );
            musteragent_conformance::fixtures::model_journal::freeze_model_request(
                content.as_ref(),
                &mut iteration,
            )
            .await;
            recovery_budget_cases::exercise_failed_start(
                &pool,
                &journal,
                &iteration,
                failure_index,
                mode,
            )
            .await;
            if failure_index < 7 {
                attempt = claim_budget_retry(&pool, fixture).await;
            }
        }
        let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT event.encoded_event FROM event_store.ledger_records AS record
             JOIN event_store.domain_events AS event USING (record_id)
             WHERE record.run_id=$1 ORDER BY record.run_seq DESC LIMIT 5",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_all(&pool)
        .await
        .expect("terminal facts");
        let expected_code = if matches!(mode, recovery_budget_cases::FailedStart::EmptyResponse) {
            ErrorCode::ModelResponseInvalid
        } else {
            ErrorCode::DependencyUnavailable
        };
        assert!(payloads.iter().any(|bytes| matches!(ProtobufLedgerPayloadCodec.decode(LedgerRecordKind::DomainEvent, bytes).expect("decode"),
            LedgerPayload::DomainEvent(DomainEvent::Run(musteragent_run::RunEvent::Failed { error, .. }))
                if error.code == expected_code && error.safe_message.as_str().contains("Recovery exhausted after 8 failed attempts"))), "{mode:?}");
    }
}

#[tokio::test]
async fn recovery_budget_bounds_port_failures_across_journal_restarts() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, mut attempt) = seed_running_loop(&pool).await;
    for failure_index in 0..3 {
        let (journal, _) = journal(&pool);
        let budget = journal
            .recovery_budget(&attempt)
            .await
            .expect("durable budget");
        assert_eq!(budget.provider_failures, 0);
        assert_eq!(budget.no_progress_failures, failure_index);
        let checkpoint = journal
            .fail(
                &attempt,
                ErrorCode::DependencyUnavailable,
                true,
                None,
                std::num::NonZeroU32::new(3).expect("limit").into(),
            )
            .await
            .expect("bounded Port failure");
        if failure_index == 2 {
            assert_eq!(checkpoint.phase, musteragent_loop::LoopPhase::Failed);
        } else {
            assert_eq!(checkpoint.phase, musteragent_loop::LoopPhase::ReadyForModel);
            attempt = claim_budget_retry(&pool, fixture).await;
        }
    }
}

async fn claim_budget_retry(pool: &PgPool, fixture: Fixture) -> AttemptContext {
    sqlx::query("UPDATE runtime.runs SET not_before_ms=0 WHERE run_id=$1")
        .bind(fixture.run_id.into_uuid())
        .execute(pool)
        .await
        .expect("advance backoff");
    let dispatch =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let ClaimOutcome::Claimed(receipt) = dispatch
        .claim_one(
            &WorkerId::new("recovery-budget").expect("worker"),
            Duration::from_secs(120),
        )
        .await
        .expect("claim retry")
    else {
        panic!("retry must be ready");
    };
    assert_eq!(receipt.fence.run_id, fixture.run_id);
    AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    }
}
