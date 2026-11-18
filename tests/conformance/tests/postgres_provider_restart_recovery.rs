use std::future::pending;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[path = "support/live_runtime.rs"]
mod live_runtime;
#[path = "support/owned_scripted_model.rs"]
mod owned_model;
#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "postgres_provider_recovery/support.rs"]
mod support;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::MediaType;
use musteragent_context::{ContextCodec, RuntimeSnapshotDecoder};
use musteragent_identity::UtcTimestamp;
use musteragent_loop::{
    AgentLoopClock, AgentLoopExit, AgentLoopFailureStage, AgentLoopJournal, ContextPlanError,
    ContextPlanInput, ContextPlanner, DefaultLoopPolicy, DurableAgentLoop, DurableLoopCheckpoint,
    ExecutionPolicySource, InvocationSupervisor, LoopControlQueue, LoopPortError,
    ModelActionRejectionCommit, ModelCompletionCommit, ModelEventBatch, ModelFailureCommit,
    ModelInvocationObservation, ModelIteration, ModelRecoveryConstraints, PinnedRunExecutionPolicy,
    PlannedToolCall, RecoveredLoopState, RunCompletionDisposition, ToolExecutionOutcome,
    ToolExecutor, ToolResultCommit,
};
use musteragent_model::ModelKey;
use musteragent_postgres::{PostgresAgentLoopJournal, PostgresContentStore, PostgresDispatchStore};
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_content::{BlobStore, ContentStore, PutContent};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, OpaqueExternalRef};
use musteragent_runtime_model::{
    CanonicalModelRequest, ModelEventStream, ModelProvider, ModelStreamEvent, ModelUsage,
    ProviderRecoveryCapability, ProviderRequestRef, StopReason,
};
use musteragent_runtime_snapshot::{PinnedModelRoute, RunExecutionPolicySnapshot, RuntimeSnapshot};
use musteragent_scheduler::{ClaimOutcome, DispatchStore};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::{
    CanonicalJsonContextCodec, ProtobufMeteringPublicationEncoder, ProtobufSnapshotCodec,
};
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};

use running_loop::{Fixture, attempt_telemetry, database_now, keyring, seed_running_loop};
use support::model_iteration;

struct PersistentAppendFailureJournal {
    inner: Arc<dyn AgentLoopJournal>,
    attempts: Mutex<Vec<ModelEventBatch>>,
}

impl PersistentAppendFailureJournal {
    fn new(inner: Arc<dyn AgentLoopJournal>) -> Self {
        Self {
            inner,
            attempts: Mutex::new(Vec::new()),
        }
    }

    fn unavailable() -> LoopPortError {
        LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        }
    }
}

impl AgentLoopJournal for PersistentAppendFailureJournal {
    fn recovery_budget(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RecoveryBudget, LoopPortError>> {
        self.inner.recovery_budget(attempt)
    }

    fn load_model_observation(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::ModelInvocationObservation, LoopPortError>>
    {
        self.inner.load_model_observation(attempt, invocation_id)
    }

    fn load_checkpoint(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.load_checkpoint(attempt)
    }

    fn recover_checkpoint(
        &self,
        attempt: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
    ) -> ApplicationFuture<'_, Result<RecoveredLoopState, LoopPortError>> {
        self.inner
            .recover_checkpoint(attempt, checkpoint, constraints)
    }

    fn begin_model(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        recovery_capability: ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .begin_model(attempt, iteration, recovery_capability)
    }

    fn mark_model_dispatched(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.mark_model_dispatched(attempt, invocation_id)
    }

    fn append_model_events(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.attempts
            .lock()
            .expect("append attempts")
            .push(batch.clone());
        if batch.batch_index == 1 {
            return Box::pin(async { Err(Self::unavailable()) });
        }
        self.inner.append_model_events(attempt, batch)
    }

    fn stage_model_completion(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.stage_model_completion(attempt, completion)
    }

    fn finish_model(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.finish_model(attempt, completion)
    }

    fn finish_failed_model(
        &self,
        attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.finish_failed_model(attempt, commit)
    }

    fn finish_rejected_model_action(
        &self,
        attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.finish_rejected_model_action(attempt, commit)
    }

    fn commit_tool_result(
        &self,
        attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.commit_tool_result(attempt, result)
    }

    fn wait_for_reconciliation(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
        observed_at: UtcTimestamp,
        resume_not_before: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        self.inner
            .wait_for_reconciliation(attempt, invocation_id, observed_at, resume_not_before)
    }

    fn complete(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunCompletionDisposition, LoopPortError>> {
        self.inner.complete(attempt)
    }

    fn fail(
        &self,
        attempt: &AttemptContext,
        code: ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        max_attempts: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .fail(attempt, code, retryable, retry_after_ms, max_attempts)
    }

    fn interrupt_external_outcome(
        &self,
        attempt: &AttemptContext,
        reconciliation_ref: OpaqueExternalRef,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .interrupt_external_outcome(attempt, reconciliation_ref)
    }

    fn acknowledge_coordination_interrupt(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .acknowledge_coordination_interrupt(attempt, observation)
    }

    fn acknowledge_cancellation(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.acknowledge_cancellation(attempt, observation)
    }
}

struct FixedClock(UtcTimestamp);

impl AgentLoopClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

struct FixedPolicySource(UtcTimestamp);

impl ExecutionPolicySource for FixedPolicySource {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let mut snapshot = RunExecutionPolicySnapshot::production_defaults();
        snapshot.max_model_events_per_commit = NonZeroUsize::new(2).expect("non-zero");
        let policy = PinnedRunExecutionPolicy {
            snapshot,
            admitted_at: self.0,
            model_route_count: NonZeroU32::MIN,
        };
        Box::pin(async move { Ok(policy) })
    }
}

struct Planner {
    fixture: Fixture,
    content: Arc<dyn ContentStore>,
    route: PinnedModelRoute,
    request_ref: musteragent_content_ref::ContentRef,
}

impl ContextPlanner for Planner {
    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let mut iteration = model_iteration(
            self.fixture,
            input.attempt,
            self.request_ref.clone(),
            input.next_iteration,
        );
        iteration.request.continuation_ref = input.continuation_ref;
        // Supervised execution binds the admitted timing, route and the exact
        // canonical request bytes before the provider is dispatched.
        iteration.request.invocation.timing = input.invocation_timing;
        iteration.request.route = self.route.route.clone();
        iteration.request.credential_grant_ref = self.route.credential_grant_ref.clone();
        Box::pin(async move {
            iteration.request_ref = self
                .content
                .put(PutContent {
                    tenant_id: iteration.request.invocation.run_context.tenant_id,
                    media_type: MediaType::new("application/vnd.musteragent.model-request.v4+json")
                        .expect("media"),
                    bytes: CanonicalJsonContextCodec
                        .encode_model_request(&iteration.request)
                        .expect("canonical request"),
                })
                .await
                .expect("store canonical request");
            Ok(iteration)
        })
    }
}

/// Production admission seals a v14 snapshot with the model liveness profile;
/// the seeded Run gets that snapshot once, and the replacement process reuses
/// the same sealed admission.
async fn install_supervised_snapshot(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
) -> PinnedModelRoute {
    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE runtime_snapshot_id=$1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read seeded snapshot");
    let snapshot = if encoded == b"snapshot" {
        let mut controlled = musteragent_conformance::fixtures::context_snapshot::snapshot_content(
            fixture.run_id,
            musteragent_conformance::fixtures::agent::system_version(7, "assistant-base@1")
                .material
                .expect("material")
                .policy_content_ref,
        );
        controlled.tools.clear();
        controlled.model.route = support::model_route();
        let mut snapshot =
            live_runtime::upgrade_snapshot(content, running_loop::run_context(fixture), controlled)
                .await;
        snapshot.execution_policy.max_model_events_per_commit =
            NonZeroUsize::new(2).expect("non-zero");
        let sealed = RuntimeSnapshot::seal(
            fixture.runtime_snapshot_id,
            snapshot,
            database_now(pool).await,
            &ProtobufSnapshotCodec,
        )
        .expect("seal supervised snapshot");
        sqlx::query(
            "UPDATE capability.runtime_snapshots
             SET schema_version=$2, encoded_snapshot=$3, content_hash=$4
             WHERE runtime_snapshot_id=$1",
        )
        .bind(fixture.runtime_snapshot_id.into_uuid())
        .bind(sealed.content.schema_version.as_str())
        .bind(&sealed.encoded_content)
        .bind(sealed.content_hash.into_bytes().to_vec())
        .execute(pool)
        .await
        .expect("install supervised snapshot");
        sealed.content
    } else {
        ProtobufSnapshotCodec
            .decode_runtime_snapshot(&encoded)
            .expect("reuse the original sealed admission")
    };
    PinnedModelRoute {
        route: snapshot.model.route,
        credential_grant_ref: snapshot.model.credential_grant_ref,
    }
}

struct EmptyControls;

impl LoopControlQueue for EmptyControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}

struct NoTools;

impl ToolExecutor for NoTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async { pending().await })
    }
}

struct SinglePhysicalInvocation {
    calls: AtomicUsize,
}

impl SinglePhysicalInvocation {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl ModelProvider for SinglePhysicalInvocation {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        Box::pin(tokio_stream::iter(
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: ProviderRequestRef::from_opaque(
                        OpaqueExternalRef::new("persistent-append-response").expect("response"),
                    ),
                    response_model_key: ModelKey::new("persistent-append-model").expect("model"),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 11,
                    output_tokens: 2,
                    total_tokens: Some(13),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                },
            ]
            .into_iter()
            .map(Ok),
        ))
    }
}

async fn runtime(
    pool: &sqlx::PgPool,
    fixture: Fixture,
    provider: Arc<SinglePhysicalInvocation>,
    blobs: Arc<dyn BlobStore>,
    fault: bool,
) -> (
    Arc<DurableAgentLoop>,
    Option<Arc<PersistentAppendFailureJournal>>,
) {
    let now = database_now(pool).await;
    let keys = keyring();
    let content: Arc<dyn ContentStore> =
        Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys.clone()));
    let route = install_supervised_snapshot(pool, content.clone(), fixture).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"persistent append restart".to_vec(),
        })
        .await
        .expect("store request");
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let inner: Arc<dyn AgentLoopJournal> = Arc::new(
        PostgresAgentLoopJournal::new(
            pool.clone(),
            content.clone(),
            codec.clone(),
            codec,
            Arc::new(ProtobufMeteringPublicationEncoder),
            keys,
            running_loop::recovery_keyring(),
        )
        .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), live_runtime::clock()),
    );
    let (journal, faults): (Arc<dyn AgentLoopJournal>, _) = if fault {
        let faults = Arc::new(PersistentAppendFailureJournal::new(inner));
        (faults.clone(), Some(faults))
    } else {
        (inner, None)
    };
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedPolicySource(now)),
        Arc::new(Planner {
            fixture,
            content,
            route,
            request_ref,
        }),
        Arc::new(NoTools),
        Arc::new(EmptyControls),
    ));
    (
        Arc::new(
            DurableAgentLoop::new(
                Arc::new(FixedClock(now)),
                policy,
                Arc::new(owned_model::OwnedScriptedModel(provider)),
                journal,
            )
            .with_execution_supervision(live_runtime::supervision(pool)),
        ),
        faults,
    )
}

async fn transfer_fence(pool: &sqlx::PgPool, attempt: &AttemptContext) -> AttemptContext {
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
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let claimed = scheduler
        .claim_one(
            &WorkerId::new("cap016-replacement-process").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim replacement");
    let ClaimOutcome::Claimed(claimed) = claimed else {
        panic!("replacement process must claim the expired run");
    };
    AttemptContext {
        attempt_id: claimed.fence.attempt_id,
        run_id: claimed.fence.run_id,
        attempt_number: claimed.fence.attempt_number,
        worker_id: claimed.fence.worker_id,
        lease_id: claimed.fence.lease_id,
        fencing_token: claimed.fence.fencing_token,
        lease_expires_at: claimed.lease_expires_at,
    }
}

#[tokio::test]
async fn persistent_append_failure_never_redispatches_the_nonresumable_invocation() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, stale_attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(SinglePhysicalInvocation::new());
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
    let (first, faults) = runtime(&pool, fixture, provider.clone(), blobs.clone(), true).await;

    assert_eq!(
        first
            .execute(
                stale_attempt.clone(),
                attempt_telemetry(fixture, &stale_attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let attempts = faults
        .expect("fault journal")
        .attempts
        .lock()
        .expect("append attempts")
        .clone();
    let unacknowledged = attempts
        .iter()
        .filter(|batch| batch.batch_index == 1)
        .collect::<Vec<_>>();
    assert_eq!(unacknowledged.len(), 2);
    assert_eq!(unacknowledged[0], unacknowledged[1]);
    let before_transfer: (i16, bool, i64, i64) = sqlx::query_as(
        "SELECT invocation.status, invocation.provider_dispatched_at_ms IS NOT NULL,
                (SELECT count(*) FROM runtime.model_event_batches
                   WHERE invocation_id = invocation.invocation_id),
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1)
         FROM runtime.model_invocations AS invocation WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read interrupted physical invocation");
    assert_eq!(before_transfer, (1, true, 1, 0));

    let replacement_attempt = transfer_fence(&pool, &stale_attempt).await;
    let (replacement, _) = runtime(&pool, fixture, provider.clone(), blobs, false).await;
    assert_eq!(
        replacement
            .execute(
                replacement_attempt.clone(),
                attempt_telemetry(fixture, &replacement_attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Failed {
            stage: AgentLoopFailureStage::RecoverCheckpoint,
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
            failure_commit_error: None,
            receipt: None,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let recovered: (i16, i16, i16, i16, i64, i64, i64, bool) = sqlx::query_as(
        "SELECT invocation.status, attempt.status, run.status,
                (SELECT status FROM runtime.attempts
                   WHERE run_id = $1 ORDER BY attempt_number DESC LIMIT 1),
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                (SELECT count(*) FROM event_store.outbox
                   WHERE topic = 'musteragent.model-usage.v1'),
                (SELECT count(*) FROM conversation.messages WHERE run_id = $1),
                run.not_before_ms IS NOT NULL
         FROM runtime.model_invocations AS invocation
         JOIN runtime.attempts AS attempt
           ON attempt.attempt_id = invocation.attempt_id
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovery close");
    assert_eq!(recovered, (3, 6, 3, 4, 1, 1, 0, true));
    pool.close().await;
}

#[tokio::test]
async fn declared_resumable_profiles_fail_closed_without_a_resume_proof_port() {
    for capability in [
        ProviderRecoveryCapability::ResumableStream,
        ProviderRecoveryCapability::QueryableResponse,
        ProviderRecoveryCapability::IdempotentRequest,
    ] {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start PostgreSQL");
        let pool = postgres.pool().await.expect("connect PostgreSQL");
        musteragent_conformance::migrate(&pool)
            .await
            .expect("migrate");
        let (fixture, stale_attempt) = seed_running_loop(&pool).await;
        let keys = keyring();
        let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(MemoryBlobs::in_memory()),
            keys.clone(),
        ));
        let request_ref = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("application/x-musteragent-model-request")
                    .expect("media"),
                bytes: b"declared resumable profile".to_vec(),
            })
            .await
            .expect("store request");
        let codec = Arc::new(ProtobufLedgerPayloadCodec);
        let journal = musteragent_conformance::fixtures::model_journal::supervise(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content.clone(),
                codec.clone(),
                codec,
                Arc::new(ProtobufMeteringPublicationEncoder),
                keys,
                running_loop::recovery_keyring(),
            ),
        );
        let mut iteration = model_iteration(fixture, stale_attempt.clone(), request_ref, 1);
        iteration.request.invocation.timing =
            musteragent_conformance::fixtures::model_journal::model_timing();
        musteragent_conformance::fixtures::model_journal::freeze_model_request(
            content.as_ref(),
            &mut iteration,
        )
        .await;
        journal
            .begin_model(&stale_attempt, &iteration, capability)
            .await
            .expect("begin physical invocation");
        journal
            .mark_model_dispatched(&stale_attempt, iteration.invocation_id)
            .await
            .expect("mark dispatch");
        let replacement = transfer_fence(&pool, &stale_attempt).await;
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
            .expect("fail-closed recovery");
        assert!(matches!(
            recovered.checkpoint.phase,
            musteragent_loop::LoopPhase::Failed
        ));
        assert!(!recovered.replacement_attempt_scheduled);
        let terminal: (i16, i16, i64) = sqlx::query_as(
            "SELECT invocation.status, run.status,
                    (SELECT count(*) FROM runtime.attempts WHERE run_id = $1)
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run USING (run_id)
             WHERE invocation.invocation_id = $2",
        )
        .bind(fixture.run_id.into_uuid())
        .bind(iteration.invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read fail-closed recovery");
        assert_eq!(terminal, (3, 6, 2), "{capability:?}");
        pool.close().await;
    }
}
