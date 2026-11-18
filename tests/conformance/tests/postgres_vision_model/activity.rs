use super::*;
use musteragent_execution_liveness::{
    Closure, ExecutionRecord, ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError,
    FencedExecutionLivenessJournal, Phase, StopReason as ExecutionStopReason,
};
use musteragent_execution_liveness_io::{HomeExecutionActivityFactory, SystemLivenessHomeClock};
use musteragent_postgres::PostgresFencedExecutionLivenessJournal;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, ActivitySource, ExecutionBinding, ExecutionLivenessPolicy,
    ExecutionScope, InvocationExecutionTiming,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_model::{ModelExecution, ProviderError};
use musteragent_runtime_snapshot::{PinnedAuxiliaryModelRoute, RuntimeSnapshot};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufSnapshotCodec;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::Stream;

use musteragent_conformance::fixtures::liveness_admission as admission;

struct ScriptedProvider {
    stall: bool,
    sends: Arc<AtomicUsize>,
    progress_interval: Duration,
}
struct OwnedModel {
    binding: ExecutionBinding,
    request: CanonicalModelRequest,
    control: Arc<Control>,
    stall: bool,
    sends: Arc<AtomicUsize>,
    progress_interval: Duration,
}
struct Control {
    binding: ExecutionBinding,
    dispatched: AtomicBool,
    terminal: AtomicBool,
    cancelled: AtomicBool,
}
impl ExecutionStopControl for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        _: ExecutionStopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async move {
            self.cancelled.store(true, Ordering::SeqCst);
            if !self.dispatched.load(Ordering::SeqCst) || self.terminal.load(Ordering::SeqCst) {
                Ok(ExecutionStopConfirmation {
                    binding: self.binding,
                })
            } else {
                Err(ExecutionStopError {
                    code: ErrorCode::DependencyUnavailable,
                    rejection: None,
                })
            }
        })
    }
}
impl ModelProvider for ScriptedProvider {
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("ActivityV1 cannot invoke legacy stream")
    }
    fn execution(
        &self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'_> + '_>, ProviderError> {
        musteragent_runtime_model::validate_model_execution_binding(&request, binding)?;
        Ok(Box::new(OwnedModel {
            binding,
            request,
            control: Arc::new(Control {
                binding,
                dispatched: AtomicBool::new(false),
                terminal: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
            }),
            stall: self.stall,
            sends: self.sends.clone(),
            progress_interval: self.progress_interval,
        }))
    }
}
impl<'a> ModelExecution<'a> for OwnedModel {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        Box::pin(async { Ok(()) })
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.control.dispatched.store(true, Ordering::SeqCst);
        let mut events = VecDeque::from([ModelStreamEvent::ResponseStarted {
            response_ref: ProviderRequestRef::from_opaque(
                OpaqueExternalRef::new("owned-vision-response").expect("reference"),
            ),
            response_model_key: ModelKey::new(self.request.route.resolved_model_key)
                .expect("model"),
        }]);
        if !self.stall {
            for _ in 0..8 {
                events.push_back(ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("visible image detail ").expect("text"),
                });
            }
            events.push_back(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 20,
                output_tokens: 30,
                total_tokens: Some(50),
                ..ModelUsage::default()
            }));
            events.push_back(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            });
        }
        Box::pin(ProgressStream {
            events,
            control: self.control,
            stall: self.stall,
            due: Box::pin(tokio::time::sleep(Duration::ZERO)),
            progress_interval: self.progress_interval,
        })
    }
}
struct ProgressStream {
    events: VecDeque<ModelStreamEvent>,
    control: Arc<Control>,
    stall: bool,
    due: Pin<Box<tokio::time::Sleep>>,
    progress_interval: Duration,
}
impl Stream for ProgressStream {
    type Item = Result<ModelStreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future as _;
        if self.due.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        let Some(event) = self.events.pop_front() else {
            return if self.stall {
                Poll::Pending
            } else {
                Poll::Ready(None)
            };
        };
        if matches!(event, ModelStreamEvent::ResponseCompleted { .. }) {
            self.control.terminal.store(true, Ordering::SeqCst);
        }
        let interval = self.progress_interval;
        self.due
            .as_mut()
            .reset(tokio::time::Instant::now() + interval);
        Poll::Ready(Some(Ok(event)))
    }
}

pub(super) async fn vision_uses_shared_supervision(stall: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, owner) = running_loop::seed_running_loop(&pool).await;
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let context = running_loop::run_context(fixture);
    let (windows, progress_interval) = admission::database_activity_timing();
    let policy = Arc::new(
        ExecutionLivenessPolicy::new(
            musteragent_identity::VersionedKey::new("test.auxiliary-tool@1").expect("key"),
            windows,
            [ActivitySource::ExecutorWork],
        )
        .expect("policy"),
    );
    let (mut snapshot, _, _) = admission::fixture(content.clone(), context, &owner, policy).await;
    let mut model = tool_material::fixture_model_route();
    model.route.route_ref = musteragent_identity::VersionedKey::new("fixture.auxiliary-vision@1")
        .expect("unique auxiliary route");
    model
        .route
        .capabilities
        .insert(DottedKey::new("vision").expect("vision"));
    let model = PinnedModelRoute {
        route: model.route,
        credential_grant_ref: model.credential_grant_ref,
    };
    snapshot
        .content
        .auxiliary_models
        .push(PinnedAuxiliaryModelRoute {
            role: musteragent_model::AuxiliaryModelRole::Vision,
            model: model.clone(),
        });
    snapshot
        .content
        .auxiliary_models
        .sort_by_key(|value| value.role);
    let snapshot = RuntimeSnapshot::seal(
        snapshot.runtime_snapshot_id,
        snapshot.content,
        running_loop::database_now(&pool).await,
        &ProtobufSnapshotCodec,
    )
    .expect("sealed auxiliary route");
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec()).bind(&snapshot.encoded_content)
        .bind(fixture.runtime_snapshot_id.into_uuid()).execute(&pool).await.expect("snapshot");
    let clock = Arc::new(SystemLivenessHomeClock::default());
    let journal = Arc::new(PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        clock.clone(),
    ));
    let sends = Arc::new(AtomicUsize::new(0));
    let runner = PersistedVisionModelInvoker::new(
        Arc::new(ScriptedProvider {
            stall,
            sends: sends.clone(),
            progress_interval,
        }),
        PostgresModelInvocationStore::new(pool.clone(), keys)
            .with_execution_liveness(journal.clone()),
        content.clone(),
        Arc::new(ProtobufMeteringPublicationEncoder),
        Arc::new(FixedClock(running_loop::database_now(&pool).await)),
    )
    .with_execution_supervision(musteragent_loop::ExecutionSupervision::new(
        journal.clone(),
        clock,
        Arc::new(HomeExecutionActivityFactory),
    ));
    let request = VisionModelInvocation {
        capture_provider_failure: false,
        invocation_context: InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context: context,
            attempt_context: owner.clone(),
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
                policy: snapshot
                    .content
                    .execution_policy
                    .liveness
                    .expect("policy")
                    .model_policy()
                    .reference()
                    .clone(),
                business_deadline: None,
            }),
        },
        model,
        stream_budget: snapshot.content.execution_policy.model_stream,
        images: vec![VisionModelImage {
            image_ref: content
                .put(PutContent {
                    tenant_id: context.tenant_id,
                    media_type: MediaType::new("image/png").expect("media"),
                    bytes: b"image".to_vec(),
                })
                .await
                .expect("image"),
            label: None,
        }],
        question: "Describe the image".into(),
        detail: VisionDetail::Auto,
        analysis_mode: VisionAnalysisMode::Joint,
        task_context: vec![],
    };
    let started = tokio::time::Instant::now();
    let result = runner.analyze(request.clone()).await;
    let scope = ExecutionScope::Invocation(request.invocation_context.invocation_id);
    let ExecutionRecord::Checkpoint(record) = journal
        .load(&owner, scope)
        .await
        .expect("load")
        .expect("original")
    else {
        panic!("checkpoint")
    };
    if stall {
        assert!(
            matches!(
                result,
                Err(
                    musteragent_runtime_capability::VisionCapabilityError::ModelFailure {
                        code: ErrorCode::DependencyTimeout,
                        ..
                    }
                )
            ),
            "{result:?}"
        );
        assert_eq!(
            record.state().phase(),
            Phase::Closed(Closure::StopUnconfirmed(ExecutionStopReason::IdleExpired))
        );
    } else {
        assert!(result.is_ok(), "{result:?}");
        assert!(
            started.elapsed() > windows.idle * 2,
            "real progress outlives multiple idle windows"
        );
        assert_eq!(
            record.state().phase(),
            Phase::Closed(Closure::TerminalObserved)
        );
    }
    assert!(
        runner.analyze(request.clone()).await.is_err(),
        "duplicate cannot redispatch"
    );
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    let row: (i16, i64, i64) = sqlx::query_as("SELECT status,(SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id=$1),(SELECT count(*) FROM security_audit.records WHERE source_model_invocation_id=$1) FROM runtime.model_invocations WHERE invocation_id=$1")
        .bind(request.invocation_context.invocation_id.into_uuid()).fetch_one(&pool).await.expect("terminal facts");
    assert_eq!(row, if stall { (3, 0, 1) } else { (2, 1, 1) });
    let run_status: i16 = sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
        .bind(owner.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("Run status");
    assert_eq!(run_status, 2, "call failure never terminates the Agent Run");
}
