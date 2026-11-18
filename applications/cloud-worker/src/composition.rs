//! Process composition. PostgreSQL owns all claims, fences and recovery facts.
use crate::PersistedContextCompactor;
use musteragent_context::DurableContextPlanner;
use musteragent_identity::UtcTimestamp;
use musteragent_loop::AgentLoopClock;
use musteragent_postgres::*;
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_model::{MeteringPublicationEncoder, ModelProvider};
use musteragent_runtime_worker::{RuntimeWorkerRunner, WorkerRunnerPolicy};
use musteragent_scheduler::{DurableScheduler, SchedulerPolicy};
use musteragent_storage_protection::{OpaqueRefKeyring, ProtectedContentKeyring};
use musteragent_wire::{
    CanonicalJsonContextCodec, ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder,
    ProtobufSnapshotCodec,
};
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Composition failures retain their original typed source.
pub type BuildError = Box<dyn std::error::Error + Send + Sync>;

/// Shared wall clock. Lease authority always comes from PostgreSQL.
pub struct SystemRuntimeClock;
impl RuntimeClock for SystemRuntimeClock {
    fn now(&self) -> UtcTimestamp {
        now()
    }
}
impl AgentLoopClock for SystemRuntimeClock {
    fn now(&self) -> UtcTimestamp {
        now()
    }
}
fn now() -> UtcTimestamp {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("UTC clock before epoch");
    UtcTimestamp::from_unix_millis(i64::try_from(value.as_millis()).expect("UTC clock overflow"))
}

/// Required host services. No credentials or commercial policy are inferred.
pub struct WorkerHost {
    pub database: PostgresDatabaseRuntime,
    pub content: Arc<dyn ContentStore>,
    pub model: Arc<dyn ModelProvider>,
    pub metering: Arc<dyn MeteringPublicationEncoder>,
    pub reference_keys: Arc<OpaqueRefKeyring>,
    pub recovery_keys: Arc<ProtectedContentKeyring>,
    pub dispatcher: Arc<dyn crate::CapabilityDispatcher>,
    pub extensions: crate::WorkerExtensionConfig,
    pub observer: Option<Arc<dyn musteragent_runtime_worker::WorkerRunnerObserver>>,
    pub liveness_clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
}

/// Validated execution limits shared by scheduler and runner.
#[derive(Clone)]
pub struct WorkerPolicy {
    pub worker_id: musteragent_run::WorkerId,
    pub concurrency: NonZeroUsize,
    pub lease: Duration,
    pub heartbeat: Duration,
    pub drain: Duration,
}

/// Scheduler and executor are stopped and joined together; no detached attempts.
pub struct CloudWorkerComposition {
    scheduler: DurableScheduler,
    runner: RuntimeWorkerRunner,
    dispatch: mpsc::Receiver<musteragent_ledger::ExecutionReceipt>,
    database: PostgresDatabaseRuntime,
    wake: Arc<PostgresWakeSignal>,
    schedules: PostgresScheduleDispatcher,
    delegation: PostgresCloudDelegation,
    device_lineages: Option<PostgresWorkItemDeviceLineageSweep>,
    extensions: Arc<musteragent_extension::ExtensionHost>,
    extension_workspace: std::path::PathBuf,
}
impl CloudWorkerComposition {
    /// Verify the kernel schema and compose all durable owners before starting claims.
    pub async fn build(host: WorkerHost, policy: WorkerPolicy) -> Result<Self, BuildError> {
        verify_migrations(host.database.pool()).await?;
        let codec = Arc::new(ProtobufLedgerPayloadCodec);
        let schedules = PostgresScheduleDispatcher::new(
            host.database.clone(),
            host.content.clone(),
            codec.clone(),
        );
        let clock = Arc::new(SystemRuntimeClock);
        let live_clock = host.liveness_clock;
        let delegation = PostgresCloudDelegation::new(
            host.database.clone(),
            host.content.clone(),
            codec.clone(),
            host.reference_keys.clone(),
            live_clock.clone(),
        );
        let journal = Arc::new(
            PostgresAgentLoopJournal::new(
                host.database.clone(),
                host.content.clone(),
                codec.clone(),
                codec.clone(),
                host.metering.clone(),
                host.reference_keys.clone(),
                host.recovery_keys,
            )
            .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), live_clock.clone()),
        );
        let supervision = journal
            .execution_supervision(Arc::new(
                musteragent_execution_liveness_io::HomeExecutionActivityFactory,
            ))
            .ok_or("execution supervision unavailable")?;
        let tool_journal = Arc::new(
            PostgresFencedToolInvocationJournal::new(
                host.database.clone(),
                codec.clone(),
                codec.clone(),
                host.reference_keys.clone(),
            )
            .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), live_clock.clone()),
        );
        let compaction_liveness = Arc::new(PostgresFencedExecutionLivenessJournal::new(
            host.database.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
            codec.clone(),
            live_clock,
        ));
        let compactor = Arc::new(
            PersistedContextCompactor::new(
                host.model.clone(),
                PostgresContextCompactionSlotStore::new(
                    host.database.clone(),
                    host.reference_keys.clone(),
                )
                .with_execution_liveness(compaction_liveness),
                host.content.clone(),
                host.metering,
                clock.clone(),
            )
            .with_execution_supervision(supervision.clone()),
        );
        let material = Arc::new(PostgresContextMaterialSource::new(
            host.database.clone(),
            host.content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
        ));
        let context = Arc::new(DurableContextPlanner::new(
            material.clone(),
            host.content.clone(),
            Arc::new(
                PostgresContextPlanStore::new(host.database.clone(), codec.clone())
                    .with_delivery_repair_sources(host.content.clone(), codec.clone()),
            ),
            Arc::new(CanonicalJsonContextCodec),
            compactor,
        ));
        let execution = Arc::new(PostgresExecutionPolicySource::new(
            host.database.pool().clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
        ));
        let controls = Arc::new(PostgresLoopControlQueue::new(
            host.database.clone(),
            codec.clone(),
        ));
        let extensions = musteragent_extension::ExtensionHost::new(
            host.extensions.registry,
            Arc::new(PostgresExtensionJournal::new(host.database.clone())),
            musteragent_extension::RuntimePorts {
                content: host.content.clone(),
                history: Arc::new(PostgresExtensionToolHistory::new(
                    host.database.clone(),
                    host.content.clone(),
                )),
                state_root: host.extensions.state_root,
                subagents: Some(delegation.restricted_spawner(host.extensions.subagent_agents)),
            },
            host.extensions.loop_detection,
        )?;
        let maintenance = Arc::new(PostgresEvolutionMaintenanceStore::new(
            host.database.clone(),
            host.content.clone(),
            journal.clone(),
            host.reference_keys.clone(),
        ));
        let extension_workspace = host.extensions.workspace_root.clone();
        let agent = Arc::new(crate::extensions::ExtendedWorkerExecutor {
            extensions: extensions.clone(),
            workspace_root: host.extensions.workspace_root,
            material,
            database: host.database.clone(),
            content: host.content,
            codec: codec.clone(),
            tool_journal,
            dispatcher: host.dispatcher,
            clock,
            execution,
            context,
            controls,
            model: host.model,
            maintenance,
            journal: journal.clone(),
            supervision,
            concurrency: policy.concurrency,
        });
        let database = host.database.clone();
        let wake = Arc::new(PostgresWakeSignal::new());
        let store = Arc::new(PostgresDispatchStore::with_interaction_expiry(
            host.database,
            Arc::new(ProtobufSchedulerEventEncoder),
            codec,
            host.reference_keys,
        ));
        let (tx, dispatch) = mpsc::channel(policy.concurrency.get());
        let scheduler = DurableScheduler::new(
            SchedulerPolicy {
                claim_batch_size: policy.concurrency.get(),
                lease_duration: policy.lease,
                idle_wakeup_ceiling: Duration::from_secs(1),
            },
            policy.worker_id,
            store.clone(),
            wake.clone(),
            tx,
        )?
        .with_cancellation_recovery(journal.clone());
        let mut runner = RuntimeWorkerRunner::new(
            WorkerRunnerPolicy {
                max_inflight_runs: policy.concurrency,
                lease_duration: policy.lease,
                heartbeat_interval: policy.heartbeat,
                shutdown_drain_timeout: policy.drain,
            },
            store,
            journal,
            agent,
        )?;
        if let Some(observer) = host.observer {
            runner = runner.with_observer(observer);
        }
        Ok(Self {
            scheduler,
            runner,
            dispatch,
            database,
            wake,
            schedules,
            device_lineages: None,
            delegation,
            extensions,
            extension_workspace,
        })
    }

    /// Enable background settlement using the same trusted membership owner as device pickup.
    #[must_use]
    pub fn with_device_membership_authority(
        mut self,
        targets: Arc<dyn musteragent_runtime_session::WorkItemDeviceTargetAuthority>,
    ) -> Self {
        let admission =
            PostgresAdmissionStore::new(self.database.clone(), Arc::new(ProtobufSnapshotCodec));
        self.device_lineages = Some(PostgresWorkItemDeviceLineageSweep::new(
            PostgresWorkItemExecutionStore::new(admission),
            targets,
        ));
        self
    }

    /// A sibling failure cancels the whole generation and waits for runner cleanup.
    pub async fn run(self, stop: CancellationToken) -> Result<(), BuildError> {
        let generation = stop.child_token();
        let scheduler_stop = generation.clone();
        let runner_stop = generation.clone();
        let scheduler = async {
            let result = self.scheduler.run(scheduler_stop).await;
            generation.cancel();
            result
        };
        let runner = async {
            let result = self.runner.run(self.dispatch, runner_stop).await;
            generation.cancel();
            result
        };
        let listener = async {
            let result = self
                .wake
                .run(
                    self.database.pool(),
                    PostgresWakeChannel::Dispatch,
                    generation.clone(),
                )
                .await;
            generation.cancel();
            result
        };
        let occurrences = async {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    _=generation.cancelled()=>return Ok::<(),BuildError>(()),
                    _=interval.tick()=>{
                        if let Err(error)=self.delegation.dispatch_lifecycle(&self.extensions,&self.extension_workspace,now()).await {
                            tracing::error!(%error,"delegation lifecycle hook delivery remains pending");
                        }
                        if let Err(error)=self.delegation.tick(now()).await {
                            tracing::error!(%error,"delegation reconciliation failed; durable child remains recoverable");
                        }
                        if let Err(error)=self.schedules.tick(now()).await {
                            tracing::error!(%error,"schedule dispatch failed; durable occurrence remains recoverable");
                        }
                    }
                }
            }
        };
        let lineages = async {
            if let Some(sweep) = self.device_lineages {
                let owner = uuid::Uuid::now_v7();
                loop {
                    let delay = match sweep.sweep_once(owner, 64).await {
                        Ok(report) if !report.wrapped => Duration::from_secs(1),
                        Ok(_) => Duration::from_secs(300),
                        Err(error) => {
                            tracing::warn!(?error, "Device membership sweep deferred");
                            Duration::from_secs(30)
                        }
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        };
        let (scheduled, executed, listened, occurred, ()) =
            tokio::join!(scheduler, runner, listener, occurrences, async {
                tokio::select! { _ = generation.cancelled() => {}, _ = lineages => {} }
            });
        scheduled?;
        executed?;
        listened?;
        occurred?;
        Ok(())
    }
}
