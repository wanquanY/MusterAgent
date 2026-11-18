//! Attempt-scoped P5 composition around the original durable execution owners.
use musteragent_extension::*;
use musteragent_loop::*;
use musteragent_postgres::*;
use musteragent_runtime_worker::{AttemptExecutor, AttemptFuture, DispatchedAttempt};
use musteragent_tool_executor::*;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSnapshotCodec};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

/// Host registrations are frozen once; scope and facts remain PostgreSQL-owned.
pub struct WorkerExtensionConfig {
    pub registry: ExtensionRegistry,
    pub state_root: PathBuf,
    pub workspace_root: PathBuf,
    /// Published agent keys extensions may spawn. Empty denies all.
    pub subagent_agents: Vec<String>,
    pub loop_detection: LoopDetectionConfig,
}

pub(crate) struct ExtendedWorkerExecutor {
    pub extensions: Arc<ExtensionHost>,
    pub workspace_root: PathBuf,
    pub material: Arc<PostgresContextMaterialSource>,
    pub database: PostgresDatabaseRuntime,
    pub content: Arc<dyn musteragent_runtime_content::ContentStore>,
    pub codec: Arc<ProtobufLedgerPayloadCodec>,
    pub tool_journal: Arc<PostgresFencedToolInvocationJournal>,
    pub dispatcher: Arc<dyn CapabilityDispatcher>,
    pub clock: Arc<crate::SystemRuntimeClock>,
    pub execution: Arc<PostgresExecutionPolicySource>,
    pub context: Arc<musteragent_context::DurableContextPlanner>,
    pub controls: Arc<PostgresLoopControlQueue>,
    pub model: Arc<dyn musteragent_runtime_model::ModelProvider>,
    pub maintenance: Arc<dyn DreamAgentStateSource>,
    pub journal: Arc<PostgresAgentLoopJournal>,
    pub supervision: ExecutionSupervision,
    pub concurrency: NonZeroUsize,
}
impl ExtendedWorkerExecutor {
    async fn run(
        &self,
        dispatched: DispatchedAttempt,
        telemetry: musteragent_telemetry::AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> Result<AgentLoopExit, crate::BuildError> {
        let attempt = dispatched.context;
        let run = self.material.fenced_run_context(&attempt).await?;
        if run.activity_kind != dispatched.activity_kind {
            return Err("attempt activity identity mismatch".into());
        }
        let scope = ExtensionContext {
            workspace_dir: self
                .workspace_root
                .join(run.tenant_id.to_string())
                .join(run.conversation_id.to_string()),
            session_key: format!("{}:{}", run.conversation_id, attempt.attempt_id),
            run,
            attempt,
            channel: None,
            sandboxed: false,
            ingress: None,
        };
        let dispatcher = Arc::new(RegisteredToolDispatcher::new(
            self.dispatcher.clone(),
            self.extensions.clone(),
            scope.clone(),
        )?);
        let tools = Arc::new(
            DurableToolExecutor::new(
                Arc::new(PostgresToolExecutionMaterialSource::new(
                    self.database.pool().clone(),
                    Arc::new(ProtobufSnapshotCodec),
                    self.codec.clone(),
                )),
                self.tool_journal.clone(),
                self.content.clone(),
                dispatcher,
                self.clock.clone(),
                Arc::new(crate::PostgresWorkerTraceContextSource::new(
                    self.database.pool().clone(),
                )),
            )
            .with_execution_supervision(self.supervision.clone())
            .with_invocation_policy_evaluator(Arc::new(
                ExtensionInvocationPolicy::new(
                    Arc::new(InheritInvocationPolicyEvaluator),
                    self.extensions.clone(),
                ),
            )),
        );
        let ordinary: Arc<dyn LoopPolicy> = Arc::new(DefaultLoopPolicy::new(
            self.execution.clone(),
            self.context.clone(),
            tools,
            self.controls.clone(),
        ));
        let policy: Arc<dyn LoopPolicy> = match scope.run.activity_kind {
            musteragent_activity::ActivityKind::Maintenance => Arc::new(
                musteragent_runtime_maintenance::DreamAgentLoopPolicy::new(
                    self.maintenance.clone(),
                    self.content.clone(),
                    Arc::new(musteragent_wire::CanonicalJsonContextCodec),
                )
                .with_private_tools(
                    musteragent_runtime_capability::PrivateToolRuntime {
                        home: self.supervision.clone(),
                        journal: self.tool_journal.clone(),
                        clock: self.clock.clone(),
                    },
                ),
            ),
            musteragent_activity::ActivityKind::Evaluation => Arc::new(
                musteragent_runtime_automation::EvaluationAgentLoopPolicy::new(
                    ordinary,
                    Arc::new(PostgresStandingGoalJudgeRunSource::new(
                        self.database.pool().clone(),
                        self.content.clone(),
                    )),
                    self.content.clone(),
                    self.tool_journal.clone(),
                    self.clock.clone(),
                )
                .with_execution_supervision(self.supervision.clone()),
            ),
            musteragent_activity::ActivityKind::Chat
            | musteragent_activity::ActivityKind::Coordination
            | musteragent_activity::ActivityKind::Automation
            | musteragent_activity::ActivityKind::Delegation => ordinary,
        };
        let agent = self
            .extensions
            .compose_loop(
                scope.clone(),
                ExtensionLoopPorts {
                    clock: self.clock.clone(),
                    policy,
                    model: self.model.clone(),
                    journal: self.journal.clone(),
                    supervision: self.supervision.clone(),
                },
            )?
            .with_tool_concurrency_limit(self.concurrency);
        self.extensions.start_services(&scope).await?;
        let execution = agent.execute(telemetry, supervisor).await;
        let lifecycle = self
            .extensions
            .lifecycle(&scope, LifecycleEvent::SessionEnd)
            .await;
        let stopped = self.extensions.stop_services(&scope).await;
        // Always join service cleanup. Hook failures are surfaced without
        // replacing a committed run outcome with a fabricated failure receipt.
        let report = execution?;
        lifecycle?;
        stopped?;
        if !report.observer_failures.is_empty() {
            tracing::warn!(
                count = report.observer_failures.len(),
                "extension observers reported failures"
            );
        }
        Ok(report.exit)
    }
}
impl AttemptExecutor for ExtendedWorkerExecutor {
    fn execute(
        &self,
        attempt: DispatchedAttempt,
        telemetry: musteragent_telemetry::AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> AttemptFuture<'_> {
        Box::pin(async move {
            match self.run(attempt, telemetry, supervisor).await {
                Ok(exit) => exit,
                Err(error) => {
                    tracing::error!(%error, "worker extension composition or audit failed; attempt remains recoverable");
                    AgentLoopExit::Failed {
                        stage: AgentLoopFailureStage::RecoverCheckpoint,
                        code: musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
                        retryable: true,
                        failure_commit_error: Some(
                            musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
                        ),
                        receipt: None,
                    }
                }
            }
        })
    }
}
