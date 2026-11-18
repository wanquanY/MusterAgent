#![allow(
    dead_code,
    reason = "Integration test binaries use different subsets of this shared fixture."
)]

//! Current-contract (v14, ActivityV1) runtime wiring shared by PostgreSQL loop
//! tests. Fixtures describe their controlled Tools on a plain snapshot; this
//! module admits them into a production-resolved isolated snapshot and seals
//! it with the frozen production liveness profile, so the engine, journals and
//! executor run exactly the supervised paths production uses.

use std::sync::Arc;

use musteragent_conformance::fixtures::liveness_admission as admission;
use musteragent_conformance::fixtures::source_selection_snapshot as source_selection;
use musteragent_identity::VersionedKey;
use musteragent_run::RunContext;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_snapshot::{RunExecutionPolicySnapshot, RuntimeSnapshotContent};
use musteragent_tool::ToolKey;

/// One process-wide liveness clock, as one Home owns one clock.
pub fn clock() -> Arc<musteragent_execution_liveness_io::SystemLivenessHomeClock> {
    static CLOCK: std::sync::OnceLock<
        Arc<musteragent_execution_liveness_io::SystemLivenessHomeClock>,
    > = std::sync::OnceLock::new();
    CLOCK.get_or_init(|| Arc::new(Default::default())).clone()
}

/// Admit the controlled snapshot's Tools (renamed `fixture.<wire>@1`) into a
/// production isolated snapshot for `route`, then seal it as v14 with the
/// production model liveness profile. A controlled Tool whose wire name the
/// admission already pins keeps the admitted definition; a new Tool's grant
/// comes from the admitted snapshot or, for a fixture-only capability, from the
/// controlled snapshot.
pub async fn upgrade_snapshot(
    content: Arc<dyn ContentStore>,
    run_context: RunContext,
    controlled: RuntimeSnapshotContent,
) -> RuntimeSnapshotContent {
    let mut snapshot =
        source_selection::isolated_snapshot(content, run_context, controlled.model.route.clone())
            .await;
    for mut tool in controlled.tools {
        tool.manifest.key = ToolKey::from_versioned_key(
            VersionedKey::new(format!("fixture.{}@1", tool.manifest.wire_name.as_str()))
                .expect("upgrade snapshot must succeed"),
        );
        // A released Tool the admission already pins (e.g. the bootstrap
        // interaction Tool) stays the admitted definition.
        if snapshot
            .content
            .tools
            .iter()
            .any(|existing| existing.manifest.wire_name == tool.manifest.wire_name)
        {
            continue;
        }
        let grant = match snapshot
            .content
            .capabilities
            .iter()
            .find(|grant| grant.key.as_str() == tool.manifest.capability_key.as_str())
        {
            Some(grant) => grant.clone(),
            None => {
                let grant = controlled
                    .capabilities
                    .iter()
                    .find(|grant| grant.key.as_str() == tool.manifest.capability_key.as_str())
                    .expect("fixture capability grant")
                    .clone();
                snapshot.content.capabilities.push(grant.clone());
                snapshot
                    .content
                    .capabilities
                    .sort_by(|a, b| a.key.cmp(&b.key));
                grant
            }
        };
        tool.manifest.capability_provider_id = grant.provider_id;
        tool.manifest.required_scopes = grant.allowed_scopes.clone();
        let key = tool.manifest.key.clone();
        snapshot.content.tools.push(tool);
        snapshot
            .content
            .discovery
            .as_mut()
            .expect("upgrade snapshot must succeed")
            .initial_exposed_tool_keys
            .push(key);
    }
    snapshot
        .content
        .tools
        .sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
    let policy = RunExecutionPolicySnapshot::production_defaults()
        .liveness
        .expect("upgrade snapshot must succeed")
        .model_policy();
    admission::seal(snapshot, Arc::new(policy)).content
}

/// The Run execution policy [`upgrade_snapshot`] seals, for loop test doubles
/// that pin the policy instead of reading the sealed snapshot.
pub fn execution_policy() -> RunExecutionPolicySnapshot {
    let mut policy = RunExecutionPolicySnapshot::production_defaults();
    let model = policy.liveness.expect("production liveness").model_policy();
    policy.liveness = Some(
        musteragent_runtime_snapshot::RunLivenessPolicyV1::new(model.windows(), model.windows())
            .expect("sealed Run liveness"),
    );
    policy.model_inactivity_timeout = model.idle_window();
    policy
}

/// Execution supervision over the original PostgreSQL liveness journal.
pub fn supervision(pool: &sqlx::PgPool) -> musteragent_loop::ExecutionSupervision {
    let codec = Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec);
    musteragent_loop::ExecutionSupervision::new(
        Arc::new(
            musteragent_postgres::PostgresFencedExecutionLivenessJournal::new(
                pool.clone(),
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                codec.clone(),
                codec,
                clock(),
            ),
        ),
        clock(),
        Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
    )
}

// --- Physical operation adapter for invoke-only test dispatchers ---
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopConfirmation, ExecutionStopControl,
    ExecutionStopError, StopReason,
};
use musteragent_loop::LoopPortError;
use musteragent_runtime_contract::{ApplicationFuture, execution_liveness::ExecutionBinding};
use musteragent_tool_executor::{DispatchedCapabilityOutcome, ToolPhysicalExecution};

/// Dropping the local invoke future is a confirmed physical stop; the
/// effect's outcome is still unknown to the executor.
struct ConfirmingStop(ExecutionBinding);

impl ExecutionStopControl for ConfirmingStop {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }

    fn stop(
        &self,
        _: StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        let binding = self.0;
        Box::pin(async move { Ok(ExecutionStopConfirmation { binding }) })
    }
}

struct InvokeOperation<'a> {
    binding: ExecutionBinding,
    stop: Arc<ConfirmingStop>,
    invocation: ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>,
}

impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
    for InvokeOperation<'a>
{
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }

    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.stop.clone()
    }

    fn execute(
        self: Box<Self>,
        _activity: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        self.invocation
    }
}

/// Wrap a double's lazily-built `invoke` future as the prepared physical
/// operation; nothing is polled until the supervisor executes it.
pub fn invoke_operation<'a>(
    binding: ExecutionBinding,
    invocation: ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>,
) -> ToolPhysicalExecution<'a> {
    Box::new(InvokeOperation {
        binding,
        stop: Arc::new(ConfirmingStop(binding)),
        invocation,
    })
}

struct UnconfirmableStop(ExecutionBinding);

impl ExecutionStopControl for UnconfirmableStop {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }

    fn stop(
        &self,
        _: StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async {
            Err(ExecutionStopError {
                code: musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
                rejection: None,
            })
        })
    }
}

/// As [`invoke_operation`], for a dispatched remote effect: dropping the local
/// future proves nothing about the remote side, so stop is never confirmed and
/// the supervisor retires the operation only after its bounded drain windows.
pub fn unconfirmable_invoke_operation<'a>(
    binding: ExecutionBinding,
    invocation: ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>,
) -> ToolPhysicalExecution<'a> {
    Box::new(UnconfirmableOperation {
        binding,
        invocation,
    })
}

struct UnconfirmableOperation<'a> {
    binding: ExecutionBinding,
    invocation: ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>,
}

impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
    for UnconfirmableOperation<'a>
{
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }

    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        Arc::new(UnconfirmableStop(self.binding))
    }

    fn execute(
        self: Box<Self>,
        _activity: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        self.invocation
    }
}
