use std::sync::Arc;

use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, InteractionResolution,
    RuntimeClock, ToolExecutionMaterial, authorize_invocation,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_memory::{
    BuiltinMemoryTools, ClassifyMemoryToolCommitError, CommitPreparedMemoryTool, EvolutionStore,
    FencedMemoryToolCommitter, MemorySearchIndex, MemoryStore, MemoryToolCommitFailure,
    MemoryToolContext, MemoryToolError, MemoryToolPreparation, UserAuthoredRunOrigin,
};

use crate::{
    ApprovalPromptOverride, AtomicCapabilityHandler, DispatchedCapabilityOutcome,
    DispatchedReconciliationOutcome,
};

pub struct MemoryCapabilityHandler<M, E, C>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
    C: FencedMemoryToolCommitter,
{
    manifest: CapabilityManifest,
    tools: BuiltinMemoryTools<M, E>,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
    ranker: Option<Arc<dyn musteragent_runtime_memory::MemoryRankerFactory>>,
    read_planner: Option<Arc<dyn musteragent_runtime_memory::MemoryReadPlannerFactory>>,
    write_planner: Option<Arc<dyn musteragent_runtime_memory::MemoryWritePlannerFactory>>,
}

impl<M, E, C> MemoryCapabilityHandler<M, E, C>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
    C: FencedMemoryToolCommitter,
{
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        tools: BuiltinMemoryTools<M, E>,
        committer: Arc<C>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            tools,
            committer,
            clock,
            write_planner: None,
            ranker: None,
            read_planner: None,
        }
    }

    #[must_use]
    pub fn with_ranker(
        mut self,
        factory: Arc<dyn musteragent_runtime_memory::MemoryRankerFactory>,
    ) -> Self {
        self.ranker = Some(factory);
        self
    }

    #[must_use]
    pub fn with_read_planner(
        mut self,
        factory: Arc<dyn musteragent_runtime_memory::MemoryReadPlannerFactory>,
    ) -> Self {
        self.read_planner = Some(factory);
        self
    }

    #[must_use]
    pub fn with_write_planner(
        mut self,
        factory: Arc<dyn musteragent_runtime_memory::MemoryWritePlannerFactory>,
    ) -> Self {
        self.write_planner = Some(factory);
        self
    }
}

impl<M, E, C> AtomicCapabilityHandler for MemoryCapabilityHandler<M, E, C>
where
    M: MemoryStore + MemorySearchIndex + 'static,
    E: EvolutionStore + 'static,
    C: FencedMemoryToolCommitter + 'static,
{
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        crate::local_atomic_execution(self, attempt, material, durable, invocation, binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }

    fn approval_prompt<'a>(
        &'a self,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        Box::pin(async move {
            if material.actor != invocation.actor
                || material.run_context.tenant_id != invocation.tenant_id
                || material.run_context.run_id != invocation.run_id
                || material.tool.manifest.key != invocation.tool_key
            {
                return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
            }
            self.tools
                .approval_prompt(invocation, material.agent_id)
                .await
                .map(|prompt| {
                    prompt.map(|(media_type, bytes)| ApprovalPromptOverride { media_type, bytes })
                })
                .map_err(|error| {
                    let (code, retryable, _) = memory_tool_failure(&error);
                    port(code, retryable)
                })
        })
    }

    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.invoke_inner(attempt, material, durable, invocation, None))
    }

    fn invoke_supervised<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        context: &'a musteragent_runtime_capability::OperationContext,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.invoke_inner(attempt, material, durable, invocation, Some(context)))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

impl<M, E, C> MemoryCapabilityHandler<M, E, C>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
    C: FencedMemoryToolCommitter,
{
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
        let user_authored_origin = if invocation.tool_key.logical_key() == "user_memory.remember" {
            user_authored_origin(material)?
        } else {
            None
        };
        let approval_interaction_present = durable.invocation.interaction_id.is_some();
        let user_authored_origin_present = user_authored_origin.is_some();
        let tools = match material
            .tool
            .manifest
            .application
            .as_ref()
            .and_then(|binding| binding.implementation.as_ref())
        {
            Some(
                musteragent_tool::ApplicationImplementation::MemoryPlansV1 { .. }
                | musteragent_tool::ApplicationImplementation::MemoryPlansV2 { .. },
            ) => {
                let planner = self
                    .write_planner
                    .as_ref()
                    .ok_or_else(|| port(ErrorCode::CapabilityUnavailable, false))?
                    .bind(material, &invocation)
                    .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
                self.tools.clone().with_write_planner(planner)
            }
            Some(musteragent_tool::ApplicationImplementation::MemoryRankingV1 { .. }) => {
                let ranker = self
                    .ranker
                    .as_ref()
                    .ok_or_else(|| port(ErrorCode::CapabilityUnavailable, false))?
                    .bind(material, &invocation)
                    .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
                self.tools.clone().with_ranker(ranker)
            }
            Some(musteragent_tool::ApplicationImplementation::MemoryReadV1 { .. }) => {
                let planner = self
                    .read_planner
                    .as_ref()
                    .ok_or_else(|| port(ErrorCode::CapabilityUnavailable, false))?
                    .bind(material, &invocation)
                    .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
                let mut tools = self.tools.clone().with_read_planner(planner);
                if matches!(
                    invocation.tool_key.logical_key(),
                    "memory.search" | "memory.list" | "memory.get"
                ) {
                    let ranker = self
                        .ranker
                        .as_ref()
                        .ok_or_else(|| port(ErrorCode::CapabilityUnavailable, false))?
                        .bind(material, &invocation)
                        .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
                    tools = tools.with_ranker(ranker);
                }
                tools
            }
            None => self.tools.clone(),
        };
        let prepared = tools
            .prepare_with_execution(
                MemoryToolContext {
                    invocation,
                    run_context: material.run_context,
                    actor: material.actor,
                    agent_id: material.agent_id,
                    approval_interaction_id: durable.invocation.interaction_id,
                    user_authored_origin,
                    evidence_record_id: durable.request_record_id,
                    now: self.clock.now(),
                },
                execution,
            )
            .await;
        match prepared {
            Ok(MemoryToolPreparation::ReadOnly(result)) => Ok(
                DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(result)),
            ),
            Ok(MemoryToolPreparation::Commit { result, mutation }) => {
                // Preparation is cancellable; the fenced write transaction
                // itself must be awaited to retain its authoritative outcome.
                if execution.is_some_and(|context| context.stopping()) {
                    return Err(port(ErrorCode::RunTransitionInvalid, false));
                }
                let committed = self
                    .committer
                    .commit(
                        attempt,
                        CommitPreparedMemoryTool {
                            run_context: material.run_context,
                            invocation_id: durable.invocation.invocation_id,
                            result: result.clone(),
                            mutation: *mutation,
                            committed_at: self.clock.now(),
                        },
                    )
                    .await;
                match committed {
                    Ok(_) => Ok(DispatchedCapabilityOutcome::Committed(result)),
                    Err(error) => commit_failure_outcome(error.failure()),
                }
            }
            Err(error) => {
                if matches!(&error, MemoryToolError::IdentityMismatch)
                    && durable.invocation.tool_key.logical_key() == "user_memory.remember"
                {
                    tracing::warn!(
                        run_id = %material.run_context.run_id,
                        invocation_id = %durable.invocation.invocation_id,
                        approval_interaction_present,
                        user_authored_origin_present,
                        "User Memory remember authorization material is incomplete"
                    );
                }
                let (code, retryable, message) = memory_tool_failure(&error);
                Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Failed {
                        error: ErrorInfo {
                            code,
                            category: code.category(),
                            safe_message: SafeDisplayText::new(message)
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                            retryable,
                            dependency_ref: None,
                            diagnostic_ref: None,
                        },
                        retry_disposition: if retryable {
                            RetryDisposition::RetryNewAttempt
                        } else {
                            RetryDisposition::DoNotRetry
                        },
                        diagnostic_result: None,
                    },
                ))
            }
        }
    }
}

fn user_authored_origin(
    material: &ToolExecutionMaterial,
) -> Result<Option<UserAuthoredRunOrigin>, LoopPortError> {
    let Some(basis) = material.policy_basis.as_ref() else {
        return Ok(None);
    };
    let mut records = basis
        .source_records
        .iter()
        .filter(|record| record.source_kind.is_user_input_authority());
    let Some(record) = records.next() else {
        return Ok(None);
    };
    if records.next().is_some() {
        return Err(port(ErrorCode::InternalInvariantViolated, false));
    }
    Ok(Some(UserAuthoredRunOrigin {
        record_id: record.record_id,
        content_ref: record.content_ref.clone(),
    }))
}

fn commit_failure_outcome(
    failure: MemoryToolCommitFailure,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    let (code, retryable, message) = match failure {
        MemoryToolCommitFailure::StaleFence => {
            return Err(port(ErrorCode::AttemptLeaseStale, false));
        }
        MemoryToolCommitFailure::Unavailable => {
            return Err(port(ErrorCode::DependencyUnavailable, true));
        }
        MemoryToolCommitFailure::InvalidCommand => (
            ErrorCode::InvalidCommand,
            false,
            "Memory commit command is invalid",
        ),
        MemoryToolCommitFailure::Authorization => (
            ErrorCode::PolicyDenied,
            false,
            "Memory approval or originating user request is not authorized",
        ),
        MemoryToolCommitFailure::ContentIntegrity => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Memory commit content integrity verification failed",
        ),
        MemoryToolCommitFailure::Conflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "Memory commit conflicts with durable state",
        ),
        MemoryToolCommitFailure::Corrupt => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Memory persistence state is invalid",
        ),
    };
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new(message)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                retryable,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        },
    ))
}

const fn memory_tool_failure(error: &MemoryToolError) -> (ErrorCode, bool, &'static str) {
    match error {
        MemoryToolError::Execution(code) => (*code, false, "Memory execution was interrupted"),
        MemoryToolError::InvalidInput | MemoryToolError::UnsupportedTool => (
            ErrorCode::InvalidCommand,
            false,
            "Memory Tool input is invalid",
        ),
        MemoryToolError::IdentityMismatch => (
            ErrorCode::PolicyDenied,
            false,
            "Memory Tool identity is not authorized",
        ),
        MemoryToolError::PublicationNotReady => (
            ErrorCode::PolicyDenied,
            false,
            "Experience proposal is not currently eligible for this request; no new publication was performed",
        ),
        MemoryToolError::InactiveMemory => (
            ErrorCode::PolicyDenied,
            false,
            "Inactive Memory requires an explicit lifecycle restore",
        ),
        MemoryToolError::ProgramUnavailable => (
            ErrorCode::DependencyUnavailable,
            true,
            "Memory business program is temporarily unavailable",
        ),
        MemoryToolError::Content | MemoryToolError::Store => (
            ErrorCode::DependencyUnavailable,
            true,
            "Memory persistence is temporarily unavailable",
        ),
        MemoryToolError::InvalidResult
        | MemoryToolError::VersionOverflow
        | MemoryToolError::Application(_) => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Memory Tool produced invalid durable state",
        ),
    }
}

fn validate_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
) -> Result<(), LoopPortError> {
    durable
        .validate()
        .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
    let tool = &material.tool.manifest;
    let grant = &material.capability_grant;
    let persisted = &durable.invocation;
    let workspace_id = workspace_id(&material.run_context.execution_scope);
    let mut authorization = material.authorization.clone();
    authorization.approval_satisfied = matches!(
        durable.interaction_resolution,
        Some(InteractionResolution::Approval(
            musteragent_approval::ApprovalDecision::Approve
                | musteragent_approval::ApprovalDecision::ApproveSession
        ))
    );
    if manifest.location != grant.location
        || authorize_invocation(tool, grant, &material.tool.execution_target, &authorization)
            != musteragent_policy::Decision::Allow
        || manifest.provider_id != grant.provider_id
        || manifest.key != grant.key
        || manifest.effect != grant.effect
        || manifest.provider_id != tool.capability_provider_id
        || manifest.key.as_str() != tool.capability_key.as_str()
        || manifest.effect != tool.effect
        || manifest.approval != tool.approval
        || manifest.required_scopes != tool.required_scopes
        || manifest.required_contexts != tool.required_contexts
        || !manifest.required_scopes.is_subset(&grant.allowed_scopes)
        || !tool
            .required_scopes
            .is_subset(&material.authorization.actor_scopes)
        || !tool
            .required_contexts
            .is_subset(&material.authorization.available_contexts)
        || material.tool.execution_target
            != musteragent_tool::CapabilityExecutionTarget::RustRuntime
        || material.run_context.run_id != attempt.run_id
        || material.actor != invocation.actor
        || invocation.actor.tenant_id != invocation.tenant_id
        || invocation.tenant_id != material.run_context.tenant_id
        || invocation.conversation_id != material.run_context.conversation_id
        || invocation.run_id != material.run_context.run_id
        || invocation.requested_by_attempt_id != persisted.requested_by_attempt_id
        || invocation.execution_attempt_id != attempt.attempt_id
        || invocation.execution_attempt_number != attempt.attempt_number
        || invocation.invocation_id != persisted.invocation_id
        || invocation.tool_call_id != persisted.tool_call_id
        || invocation.tool_key != tool.key
        || invocation.tool_key != persisted.tool_key
        || invocation.capability_key != manifest.key
        || invocation.capability_provider_id != manifest.provider_id
        || invocation.capability_provider_id != persisted.capability_provider_id
        || invocation.idempotency_key != persisted.idempotency_key
        || invocation.request_ref != persisted.request_ref
        || invocation.request_schema != tool.input_schema
        || invocation.result_schema != tool.output_schema
        || invocation.authorized_content != material.authorized_content
        || invocation.workspace_id != workspace_id
        || invocation.timing != persisted.timing
        || persisted.run_id != attempt.run_id
        || persisted.execution_attempt_id != Some(attempt.attempt_id)
        || persisted.status != musteragent_tool::ToolInvocationStatus::Running
        || persisted.effect != tool.effect
        || persisted.approval != tool.approval
        || durable.retry_contract != tool.retry_contract
        || durable.recorded_outcome.is_some()
    {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(())
}

fn workspace_id(
    scope: &musteragent_event_envelope::ExecutionScope,
) -> Option<musteragent_identity::WorkspaceId> {
    match scope {
        musteragent_event_envelope::ExecutionScope::HostedProject { scope } => {
            Some(scope.workspace_id)
        }
        musteragent_event_envelope::ExecutionScope::Workspace { workspace_id }
        | musteragent_event_envelope::ExecutionScope::DevicePrivate {
            workspace_id: Some(workspace_id),
            ..
        } => Some(*workspace_id),
        musteragent_event_envelope::ExecutionScope::Tenant { .. }
        | musteragent_event_envelope::ExecutionScope::DevicePrivate {
            workspace_id: None, ..
        } => None,
    }
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

#[cfg(test)]
mod tests {
    use musteragent_runtime_contract::ErrorCode;
    use musteragent_runtime_memory::{MemoryApplicationError, MemoryToolError};

    use super::memory_tool_failure;

    #[test]
    fn only_memory_dependency_failures_are_retryable() {
        for error in [
            MemoryToolError::Content,
            MemoryToolError::Store,
            MemoryToolError::ProgramUnavailable,
        ] {
            let (code, retryable, _) = memory_tool_failure(&error);
            assert_eq!(code, ErrorCode::DependencyUnavailable);
            assert!(retryable);
        }

        for error in [
            MemoryToolError::InvalidResult,
            MemoryToolError::VersionOverflow,
            MemoryToolError::Application(MemoryApplicationError::InvalidSearchDocument),
        ] {
            let (code, retryable, _) = memory_tool_failure(&error);
            assert_eq!(code, ErrorCode::InternalInvariantViolated);
            assert!(!retryable);
        }
    }
}
