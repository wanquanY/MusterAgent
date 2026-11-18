//! Account capabilities share original execution, authorization and receipt recovery.
//! Each domain owner still controls its own admission and mutation semantics.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    atomic_authorization::validate_argument_invocation,
};
use musteragent_capability::CapabilityManifest;
use musteragent_identity::PrincipalKind;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_automation::{
    CloudDelegationTools, DelegationError, OwnerGoalToolError, OwnerGoalTools,
    OwnerScheduleToolError, OwnerScheduleTools,
};
use musteragent_runtime_capability::DialogueToolInvocation;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, OperationContext, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use std::sync::Arc;
pub struct AccountCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    owner: AccountToolOwner,
    clock: Arc<dyn RuntimeClock>,
    receipts: Arc<dyn musteragent_runtime_capability::OriginalToolReceiptReader>,
}
enum AccountToolOwner {
    Delegation(Arc<dyn CloudDelegationTools>),
    Goals(Arc<dyn OwnerGoalTools>),
    Schedules(Arc<dyn OwnerScheduleTools>),
}
enum AccountToolError {
    Delegation(DelegationError),
    Goals(OwnerGoalToolError),
    Schedules(OwnerScheduleToolError),
}
impl AccountCapabilityHandler {
    pub fn goals(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        owner: Arc<dyn OwnerGoalTools>,
        clock: Arc<dyn RuntimeClock>,
        receipts: Arc<dyn musteragent_runtime_capability::OriginalToolReceiptReader>,
    ) -> Self {
        Self {
            manifest,
            content,
            owner: AccountToolOwner::Goals(owner),
            clock,
            receipts,
        }
    }
    pub fn schedules(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        owner: Arc<dyn OwnerScheduleTools>,
        clock: Arc<dyn RuntimeClock>,
        receipts: Arc<dyn musteragent_runtime_capability::OriginalToolReceiptReader>,
    ) -> Self {
        Self {
            manifest,
            content,
            owner: AccountToolOwner::Schedules(owner),
            clock,
            receipts,
        }
    }
    /// Compose the real orchestration owner with original Tool recovery.
    pub fn delegation(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        owner: Arc<dyn CloudDelegationTools>,
        clock: Arc<dyn RuntimeClock>,
        receipts: Arc<dyn musteragent_runtime_capability::OriginalToolReceiptReader>,
    ) -> Self {
        Self {
            manifest,
            content,
            owner: AccountToolOwner::Delegation(owner),
            clock,
            receipts,
        }
    }
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
        activity: Option<&OperationContext>,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        if invocation.actor != material.actor
            || invocation.authorized_content != material.authorized_content
            || material.run_context.run_id != attempt.run_id
            || invocation.actor.tenant_id != invocation.tenant_id
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        // Goal creation and schedule management require a direct input root.
        if material.actor.principal_kind != PrincipalKind::User
            || (material.run_context.delegation_id.is_some()
                && !matches!(self.owner, AccountToolOwner::Delegation(_)))
            || !crate::atomic_authorization::dialogue_workspace_matches(
                material,
                invocation.workspace_id,
            )
            || ((matches!(self.owner, AccountToolOwner::Schedules(_))
                || (matches!(self.owner, AccountToolOwner::Goals(_))
                    && !matches!(invocation.tool_key.as_str(), "goal.get@3" | "goal.list@2")))
                && (material.run_context.parent_run_id.is_some()
                    || !material.policy_basis.as_ref().is_some_and(|basis| {
                        !basis.source_records.is_empty()
                            && basis
                                .source_records
                                .iter()
                                .all(|s| s.source_kind.is_user_input_authority())
                    })))
        {
            return failure(
                ErrorCode::PolicyDenied,
                false,
                "This account operation is not authorized for the current Dialogue execution",
            );
        }
        check_stopping(activity)?;
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&invocation.request_ref, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let arguments =
            serde_json::from_slice(&bytes).map_err(|_| port(ErrorCode::InvalidCommand, false))?;
        validate_argument_invocation(
            &self.manifest,
            attempt,
            material,
            durable,
            &invocation,
            &arguments,
            self.clock.now(),
        )?;
        let key = invocation.tool_key.as_str();
        let allowed = match &self.owner {
            AccountToolOwner::Delegation(_) => matches!(
                key,
                "delegation.spawn@5"
                    | "delegation.send@3"
                    | "delegation.receive@1"
                    | "delegation.list@1"
                    | "delegation.join@4"
                    | "delegation.cancel@2"
                    | "delegation.retry@2"
            ),
            AccountToolOwner::Goals(_) => matches!(
                key,
                "goal.create@3"
                    | "goal.draft.save@1"
                    | "goal.get@3"
                    | "goal.list@2"
                    | "goal.update@3"
                    | "goal.sources.catalog@1"
                    | "goal.sources.resources@1"
                    | "goal.sources.configure@1"
                    | "goal.sources.list@1"
                    | "goal.change_status@2"
            ),
            AccountToolOwner::Schedules(_) => matches!(
                key,
                "schedules.list@1"
                    | "schedules.list@2"
                    | "schedules.history@1"
                    | "schedules.create@2"
                    | "schedules.update@2"
                    | "schedules.control@2"
                    | "schedules.create@1"
                    | "schedules.update@1"
                    | "schedules.control@1"
            ),
        };
        if !allowed {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        let read = matches!(
            key,
            "delegation.list@1"
                | "delegation.receive@1"
                | "delegation.join@4"
                | "goal.get@3"
                | "goal.list@2"
                | "goal.sources.catalog@1"
                | "goal.sources.resources@1"
                | "goal.sources.list@1"
                | "schedules.list@1"
                | "schedules.list@2"
                | "schedules.history@1"
        );
        let source_tool = key.starts_with("goal.sources.");
        let command = DialogueToolInvocation {
            actor: material.actor,
            agent_id: material.agent_id,
            run_context: material.run_context,
            invocation_id: invocation.invocation_id,
            tool_call_id: invocation.tool_call_id,
            tool_key: invocation.tool_key.as_str().to_owned(),
            request_ref: invocation.request_ref,
            result_schema: invocation.result_schema,
            requested_at: self.clock.now(),
        };
        check_stopping(activity)?;
        let result = match &self.owner {
            AccountToolOwner::Delegation(owner) => owner
                .invoke(attempt, command)
                .await
                .map_err(AccountToolError::Delegation),
            AccountToolOwner::Goals(owner) => owner
                .invoke(attempt, command)
                .await
                .map_err(AccountToolError::Goals),
            AccountToolOwner::Schedules(owner) => owner
                .invoke(attempt, command)
                .await
                .map_err(AccountToolError::Schedules),
        };
        let result = result.map(|r| {
            (
                musteragent_runtime_capability::CapabilityResult {
                    result_hash: r.result_ref.sha256,
                    result_ref: r.result_ref,
                    result_schema: r.result_schema,
                    provider_request_ref: None,
                },
                r.authorized_content,
            )
        });
        match result {
            Ok((result, refs)) if read && !refs.is_empty() => {
                Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::SucceededWithAuthorizedContent {
                        result,
                        authorized_content: refs,
                    },
                ))
            }
            Ok((result, _)) if read => Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(result),
            )),
            // Mutation owners already committed their original Tool outcome;
            // a later return value cannot add authority to that receipt.
            Ok((result, refs)) if refs.is_empty() => {
                Ok(DispatchedCapabilityOutcome::Committed(result))
            }
            Ok(_) => Err(port(ErrorCode::InternalInvariantViolated, false)),
            Err(AccountToolError::Delegation(error)) => {
                let code = match error {
                    DelegationError::StaleAttempt => {
                        return Err(port(ErrorCode::AttemptLeaseStale, false));
                    }
                    DelegationError::Unavailable => ErrorCode::DependencyUnavailable,
                    DelegationError::Corrupt => ErrorCode::InternalInvariantViolated,
                    DelegationError::Invalid => ErrorCode::InvalidCommand,
                    _ => ErrorCode::PolicyDenied,
                };
                failure(
                    code,
                    error == DelegationError::Unavailable,
                    &error.to_string(),
                )
            }
            Err(AccountToolError::Goals(error)) => goal_failure(error, source_tool),
            Err(AccountToolError::Schedules(error)) => configuration_failure(error),
        }
    }
}
impl AtomicCapabilityHandler for AccountCapabilityHandler {
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
        activity: &'a OperationContext,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.invoke_inner(attempt, material, durable, invocation, Some(activity)))
    }
    fn reconciliation_identity(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Option<musteragent_runtime_contract::OpaqueExternalRef> {
        crate::atomic_tool_receipt::enabled(self.manifest.key.as_str()).then(|| {
            musteragent_runtime_capability::original_tool_receipt_identity(invocation.invocation_id)
        })
    }

    fn retained_result_query_timeout(&self) -> Option<std::time::Duration> {
        crate::atomic_tool_receipt::enabled(self.manifest.key.as_str())
            .then_some(crate::atomic_tool_receipt::QUERY_TIMEOUT)
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<crate::RetainedToolResult, LoopPortError>> {
        Box::pin(async move {
            if !crate::atomic_tool_receipt::enabled(self.manifest.key.as_str()) {
                return Ok(crate::RetainedToolResult::Incomplete);
            }
            let query = crate::atomic_tool_receipt::query(
                &self.manifest,
                attempt,
                material,
                durable,
                self.clock.now(),
                deadline,
            )?;
            let outcome = crate::atomic_tool_receipt::lookup(
                self.clock.as_ref(),
                query.deadline,
                &query.result_schema,
                self.receipts.lookup(attempt, query.clone()),
            )
            .await?;
            Ok(crate::atomic_tool_receipt::retained(outcome))
        })
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move {
            if !crate::atomic_tool_receipt::enabled(self.manifest.key.as_str()) {
                return Err(port(ErrorCode::InternalInvariantViolated, false));
            }
            crate::atomic_tool_receipt::validate_reconciliation(
                attempt, material, durable, &request,
            )?;
            let query = crate::atomic_tool_receipt::query(
                &self.manifest,
                attempt,
                material,
                durable,
                self.clock.now(),
                request.deadline,
            )?;
            let outcome = crate::atomic_tool_receipt::lookup(
                self.clock.as_ref(),
                query.deadline,
                &query.result_schema,
                self.receipts.lookup(attempt, query.clone()),
            )
            .await?;
            Ok(crate::atomic_tool_receipt::reconciled(
                outcome,
                request.reconciliation_ref,
            ))
        })
    }
}
fn check_stopping(activity: Option<&OperationContext>) -> Result<(), LoopPortError> {
    if activity.is_some_and(OperationContext::stopping) {
        Err(port(ErrorCode::RunTransitionInvalid, false))
    } else {
        Ok(())
    }
}
const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}
fn configuration_failure(
    error: OwnerScheduleToolError,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    let (code, retryable, message) = match error {
        OwnerScheduleToolError::StaleFence => {
            return Err(port(ErrorCode::AttemptLeaseStale, false));
        }
        OwnerScheduleToolError::Forbidden => (
            ErrorCode::PolicyDenied,
            false,
            "Manage schedules only for the current Agent and a direct user request",
        ),
        OwnerScheduleToolError::ApplicationCatalogUnsupported => (
            ErrorCode::PolicyDenied,
            false,
            "schedule.application_catalog_source_unsupported",
        ),
        OwnerScheduleToolError::InvalidArguments => (
            ErrorCode::InvalidCommand,
            false,
            "Provide valid timing, timezone and instructions; inspect schedules.list before changing an existing task",
        ),
        OwnerScheduleToolError::Conflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "Schedule changed. Read schedules.list and preserve the latest user changes before retrying",
        ),
        OwnerScheduleToolError::Infrastructure => (
            ErrorCode::DependencyUnavailable,
            true,
            "Scheduled tasks are temporarily unavailable",
        ),
        OwnerScheduleToolError::Corrupt => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Schedule execution custody could not be verified",
        ),
    };
    failure(code, retryable, message)
}
fn goal_failure(
    error: OwnerGoalToolError,
    source_tool: bool,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    use musteragent_runtime_automation::owner_event_sources::OwnerGoalSourceRefusal as Source;
    let (code, retryable, message) = match error {
        OwnerGoalToolError::StaleFence => {
            return Err(port(ErrorCode::AttemptLeaseStale, false));
        }
        OwnerGoalToolError::Forbidden => (
            ErrorCode::PolicyDenied,
            false,
            if source_tool {
                Source::AccessDenied.safe_message()
            } else {
                "Manage standing tasks only for the current Agent and a direct user request"
            },
        ),
        OwnerGoalToolError::InvalidArguments => (
            ErrorCode::InvalidCommand,
            false,
            if source_tool {
                Source::InvalidArguments.safe_message()
            } else {
                "Provide a task title, instructions, a stable action key and valid completion rules"
            },
        ),
        OwnerGoalToolError::Conflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "The original task command has different accepted arguments. Preserve the action key and exact original definition when recovering its receipt",
        ),
        OwnerGoalToolError::SourceRevisionConflict => (
            ErrorCode::ResourceRevisionConflict,
            false,
            Source::RevisionConflict.safe_message(),
        ),
        OwnerGoalToolError::Infrastructure => (
            ErrorCode::DependencyUnavailable,
            true,
            "Standing tasks are temporarily unavailable",
        ),
        OwnerGoalToolError::Corrupt => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Standing task execution custody could not be verified",
        ),
    };
    failure(code, retryable, message)
}
fn failure(
    code: ErrorCode,
    retryable: bool,
    message: &str,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
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
