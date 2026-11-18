//! Private Agent protocol calls use the original Tool journal and the same
//! execution reactor as public tools. No detached task or second timer owner.
use crate::*;
use musteragent_execution_liveness::{Closure, ControlStop, ExecutionOperation, StopReason};
use musteragent_execution_supervision::{
    ExecutionBoundary, ExecutionSupervision, ExecutionSupervisionError, ExecutionSupervisionOutcome,
};
use musteragent_identity::{IdempotencyKey, InvocationId, ToolCallId};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::execution_liveness::ExecutionScope;
use musteragent_runtime_contract::{
    ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_snapshot::PrivateToolPolicy;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey};
use std::future::Future;

#[derive(Clone)]
pub struct PrivateToolRuntime {
    pub journal: Arc<dyn FencedToolInvocationJournal>,
    pub home: ExecutionSupervision,
    pub clock: Arc<dyn RuntimeClock>,
}
pub struct PrivateToolRequest {
    pub id: InvocationId,
    pub call_id: ToolCallId,
    pub key: ToolKey,
    pub request: ContentRef,
    pub idempotency_key: IdempotencyKey,
    pub policy: PrivateToolPolicy,
}
impl PrivateToolRuntime {
    pub async fn execute<'a, C, F>(
        &'a self,
        owner: &'a AttemptContext,
        request: PrivateToolRequest,
        cancellation: C,
        body: F,
    ) -> Result<DurableToolInvocation, ToolExecutionPortError>
    where
        C: Future<Output = ControlStop> + Send,
        F: FnOnce(
                OperationContext,
            )
                -> ApplicationFuture<'a, Result<ToolTerminalOutcome, ToolExecutionPortError>>
            + Send
            + 'a,
    {
        let policy = request.policy.liveness.clone();
        let scope = ExecutionScope::Invocation(request.id);
        let window = policy.windows().startup;
        let mut durable = bounded(
            window,
            self.journal.load_or_begin(
                owner,
                ToolInvocation {
                    invocation_id: request.id,
                    run_id: owner.run_id,
                    requested_by_attempt_id: owner.attempt_id,
                    execution_attempt_id: None,
                    tool_call_id: request.call_id,
                    tool_key: request.key,
                    capability_provider_id: request.policy.provider_id,
                    status: ToolInvocationStatus::Requested,
                    effect: Effect::Read,
                    approval: ApprovalRequirement::Never,
                    idempotency_key: request.idempotency_key,
                    request_ref: request.request,
                    result_ref: None,
                    interaction_id: None,
                    timing: request.policy.timing(),
                },
                request.policy.retry,
            ),
        )
        .await?;
        if durable.recorded_outcome.is_some() {
            return Ok(durable);
        }
        if durable.invocation.status == ToolInvocationStatus::Running {
            self.home
                .close_unobserved(owner, scope, &policy)
                .await
                .map_err(home_error)?;
            return bounded(
                window,
                self.journal.complete(
                    owner,
                    request.id,
                    unknown(request.id, "recovery_unverified")?,
                    self.clock.now(),
                ),
            )
            .await;
        }
        if durable.invocation.status != ToolInvocationStatus::Requested {
            return Err(error(ErrorCode::RunTransitionInvalid));
        }
        durable = bounded(
            window,
            self.journal.start_with_reconciliation_identity(
                owner,
                request.id,
                ToolStartMode::Initial,
                (request.policy.retry == musteragent_tool::ToolRetryContract::QueryBeforeRetry)
                    .then(|| reconciliation(request.id))
                    .transpose()?,
                ToolExecutionStart {
                    started_at: self.clock.now(),
                    timing: request.policy.timing(),
                },
            ),
        )
        .await?;
        let record = self
            .home
            .load(owner, scope, &policy)
            .await
            .map_err(home_error)?;
        let operation = Box::new(CooperativeExecution::new(
            record.binding(),
            || Err(error(ErrorCode::DependencyUnavailable)),
            body,
        ));
        let (reactor, sink) = self
            .home
            .attach(owner.clone(), record, operation.stop_control())
            .map_err(|_| error(ErrorCode::RuntimeSnapshotInvalid))?;
        let (failure_tx, mut failure_rx) = tokio::sync::oneshot::channel();
        let mut failure = None;
        let boundary = async {
            let outcome = operation.execute(sink).await;
            match outcome {
                Ok(
                    outcome @ (ToolTerminalOutcome::Succeeded(_)
                    | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. }),
                ) => {
                    match self
                        .journal
                        .complete(owner, request.id, outcome, self.clock.now())
                        .await
                    {
                        Ok(done) => ExecutionBoundary::Committed(done),
                        Err(e) => {
                            ExecutionBoundary::Interrupted(ExecutionSupervisionError::Journal(
                                musteragent_execution_liveness::LivenessJournalError {
                                    code: e.code,
                                    retryable: e.retryable,
                                    rejection: None,
                                },
                            ))
                        }
                    }
                }
                other => {
                    let _ = failure_tx.send(other);
                    std::future::pending().await
                }
            }
        };
        let cancel = async {
            tokio::select! {
                biased;
                stop = cancellation => stop,
                result = &mut failure_rx => {
                    failure = result.ok();
                    ControlStop::ExecutionFailed
                }
            }
        };
        let observed = reactor
            .run(boundary, cancel)
            .await
            .map_err(|e| home_error(e.cause))?;
        if failure.is_none() {
            failure = failure_rx.try_recv().ok();
        }
        let outcome = match observed {
            ExecutionSupervisionOutcome::Boundary(done) => return Ok(done),
            ExecutionSupervisionOutcome::Stopped {
                closure: Closure::StopConfirmed(reason),
                ..
            } => match failure {
                Some(Ok(outcome)) => outcome,
                Some(Err(e)) => failed(e.code)?,
                None => failed(stop_code(reason))?,
            },
            ExecutionSupervisionOutcome::Stopped {
                closure: Closure::StopUnconfirmed(reason),
                ..
            } => {
                let evidence = match failure {
                    Some(Ok(outcome)) => Some(outcome),
                    Some(Err(error)) => Some(failed(error.code)?),
                    None => None,
                };
                let mut outcome = unknown(request.id, &format!("{reason:?}"))?;
                if let ToolTerminalOutcome::Indeterminate {
                    observed_error,
                    diagnostic_result,
                    ..
                } = &mut outcome
                {
                    match evidence {
                        Some(ToolTerminalOutcome::Failed {
                            error,
                            diagnostic_result: result,
                            ..
                        }) => {
                            *observed_error = Some(error);
                            *diagnostic_result = result;
                        }
                        Some(ToolTerminalOutcome::Indeterminate {
                            observed_error: error,
                            diagnostic_result: result,
                            ..
                        }) => {
                            *observed_error = error;
                            *diagnostic_result = result;
                        }
                        _ => {}
                    }
                }
                outcome
            }
            ExecutionSupervisionOutcome::Stopped { .. } => unknown(request.id, "result_unknown")?,
        };
        // A timeout seals this invocation only. The next model decision owns
        // retry/repair; a private child Agent is never cancelled by this join.
        bounded(
            policy.windows().post_exit_drain,
            self.journal.complete(
                owner,
                durable.invocation.invocation_id,
                outcome,
                self.clock.now(),
            ),
        )
        .await
    }
}
fn reconciliation(id: InvocationId) -> Result<OpaqueExternalRef, ToolExecutionPortError> {
    OpaqueExternalRef::new(format!("private-tool:{id}"))
        .map_err(|_| error(ErrorCode::InternalInvariantViolated))
}
fn unknown(id: InvocationId, cause: &str) -> Result<ToolTerminalOutcome, ToolExecutionPortError> {
    Ok(ToolTerminalOutcome::Indeterminate {
        observed_error: None,
        diagnostic_result: None,
        reason: SafeDisplayText::new(format!("Tool observation failed ({cause}); its physical outcome is unknown. Inspect its retained result or choose another approach."))
            .map_err(|_| error(ErrorCode::InternalInvariantViolated))?,
        reconciliation_ref: reconciliation(id)?,
    })
}
fn failed(code: ErrorCode) -> Result<ToolTerminalOutcome, ToolExecutionPortError> {
    Ok(ToolTerminalOutcome::Failed {
        error: ErrorInfo { code, category: code.category(), retryable: false,
            safe_message: SafeDisplayText::new("This tool call failed or timed out. The Agent may continue with another call or approach.")
                .map_err(|_| error(ErrorCode::InternalInvariantViolated))?,
            dependency_ref: None, diagnostic_ref: None },
        retry_disposition: RetryDisposition::DoNotRetry, diagnostic_result: None,
    })
}
fn stop_code(reason: StopReason) -> ErrorCode {
    match reason {
        StopReason::OwnershipLost => ErrorCode::AttemptLeaseStale,
        StopReason::AuthorizationLost => ErrorCode::PolicyDenied,
        StopReason::StartupExpired
        | StopReason::IdleExpired
        | StopReason::BusinessDeadlineExpired
        | StopReason::RequestTimedOut
        | StopReason::PostExitDrainExpired => ErrorCode::DependencyTimeout,
        _ => ErrorCode::DependencyUnavailable,
    }
}
fn error(code: ErrorCode) -> ToolExecutionPortError {
    ToolExecutionPortError {
        code,
        retryable: false,
    }
}
fn home_error(value: ExecutionSupervisionError) -> ToolExecutionPortError {
    match value {
        ExecutionSupervisionError::Journal(e) => ToolExecutionPortError {
            code: e.code,
            retryable: e.retryable,
        },
        ExecutionSupervisionError::JournalRequestExpired
        | ExecutionSupervisionError::BoundaryDrainExpired => error(ErrorCode::DependencyTimeout),
        _ => error(ErrorCode::InternalInvariantViolated),
    }
}
async fn bounded<T>(
    window: std::time::Duration,
    future: impl Future<Output = Result<T, ToolExecutionPortError>>,
) -> Result<T, ToolExecutionPortError> {
    tokio::time::timeout(window, future)
        .await
        .map_err(|_| error(ErrorCode::DependencyTimeout))?
}
