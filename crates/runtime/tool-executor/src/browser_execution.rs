//! Own a durable Browser Job until its original receipt has been materialized.
//! Enqueue is admission, not a terminal outcome. Polls never redispatch work,
//! and only a new provider sequence counts as execution progress.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    ToolPhysicalExecution,
};
use musteragent_capability::{BrowserJobRequest, BrowserProviderContract};
use musteragent_identity::UtcTimestamp;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    BrowserGatewayStore, BrowserJobDeliveryState, BrowserJobTerminalReceipt,
    BrowserProviderRuntimeError, CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation,
    CooperativeExecution, DurableBrowserJob, DurableToolInvocation, OperationContext,
    ReconciliationOutcome, RuntimeClock, ToolExecutionMaterial,
    validate_capability_execution_binding,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
    execution_liveness::{ExecutionBinding, ExecutionLivenessPolicy},
};
use std::{sync::Arc, time::Duration};

const OBSERVATION_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) struct PreparedBrowserJob {
    pub contract: BrowserProviderContract,
    pub request: BrowserJobRequest,
}
pub(crate) type BrowserPreparation = Result<PreparedBrowserJob, CapabilityOutcome>;
pub(crate) type GatewayFailure = fn(
    BrowserProviderRuntimeError,
    &BrowserUnavailableNotice,
) -> Result<CapabilityOutcome, LoopPortError>;

/// User-facing text for [`ErrorCode::BrowserExecutionUnavailable`]: no browser
/// execution provider (an open Web page running the browser sandbox) can take
/// the routed work. The machine code is stable; hosts localize by code or by
/// configuring this text. The failure is retryable, so the same call can run
/// once the user reopens the page.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserUnavailableNotice(SafeDisplayText);

impl BrowserUnavailableNotice {
    /// English default used when the host configures no text.
    pub const DEFAULT_TEXT: &'static str = "No browser execution environment is available. \
         Keep the Web page that runs the browser sandbox open, then retry.";

    /// Host-configured text; it must be safe display text.
    pub fn new(
        text: impl Into<String>,
    ) -> Result<Self, musteragent_runtime_contract::TextContractError> {
        SafeDisplayText::new(text).map(Self)
    }

    #[must_use]
    pub fn text(&self) -> &SafeDisplayText {
        &self.0
    }

    /// The retryable tool failure carrying this notice.
    #[must_use]
    pub fn failure(&self) -> CapabilityOutcome {
        let code = ErrorCode::BrowserExecutionUnavailable;
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: self.0.clone(),
                retryable: true,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::RetryNewAttempt,
            diagnostic_result: None,
        }
    }
}

impl Default for BrowserUnavailableNotice {
    fn default() -> Self {
        Self(SafeDisplayText::new(Self::DEFAULT_TEXT).expect("valid default notice"))
    }
}

/// Whether the gateway reported that no browser execution provider can take
/// the work (as opposed to a server sandbox or an integrity failure).
pub(crate) fn browser_unavailable(error: &BrowserProviderRuntimeError) -> bool {
    matches!(
        error,
        BrowserProviderRuntimeError::ProviderUnavailable
            | BrowserProviderRuntimeError::SandboxRoute(
                musteragent_sandbox::SandboxRouteError::BrowserUnavailable
            )
    )
}

pub(crate) struct BrowserExecution<'a> {
    pub handler: &'a dyn AtomicCapabilityHandler,
    pub store: &'a dyn BrowserGatewayStore,
    pub clock: &'a dyn RuntimeClock,
    pub attempt: &'a AttemptContext,
    pub material: &'a ToolExecutionMaterial,
    pub durable: &'a DurableToolInvocation,
    pub invocation: CapabilityInvocation,
    pub prepare: ApplicationFuture<'a, Result<BrowserPreparation, LoopPortError>>,
    pub gateway_failure: GatewayFailure,
    pub unavailable_notice: &'a BrowserUnavailableNotice,
}

impl<'a> BrowserExecution<'a> {
    pub fn operation(
        self,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        if !self
            .handler
            .accepts_execution_target(&self.material.tool.execution_target)
        {
            return Err(invalid());
        }
        crate::atomic_authorization::validate_sealed_invocation(
            self.handler.manifest(),
            self.attempt,
            self.material,
            self.durable,
            &self.invocation,
        )?;
        validate_capability_execution_binding(&self.invocation, self.attempt, binding)
            .map_err(|_| invalid())?;
        let windows = self
            .material
            .tool
            .manifest
            .execution_policy
            .liveness()
            .ok_or_else(invalid)?
            .windows();
        let reference = self
            .handler
            .reconciliation_identity(&self.invocation)
            .ok_or_else(invalid)?;
        Ok(Box::new(CooperativeExecution::new(
            binding,
            || Err(invalid()),
            move |context| {
                Box::pin(self.run(
                    context,
                    reference,
                    windows.startup,
                    windows.observation_request,
                ))
            },
        )))
    }

    async fn run(
        self,
        context: OperationContext,
        reference: OpaqueExternalRef,
        startup_window: Duration,
        window: Duration,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        // Preparation owns only awaited local reads. No Job can exist yet.
        let prepared = tokio::select! {
            biased;
            _ = context.cancelled() => { context.quiescent(); return Ok(unknown(reference, "Browser execution cancelled before dispatch")); }
            value = self.prepare => value,
        };
        let mut job = match prepared {
            Ok(Ok(job)) => job,
            Ok(Err(failure)) => {
                context.quiescent();
                return Ok(DispatchedCapabilityOutcome::Provider(failure));
            }
            Err(error) => {
                context.quiescent();
                return Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Failed {
                        error: musteragent_runtime_contract::ErrorInfo {
                            code: error.code,
                            category: error.code.category(),
                            safe_message: SafeDisplayText::new(
                                "Browser Job preparation failed before execution was submitted",
                            )
                            .expect("static reason"),
                            retryable: error.retryable,
                            dependency_ref: None,
                            diagnostic_ref: None,
                        },
                        retry_disposition:
                            musteragent_runtime_contract::RetryDisposition::DoNotRetry,
                        diagnostic_result: None,
                    },
                ));
            }
        };
        if context.stopping() {
            context.quiescent();
            return Ok(unknown(
                reference,
                "Browser execution cancelled before dispatch",
            ));
        }
        let invocation = &self.invocation;
        if job.request.invocation_id != invocation.invocation_id
            || job.request.conversation_id != invocation.conversation_id
            || job.request.validate().is_err()
        {
            context.quiescent();
            return Err(invalid());
        }
        // A recovered Job keeps its exact original policy, including None for
        // historical fixed budgets. Only a new admission projects this sealed
        // Tool policy; the browser must not introduce another total lifetime.
        let original = tokio::time::timeout(
            window,
            self.store
                .load_job(invocation.tenant_id, job.request.job_id),
        )
        .await
        .map_err(|_| LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        })?
        .map_err(|_| LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        })?;
        job.request.execution_liveness = original_browser_policy(
            original
                .as_ref()
                .map(|value| &value.delivery.request.execution_liveness),
            self.material
                .tool
                .manifest
                .execution_policy
                .liveness()
                .ok_or_else(invalid)?,
        );
        // A timed-out database commit may have admitted the Job. Keep observing
        // that exact identity; never submit a replacement Job or claim no effect.
        let submitted = tokio::time::timeout(
            startup_window,
            crate::sandbox_routing::SandboxDispatch {
                clock: self.clock,
                attempt: self.attempt,
                material: self.material,
                durable: self.durable,
                invocation,
                contract: job.contract,
                request: job.request.clone(),
            }
            .enqueue(self.store),
        )
        .await;
        let initial = match submitted {
            Ok(Ok(value)) => Some(value),
            Ok(Err(
                BrowserProviderRuntimeError::DuplicateJob
                | BrowserProviderRuntimeError::DurabilityUnavailable,
            ))
            | Err(_) => {
                tracing::warn!(event = "tool.browser_job.admission_unconfirmed", invocation_id = %invocation.invocation_id,
                    job_id = %job.request.job_id, "Observing original Browser Job after inconclusive admission; no redispatch");
                None
            }
            Ok(Err(error)) => {
                context.quiescent();
                return (self.gateway_failure)(error, self.unavailable_notice)
                    .map(DispatchedCapabilityOutcome::Provider);
            }
        };
        let receipt = BrowserJobObserver {
            store: self.store,
            actor: invocation.actor,
            job: &job,
            window,
        }
        .observe(&context, initial)
        .await?;
        let Some(receipt) = receipt else {
            context.quiescent();
            return Ok(unknown(
                reference,
                "The original browser provider is no longer connected; its execution outcome is unconfirmed. Do not replay the script automatically.",
            ));
        };
        tracing::info!(event = "tool.browser_job.terminal_received", invocation_id = %invocation.invocation_id,
            job_id = %receipt.job_id, status = ?receipt.result.status, provider_sequence = receipt.provider_sequence,
            "Original Browser Job terminal receipt verified");
        if receipt.result.status == musteragent_capability::BrowserJobStatus::Indeterminate {
            context.quiescent();
            return Ok(unknown(
                reference,
                "Browser execution generation ended without a confirmed result; inspect original files before issuing another execution",
            ));
        }
        let observer = BrowserCompletion {
            handler: self.handler,
            clock: self.clock,
            attempt: self.attempt,
            material: self.material,
            durable: self.durable,
            invocation,
            reference,
            window,
        };
        loop {
            if context.stopping() {
                context.quiescent();
                return Ok(
                    observer.unknown("Browser Job stopped; its original receipt is retained")
                );
            }
            if let Some(outcome) = observer.materialize(&context).await? {
                return Ok(outcome);
            }
            tokio::select! { biased; _ = context.cancelled() => {}, _ = tokio::time::sleep(OBSERVATION_INTERVAL) => {} }
        }
    }
}

struct BrowserCompletion<'a> {
    handler: &'a dyn AtomicCapabilityHandler,
    clock: &'a dyn RuntimeClock,
    attempt: &'a AttemptContext,
    material: &'a ToolExecutionMaterial,
    durable: &'a DurableToolInvocation,
    invocation: &'a CapabilityInvocation,
    reference: OpaqueExternalRef,
    window: Duration,
}

struct BrowserJobObserver<'a> {
    store: &'a dyn BrowserGatewayStore,
    actor: musteragent_identity::Actor,
    job: &'a PreparedBrowserJob,
    window: Duration,
}
impl BrowserJobObserver<'_> {
    async fn observe(
        &self,
        context: &OperationContext,
        mut observed: Option<DurableBrowserJob>,
    ) -> Result<Option<BrowserJobTerminalReceipt>, LoopPortError> {
        let mut proof = None;
        let mut sequence = None;
        let mut cancel_sent = false;
        let mut terminal_seen = false;
        let mut observation_unavailable = false;
        loop {
            if let Some(job) = observed.take() {
                let delivery = &job.delivery;
                if delivery.request != self.job.request
                    || delivery.capability.as_str() != self.job.contract.capability_key().as_str()
                    || delivery.lease.tenant_id != self.actor.tenant_id
                    || delivery.lease.principal_id != self.actor.principal_id
                    || (proof.is_some_and(|value| value != delivery.lease)
                        && sequence.is_some_and(|value| value > 0))
                    || sequence.is_some_and(|value| value > delivery.last_provider_sequence)
                {
                    return Err(invalid());
                }
                proof = Some(delivery.lease);
                if sequence != Some(delivery.last_provider_sequence) {
                    let _ = context.completed_stage();
                    sequence = Some(delivery.last_provider_sequence);
                    tracing::info!(event = "tool.browser_job.advanced", invocation_id = %self.job.request.invocation_id,
                        job_id = %self.job.request.job_id, state = ?delivery.state,
                        provider_sequence = delivery.last_provider_sequence, "Original Browser Job advanced");
                }
                if delivery.state == BrowserJobDeliveryState::Terminal {
                    terminal_seen = true;
                } else if matches!(
                    tokio::time::timeout(self.window, self.store.lease_is_current(delivery.lease))
                        .await,
                    Ok(Ok(false))
                ) {
                    return Ok(None);
                }
            }
            if terminal_seen {
                // Load the authority's terminal receipt, not merely a delivery
                // status. This also validates the result against the closed Job.
                if let Ok(Ok(Some(receipt))) = tokio::time::timeout(
                    self.window,
                    self.store
                        .load_terminal_receipt(self.actor.tenant_id, self.job.request.job_id),
                )
                .await
                {
                    if receipt.invocation_id != self.job.request.invocation_id
                        || receipt.job_id != self.job.request.job_id
                        || Some(receipt.lease) != proof
                        || Some(receipt.provider_sequence) != sequence
                        || self.job.request.validate_result(&receipt.result).is_err()
                    {
                        return Err(invalid());
                    }
                    return Ok(Some(receipt));
                }
            }
            if context.stopping() && !cancel_sent {
                // An accepted cancel is not proof of termination. Keep owning
                // execution until the fenced browser acknowledges with a receipt.
                cancel_sent = matches!(
                    tokio::time::timeout(
                        self.window,
                        self.store.request_cancel(self.job.request.job_id)
                    )
                    .await,
                    Ok(Ok(()))
                );
                tracing::info!(event = "tool.browser_job.cancel_requested", invocation_id = %self.job.request.invocation_id,
                    acknowledged = cancel_sent, "Requested cancellation of original Browser Job");
            }
            if context.stopping() {
                tokio::time::sleep(OBSERVATION_INTERVAL).await;
            } else {
                tokio::select! { biased; _ = context.cancelled() => {}, _ = tokio::time::sleep(OBSERVATION_INTERVAL) => {} }
            }
            observed = match tokio::time::timeout(
                self.window,
                self.store
                    .load_job(self.actor.tenant_id, self.job.request.job_id),
            )
            .await
            {
                Ok(Ok(value)) => {
                    observation_unavailable = false;
                    value
                }
                _ => {
                    if !observation_unavailable {
                        tracing::warn!(event = "tool.browser_job.observation_unavailable", invocation_id = %self.job.request.invocation_id,
                            job_id = %self.job.request.job_id, "Browser Job query unavailable; retaining original execution without refreshing progress");
                    }
                    observation_unavailable = true;
                    None
                }
            };
        }
    }
}
impl BrowserCompletion<'_> {
    async fn materialize(
        &self,
        context: &OperationContext,
    ) -> Result<Option<DispatchedCapabilityOutcome>, LoopPortError> {
        let deadline = self
            .clock
            .now()
            .unix_millis()
            .checked_add(i64::try_from(self.window.as_millis()).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
        let request = CapabilityReconciliation {
            actor: self.invocation.actor,
            tenant_id: self.invocation.tenant_id,
            conversation_id: self.invocation.conversation_id,
            run_id: self.invocation.run_id,
            workspace_id: self.invocation.workspace_id,
            execution_attempt_id: self.invocation.execution_attempt_id,
            execution_attempt_number: self.invocation.execution_attempt_number,
            invocation_id: self.invocation.invocation_id,
            provider_id: self.invocation.capability_provider_id,
            idempotency_key: self.invocation.idempotency_key.clone(),
            result_schema: self.invocation.result_schema.clone(),
            reconciliation_ref: self.reference.clone(),
            deadline: UtcTimestamp::from_unix_millis(deadline),
            trace_context: self.invocation.trace_context.clone(),
            execution_grant_proof: self.invocation.execution_grant_proof.clone(),
        };
        // Await the local fenced transaction even if cancellation arrives. Its
        // original commit is the terminal boundary; dropping it is not stop proof.
        let outcome = match self
            .handler
            .reconcile(self.attempt, self.material, self.durable, request)
            .await
        {
            Ok(value) => value,
            Err(error) if error.retryable => return Ok(None),
            Err(error) => {
                context.quiescent();
                return Err(error);
            }
        };
        let outcome = match outcome {
            DispatchedReconciliationOutcome::Committed(result) => {
                DispatchedCapabilityOutcome::Committed(result)
            }
            DispatchedReconciliationOutcome::Provider(outcome) => {
                DispatchedCapabilityOutcome::Provider(match outcome {
                    ReconciliationOutcome::ProvenSucceeded(result) => {
                        CapabilityOutcome::Succeeded(result)
                    }
                    ReconciliationOutcome::ProvenSucceededWithAuthorizedContent {
                        result,
                        authorized_content,
                    } => CapabilityOutcome::SucceededWithAuthorizedContent {
                        result,
                        authorized_content,
                    },
                    ReconciliationOutcome::ProvenFailed {
                        error,
                        retry_disposition,
                        diagnostic_result,
                    } => CapabilityOutcome::Failed {
                        error,
                        retry_disposition,
                        diagnostic_result,
                    },
                    ReconciliationOutcome::StillIndeterminate {
                        reconciliation_ref, ..
                    } if reconciliation_ref == self.reference => return Ok(None),
                    _ => {
                        context.quiescent();
                        return Ok(Some(self.unknown(
                            "Browser receipt could not be materialized under its original contract",
                        )));
                    }
                })
            }
        };
        context.quiescent();
        tracing::info!(event = "tool.browser_job.materialized", invocation_id = %self.invocation.invocation_id, "Browser receipt materialized for original Tool invocation");
        Ok(Some(outcome))
    }
    fn unknown(&self, reason: &'static str) -> DispatchedCapabilityOutcome {
        unknown(self.reference.clone(), reason)
    }
}

fn unknown(reference: OpaqueExternalRef, reason: &'static str) -> DispatchedCapabilityOutcome {
    DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
        diagnostic_result: None,
        observed_error: None,
        reason: SafeDisplayText::new(reason).expect("static reason"),
        reconciliation_ref: reference,
    })
}
fn invalid() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::RuntimeSnapshotInvalid,
        retryable: false,
    }
}

fn original_browser_policy(
    original: Option<&Option<Arc<ExecutionLivenessPolicy>>>,
    current: &Arc<ExecutionLivenessPolicy>,
) -> Option<Arc<ExecutionLivenessPolicy>> {
    match original {
        Some(policy) => policy.clone(),
        None => Some(current.clone()),
    }
}

/// Worker recovery may consume the original Browser receipt, but cannot enqueue
/// another Job. The journal owns the bounded recovery window and final commit.
pub(crate) async fn retained_result(
    handler: &dyn AtomicCapabilityHandler,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    deadline: UtcTimestamp,
) -> Result<crate::RetainedToolResult, LoopPortError> {
    let reference = durable
        .prebound_reconciliation_ref
        .clone()
        .ok_or_else(invalid)?;
    let trace = uuid::Uuid::now_v7().simple().to_string();
    let request = CapabilityReconciliation {
        actor: material.actor,
        tenant_id: material.run_context.tenant_id,
        conversation_id: material.run_context.conversation_id,
        run_id: material.run_context.run_id,
        workspace_id: match material.run_context.execution_scope {
            musteragent_event_envelope::ExecutionScope::Workspace { workspace_id, .. } => {
                Some(workspace_id)
            }
            _ => None,
        },
        execution_attempt_id: attempt.attempt_id,
        execution_attempt_number: attempt.attempt_number,
        invocation_id: durable.invocation.invocation_id,
        provider_id: durable.invocation.capability_provider_id,
        idempotency_key: durable.invocation.idempotency_key.clone(),
        result_schema: material.tool.manifest.output_schema.clone(),
        reconciliation_ref: reference,
        deadline,
        trace_context: musteragent_runtime_contract::TraceContext {
            traceparent: musteragent_runtime_contract::TraceParent::new(format!(
                "00-{trace}-{}-00",
                &trace[16..]
            ))
            .map_err(|_| invalid())?,
            tracestate: None,
        },
        execution_grant_proof: material.execution_grant_proof.clone(),
    };
    Ok(
        match handler
            .reconcile(attempt, material, durable, request)
            .await?
        {
            DispatchedReconciliationOutcome::Committed(result) => {
                crate::RetainedToolResult::Committed(result)
            }
            DispatchedReconciliationOutcome::Provider(outcome) => match outcome {
                ReconciliationOutcome::ProvenSucceeded(result) => {
                    crate::RetainedToolResult::Complete(Box::new(CapabilityOutcome::Succeeded(
                        result,
                    )))
                }
                ReconciliationOutcome::ProvenSucceededWithAuthorizedContent {
                    result,
                    authorized_content,
                } => crate::RetainedToolResult::Complete(Box::new(
                    CapabilityOutcome::SucceededWithAuthorizedContent {
                        result,
                        authorized_content,
                    },
                )),
                ReconciliationOutcome::ProvenFailed {
                    error,
                    retry_disposition,
                    diagnostic_result,
                } => crate::RetainedToolResult::Complete(Box::new(CapabilityOutcome::Failed {
                    error,
                    retry_disposition,
                    diagnostic_result,
                })),
                _ => crate::RetainedToolResult::Incomplete,
            },
        },
    )
}

pub(crate) fn gateway_failure(
    error: BrowserProviderRuntimeError,
    unavailable: &BrowserUnavailableNotice,
) -> Result<CapabilityOutcome, LoopPortError> {
    if browser_unavailable(&error) {
        return Ok(unavailable.failure());
    }
    if matches!(
        &error,
        BrowserProviderRuntimeError::SandboxRoute(_)
            | BrowserProviderRuntimeError::SandboxRoutingUnavailable
    ) {
        return Ok(CapabilityOutcome::Failed {
            error: ErrorInfo {
                code: ErrorCode::CapabilityUnavailable,
                category: ErrorCode::CapabilityUnavailable.category(),
                safe_message: SafeDisplayText::new(error.to_string()).map_err(|_| invalid())?,
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        });
    }
    let (code, message, retryable) = match error {
        BrowserProviderRuntimeError::CapabilityNotAuthorized
        | BrowserProviderRuntimeError::RuntimeReleaseMismatch => (
            ErrorCode::AgentNotAvailable,
            "The selected sandbox executor is unavailable; the original route was retained",
            true,
        ),
        BrowserProviderRuntimeError::DurabilityUnavailable => (
            ErrorCode::DependencyUnavailable,
            "Browser execution coordination is temporarily unavailable",
            true,
        ),
        _ => (
            ErrorCode::ContentIntegrityFailed,
            "Browser execution request failed Runtime integrity validation",
            false,
        ),
    };
    Ok(CapabilityOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::new(message).map_err(|_| LoopPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            })?,
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
    })
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;
    use musteragent_identity::VersionedKey;
    use musteragent_runtime_contract::execution_liveness::{
        ActivitySource, ExecutionLivenessPolicy, ExecutionLivenessWindows,
    };
    use std::sync::Arc;

    #[test]
    fn new_jobs_use_sealed_activity_but_recovery_never_rebinds_an_original_policy() {
        let policy = |key| {
            Arc::new(
                ExecutionLivenessPolicy::new(
                    VersionedKey::new(key).unwrap(),
                    ExecutionLivenessWindows::for_idle_window(Duration::from_secs(60)),
                    ActivitySource::ALL,
                )
                .unwrap(),
            )
        };
        let current = policy("new.tool@1");
        assert_eq!(
            original_browser_policy(None, &current),
            Some(current.clone())
        );
        assert_eq!(original_browser_policy(Some(&None), &current), None);
        let original = Some(policy("prior.tool@1"));
        assert_eq!(original_browser_policy(Some(&original), &current), original);
    }
}
