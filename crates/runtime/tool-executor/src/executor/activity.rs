//! Complete activity Tool operations remain owned by the calling Runtime task.
//! Authorization can be cancelled before dispatch; physical work and its
//! original result transaction stay inside the shared reactor until settled.
use super::*;
use musteragent_execution_liveness::{Closure, ControlStop, StopReason};
use musteragent_loop::{
    CancellationCause, ExecutionBoundary, ExecutionSupervisionError, ExecutionSupervisionOutcome,
    InvocationAborted, InvocationSupervisor,
};
use musteragent_runtime_contract::execution_liveness::ExecutionScope;

type OwnedToolResult = Result<Result<ToolExecutionOutcome, LoopPortError>, InvocationAborted>;

/// A confirmed physical stop does not imply that the Agent was cancelled.
/// Emergency cleanup retains its resource closure separately from the error
/// that interrupted observation of the original Tool.
#[derive(Clone, Copy)]
enum ToolStopOrigin {
    Control,
    SupervisionFailure(ErrorCode),
}

/// Physical closure and observation evidence from one supervised execution.
struct StoppedTool {
    closure: Closure,
    failed: Option<Result<DispatchedCapabilityOutcome, LoopPortError>>,
    origin: ToolStopOrigin,
}

impl DurableToolExecutor {
    pub(super) async fn execute_activity(
        &self,
        attempt: &AttemptContext,
        telemetry: Option<&AttemptTelemetryContext>,
        supervisor: &InvocationSupervisor,
        call: PlannedToolCall,
    ) -> OwnedToolResult {
        if self.execution_supervision.is_none() {
            return Ok(Err(port(ErrorCode::CapabilityUnavailable, false)));
        }
        let (material, proposed, durable) = match supervisor
            .run(
                CancellationCause::UserRequested,
                control_request(
                    musteragent_runtime_contract::execution_liveness::DISCOVERY_REQUEST_WINDOW,
                    self.load_requested(attempt, &call),
                ),
            )
            .await?
        {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let mut aborted = None;
        let result = self
            .observe_tool_transition(
                attempt,
                ObservedToolTransition {
                    telemetry,
                    call: &call,
                    material: &material,
                    proposed,
                    retry_contract: material.tool.manifest.retry_contract,
                },
                async {
                    match self
                        .execute_activity_loaded(attempt, supervisor, &call, &material, durable)
                        .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            aborted = Some(error);
                            Err(port(
                                if error.cause == CancellationCause::LeaseLost {
                                    ErrorCode::AttemptLeaseStale
                                } else {
                                    ErrorCode::DependencyUnavailable
                                },
                                false,
                            ))
                        }
                    }
                },
            )
            .await;
        match aborted {
            Some(error) => Err(error),
            None => Ok(result),
        }
    }

    fn execute_activity_loaded<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        supervisor: &'a InvocationSupervisor,
        call: &'a PlannedToolCall,
        material: &'a ToolExecutionMaterial,
        durable: DurableToolInvocation,
    ) -> ApplicationFuture<'a, OwnedToolResult> {
        // The execution phase owns sizeable authorization, recovery and physical
        // completion states. Keep it behind one heap boundary so telemetry and
        // caller futures do not each embed and move that entire state machine.
        // This remains borrowed, inline-polled work: cancellation still drops
        // the original future and no detached task can outlive the invocation.
        Box::pin(async move {
            if durable.invocation.status == ToolInvocationStatus::Running {
                let Some(home) = self.execution_supervision.as_ref() else {
                    return Ok(Err(port(ErrorCode::RuntimeSnapshotInvalid, false)));
                };
                let Some(policy) = material.tool.manifest.execution_policy.liveness() else {
                    return Ok(Err(port(ErrorCode::RuntimeSnapshotInvalid, false)));
                };
                if let Some(result) = self
                    .recover_retained_result(attempt, call, material, &durable)
                    .await?
                {
                    return Ok(result);
                }
                if let Err(error) = home
                    .close_unobserved(
                        attempt,
                        ExecutionScope::Invocation(durable.invocation.invocation_id),
                        policy,
                    )
                    .await
                {
                    return supervision_failure(&error);
                }
                if durable.retry_contract == musteragent_tool::ToolRetryContract::ReadOnly
                    && durable.invocation.effect == musteragent_policy::Effect::Read
                {
                    return Ok(control_request(policy.windows().post_exit_drain, async {
                        let outcome = failed_outcome(
                            ErrorCode::DependencyUnavailable,
                            "The original read result could not be recovered after observation of its worker ended. No value was returned; a new read requires current authorization.",
                        )?;
                        let completed = self.journal.complete(
                            attempt,
                            durable.invocation.invocation_id,
                            outcome.clone(),
                            self.clock.now(),
                        ).await.map_err(port_from_tool)?;
                        self.message_for_outcome(call, material, &completed, &outcome).await
                    }).await);
                }
                return Ok(
                    match control_request(
                        policy.windows().post_exit_drain,
                        self.seal_unknown_outcome(
                            attempt,
                            durable,
                            SafeDisplayText::new(
                                "The previous Worker stopped after external execution began",
                            )
                            .expect("static reason"),
                        ),
                    )
                    .await
                    {
                        Ok(recorded) => {
                            self.activity_unknown_feedback(attempt, call, material, &recorded)
                                .await
                        }
                        Err(error) => Err(error),
                    },
                );
            }
            let Some(policy) = material.tool.manifest.execution_policy.liveness() else {
                return Ok(Err(port(ErrorCode::RuntimeSnapshotInvalid, false)));
            };
            // Preparation is owned work, not a short status observation. Its
            // Its budget follows the sealed startup profile, including the shared
            // minimum window and any longer capability-specific window.
            let window = policy.windows().startup;
            let prepared = match supervisor
                .run(
                    CancellationCause::UserRequested,
                    preparation_request(
                        window,
                        durable.invocation.invocation_id,
                        "authorize",
                        self.prepare_loaded(attempt, call, material, durable),
                    ),
                )
                .await?
            {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            match prepared {
                PreparedTool::Outcome(outcome) => Ok(Ok(outcome)),
                PreparedTool::Recorded { durable, outcome } => {
                    // A retained unknown result is not permission to dispatch again.
                    // Only the original reconciliation authority may resolve it.
                    if matches!(outcome, ToolTerminalOutcome::Indeterminate { .. }) {
                        Ok(self
                            .activity_unknown_feedback(attempt, call, material, &durable)
                            .await)
                    } else {
                        Ok(control_request(
                            window,
                            self.message_for_outcome(call, material, &durable, &outcome),
                        )
                        .await)
                    }
                }
                PreparedTool::Authorized { material, durable } => {
                    match supervisor
                        .run(
                            CancellationCause::UserRequested,
                            preparation_request(
                                window,
                                durable.invocation.invocation_id,
                                "interaction",
                                self.resolve_authorized_interaction(
                                    attempt, call, &material, &durable,
                                ),
                            ),
                        )
                        .await?
                    {
                        Ok(Some(outcome)) => return Ok(Ok(outcome)),
                        Ok(None) => {}
                        Err(error) => return Ok(Err(error)),
                    }
                    let (started, capability) = match supervisor
                        .run(
                            CancellationCause::UserRequested,
                            preparation_request(
                                window,
                                durable.invocation.invocation_id,
                                "start",
                                self.start_activity_tool(attempt, call, &material, &durable),
                            ),
                        )
                        .await?
                    {
                        Ok(value) => value,
                        Err(error) => {
                            return Ok(self
                                .reject_undispatched_tool(attempt, call, &material, &durable, error)
                                .await);
                        }
                    };
                    self.supervise_activity_tool(
                        attempt, supervisor, call, &material, started, capability,
                    )
                    .await
                }
            }
        })
    }

    async fn start_activity_tool(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<(DurableToolInvocation, CapabilityInvocation), LoopPortError> {
        // A previous physical generation requires the original recovery path.
        // In particular read-only/idempotent Tools do not buy a new startup.
        if !matches!(
            durable.invocation.status,
            ToolInvocationStatus::Requested | ToolInvocationStatus::WaitingInteraction
        ) {
            return Err(port(ErrorCode::RunTransitionInvalid, false));
        }
        let start = self.execution_start(material, durable, ToolStartMode::Initial)?;
        // Build the authorized invocation once before journaling startup. In
        // particular, trace-context database reads must not be repeated after
        // startup under the status-observation window.
        let capability = self
            .capability_invocation(attempt, call, material, durable)
            .await?;
        let reconciliation = self.reconciliation_identity(material, durable, &capability)?;
        let started = self
            .journal
            .start_with_reconciliation_identity(
                attempt,
                durable.invocation.invocation_id,
                ToolStartMode::Initial,
                reconciliation,
                start,
            )
            .await
            .map_err(port_from_tool)?;
        Ok((started, capability))
    }

    async fn supervise_activity_tool(
        &self,
        attempt: &AttemptContext,
        supervisor: &InvocationSupervisor,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: DurableToolInvocation,
        capability: CapabilityInvocation,
    ) -> OwnedToolResult {
        let Some(home) = &self.execution_supervision else {
            return Ok(Err(port(ErrorCode::RuntimeSnapshotInvalid, false)));
        };
        let Some(policy) = material.tool.manifest.execution_policy.liveness() else {
            return Ok(Err(port(ErrorCode::RuntimeSnapshotInvalid, false)));
        };
        let record = match home
            .load(
                attempt,
                ExecutionScope::Invocation(durable.invocation.invocation_id),
                policy,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => return supervision_failure(&error),
        };
        if let Err(error) =
            self.verify_prebound_reconciliation_identity(material, &durable, &capability)
        {
            return Ok(Err(error));
        }
        let dispatcher = match self.bound_dispatcher(material) {
            Ok(value) => value,
            Err(error) => {
                return Ok(self
                    .reject_undispatched_tool(attempt, call, material, &durable, error)
                    .await);
            }
        };
        let operation =
            match dispatcher.execution(attempt, material, &durable, capability, record.binding()) {
                Ok(value) => value,
                Err(error) => {
                    // The factory performs no I/O. A missing/unsupported capability
                    // is a Tool failure, not a failure of the Agent's task.
                    return Ok(self
                        .reject_undispatched_tool(attempt, call, material, &durable, error)
                        .await);
                }
            };
        let (reactor, sink) = match home.attach(attempt.clone(), record, operation.stop_control()) {
            Ok(value) => value,
            Err(_) => return Ok(Err(port(ErrorCode::RuntimeSnapshotInvalid, false))),
        };
        let (failed_sender, mut failed_receiver) = tokio::sync::oneshot::channel();
        let mut failed = None;
        let mut requested_cancellation = None;
        let cancellation = async {
            tokio::select! {
                biased;
                aborted = supervisor.cancelled(CancellationCause::UserRequested) => {
                    requested_cancellation = Some(aborted.cause);
                    control_stop(aborted.cause)
                },
                result = &mut failed_receiver => {
                    failed = result.ok();
                    if matches!(&failed, Some(Ok(DispatchedCapabilityOutcome::Provider(
                        CapabilityOutcome::Failed { error, .. }))) if error.code == ErrorCode::DependencyTimeout)
                    {
                        ControlStop::RequestTimedOut
                    } else {
                        ControlStop::ExecutionFailed
                    }
                }
            }
        };
        let boundary = async {
            let dispatched = operation.execute(sink).await;
            if let Err(error) = &dispatched {
                tracing::warn!(
                    event = "tool.physical_execution.failed",
                    run_id = %attempt.run_id,
                    attempt_id = %attempt.attempt_id,
                    invocation_id = %durable.invocation.invocation_id,
                    tool_key = %durable.invocation.tool_key.as_str(),
                    code = ?error.code,
                    retryable = error.retryable,
                    "Physical tool execution failed before its durable result boundary"
                );
            }
            match dispatched {
                Err(_)
                | Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Failed { .. } | CapabilityOutcome::Indeterminate { .. },
                )) => {
                    let _ = failed_sender.send(dispatched);
                    std::future::pending().await
                }
                Ok(dispatched) => match self
                    .settle_dispatched(attempt, call, material, durable.clone(), dispatched)
                    .await
                {
                    Ok(outcome) => ExecutionBoundary::Committed(outcome),
                    Err(error) => ExecutionBoundary::Interrupted(journal_error(error.source)),
                },
            }
        };
        let observed = reactor.run(boundary, cancellation).await;
        let provider_failed_before_stop = failed.is_some();
        // The native failure may return during cleanup, after the cancellation
        // future has resolved. Retain that result instead of discarding it.
        if failed.is_none() {
            failed = failed_receiver.try_recv().ok();
        }
        match observed {
            Ok(ExecutionSupervisionOutcome::Boundary(outcome)) => Ok(Ok(outcome)),
            Ok(ExecutionSupervisionOutcome::Stopped { closure, .. }) => match tokio::time::timeout(
                policy.windows().post_exit_drain,
                self.finish_stopped_tool(
                    attempt,
                    call,
                    material,
                    durable,
                    StoppedTool {
                        closure,
                        failed,
                        origin: ToolStopOrigin::Control,
                    },
                ),
            )
            .await
            {
                Ok(value) => value,
                Err(_) => Ok(Err(port(ErrorCode::DependencyTimeout, false))),
            },
            Err(error) => {
                tracing::warn!(
                    event = "tool.execution_supervision.failed",
                    run_id = %attempt.run_id,
                    invocation_id = %durable.invocation.invocation_id,
                    tool_key = %durable.invocation.tool_key.as_str(),
                    cause = ?error.cause,
                    "Tool execution supervision failed before its durable boundary"
                );
                let failure = supervision_port_error(&error.cause)?;
                // A provider cancellation returned by emergency cleanup is not
                // the cause of an earlier observation failure. In particular,
                // replay must retain a timeout rather than "operation stopped".
                let failed = failed.filter(|_| provider_failed_before_stop);
                let origin = if requested_cancellation.is_some() {
                    ToolStopOrigin::Control
                } else {
                    ToolStopOrigin::SupervisionFailure(failure.code)
                };
                if let Some(closure @ Closure::StopConfirmed(_)) = error.durable_closure {
                    // Resource shutdown and business success are separate.
                    // Once the original Tool commits its failure, return that
                    // authoritative feedback so the Agent can continue. Only
                    // an unresolved observation remains an infrastructure error.
                    match tokio::time::timeout(
                        policy.windows().post_exit_drain,
                        self.finish_stopped_tool(
                            attempt,
                            call,
                            material,
                            durable,
                            StoppedTool {
                                closure,
                                failed,
                                origin,
                            },
                        ),
                    )
                    .await
                    {
                        Ok(outcome) => return outcome,
                        Err(_) => return Ok(Err(port(ErrorCode::DependencyTimeout, false))),
                    }
                }
                match requested_cancellation {
                    Some(cause) => Err(InvocationAborted { cause }),
                    None => Ok(Err(failure)),
                }
            }
        }
    }

    async fn reject_undispatched_tool(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        error: LoopPortError,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        let policy = material
            .tool
            .manifest
            .execution_policy
            .liveness()
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        control_request(policy.windows().post_exit_drain, async {
        // This path has not constructed/polled physical work. Even a write
        // Tool can safely report a preparation dependency failure as failed.
        let outcome = if matches!(
            error.code,
            ErrorCode::DependencyTimeout | ErrorCode::DependencyUnavailable
        ) {
            failed_outcome(
                error.code,
                "Tool could not be dispatched because its preparation dependency failed or timed out",
            )?
        } else {
            let outcome = match dispatch_rejection_outcome(&error, durable.retry_contract)? {
                Some(outcome) => Some(outcome),
                None => undispatched_binding_outcome(&error)?,
            };
            let Some(outcome) = outcome else {
                return Err(error);
            };
            outcome
        };
        let completed = self
            .journal
            .complete(
                attempt,
                durable.invocation.invocation_id,
                outcome.clone(),
                self.clock.now(),
            )
            .await
            .map_err(port_from_tool)?;
        self.message_for_outcome(call, material, &completed, &outcome)
            .await
        }).await
    }

    pub(super) async fn activity_unknown_feedback(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        let outcome = durable
            .recorded_outcome
            .as_ref()
            .filter(|outcome| matches!(outcome, ToolTerminalOutcome::Indeterminate { .. }))
            .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
        let home = self
            .execution_supervision
            .as_ref()
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        let policy = material
            .tool
            .manifest
            .execution_policy
            .liveness()
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        let closure = home
            .closed_outcome(
                attempt,
                ExecutionScope::Invocation(durable.invocation.invocation_id),
                policy,
            )
            .await
            .map_err(|error| match error {
                ExecutionSupervisionError::Journal(error) => port(error.code, error.retryable),
                ExecutionSupervisionError::JournalRequestExpired => {
                    port(ErrorCode::DependencyTimeout, true)
                }
                _ => port(ErrorCode::InternalInvariantViolated, false),
            })?;
        let failure = match closure {
            Some(Closure::StopUnconfirmed(reason)) => {
                Some(observation::ObservationFailure::Execution(reason))
            }
            Some(Closure::StopConfirmed(reason)) => Some(
                observation::ObservationFailure::StoppedOutcomeUnknown(reason),
            ),
            _ => None,
        };
        // Unknown external effects remain recorded under the original invocation.
        // They are information for the next model decision, never a Run stop.
        control_request(
            policy.windows().post_exit_drain,
            self.message_for_outcome_with_failure(call, material, durable, outcome, failure),
        )
        .await
    }

    async fn finish_stopped_tool(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: DurableToolInvocation,
        stopped: StoppedTool,
    ) -> OwnedToolResult {
        let StoppedTool {
            closure,
            failed,
            origin,
        } = stopped;
        let failed = failed.or_else(|| match origin {
            ToolStopOrigin::Control => None,
            ToolStopOrigin::SupervisionFailure(code) => Some(Err(port(code, false))),
        });
        let Closure::StopConfirmed(reason) = closure else {
            let unknown = || {
                SafeDisplayText::new(
                    "Tool observation ended without confirmation of its external result",
                )
                .expect("static reason")
            };
            let (reason, observed_error, diagnostic_result) = match failed {
                Some(Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Indeterminate {
                        reason,
                        observed_error,
                        diagnostic_result,
                        ..
                    },
                ))) => (reason, observed_error, diagnostic_result),
                Some(Ok(DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
                    error,
                    diagnostic_result,
                    ..
                }))) => (unknown(), Some(error), diagnostic_result),
                Some(Err(error)) => (
                    unknown(),
                    Some(musteragent_runtime_contract::ErrorInfo {
                        code: error.code,
                        category: error.code.category(),
                        retryable: false,
                        safe_message: SafeDisplayText::new(
                            "Physical tool execution failed before result confirmation",
                        )
                        .expect("static reason"),
                        dependency_ref: None,
                        diagnostic_ref: None,
                    }),
                    None,
                ),
                _ => (unknown(), None, None),
            };
            return Ok(
                match self
                    .seal_unknown_with_evidence(
                        attempt,
                        durable,
                        reason,
                        observed_error,
                        diagnostic_result,
                    )
                    .await
                {
                    Ok(durable) => {
                        self.activity_unknown_feedback(attempt, call, material, &durable)
                            .await
                    }
                    Err(error) => Err(error),
                },
            );
        };
        let aborted = match (&origin, reason) {
            (ToolStopOrigin::SupervisionFailure(_), StopReason::SystemShutdown) => None,
            (_, StopReason::UserCancelled) => Some(CancellationCause::UserRequested),
            (_, StopReason::ParentCancelled) => Some(CancellationCause::ParentCancelled),
            (_, StopReason::CoordinationRequested) => {
                Some(CancellationCause::CoordinationRequested)
            }
            (_, StopReason::OwnershipLost) => {
                return Err(InvocationAborted {
                    cause: CancellationCause::LeaseLost,
                });
            }
            (_, StopReason::SystemShutdown) => {
                return Err(InvocationAborted {
                    cause: CancellationCause::SystemShutdown,
                });
            }
            _ => None,
        };
        let terminal = if let Some(cause) = aborted {
            ToolTerminalOutcome::Cancelled(match cause {
                CancellationCause::ParentCancelled => CancelReason::ParentCancelled,
                CancellationCause::CoordinationRequested => CancelReason::CoordinationInterrupted,
                CancellationCause::UserRequested => CancelReason::UserRequested,
                CancellationCause::LeaseLost | CancellationCause::SystemShutdown => {
                    return Err(InvocationAborted { cause });
                }
            })
        } else if let Some(Ok(DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
            mut error,
            diagnostic_result,
            ..
        }))) = failed
        {
            // Cleanup can return a transport cancellation error after the
            // authoritative timeout. Preserve the committed stop reason in
            // model feedback instead of relabeling it as a different failure.
            if matches!(
                reason,
                StopReason::StartupExpired
                    | StopReason::IdleExpired
                    | StopReason::BusinessDeadlineExpired
                    | StopReason::PostExitDrainExpired
            ) {
                error.code = ErrorCode::DependencyTimeout;
                error.category = error.code.category();
                error.retryable = false;
                error.safe_message =
                    match safe_text("Tool stopped after its execution activity window expired") {
                        Ok(message) => message,
                        Err(error) => return Ok(Err(error)),
                    };
            }
            ToolTerminalOutcome::Failed {
                error,
                diagnostic_result,
                retry_disposition: musteragent_runtime_contract::RetryDisposition::DoNotRetry,
            }
        } else if let Some(Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Indeterminate {
                reason,
                reconciliation_ref: _,
                observed_error,
                diagnostic_result,
            },
        ))) = failed
        {
            ToolTerminalOutcome::Indeterminate {
                reason,
                observed_error,
                diagnostic_result,
                reconciliation_ref: match durable.prebound_reconciliation_ref.clone() {
                    Some(reference) => reference,
                    None => match operator_reconciliation_ref(durable.invocation.invocation_id) {
                        Ok(reference) => reference,
                        Err(error) => return Ok(Err(error)),
                    },
                },
            }
        } else if durable.retry_contract != musteragent_tool::ToolRetryContract::ReadOnly {
            // Physical quiescence does not prove that a dispatched write had
            // no partial effects or that its result was retained.
            ToolTerminalOutcome::Indeterminate {
                observed_error: failed.and_then(|result| result.err()).map(|error| {
                    musteragent_runtime_contract::ErrorInfo {
                        code: error.code,
                        category: error.code.category(),
                        retryable: false,
                        safe_message: SafeDisplayText::new(
                            "Physical tool execution failed before result confirmation",
                        )
                        .expect("static reason"),
                        dependency_ref: None,
                        diagnostic_ref: None,
                    }
                }),
                diagnostic_result: None,
                reason: match safe_text(
                    "Tool execution stopped but its external result could not be confirmed",
                ) {
                    Ok(reason) => reason,
                    Err(error) => return Ok(Err(error)),
                },
                reconciliation_ref: match durable.prebound_reconciliation_ref.clone() {
                    Some(reference) => reference,
                    None => match operator_reconciliation_ref(durable.invocation.invocation_id) {
                        Ok(reference) => reference,
                        Err(error) => return Ok(Err(error)),
                    },
                },
            }
        } else {
            let timeout = matches!(
                reason,
                StopReason::RequestTimedOut
                    | StopReason::StartupExpired
                    | StopReason::IdleExpired
                    | StopReason::BusinessDeadlineExpired
                    | StopReason::PostExitDrainExpired
            );
            match failed_outcome(
                stopped_read_only_error(
                    origin,
                    reason,
                    failed
                        .as_ref()
                        .and_then(|outcome| outcome.as_ref().err())
                        .map(|error| error.code),
                ),
                if matches!(origin, ToolStopOrigin::SupervisionFailure(_)) {
                    "Tool observation failed and its original operation has stopped"
                } else if timeout {
                    "Tool stopped after its execution activity window expired"
                } else {
                    "Tool execution failed and its original operation has stopped"
                },
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            }
        };
        let completed = match self
            .journal
            .complete(
                attempt,
                durable.invocation.invocation_id,
                terminal.clone(),
                self.clock.now(),
            )
            .await
        {
            Ok(value) => value,
            Err(error) if error.code == ErrorCode::IdempotencyConflict && aborted.is_none() => {
                // The operation's own atomic receipt committed a terminal before
                // the supervised stop closed it. That durable fact, not the
                // stop's inferred outcome, is the observation the Agent gets.
                match self
                    .journal
                    .load_observation(attempt, durable.invocation.clone(), durable.retry_contract)
                    .await
                {
                    Ok(observation) => {
                        let recorded = observation.durable().clone();
                        match recorded.recorded_outcome.clone() {
                            Some(outcome)
                                if !matches!(
                                    outcome,
                                    ToolTerminalOutcome::Indeterminate { .. }
                                ) =>
                            {
                                return Ok(self
                                    .materialize_outcome(call, material, &recorded, &outcome)
                                    .await
                                    .map_err(|error| error.source));
                            }
                            _ => return Ok(Err(port_from_tool(error))),
                        }
                    }
                    Err(_) => return Ok(Err(port_from_tool(error))),
                }
            }
            Err(error) => return Ok(Err(port_from_tool(error))),
        };
        if let Some(cause) = aborted {
            Err(InvocationAborted { cause })
        } else if matches!(terminal, ToolTerminalOutcome::Indeterminate { .. }) {
            Ok(self
                .activity_unknown_feedback(attempt, call, material, &completed)
                .await)
        } else {
            Ok(self
                .materialize_outcome(call, material, &completed, &terminal)
                .await
                .map_err(|error| error.source))
        }
    }
}
fn control_stop(cause: CancellationCause) -> ControlStop {
    match cause {
        CancellationCause::UserRequested => ControlStop::UserCancelled,
        CancellationCause::ParentCancelled => ControlStop::ParentCancelled,
        CancellationCause::LeaseLost => ControlStop::OwnershipLost,
        CancellationCause::SystemShutdown => ControlStop::SystemShutdown,
        CancellationCause::CoordinationRequested => ControlStop::CoordinationRequested,
    }
}
fn journal_error(error: LoopPortError) -> ExecutionSupervisionError {
    ExecutionSupervisionError::Journal(musteragent_execution_liveness::LivenessJournalError {
        code: error.code,
        retryable: error.retryable,
        rejection: None,
    })
}
pub(super) fn supervision_failure(error: &ExecutionSupervisionError) -> OwnedToolResult {
    Ok(Err(supervision_port_error(error)?))
}

fn supervision_port_error(
    error: &ExecutionSupervisionError,
) -> Result<LoopPortError, InvocationAborted> {
    let failure = match error {
        ExecutionSupervisionError::Journal(error) if error.code == ErrorCode::AttemptLeaseStale => {
            return Err(InvocationAborted {
                cause: CancellationCause::LeaseLost,
            });
        }
        ExecutionSupervisionError::Journal(error) => port(error.code, error.retryable),
        ExecutionSupervisionError::JournalRequestExpired
        | ExecutionSupervisionError::BoundaryDrainExpired => {
            port(ErrorCode::DependencyTimeout, false)
        }
        ExecutionSupervisionError::Invalid(_) => port(ErrorCode::DependencyUnavailable, false),
    };
    // A dependency/observation failure leaves any unknown physical effects for
    // reconciliation. It is never a synthetic Agent shutdown signal.
    Ok(failure)
}

pub(super) async fn control_request<T>(
    window: std::time::Duration,
    request: impl std::future::Future<Output = Result<T, LoopPortError>>,
) -> Result<T, LoopPortError> {
    tokio::time::timeout(window, request)
        .await
        .map_err(|_| port(ErrorCode::DependencyTimeout, false))?
}

/// Startup/preparation work has its own finite request boundary. Stage labels
/// are static and contain no arguments or credentials; retain them for diagnosis.
async fn preparation_request<T>(
    window: std::time::Duration,
    invocation_id: musteragent_identity::InvocationId,
    stage: &'static str,
    request: impl std::future::Future<Output = Result<T, LoopPortError>>,
) -> Result<T, LoopPortError> {
    let started = std::time::Instant::now();
    let result = control_request(window, request).await;
    if let Err(error) = &result {
        tracing::warn!(
            event = "tool.preparation.failed",
            %invocation_id,
            stage,
            timeout_ms = window.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            code = ?error.code,
            "Tool preparation failed before physical dispatch"
        );
    }
    result
}

// Only called after confirmed stop and after writes have taken their separate
// indeterminate branch. Cleanup errors must not replace authoritative deadlines
// or observation failures, but a local read denial is not dependency loss.
fn stopped_read_only_error(
    origin: ToolStopOrigin,
    reason: StopReason,
    observed_error: Option<ErrorCode>,
) -> ErrorCode {
    if let ToolStopOrigin::SupervisionFailure(code) = origin {
        return code;
    }
    match reason {
        StopReason::RequestTimedOut
        | StopReason::StartupExpired
        | StopReason::IdleExpired
        | StopReason::BusinessDeadlineExpired
        | StopReason::PostExitDrainExpired => ErrorCode::DependencyTimeout,
        StopReason::AuthorizationLost => ErrorCode::PolicyDenied,
        StopReason::ExecutionFailed => observed_error.unwrap_or(ErrorCode::DependencyUnavailable),
        _ => ErrorCode::DependencyUnavailable,
    }
}

#[cfg(test)]
mod stopped_read_error_tests {
    use super::*;

    #[test]
    fn confirmed_read_failure_keeps_the_original_typed_cause() {
        for code in [
            ErrorCode::PolicyDenied,
            ErrorCode::InvalidCommand,
            ErrorCode::ContentUnavailable,
            ErrorCode::ContentIntegrityFailed,
            ErrorCode::RuntimeSnapshotInvalid,
            ErrorCode::DependencyUnavailable,
        ] {
            assert_eq!(
                stopped_read_only_error(
                    ToolStopOrigin::Control,
                    StopReason::ExecutionFailed,
                    Some(code),
                ),
                code,
            );
        }
        assert_eq!(
            stopped_read_only_error(ToolStopOrigin::Control, StopReason::ExecutionFailed, None,),
            ErrorCode::DependencyUnavailable,
        );
    }

    #[test]
    fn cleanup_failure_never_relabels_authoritative_deadline_or_revocation() {
        for reason in [
            StopReason::RequestTimedOut,
            StopReason::StartupExpired,
            StopReason::IdleExpired,
            StopReason::BusinessDeadlineExpired,
            StopReason::PostExitDrainExpired,
        ] {
            assert_eq!(
                stopped_read_only_error(
                    ToolStopOrigin::Control,
                    reason,
                    Some(ErrorCode::PolicyDenied),
                ),
                ErrorCode::DependencyTimeout,
            );
        }
        assert_eq!(
            stopped_read_only_error(
                ToolStopOrigin::Control,
                StopReason::AuthorizationLost,
                Some(ErrorCode::DependencyUnavailable),
            ),
            ErrorCode::PolicyDenied,
        );
    }

    #[test]
    fn observation_failure_keeps_precedence_over_provider_cleanup() {
        assert_eq!(
            stopped_read_only_error(
                ToolStopOrigin::SupervisionFailure(ErrorCode::ContentIntegrityFailed),
                StopReason::SystemShutdown,
                Some(ErrorCode::DependencyUnavailable),
            ),
            ErrorCode::ContentIntegrityFailed,
        );
        assert_eq!(
            stopped_read_only_error(
                ToolStopOrigin::Control,
                StopReason::RecoveryUnverified,
                Some(ErrorCode::PolicyDenied),
            ),
            ErrorCode::DependencyUnavailable,
        );
    }
}
