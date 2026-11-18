use super::*;

/// The classified durable identity of one failed physical model invocation:
/// the frozen failure-table entry, any Provider retry hint, and whether the
/// failure carried a verified explicit context-capacity signal.
pub(super) struct ClassifiedModelFailure {
    pub provider_code: Option<musteragent_runtime_model::ProviderErrorCode>,
    pub failure: ModelFailure,
    pub retry_after_ms: Option<u64>,
    pub capacity_signal: crate::ContextCapacitySignal,
}

impl DurableAgentLoop {
    pub(super) async fn finalize_model_failure(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        checkpoint: &DurableLoopCheckpoint,
        observation: ModelInvocationObservation,
        classified: ClassifiedModelFailure,
    ) -> Result<DurableLoopCheckpoint, AgentLoopExit> {
        let ClassifiedModelFailure {
            provider_code,
            failure,
            retry_after_ms,
            capacity_signal,
        } = classified;
        // Explicit Provider context-capacity overflow under an admitted @7
        // compaction policy recovers transparently: the physical failure is
        // still committed below as a durable audit fact, but the failure
        // identity becomes retryable so the resolution schedules a same-route
        // replacement Attempt (the next Context plan compacts before it
        // requests). A failed policy lookup interrupts without inventing a terminal failure,
        // and cancellation/lease loss keep precedence over this recovery.
        let admitted_policy = match failure {
            ModelFailure {
                code: ErrorCode::ModelContextExceeded,
                ..
            } if matches!(
                capacity_signal,
                crate::ContextCapacitySignal::ExplicitOverflow
            ) =>
            {
                self.policy
                    .context_compaction_policy_ref(attempt)
                    .await
                    .map_err(|error| AgentLoopExit::Interrupted {
                        cause: if error.code == ErrorCode::AttemptLeaseStale {
                            CancellationCause::LeaseLost
                        } else {
                            CancellationCause::SystemShutdown
                        },
                    })?
            }
            _ => None,
        };
        let failure = crate::recoverable_overflow_failure(
            failure,
            capacity_signal,
            observation.replay_barrier_observed
                && !execution
                    .snapshot
                    .recovery_policy
                    .permits_sealed_continuation(true),
            admitted_policy.as_ref(),
        );
        let recovery_budget = self
            .journal
            .recovery_budget(attempt)
            .await
            .map_err(|error| AgentLoopExit::Interrupted {
                cause: if error.code == ErrorCode::AttemptLeaseStale {
                    CancellationCause::LeaseLost
                } else {
                    CancellationCause::SystemShutdown
                },
            })?;
        let mut terminal_override = None;
        let modern = execution.snapshot.provider_failover_v1;
        let cause = provider_code.filter(|code| {
            code.permits_alternate_provider() && code.runtime_code() == failure.code
        });
        let may_advance = if modern {
            cause.is_some()
        } else {
            failure.permits_fallback_for(execution.snapshot.recovery_policy)
        };
        let skip = if modern {
            cause.is_none()
        } else {
            failure.prefers_persistent_retry(execution.snapshot.recovery_policy)
        };
        let decision = if observation.replay_barrier_observed || skip {
            // Never stitch a second Provider onto already delivered output.
            FallbackDecision::DoNotFallback
        } else {
            let context = StepContext {
                attempt: attempt.clone(),
                checkpoint: checkpoint.clone(),
                execution,
                now: self.clock.now(),
            };
            let selection = if modern {
                self.policy.fallback_after_upstream_failure(&context).await
            } else {
                self.policy
                    .fallback_after_model_failure(failure, &context)
                    .await
            };
            match selection {
                Ok(decision) => decision,
                Err(error) => {
                    terminal_override = Some((
                        AgentLoopFailureStage::FallbackPolicy,
                        ModelFailure {
                            code: error.code,
                            retryable: false,
                        },
                    ));
                    FallbackDecision::DoNotFallback
                }
            }
        };
        let resolution = if let Some((_, terminal_failure)) = terminal_override {
            ModelFailureResolution::stop(execution.snapshot.recovery_policy, terminal_failure)
        } else {
            match (decision, cause) {
                (FallbackDecision::UseSealedRoute { next_route_ordinal }, _)
                    if may_advance
                        && next_route_ordinal
                            == checkpoint.active_model_route_ordinal.saturating_add(1)
                        && next_route_ordinal < execution.model_route_count.get() =>
                {
                    match (modern, cause) {
                        (true, Some(cause)) => ModelFailureResolution::ProviderFallbackV1 {
                            next_route_ordinal,
                            cause,
                        },
                        _ => ModelFailureResolution::RetryFallbackRoute { next_route_ordinal },
                    }
                }
                (FallbackDecision::UseSealedRoute { .. }, _) => {
                    let terminal_failure = ModelFailure {
                        code: ErrorCode::InternalInvariantViolated,
                        retryable: false,
                    };
                    terminal_override =
                        Some((AgentLoopFailureStage::FallbackPolicy, terminal_failure));
                    ModelFailureResolution::stop(
                        execution.snapshot.recovery_policy,
                        terminal_failure,
                    )
                }
                (FallbackDecision::SealedRoutesExhausted, Some(cause))
                    if modern
                        && !observation.replay_barrier_observed
                        && matches!(
                            execution.snapshot.recovery_policy,
                            musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
                        ) =>
                {
                    ModelFailureResolution::ExhaustedProviderRoundV1 {
                        cause,
                        retry_route_ordinal: ModelFailureResolution::retry_ordinal(
                            cause,
                            checkpoint.active_model_route_ordinal,
                            recovery_budget,
                        ),
                    }
                }
                (FallbackDecision::SealedRoutesExhausted | FallbackDecision::DoNotFallback, _)
                    if execution
                        .snapshot
                        .recovery_policy
                        .permits_sealed_continuation(observation.replay_barrier_observed)
                        && failure.permits_new_attempt()
                        && recovery_budget
                            .permits_failure_retry(execution.snapshot.recovery_policy, failure) =>
                {
                    ModelFailureResolution::RetryNewAttempt
                }
                (FallbackDecision::SealedRoutesExhausted | FallbackDecision::DoNotFallback, _) => {
                    ModelFailureResolution::stop(execution.snapshot.recovery_policy, failure)
                }
            }
        };
        let exit_failure = terminal_override.map_or_else(
            || match resolution {
                ModelFailureResolution::RetryFallbackRoute { .. }
                | ModelFailureResolution::ProviderFallbackV1 { .. } => failure,
                ModelFailureResolution::ExhaustedProviderRoundV1 { .. }
                | ModelFailureResolution::RetryNewAttempt
                | ModelFailureResolution::ExternalOutcomeUnknown => failure,
                ModelFailureResolution::FailRun { failure } => failure,
            },
            |(_, failure)| failure,
        );
        let exit_stage = terminal_override
            .map(|(stage, _)| stage)
            .unwrap_or(AgentLoopFailureStage::ModelExecution);
        let committed = self
            .journal
            .finish_failed_model(
                attempt,
                ModelFailureCommit {
                    recovery_policy: execution.snapshot.recovery_policy,
                    observation,
                    provider_failure: failure,
                    retry_after_ms,
                    resolution,
                },
            )
            .await;
        match committed {
            Ok(committed)
                if committed.run_id == attempt.run_id
                    && committed.attempt_id == attempt.attempt_id
                    && committed.continuation_ref.is_none()
                    && committed.continuation_source_invocation_id.is_none() =>
            {
                if committed.phase == LoopPhase::Cancelled {
                    return Err(AgentLoopExit::Cancelled {
                        receipt: DurableExecutionReceipt::new(
                            attempt,
                            DurableAttemptOutcome::Cancelled,
                            DurableRunOutcome::Cancelled,
                        ),
                    });
                }
                match resolution {
                    ModelFailureResolution::RetryFallbackRoute { next_route_ordinal }
                    | ModelFailureResolution::ProviderFallbackV1 {
                        next_route_ordinal, ..
                    } if committed.phase == LoopPhase::ReadyForModel
                        && committed.active_model_route_ordinal == next_route_ordinal =>
                    {
                        Ok(committed)
                    }
                    ModelFailureResolution::ExhaustedProviderRoundV1 {
                        retry_route_ordinal: None,
                        ..
                    } if committed.phase == LoopPhase::ReadyForModel => {
                        Err(AgentLoopExit::NeedsAction {
                            reason: musteragent_run::RunWaitReason::RecoveryRequired {
                                code: failure.code,
                            },
                            receipt: DurableExecutionReceipt::new(
                                attempt,
                                DurableAttemptOutcome::Failed,
                                DurableRunOutcome::Waiting,
                            ),
                        })
                    }
                    ModelFailureResolution::ExhaustedProviderRoundV1 {
                        retry_route_ordinal: Some(_),
                        ..
                    }
                    | ModelFailureResolution::RetryNewAttempt
                        if committed.phase == LoopPhase::ReadyForModel =>
                    {
                        Err(AgentLoopExit::Failed {
                            stage: exit_stage,
                            code: exit_failure.code,
                            retryable: true,
                            failure_commit_error: None,
                            receipt: Some(DurableExecutionReceipt::new(
                                attempt,
                                DurableAttemptOutcome::Failed,
                                DurableRunOutcome::RetryBackoff,
                            )),
                        })
                    }
                    ModelFailureResolution::FailRun { .. }
                        if committed.phase == LoopPhase::Failed =>
                    {
                        Err(AgentLoopExit::Failed {
                            stage: exit_stage,
                            code: exit_failure.code,
                            retryable: false,
                            failure_commit_error: None,
                            receipt: Some(DurableExecutionReceipt::new(
                                attempt,
                                DurableAttemptOutcome::Failed,
                                DurableRunOutcome::Failed,
                            )),
                        })
                    }
                    _ => Err(model_failure_commit_exit(
                        exit_failure,
                        ErrorCode::InternalInvariantViolated,
                    )),
                }
            }
            Ok(_) => Err(model_failure_commit_exit(
                exit_failure,
                ErrorCode::InternalInvariantViolated,
            )),
            Err(error) if error.code == ErrorCode::AttemptLeaseStale => {
                Err(AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                })
            }
            Err(error) => Err(model_failure_commit_exit(exit_failure, error.code)),
        }
    }
}
