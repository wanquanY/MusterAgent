use super::*;

impl AgentLoopJournal for PostgresAgentLoopJournal {
    fn recovery_budget(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RecoveryBudget, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            let (mut tx, state) = self
                .load_state_in_commit_lane(&attempt)
                .await
                .map_err(loop_error)?;
            self.read_recovery_budget(&mut tx, &attempt, state.tenant_id)
                .await
                .map_err(loop_error)
        })
    }

    fn load_model_observation(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<ModelInvocationObservation, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            // Use the original Conversation lane to get a single fenced prefix.
            let (_transaction, state) = self
                .load_state_in_commit_lane(&attempt)
                .await
                .map_err(loop_error)?;
            if !matches!(state.checkpoint.phase, LoopPhase::ModelInFlight { invocation_id: current } if current == invocation_id)
            {
                return Err(loop_error(
                    PostgresAgentLoopJournalError::CheckpointConflict,
                ));
            }
            Ok(state.model_observation(invocation_id))
        })
    }

    fn load_checkpoint(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.load_checkpoint_value(&attempt)
                .await
                .map_err(loop_error)
        })
    }

    fn recover_checkpoint(
        &self,
        attempt: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
    ) -> ApplicationFuture<'_, Result<RecoveredLoopState, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.recover_state(&attempt, checkpoint, constraints, None)
                .await
                .map_err(loop_error)
        })
    }

    fn recover_checkpoint_with_provider<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
        provider: &'a dyn musteragent_runtime_model::ModelProvider,
    ) -> ApplicationFuture<'a, Result<RecoveredLoopState, LoopPortError>> {
        Box::pin(async move {
            self.recover_state(attempt, checkpoint, constraints, Some(provider))
                .await
                .map_err(loop_error)
        })
    }

    fn append_recovered_model_events<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        recovery: &'a musteragent_loop::RecoverableModelInvocation,
        events: Vec<ModelStreamEvent>,
    ) -> ApplicationFuture<'a, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            self.append_provider_recovery(attempt, recovery, events)
                .await
                .map_err(loop_error)
        })
    }

    fn close_model_recovery<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation: InvocationId,
    ) -> ApplicationFuture<'a, Result<(), LoopPortError>> {
        Box::pin(async move {
            let state = self.load_state(attempt).await.map_err(loop_error)?;
            let journal = self
                .model_liveness(&state)
                .map_err(loop_error)?
                .ok_or_else(|| loop_error(PostgresAgentLoopJournalError::CorruptState))?;
            journal
                .recover_orphaned_model(attempt, invocation)
                .await
                .map_err(|e| loop_error(e.into()))
        })
    }

    fn begin_model(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        recovery_capability: ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        let iteration = iteration.clone();
        Box::pin(async move {
            self.begin_model_transition(&attempt, &iteration, recovery_capability)
                .await
                .map_err(loop_error)
        })
    }

    fn mark_model_dispatched(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.mark_model_dispatched_transition(&attempt, invocation_id)
                .await
                .map_err(loop_error)
        })
    }

    fn append_model_events(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.append_model_batch(&attempt, batch)
                .await
                .map_err(loop_error)
        })
    }

    fn stage_model_completion(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.stage_model_completion_intent(&attempt, &completion)
                .await
                .map_err(loop_error)
        })
    }

    fn finish_model(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.finish_model_transition(&attempt, completion)
                .await
                .map_err(loop_error)
        })
    }

    fn finish_failed_model(
        &self,
        attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.fail_inflight_model(&attempt, commit)
                .await
                .map_err(loop_error)
        })
    }

    fn validate_public_delivery(
        &self,
        attempt: &AttemptContext,
        completion: &ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<Option<ModelActionRejection>, LoopPortError>> {
        let attempt = attempt.clone();
        let completion = completion.clone();
        Box::pin(async move {
            self.validate_dialogue_completion(&attempt, &completion)
                .await
                .map_err(loop_error)
        })
    }

    fn finish_rejected_model_action(
        &self,
        attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.reject_inflight_model_action(&attempt, commit)
                .await
                .map_err(loop_error)
        })
    }

    fn commit_tool_result(
        &self,
        attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move { self.commit_tool(&attempt, result).await.map_err(loop_error) })
    }

    fn wait_for_reconciliation(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        observed_at: musteragent_identity::UtcTimestamp,
        resume_not_before: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            let journal =
                crate::tool_commit::PostgresFencedToolInvocationJournal::with_security_audit(
                    self.pool.clone(),
                    Arc::clone(&self.encoder),
                    Arc::clone(&self.decoder),
                    Arc::clone(&self.reference_keys),
                    Arc::clone(&self.audit),
                );
            musteragent_runtime_capability::FencedToolInvocationJournal::wait_for_reconciliation(
                &journal,
                &attempt,
                musteragent_runtime_capability::ReconciliationWaitRequest {
                    invocation_id,
                    observed_at,
                    resume_not_before,
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| LoopPortError {
                code: error.code,
                retryable: error.retryable,
            })
        })
    }

    fn completion_readiness(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RunCompletionReadiness, LoopPortError>>
    {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.native_completion_readiness(&attempt)
                .await
                .map_err(loop_error)
        })
    }

    fn complete(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RunCompletionDisposition, LoopPortError>>
    {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.drain_public_deliveries(&attempt)
                .await
                .map_err(loop_error)?;
            match self.finish_run(&attempt, TerminalMutation::Completed).await {
                Ok(checkpoint) if checkpoint.phase == LoopPhase::Cancelled => Ok(
                    musteragent_loop::RunCompletionDisposition::Cancelled(checkpoint),
                ),
                Ok(checkpoint) => Ok(musteragent_loop::RunCompletionDisposition::Completed(
                    checkpoint,
                )),
                Err(PostgresAgentLoopJournalError::PendingRunControls) => {
                    Ok(musteragent_loop::RunCompletionDisposition::ControlsPending)
                }
                Err(PostgresAgentLoopJournalError::PendingDelegation) => {
                    self.wait_for_delegation(&attempt).await.map_err(loop_error)
                }
                Err(PostgresAgentLoopJournalError::PendingRequiredWork) => {
                    Ok(musteragent_loop::RunCompletionDisposition::RequiredWorkPending)
                }
                Err(error) => Err(loop_error(error)),
            }
        })
    }

    fn fail(
        &self,
        attempt: &AttemptContext,
        code: ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            let recoverable = (ModelFailure { code, retryable }).permits_new_attempt();
            let persistent = matches!(
                recovery_policy,
                musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
            );
            if recoverable || persistent {
                let state = self.load_state(&attempt).await.map_err(loop_error)?;
                if let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase {
                    let budget = self.recovery_budget(&attempt).await?;
                    let observation = state.model_observation(invocation_id);
                    let failure = ModelFailure { code, retryable };
                    let resolution = if failure.permits_new_attempt()
                        && recovery_policy
                            .permits_sealed_continuation(observation.replay_barrier_observed)
                        && budget.permits_failure_retry(recovery_policy, failure)
                    {
                        ModelFailureResolution::RetryNewAttempt
                    } else {
                        ModelFailureResolution::stop(recovery_policy, failure)
                    };
                    return self
                        .fail_inflight_model(
                            &attempt,
                            ModelFailureCommit {
                                recovery_policy,
                                observation,
                                provider_failure: failure,
                                retry_after_ms,
                                resolution,
                            },
                        )
                        .await
                        .map_err(loop_error);
                }
                if (ModelFailure { code, retryable }).permits_dependency_recovery() {
                    return self
                        .schedule_retry(&attempt, &state, code, retry_after_ms, recovery_policy)
                        .await
                        .map_err(loop_error);
                }
            }
            self.finish_run(
                &attempt,
                TerminalMutation::Failed {
                    code,
                    retryable: false,
                    recovery_limit: None,
                },
            )
            .await
            .map_err(loop_error)
        })
    }

    fn interrupt_external_outcome(
        &self,
        attempt: &AttemptContext,
        reconciliation_ref: OpaqueExternalRef,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.interrupt_for_external_outcome(&attempt, reconciliation_ref)
                .await
                .map_err(loop_error)
        })
    }

    fn acknowledge_coordination_interrupt(
        &self,
        attempt: &AttemptContext,
        _observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.finish_run(&attempt, TerminalMutation::InterruptedCoordination)
                .await
                .map_err(loop_error)
        })
    }

    fn acknowledge_cancellation(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.recover_model_liveness(&attempt)
                .await
                .map_err(loop_error)?;
            let state = self.load_state(&attempt).await.map_err(loop_error)?;
            if let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase {
                return self
                    .cancel_inflight_model(&attempt, invocation_id, observation)
                    .await
                    .map_err(loop_error);
            }
            self.finish_run(&attempt, TerminalMutation::Cancelled)
                .await
                .map_err(|error| {
                    tracing::error!(
                        run_id = %attempt.run_id,
                        attempt_id = %attempt.attempt_id,
                        error = ?error,
                        "cancellation terminal transaction failed",
                    );
                    loop_error(error)
                })
        })
    }
}
