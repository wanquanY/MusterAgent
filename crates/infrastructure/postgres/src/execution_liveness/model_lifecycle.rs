//! Model phase staging inside the existing Agent Loop transaction.
use super::*;
use musteragent_execution_liveness::{Closure, Event, LivenessError, Phase};
use musteragent_identity::InvocationId;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionOperationKind, execution_operation_identity,
};

impl PostgresFencedExecutionLivenessJournal {
    /// Reconcile a physical invocation left open by a former Attempt. Current
    /// live owners still require their real stop/terminal boundary. Recovery
    /// tickets and cleanup deadlines survive another owner or process loss.
    pub(crate) async fn recover_orphaned_model(
        &self,
        owner: &AttemptContext,
        invocation: InvocationId,
    ) -> Result<(), LivenessJournalError> {
        let scope = ExecutionScope::Invocation(invocation);
        let record = self
            .load(owner, scope)
            .await?
            .ok_or_else(|| StoreError::Corrupt.port())?;
        if record.is_closed() {
            return Ok(());
        }
        musteragent_loop::close_unobserved_execution(
            self,
            self.clock.as_ref(),
            owner,
            scope,
            record.policy(),
        )
        .await
        .map_err(musteragent_loop::ExecutionSupervisionError::into_journal_error)
    }

    /// A canonical terminal from the original idempotent operation is positive
    /// result proof, never fresh activity. This change joins its output batch.
    pub(crate) async fn prepare_recovered_model_terminal(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        recovery: &musteragent_loop::RecoverableModelInvocation,
    ) -> Result<LifecycleChange, LivenessJournalError> {
        async {
            let scope = ExecutionScope::Invocation(recovery.iteration.invocation_id);
            let prior = storage::load(tx, state.tenant_id, owner.run_id, scope)
                .await?
                .ok_or(StoreError::Corrupt)?;
            validate_owner(prior.binding(), state, owner)?;
            if prior.binding() != recovery.binding
                || admission::operation(tx, prior.binding()).await?.kind
                    != ExecutionOperationKind::Model
            {
                return Err(StoreError::Invalid);
            }
            let next = prior
                .reduce(
                    prior.revision(),
                    RecordMutation::ConfirmRecoveryTerminal {
                        binding: recovery.binding,
                        nonce: recovery.nonce,
                    },
                    self.clock.sample().map_err(StoreError::Rejected)?,
                )
                .map_err(StoreError::Rejected)?
                .next;
            Ok(LifecycleChange {
                prior: Some(prior),
                next,
            })
        }
        .await
        .map_err(StoreError::port)
    }

    /// Adopt an already prepared canonical terminal, not new physical output.
    /// The caller holds the current Run lane and has verified the exact protected
    /// Content. The original producer and terminal receipt must still agree.
    pub(crate) async fn validate_completion_link(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: InvocationId,
        content: musteragent_identity::ContentId,
    ) -> Result<(), LivenessJournalError> {
        async {
            let record = storage::load(
                tx,
                state.tenant_id,
                owner.run_id,
                ExecutionScope::Invocation(invocation),
            )
            .await?
            .ok_or(StoreError::Corrupt)?;
            validate_retained_owner(record.binding(), state, owner)?;
            if admission::operation(tx, record.binding()).await?.kind
                != ExecutionOperationKind::Model
            {
                return Err(StoreError::Invalid);
            }
            let exact: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM runtime.model_completion_intents i
                    JOIN runtime.model_invocations m USING(invocation_id)
                    WHERE i.invocation_id=$1 AND i.content_id=$2
                      AND i.tenant_id=$3 AND i.run_id=$4
                      AND i.attempt_id=m.attempt_id AND m.tenant_id=i.tenant_id
                      AND m.run_id=i.run_id AND i.lifecycle IN (2,3)
                      AND m.status=1 AND m.terminal_stop_reason IS NOT NULL
                )",
            )
            .bind(invocation.into_uuid())
            .bind(content.into_uuid())
            .bind(state.tenant_id.into_uuid())
            .bind(owner.run_id.into_uuid())
            .fetch_one(&mut *tx)
            .await?;
            if !exact {
                return Err(StoreError::Invalid);
            }
            let eligible = matches!(
                &record,
                ExecutionRecord::Checkpoint(checkpoint)
                    if matches!(checkpoint.state().phase(),
                        Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. }
                            | Phase::Closed(Closure::TerminalObserved)
                            | Phase::Closed(Closure::StopConfirmed(
                                musteragent_execution_liveness::StopReason::SystemShutdown
                            ))
                    )
            );
            if !eligible {
                return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
            }
            Ok(())
        }
        .await
        .map_err(StoreError::port)
    }

    /// Result eligibility is fenced by the committed stop decision in the
    /// same original lane. Reading a PID or retaining an old dispatch receipt
    /// cannot reopen a stopped model's public output or provider dispatch.
    pub(crate) async fn validate_model_activity(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: InvocationId,
    ) -> Result<(), LivenessJournalError> {
        async {
            let record = storage::load(
                tx,
                state.tenant_id,
                owner.run_id,
                ExecutionScope::Invocation(invocation),
            )
            .await?
            .ok_or(StoreError::Corrupt)?;
            validate_owner(record.binding(), state, owner)?;
            let permitted = matches!(&record, ExecutionRecord::Checkpoint(c)
                if matches!(c.state().phase(), Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. }));
            if !permitted {
                return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
            }
            if admission::operation(tx, record.binding()).await?.kind
                != ExecutionOperationKind::Model
            {
                return Err(StoreError::Invalid);
            }
            Ok(())
        }
        .await
        .map_err(StoreError::port)
    }

    pub(crate) async fn prepare_model_start(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        context: &musteragent_run::InvocationContext,
        request_hash: Sha256,
    ) -> Result<LifecycleChange, LivenessJournalError> {
        async {
            let (profile, snapshot_hash) = admission::run_profile(self, tx, state, owner).await?;
            let policy = profile.model_policy();
            if !matches!(&context.timing,
                musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(timing)
                if timing.policy == *policy.reference() && timing.business_deadline.is_none()) {
                return Err(StoreError::Invalid);
            }
            let scope = ExecutionScope::Invocation(context.invocation_id);
            let admission = ExecutionAdmission {
                binding: ExecutionBinding {
                    tenant_id: state.tenant_id,
                    run_id: owner.run_id,
                    scope,
                    operation_identity: execution_operation_identity(
                        state.tenant_id,
                        owner.run_id,
                        scope,
                        ExecutionOperationKind::Model,
                        snapshot_hash,
                        request_hash,
                    ),
                    owner_attempt_id: owner.attempt_id,
                    owner_fence: owner.fencing_token,
                    producer_generation: std::num::NonZeroU64::MIN,
                },
                policy: Arc::new(policy),
                business_deadline: None,
            };
            admission::validate_initial(state, owner, &admission)?;
            if storage::load(tx, state.tenant_id, owner.run_id, scope)
                .await?
                .is_some()
            {
                // Retries must resolve the original Model begin receipt; this
                // API cannot replace a live/stopped/recovering physical scope.
                return Err(StoreError::Conflict);
            }
            let now = self.clock.sample().map_err(StoreError::Rejected)?;
            let next = admission
                .into_checkpoint(now)
                .map_err(StoreError::Rejected)?
                .into();
            Ok(LifecycleChange { prior: None, next })
        }
        .await
        .map_err(StoreError::port)
    }

    /// This is a validation of the original closed scope, never a new terminal
    /// transition. Unknown physical outcome remains unknown after local sealing.
    pub(crate) async fn require_unconfirmed_model_stop(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: InvocationId,
    ) -> Result<musteragent_runtime_contract::OpaqueExternalRef, LivenessJournalError> {
        async {
            let record = storage::load(
                tx,
                state.tenant_id,
                owner.run_id,
                ExecutionScope::Invocation(invocation),
            )
            .await?
            .ok_or(StoreError::Corrupt)?;
            validate_retained_owner(record.binding(), state, owner)?;
            if admission::operation(tx, record.binding()).await?.kind
                != ExecutionOperationKind::Model
                || !matches!(&record, ExecutionRecord::Checkpoint(c)
                    if matches!(c.state().phase(), Phase::Closed(Closure::StopUnconfirmed(_))))
            {
                return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
            }
            musteragent_runtime_contract::OpaqueExternalRef::new(format!(
                "model-invocation:{invocation}:generation:{}:outcome-unknown",
                record.binding().producer_generation
            ))
            .map_err(|_| StoreError::Invalid)
        }
        .await
        .map_err(StoreError::port)
    }

    pub(crate) async fn prepare_model_terminal(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: InvocationId,
        allow_failed_observation: bool,
    ) -> Result<Option<LifecycleChange>, LivenessJournalError> {
        async {
            let scope = ExecutionScope::Invocation(invocation);
            let prior = storage::load(tx, state.tenant_id, owner.run_id, scope)
                .await?
                .ok_or(StoreError::Corrupt)?;
            let source = admission::operation(tx, prior.binding()).await?;
            if source.kind != ExecutionOperationKind::Model {
                return Err(StoreError::Invalid);
            }
            if let ExecutionRecord::Checkpoint(checkpoint) = &prior
                && let Phase::Closed(closure) = checkpoint.state().phase()
            {
                validate_retained_owner(prior.binding(), state, owner)?;
                let sealed_result: bool = sqlx::query_scalar(
                    "SELECT (completion_intent_content_id IS NOT NULL AND terminal_stop_reason IS NOT NULL)
                     FROM runtime.model_invocations WHERE invocation_id = $1",
                ).bind(invocation.into_uuid()).fetch_one(&mut *tx).await?;
                if (closure == Closure::TerminalObserved && (source.terminal || sealed_result))
                    || (sealed_result && closure == Closure::StopConfirmed(musteragent_execution_liveness::StopReason::SystemShutdown))
                    || (allow_failed_observation
                        && matches!(
                            closure,
                            Closure::StopConfirmed(_) | Closure::StopUnconfirmed(_)
                        ))
                {
                    // Close only the logical call. In particular an unknown
                    // physical stop stays unknown in its original record; it
                    // does not prevent model recovery in the same Agent Run.
                    return Ok(None);
                }
                return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
            }
            validate_retained_owner(prior.binding(), state, owner)?;
            // A local error, EOF or Run cancellation is not proof that a
            // dispatched provider stopped. Only an original canonical terminal
            // or a never-dispatched marker can close an active model scope.
            let physical_terminal: bool = sqlx::query_scalar(
                "SELECT (provider_dispatched_at_ms IS NULL OR terminal_stop_reason IS NOT NULL)
                 FROM runtime.model_invocations WHERE invocation_id = $1",
            )
            .bind(invocation.into_uuid())
            .fetch_one(&mut *tx)
            .await?;
            if !physical_terminal {
                return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
            }
            let now = self.clock.sample().map_err(StoreError::Rejected)?;
            // Positive durable evidence can cross owner/clock epochs without
            // pretending that the original process is still observing work.
            let mut rebound = prior.binding();
            rebound.owner_attempt_id = owner.attempt_id;
            rebound.owner_fence = owner.fencing_token;
            let next = if prior.binding() != rebound {
                let nonce = Sha256::digest(InvocationId::generate().into_uuid().as_bytes());
                let recovering = prior.reduce(prior.revision(),
                    RecordMutation::BeginRecovery { rebound, nonce }, now)
                    .map_err(StoreError::Rejected)?.next;
                recovering.reduce(recovering.revision(),
                    RecordMutation::ConfirmRecoveryTerminal { binding: rebound, nonce }, now)
                    .map_err(StoreError::Rejected)?.next
            } else {
                prior.reduce(prior.revision(), RecordMutation::Event(Event::TerminalObserved), now)
                    .map_err(StoreError::Rejected)?.next
            };
            Ok(Some(LifecycleChange {
                prior: Some(prior),
                next,
            }))
        }
        .await
        .map_err(StoreError::port)
    }
}

/// The caller already holds the current Run commit lane. A closed invocation
/// is immutable evidence from a former owner, not permission to dispatch or
/// accept new output. Do not require its old Attempt to become current again.
fn validate_retained_owner(
    binding: ExecutionBinding,
    state: &CommitLaneState,
    owner: &AttemptContext,
) -> Result<(), StoreError> {
    if binding.tenant_id != state.tenant_id
        || binding.run_id != owner.run_id
        || binding.owner_fence.get() > owner.fencing_token.get()
        || (binding.owner_fence == owner.fencing_token
            && binding.owner_attempt_id != owner.attempt_id)
    {
        return Err(StoreError::Stale);
    }
    Ok(())
}
