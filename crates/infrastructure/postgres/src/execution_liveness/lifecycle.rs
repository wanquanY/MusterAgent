//! Staging for original invocation transactions. No commit, lease renewal,
//! Run sequence update or separate append may occur in this module.
use super::*;
use musteragent_execution_liveness::Event;
use musteragent_ledger::DomainEvent;
use musteragent_run::RunEvent;
use musteragent_tool::ToolInvocation;

pub(crate) struct LifecycleChange {
    pub(super) prior: Option<ExecutionRecord>,
    pub(super) next: ExecutionRecord,
}
impl LifecycleChange {
    pub(crate) fn event(&self) -> DomainEvent {
        DomainEvent::Run(RunEvent::ExecutionLivenessRecorded {
            record: self.next.clone(),
        })
    }
    /// Add a derived phase fact to the original owner's append. The original
    /// command fingerprint remains its replay identity across clock samples.
    pub(crate) fn append_to(
        &self,
        append: &mut musteragent_ledger::AppendRequest,
        encoder: &dyn LedgerPayloadEncoder,
    ) -> Result<(), LivenessJournalError> {
        use musteragent_identity::{LedgerRecordId, RunSeq};
        use musteragent_ledger::LedgerPayload;
        let result = (|| {
            let binding = self.next.binding();
            let (kind, invocation) = scope_parts(binding.scope);
            let last = append.records.last().ok_or(StoreError::Invalid)?;
            if append.tenant_id != binding.tenant_id
                || last.run_id != Some(binding.run_id)
                || last.attempt_id != Some(binding.owner_attempt_id)
            {
                return Err(StoreError::Invalid);
            }
            let sequence = last
                .run_seq
                .ok_or(StoreError::Invalid)?
                .get()
                .checked_add(1)
                .and_then(|value| RunSeq::new(value).ok())
                .ok_or(StoreError::Corrupt)?;
            let key = format!(
                "liveness:{}:{}:{}:{}",
                binding.run_id,
                kind,
                invocation,
                self.next.revision()
            );
            let payload = encoder.encode(LedgerPayload::DomainEvent(self.event()));
            let mut record = last.clone();
            record.record_kind = musteragent_event_envelope::LedgerRecordKind::DomainEvent;
            record.record_id = LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                key.as_bytes(),
            ))
            .map_err(|_| StoreError::Corrupt)?;
            record.run_seq = Some(sequence);
            record.participant_id = None;
            record.occurred_at = match &self.next {
                ExecutionRecord::Checkpoint(value) => value.saved_at().utc,
                ExecutionRecord::Recovering(value) => value.started_at().utc,
            };
            record.payload_hash = Sha256::digest(&payload);
            record.payload = payload;
            append.records.push(record);
            Ok(())
        })();
        result.map_err(StoreError::port)
    }
    pub(crate) async fn persist(
        &self,
        tx: &mut sqlx::PgConnection,
    ) -> Result<(), LivenessJournalError> {
        match &self.prior {
            Some(prior) => storage::update(tx, prior, &self.next).await,
            None => storage::insert(tx, &self.next).await,
        }
        .map_err(StoreError::port)
    }
}
impl PostgresFencedExecutionLivenessJournal {
    pub(crate) async fn validate_tool_intent(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        retry_contract: musteragent_tool::ToolRetryContract,
    ) -> Result<(), LivenessJournalError> {
        admission::tool_intent(self, tx, state, owner, invocation, retry_contract)
            .await
            .map(|_| ())
            .map_err(StoreError::port)
    }

    pub(crate) async fn prepare_tool_start(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        retry_contract: musteragent_tool::ToolRetryContract,
    ) -> Result<LifecycleChange, LivenessJournalError> {
        async {
            let admission =
                admission::tool_intent(self, tx, state, owner, invocation, retry_contract).await?;
            let prior =
                storage::load(tx, state.tenant_id, owner.run_id, admission.binding.scope).await?;
            let now = self.clock.sample().map_err(StoreError::Rejected)?;
            if let Some(prior) = prior {
                let ExecutionRecord::Checkpoint(checkpoint) = &prior else {
                    return Err(StoreError::Conflict);
                };
                let musteragent_execution_liveness::Phase::Waiting { binding: wait, .. } =
                    checkpoint.state().phase()
                else {
                    // A live/recovering execution cannot buy a new dispatch clock.
                    return Err(StoreError::Conflict);
                };
                if invocation.status != musteragent_tool::ToolInvocationStatus::WaitingInteraction
                    || prior.policy() != admission.policy.as_ref()
                    || prior.binding().operation_identity != admission.binding.operation_identity
                {
                    return Err(StoreError::Invalid);
                }
                let resume = RecordMutation::Event(Event::Resume(wait));
                transitions::validate(self, tx, state, owner, &prior, &resume).await?;
                let rebound = prior
                    .reduce(
                        prior.revision(),
                        RecordMutation::RebindPassive {
                            rebound: admission.binding,
                        },
                        now,
                    )
                    .map_err(StoreError::Rejected)?;
                if rebound.action != musteragent_execution_liveness::Action::None {
                    return Err(StoreError::Invalid);
                }
                let resumed = rebound
                    .next
                    .reduce(rebound.next.revision(), resume, now)
                    .map_err(StoreError::Rejected)?;
                if resumed.action != musteragent_execution_liveness::Action::None {
                    return Err(StoreError::Invalid);
                }
                return Ok(LifecycleChange {
                    prior: Some(prior),
                    next: resumed.next,
                });
            }
            let next = admission
                .into_checkpoint(now)
                .map_err(StoreError::Rejected)?
                .into();
            Ok(LifecycleChange { prior: None, next })
        }
        .await
        .map_err(StoreError::port)
    }

    /// Called only for a terminal proposal already validated by the original
    /// Tool owner. Its row, terminal event and this phase fact commit together.
    pub(crate) async fn prepare_tool_terminal(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        retry_contract: musteragent_tool::ToolRetryContract,
        outcome: &musteragent_runtime_capability::ToolTerminalOutcome,
    ) -> Result<Option<LifecycleChange>, LivenessJournalError> {
        async {
            let prior = storage::load(
                tx,
                state.tenant_id,
                owner.run_id,
                ExecutionScope::Invocation(invocation.invocation_id),
            )
            .await?;
            let Some(prior) = prior else {
                // A rejected/denied Tool can close without having dispatched.
                if invocation.execution_attempt_id.is_none() {
                    return Ok(None);
                }
                return Err(StoreError::Corrupt);
            };
            // A durable business wait can be rejected by a later Scheduler
            // Attempt. Transfer only passive custody under the exact original
            // Run::Waiting fact; no Tool start or new physical budget is granted.
            if invocation.status == musteragent_tool::ToolInvocationStatus::WaitingInteraction
                && matches!(
                    outcome,
                    musteragent_runtime_capability::ToolTerminalOutcome::Failed { .. }
                        | musteragent_runtime_capability::ToolTerminalOutcome::Cancelled(_)
                )
                && let ExecutionRecord::Checkpoint(checkpoint) = &prior
                && let musteragent_execution_liveness::Phase::Waiting { binding: wait, .. } =
                    checkpoint.state().phase()
            {
                let admission =
                    admission::retained_tool_intent(tx, state, owner, invocation, &prior).await?;
                if prior.policy() != admission.policy.as_ref()
                    || prior.binding().operation_identity != admission.binding.operation_identity
                {
                    return Err(StoreError::Invalid);
                }
                transitions::validate(
                    self,
                    tx,
                    state,
                    owner,
                    &prior,
                    &RecordMutation::Event(Event::Resume(wait)),
                )
                .await?;
                let now = self.clock.sample().map_err(StoreError::Rejected)?;
                let rebound = prior
                    .reduce(
                        prior.revision(),
                        RecordMutation::RebindPassive {
                            rebound: admission.binding,
                        },
                        now,
                    )
                    .map_err(StoreError::Rejected)?;
                if rebound.action != musteragent_execution_liveness::Action::None {
                    return Err(StoreError::Invalid);
                }
                let terminal = rebound
                    .next
                    .reduce(
                        rebound.next.revision(),
                        RecordMutation::Event(Event::TerminalObserved),
                        now,
                    )
                    .map_err(StoreError::Rejected)?;
                return Ok(Some(LifecycleChange {
                    prior: Some(prior),
                    next: terminal.next,
                }));
            }
            // A successor can recover a read result only under the original
            // bounded ticket. If observation has closed without a result, a
            // failed read may be recorded while preserving the physical closure.
            // Neither case creates a new startup or rewrites the old owner.
            if outcome.is_activity_read_recovery_candidate(invocation, retry_contract, owner)
                && (invocation.execution_attempt_id != Some(owner.attempt_id)
                    || outcome.is_unobserved_read_failure())
            {
                let admission =
                    admission::retained_tool_intent(tx, state, owner, invocation, &prior).await?;
                let binding = prior.binding();
                if binding.tenant_id != admission.binding.tenant_id
                    || binding.run_id != admission.binding.run_id
                    || binding.scope != admission.binding.scope
                    || binding.operation_identity != admission.binding.operation_identity
                    || binding.producer_generation != admission.binding.producer_generation
                    || prior.policy() != admission.policy.as_ref()
                    || (binding.owner_attempt_id == owner.attempt_id
                        && binding.owner_fence != owner.fencing_token)
                    || (binding.owner_attempt_id != owner.attempt_id
                        && binding.owner_fence >= owner.fencing_token)
                {
                    return Err(StoreError::Invalid);
                }
                if let ExecutionRecord::Checkpoint(checkpoint) = &prior {
                    if matches!(
                        checkpoint.state().phase(),
                        musteragent_execution_liveness::Phase::Closed(
                            musteragent_execution_liveness::Closure::StopConfirmed(_)
                                | musteragent_execution_liveness::Closure::StopUnconfirmed(_)
                        )
                    ) && outcome.is_unobserved_read_failure()
                    {
                        // This closes only the missing read result. In particular
                        // StopUnconfirmed remains StopUnconfirmed, across restarts.
                        return Ok(None);
                    }
                    if invocation.execution_attempt_id != Some(owner.attempt_id) {
                        return Err(StoreError::Rejected(
                            musteragent_execution_liveness::LivenessError::ReconciliationRequired,
                        ));
                    }
                }
            }
            validate_owner(prior.binding(), state, owner)?;
            if let ExecutionRecord::Checkpoint(checkpoint) = &prior
                && let musteragent_execution_liveness::Phase::Closed(closure) =
                    checkpoint.state().phase()
            {
                use musteragent_execution_liveness::Closure;
                use musteragent_runtime_capability::ToolTerminalOutcome;
                let permitted = match (closure, outcome) {
                    (Closure::TerminalObserved, _) => invocation.status.is_terminal(),
                    (
                        Closure::StopConfirmed(_),
                        ToolTerminalOutcome::Failed { .. } | ToolTerminalOutcome::Cancelled(_),
                    ) => true,
                    _ => false,
                };
                if !permitted {
                    return Err(StoreError::Rejected(
                        musteragent_execution_liveness::LivenessError::ReconciliationRequired,
                    ));
                }
                return Ok(None);
            }
            let now = self.clock.sample().map_err(StoreError::Rejected)?;
            let mutation = match &prior {
                ExecutionRecord::Recovering(ticket) => RecordMutation::ConfirmRecoveryTerminal {
                    binding: ticket.binding(),
                    nonce: ticket.nonce(),
                },
                ExecutionRecord::Checkpoint(_) => RecordMutation::Event(Event::TerminalObserved),
            };
            let transition = prior
                .reduce(prior.revision(), mutation, now)
                .map_err(StoreError::Rejected)?;
            Ok(Some(LifecycleChange {
                prior: Some(prior),
                next: transition.next,
            }))
        }
        .await
        .map_err(StoreError::port)
    }
    /// The supplied binding identifies the exact canonical Run::Waiting fact
    /// staged by the original Tool/Interaction owner in this same transaction.
    pub(crate) async fn prepare_tool_wait(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        wait: musteragent_runtime_contract::execution_liveness::DurableWaitBinding,
    ) -> Result<Option<LifecycleChange>, LivenessJournalError> {
        async {
            let prior = storage::load(
                tx,
                state.tenant_id,
                owner.run_id,
                ExecutionScope::Invocation(invocation.invocation_id),
            )
            .await?;
            let Some(prior) = prior else {
                if invocation.execution_attempt_id.is_none() {
                    return Ok(None);
                }
                return Err(StoreError::Corrupt);
            };
            validate_owner(prior.binding(), state, owner)?;
            let now = self.clock.sample().map_err(StoreError::Rejected)?;
            let changed = prior
                .reduce(
                    prior.revision(),
                    RecordMutation::Event(Event::EnterWait(wait)),
                    now,
                )
                .map_err(StoreError::Rejected)?;
            // An expired scope must stop under its current owner; a wait cannot
            // release that owner and silently discard the committed stop action.
            if changed.action != musteragent_execution_liveness::Action::None {
                return Err(StoreError::Rejected(
                    musteragent_execution_liveness::LivenessError::ReconciliationRequired,
                ));
            }
            Ok(Some(LifecycleChange {
                prior: Some(prior),
                next: changed.next,
            }))
        }
        .await
        .map_err(StoreError::port)
    }
}
