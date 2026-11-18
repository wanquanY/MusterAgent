//! Auxiliary model rows, usage and supervision commit in the original lane.
use super::*;
use musteragent_execution_liveness::Event;
use musteragent_run::InvocationContext;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionOperationKind, execution_operation_identity,
};

impl PostgresFencedExecutionLivenessJournal {
    pub(crate) async fn start_auxiliary(
        &self,
        mut tx: GovernedPostgresTransaction<'static>,
        state: &CommitLaneState,
        request: &musteragent_runtime_model::SupervisedModelAdmission,
    ) -> Result<ExecutionRecord, LivenessJournalError> {
        async {
            let context = &request.context;
            let request_hash = request.admission.invocation.request_hash;
            let owner = &context.attempt_context;
            let (snapshot, hash) = admission::snapshot(self, &mut tx, state, owner).await?;
            request
                .validate_snapshot(&snapshot)
                .map_err(|_| StoreError::Invalid)?;
            let scope = ExecutionScope::Invocation(context.invocation_id);
            let intent = ExecutionAdmission {
                binding: ExecutionBinding {
                    tenant_id: state.tenant_id,
                    run_id: owner.run_id,
                    scope,
                    owner_attempt_id: owner.attempt_id,
                    owner_fence: owner.fencing_token,
                    producer_generation: std::num::NonZeroU64::MIN,
                    operation_identity: execution_operation_identity(
                        state.tenant_id,
                        owner.run_id,
                        scope,
                        ExecutionOperationKind::AuxiliaryModel,
                        hash,
                        request_hash,
                    ),
                },
                policy: Arc::new(
                    snapshot
                        .execution_policy
                        .liveness
                        .ok_or(StoreError::Invalid)?
                        .model_policy(),
                ),
                business_deadline: None,
            };
            admission::validate_initial(state, owner, &intent)?;
            if let Some(existing) =
                storage::load(&mut tx, state.tenant_id, owner.run_id, scope).await?
            {
                if existing.binding() != intent.binding
                    || existing.policy() != intent.policy.as_ref()
                {
                    return Err(StoreError::Conflict);
                }
                tx.commit().await?;
                return Ok(existing);
            }
            admission::validate(self, &mut tx, state, owner, &intent).await?;
            let record: ExecutionRecord = intent
                .into_checkpoint(self.clock.sample().map_err(StoreError::Rejected)?)
                .map_err(|_| StoreError::Invalid)?
                .into();
            self.commit(tx, state, owner, None, &record).await?;
            Ok(record)
        }
        .await
        .map_err(StoreError::port)
    }

    pub(crate) async fn validate_auxiliary_completion(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        completion: &musteragent_runtime_model::SupervisedModelCompletion,
    ) -> Result<ExecutionRecord, LivenessJournalError> {
        async {
            let context = &completion.context;
            let scope = ExecutionScope::Invocation(context.invocation_id);
            let record = storage::load(tx, state.tenant_id, context.attempt_context.run_id, scope)
                .await?
                .ok_or(StoreError::Invalid)?;
            validate_owner(record.binding(), state, &context.attempt_context)?;
            let source = admission::operation(tx, record.binding()).await?;
            let (snapshot, _) =
                admission::snapshot(self, tx, state, &context.attempt_context).await?;
            completion
                .validate_snapshot(&snapshot)
                .map_err(|_| StoreError::Invalid)?;
            if source.kind != ExecutionOperationKind::AuxiliaryModel
                || source.request_hash != completion.completion.invocation.request_hash
            {
                return Err(StoreError::Invalid);
            }
            completion
                .validate_record(&record)
                .map_err(StoreError::Rejected)?;
            Ok(record)
        }
        .await
        .map_err(StoreError::port)
    }

    pub(crate) async fn finish_auxiliary(
        &self,
        tx: GovernedPostgresTransaction<'static>,
        state: &CommitLaneState,
        context: &InvocationContext,
        prior: ExecutionRecord,
    ) -> Result<(), LivenessJournalError> {
        async {
            if prior.is_closed() {
                tx.commit().await?;
                return Ok(());
            }
            let next = prior
                .reduce(
                    prior.revision(),
                    RecordMutation::Event(Event::TerminalObserved),
                    self.clock.sample().map_err(StoreError::Rejected)?,
                )
                .map_err(StoreError::Rejected)?;
            self.commit(
                tx,
                state,
                &context.attempt_context,
                Some(&prior),
                &next.next,
            )
            .await
        }
        .await
        .map_err(StoreError::port)
    }
}

/// Original native end or positive cleanup proof is required before a new
/// physical Compression generation can follow a failed call.
pub(crate) async fn auxiliary_stop_confirmed(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: musteragent_identity::RunId,
    invocation: musteragent_identity::InvocationId,
) -> Result<bool, StoreError> {
    let record = storage::load(tx, tenant, run, ExecutionScope::Invocation(invocation)).await?;
    Ok(
        matches!(record, Some(ExecutionRecord::Checkpoint(checkpoint)) if matches!(checkpoint.state().phase(), musteragent_execution_liveness::Phase::Closed(musteragent_execution_liveness::Closure::TerminalObserved | musteragent_execution_liveness::Closure::StopConfirmed(_)))),
    )
}
