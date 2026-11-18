//! Original Tool journal completion for account-domain effects. The caller
//! holds the commit lane and stages its domain changes in this same UnitOfWork.
//! No independent effect queue, receipt table or retry state lives here.
use super::*;
use musteragent_identity::{Actor, DottedKey, InvocationId, RunId};
use musteragent_run::RunContext;

pub(crate) struct AtomicEffectCommitter<'a> {
    pub ledger: &'a PostgresLedgerStore,
    pub encoder: &'a dyn LedgerPayloadEncoder,
    pub keys: &'a OpaqueRefKeyring,
    pub audit: &'a crate::PostgresSecurityAuditPipeline,
    pub clock: Option<&'a dyn musteragent_execution_liveness::LivenessHomeClock>,
}
pub(crate) struct AtomicEffectIdentity<'a> {
    pub actor: Actor,
    pub run_context: RunContext,
    pub invocation_id: InvocationId,
    pub tool_call_id: &'a ToolCallId,
    pub tool_key: &'a str,
    pub request_ref: &'a ContentRef,
    pub occurred_at: UtcTimestamp,
    /// Preserve existing released record identities when migrating a caller.
    pub record_tag: &'a [u8],
    pub idempotency_prefix: &'a str,
}

pub(crate) async fn validate_live_state(
    tx: &mut PgConnection,
    attempt: &AttemptContext,
    state: CommitLaneState,
) -> Result<(), ToolExecutionPortError> {
    async fn validate(
        tx: &mut PgConnection,
        attempt: &AttemptContext,
        state: CommitLaneState,
    ) -> Result<(), ToolCommitError> {
        let now = database_now_ms(tx).await?;
        lock_and_validate_fence(
            tx,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        let cancelled: bool = sqlx::query_scalar("SELECT status<>2 OR cancel_requested_at_ms IS NOT NULL FROM runtime.runs WHERE run_id=$1")
            .bind(attempt.run_id.into_uuid()).fetch_one(tx).await?;
        if cancelled {
            return Err(ToolCommitError::Execution(
                PostgresExecutionError::StaleFence,
            ));
        }
        Ok(())
    }
    validate(tx, attempt, state)
        .await
        .map_err(ToolCommitError::port)
}

impl AtomicEffectCommitter<'_> {
    pub(crate) async fn commit(
        &self,
        mut tx: crate::GovernedPostgresTransaction<'static>,
        state: CommitLaneState,
        attempt: &AttemptContext,
        identity: AtomicEffectIdentity<'_>,
        result: CapabilityResult,
        authorized_content: Vec<ContentRef>,
    ) -> Result<CapabilityResult, ToolExecutionPortError> {
        if state.tenant_id != identity.actor.tenant_id
            || identity.run_context.tenant_id != state.tenant_id
            || identity.run_context.run_id != attempt.run_id
            || identity.run_context.conversation_id != state.conversation_id
            || identity.run_context.activity_id != state.activity_id
            || identity.run_context.home_epoch != state.home_epoch
        {
            return Err(ToolCommitError::IdempotencyConflict.port());
        }
        // Read-only content validation and staging use the original reference
        // registry, including any newly returned source/content capabilities.
        crate::artifact::authorize_result_content(&mut tx, state.tenant_id, &result)
            .await
            .map_err(|_| ToolCommitError::ContentUnavailable.port())?;
        for reference in &authorized_content {
            authorize_content(&mut tx, state.tenant_id, reference)
                .await
                .map_err(ToolCommitError::port)?;
        }
        let encoded = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Tool(
                ToolEvent::InvocationSucceeded {
                    invocation_id: identity.invocation_id,
                    result_ref: result.result_ref.clone(),
                    result_hash: result.result_hash,
                    provider_request_ref: None,
                    authorized_content,
                },
            )));
        let record = ProposedRecord {
            schema_version: DottedKey::new(RECORD_SCHEMA)
                .map_err(|_| ToolCommitError::Corrupt.port())?,
            record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                &identity.invocation_id.into_uuid(),
                identity.record_tag,
            ))
            .map_err(|_| ToolCommitError::Corrupt.port())?,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(state.activity_id),
            run_id: Some(attempt.run_id),
            run_seq: Some(
                RunSeq::new(
                    state
                        .next_sequence(1)
                        .map_err(|e| ToolCommitError::CommitLane(e).port())?,
                )
                .map_err(|_| ToolCommitError::Corrupt.port())?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id: Some(identity.run_context.executor_participant_id),
            producer: Producer::Worker,
            occurred_at: identity.occurred_at,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&encoded),
            payload: encoded,
        };
        let mut append = state
            .append(
                format!("{}:{}", identity.idempotency_prefix, identity.invocation_id),
                vec![record],
            )
            .map_err(|e| ToolCommitError::CommitLane(e).port())?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut tx,
            self.clock,
            self.encoder,
            attempt,
            identity.invocation_id,
            &mut append,
        )
        .await
        .map_err(|e| ToolCommitError::Liveness(e).port())?;
        validate_live_state(&mut tx, attempt, state).await?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(tx, &append)
            .await
            .map_err(|e| ToolCommitError::Ledger(e).port())?
        {
            BeginAppend::Ready(value) => value,
            BeginAppend::Duplicate(_) => {
                return if self.duplicate(&identity, &result).await? {
                    Ok(result)
                } else {
                    Err(ToolCommitError::IdempotencyConflict.port())
                };
            }
        };
        crate::artifact::complete_tool(
            prepared.transaction_mut(),
            self.keys,
            state.tenant_id,
            identity.invocation_id,
            &result,
            identity.occurred_at,
            true,
        )
        .await
        .map_err(|_| ToolCommitError::Corrupt.port())?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            self.audit,
            prepared.transaction_mut(),
            state.tenant_id,
            identity.invocation_id,
        )
        .await
        .map_err(|e| audit_error(e).port())?;
        if let Some(change) = liveness {
            change
                .persist(prepared.transaction_mut())
                .await
                .map_err(|e| ToolCommitError::Liveness(e).port())?;
        }
        let next = state
            .next_sequence(
                u64::try_from(append.records.len())
                    .map_err(|e| ToolCommitError::NumericConversion(e).port())?,
            )
            .map_err(|e| ToolCommitError::CommitLane(e).port())?;
        advance_running_sequence(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            next,
        )
        .await
        .map_err(ToolCommitError::port)?;
        prepared
            .commit(&append)
            .await
            .map_err(|e| ToolCommitError::Ledger(e).port())?;
        Ok(result)
    }

    async fn duplicate(
        &self,
        identity: &AtomicEffectIdentity<'_>,
        result: &CapabilityResult,
    ) -> Result<bool, ToolExecutionPortError> {
        async fn lookup(
            committer: &AtomicEffectCommitter<'_>,
            identity: &AtomicEffectIdentity<'_>,
            result: &CapabilityResult,
        ) -> Result<bool, ToolCommitError> {
            let row = sqlx::query("SELECT status,result_content_id,tool_call_id,run_id,request_content_id,tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2")
                .bind(identity.actor.tenant_id.into_uuid()).bind(identity.invocation_id.into_uuid()).fetch_optional(committer.ledger.database().pool()).await?.ok_or(ToolCommitError::NotFound)?;
            if row.try_get::<String, _>("tool_call_id")? != identity.tool_call_id.as_str()
                || row.try_get::<uuid::Uuid, _>("run_id")?
                    != identity.run_context.run_id.into_uuid()
                || row.try_get::<uuid::Uuid, _>("request_content_id")?
                    != identity.request_ref.content_id.into_uuid()
                || row.try_get::<String, _>("tool_ref")? != identity.tool_key
            {
                return Err(ToolCommitError::IdempotencyConflict);
            }
            if row.try_get::<i16, _>("status")? != 4 {
                return Ok(false);
            }
            if row.try_get::<Option<uuid::Uuid>, _>("result_content_id")?
                != Some(result.result_ref.content_id.into_uuid())
            {
                return Err(ToolCommitError::IdempotencyConflict);
            }
            crate::tool_security_audit::replay_succeeded(
                committer.audit,
                committer.ledger.database().pool(),
                identity.actor.tenant_id,
                identity.invocation_id,
            )
            .await
            .map_err(audit_error)?;
            Ok(true)
        }
        lookup(self, identity, result)
            .await
            .map_err(ToolCommitError::port)
    }
}
async fn advance_running_sequence(
    tx: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    sequence: u64,
) -> Result<(), ToolCommitError> {
    let changed = sqlx::query("UPDATE runtime.runs SET last_run_sequence=$3::bigint,run_version=run_version+1 WHERE tenant_id=$1 AND run_id=$2 AND status=2")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(sequence.to_string()).execute(tx).await?;
    if changed.rows_affected() != 1 {
        return Err(ToolCommitError::Execution(
            PostgresExecutionError::StaleFence,
        ));
    }
    Ok(())
}

fn audit_error(error: crate::tool_security_audit::AtomicToolSecurityAuditError) -> ToolCommitError {
    use crate::tool_security_audit::AtomicToolSecurityAuditError;
    match error {
        AtomicToolSecurityAuditError::MissingTerminal => ToolCommitError::Corrupt,
        AtomicToolSecurityAuditError::Database(error) => ToolCommitError::Database(error),
        AtomicToolSecurityAuditError::Audit(error) => ToolCommitError::Audit(error),
    }
}
