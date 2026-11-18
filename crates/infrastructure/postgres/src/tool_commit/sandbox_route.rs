//! Immutable route facts share the original Invocation transaction and Ledger.
use super::*;
use musteragent_sandbox::{
    SandboxCompatibility, SandboxExecutor, SandboxRouteDecision, SandboxRouteReason,
    SandboxRouteRecord,
};

pub(super) fn decode(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<SandboxRouteRecord>, ToolCommitError> {
    match (
        row.try_get::<Option<String>, _>("sandbox_executor")?,
        row.try_get::<Option<String>, _>("sandbox_reason")?,
        row.try_get::<Option<String>, _>("sandbox_compatibility")?,
        row.try_get::<Option<i64>, _>("sandbox_decided_at_ms")?,
        row.try_get::<Option<Vec<u8>>, _>("sandbox_route_hash")?,
    ) {
        (None, None, None, None, None) => Ok(None),
        (Some(executor), Some(reason), Some(compatibility), Some(at), Some(hash)) => {
            let bad = |_| ToolCommitError::Corrupt;
            let record = SandboxRouteRecord {
                decision: SandboxRouteDecision {
                    executor: SandboxExecutor::parse(&executor).map_err(bad)?,
                    reason: SandboxRouteReason::parse(&reason).map_err(bad)?,
                    compatibility: SandboxCompatibility::parse(&compatibility).map_err(bad)?,
                },
                decided_at: UtcTimestamp::from_unix_millis(at),
            };
            record.validate().map_err(bad)?;
            if Sha256::digest(record.canonical_bytes())
                .into_bytes()
                .as_slice()
                != hash.as_slice()
            {
                return Err(ToolCommitError::Corrupt);
            }
            Ok(Some(record))
        }
        _ => Err(ToolCommitError::Corrupt),
    }
}

impl PostgresFencedToolInvocationJournal {
    /// Persist exactly one routing decision beside its Ledger/Outbox fact.
    pub async fn commit_sandbox_route(
        &self,
        attempt: &AttemptContext,
        commit: musteragent_runtime_capability::SandboxRouteCommit,
    ) -> Result<DurableToolInvocation, musteragent_runtime_capability::BrowserProviderRuntimeError>
    {
        self.commit_sandbox_route_inner(attempt, commit)
            .await
            .map_err(route_error)
    }

    async fn commit_sandbox_route_inner(
        &self,
        attempt: &AttemptContext,
        commit: musteragent_runtime_capability::SandboxRouteCommit,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        commit
            .route
            .validate()
            .map_err(|_| ToolCommitError::Corrupt)?;
        let (mut tx, state) = self.begin_commit_lane(attempt).await?;
        crate::commit_lane::validate_active_fence(&mut tx, attempt, state).await?;
        let existing = self
            .load_invocation_on(&mut tx, state.tenant_id, commit.invocation_id)
            .await?
            .ok_or(ToolCommitError::NotFound)?;
        if existing.invocation.run_id != attempt.run_id
            || existing.invocation.execution_attempt_id != Some(attempt.attempt_id)
            || existing.invocation.status != ToolInvocationStatus::Running
        {
            return Err(ToolCommitError::InvalidTransition);
        }
        if let Some(record) = existing.sandbox_route {
            if record != commit.route {
                return Err(ToolCommitError::IdempotencyConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let mut append = self.append_for_event(
            &state,
            attempt,
            commit.invocation_id,
            "sandbox-route",
            commit.route.decided_at,
            DomainEvent::Tool(commit.event()),
        )?;
        let record = &append.records[0];
        append.outbox.push(musteragent_ledger::ProposedOutbox {
            message_id: musteragent_identity::OutboxMessageId::from_uuid(
                record.record_id.into_uuid(),
            )
            .map_err(|_| ToolCommitError::Corrupt)?,
            topic: musteragent_identity::DottedKey::new("musteragent.tool.sandbox-routed")
                .map_err(|_| ToolCommitError::Corrupt)?,
            payload: record.payload.clone(),
            payload_hash: record.payload_hash,
        });
        let mut prepared = match self.ledger.begin_append_in_transaction(tx, &append).await? {
            // An event without its row is corruption, never an invitation to repair a fact.
            BeginAppend::Duplicate(_) => return Err(ToolCommitError::Corrupt),
            BeginAppend::Ready(value) => value,
        };
        crate::commit_lane::validate_active_fence(prepared.transaction_mut(), attempt, state)
            .await?;
        let changed=sqlx::query("UPDATE tool.tool_invocations SET sandbox_executor=$3,sandbox_reason=$4,sandbox_compatibility=$5,sandbox_decided_at_ms=$6,sandbox_route_hash=$7 WHERE tenant_id=$1 AND invocation_id=$2 AND sandbox_executor IS NULL")
            .bind(state.tenant_id.into_uuid()).bind(commit.invocation_id.into_uuid())
            .bind(commit.route.decision.executor.as_str()).bind(commit.route.decision.reason.as_str()).bind(commit.route.decision.compatibility.as_str())
            .bind(commit.route.decided_at.unix_millis()).bind(Sha256::digest(commit.route.canonical_bytes()).into_bytes().to_vec())
            .execute(prepared.transaction_mut()).await?;
        require_one(changed.rows_affected())?;
        advance_run_sequence(
            prepared.transaction_mut(),
            attempt.run_id,
            state.next_sequence(1)?,
        )
        .await?;
        prepared.commit(&append).await?;
        self.required_invocation(state.tenant_id, commit.invocation_id)
            .await
    }
}
fn route_error(
    error: ToolCommitError,
) -> musteragent_runtime_capability::BrowserProviderRuntimeError {
    use musteragent_runtime_capability::BrowserProviderRuntimeError as E;
    if matches!(error, ToolCommitError::IdempotencyConflict) {
        return E::RouteConflict;
    }
    match error.port().code {
        ErrorCode::AttemptLeaseStale => E::StaleLease,
        ErrorCode::DependencyUnavailable => E::DurabilityUnavailable,
        ErrorCode::IdempotencyConflict => E::RouteConflict,
        _ => E::CorruptDurableState,
    }
}
