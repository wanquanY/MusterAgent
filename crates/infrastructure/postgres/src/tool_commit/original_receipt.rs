//! Recover the original atomic Tool receipt, never a second effect ledger.
use super::*;
use musteragent_runtime_capability::{OriginalToolReceiptQuery, original_tool_receipt_identity};
impl PostgresFencedToolInvocationJournal {
    pub(crate) async fn lookup_original_receipt(
        &self,
        attempt: &AttemptContext,
        query: OriginalToolReceiptQuery,
    ) -> Result<Option<ToolTerminalOutcome>, ToolExecutionPortError> {
        if !matches!(
            query.invocation.tool_key.as_str(),
            "agenda.todo.create@1"
                | "agenda.todo.create@2"
                | "agenda.todo.update@1"
                | "agenda.todo.update@2"
                | "agenda.todo.control@1"
                | "agenda.todo.control@2"
                | "agenda.entry.create@1"
                | "agenda.entry.create@2"
                | "agenda.entry.update@1"
                | "agenda.entry.update@2"
                | "agenda.entry.control@1"
                | "agenda.entry.control@2"
                | "agenda.changes.apply@1"
                | "agenda.changes.apply@2"
                | "agenda.changes.apply@3"
                | "goal.create@3"
                | "goal.draft.save@1"
                | "goal.update@3"
                | "goal.sources.configure@1"
                | "goal.change_status@2"
                | "schedules.create@1"
                | "schedules.update@1"
                | "schedules.control@1"
                | "agents.self.configure@2"
                | "resource.file.create@4"
                | "resource.file.edit.begin@5"
                | "resource.file.edit.append@4"
                | "resource.file.edit.patch@6"
                | "resource.file.edit.finalize@4"
                | "resource.file.edit.abort@4"
                | "resource.folder.create@5"
                | "resource.node.rename@5"
                | "resource.node.move@4"
                | "resource.node.trash@4"
                | "resource.node.restore@4"
                | "resource.change-set.commit@6"
        ) {
            return Err(ToolExecutionPortError {
                code: ErrorCode::InvalidCommand,
                retryable: false,
            });
        }
        let snapshots = self
            .receipt_snapshots
            .as_ref()
            .ok_or(ToolExecutionPortError {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            })?;
        // Resolve the immutable sealed material before taking the original commit
        // lane. This also works with a single-connection pool and preserves the
        // material owner's actual stale/infrastructure error boundary.
        let materials = crate::PostgresToolExecutionMaterialSource::new(
            self.pool.clone(),
            snapshots.clone(),
            self.decoder.clone(),
        )
        .with_application_catalogs(self.application_catalogs.clone());
        let material = materials
            .original_receipt_material(attempt, &query.invocation.tool_key)
            .await?;
        self.original_receipt_inner(attempt, query, material)
            .await
            .map_err(ToolCommitError::port)
    }
    async fn original_receipt_inner(
        &self,
        attempt: &AttemptContext,
        query: OriginalToolReceiptQuery,
        material: (
            musteragent_identity::Actor,
            musteragent_run::RunContext,
            musteragent_runtime_snapshot::ToolDefinitionSnapshot,
        ),
    ) -> Result<Option<ToolTerminalOutcome>, ToolCommitError> {
        if query.invocation.run_id != attempt.run_id
            || query.run_context.run_id != attempt.run_id
            || query.actor.tenant_id != query.run_context.tenant_id
            || query.reconciliation_ref
                != original_tool_receipt_identity(query.invocation.invocation_id)
            || query.retry_contract != ToolRetryContract::QueryBeforeRetry
        {
            return Err(ToolCommitError::IdempotencyConflict);
        }
        let (mut tx, state) = self.begin_commit_lane(attempt).await?;
        let now = database_now_ms(&mut tx).await?;
        if state.tenant_id != query.actor.tenant_id
            || state.conversation_id != query.run_context.conversation_id
            || state.activity_id != query.run_context.activity_id
            || state.home_epoch != query.run_context.home_epoch
            || query.deadline <= now
            || query
                .deadline
                .unix_millis()
                .saturating_sub(now.unix_millis())
                > 10_000
        {
            return Err(ToolCommitError::InvalidTransition);
        }
        let fence = crate::resource_commit::attempt_fence(attempt, state.home_epoch);
        lock_and_validate_fence(&mut tx, &fence, state.last_run_sequence, now).await?;
        let live:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.runs r WHERE r.tenant_id=$1 AND r.run_id=$2 AND r.status IN (2,4) AND r.cancel_requested_at_ms IS NULL AND r.erasure_job_id IS NULL)")
   .bind(state.tenant_id.into_uuid()).bind(attempt.run_id.into_uuid()).fetch_one(&mut *tx).await?;
        if !live {
            return Err(ToolCommitError::InvalidTransition);
        }
        let (actor, run, definition) = material;
        if actor != query.actor
            || run != query.run_context
            || definition.manifest.input_schema != query.request_schema
            || definition.manifest.output_schema != query.result_schema
            || definition.manifest.retry_contract != query.retry_contract
            || definition.manifest.capability_provider_id != query.invocation.capability_provider_id
            || definition.manifest.effect != query.invocation.effect
            || definition.manifest.approval != query.invocation.approval
        {
            return Err(ToolCommitError::IdempotencyConflict);
        }
        let Some(durable) = self
            .load_invocation_on(&mut tx, state.tenant_id, query.invocation.invocation_id)
            .await?
        else {
            return Ok(None);
        };
        ensure_same_request(&durable, &query.invocation, query.retry_contract)?;
        // Terminal persistence clears the pending reconciliation identity by
        // design. At that point the validated original Ledger outcome is the
        // receipt; requiring a pending identity would reject every completed
        // mutation. Unfinished invocations must still carry the exact binding.
        if !durable.invocation.status.is_terminal()
            && durable.prebound_reconciliation_ref.as_ref() != Some(&query.reconciliation_ref)
        {
            return Err(ToolCommitError::IdempotencyConflict);
        }
        super::retained::ensure_retained(&mut tx, state.tenant_id, query.invocation.invocation_id)
            .await?;
        let outcome = match durable.recorded_outcome {
            Some(
                value @ (ToolTerminalOutcome::Succeeded(_)
                | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. }
                | ToolTerminalOutcome::Failed { .. }),
            ) => {
                if value
                    .retained_result()
                    .is_some_and(|result| result.result_schema != query.result_schema)
                {
                    return Err(ToolCommitError::Corrupt);
                }
                Some(value)
            }
            _ => None,
        };
        // The decoder above verified original Ledger payload hash and terminal
        // result identity. Recheck the real lease after all reads, before release.
        let final_now = database_now_ms(&mut tx).await?;
        if query.deadline <= final_now {
            return Err(ToolCommitError::InvalidTransition);
        }
        lock_and_validate_fence(&mut tx, &fence, state.last_run_sequence, final_now).await?;
        tx.commit().await?;
        Ok(outcome)
    }
}

impl musteragent_runtime_capability::OriginalToolReceiptReader
    for PostgresFencedToolInvocationJournal
{
    fn lookup<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        query: OriginalToolReceiptQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Option<ToolTerminalOutcome>, ToolExecutionPortError>,
    > {
        Box::pin(self.lookup_original_receipt(attempt, query))
    }
}
