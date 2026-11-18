//! Coordination handoff has one authoritative receipt: the successful registered
//! Tool committed with the original Ledger. No second Run/Task status is written.
use super::*;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
mod source;

impl PostgresWorkItemStartStore {
    pub(super) async fn prepare_handoff_inner(
        &self,
        attempt: &AttemptContext,
        p: PrepareWorkItemHandoff,
    ) -> Result<WorkItemHandoffMaterial, DialogueStoreError> {
        p.validate()?;
        if attempt.run_id != p.invocation.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let replay = start::completed_invocation(
            &mut tx,
            p.invocation.actor.tenant_id,
            p.invocation.invocation_id,
        )
        .await?;
        let material = source::load(&mut tx, &p, replay).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(material)
    }
    pub(super) async fn commit_handoff_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemHandoff,
    ) -> Result<(), DialogueStoreError> {
        c.validate()?;
        let i = &c.preparation.invocation;
        if attempt.run_id != i.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        if self.handoff_replay(&c).await? {
            return Ok(());
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if state.tenant_id != i.actor.tenant_id
            || state.conversation_id != i.run_context.conversation_id
            || state.activity_id != i.run_context.activity_id
            || state.home_epoch != i.run_context.home_epoch
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if start::completed_invocation(&mut tx, i.actor.tenant_id, i.invocation_id).await? {
            drop(tx);
            return if self.handoff_replay(&c).await? {
                Ok(())
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        start::fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(&mut tx, state.tenant_id, i.invocation_id, attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let s = &c.material.source;
        preparation::lock_agents(&mut tx, s.actor, s.scope, s.scope.agent_id).await?;
        // Follow the original WorkItem owner lock order, then freeze all current
        // revisions and rejection receipts through the atomic terminal commit.
        for proof in &c.material.tasks {
            sqlx::query("SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 FOR UPDATE")
                .bind(proof.target.task_id).fetch_one(&mut *tx).await.map_err(db)?;
            sqlx::query("SELECT o.event_seq FROM collaboration.work_item_outbox o JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=o.work_item_id AND x.accepted_state_revision=o.state_revision WHERE x.command_id=$1 AND o.event_kind='execution_requested' FOR SHARE OF o")
                .bind(proof.execution_command_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        }
        if source::load(&mut tx, &c.preparation, false).await? != c.material {
            return Err(DialogueStoreError::RevisionConflict);
        }
        lock_ready_content_reference(&mut tx, i.actor.tenant_id, &i.request_ref).await?;
        let result = result(&c);
        crate::artifact::authorize_result_content(&mut tx, i.actor.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, state, attempt, i, &result, c.at).await
    }
    async fn handoff_replay(&self, c: &CommitWorkItemHandoff) -> Result<bool, DialogueStoreError> {
        let i = &c.preparation.invocation;
        let mut tx = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        if !start::completed_invocation(&mut tx, i.actor.tenant_id, i.invocation_id).await? {
            return Ok(false);
        }
        if source::load(&mut tx, &c.preparation, true).await? != c.material {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations i JOIN tool.tool_results r ON r.invocation_id=i.invocation_id AND r.result_content_id=i.result_content_id WHERE i.tenant_id=$1 AND i.invocation_id=$2 AND i.tool_ref='tasks.handoff@1' AND i.status=4 AND i.result_content_id=$3 AND r.result_hash=$4 AND r.result_schema_key=$5 AND r.result_schema_hash=$6)")
            .bind(i.actor.tenant_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(c.result.content_id.into_uuid()).bind(c.result.sha256.into_bytes().to_vec())
            .bind(c.result_schema.schema_key.as_str()).bind(c.result_schema.schema_hash.into_bytes().to_vec()).fetch_one(&mut *tx).await.map_err(db)?;
        if !valid {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        crate::artifact::authorize_result_content(&mut tx, i.actor.tenant_id, &result(c))
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        crate::tool_security_audit::replay_succeeded(
            &self.audit,
            self.ledger.pool(),
            i.actor.tenant_id,
            i.invocation_id,
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(true)
    }
}
fn result(c: &CommitWorkItemHandoff) -> CapabilityResult {
    CapabilityResult {
        result_ref: c.result.clone(),
        result_hash: c.result.sha256,
        result_schema: c.result_schema.clone(),
        provider_request_ref: None,
    }
}
