//! Registered goal/control commands commit in the original WorkItem and Tool
//! transaction. Causation columns belong to the immutable command receipt.
use super::*;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_session::{AmendAgentWorkItem, CancelAgentWorkItem};

impl PostgresWorkItemStartStore {
    pub(super) async fn mutate_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemMutation,
    ) -> Result<(), DialogueStoreError> {
        c.validate()?;
        if attempt.run_id != c.invocation.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        if self.mutation_replay(&c).await? {
            return Ok(());
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if state.tenant_id != c.invocation.actor.tenant_id
            || state.conversation_id != c.invocation.run_context.conversation_id
            || state.activity_id != c.invocation.run_context.activity_id
            || state.home_epoch != c.invocation.run_context.home_epoch
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if start::completed_invocation(
            &mut tx,
            c.invocation.actor.tenant_id,
            c.invocation.invocation_id,
        )
        .await?
        {
            drop(tx);
            return if self.mutation_replay(&c).await? {
                Ok(())
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        start::fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(
            &mut tx,
            state.tenant_id,
            c.invocation.invocation_id,
            attempt,
        )
        .await
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let actual:String=sqlx::query_scalar("SELECT tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2 AND run_id=$3").bind(c.invocation.actor.tenant_id.into_uuid()).bind(c.invocation.invocation_id.into_uuid()).bind(c.invocation.run_context.run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !c.command.matches_tool(&actual)
            || parse_work_item_mutation(&actual, &c.request_bytes)? != c.command
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let source = start_source::load_for(
            &mut tx,
            &c.invocation,
            c.command.tool(),
            false,
            Some(c.command.task_id()),
        )
        .await?;
        apply(&mut tx, &source, &c).await?;
        let result = result(&c);
        crate::artifact::authorize_result_content(&mut tx, source.actor.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, state, attempt, &c.invocation, &result, c.changed_at)
            .await
    }
    async fn mutation_replay(
        &self,
        c: &CommitWorkItemMutation,
    ) -> Result<bool, DialogueStoreError> {
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("SELECT i.status,i.result_content_id,i.request_content_id,i.tool_call_id,i.tool_ref,i.run_id,command.command_id,command.work_item_id,command.source_snapshot_id,command.operation,task.erasure_job_id FROM tool.tool_invocations i LEFT JOIN collaboration.input_task_action_invocations link ON link.tenant_id=i.tenant_id AND link.invocation_id=i.invocation_id LEFT JOIN collaboration.input_task_actions action ON action.tenant_id=link.tenant_id AND action.action_id=link.action_id AND action.owner_id=$3 LEFT JOIN collaboration.work_item_commands command ON command.tenant_id=i.tenant_id AND command.owner_id=$3 AND (command.source_invocation_id=i.invocation_id OR command.command_id=action.command_id) LEFT JOIN collaboration.work_items task ON task.work_item_id=command.work_item_id WHERE i.tenant_id=$1 AND i.invocation_id=$2")
   .bind(c.invocation.actor.tenant_id.into_uuid()).bind(c.invocation.invocation_id.into_uuid()).bind(c.invocation.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if row.try_get::<i16, _>("status").map_err(db)? != 4 {
            return Ok(false);
        }
        if row
            .try_get::<Option<uuid::Uuid>, _>("erasure_job_id")
            .map_err(db)?
            .is_some()
        {
            return Err(DialogueStoreError::Inactive);
        }
        if row
            .try_get::<Option<uuid::Uuid>, _>("command_id")
            .map_err(db)?
            != Some(c.command_id.into_uuid())
            || row
                .try_get::<Option<uuid::Uuid>, _>("work_item_id")
                .map_err(db)?
                != Some(c.command.task_id().into_uuid())
            || row.try_get::<uuid::Uuid, _>("run_id").map_err(db)?
                != c.invocation.run_context.run_id.into_uuid()
            || row
                .try_get::<uuid::Uuid, _>("request_content_id")
                .map_err(db)?
                != c.invocation.request_ref.content_id.into_uuid()
            || row.try_get::<&str, _>("tool_call_id").map_err(db)?
                != c.invocation.tool_call_id.as_str()
            || !c
                .command
                .matches_tool(row.try_get::<&str, _>("tool_ref").map_err(db)?)
            || row.try_get::<Option<&str>, _>("operation").map_err(db)? != Some(c.command.kind())
            || row
                .try_get::<Option<uuid::Uuid>, _>("result_content_id")
                .map_err(db)?
                != Some(c.result.content_id.into_uuid())
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        crate::artifact::authorize_result_content(
            &mut tx,
            c.invocation.actor.tenant_id,
            &result(c),
        )
        .await
        .map_err(|_| DialogueStoreError::Corrupt)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        crate::tool_security_audit::replay_succeeded(
            &self.audit,
            self.ledger.pool(),
            c.invocation.actor.tenant_id,
            c.invocation.invocation_id,
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(true)
    }
}
fn result(c: &CommitWorkItemMutation) -> CapabilityResult {
    CapabilityResult {
        result_ref: c.result.clone(),
        result_hash: c.result.sha256,
        result_schema: c.result_schema.clone(),
        provider_request_ref: None,
    }
}

/// Original Task mutation effects shared by Cloud and authenticated Native source.
pub(super) async fn apply(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    c: &CommitWorkItemMutation,
) -> Result<(), DialogueStoreError> {
    preparation::lock_agents(tx, source.actor, source.scope, source.scope.agent_id).await?;
    lock_ready_content_reference(tx, source.actor.tenant_id, &c.invocation.request_ref).await?;
    let command_id = input_actions::bind(tx, source, &c.command, c.changed_at).await?;
    if command_id != c.command_id {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    let receipt = match &c.command {
        WorkItemMutation::Cancel {
            task_id,
            expected_goal,
            expected_control,
        } => crate::work_item_authority::commands::cancel_in_transaction(
            tx,
            CancelAgentWorkItem {
                actor: source.actor,
                scope: source.scope,
                command_id,
                work_item_id: *task_id,
                expected_goal: *expected_goal,
                expected_control: *expected_control,
                changed_at: c.changed_at,
            },
        )
        .await
        .map_err(authority_error)?,
        WorkItemMutation::Amend {
            task_id,
            expected_goal,
            expected_control,
            spec,
        } => {
            let current =
                crate::work_item_authority::load(tx, source.actor, source.scope, *task_id, None)
                    .await
                    .map_err(authority_error)?
                    .ok_or(DialogueStoreError::Unavailable)?;
            let mut spec = spec.clone();
            spec.priority = current.core.spec.priority;
            spec.follow_up_of = current.core.spec.follow_up_of;
            let report:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id WHERE r.run_id=$1 AND s.subject_kind='work_item_report')").bind(source.source_run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            // Evidence can trigger further work on the accepted goal. It cannot change
            // the user's objective, evidence boundary, dependencies or success criteria.
            if report
                && (musteragent_content_ref::Sha256::digest(work_item_goal_bytes(&spec))
                    != current.goal_content_ref.sha256
                    || spec.metadata() != current.core.spec)
            {
                return Err(DialogueStoreError::Unavailable);
            }

            crate::work_item_authority::commands::amend_for_execution(
                tx,
                AmendAgentWorkItem {
                    actor: source.actor,
                    scope: source.scope,
                    command_id,
                    work_item_id: *task_id,
                    expected_goal: *expected_goal,
                    expected_control: *expected_control,
                    spec,
                    goal_content_ref: c.goal.clone().ok_or(DialogueStoreError::Corrupt)?,
                    changed_at: c.changed_at,
                },
            )
            .await
            .map_err(authority_error)?
        }
    };
    if !receipt.replayed {
        let changed=sqlx::query("UPDATE collaboration.work_item_commands SET operation=$4,source_run_id=$5,source_invocation_id=$6,source_snapshot_id=$7,source_home=CASE WHEN EXISTS(SELECT 1 FROM collaboration.work_item_native_sources n WHERE n.invocation_id=$6) THEN 'device' ELSE 'cloud' END WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3 AND source_invocation_id IS NULL")
   .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(command_id.into_uuid()).bind(c.command.kind()).bind(source.source_run_id.into_uuid()).bind(source.source_invocation_id.into_uuid()).bind(source.source_snapshot_id.into_uuid()).execute(&mut *tx).await.map_err(db)?.rows_affected();
        if changed != 1 {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
    }
    Ok(())
}
