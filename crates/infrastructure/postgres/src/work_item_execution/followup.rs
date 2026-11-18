//! Accepted result-reply -> one formal Task successor. All effects use the
//! existing authority, action registry, reservation and fenced Tool commit.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
mod source;
use source::{action_hash, load};
impl PostgresWorkItemStartStore {
    pub(super) async fn prepare_followup_inner(
        &self,
        attempt: &AttemptContext,
        r: PrepareWorkItemFollowup,
    ) -> Result<WorkItemFollowupMaterial, DialogueStoreError> {
        r.validate()?;
        if attempt.run_id != r.invocation.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let material = load(&mut tx, &r, true).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(material)
    }
    pub(super) async fn commit_followup_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemFollowup,
    ) -> Result<WorkItemExecutionRequest, DialogueStoreError> {
        c.validate()?;
        if attempt.run_id != c.material.source.source_run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Some(receipt) = self.followup_replay(&c).await? {
            return Ok(receipt);
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let r = &c.preparation.invocation;
        if state.tenant_id != r.actor.tenant_id
            || state.conversation_id != r.run_context.conversation_id
            || state.activity_id != r.run_context.activity_id
            || state.home_epoch != r.run_context.home_epoch
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if start::completed_invocation(&mut tx, r.actor.tenant_id, r.invocation_id).await? {
            drop(tx);
            return self
                .followup_replay(&c)
                .await?
                .ok_or(DialogueStoreError::IdempotencyConflict);
        }
        start::fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(&mut tx, state.tenant_id, r.invocation_id, attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let material = load(&mut tx, &c.preparation, false).await?;
        if material != c.material {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let s = &material.source;
        let executor = material
            .input
            .executing_agent_id
            .ok_or(DialogueStoreError::Corrupt)?;
        preparation::lock_agents(&mut tx, s.actor, s.scope, executor).await?;
        lock_ready_content_reference(&mut tx, s.actor.tenant_id, &r.request_ref).await?;
        let existing:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_followups WHERE work_item_id=$1 AND source_intent_id=$2 AND tenant_id=$3 AND owner_id=$4)")
            .bind(material.task_id().into_uuid()).bind(s.source_intent_id.into_uuid()).bind(s.actor.tenant_id.into_uuid()).bind(s.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        // An already committed action is acknowledged from its original receipt;
        // it neither reserves again nor renews a device consent.
        if !existing {
            if let WorkItemExecutionTarget::RemoteDevice(target) = &material.destination {
                let current = self
                    .device_targets
                    .as_ref()
                    .ok_or(DialogueStoreError::Unavailable)?
                    .resolve(
                        s.actor,
                        s.scope.agent_id,
                        executor,
                        WorkItemDeviceSelection {
                            membership_id: target.membership_id,
                            expected_membership_epoch: target.membership_epoch,
                        },
                    )
                    .await?;
                if &current != target {
                    return Err(DialogueStoreError::Unavailable);
                }
            }
            let receipt = crate::work_item_authority::commands::create_in_transaction(
                &mut tx,
                material.create(c.goal.clone(), c.at),
            )
            .await
            .map_err(authority_error)?;
            if receipt.replayed {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            input_actions::bind_identity(
                &mut tx,
                s,
                material.action(),
                material.task_id(),
                "followup",
                action_hash(&material)?,
                c.at,
            )
            .await?;
            source::record(&mut tx, &material, c.at).await?;
            let request = start_reservation::reserve_existing(
                &mut tx,
                receipt.record,
                s,
                executor,
                c.partner_input.as_ref(),
                c.at,
            )
            .await?;
            device_target::persist(&mut tx, request, &material.destination).await?;
            if matches!(
                material.destination,
                WorkItemExecutionTarget::RemoteDevice(_)
            ) {
                sqlx::query("UPDATE collaboration.work_item_execution_reservations SET device_placement_captured=TRUE,device_workspace_id=$2 WHERE work_item_id=$1 AND goal_revision=1 AND control_revision=1 AND assignment_epoch=1 AND NOT device_placement_captured")
                    .bind(material.task_id().into_uuid()).bind(material.device_workspace_id.map(WorkspaceId::into_uuid)).execute(&mut *tx).await.map_err(db)?;
            }
            // No second creation card: the original work root now projects its
            // current successor while every prior Task/result remains immutable.
        } else {
            input_actions::bind_identity(
                &mut tx,
                s,
                material.action(),
                material.task_id(),
                "followup",
                action_hash(&material)?,
                c.at,
            )
            .await?;
        }
        let result = result(&c);
        crate::artifact::authorize_result_content(&mut tx, s.actor.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, state, attempt, r, &result, c.at)
            .await?;
        Ok(request(&material))
    }
    async fn followup_replay(
        &self,
        c: &CommitWorkItemFollowup,
    ) -> Result<Option<WorkItemExecutionRequest>, DialogueStoreError> {
        let r = &c.preparation.invocation;
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("SELECT i.*,a.command_id,a.request_sha256,a.work_item_id,w.erasure_job_id,f.parent_work_item_id FROM tool.tool_invocations i
            LEFT JOIN collaboration.input_task_action_invocations link ON link.tenant_id=i.tenant_id AND link.invocation_id=i.invocation_id
            LEFT JOIN collaboration.input_task_actions a ON a.tenant_id=link.tenant_id AND a.action_id=link.action_id AND a.owner_id=$3 AND a.operation='followup'
            LEFT JOIN collaboration.work_items w ON w.work_item_id=a.work_item_id AND w.tenant_id=a.tenant_id AND w.owner_id=a.owner_id
            LEFT JOIN collaboration.work_item_followups f ON f.work_item_id=w.work_item_id
            WHERE i.tenant_id=$1 AND i.invocation_id=$2")
            .bind(r.actor.tenant_id.into_uuid()).bind(r.invocation_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if row.try_get::<i16, _>("status").map_err(db)? != 4 {
            return Ok(None);
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
            != Some(c.material.action().command_id.into_uuid())
            || row
                .try_get::<Option<Vec<u8>>, _>("request_sha256")
                .map_err(db)?
                != Some(action_hash(&c.material)?.to_vec())
            || row
                .try_get::<Option<uuid::Uuid>, _>("work_item_id")
                .map_err(db)?
                != Some(c.material.task_id().into_uuid())
            || row
                .try_get::<Option<uuid::Uuid>, _>("parent_work_item_id")
                .map_err(db)?
                != Some(c.material.parent()?.work_item_id.into_uuid())
            || row.try_get::<uuid::Uuid, _>("run_id").map_err(db)?
                != r.run_context.run_id.into_uuid()
            || row
                .try_get::<uuid::Uuid, _>("request_content_id")
                .map_err(db)?
                != r.request_ref.content_id.into_uuid()
            || row.try_get::<&str, _>("tool_call_id").map_err(db)? != r.tool_call_id.as_str()
            || row.try_get::<&str, _>("tool_ref").map_err(db)? != WORK_ITEM_FOLLOWUP_TOOL
            || row
                .try_get::<Option<uuid::Uuid>, _>("result_content_id")
                .map_err(db)?
                != Some(c.result.content_id.into_uuid())
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        crate::artifact::authorize_result_content(&mut tx, r.actor.tenant_id, &result(c))
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        crate::tool_security_audit::replay_succeeded(
            &self.audit,
            self.ledger.pool(),
            r.actor.tenant_id,
            r.invocation_id,
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(Some(request(&c.material)))
    }
}
fn request(m: &WorkItemFollowupMaterial) -> WorkItemExecutionRequest {
    WorkItemExecutionRequest {
        actor: m.source.actor,
        work_item_id: m.task_id(),
        goal_revision: WorkItemGoalRevision::new(1).expect("one"),
        control_revision: WorkItemControlRevision::new(1).expect("one"),
        assignment_epoch: AssignmentEpoch::new(1).expect("one"),
        report_from_run_id: None,
    }
}
fn result(c: &CommitWorkItemFollowup) -> CapabilityResult {
    CapabilityResult {
        result_ref: c.result.clone(),
        result_hash: c.result.sha256,
        result_schema: c.result_schema.clone(),
        provider_request_ref: None,
    }
}
