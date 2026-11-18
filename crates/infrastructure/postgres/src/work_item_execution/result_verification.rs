//! Only the current requester report can assess and deliver the sealed result.
use super::result_receipts as receipts;
use super::*;

use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;

pub(crate) struct AcceptedWorkItemResult<'a> {
    manifest: &'a WorkItemResultManifest,
    report: Option<(RunId, InvocationId)>,
    state_revision: u64,
    publication: Option<InvocationId>,
}
impl<'a> AcceptedWorkItemResult<'a> {
    pub(crate) fn scope(&self) -> &WorkItemResultScope {
        &self.manifest.scope
    }
    pub(crate) fn manifest(&self) -> &WorkItemResultManifest {
        self.manifest
    }
    pub(super) fn from_device_terminal(
        manifest: &'a WorkItemResultManifest,
        state_revision: u64,
    ) -> Self {
        Self {
            manifest,
            report: None,
            publication: None,
            state_revision,
        }
    }
    pub(crate) fn from_publication(
        manifest: &'a WorkItemResultManifest,
        report: RunId,
        invocation: InvocationId,
        state_revision: u64,
    ) -> Self {
        Self {
            manifest,
            report: Some((report, invocation)),
            publication: Some(invocation),
            state_revision,
        }
    }
    pub(crate) fn publication_invocation_id(&self) -> Option<InvocationId> {
        self.publication
    }
    pub(crate) fn verification_invocation_id(&self) -> Option<InvocationId> {
        self.report
            .filter(|_| self.publication.is_none())
            .map(|(_, invocation)| invocation)
    }
    pub(crate) fn report_run_id(&self) -> Option<RunId> {
        self.report.map(|(run, _)| run)
    }
    pub(crate) fn publication_run_id(&self) -> RunId {
        self.report_run_id()
            .unwrap_or(self.scope().producing_run_id)
    }
    pub(crate) fn result_id(&self) -> uuid::Uuid {
        self.manifest.result_id()
    }
    pub(crate) fn accepted_state_revision(&self) -> u64 {
        self.state_revision
    }
}

/// A terminal business action may only consume its own final isolated Tool
/// batch. A parallel sibling must not observe or mutate results after publish.
pub(super) async fn require_terminal_tool_batch(
    tx: &mut PgConnection,
    i: &ResolveWorkItemStart,
) -> Result<(), DialogueStoreError> {
    let ok:bool=sqlx::query_scalar("WITH latest AS (
        SELECT p.invocation_id,p.source_sequence_to FROM runtime.context_plans p JOIN runtime.model_invocations m USING(tenant_id,run_id,invocation_id)
        WHERE p.tenant_id=$1 AND p.run_id=$2 AND m.status=2 ORDER BY p.committed_sequence DESC LIMIT 1)
        SELECT EXISTS(SELECT 1 FROM latest WHERE (SELECT count(*) FROM runtime.pending_tool_calls p WHERE p.run_id=$2 AND p.invocation_id=latest.invocation_id)=1
          AND EXISTS(SELECT 1 FROM runtime.pending_tool_calls p JOIN tool.tool_invocations t ON t.tenant_id=$1 AND t.run_id=p.run_id AND t.tool_call_id=p.tool_call_id
            WHERE p.run_id=$2 AND p.invocation_id=latest.invocation_id AND t.invocation_id=$3)
          AND NOT EXISTS(SELECT 1 FROM runtime.loop_controls c WHERE c.tenant_id=$1 AND c.run_id=$2 AND (c.status=1 OR(c.status=2 AND c.source_conversation_seq>latest.source_sequence_to)))
          AND NOT EXISTS(SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id=$1 AND t.run_id=$2 AND t.invocation_id<>$3 AND
            (t.status IN(1,2,3) OR (t.status=6 AND NOT EXISTS(SELECT 1 FROM runtime.pending_tool_calls p WHERE p.run_id=t.run_id AND p.tool_call_id=t.tool_call_id
             AND p.completed_at_ms IS NOT NULL AND p.result_content_id IS NOT NULL AND p.result_content_hash IS NOT NULL))))
          AND NOT EXISTS(SELECT 1 FROM approval.interactions WHERE tenant_id=$1 AND run_id=$2 AND status=1)
          AND NOT EXISTS(SELECT 1 FROM runtime.delegations WHERE tenant_id=$1 AND parent_run_id=$2 AND kind=1 AND status=1))")
        .bind(i.actor.tenant_id.into_uuid()).bind(i.run_context.run_id.into_uuid()).bind(i.invocation_id.into_uuid()).fetch_one(tx).await.map_err(db)?;
    if ok {
        Ok(())
    } else {
        Err(DialogueStoreError::RevisionConflict)
    }
}
async fn material(
    tx: &mut PgConnection,
    p: &PrepareWorkItemResultVerification,
    lock: bool,
) -> Result<WorkItemResultVerificationMaterial, DialogueStoreError> {
    let a = p.validate()?;
    let i = &p.invocation;
    require_user(i.actor)?;
    let producer = RunId::from_uuid(a.producing_run_id).map_err(|_| DialogueStoreError::Corrupt)?;
    let (submission, manifest_ref, _) = receipts::load(tx, producer)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    let m = submission.manifest;
    a.validate_manifest(&m)?;
    let s = &m.scope;
    if s.tenant_id != i.actor.tenant_id
        || s.owner_id != i.actor.principal_id
        || s.requesting_agent_id != i.requesting_agent_id
    {
        return Err(DialogueStoreError::Unavailable);
    }
    if lock {
        receipts::lock_current(tx, s).await?;
    }
    let r=sqlx::query("SELECT t.space_id,r.report_content_ref,tool.status,c.state_revision FROM collaboration.work_item_execution_reports r
        JOIN collaboration.work_items t ON t.work_item_id=r.work_item_id AND t.tenant_id=r.tenant_id AND t.owner_id=r.owner_id
        JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.owner_id=r.owner_id AND s.conversation_id=r.report_conversation_id
        JOIN runtime.runs run ON run.run_id=r.report_run_id AND run.conversation_id=s.conversation_id AND run.tenant_id=s.tenant_id
        JOIN tool.tool_invocations tool ON tool.run_id=run.run_id AND tool.tenant_id=run.tenant_id AND tool.conversation_id=run.conversation_id
        LEFT JOIN collaboration.work_item_commands c ON c.source_invocation_id=tool.invocation_id AND c.source_run_id=run.run_id AND c.operation='verify'
        WHERE r.tenant_id=$1 AND r.owner_id=$2 AND r.producing_run_id=$3 AND r.report_run_id=$4 AND r.private_result_id=$5 AND r.private_result_sha256=$6
          AND r.report_content_id=$7 AND r.outcome='completed' AND s.subject_kind='work_item_report' AND s.agent_id=r.requesting_agent_id
          AND s.dialogue_id=r.source_dialogue_id AND s.report_from_run_id=r.producing_run_id AND s.work_item_id=r.work_item_id
          AND (s.work_item_goal_revision,s.work_item_control_revision,s.work_item_assignment_epoch)=(r.goal_revision,r.control_revision,r.assignment_epoch)
          AND tool.invocation_id=$8 AND tool.tool_ref='tasks.verify@3' AND tool.request_content_id=$9 AND tool.request_hash=$10
          AND run.runtime_snapshot_id=$11 AND t.erasure_job_id IS NULL AND run.erasure_job_id IS NULL AND tool.erasure_job_id IS NULL
          AND (tool.status=4 OR (run.status=2 AND run.cancel_requested_at_ms IS NULL))")
        .bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(producer.into_uuid()).bind(i.run_context.run_id.into_uuid()).bind(m.result_id())
        .bind(manifest_ref.sha256.into_bytes().to_vec()).bind(a.report_content_id).bind(i.invocation_id.into_uuid()).bind(i.request_ref.content_id.into_uuid())
        .bind(i.request_ref.sha256.into_bytes().to_vec()).bind(i.run_context.runtime_snapshot_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let replay = r.try_get::<i16, _>("status").map_err(db)? == 4;
    let state = if replay {
        Some(
            unsigned(
                r.try_get::<Option<i64>, _>("state_revision")
                    .map_err(db)?
                    .ok_or(DialogueStoreError::Corrupt)?,
            )?
            .checked_sub(1)
            .filter(|v| *v > 0)
            .ok_or(DialogueStoreError::Corrupt)?,
        )
    } else {
        None
    };
    let scope = OwnedAgentWorkItemScope {
        owner_id: s.owner_id,
        agent_id: s.requesting_agent_id,
        space_id: receipts::typed_id(&r, "space_id")?,
    };
    let task = crate::work_item_authority::load(tx, i.actor, scope, s.work_item_id, state)
        .await
        .map_err(authority_error)?
        .ok_or(DialogueStoreError::Unavailable)?;
    if task.core.status != WorkItemStatus::Verifying
        || task.core.revisions.goal.get() != s.goal_revision
        || task.core.revisions.control.get() != s.control_revision
        || task.core.assignment_epoch.map(AssignmentEpoch::get) != Some(s.assignment_epoch)
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    if !replay {
        crate::dialogue_context::run::run_cut(tx, i.run_context.run_id, lock)
            .await?
            .ok_or(DialogueStoreError::Unavailable)?;
        if s.producer_home == WorkItemResultHome::Cloud {
            crate::dialogue_context::run::run_cut(tx, producer, lock)
                .await?
                .ok_or(DialogueStoreError::Unavailable)?;
        }
        if s.producer_home == WorkItemResultHome::Device {
            super::device::revalidate_result_return(tx, s).await?;
        }
        for item in &m.items {
            if item.asset.is_some() {
                crate::artifact::task_result::revalidate_result_asset(tx, s, item).await?;
            }
            if lock {
                lock_ready_content_reference(tx, s.tenant_id, &item.content_ref).await?;
            }
        }
    }
    Ok(WorkItemResultVerificationMaterial {
        task,
        report_ref: content_from_bytes(r.try_get("report_content_ref").map_err(db)?)?,
        manifest_ref,
        manifest: m,
    })
}

impl PostgresWorkItemStartStore {
    pub(super) async fn prepare_result_verification_inner(
        &self,
        attempt: &AttemptContext,
        p: PrepareWorkItemResultVerification,
    ) -> Result<WorkItemResultVerificationMaterial, DialogueStoreError> {
        if attempt.run_id != p.invocation.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let m = material(&mut tx, &p, false).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(m)
    }
    pub(super) async fn commit_result_verification_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemResultVerification,
    ) -> Result<WorkItemResultVerificationReceipt, DialogueStoreError> {
        let receipt = c.validate()?;
        let i = &c.preparation.invocation;
        if attempt.run_id != i.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        if self.result_verification_replay(&c).await? {
            return Ok(receipt);
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if start::completed_invocation(&mut tx, i.actor.tenant_id, i.invocation_id).await? {
            drop(tx);
            return if self.result_verification_replay(&c).await? {
                Ok(receipt)
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        start::fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(&mut tx, state.tenant_id, i.invocation_id, attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let m = material(&mut tx, &c.preparation, true).await?;
        if m != c.material {
            return Err(DialogueStoreError::RevisionConflict);
        }
        require_terminal_tool_batch(&mut tx, i).await?;
        for reference in [
            &m.task.goal_content_ref,
            &m.report_ref,
            &m.manifest_ref,
            &i.request_ref,
        ] {
            lock_ready_content_reference(&mut tx, i.actor.tenant_id, reference).await?;
        }
        let s = &m.manifest.scope;
        let accepted = receipt.decision == WorkItemVerificationDecision::Accepted;
        let mut task = m.task.clone();
        task.core = task
            .core
            .assess(
                WorkItemGoalRevision::new(s.goal_revision)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                WorkItemControlRevision::new(s.control_revision)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                AssignmentEpoch::new(s.assignment_epoch)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                accepted,
                receipt.decision == WorkItemVerificationDecision::Failed,
            )
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        task.updated_at = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        crate::work_item_authority::commands::revision_and_outbox(
            &mut tx,
            &task,
            "verification_recorded",
        )
        .await
        .map_err(authority_error)?;
        let revision = task.core.revisions.state.get();
        sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
            .bind(s.work_item_id.into_uuid())
            .bind(integer(revision)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("INSERT INTO collaboration.work_item_commands(tenant_id,owner_id,command_id,request_hash,work_item_id,state_revision,operation,source_run_id,source_invocation_id,source_snapshot_id)
            VALUES($1,$2,$3,$4,$5,$6,'verify',$7,$8,$9)").bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(work_item_verification_id(i.invocation_id).into_uuid())
            .bind(i.request_ref.sha256.into_bytes().to_vec()).bind(s.work_item_id.into_uuid()).bind(integer(revision)?).bind(i.run_context.run_id.into_uuid())
            .bind(i.invocation_id.into_uuid()).bind(i.run_context.runtime_snapshot_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        if accepted {
            let proof = AcceptedWorkItemResult {
                publication: None,
                manifest: &m.manifest,
                report: Some((i.run_context.run_id, i.invocation_id)),
                state_revision: revision,
            };
            crate::dialogue_publication::task_result::promote(&mut tx, &proof).await?;
        }
        let result = CapabilityResult {
            result_ref: c.result.clone(),
            result_hash: c.result.sha256,
            result_schema: c.result_schema.clone(),
            provider_request_ref: None,
        };
        crate::artifact::authorize_result_content(&mut tx, i.actor.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, state, attempt, i, &result, c.at)
            .await?;
        Ok(receipt)
    }
    async fn result_verification_replay(
        &self,
        c: &CommitWorkItemResultVerification,
    ) -> Result<bool, DialogueStoreError> {
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
        let m = material(&mut tx, &c.preparation, false).await?;
        if m != c.material {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let r=sqlx::query("SELECT t.result_content_id,z.result_hash,d.delivery_id FROM tool.tool_invocations t JOIN tool.tool_results z USING(invocation_id,result_content_id)
            LEFT JOIN collaboration.work_item_result_deliveries d ON d.verification_invocation_id=t.invocation_id
            WHERE t.tenant_id=$1 AND t.run_id=$2 AND t.invocation_id=$3 AND t.status=4 AND t.tool_ref='tasks.verify@3'")
            .bind(i.actor.tenant_id.into_uuid()).bind(i.run_context.run_id.into_uuid()).bind(i.invocation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if r.try_get::<uuid::Uuid, _>("result_content_id")
            .map_err(db)?
            != c.result.content_id.into_uuid()
            || r.try_get::<Vec<u8>, _>("result_hash").map_err(db)? != c.result.sha256.into_bytes()
            || r.try_get::<Option<uuid::Uuid>, _>("delivery_id")
                .map_err(db)?
                != c.validate()?.delivery_id.map(CommandId::into_uuid)
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        lock_ready_content_reference(&mut tx, i.actor.tenant_id, &c.result).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(true)
    }
}
