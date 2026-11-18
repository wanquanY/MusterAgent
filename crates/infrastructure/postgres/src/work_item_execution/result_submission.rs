use super::result_receipts as receipts;
pub(crate) use super::result_receipts::load_submission;
use super::*;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;

/// Native calls this only inside its admitted, live custody transaction. Its
/// adapter authenticates the original signed source and stages the Tool result.
/// Cloud calls it inside the original Tool commit lane. Both persist one Task
/// fact and one set of private content pins.
pub(crate) async fn record_submission(
    tx: &mut PgConnection,
    material: &WorkItemResultSubmissionMaterial,
    call_id: &musteragent_identity::ToolCallId,
    manifest_ref: &ContentRef,
    receipt_ref: &ContentRef,
    at: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    let m = &material.manifest;
    let s = &m.scope;
    let receipt = m.receipt()?;
    if manifest_ref.content_id != work_item_result_manifest_id(m.submission_invocation_id)
        || manifest_ref.media_type.as_str() != WORK_ITEM_RESULT_MANIFEST_MEDIA
        || manifest_ref.sha256 != receipt.manifest_sha256
        || manifest_ref.byte_length != m.canonical_bytes()?.len() as u64
        || receipt_ref.content_id != work_item_result_submit_receipt_id(m.submission_invocation_id)
        || receipt_ref.media_type.as_str() != "application/json"
        || receipt_ref.sha256 != Sha256::digest(receipt.canonical_bytes())
        || receipt_ref.byte_length != receipt.canonical_bytes().len() as u64
    {
        return Err(DialogueStoreError::Corrupt);
    }
    receipts::lock_current(tx, s).await?;
    if let Some((old, old_manifest, old_receipt)) = receipts::load(tx, s.producing_run_id).await? {
        return if &old == material && &old_manifest == manifest_ref && &old_receipt == receipt_ref {
            Ok(())
        } else {
            Err(DialogueStoreError::IdempotencyConflict)
        };
    }
    sqlx::query("INSERT INTO collaboration.work_item_result_submissions
        (result_id,tenant_id,owner_id,work_item_id,goal_revision,control_revision,assignment_epoch,producing_run_id,producing_snapshot_id,producing_agent_id,producer_home,
         requesting_agent_id,source_dialogue_id,source_intent_id,submission_invocation_id,request_sha256,manifest_ref,manifest_content_id,manifest_sha256,receipt_ref,receipt_content_id,item_count,created_at_ms,submission_call_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)")
        .bind(m.result_id()).bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.work_item_id.into_uuid()).bind(integer(s.goal_revision)?)
        .bind(integer(s.control_revision)?).bind(integer(s.assignment_epoch)?).bind(s.producing_run_id.into_uuid()).bind(s.producing_snapshot_id.into_uuid())
        .bind(s.producing_agent_id.into_uuid()).bind(s.producer_home.key()).bind(s.requesting_agent_id.into_uuid()).bind(s.source_dialogue_id.into_uuid())
        .bind(s.source_intent_id.into_uuid()).bind(m.submission_invocation_id.into_uuid()).bind(m.request_sha256.into_bytes().to_vec())
        .bind(content_bytes(manifest_ref)).bind(manifest_ref.content_id.into_uuid()).bind(manifest_ref.sha256.into_bytes().to_vec()).bind(content_bytes(receipt_ref))
        .bind(receipt_ref.content_id.into_uuid()).bind(m.items.len() as i16).bind(at.unix_millis()).bind(call_id.as_str()).execute(&mut *tx).await.map_err(db)?;
    let mut pins = vec![
        ("work_item_result", m.result_id(), manifest_ref),
        ("work_item_result", m.result_id(), receipt_ref),
    ];
    for (ordinal, item) in m.items.iter().enumerate() {
        sqlx::query("INSERT INTO collaboration.work_item_result_items(result_id,ordinal,item_id,content_ref,artifact_id,artifact_version_id,asset_authority_sha256,content_id)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)").bind(m.result_id()).bind(ordinal as i16).bind(item.item_id).bind(content_bytes(&item.content_ref))
            .bind(item.asset.as_ref().map(|a|a.artifact_id.into_uuid())).bind(item.asset.as_ref().map(|a|a.artifact_version_id.into_uuid()))
            .bind(item.asset.as_ref().map(|a|a.authority_sha256.into_bytes().to_vec())).bind(item.content_ref.content_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        pins.push(("work_item_result", m.result_id(), &item.content_ref));
    }
    retain_content_refs(tx, receipts::actor(s), s.producing_agent_id, pins).await
}
async fn cloud_material(
    tx: &mut PgConnection,
    p: &PrepareWorkItemResultSubmission,
    lock: bool,
) -> Result<WorkItemResultSubmissionMaterial, DialogueStoreError> {
    let input = p.validate()?;
    let i = &p.invocation;
    require_user(i.actor)?;
    let row=sqlx::query("SELECT x.tenant_id,x.owner_id,x.work_item_id,x.goal_revision,x.control_revision,x.assignment_epoch,x.run_id AS producing_run_id,
        r.runtime_snapshot_id AS producing_snapshot_id,x.executing_agent_id AS producing_agent_id,x.requesting_agent_id,t.source_dialogue_id,t.source_intent_id,'cloud' AS producer_home
        FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t USING(work_item_id,tenant_id,owner_id)
        JOIN collaboration.work_item_revisions rev ON rev.work_item_id=t.work_item_id AND rev.state_revision=t.state_revision
        JOIN agent.dialogue_sessions s ON s.tenant_id=x.tenant_id AND s.conversation_id=x.conversation_id AND s.owner_id=x.owner_id
        JOIN runtime.runs r ON r.tenant_id=x.tenant_id AND r.conversation_id=x.conversation_id AND r.run_id=x.run_id
        JOIN tool.tool_invocations i ON i.tenant_id=r.tenant_id AND i.run_id=r.run_id AND i.conversation_id=r.conversation_id
        WHERE x.result_protocol='verified_result_v1' AND x.work_item_id=$1 AND x.goal_revision=$2 AND x.control_revision=$3 AND x.assignment_epoch=$4
          AND (rev.goal_revision,rev.control_revision,rev.assignment_epoch)=(x.goal_revision,x.control_revision,x.assignment_epoch) AND rev.status='active'
          AND x.tenant_id=$5 AND x.owner_id=$6 AND x.executing_agent_id=$7 AND x.run_id=$8 AND r.runtime_snapshot_id=$9
          AND s.subject_kind='work_item' AND s.work_item_id=x.work_item_id AND s.agent_id=x.executing_agent_id AND s.dialogue_id=x.executing_dialogue_id
          AND r.status=2 AND r.cancel_requested_at_ms IS NULL AND r.erasure_job_id IS NULL AND t.erasure_job_id IS NULL
          AND i.invocation_id=$10 AND i.tool_ref='tasks.submit_result@1' AND i.request_content_id=$11 AND i.request_hash=$12 AND i.erasure_job_id IS NULL")
        .bind(input.task_id.into_uuid()).bind(integer(input.goal_revision)?).bind(integer(input.control_revision)?).bind(integer(input.assignment_epoch)?)
        .bind(i.actor.tenant_id.into_uuid()).bind(i.actor.principal_id.into_uuid()).bind(i.requesting_agent_id.into_uuid()).bind(i.run_context.run_id.into_uuid())
        .bind(i.run_context.runtime_snapshot_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(i.request_ref.content_id.into_uuid()).bind(i.request_ref.sha256.into_bytes().to_vec())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::RevisionConflict)?;
    let scope = receipts::scope(&row)?;
    if lock {
        receipts::lock_current(tx, &scope).await?;
    }
    crate::dialogue_context::run::run_cut(tx, i.run_context.run_id, lock)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    crate::dialogue_publication::task_result::prepare(
        tx,
        &scope,
        i.invocation_id,
        &input,
        i.request_ref.sha256,
        p.message.as_ref(),
    )
    .await
}
impl PostgresWorkItemStartStore {
    pub(super) async fn prepare_result_submission_inner(
        &self,
        attempt: &AttemptContext,
        p: PrepareWorkItemResultSubmission,
    ) -> Result<WorkItemResultSubmissionMaterial, DialogueStoreError> {
        p.validate()?;
        if attempt.run_id != p.invocation.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let value = if let Some((old, _, _)) = receipts::load(&mut tx, attempt.run_id).await? {
            if old.manifest.submission_invocation_id != p.invocation.invocation_id
                || old.manifest.request_sha256 != p.invocation.request_ref.sha256
                || old.manifest.scope.tenant_id != p.invocation.actor.tenant_id
                || old.manifest.scope.owner_id != p.invocation.actor.principal_id
                || old.manifest.scope.producing_agent_id != p.invocation.requesting_agent_id
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            old
        } else {
            cloud_material(&mut tx, &p, false).await?
        };
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(value)
    }
    pub(super) async fn commit_result_submission_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemResultSubmission,
    ) -> Result<WorkItemResultSubmissionReceipt, DialogueStoreError> {
        let receipt = c.validate()?;
        let p = &c.preparation;
        let i = &p.invocation;
        if attempt.run_id != i.run_context.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        if self.submission_replay(&c).await? {
            return Ok(receipt);
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if start::completed_invocation(&mut tx, i.actor.tenant_id, i.invocation_id).await? {
            drop(tx);
            return if self.submission_replay(&c).await? {
                Ok(receipt)
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        start::fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(&mut tx, state.tenant_id, i.invocation_id, attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if cloud_material(&mut tx, p, true).await? != c.material {
            return Err(DialogueStoreError::RevisionConflict);
        }
        lock_ready_content_reference(&mut tx, i.actor.tenant_id, &i.request_ref).await?;
        super::result_verification::require_terminal_tool_batch(&mut tx, i).await?;
        record_submission(
            &mut tx,
            &c.material,
            &i.tool_call_id,
            &c.manifest_ref,
            &c.result,
            c.at,
        )
        .await?;
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
    async fn submission_replay(
        &self,
        c: &CommitWorkItemResultSubmission,
    ) -> Result<bool, DialogueStoreError> {
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let i = &c.preparation.invocation;
        if !start::completed_invocation(&mut tx, i.actor.tenant_id, i.invocation_id).await? {
            return Ok(false);
        }
        let (m, manifest, receipt) = receipts::load(&mut tx, i.run_context.run_id)
            .await?
            .ok_or(DialogueStoreError::Unavailable)?;
        if m != c.material
            || manifest != c.manifest_ref
            || receipt != c.result
            || m.manifest.scope.tenant_id != i.actor.tenant_id
            || m.manifest.scope.owner_id != i.actor.principal_id
            || m.manifest.scope.producing_agent_id != i.requesting_agent_id
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        crate::dialogue_codec::read_ready_content_reference(&mut tx, i.actor.tenant_id, &receipt)
            .await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(true)
    }
}
