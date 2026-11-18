//! Original Task acceptance proof and Tool success share one transaction. Only
//! the exact requester report Run can assess its own current Task result.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
impl PostgresWorkItemStartStore {
    pub(super) async fn prepare_verification_inner(
        &self,
        attempt: &AttemptContext,
        p: PrepareWorkItemVerification,
    ) -> Result<WorkItemVerificationMaterial, DialogueStoreError> {
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
        let result = load(&mut tx, &p, false).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(result)
    }
    pub(super) async fn commit_verification_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemVerification,
    ) -> Result<(), DialogueStoreError> {
        c.preparation.validate()?;
        let i = &c.preparation.invocation;
        let input = &c.preparation.input;
        if validate_work_item_verification_content(
            input,
            &c.material,
            &c.goal_bytes,
            &c.command_bytes,
        )? != c.decision
        {
            return Err(DialogueStoreError::Corrupt);
        }
        validate_work_item_acceptance_prompt(
            input,
            &c.material,
            &c.goal_bytes,
            c.human_prompt_bytes.as_deref(),
        )?;
        let bytes = work_item_verification_result_bytes(
            input,
            c.decision,
            c.material.human_acceptance,
            c.material.direct_result_contract,
        )?;
        if c.result.content_id != work_item_verification_result_id(i.invocation_id)
            || c.result.media_type.as_str() != "application/json"
            || c.result.byte_length != bytes.len() as u64
            || c.result.sha256 != Sha256::digest(&bytes)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if self.verification_replay(&c).await? {
            return Ok(());
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if start::completed_invocation(&mut tx, i.actor.tenant_id, i.invocation_id).await? {
            drop(tx);
            return if self.verification_replay(&c).await? {
                Ok(())
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        start::fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(&mut tx, state.tenant_id, i.invocation_id, attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let WorkItemScope::OwnedAgent(scope) = c.material.task.core.scope;
        preparation::lock_agents(&mut tx, i.actor, scope, scope.agent_id).await?;
        sqlx::query(
            "SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 FOR UPDATE",
        )
        .bind(input.task_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if load(&mut tx, &c.preparation, true).await? != c.material {
            return Err(DialogueStoreError::RevisionConflict);
        }
        for reference in std::iter::once(&c.material.task.goal_content_ref)
            .chain(std::iter::once(&c.material.report_ref))
            .chain(c.material.evidence.iter())
            .chain(c.material.command_requests.iter())
            .chain(c.material.human_prompt_ref.iter())
            .chain(std::iter::once(&i.request_ref))
        {
            lock_ready_content_reference(&mut tx, i.actor.tenant_id, reference).await?;
        }
        let mut record = c.material.task.clone();
        record.core = record
            .core
            .assess(
                WorkItemGoalRevision::new(input.goal_revision)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                WorkItemControlRevision::new(input.control_revision)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                AssignmentEpoch::new(input.assignment_epoch)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                c.decision == WorkItemVerificationDecision::Accepted,
                c.decision == WorkItemVerificationDecision::Failed,
            )
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        record.updated_at = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        crate::work_item_authority::commands::revision_and_outbox(
            &mut tx,
            &record,
            "verification_recorded",
        )
        .await
        .map_err(authority_error)?;
        sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
            .bind(input.task_id)
            .bind(integer(record.core.revisions.state.get())?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("INSERT INTO collaboration.work_item_commands(tenant_id,owner_id,command_id,request_hash,work_item_id,state_revision,operation,source_run_id,source_invocation_id,source_snapshot_id) VALUES($1,$2,$3,$4,$5,$6,'verify',$7,$8,$9)")
   .bind(i.actor.tenant_id.into_uuid()).bind(i.actor.principal_id.into_uuid()).bind(work_item_verification_id(i.invocation_id).into_uuid()).bind(i.request_ref.sha256.into_bytes().to_vec()).bind(input.task_id).bind(integer(record.core.revisions.state.get())?).bind(i.run_context.run_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(i.run_context.runtime_snapshot_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        let result = result(&c);
        crate::artifact::authorize_result_content(&mut tx, i.actor.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, state, attempt, i, &result, c.at).await
    }
    async fn verification_replay(
        &self,
        c: &CommitWorkItemVerification,
    ) -> Result<bool, DialogueStoreError> {
        let i = &c.preparation.invocation;
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("SELECT i.status,i.result_content_id,c.work_item_id,c.request_hash,t.erasure_job_id FROM tool.tool_invocations i LEFT JOIN collaboration.work_item_commands c ON c.source_invocation_id=i.invocation_id AND c.tenant_id=i.tenant_id AND c.owner_id=$3 AND c.operation='verify' LEFT JOIN collaboration.work_items t ON t.work_item_id=c.work_item_id WHERE i.tenant_id=$1 AND i.invocation_id=$2 AND i.run_id=$4 AND i.tool_ref IN ('tasks.verify@1','tasks.verify@2')")
   .bind(i.actor.tenant_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(i.actor.principal_id.into_uuid()).bind(i.run_context.run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
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
            .try_get::<Option<uuid::Uuid>, _>("result_content_id")
            .map_err(db)?
            != Some(c.result.content_id.into_uuid())
            || row
                .try_get::<Option<uuid::Uuid>, _>("work_item_id")
                .map_err(db)?
                != Some(c.preparation.input.task_id)
            || row
                .try_get::<Option<Vec<u8>>, _>("request_hash")
                .map_err(db)?
                != Some(i.request_ref.sha256.into_bytes().to_vec())
        {
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
fn result(c: &CommitWorkItemVerification) -> CapabilityResult {
    CapabilityResult {
        result_ref: c.result.clone(),
        result_hash: c.result.sha256,
        result_schema: c.result_schema.clone(),
        provider_request_ref: None,
    }
}
async fn load(
    tx: &mut PgConnection,
    p: &PrepareWorkItemVerification,
    lock: bool,
) -> Result<WorkItemVerificationMaterial, DialogueStoreError> {
    let i = &p.invocation;
    let a = &p.input;
    require_user(i.actor)?;
    let r=sqlx::query("SELECT t.space_id,t.agent_id,report.report_content_ref,tool.status AS tool_status,tool.tool_ref,command.state_revision AS accepted_state_revision FROM agent.dialogue_sessions s JOIN runtime.runs run ON run.tenant_id=s.tenant_id AND run.conversation_id=s.conversation_id JOIN tool.tool_invocations tool ON tool.tenant_id=run.tenant_id AND tool.run_id=run.run_id LEFT JOIN collaboration.work_item_commands command ON command.source_invocation_id=tool.invocation_id AND command.tenant_id=tool.tenant_id AND command.operation='verify' JOIN collaboration.work_items t ON t.work_item_id=s.work_item_id AND t.tenant_id=s.tenant_id AND t.owner_id=s.owner_id JOIN collaboration.work_item_execution_reports report ON report.producing_run_id=s.report_from_run_id AND report.work_item_id=t.work_item_id AND report.report_run_id=run.run_id WHERE run.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND s.agent_id=t.agent_id AND s.subject_kind='work_item_report' AND t.work_item_id=$5 AND s.report_from_run_id=$6 AND s.work_item_goal_revision=$7 AND s.work_item_control_revision=$8 AND s.work_item_assignment_epoch=$9 AND report.report_content_id=$10 AND report.outcome='completed' AND report.private_result_id IS NULL AND t.erasure_job_id IS NULL AND tool.invocation_id=$11 AND tool.tool_ref IN ('tasks.verify@1','tasks.verify@2') AND tool.request_content_id=$12 AND (tool.status=4 OR (run.status=2 AND run.cancel_requested_at_ms IS NULL))")
  .bind(i.run_context.run_id.into_uuid()).bind(i.actor.tenant_id.into_uuid()).bind(i.actor.principal_id.into_uuid()).bind(i.requesting_agent_id.into_uuid()).bind(a.task_id).bind(a.producing_run_id).bind(integer(a.goal_revision)?).bind(integer(a.control_revision)?).bind(integer(a.assignment_epoch)?).bind(a.report_content_id).bind(i.invocation_id.into_uuid()).bind(i.request_ref.content_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let scope = OwnedAgentWorkItemScope {
        owner_id: i.actor.principal_id,
        space_id: OwnerSpaceId::from_uuid(r.try_get("space_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        agent_id: i.requesting_agent_id,
    };
    let replay_revision = if r.try_get::<i16, _>("tool_status").map_err(db)? == 4 {
        Some(
            u64::try_from(
                r.try_get::<Option<i64>, _>("accepted_state_revision")
                    .map_err(db)?
                    .ok_or(DialogueStoreError::Corrupt)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?
            .checked_sub(1)
            .filter(|v| *v > 0)
            .ok_or(DialogueStoreError::Corrupt)?,
        )
    } else {
        None
    };
    let task = crate::work_item_authority::load(
        tx,
        i.actor,
        scope,
        WorkItemId::from_uuid(a.task_id).map_err(|_| DialogueStoreError::Corrupt)?,
        replay_revision,
    )
    .await
    .map_err(authority_error)?
    .ok_or(DialogueStoreError::Unavailable)?;
    if replay_revision.is_none() {
        if task.core.status != WorkItemStatus::Verifying {
            return Err(DialogueStoreError::RevisionConflict);
        }
        crate::dialogue_context::run::run_cut(tx, i.run_context.run_id, lock)
            .await?
            .ok_or(DialogueStoreError::Unavailable)?;
    }
    if task.core.revisions.goal.get() != a.goal_revision
        || task.core.revisions.control.get() != a.control_revision
        || task.core.assignment_epoch.map(AssignmentEpoch::get) != Some(a.assignment_epoch)
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let mut evidence = Vec::new();
    let mut review_evidence = Vec::new();
    for entry in a
        .criteria
        .iter()
        .flat_map(|c| c.evidence_entry_ids.iter())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let e=sqlx::query("SELECT e.content_ref,v.content_id,e.dialogue_id,d.agent_id,e.kind,e.artifact_id,e.artifact_version_id,v.version::text AS artifact_version FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=p.tenant_id AND e.owner_id=p.owner_id JOIN agent.agent_dialogues d ON d.dialogue_id=e.dialogue_id AND d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id LEFT JOIN artifact.artifact_versions v ON v.artifact_version_id=e.artifact_version_id AND v.artifact_id=e.artifact_id WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.source_run_id=$3 AND e.entry_id=$4 AND e.kind IN ('agent_message','artifact')")
   .bind(i.actor.tenant_id.into_uuid()).bind(i.actor.principal_id.into_uuid()).bind(a.producing_run_id).bind(entry).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let reference = match e.try_get::<Option<Vec<u8>>, _>("content_ref").map_err(db)? {
            Some(bytes) => content_from_bytes(bytes)?,
            None => {
                content(
                    tx,
                    i.actor.tenant_id,
                    e.try_get::<Option<uuid::Uuid>, _>("content_id")
                        .map_err(db)?
                        .ok_or(DialogueStoreError::Unavailable)?,
                )
                .await?
            }
        };
        review_evidence.push(musteragent_runtime_interaction::TaskReviewEvidence {
            entry_id: *entry,
            dialogue_id: e.try_get("dialogue_id").map_err(db)?,
            agent_id: e.try_get("agent_id").map_err(db)?,
            kind: match e.try_get::<&str, _>("kind").map_err(db)? {
                "agent_message" => musteragent_runtime_interaction::TaskReviewEvidenceKind::Message,
                "artifact" => musteragent_runtime_interaction::TaskReviewEvidenceKind::Artifact,
                _ => return Err(DialogueStoreError::Corrupt),
            },
            content_ref: (&reference).into(),
            artifact_id: e.try_get("artifact_id").map_err(db)?,
            artifact_version_id: e.try_get("artifact_version_id").map_err(db)?,
            artifact_version: e.try_get("artifact_version").map_err(db)?,
            display_name: None,
        });
        evidence.push(reference);
    }
    let rows:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT DISTINCT request_content_id FROM tool.tool_invocations i WHERE i.tenant_id=$1 AND (EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations x WHERE x.work_item_id=$2 AND x.source_invocation_id=i.invocation_id AND x.goal_revision<=$3) OR EXISTS(SELECT 1 FROM collaboration.work_item_commands c WHERE c.work_item_id=$2 AND c.source_invocation_id=i.invocation_id AND c.operation='amend' AND c.state_revision<=$4)) ORDER BY request_content_id LIMIT 1001")
  .bind(i.actor.tenant_id.into_uuid()).bind(a.task_id).bind(integer(task.core.revisions.goal.get())?).bind(integer(task.core.revisions.state.get())?).fetch_all(&mut *tx).await.map_err(db)?;
    if rows.is_empty() || rows.len() > 1000 {
        return Err(DialogueStoreError::Unavailable);
    }
    let mut commands = Vec::new();
    for id in rows {
        commands.push(content(tx, i.actor.tenant_id, id).await?);
    }
    Ok(WorkItemVerificationMaterial {
        task,
        report_ref: content_from_bytes(r.try_get("report_content_ref").map_err(db)?)?,
        evidence,
        command_requests: commands,
        direct_result_contract: r.try_get::<&str, _>("tool_ref").map_err(db)? == "tasks.verify@2",
        historical_receipt: r.try_get::<i16, _>("tool_status").map_err(db)? == 4
            && r.try_get::<&str, _>("tool_ref").map_err(db)? == "tasks.verify@1",
        human_acceptance: super::verification_interaction::load(tx, &p.invocation).await?,
        human_prompt_ref: super::verification_interaction::load_prompt(tx, &p.invocation).await?,
        review_evidence,
    })
}
pub(super) async fn content(
    tx: &mut PgConnection,
    tenant: TenantId,
    id: uuid::Uuid,
) -> Result<ContentRef, DialogueStoreError> {
    let r=sqlx::query("SELECT media_type,byte_length::text AS byte_length,content_hash FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2 AND state=2").bind(tenant.into_uuid()).bind(id).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    Ok(ContentRef {
        content_id: ContentId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt)?,
        media_type: MediaType::new(r.try_get::<String, _>("media_type").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        byte_length: r
            .try_get::<&str, _>("byte_length")
            .map_err(db)?
            .parse()
            .map_err(|_| DialogueStoreError::Corrupt)?,
        sha256: Sha256::from_bytes(r.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}
