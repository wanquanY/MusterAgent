//! Recoverable terminal evidence producer. The original Run and Publication
//! owners prove the result; the Task inbox only schedules its typed continuation.
use super::*;
mod device;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_telemetry::DbCommitKind;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct PostgresWorkItemReportStore {
    executions: PostgresWorkItemExecutionStore,
    content: Arc<dyn ContentStore>,
}
impl PostgresWorkItemReportStore {
    pub fn new(executions: PostgresWorkItemExecutionStore, content: Arc<dyn ContentStore>) -> Self {
        Self {
            executions,
            content,
        }
    }
    /// Idempotent across process loss before/after put_at and transaction commit.
    /// Only a complete terminal publication cut may create the immutable report.
    pub async fn produce_one(&self) -> Result<bool, DialogueStoreError> {
        let Some(claim) = self.claim().await? else {
            return Ok(false);
        };
        match self.produce(&claim).await {
            Ok(value) => Ok(value),
            Err(error) => {
                // Keep the obligation durable, but release this item with backoff so a
                // broken Content provider or invalid cut cannot starve unrelated Tasks.
                sqlx::query("UPDATE collaboration.work_item_outbox SET claim_owner=NULL,lease_expires_at_ms=NULL,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+5000,failure_code='report_evidence_unavailable' WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL")
     .bind(claim.event).bind(claim.owner).bind(claim.generation).execute(self.executions.admission.ledger.pool()).await.map_err(db)?;
                Err(error)
            }
        }
    }
    async fn claim(&self) -> Result<Option<ReportClaim>, DialogueStoreError> {
        let owner = uuid::Uuid::now_v7();
        let row=sqlx::query("WITH clock AS MATERIALIZED(SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint ms), candidate AS (
    SELECT q.event_seq,x.run_id FROM collaboration.work_item_outbox q
    JOIN collaboration.work_item_revisions accepted ON accepted.work_item_id=q.work_item_id AND accepted.state_revision=q.state_revision
    JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=q.work_item_id AND x.goal_revision=accepted.goal_revision AND x.control_revision=accepted.control_revision AND x.assignment_epoch=accepted.assignment_epoch
    LEFT JOIN runtime.runs r ON r.tenant_id=x.tenant_id AND r.run_id=x.run_id
    LEFT JOIN collaboration.work_item_device_custody device ON device.run_id=x.run_id
    LEFT JOIN collaboration.work_item_device_terminals terminal ON terminal.run_id=device.run_id
    JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id
    JOIN collaboration.work_item_revisions current ON current.work_item_id=t.work_item_id AND current.state_revision=t.state_revision,clock
    WHERE q.event_kind='execution_admitted' AND q.delivered_at_ms IS NULL AND q.available_at_ms<=clock.ms AND (q.claim_owner IS NULL OR q.lease_expires_at_ms<=clock.ms)
    AND ((r.status IN (5,6,7) AND device.run_id IS NULL) OR (r.run_id IS NULL AND device.admitted_snapshot_hash IS NOT NULL AND terminal.run_id IS NOT NULL)) AND t.erasure_job_id IS NULL AND current.goal_revision=x.goal_revision AND current.control_revision=x.control_revision AND current.assignment_epoch=x.assignment_epoch
    AND NOT EXISTS(SELECT 1 FROM runtime.leases l WHERE l.run_id=r.run_id)
    AND NOT EXISTS(SELECT 1 FROM agent.dialogue_terminal_deliveries d WHERE d.run_id=r.run_id AND d.state='pending')
    ORDER BY q.available_at_ms,q.event_seq LIMIT 1 FOR UPDATE OF q SKIP LOCKED), claimed AS (
    UPDATE collaboration.work_item_outbox q SET claim_owner=$1,claim_generation=q.claim_generation+1,attempt_count=q.attempt_count+1,lease_expires_at_ms=clock.ms+120000 FROM candidate,clock WHERE q.event_seq=candidate.event_seq RETURNING q.event_seq,q.claim_generation,candidate.run_id)
    SELECT * FROM claimed")
   .bind(owner).fetch_optional(self.executions.admission.ledger.pool()).await.map_err(db)?;
        row.map(|r| {
            Ok(ReportClaim {
                event: r.try_get("event_seq").map_err(db)?,
                generation: r.try_get("claim_generation").map_err(db)?,
                owner,
                run: RunId::from_uuid(r.try_get("run_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            })
        })
        .transpose()
    }
    async fn private_items(
        &self,
        source: &Candidate,
        run: RunId,
    ) -> Result<Vec<serde_json::Value>, DialogueStoreError> {
        let Some(submission) = &source.private_result else {
            return Ok(Vec::new());
        };
        let items = &submission.manifest.items;
        let file_call: Option<String> = if source.requester_uses_result
            && items.iter().any(|item| item.asset.is_some())
        {
            sqlx::query_scalar("SELECT submission_call_id FROM collaboration.work_item_result_submissions WHERE producing_run_id=$1")
                .bind(run.into_uuid()).fetch_one(self.executions.admission.ledger.pool()).await.map_err(db)?
        } else {
            None
        };
        let mut returned = Vec::with_capacity(items.len());
        for item in items {
            let reference = &item.content_ref;
            let text = if item.asset.is_none() {
                let bytes = self
                    .content
                    .read(source.request.actor.tenant_id, reference)
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?;
                if bytes.len() as u64 != reference.byte_length
                    || Sha256::digest(&bytes) != reference.sha256
                {
                    return Err(DialogueStoreError::Corrupt);
                }
                Some(String::from_utf8(bytes).map_err(|_| DialogueStoreError::Corrupt)?)
            } else {
                None
            };
            let value = if source.requester_uses_result {
                if let Some(asset) = &item.asset {
                    json!({"kind":"artifact","content_id":reference.content_id.to_string(),"delivery_reference":{
                        "artifact_ref":{"kind":"artifact","id":asset.artifact_id.to_string()},
                        "artifact_tool_call_id":file_call.as_ref().ok_or(DialogueStoreError::Corrupt)?}})
                } else {
                    json!({"kind":"message","text":text})
                }
            } else {
                json!({"item_id":item.item_id,"kind":if item.asset.is_some(){"artifact"}else{"message"},"text":text,"content_ref":reference_json(reference),
                    "artifact_id":item.asset.as_ref().map(|a|a.artifact_id.to_string()),"artifact_version_id":item.asset.as_ref().map(|a|a.artifact_version_id.to_string())})
            };
            returned.push(value);
        }
        Ok(returned)
    }

    async fn produce(&self, claim: &ReportClaim) -> Result<bool, DialogueStoreError> {
        let pool = self.executions.admission.ledger.pool();
        let run = claim.run;
        let mut connection = pool.acquire().await.map_err(db)?;
        let initial = load(&mut connection, run).await?;
        drop(connection);
        let mut values = Vec::new();
        for p in &initial.publications {
            let text = if let Some(reference) = &p.content {
                if reference.byte_length > 2 * 1024 * 1024 {
                    return Err(DialogueStoreError::Unavailable);
                }
                let bytes = self
                    .content
                    .read(initial.request.actor.tenant_id, reference)
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?;
                if bytes.len() as u64 != reference.byte_length
                    || Sha256::digest(&bytes) != reference.sha256
                {
                    return Err(DialogueStoreError::Corrupt);
                }
                Some(String::from_utf8(bytes).map_err(|_| DialogueStoreError::Corrupt)?)
            } else {
                None
            };
            values.push(json!({"entry_id":p.entry.to_string(),"purpose":p.purpose,"kind":p.kind,"text":text,"artifact_id":p.artifact.map(|x|x.to_string()),"artifact_version_id":p.version.map(|x|x.to_string()),"content_ref":p.content.as_ref().map(reference_json)}));
        }
        // The continuation receives the exact Task-owned acceptance criteria, not
        // an unrelated source Run copy. This report is in the same erasure closure.
        if initial.goal.byte_length > 65536 {
            return Err(DialogueStoreError::Corrupt);
        }
        let goal_bytes = self
            .content
            .read(initial.request.actor.tenant_id, &initial.goal)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        if goal_bytes.len() as u64 != initial.goal.byte_length
            || Sha256::digest(&goal_bytes) != initial.goal.sha256
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let goal: serde_json::Value =
            serde_json::from_slice(&goal_bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        let mut report = if initial.requester_uses_result {
            let items = self.private_items(&initial, run).await?;
            json!({"schema":"musteragent.task-report.v3","goal":goal,"outcome":initial.outcome.as_str(),"items":items})
        } else {
            let mut report = json!({"schema":"musteragent.task-report.v1","goal":goal,"task_id":initial.request.work_item_id.to_string(),
                "producing_run_id":run.to_string(),"producing_agent_id":initial.producing_agent.to_string(),"outcome":initial.outcome.as_str()});
            if initial.verified_result_protocol {
                report["schema"] = json!("musteragent.task-report.v2");
                report["result"] = if let Some(submission) = &initial.private_result {
                    let receipt = submission.manifest.receipt()?;
                    let items = self.private_items(&initial, run).await?;
                    json!({"result_id":receipt.result_id,"manifest_sha256":receipt.manifest_sha256.to_string(),"items":items})
                } else {
                    serde_json::Value::Null
                };
            } else {
                report["deliveries"] = json!(values);
            }
            report
        };
        if initial.producer_home == "device"
            && initial.outcome != WorkItemExecutionReportOutcome::Completed
        {
            // These are all confirmed Cloud publications, not a claim that
            // every external effect succeeded or stopped. Never retry an
            // uncertain action solely because no delivery appears here.
            report["execution_observation"] = json!({
                "local_attempt_ended": true,
                "external_outcomes_may_be_unconfirmed": true,
                "require_original_result_check_before_repeating_effects": true
            });
        }
        let body = serde_json::to_vec(&report).map_err(|_| DialogueStoreError::Corrupt)?;
        if body.len() > 8 * 1024 * 1024 {
            return Err(DialogueStoreError::Unavailable);
        }
        let content_id = ContentId::from_uuid(uuid::Uuid::new_v5(
            &run.into_uuid(),
            if initial.requester_uses_result {
                b"musteragent.task-report-content.v3"
            } else if initial.verified_result_protocol {
                b"musteragent.task-report-content.v2"
            } else {
                b"musteragent.task-report-content.v1"
            },
        ))
        .map_err(|_| DialogueStoreError::Corrupt)?;
        let reference = self
            .content
            .put_reclaimable_at(
                content_id,
                PutContent {
                    tenant_id: initial.request.actor.tenant_id,
                    media_type: MediaType::new("application/vnd.musteragent.task-report+json")
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: body,
                },
            )
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let device_result = initial.private_result.as_ref().filter(|_| {
            !initial.requester_uses_result
                && initial.producer_home == "device"
                && initial.outcome == WorkItemExecutionReportOutcome::Completed
        });
        if let Some(result) = device_result {
            super::result_receipts::lock_current(&mut tx, &result.manifest.scope).await?;
        } else {
            preparation::lock_agents(
                &mut tx,
                initial.request.actor,
                initial.scope,
                initial.producing_agent,
            )
            .await?;
            sqlx::query(
            "SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 FOR UPDATE",
        )
        .bind(initial.request.work_item_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        }
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_outbox WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint)")
   .bind(claim.event).bind(claim.owner).bind(claim.generation).fetch_one(&mut *tx).await.map_err(db)?;
        if !valid {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let current = load(&mut tx, run).await?;
        if current != initial {
            return Err(DialogueStoreError::RevisionConflict);
        }
        if sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_execution_reports WHERE producing_run_id=$1)").bind(run.into_uuid()).fetch_one(&mut *tx).await.map_err(db)? {ack(&mut tx,claim).await?;tx.commit().await.map_err(|_|DialogueStoreError::Infrastructure)?;return Ok(true);}
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let mut record = crate::work_item_authority::load(
            &mut tx,
            initial.request.actor,
            initial.scope,
            initial.request.work_item_id,
            None,
        )
        .await
        .map_err(authority_error)?
        .ok_or(DialogueStoreError::Unavailable)?;
        let request = &initial.request;
        record.core = if device_result.is_some() {
            record.core.device_result_delivered(
                request.goal_revision,
                request.control_revision,
                request.assignment_epoch,
            )
        } else {
            record.core.execution_reported(
                request.goal_revision,
                request.control_revision,
                request.assignment_epoch,
                initial.outcome == WorkItemExecutionReportOutcome::Completed,
                initial.outcome == WorkItemExecutionReportOutcome::Cancelled,
            )
        }
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
        record.updated_at = now;
        let d = sqlx::query(
            "SELECT epoch,head_seq FROM agent.agent_dialogues WHERE dialogue_id=$1 AND agent_id=$2",
        )
        .bind(initial.source_dialogue.into_uuid())
        .bind(initial.scope.agent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let cut = crate::dialogue_context::load_cut(
            &mut tx,
            initial.request.actor,
            initial.scope.agent_id,
            initial.source_dialogue,
            DialogueCursor {
                epoch: unsigned(d.try_get("epoch").map_err(db)?)?,
                seq: unsigned(d.try_get("head_seq").map_err(db)?)?,
            },
        )
        .await?;
        let key = WorkItemExecutionKey {
            actor: initial.request.actor,
            scope: initial.scope,
            work_item_id: initial.request.work_item_id,
            goal_revision: initial.request.goal_revision,
            control_revision: initial.request.control_revision,
            assignment_epoch: initial.request.assignment_epoch,
            executing_agent_id: initial.scope.agent_id,
            report_from_run_id: Some(run),
        };
        let ids = key.identities();
        retain_content_refs(
            &mut tx,
            initial.request.actor,
            initial.scope.agent_id,
            vec![("work_item_report", run.into_uuid(), &reference)],
        )
        .await?;
        let sealed_result = initial
            .private_result
            .as_ref()
            .map(|m| m.manifest.receipt())
            .transpose()?;
        sqlx::query("INSERT INTO collaboration.work_item_execution_reports(producing_run_id,producing_snapshot_id,producing_agent_id,tenant_id,owner_id,work_item_id,goal_revision,control_revision,assignment_epoch,requesting_agent_id,source_dialogue_id,outcome,report_content_ref,report_content_id,report_sha256,publication_sha256,context_epoch,context_seq,context_chain_sha256,report_command_id,report_conversation_id,report_run_id,state_revision,created_at_ms,producer_home,private_result_id,private_result_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)")
   .bind(run.into_uuid()).bind(initial.snapshot.into_uuid()).bind(initial.producing_agent.into_uuid()).bind(initial.request.actor.tenant_id.into_uuid()).bind(initial.scope.owner_id.into_uuid()).bind(initial.request.work_item_id.into_uuid()).bind(integer(initial.request.goal_revision.get())?).bind(integer(initial.request.control_revision.get())?).bind(integer(initial.request.assignment_epoch.get())?).bind(initial.scope.agent_id.into_uuid()).bind(initial.source_dialogue.into_uuid()).bind(initial.outcome.as_str()).bind(content_bytes(&reference)).bind(reference.content_id.into_uuid()).bind(reference.sha256.into_bytes().to_vec()).bind(initial.digest.into_bytes().to_vec()).bind(integer(cut.through.epoch)?).bind(integer(cut.through.seq)?).bind(cut.source_chain_sha256.into_bytes().to_vec()).bind(ids.command_id.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(ids.run_id()?.into_uuid()).bind(integer(record.core.revisions.state.get())?).bind(now.unix_millis()).bind(initial.producer_home).bind(sealed_result.as_ref().map(|r|r.result_id)).bind(sealed_result.as_ref().map(|r|r.manifest_sha256.into_bytes().to_vec())).execute(&mut *tx).await.map_err(db)?;
        crate::work_item_authority::commands::revision_and_outbox(
            &mut tx,
            &record,
            "execution_reported",
        )
        .await
        .map_err(authority_error)?;
        sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
            .bind(initial.request.work_item_id.into_uuid())
            .bind(integer(record.core.revisions.state.get())?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        if let Some(result) = device_result {
            let proof = super::result_verification::AcceptedWorkItemResult::from_device_terminal(
                &result.manifest,
                record.core.revisions.state.get(),
            );
            crate::dialogue_publication::task_result::promote(&mut tx, &proof).await?;
        } else {
            let inbox=sqlx::query("UPDATE agent.owned_agent_inboxes SET head_seq=head_seq+1 WHERE agent_id=$1 AND dialogue_id=$2 RETURNING epoch,head_seq").bind(initial.scope.agent_id.into_uuid()).bind(initial.source_dialogue.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            sqlx::query("INSERT INTO agent.owned_agent_inbox_entries(tenant_id,owner_id,agent_id,dialogue_id,epoch,seq,report_from_run_id,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)").bind(initial.request.actor.tenant_id.into_uuid()).bind(initial.scope.owner_id.into_uuid()).bind(initial.scope.agent_id.into_uuid()).bind(initial.source_dialogue.into_uuid()).bind(inbox.try_get::<i64,_>("epoch").map_err(db)?).bind(inbox.try_get::<i64,_>("head_seq").map_err(db)?).bind(run.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        }
        sqlx::query(
            "UPDATE collaboration.work_item_outbox SET report_from_run_id=$3,delivered_at_ms=$4
            WHERE work_item_id=$1 AND state_revision=$2 AND event_kind='execution_reported'",
        )
        .bind(initial.request.work_item_id.into_uuid())
        .bind(integer(record.core.revisions.state.get())?)
        .bind(run.into_uuid())
        .bind(device_result.map(|_| now.unix_millis()))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        ack(&mut tx, claim).await?;
        tx.commit()
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(true)
    }
}
struct ReportClaim {
    event: i64,
    generation: i64,
    owner: uuid::Uuid,
    run: RunId,
}
async fn ack(tx: &mut PgConnection, c: &ReportClaim) -> Result<(), DialogueStoreError> {
    let rows=sqlx::query("UPDATE collaboration.work_item_outbox SET delivered_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,claim_owner=NULL,lease_expires_at_ms=NULL,failure_code=NULL WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
  .bind(c.event).bind(c.owner).bind(c.generation).execute(&mut *tx).await.map_err(db)?.rows_affected();
    if rows != 1 {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
#[derive(Debug, Clone, Eq, PartialEq)]
struct Candidate {
    producer_home: &'static str,
    goal: ContentRef,
    request: WorkItemExecutionRequest,
    scope: OwnedAgentWorkItemScope,
    source_dialogue: AgentDialogueId,
    producing_agent: AgentId,
    snapshot: RuntimeSnapshotId,
    outcome: WorkItemExecutionReportOutcome,
    requester_uses_result: bool,
    verified_result_protocol: bool,
    private_result: Option<WorkItemResultSubmissionMaterial>,
    publications: Vec<Publication>,
    digest: Sha256,
}
#[derive(Debug, Clone, Eq, PartialEq)]
struct Publication {
    entry: DialogueEntryId,
    purpose: String,
    kind: String,
    content: Option<ContentRef>,
    artifact: Option<uuid::Uuid>,
    version: Option<uuid::Uuid>,
}
fn reference_json(r: &ContentRef) -> serde_json::Value {
    json!({"content_id":r.content_id.to_string(),"media_type":r.media_type.as_str(),"byte_length":r.byte_length,"sha256":r.sha256.to_string()})
}
async fn load(tx: &mut PgConnection, run: RunId) -> Result<Candidate, DialogueStoreError> {
    let is_device: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_custody WHERE run_id=$1)",
    )
    .bind(run.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    if is_device {
        return device::load(tx, run).await;
    }
    let r=sqlx::query("SELECT x.*,v.goal_content_ref,t.source_dialogue_id,r.runtime_snapshot_id,r.status FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id JOIN collaboration.work_item_revisions v ON v.work_item_id=t.work_item_id AND v.state_revision=t.state_revision JOIN agent.dialogue_sessions s ON s.conversation_id=x.conversation_id AND s.subject_kind='work_item' JOIN runtime.runs r ON r.tenant_id=x.tenant_id AND r.run_id=x.run_id AND r.conversation_id=x.conversation_id WHERE x.run_id=$1 AND r.status IN (5,6,7) AND t.erasure_job_id IS NULL AND v.goal_revision=x.goal_revision AND v.control_revision=x.control_revision AND v.assignment_epoch=x.assignment_epoch AND NOT EXISTS(SELECT 1 FROM runtime.leases l WHERE l.run_id=r.run_id) AND NOT EXISTS(SELECT 1 FROM agent.dialogue_terminal_deliveries d WHERE d.run_id=r.run_id AND d.state='pending')").bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    macro_rules! id {
        ($t:ty,$n:literal) => {
            <$t>::from_uuid(r.try_get($n).map_err(db)?).map_err(|_| DialogueStoreError::Corrupt)?
        };
    }
    let actor = Actor {
        tenant_id: id!(TenantId, "tenant_id"),
        principal_id: id!(PrincipalId, "owner_id"),
        principal_kind: PrincipalKind::User,
    };
    let scope = OwnedAgentWorkItemScope {
        owner_id: actor.principal_id,
        space_id: id!(OwnerSpaceId, "space_id"),
        agent_id: id!(AgentId, "requesting_agent_id"),
    };
    let request = WorkItemExecutionRequest {
        actor,
        work_item_id: id!(WorkItemId, "work_item_id"),
        goal_revision: WorkItemGoalRevision::new(unsigned(
            r.try_get("goal_revision").map_err(db)?,
        )?)
        .map_err(|_| DialogueStoreError::Corrupt)?,
        control_revision: WorkItemControlRevision::new(unsigned(
            r.try_get("control_revision").map_err(db)?,
        )?)
        .map_err(|_| DialogueStoreError::Corrupt)?,
        assignment_epoch: AssignmentEpoch::new(unsigned(
            r.try_get("assignment_epoch").map_err(db)?,
        )?)
        .map_err(|_| DialogueStoreError::Corrupt)?,
        report_from_run_id: Some(run),
    };
    let outcome = match r.try_get::<i16, _>("status").map_err(db)? {
        5 => WorkItemExecutionReportOutcome::Completed,
        6 => WorkItemExecutionReportOutcome::Failed,
        7 => WorkItemExecutionReportOutcome::Cancelled,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let verified_result_protocol =
        r.try_get::<&str, _>("result_protocol").map_err(db)? == "verified_result_v1";
    let private_result = if verified_result_protocol {
        super::result_submission::load_submission(tx, run).await?
    } else {
        None
    };
    if verified_result_protocol
        && outcome == WorkItemExecutionReportOutcome::Completed
        && private_result.is_none()
    {
        return Err(DialogueStoreError::Unavailable);
    }
    let rows=sqlx::query("SELECT p.entry_id,p.purpose,p.payload_sha256,e.kind,e.content_ref,e.artifact_id,e.artifact_version_id FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=p.tenant_id AND e.owner_id=p.owner_id WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.source_run_id=$3 ORDER BY e.epoch,e.seq LIMIT 1001").bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(run.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
    if rows.len() > 1000 {
        return Err(DialogueStoreError::Unavailable);
    }
    let mut publications = Vec::new();
    let mut proof = b"musteragent.work-item-publication-cut.v1\0".to_vec();
    for p in rows {
        let entry = DialogueEntryId::from_uuid(p.try_get("entry_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        proof.extend_from_slice(entry.into_uuid().as_bytes());
        proof.extend(p.try_get::<Vec<u8>, _>("payload_sha256").map_err(db)?);
        publications.push(Publication {
            entry,
            purpose: p.try_get("purpose").map_err(db)?,
            kind: p.try_get("kind").map_err(db)?,
            content: p
                .try_get::<Option<Vec<u8>>, _>("content_ref")
                .map_err(db)?
                .map(content_from_bytes)
                .transpose()?,
            artifact: p.try_get("artifact_id").map_err(db)?,
            version: p.try_get("artifact_version_id").map_err(db)?,
        });
    }
    if !verified_result_protocol
        && outcome == WorkItemExecutionReportOutcome::Completed
        && !publications
            .iter()
            .any(|p| matches!(p.purpose.as_str(), "result" | "reply" | "blocked"))
    {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(Candidate {
        requester_uses_result: r.try_get("requester_uses_result").map_err(db)?,
        producer_home: "cloud",
        goal: content_from_bytes(r.try_get("goal_content_ref").map_err(db)?)?,
        request,
        scope,
        source_dialogue: id!(AgentDialogueId, "source_dialogue_id"),
        producing_agent: id!(AgentId, "executing_agent_id"),
        snapshot: id!(RuntimeSnapshotId, "runtime_snapshot_id"),
        outcome,
        verified_result_protocol,
        private_result,
        publications,
        digest: Sha256::digest(proof),
    })
}
