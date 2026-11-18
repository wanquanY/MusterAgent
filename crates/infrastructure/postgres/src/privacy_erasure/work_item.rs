//! WorkItem admission extends the original Privacy Erasure aggregate. No
//! task-deletion status and no execution-side provenance inference lives here.
use super::{insert_plan_exception, insert_root_resource, insert_transition};
use crate::{PostgresPrivacyErasureError as Error, PostgresPrivacyErasureStore};
use musteragent_content_ref::ContentRef;
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_privacy::*;
use sqlx::Row;
mod contents;
mod coverage;
mod eligibility;
mod execution;
mod execution_receipt;
mod frozen;
mod private_payload;
mod provider;
mod reports;
mod source_commands;
mod source_evidence;
pub(crate) use coverage::WorkItemProviderScope;
pub(super) use provider::verify_work_item_receipt;

impl WorkItemErasureAdmissionStore for PostgresPrivacyErasureStore {
    type Error = Error;
    fn request_work_item_erasure(
        &self,
        command: RequestWorkItemErasure,
    ) -> ApplicationFuture<'_, Result<WorkItemErasureAdmission, Error>> {
        Box::pin(self.admit_work_item_erasure(command))
    }
}
impl PostgresPrivacyErasureStore {
    async fn admit_work_item_erasure(
        &self,
        c: RequestWorkItemErasure,
    ) -> Result<WorkItemErasureAdmission, Error> {
        c.validate()?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        // Same order as configuration and WorkItem mutation. Privacy governance
        // continues after archive/soft-delete; it never reactivates business use.
        let space: Option<uuid::Uuid> = sqlx::query_scalar("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(c.space_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await?;
        if space.is_none() {
            return Err(Error::WorkItemUnauthorized);
        }
        let dialogue: Option<uuid::Uuid> = sqlx::query_scalar("SELECT dialogue_id FROM agent.agent_dialogues WHERE agent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(c.agent_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await?;
        let agent: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT agent_id FROM agent.owned_agents WHERE agent_id=$1 AND space_id=$2 FOR UPDATE",
        )
        .bind(c.agent_id.into_uuid())
        .bind(c.space_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?;
        if dialogue.is_none() || agent.is_none() {
            return Err(Error::WorkItemUnauthorized);
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "{}:privacy-erasure:{}",
                c.actor.tenant_id,
                c.idempotency_key.as_str()
            ))
            .execute(&mut *tx)
            .await?;
        let existing: Option<uuid::Uuid> = sqlx::query_scalar("SELECT r.erasure_job_id FROM privacy_erasure.requests r LEFT JOIN privacy_erasure.work_item_lineage l USING(tenant_id,erasure_job_id) WHERE r.tenant_id=$1 AND (r.idempotency_key=$2 OR l.work_item_id=$3) ORDER BY r.erasure_job_id LIMIT 1")
            .bind(c.actor.tenant_id.into_uuid()).bind(c.idempotency_key.as_str()).bind(c.work_item_id.into_uuid())
            .fetch_optional(&mut *tx).await?;
        if let Some(id) = existing {
            let id =
                ErasureJobId::from_uuid(id).map_err(|_| Error::CorruptState("invalid job id"))?;
            let lineage = load_lineage(&mut tx, c.actor.tenant_id, id)
                .await?
                .ok_or(Error::IdempotencyConflict)?;
            tx.rollback().await?;
            let job = self
                .load(c.actor.tenant_id, id)
                .await?
                .ok_or(Error::JobNotFound)?;
            if !c.matches_admitted_intent(&job, &lineage)? {
                return Err(Error::IdempotencyConflict);
            }
            return Ok(WorkItemErasureAdmission {
                disposition: ErasureAppendDisposition::Duplicate,
                lineage,
                job,
            });
        }
        let row = sqlx::query("SELECT t.source_dialogue_id,t.source_intent_id,t.state_revision,r.control_revision,r.updated_at_ms,t.erasure_job_id FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.space_id=$4 AND t.agent_id=$5 FOR UPDATE OF t")
            .bind(c.work_item_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid())
            .bind(c.space_id.into_uuid()).bind(c.agent_id.into_uuid()).fetch_optional(&mut *tx).await?
            .ok_or(Error::WorkItemUnauthorized)?;
        let state: i64 = row.try_get("state_revision")?;
        let control: i64 = row.try_get("control_revision")?;
        if u64::try_from(state).ok() != Some(c.expected_state_revision)
            || u64::try_from(control).ok() != Some(c.expected_control_revision)
            || row
                .try_get::<Option<uuid::Uuid>, _>("erasure_job_id")?
                .is_some()
            || c.requested_at.unix_millis() < row.try_get::<i64, _>("updated_at_ms")?
        {
            return Err(Error::WorkItemRevisionConflict);
        }
        let source_intent_id = UserIntentId::from_uuid(row.try_get("source_intent_id")?)
            .map_err(|_| Error::CorruptState("invalid source intent"))?;
        let executed = match eligibility::require_unexecuted(
            &mut tx,
            c.work_item_id,
            source_intent_id,
        )
        .await
        {
            Ok(()) => false,
            Err(Error::WorkItemExecutionClosureRequired) => {
                execution::require_reserved_task(&mut tx, &c).await?;
                true
            }
            Err(error) => return Err(error),
        };
        let (revision_count, goal_content_refs) =
            load_goal_refs(&mut tx, c.work_item_id, state).await?;
        if !executed {
            eligibility::require_exclusive_roots(
                &mut tx,
                c.actor,
                c.agent_id,
                c.work_item_id,
                &goal_content_refs,
            )
            .await?;
        }
        let lineage = WorkItemErasureLineage {
            actor: c.actor,
            erasure_job_id: ErasureJobId::generate(),
            space_id: c.space_id,
            agent_id: c.agent_id,
            work_item_id: c.work_item_id,
            source_dialogue_id: AgentDialogueId::from_uuid(row.try_get("source_dialogue_id")?)
                .map_err(|_| Error::CorruptState("invalid dialogue"))?,
            source_intent_id,
            state_revision: c.expected_state_revision,
            control_revision: c.expected_control_revision,
            revision_count,
            goal_content_refs,
            reason_code: c.reason_code.clone(),
            frozen_at: c.requested_at,
        };
        let (request, plan) = if executed {
            build_work_item_execution_erasure_facts(&c, &lineage)?
        } else {
            build_work_item_erasure_facts(&c, &lineage)?
        };
        persist_facts(&mut tx, &request, &plan).await?;
        sqlx::query("INSERT INTO privacy_erasure.work_item_lineage(tenant_id,erasure_job_id,owner_id,space_id,agent_id,work_item_id,source_dialogue_id,source_intent_id,state_revision,control_revision,revision_count,reason_code,frozen_at_ms,lineage_fingerprint) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(c.actor.tenant_id.into_uuid()).bind(lineage.erasure_job_id.into_uuid()).bind(c.actor.principal_id.into_uuid())
            .bind(c.space_id.into_uuid()).bind(c.agent_id.into_uuid()).bind(c.work_item_id.into_uuid())
            .bind(lineage.source_dialogue_id.into_uuid()).bind(source_intent_id.into_uuid()).bind(state).bind(control)
            .bind(i64::try_from(revision_count).map_err(|_| Error::IntegerOverflow("revision count"))?)
            .bind(c.reason_code.as_str()).bind(c.requested_at.unix_millis()).bind(lineage.fingerprint().into_bytes().as_slice())
            .execute(&mut *tx).await?;
        let fenced_state = state
            .checked_add(1)
            .ok_or(Error::IntegerOverflow("state revision"))?;
        let fenced_control = control
            .checked_add(1)
            .ok_or(Error::IntegerOverflow("control revision"))?;
        sqlx::query("INSERT INTO collaboration.work_item_revisions(work_item_id,state_revision,goal_revision,control_revision,status,assignment_epoch,goal_content_id,goal_content_ref,dependencies,priority,follow_up_of,updated_at_ms) SELECT work_item_id,$3,goal_revision,$4,status,assignment_epoch,goal_content_id,goal_content_ref,dependencies,priority,follow_up_of,$5 FROM collaboration.work_item_revisions WHERE work_item_id=$1 AND state_revision=$2")
            .bind(c.work_item_id.into_uuid()).bind(state).bind(fenced_state).bind(fenced_control).bind(c.requested_at.unix_millis()).execute(&mut *tx).await?;
        sqlx::query("UPDATE collaboration.work_items SET erasure_job_id=$2,state_revision=$3 WHERE work_item_id=$1")
            .bind(c.work_item_id.into_uuid()).bind(lineage.erasure_job_id.into_uuid()).bind(fenced_state).execute(&mut *tx).await?;
        if !executed {
            crate::content::tombstone_in_transaction(
                &mut tx,
                c.actor.tenant_id,
                &lineage.goal_content_refs,
            )
            .await
            .map_err(Error::Content)?;
        }
        // v2 bytes are fenced only after producer quiescence and frozen,
        // exclusive provider coverage; the Task itself is already permanent.
        sqlx::query("INSERT INTO collaboration.work_item_outbox(work_item_id,state_revision,event_kind,occurred_at_ms) VALUES($1,$2,'privacy_fenced',$3)")
            .bind(c.work_item_id.into_uuid()).bind(fenced_state).bind(c.requested_at.unix_millis()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO privacy_erasure.execution_work(tenant_id,erasure_job_id,available_at_ms) VALUES($1,$2,$3)")
            .bind(c.actor.tenant_id.into_uuid()).bind(lineage.erasure_job_id.into_uuid()).bind(c.requested_at.unix_millis()).execute(&mut *tx).await?;
        tx.commit().await?;
        let job = self
            .load(c.actor.tenant_id, lineage.erasure_job_id)
            .await?
            .ok_or(Error::JobNotFound)?;
        Ok(WorkItemErasureAdmission {
            disposition: ErasureAppendDisposition::Applied,
            lineage,
            job,
        })
    }
}

async fn persist_facts(
    tx: &mut sqlx::PgConnection,
    r: &ErasureRequest,
    p: &ErasurePlanSnapshot,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO privacy_erasure.requests(erasure_job_id,schema_version,tenant_id,actor_principal_id,actor_principal_kind,retention_policy_ref,idempotency_key,requested_at_ms,complete_by_ms,root_resource_count,request_fingerprint) VALUES($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10)")
        .bind(r.erasure_job_id.into_uuid()).bind(r.schema_version.as_str()).bind(r.tenant_id.into_uuid()).bind(r.requested_by.principal_id.into_uuid())
        .bind(r.retention_policy_ref.as_str()).bind(r.idempotency_key.as_str()).bind(r.requested_at.unix_millis()).bind(r.complete_by.unix_millis())
        .bind(i16::try_from(r.root_resource_refs.len()).map_err(|_|Error::IntegerOverflow("roots"))?).bind(r.fingerprint()?.into_bytes().as_slice())
        .execute(&mut *tx).await?;
    for (i, root) in r.root_resource_refs.iter().enumerate() {
        insert_root_resource(tx, r.tenant_id, r.erasure_job_id, i, root).await?;
    }
    insert_transition(
        tx,
        r.tenant_id,
        r.erasure_job_id,
        ErasureTransition {
            sequence: ErasureTransitionSequence::new(1)?,
            status: ErasureJobStatus::PendingPlan,
            occurred_at: r.requested_at,
        },
    )
    .await?;
    sqlx::query("INSERT INTO privacy_erasure.plans(tenant_id,erasure_job_id,plan_version,schema_version,data_class_count,retention_exception_count,lineage_fingerprint,plan_hash,frozen_at_ms) VALUES($1,$2,1,$3,$4,$5,$6,$7,$8)")
        .bind(r.tenant_id.into_uuid()).bind(r.erasure_job_id.into_uuid()).bind(p.schema_version.as_str()).bind(p.data_classes.len() as i16).bind(p.retention_exceptions.len() as i16)
        .bind(p.lineage_fingerprint.into_bytes().as_slice()).bind(p.plan_hash.into_bytes().as_slice()).bind(p.frozen_at.unix_millis()).execute(&mut *tx).await?;
    for class in &p.data_classes {
        sqlx::query("INSERT INTO privacy_erasure.plan_data_classes(tenant_id,erasure_job_id,plan_version,data_class) VALUES($1,$2,1,$3)")
            .bind(r.tenant_id.into_uuid()).bind(r.erasure_job_id.into_uuid()).bind(class.code()).execute(&mut *tx).await?;
    }
    for exception in &p.retention_exceptions {
        insert_plan_exception(tx, r.tenant_id, p, exception).await?;
    }
    for (seq, status) in [
        (2, ErasureJobStatus::Planned),
        (3, ErasureJobStatus::Fenced),
    ] {
        insert_transition(
            tx,
            r.tenant_id,
            r.erasure_job_id,
            ErasureTransition {
                sequence: ErasureTransitionSequence::new(seq)?,
                status,
                occurred_at: r.requested_at,
            },
        )
        .await?;
    }
    Ok(())
}

async fn load_goal_refs(
    tx: &mut sqlx::PgConnection,
    id: WorkItemId,
    through: i64,
) -> Result<(u64, Vec<ContentRef>), Error> {
    let rows=sqlx::query("SELECT state_revision,goal_content_id,goal_content_ref FROM collaboration.work_item_revisions WHERE work_item_id=$1 AND state_revision<=$2 ORDER BY state_revision")
        .bind(id.into_uuid()).bind(through).fetch_all(&mut *tx).await?;
    let mut roots = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        if row.try_get::<i64, _>("state_revision")? != index as i64 + 1 {
            return Err(Error::CorruptState("incomplete task revision history"));
        }
        let reference = crate::dialogue_codec::content_from_bytes(row.try_get("goal_content_ref")?)
            .map_err(|_| Error::CorruptState("invalid goal ref"))?;
        if reference.content_id.into_uuid() != row.try_get::<uuid::Uuid, _>("goal_content_id")? {
            return Err(Error::CorruptState("goal ref identity mismatch"));
        }
        if !roots.contains(&reference) {
            roots.push(reference);
        }
    }
    if rows.len() as i64 != through {
        return Err(Error::CorruptState("incomplete task revision history"));
    }
    Ok((rows.len() as u64, roots))
}

async fn load_lineage(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    job: ErasureJobId,
) -> Result<Option<WorkItemErasureLineage>, Error> {
    let Some(row) = sqlx::query(
        "SELECT * FROM privacy_erasure.work_item_lineage WHERE tenant_id=$1 AND erasure_job_id=$2",
    )
    .bind(tenant.into_uuid())
    .bind(job.into_uuid())
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(None);
    };
    macro_rules! id {
        ($name:literal,$ty:ident) => {
            $ty::from_uuid(row.try_get($name)?)
                .map_err(|_| Error::CorruptState("invalid lineage identity"))?
        };
    }
    let state: i64 = row.try_get("state_revision")?;
    let work_item_id = id!("work_item_id", WorkItemId);
    let (count, roots) = load_goal_refs(tx, work_item_id, state).await?;
    let lineage = WorkItemErasureLineage {
        actor: Actor {
            tenant_id: tenant,
            principal_id: id!("owner_id", PrincipalId),
            principal_kind: PrincipalKind::User,
        },
        erasure_job_id: job,
        space_id: id!("space_id", OwnerSpaceId),
        agent_id: id!("agent_id", AgentId),
        work_item_id,
        source_dialogue_id: id!("source_dialogue_id", AgentDialogueId),
        source_intent_id: id!("source_intent_id", UserIntentId),
        state_revision: super::from_i64(state, "state")?,
        control_revision: super::from_i64(row.try_get("control_revision")?, "control")?,
        revision_count: count,
        goal_content_refs: roots,
        reason_code: DottedKey::new(row.try_get::<String, _>("reason_code")?)
            .map_err(|_| Error::CorruptState("reason code"))?,
        frozen_at: UtcTimestamp::from_unix_millis(row.try_get("frozen_at_ms")?),
    };
    lineage.validate()?;
    if count != super::from_i64(row.try_get("revision_count")?, "revision count")?
        || lineage.fingerprint().into_bytes().as_slice()
            != row.try_get::<Vec<u8>, _>("lineage_fingerprint")?
    {
        return Err(Error::CorruptState("task erasure lineage changed"));
    }
    Ok(Some(lineage))
}
