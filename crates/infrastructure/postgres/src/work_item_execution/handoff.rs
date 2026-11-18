//! Claim/ack metadata belongs to the existing WorkItem outbox. A receipt is
//! acknowledged only after the canonical independently admitted Run exists.
use super::*;
use musteragent_storage_protection::GrantKeyring;
use prost::Message;
use std::{sync::Arc, time::Duration};
#[derive(Clone)]
pub struct PostgresWorkItemExecutionHandoffStore {
    executions: PostgresWorkItemExecutionStore,
    keys: Arc<GrantKeyring>,
    facts: Arc<dyn WorkItemGrantFactsHasher>,
}
impl PostgresWorkItemExecutionHandoffStore {
    pub fn new(
        executions: PostgresWorkItemExecutionStore,
        keys: Arc<GrantKeyring>,
        facts: Arc<dyn WorkItemGrantFactsHasher>,
    ) -> Self {
        Self {
            executions,
            keys,
            facts,
        }
    }
    async fn reject_inner(
        &self,
        claim: WorkItemExecutionClaim,
        p: WorkItemExecutionPreparation,
        proof: SignedWorkItemAdmissionRejection,
    ) -> Result<bool, DialogueStoreError> {
        let c = &proof.claims;
        if p.key.request() != claim.request
            || c.actor != claim.request.actor
            || c.work_item_id != claim.request.work_item_id
            || c.goal_revision != claim.request.goal_revision
            || c.control_revision != claim.request.control_revision
            || c.assignment_epoch != claim.request.assignment_epoch
        {
            return Err(DialogueStoreError::Corrupt);
        }
        self.keys
            .verify_work_item_admission_rejection(&proof, c.issued_at)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let bytes = musteragent_persistence_codec::work_item_admission_codec::rejection(&proof)
            .encode_to_vec();
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        // Original authority lock order precedes the outbox lock for all writers.
        preparation::lock_agents(&mut tx, p.key.actor, p.key.scope, p.key.executing_agent_id)
            .await?;
        let row=sqlx::query("SELECT signed_admission_rejection FROM collaboration.work_item_outbox WHERE event_seq=$1 AND work_item_id=$2 FOR UPDATE")
   .bind(integer(claim.event_seq)?).bind(claim.request.work_item_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if let Some(existing) = row
            .try_get::<Option<Vec<u8>>, _>("signed_admission_rejection")
            .map_err(db)?
        {
            return if existing == bytes {
                Ok(true)
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        if lookup(&mut tx, claim.request).await?.is_some() {
            return Ok(false);
        }
        let current = preparation::load(
            &mut tx,
            claim.request,
            false,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        if current != p {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let workspace = device::placement::read(&mut tx, &current).await?;
        if c.facts_sha256 != self.facts.placed_facts_sha256(&current, workspace)? {
            return Err(DialogueStoreError::Corrupt);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        self.keys
            .verify_work_item_admission_rejection(&proof, now)
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        // A failed coordinator continuation does not rewrite the task execution's
        // business outcome. Its exact signed rejection remains on this outbox item.
        if p.report.is_none() {
            let mut record = p.work_item.clone();
            record.core = record
                .core
                .execution_rejected(
                    p.key.goal_revision,
                    p.key.control_revision,
                    p.key.assignment_epoch,
                )
                .map_err(|_| DialogueStoreError::RevisionConflict)?;
            record.updated_at = now;
            crate::work_item_authority::commands::revision_and_outbox(
                &mut tx,
                &record,
                "execution_rejected",
            )
            .await
            .map_err(authority_error)?;
            sqlx::query(
                "UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1",
            )
            .bind(record.core.work_item_id.into_uuid())
            .bind(integer(record.core.revisions.state.get())?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        let changed=sqlx::query("UPDATE collaboration.work_item_outbox SET signed_admission_rejection=$4,delivered_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,failure_code=$5,claim_owner=NULL,lease_expires_at_ms=NULL WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
   .bind(integer(claim.event_seq)?).bind(claim.owner).bind(integer(claim.generation)?).bind(bytes).bind(c.reason.as_str()).execute(&mut *tx).await.map_err(db)?.rows_affected()==1;
        if !changed {
            return Ok(false);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        self.keys
            .verify_work_item_admission_rejection(&proof, now)
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(true)
    }
}
impl WorkItemExecutionHandoffStore for PostgresWorkItemExecutionHandoffStore {
    fn claim(
        &self,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemExecutionClaim>, DialogueStoreError>> {
        Box::pin(async move {
            if owner.is_nil()
                || !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&lease)
            {
                return Err(DialogueStoreError::Corrupt);
            }
            let row=sqlx::query("WITH clock AS MATERIALIZED(SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS ms), candidate AS (
   SELECT q.event_seq FROM collaboration.work_item_outbox q
   JOIN collaboration.work_items t ON t.work_item_id=q.work_item_id
   JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
   JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id AND x.goal_revision=r.goal_revision AND x.control_revision=r.control_revision AND x.assignment_epoch=r.assignment_epoch
   LEFT JOIN collaboration.work_item_execution_reports p ON p.producing_run_id=q.report_from_run_id AND p.work_item_id=t.work_item_id,clock
   WHERE q.delivered_at_ms IS NULL AND q.available_at_ms<=clock.ms AND (q.claim_owner IS NULL OR q.lease_expires_at_ms<=clock.ms)
   AND t.erasure_job_id IS NULL AND (
    (q.event_kind='execution_requested' AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_targets target WHERE target.work_item_id=x.work_item_id AND target.goal_revision=x.goal_revision AND target.control_revision=x.control_revision AND target.assignment_epoch=x.assignment_epoch) AND x.accepted_state_revision=q.state_revision AND (r.status='ready' OR EXISTS(SELECT 1 FROM runtime.runs run WHERE run.run_id=x.run_id AND run.tenant_id=x.tenant_id))
     AND NOT EXISTS(SELECT 1 FROM collaboration.work_items d JOIN collaboration.work_item_revisions dr ON dr.work_item_id=d.work_item_id AND dr.state_revision=d.state_revision WHERE d.work_item_id=ANY(r.dependencies) AND (dr.status<>'completed' OR d.erasure_job_id IS NOT NULL)))
    OR (q.event_kind='execution_reported' AND p.state_revision=q.state_revision AND p.goal_revision=r.goal_revision AND p.control_revision=r.control_revision AND p.assignment_epoch=r.assignment_epoch))
   ORDER BY q.available_at_ms,q.event_seq LIMIT 1 FOR UPDATE OF q SKIP LOCKED), claimed AS (
   UPDATE collaboration.work_item_outbox q SET claim_owner=$1,claim_generation=q.claim_generation+1,attempt_count=q.attempt_count+1,lease_expires_at_ms=clock.ms+$2 FROM candidate,clock WHERE q.event_seq=candidate.event_seq RETURNING q.*)
   SELECT c.*,t.tenant_id,t.owner_id,r.goal_revision,r.control_revision,r.assignment_epoch FROM claimed c JOIN collaboration.work_items t ON t.work_item_id=c.work_item_id JOIN collaboration.work_item_revisions r ON r.work_item_id=c.work_item_id AND r.state_revision=c.state_revision")
   .bind(owner).bind(i64::try_from(lease.as_millis()).map_err(|_|DialogueStoreError::Corrupt)?).fetch_optional(self.executions.admission.ledger.pool()).await.map_err(db)?;
            row.map(|r| {
                Ok(WorkItemExecutionClaim {
                    request: WorkItemExecutionRequest {
                        report_from_run_id: r
                            .try_get::<Option<uuid::Uuid>, _>("report_from_run_id")
                            .map_err(db)?
                            .map(RunId::from_uuid)
                            .transpose()
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        actor: Actor {
                            tenant_id: TenantId::from_uuid(r.try_get("tenant_id").map_err(db)?)
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            principal_id: PrincipalId::from_uuid(
                                r.try_get("owner_id").map_err(db)?,
                            )
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                            principal_kind: PrincipalKind::User,
                        },
                        work_item_id: WorkItemId::from_uuid(r.try_get("work_item_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
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
                    },
                    event_seq: unsigned(r.try_get("event_seq").map_err(db)?)?,
                    owner,
                    generation: unsigned(r.try_get("claim_generation").map_err(db)?)?,
                    attempt_count: unsigned(r.try_get("attempt_count").map_err(db)?)?,
                    lease_expires_at: UtcTimestamp::from_unix_millis(
                        r.try_get("lease_expires_at_ms").map_err(db)?,
                    ),
                })
            })
            .transpose()
        })
    }
    fn complete(
        &self,
        c: WorkItemExecutionClaim,
        receipt: WorkItemExecutionReceipt,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .executions
                .admission
                .ledger
                .database()
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            if lookup(&mut tx, c.request).await? != Some(receipt) {
                return Err(DialogueStoreError::Corrupt);
            }
            let changed=sqlx::query("UPDATE collaboration.work_item_outbox q SET delivered_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,claim_owner=NULL,lease_expires_at_ms=NULL,failure_code=NULL WHERE event_seq=$1 AND work_item_id=$2 AND claim_owner=$3 AND claim_generation=$4 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
   .bind(integer(c.event_seq)?).bind(c.request.work_item_id.into_uuid()).bind(c.owner).bind(integer(c.generation)?).execute(&mut *tx).await.map_err(db)?.rows_affected()==1;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(changed)
        })
    }
    fn reject(
        &self,
        c: WorkItemExecutionClaim,
        p: WorkItemExecutionPreparation,
        proof: SignedWorkItemAdmissionRejection,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>> {
        Box::pin(self.reject_inner(c, p, proof))
    }
    fn retry(
        &self,
        c: WorkItemExecutionClaim,
        failure: WorkItemHandoffFailure,
        delay: Duration,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>> {
        Box::pin(async move {
            if !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&delay) {
                return Err(DialogueStoreError::Corrupt);
            }
            let n=sqlx::query("UPDATE collaboration.work_item_outbox q SET claim_owner=NULL,lease_expires_at_ms=NULL,failure_code=$5,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+$6 WHERE event_seq=$1 AND work_item_id=$2 AND claim_owner=$3 AND claim_generation=$4 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND EXISTS(SELECT 1 FROM collaboration.work_items t WHERE t.work_item_id=q.work_item_id AND t.tenant_id=$7 AND t.owner_id=$8)")
   .bind(integer(c.event_seq)?).bind(c.request.work_item_id.into_uuid()).bind(c.owner).bind(integer(c.generation)?).bind(failure.as_str()).bind(i64::try_from(delay.as_millis()).map_err(|_|DialogueStoreError::Corrupt)?).bind(c.request.actor.tenant_id.into_uuid()).bind(c.request.actor.principal_id.into_uuid()).execute(self.executions.admission.ledger.pool()).await.map_err(db)?.rows_affected();
            Ok(n == 1)
        })
    }
}
