//! The complete original producer cut, including a download accepted before
//! Artifact staging. Receipts live under the existing Erasure job, not a queue.
use super::*;
use musteragent_content_ref::Sha256;

pub(super) async fn collect(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
) -> Result<Vec<SourceEvidenceProducer>, Error> {
    let rows=sqlx::query("SELECT invocation_id,run_id,conversation_id FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=ANY($2) AND tool_ref=$3 ORDER BY invocation_id LIMIT 65537")
        .bind(scope.tenant()).bind(scope.runs()).bind(SOURCE_ATTACHMENT_PRODUCER_TOOL).fetch_all(tx).await?;
    if rows.len() > MAX_SOURCE_EVIDENCE_PRODUCERS {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    rows.into_iter()
        .map(|r| {
            Ok(SourceEvidenceProducer {
                run_id: RunId::from_uuid(r.try_get("run_id")?)
                    .map_err(|_| Error::CorruptState("source run"))?,
                conversation_id: ConversationId::from_uuid(r.try_get("conversation_id")?)
                    .map_err(|_| Error::CorruptState("source conversation"))?,
                invocation_id: InvocationId::from_uuid(r.try_get("invocation_id")?)
                    .map_err(|_| Error::CorruptState("source invocation"))?,
            })
        })
        .collect()
}
pub(super) async fn freeze(
    tx: &mut sqlx::PgConnection,
    job: &ErasureJob,
    plan: &ErasurePlanSnapshot,
    coverage: &WorkItemExecutionErasureCoverage,
) -> Result<(), Error> {
    for p in &coverage.source_evidence {
        sqlx::query("INSERT INTO privacy_erasure.source_evidence_producers(tenant_id,erasure_job_id,producer_invocation_id,run_id,conversation_id,plan_hash) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(job.request.tenant_id.into_uuid()).bind(job.request.erasure_job_id.into_uuid())
            .bind(p.invocation_id.into_uuid()).bind(p.run_id.into_uuid()).bind(p.conversation_id.into_uuid())
            .bind(plan.plan_hash.into_bytes().to_vec()).execute(&mut *tx).await?;
    }
    Ok(())
}
fn command(
    job: &ErasureJob,
    p: SourceEvidenceProducer,
) -> Result<SourceEvidenceErasureCommand, Error> {
    let c = SourceEvidenceErasureCommand {
        request: job.request.clone(),
        plan: job.latest_plan()?.clone(),
        producer: p,
    };
    c.validate()?;
    Ok(c)
}
async fn completed(
    tx: &mut sqlx::PgConnection,
    c: &SourceEvidenceErasureCommand,
) -> Result<Option<SourceEvidenceErasureReceipt>, Error> {
    let row=sqlx::query("SELECT plan_hash,run_id,conversation_id,completed_at_ms,object_ids,proof_digest FROM privacy_erasure.source_evidence_producers WHERE tenant_id=$1 AND erasure_job_id=$2 AND producer_invocation_id=$3")
        .bind(c.request.tenant_id.into_uuid()).bind(c.request.erasure_job_id.into_uuid())
        .bind(c.producer.invocation_id.into_uuid()).fetch_optional(tx).await?.ok_or(Error::WorkItemExecutionClosureRequired)?;
    if row.try_get::<Vec<u8>, _>("plan_hash")? != c.plan.plan_hash.into_bytes()
        || row.try_get::<uuid::Uuid, _>("run_id")? != c.producer.run_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("conversation_id")?
            != c.producer.conversation_id.into_uuid()
    {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let Some(time) = row.try_get::<Option<i64>, _>("completed_at_ms")? else {
        return Ok(None);
    };
    let r = SourceEvidenceErasureReceipt {
        command: c.clone(),
        completed_at: UtcTimestamp::from_unix_millis(time),
        object_ids: row.try_get("object_ids")?,
        proof_digest: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("proof_digest")?)
            .map_err(|_| Error::CorruptState("source receipt"))?,
    };
    r.validate()?;
    Ok(Some(r))
}
pub(super) async fn verify(
    tx: &mut sqlx::PgConnection,
    job: &ErasureJob,
    coverage: &WorkItemExecutionErasureCoverage,
    completed_at: UtcTimestamp,
) -> Result<(), Error> {
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM privacy_erasure.source_evidence_producers WHERE tenant_id=$1 AND erasure_job_id=$2")
        .bind(job.request.tenant_id.into_uuid()).bind(job.request.erasure_job_id.into_uuid()).fetch_one(&mut *tx).await?;
    if usize::try_from(count).ok() != Some(coverage.source_evidence.len()) {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    for p in &coverage.source_evidence {
        if completed(tx, &command(job, *p)?)
            .await?
            .is_none_or(|provider| provider.completed_at > completed_at)
        {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    Ok(())
}
impl PostgresPrivacyErasureStore {
    pub(crate) async fn erase_source_evidence(&self, job: &ErasureJob) -> Result<(), Error> {
        let tenant = job.request.tenant_id;
        let id = job.request.erasure_job_id;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        super::super::recovery::require_execution_claim(&mut tx, tenant, id, self.execution_claim)
            .await?;
        let lineage = load_lineage(&mut tx, tenant, id)
            .await?
            .ok_or(Error::WorkItemExecutionClosureRequired)?;
        let scope = super::coverage::provider_scope(&mut tx, &lineage).await?;
        let coverage = super::frozen::load(&mut tx, &lineage, &scope)
            .await?
            .ok_or(Error::WorkItemExecutionClosureRequired)?;
        if build_work_item_execution_plan(job, &coverage)? != *job.latest_plan()? {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
        let mut pending = Vec::new();
        for p in &coverage.source_evidence {
            let c = command(job, *p)?;
            if completed(&mut tx, &c).await?.is_none() {
                pending.push(c);
            }
        }
        tx.commit().await?;
        for c in pending {
            let provider = self
                .source_evidence
                .as_ref()
                .ok_or(Error::WorkItemExecutionClosureRequired)?;
            let receipt = provider
                .erase(c.clone())
                .await
                .map_err(|_| Error::WorkItemExecutionClosureRequired)?;
            receipt.validate()?;
            if receipt.command != c {
                return Err(Error::WorkItemExecutionClosureRequired);
            }
            // Physical provider IO runs without a database transaction. A lost
            // claim cannot publish its receipt; the next original job repeats
            // the same immutable command and recovers the provider's receipt.
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
                .await?;
            super::super::recovery::require_execution_claim(
                &mut tx,
                tenant,
                id,
                self.execution_claim,
            )
            .await?;
            let latest = super::super::loading::load_jobs(&mut tx, tenant, &[id.into_uuid()])
                .await?
                .pop()
                .ok_or(Error::JobNotFound)?;
            if latest.request != c.request || latest.latest_plan()? != &c.plan {
                return Err(Error::WorkItemExecutionClosureRequired);
            }
            // A provider clock is evidence, not authority over Runtime time.
            // Refuse future claims; a retry can re-observe the same receipt.
            let observed_now: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&mut *tx)
            .await?;
            if receipt.completed_at.unix_millis() > observed_now {
                return Err(Error::WorkItemExecutionClosureRequired);
            }
            if let Some(existing) = completed(&mut tx, &c).await? {
                if existing != receipt {
                    return Err(Error::WorkItemExecutionClosureRequired);
                }
            } else {
                let n=sqlx::query("UPDATE privacy_erasure.source_evidence_producers SET object_ids=$4,completed_at_ms=$5,proof_digest=$6 WHERE tenant_id=$1 AND erasure_job_id=$2 AND producer_invocation_id=$3 AND completed_at_ms IS NULL")
                    .bind(tenant.into_uuid()).bind(id.into_uuid()).bind(c.producer.invocation_id.into_uuid())
                    .bind(&receipt.object_ids).bind(receipt.completed_at.unix_millis()).bind(receipt.proof_digest.into_bytes().to_vec())
                    .execute(&mut *tx).await?;
                if n.rows_affected() != 1 {
                    return Err(Error::WorkItemExecutionClosureRequired);
                }
            }
            tx.commit().await?;
        }
        Ok(())
    }
}
