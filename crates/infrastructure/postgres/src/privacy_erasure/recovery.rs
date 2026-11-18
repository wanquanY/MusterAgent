use super::*;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_privacy::{
    ErasureExecutor, ErasureRecoveryStore, ErasureWorkClaim, is_work_item_erasure_schema,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl ErasureRecoveryStore for PostgresPrivacyErasureStore {
    type Error = PostgresPrivacyErasureError;
    fn claim_erasure_work(
        &self,
        worker_id: uuid::Uuid,
        now: UtcTimestamp,
        lease_ms: u32,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<ErasureWorkClaim>, Self::Error>,
    > {
        Box::pin(async move {
            if worker_id.is_nil() || !(1000..=300_000).contains(&lease_ms) {
                return Err(PostgresPrivacyErasureError::ErasureLeaseConflict);
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
                .await?;
            // No copied status: terminal authority is the original receipt.
            // Receipt-before-ACK crashes leave only reclaimable delivery state.
            sqlx::query("DELETE FROM privacy_erasure.execution_work w WHERE (w.lease_expires_at_ms IS NULL OR w.lease_expires_at_ms<=$1) AND (w.tenant_id,w.erasure_job_id) IN (SELECT r.tenant_id,r.erasure_job_id FROM privacy_erasure.receipts r JOIN privacy_erasure.execution_work pending USING(tenant_id,erasure_job_id) WHERE pending.lease_expires_at_ms IS NULL OR pending.lease_expires_at_ms<=$1 ORDER BY r.erasure_job_id LIMIT 64)")
                .bind(now.unix_millis()).execute(&mut *tx).await?;
            let row=sqlx::query("WITH candidate AS (
                SELECT w.tenant_id,w.erasure_job_id FROM privacy_erasure.execution_work w
                WHERE w.available_at_ms<=$1 AND (w.lease_expires_at_ms IS NULL OR w.lease_expires_at_ms<=$1)
                AND NOT EXISTS(SELECT 1 FROM privacy_erasure.receipts r WHERE r.tenant_id=w.tenant_id AND r.erasure_job_id=w.erasure_job_id)
                ORDER BY w.available_at_ms,w.erasure_job_id LIMIT 1 FOR UPDATE OF w SKIP LOCKED)
                UPDATE privacy_erasure.execution_work w SET claim_owner=$2,claim_generation=w.claim_generation+1,attempt_count=w.attempt_count+1,lease_expires_at_ms=$3
                FROM candidate c WHERE w.tenant_id=c.tenant_id AND w.erasure_job_id=c.erasure_job_id
                RETURNING w.tenant_id,w.erasure_job_id,w.claim_generation")
                .bind(now.unix_millis()).bind(worker_id).bind(now.unix_millis().saturating_add(i64::from(lease_ms)))
                .fetch_optional(&mut *tx).await?;
            tx.commit().await?;
            row.map(|r| {
                Ok(ErasureWorkClaim {
                    tenant_id: TenantId::from_uuid(r.try_get("tenant_id")?).map_err(|_| {
                        PostgresPrivacyErasureError::CorruptState("invalid recovery tenant")
                    })?,
                    erasure_job_id: ErasureJobId::from_uuid(r.try_get("erasure_job_id")?).map_err(
                        |_| PostgresPrivacyErasureError::CorruptState("invalid recovery job"),
                    )?,
                    worker_id,
                    generation: from_i64(r.try_get("claim_generation")?, "claim generation")?,
                    lease_expires_at: UtcTimestamp::from_unix_millis(
                        now.unix_millis().saturating_add(i64::from(lease_ms)),
                    ),
                })
            })
            .transpose()
        })
    }
    fn release_erasure_work(
        &self,
        claim: ErasureWorkClaim,
        now: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
                .await?;
            require_claim(&mut tx, claim).await?;
            let terminal:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM privacy_erasure.receipts WHERE tenant_id=$1 AND erasure_job_id=$2)")
                .bind(claim.tenant_id.into_uuid()).bind(claim.erasure_job_id.into_uuid()).fetch_one(&mut *tx).await?;
            if terminal {
                sqlx::query("DELETE FROM privacy_erasure.execution_work WHERE tenant_id=$1 AND erasure_job_id=$2")
                    .bind(claim.tenant_id.into_uuid()).bind(claim.erasure_job_id.into_uuid()).execute(&mut *tx).await?;
            } else {
                sqlx::query("UPDATE privacy_erasure.execution_work SET claim_owner=NULL,lease_expires_at_ms=NULL,available_at_ms=$3+LEAST(300000,1000*(1::bigint << LEAST(attempt_count,8)::integer)) WHERE tenant_id=$1 AND erasure_job_id=$2")
                    .bind(claim.tenant_id.into_uuid()).bind(claim.erasure_job_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }
}

impl PostgresPrivacyErasureStore {
    /// The lease adapter shares the original store and facts. A stale worker
    /// cannot append a retry or receipt after another worker has reclaimed it.
    pub fn with_erasure_claim(&self, claim: ErasureWorkClaim) -> Self {
        let mut store = self.clone();
        store.execution_claim = Some(claim);
        store
    }

    /// One bounded production attempt. Cancellation leaves a durable expired
    /// claim, and errors release with backoff rather than poisoning the loop.
    pub async fn recover_one_erasure(
        &self,
        content: &dyn ContentStore,
        worker_id: uuid::Uuid,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<bool, PostgresPrivacyErasureError> {
        let Some(claim) = self.claim_erasure_work(worker_id, now(), 60_000).await? else {
            return Ok(false);
        };
        let leased = self.with_erasure_claim(claim);
        let executor = ErasureExecutor::new(&leased, content);
        let execution = tokio::select! {
            ()=cancel.cancelled()=>return Ok(true),
            result=tokio::time::timeout(Duration::from_secs(45),executor.execute(claim.tenant_id,claim.erasure_job_id,now()))=>result,
        };
        if !matches!(execution, Ok(Ok(_))) {
            tracing::warn!(erasure_job_id=%claim.erasure_job_id,"privacy erasure attempt deferred; original durable job retained");
        }
        self.release_erasure_work(claim, now()).await?;
        Ok(true)
    }
}

fn now() -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64,
    )
}

async fn require_claim(
    tx: &mut sqlx::PgConnection,
    claim: ErasureWorkClaim,
) -> Result<(), PostgresPrivacyErasureError> {
    let valid:Option<bool>=sqlx::query_scalar("SELECT claim_owner=$3 AND claim_generation=$4 AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint FROM privacy_erasure.execution_work WHERE tenant_id=$1 AND erasure_job_id=$2 FOR UPDATE")
        .bind(claim.tenant_id.into_uuid()).bind(claim.erasure_job_id.into_uuid()).bind(claim.worker_id).bind(to_i64(claim.generation,"claim generation")?)
        .fetch_optional(tx).await?;
    if valid != Some(true) {
        return Err(PostgresPrivacyErasureError::ErasureLeaseConflict);
    }
    Ok(())
}

pub(super) async fn require_execution_claim(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    job: ErasureJobId,
    claim: Option<ErasureWorkClaim>,
) -> Result<(), PostgresPrivacyErasureError> {
    let schema:String=sqlx::query_scalar("SELECT schema_version FROM privacy_erasure.requests WHERE tenant_id=$1 AND erasure_job_id=$2")
        .bind(tenant.into_uuid()).bind(job.into_uuid()).fetch_one(&mut *tx).await?;
    if !is_work_item_erasure_schema(&schema) {
        return Ok(());
    }
    let claim = claim.ok_or(PostgresPrivacyErasureError::ErasureLeaseConflict)?;
    if claim.tenant_id != tenant || claim.erasure_job_id != job {
        return Err(PostgresPrivacyErasureError::ErasureLeaseConflict);
    }
    require_claim(tx, claim).await
}
