#![allow(
    dead_code,
    reason = "Integration test binaries use different subsets of this shared fixture."
)]

//! Shared existing fixture for a real durable Attempt replacement.
use musteragent_identity::{
    AttemptId, AttemptNumber, FencingToken, LeaseId, TenantId, UtcTimestamp,
};
use musteragent_run::{AttemptContext, WorkerId};
use sqlx::PgPool;

pub(crate) async fn replace_attempt_before_main_begin(
    pool: &PgPool,
    tenant_id: TenantId,
    stale: &AttemptContext,
) -> AttemptContext {
    let now = UtcTimestamp::from_unix_millis(
        sqlx::query_scalar::<_, i64>(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
        )
        .fetch_one(pool)
        .await
        .expect("read original database clock"),
    );
    let attempt_id = AttemptId::generate();
    let lease_id = LeaseId::generate();
    let lease_expires_at =
        UtcTimestamp::from_unix_millis(now.unix_millis().saturating_add(120_000));
    let mut transaction = pool.begin().await.expect("begin Attempt takeover");
    sqlx::query(
        "UPDATE runtime.attempts
         SET status = 3, finished_at_ms = $2
         WHERE attempt_id = $1 AND status = 1",
    )
    .bind(stale.attempt_id.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await
    .expect("seal stale Attempt");
    sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1")
        .bind(stale.run_id.into_uuid())
        .execute(&mut *transaction)
        .await
        .expect("release stale lease");
    sqlx::query(
        "INSERT INTO runtime.attempts (
             attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
             lease_id, fencing_token, lease_expires_at_ms, status,
             claimed_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, $7, 1, 'worker-context-replacement',
                   $4, $8, $5, 1, $6, NULL)",
    )
    .bind(attempt_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(stale.run_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .bind(i32::try_from(stale.attempt_number.get() + 1).expect("attempt number"))
    .bind(i64::try_from(stale.fencing_token.get() + 1).expect("fence"))
    .execute(&mut *transaction)
    .await
    .expect("insert replacement Attempt");
    sqlx::query(
        "INSERT INTO runtime.leases (
             run_id, tenant_id, attempt_id, worker_id, lease_id,
             fencing_token, expires_at_ms, renewed_at_ms
         ) VALUES ($1, $2, $3, 'worker-context-replacement', $4, $7, $5, $6)",
    )
    .bind(stale.run_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(attempt_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .bind(i64::try_from(stale.fencing_token.get() + 1).expect("fence"))
    .execute(&mut *transaction)
    .await
    .expect("insert replacement lease");
    transaction.commit().await.expect("commit Attempt takeover");
    AttemptContext {
        attempt_id,
        run_id: stale.run_id,
        attempt_number: AttemptNumber::new(stale.attempt_number.get() + 1)
            .expect("replacement Attempt"),
        worker_id: WorkerId::new("worker-context-replacement").expect("replacement worker"),
        lease_id,
        fencing_token: FencingToken::new(stale.fencing_token.get() + 1).expect("replacement fence"),
        lease_expires_at,
    }
}

/// Full recovery for tests that continue through ModelStarted. The original
/// older helper intentionally covers only Context identity; the Scheduler owns
/// rebinding a live loop checkpoint and must be exercised rather than copied.
pub(crate) async fn recover_attempt_before_main_begin(
    pool: &PgPool,
    tenant_id: TenantId,
    stale: &AttemptContext,
) -> AttemptContext {
    use musteragent_scheduler::{ClaimOutcome, DispatchStore};
    let mut tx = pool.begin().await.expect("begin lease expiry");
    let expired: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint-1")
            .fetch_one(&mut *tx)
            .await
            .expect("original clock");
    assert_eq!(sqlx::query("UPDATE runtime.leases SET expires_at_ms=$3 WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$4")
        .bind(tenant_id.into_uuid()).bind(stale.run_id.into_uuid()).bind(expired).bind(stale.attempt_id.into_uuid())
        .execute(&mut *tx).await.expect("expire original lease").rows_affected(),1);
    assert_eq!(sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms=$3 WHERE tenant_id=$1 AND attempt_id=$2")
        .bind(tenant_id.into_uuid()).bind(stale.attempt_id.into_uuid()).bind(expired)
        .execute(&mut *tx).await.expect("expire original Attempt").rows_affected(),1);
    tx.commit().await.expect("commit lease expiry");
    let scheduler = musteragent_postgres::PostgresDispatchStore::new(
        pool.clone(),
        std::sync::Arc::new(musteragent_wire::ProtobufSchedulerEventEncoder),
    );
    let ClaimOutcome::Claimed(claimed) = scheduler
        .claim_one(
            &WorkerId::new("context-plan-recovery").expect("worker"),
            std::time::Duration::from_secs(120),
        )
        .await
        .expect("recover via original Scheduler")
    else {
        panic!("expired original Run must recover")
    };
    assert_eq!(claimed.fence.run_id, stale.run_id);
    assert_eq!(
        claimed.fence.attempt_number.get(),
        stale.attempt_number.get() + 1
    );
    AttemptContext {
        attempt_id: claimed.fence.attempt_id,
        run_id: claimed.fence.run_id,
        attempt_number: claimed.fence.attempt_number,
        worker_id: claimed.fence.worker_id,
        lease_id: claimed.fence.lease_id,
        fencing_token: claimed.fence.fencing_token,
        lease_expires_at: claimed.lease_expires_at,
    }
}
