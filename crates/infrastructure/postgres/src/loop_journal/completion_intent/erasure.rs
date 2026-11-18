use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::*;

const DEFAULT_LEASE: Duration = Duration::from_secs(30);
const BASE_BACKOFF_MS: i64 = 100;
const MAX_BACKOFF_MS: i64 = 60_000;

struct ErasureClaim {
    invocation_id: InvocationId,
    tenant_id: TenantId,
    intent: CompletionIntentRef,
    owner_id: uuid::Uuid,
    fence: i64,
    attempt_count: i32,
}

enum DeleteOutcome {
    Erased,
    Cancelled,
}

impl PostgresAgentLoopJournal {
    /// Idempotently erases one exact queued intent under a short-lived fenced
    /// claim. This is the post-terminal fast path; the production worker queue
    /// remains authoritative after errors or process loss.
    pub async fn purge_completion_intent(
        &self,
        invocation_id: InvocationId,
    ) -> Result<bool, PostgresAgentLoopJournalError> {
        let owner_id = uuid::Uuid::now_v7();
        let Some(claim) = self
            .claim_completion_intent(Some(invocation_id), owner_id, DEFAULT_LEASE)
            .await?
        else {
            let lifecycle: Option<i16> = sqlx::query_scalar(
                "SELECT lifecycle FROM runtime.model_completion_intents
                 WHERE invocation_id = $1",
            )
            .bind(invocation_id.into_uuid())
            .fetch_optional(&self.pool)
            .await?;
            return Ok(lifecycle == Some(INTENT_ERASED));
        };
        let cancellation = CancellationToken::new();
        match self.delete_claimed_intent(&claim, &cancellation).await {
            Ok(DeleteOutcome::Erased) => self.finalize_erasure_claim(&claim).await,
            Ok(DeleteOutcome::Cancelled) => Ok(false),
            Err(error) => {
                self.reschedule_erasure_claim(&claim, failure_code(&error))
                    .await?;
                Err(error)
            }
        }
    }

    /// Drains a durable, leased erasure queue. Claims use `SKIP LOCKED`, every
    /// physical delete is idempotent, and only the matching owner/fence may
    /// finalize. Poison rows receive bounded backoff so later rows remain fair.
    pub async fn purge_pending_completion_intents(
        &self,
        owner_id: uuid::Uuid,
        limit: u32,
        cancellation: &CancellationToken,
    ) -> Result<usize, PostgresAgentLoopJournalError> {
        let mut erased = 0_usize;
        let mut first_error = None;
        for _ in 0..limit {
            if cancellation.is_cancelled() {
                break;
            }
            let Some(claim) = self
                .claim_completion_intent(None, owner_id, DEFAULT_LEASE)
                .await?
            else {
                break;
            };
            match self.delete_claimed_intent(&claim, cancellation).await {
                Ok(DeleteOutcome::Erased) => {
                    if self.finalize_erasure_claim(&claim).await? {
                        erased = erased.saturating_add(1);
                    }
                }
                Ok(DeleteOutcome::Cancelled) => break,
                Err(error) => {
                    self.reschedule_erasure_claim(&claim, failure_code(&error))
                        .await?;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(erased)
        }
    }

    async fn claim_completion_intent(
        &self,
        exact_invocation_id: Option<InvocationId>,
        owner_id: uuid::Uuid,
        lease: Duration,
    ) -> Result<Option<ErasureClaim>, PostgresAgentLoopJournalError> {
        let lease_ms = i64::try_from(lease.as_millis())?;
        let mut transaction = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Checkpoint)
            .await?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .fetch_one(&mut *transaction)
        .await?;
        reconcile_closed_intents(&mut transaction, now).await?;
        let row = sqlx::query(
            "SELECT invocation_id, tenant_id, content_id, media_type, content_hash,
                    content_bytes::text AS content_bytes,
                    plaintext_hash, plaintext_bytes::text AS plaintext_bytes,
                    key_version, schema_version, lifecycle, erase_fence,
                    erase_attempt_count
             FROM runtime.model_completion_intents
             WHERE lifecycle IN ($1, $2)
               AND erase_next_attempt_at_ms <= $3
               AND (erase_lease_expires_at_ms IS NULL
                    OR erase_lease_expires_at_ms <= $3)
               AND ($4::UUID IS NULL OR invocation_id = $4)
             ORDER BY erase_next_attempt_at_ms, erase_requested_at_ms,
                      created_at_ms, invocation_id
             FOR UPDATE SKIP LOCKED
             LIMIT 1",
        )
        .bind(INTENT_ERASE_REQUESTED)
        .bind(INTENT_ORPHAN)
        .bind(now)
        .bind(exact_invocation_id.map(InvocationId::into_uuid))
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let invocation_id = InvocationId::from_uuid(row.try_get("invocation_id")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let intent = decode_completion_intent_ref(&row)?;
        let fence = row
            .try_get::<i64, _>("erase_fence")?
            .checked_add(1)
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let attempt_count = row
            .try_get::<i32, _>("erase_attempt_count")?
            .checked_add(1)
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let claimed = sqlx::query(
            "UPDATE runtime.model_completion_intents
             SET erase_owner_id = $2, erase_fence = $3,
                 erase_lease_expires_at_ms = $4,
                 erase_attempt_count = $5
             WHERE invocation_id = $1 AND lifecycle IN ($6, $7)
               AND (erase_lease_expires_at_ms IS NULL
                    OR erase_lease_expires_at_ms <= $8)",
        )
        .bind(invocation_id.into_uuid())
        .bind(owner_id)
        .bind(fence)
        .bind(now.saturating_add(lease_ms))
        .bind(attempt_count)
        .bind(INTENT_ERASE_REQUESTED)
        .bind(INTENT_ORPHAN)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        require_one(claimed.rows_affected())?;
        transaction.commit().await?;
        Ok(Some(ErasureClaim {
            invocation_id,
            tenant_id,
            intent,
            owner_id,
            fence,
            attempt_count,
        }))
    }

    async fn delete_claimed_intent(
        &self,
        claim: &ErasureClaim,
        cancellation: &CancellationToken,
    ) -> Result<DeleteOutcome, PostgresAgentLoopJournalError> {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(DeleteOutcome::Cancelled),
            result = self.content.delete(claim.tenant_id, &claim.intent.content) => {
                match result {
                    Ok(()) | Err(musteragent_runtime_content::ContentStoreError::NotFound) => {
                        Ok(DeleteOutcome::Erased)
                    }
                    Err(error) => Err(error.into()),
                }
            }
        }
    }

    async fn finalize_erasure_claim(
        &self,
        claim: &ErasureClaim,
    ) -> Result<bool, PostgresAgentLoopJournalError> {
        let changed = sqlx::query(
            "UPDATE runtime.model_completion_intents
             SET lifecycle = $4,
                 erased_at_ms =
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                 erase_owner_id = NULL, erase_lease_expires_at_ms = NULL,
                 erase_next_attempt_at_ms = NULL, erase_last_failure = NULL
             WHERE invocation_id = $1 AND erase_owner_id = $2 AND erase_fence = $3
               AND lifecycle IN ($5, $6)",
        )
        .bind(claim.invocation_id.into_uuid())
        .bind(claim.owner_id)
        .bind(claim.fence)
        .bind(INTENT_ERASED)
        .bind(INTENT_ERASE_REQUESTED)
        .bind(INTENT_ORPHAN)
        .execute(&self.pool)
        .await?;
        Ok(changed.rows_affected() == 1)
    }

    async fn reschedule_erasure_claim(
        &self,
        claim: &ErasureClaim,
        failure: i16,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let exponent = u32::try_from(claim.attempt_count.clamp(0, 16))?;
        let backoff = BASE_BACKOFF_MS
            .saturating_mul(2_i64.saturating_pow(exponent))
            .min(MAX_BACKOFF_MS);
        let changed = sqlx::query(
            "UPDATE runtime.model_completion_intents
             SET erase_owner_id = NULL, erase_lease_expires_at_ms = NULL,
                 erase_next_attempt_at_ms =
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint + $4,
                 erase_last_failure = $5
             WHERE invocation_id = $1 AND erase_owner_id = $2 AND erase_fence = $3
               AND lifecycle IN ($6, $7)",
        )
        .bind(claim.invocation_id.into_uuid())
        .bind(claim.owner_id)
        .bind(claim.fence)
        .bind(backoff)
        .bind(failure)
        .bind(INTENT_ERASE_REQUESTED)
        .bind(INTENT_ORPHAN)
        .execute(&self.pool)
        .await?;
        require_one(changed.rows_affected())
    }
}

pub(crate) async fn request_completion_intent_erasure(
    transaction: &mut sqlx::PgConnection,
    invocation_id: InvocationId,
    now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let changed = sqlx::query(
        "UPDATE runtime.model_completion_intents
         SET lifecycle = CASE
                 WHEN lifecycle IN ($3, $4) THEN $5
                 WHEN lifecycle = $6 THEN $7
                 ELSE lifecycle
             END,
             erase_requested_at_ms = COALESCE(erase_requested_at_ms, $2),
             erase_next_attempt_at_ms = COALESCE(erase_next_attempt_at_ms, $2),
             erase_owner_id = NULL, erase_lease_expires_at_ms = NULL
         WHERE invocation_id = $1 AND lifecycle IN ($3, $4, $6)",
    )
    .bind(invocation_id.into_uuid())
    .bind(now.unix_millis())
    .bind(INTENT_STAGED)
    .bind(INTENT_READY)
    .bind(INTENT_ORPHAN)
    .bind(INTENT_ACTIVE)
    .bind(INTENT_ERASE_REQUESTED)
    .execute(&mut *transaction)
    .await?;
    if changed.rows_affected() > 1 {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    sqlx::query(
        "UPDATE runtime.model_invocations
         SET completion_intent_content_id = NULL,
             completion_intent_protected = NULL
         WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

/// Exact producer inventory for Task erasure. This never widens to all
/// tenant intents and does not enqueue deletion before coverage is frozen.
pub(crate) async fn collect_work_item_completion_intents(
    transaction: &mut sqlx::PgConnection,
    scope: &crate::privacy_erasure::WorkItemProviderScope,
) -> Result<Vec<ContentRef>, crate::PostgresPrivacyErasureError> {
    use crate::PostgresPrivacyErasureError as Error;
    let rows = sqlx::query(
        "SELECT intent.content_id,intent.media_type,intent.content_hash,
                intent.content_bytes::text AS content_bytes,
                invocation.run_id=intent.run_id AND invocation.tenant_id=intent.tenant_id
                  AND invocation.attempt_id=intent.attempt_id AND invocation.status<>1 AS producer_closed
         FROM runtime.model_completion_intents intent
         LEFT JOIN runtime.model_invocations invocation ON invocation.invocation_id=intent.invocation_id
         WHERE intent.tenant_id=$1 AND intent.run_id=ANY($2)
         ORDER BY intent.invocation_id LIMIT 65537 FOR SHARE OF intent",
    ).bind(scope.tenant()).bind(scope.runs()).fetch_all(&mut *transaction).await?;
    if rows.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    rows.iter()
        .map(|row| {
            if row.try_get::<Option<bool>, _>("producer_closed")? != Some(true) {
                return Err(Error::WorkItemExecutionClosureRequired);
            }
            Ok(ContentRef {
                content_id: ContentId::from_uuid(row.try_get("content_id")?)
                    .map_err(|_| Error::CorruptState("completion intent content id"))?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| Error::CorruptState("completion intent media type"))?,
                byte_length: row
                    .try_get::<String, _>("content_bytes")?
                    .parse()
                    .map_err(|_| Error::CorruptState("completion intent content length"))?,
                sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| Error::CorruptState("completion intent content hash"))?,
            })
        })
        .collect()
}

/// Capture and enqueue only the already-quiescent Task's exact invocations.
/// The original durable purge remains the sole physical deletion worker.
pub(crate) async fn request_work_item_completion_intent_erasure(
    tx: &mut sqlx::PgConnection,
    scope: &crate::privacy_erasure::WorkItemProviderScope,
    job: musteragent_identity::ErasureJobId,
    now: UtcTimestamp,
) -> Result<(), crate::PostgresPrivacyErasureError> {
    use crate::PostgresPrivacyErasureError as Error;
    let frozen:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM privacy_erasure.work_item_lineage l JOIN privacy_erasure.plans p USING(tenant_id,erasure_job_id) WHERE l.tenant_id=$1 AND l.erasure_job_id=$2 AND l.execution_run_ids=$3 AND p.plan_version=2 AND p.lineage_fingerprint=l.execution_coverage_hash)")
        .bind(scope.tenant()).bind(job.into_uuid()).bind(scope.runs()).fetch_one(&mut *tx).await?;
    if !frozen {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let ids:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT invocation_id FROM runtime.model_completion_intents WHERE tenant_id=$1 AND run_id=ANY($2) ORDER BY invocation_id FOR UPDATE")
        .bind(scope.tenant()).bind(scope.runs()).fetch_all(&mut *tx).await?;
    sqlx::query("INSERT INTO privacy_erasure.completion_intent_coverage(tenant_id,erasure_job_id,plan_version,invocation_id,lifecycle_at_capture,covered_at_ms)
        SELECT tenant_id,$2,2,invocation_id,lifecycle,$3 FROM runtime.model_completion_intents
        WHERE tenant_id=$1 AND invocation_id=ANY($4) ON CONFLICT DO NOTHING")
        .bind(scope.tenant()).bind(job.into_uuid()).bind(now.unix_millis()).bind(&ids).execute(&mut *tx).await?;
    for id in ids {
        request_completion_intent_erasure(
            tx,
            InvocationId::from_uuid(id).map_err(|_| Error::CorruptState("intent invocation"))?,
            now,
        )
        .await
        .map_err(|_| Error::WorkItemExecutionClosureRequired)?;
    }
    Ok(())
}

pub(crate) async fn request_tenant_completion_intent_privacy_erasure(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: musteragent_identity::ErasureJobId,
    plan_version: u64,
    requested_at: UtcTimestamp,
) -> Result<(), sqlx::Error> {
    let plan_version =
        i64::try_from(plan_version).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
    sqlx::query(
        "INSERT INTO privacy_erasure.completion_intent_coverage (
             tenant_id, erasure_job_id, plan_version, invocation_id,
             lifecycle_at_capture, covered_at_ms
         )
         SELECT tenant_id, $2, $3, invocation_id, lifecycle, $4
         FROM runtime.model_completion_intents
         WHERE tenant_id = $1
         ON CONFLICT DO NOTHING",
    )
    .bind(tenant_id.into_uuid())
    .bind(erasure_job_id.into_uuid())
    .bind(plan_version)
    .bind(requested_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE runtime.model_completion_intents
         SET privacy_erasure_requested_at_ms =
               COALESCE(privacy_erasure_requested_at_ms, $2)
         WHERE tenant_id = $1 AND lifecycle IN ($3, $4, $5)",
    )
    .bind(tenant_id.into_uuid())
    .bind(requested_at.unix_millis())
    .bind(INTENT_STAGED)
    .bind(INTENT_READY)
    .bind(INTENT_ACTIVE)
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn reconcile_closed_intents(
    transaction: &mut sqlx::PgConnection,
    now: i64,
) -> Result<(), PostgresAgentLoopJournalError> {
    sqlx::query(
        "UPDATE runtime.model_completion_intents AS intent
         SET lifecycle = CASE
                 WHEN intent.lifecycle IN ($2, $3) THEN $4
                 ELSE $5
             END,
             erase_requested_at_ms = COALESCE(intent.erase_requested_at_ms, $1),
             erase_next_attempt_at_ms = COALESCE(intent.erase_next_attempt_at_ms, $1),
             erase_owner_id = NULL, erase_lease_expires_at_ms = NULL
         FROM runtime.model_invocations AS invocation
         WHERE intent.invocation_id = invocation.invocation_id
           AND invocation.status <> 1
           AND intent.lifecycle IN ($2, $3, $6)",
    )
    .bind(now)
    .bind(INTENT_STAGED)
    .bind(INTENT_READY)
    .bind(INTENT_ORPHAN)
    .bind(INTENT_ERASE_REQUESTED)
    .bind(INTENT_ACTIVE)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE runtime.model_invocations AS invocation
         SET completion_intent_content_id = NULL,
             completion_intent_protected = NULL
         FROM runtime.model_completion_intents AS intent
         WHERE invocation.invocation_id = intent.invocation_id
           AND invocation.status <> 1
           AND intent.lifecycle IN ($1, $2)",
    )
    .bind(INTENT_ERASE_REQUESTED)
    .bind(INTENT_ORPHAN)
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

const fn failure_code(error: &PostgresAgentLoopJournalError) -> i16 {
    match error {
        PostgresAgentLoopJournalError::Content(
            musteragent_runtime_content::ContentStoreError::Unavailable,
        ) => 1,
        PostgresAgentLoopJournalError::Content(_) => 2,
        _ => 3,
    }
}
