//! Aggregate reads share the caller's transaction/connection. Completing an
//! erasure must not wait for a second pool slot while holding its original lock.
use super::*;
use std::collections::{BTreeMap, HashMap};

pub(super) async fn load_jobs(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    job_ids: &[uuid::Uuid],
) -> Result<Vec<ErasureJob>, PostgresPrivacyErasureError> {
    if job_ids.is_empty() {
        return Ok(Vec::new());
    }
    let request_rows = sqlx::query(
        "SELECT erasure_job_id, schema_version, tenant_id,
                    actor_principal_id, actor_principal_kind,
                    retention_policy_ref, idempotency_key,
                    requested_at_ms, complete_by_ms, request_fingerprint
             FROM privacy_erasure.requests
             WHERE tenant_id = $1 AND erasure_job_id = ANY($2)",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let roots = load_roots(connection, tenant_id, job_ids).await?;
    let plans = load_plans(connection, tenant_id, job_ids).await?;
    let transitions = load_transitions(connection, tenant_id, job_ids).await?;
    let receipts = load_receipts(connection, tenant_id, job_ids).await?;
    let mut request_map = HashMap::new();
    for row in request_rows {
        let job_id: uuid::Uuid = row.try_get("erasure_job_id")?;
        request_map.insert(job_id, row);
    }
    let mut jobs = Vec::with_capacity(job_ids.len());
    for job_id in job_ids {
        let Some(row) = request_map.remove(job_id) else {
            continue;
        };
        let request = decode_request(
            &row,
            roots
                .get(job_id)
                .cloned()
                .ok_or(PostgresPrivacyErasureError::CorruptState(
                    "request has no root resources",
                ))?,
        )?;
        let request_fingerprint = digest(row.try_get("request_fingerprint")?)?;
        let job = ErasureJob {
            request,
            request_fingerprint,
            plans: plans.get(job_id).cloned().unwrap_or_default(),
            transitions: transitions.get(job_id).cloned().unwrap_or_default(),
            receipt: receipts.get(job_id).cloned(),
        };
        job.validate().map_err(|_| {
            PostgresPrivacyErasureError::CorruptState("invalid persisted erasure aggregate")
        })?;
        jobs.push(job);
    }
    Ok(jobs)
}

async fn load_roots(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    job_ids: &[uuid::Uuid],
) -> Result<
    HashMap<uuid::Uuid, Vec<musteragent_runtime_contract::ResourceRef>>,
    PostgresPrivacyErasureError,
> {
    let rows = sqlx::query(
        "SELECT erasure_job_id, ordinal, resource_kind, resource_id,
                content_id, content_media_type AS media_type,
                content_byte_length::TEXT AS byte_length, content_sha256 AS sha256
         FROM privacy_erasure.root_resources
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let mut values = HashMap::<uuid::Uuid, Vec<_>>::new();
    for row in rows {
        values
            .entry(row.try_get("erasure_job_id")?)
            .or_default()
            .push(
                decode_resource(&row).map_err(|_| {
                    PostgresPrivacyErasureError::CorruptState("invalid root resource")
                })?,
            );
    }
    Ok(values)
}

async fn load_plans(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    job_ids: &[uuid::Uuid],
) -> Result<HashMap<uuid::Uuid, Vec<ErasurePlanSnapshot>>, PostgresPrivacyErasureError> {
    let plan_rows = sqlx::query(
        "SELECT erasure_job_id, plan_version, schema_version,
                lineage_fingerprint, plan_hash, frozen_at_ms
         FROM privacy_erasure.plans
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, plan_version",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let class_rows: Vec<(uuid::Uuid, i64, i16)> = sqlx::query_as(
        "SELECT erasure_job_id, plan_version, data_class
         FROM privacy_erasure.plan_data_classes
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, plan_version, data_class",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let exception_rows = sqlx::query(
        "SELECT erasure_job_id, plan_version, data_class,
                policy_ref, owner_principal_id, expires_at_ms
         FROM privacy_erasure.plan_retention_exceptions
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, plan_version, data_class",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let roots = load_roots(connection, tenant_id, job_ids).await?;
    let mut classes = BTreeMap::<(uuid::Uuid, i64), Vec<ErasureDataClass>>::new();
    for (job_id, version, data_class) in class_rows {
        classes
            .entry((job_id, version))
            .or_default()
            .push(ErasureDataClass::from_code(data_class)?);
    }
    let mut exceptions = BTreeMap::<(uuid::Uuid, i64), Vec<ErasureRetentionException>>::new();
    for row in exception_rows {
        let job_id = row.try_get("erasure_job_id")?;
        let version = row.try_get("plan_version")?;
        exceptions
            .entry((job_id, version))
            .or_default()
            .push(decode_exception(&row)?);
    }
    let mut values = HashMap::<uuid::Uuid, Vec<ErasurePlanSnapshot>>::new();
    for row in plan_rows {
        let job_id: uuid::Uuid = row.try_get("erasure_job_id")?;
        let version: i64 = row.try_get("plan_version")?;
        values.entry(job_id).or_default().push(ErasurePlanSnapshot {
            schema_version: dotted(row.try_get("schema_version")?)?,
            erasure_job_id: ErasureJobId::from_uuid(job_id)
                .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid job id"))?,
            plan_version: ErasurePlanVersion::new(from_i64(version, "plan version")?)?,
            root_resource_refs: roots.get(&job_id).cloned().ok_or(
                PostgresPrivacyErasureError::CorruptState("plan has no request roots"),
            )?,
            data_classes: classes.remove(&(job_id, version)).unwrap_or_default(),
            retention_exceptions: exceptions.remove(&(job_id, version)).unwrap_or_default(),
            lineage_fingerprint: digest(row.try_get("lineage_fingerprint")?)?,
            plan_hash: digest(row.try_get("plan_hash")?)?,
            frozen_at: UtcTimestamp::from_unix_millis(row.try_get("frozen_at_ms")?),
        });
    }
    Ok(values)
}

async fn load_transitions(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    job_ids: &[uuid::Uuid],
) -> Result<HashMap<uuid::Uuid, Vec<ErasureTransition>>, PostgresPrivacyErasureError> {
    let rows: Vec<(uuid::Uuid, i64, i16, i64)> = sqlx::query_as(
        "SELECT erasure_job_id, sequence, status, occurred_at_ms
         FROM privacy_erasure.transitions
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, sequence",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let mut values = HashMap::<uuid::Uuid, Vec<ErasureTransition>>::new();
    for (job_id, sequence, status, occurred_at) in rows {
        values.entry(job_id).or_default().push(ErasureTransition {
            sequence: ErasureTransitionSequence::new(from_i64(sequence, "transition sequence")?)?,
            status: ErasureJobStatus::from_code(status)?,
            occurred_at: UtcTimestamp::from_unix_millis(occurred_at),
        });
    }
    Ok(values)
}

async fn load_receipts(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    job_ids: &[uuid::Uuid],
) -> Result<HashMap<uuid::Uuid, ErasureReceipt>, PostgresPrivacyErasureError> {
    let rows = sqlx::query(
        "SELECT erasure_receipt_id, erasure_job_id, schema_version, tenant_id,
                outcome, plan_hash, proof_digest, completed_at_ms
         FROM privacy_erasure.receipts
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let class_rows: Vec<(uuid::Uuid, i16)> = sqlx::query_as(
        "SELECT erasure_job_id, data_class
         FROM privacy_erasure.receipt_completed_data_classes
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, data_class",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let exception_rows = sqlx::query(
        "SELECT erasure_job_id, data_class, policy_ref,
                owner_principal_id, expires_at_ms
         FROM privacy_erasure.receipt_retention_exceptions
         WHERE tenant_id = $1 AND erasure_job_id = ANY($2)
         ORDER BY erasure_job_id, data_class",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_ids)
    .fetch_all(&mut *connection)
    .await?;
    let mut classes = HashMap::<uuid::Uuid, Vec<ErasureDataClass>>::new();
    for (job_id, data_class) in class_rows {
        classes
            .entry(job_id)
            .or_default()
            .push(ErasureDataClass::from_code(data_class)?);
    }
    let mut exceptions = HashMap::<uuid::Uuid, Vec<ErasureRetentionException>>::new();
    for row in exception_rows {
        exceptions
            .entry(row.try_get("erasure_job_id")?)
            .or_default()
            .push(decode_exception(&row)?);
    }
    let mut values = HashMap::new();
    for row in rows {
        let job_id: uuid::Uuid = row.try_get("erasure_job_id")?;
        values.insert(
            job_id,
            ErasureReceipt {
                schema_version: dotted(row.try_get("schema_version")?)?,
                erasure_receipt_id: musteragent_identity::ErasureReceiptId::from_uuid(
                    row.try_get("erasure_receipt_id")?,
                )
                .map_err(|_| {
                    PostgresPrivacyErasureError::CorruptState("invalid erasure receipt id")
                })?,
                erasure_job_id: ErasureJobId::from_uuid(job_id)
                    .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid job id"))?,
                tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                    .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid tenant id"))?,
                outcome: ErasureReceiptOutcome::from_code(row.try_get("outcome")?)?,
                plan_hash: digest(row.try_get("plan_hash")?)?,
                completed_data_classes: classes.remove(&job_id).unwrap_or_default(),
                retention_exceptions: exceptions.remove(&job_id).unwrap_or_default(),
                proof_digest: digest(row.try_get("proof_digest")?)?,
                completed_at: UtcTimestamp::from_unix_millis(row.try_get("completed_at_ms")?),
            },
        );
    }
    Ok(values)
}
