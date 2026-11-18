use super::*;
use crate::numeric::encode_u64;
use musteragent_identity::RunId;
use musteragent_persistence_codec::{
    decode_execution_liveness_record, encode_execution_liveness_record,
};
use sqlx::postgres::PgRow;

pub(super) async fn load(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    scope: ExecutionScope,
) -> Result<Option<ExecutionRecord>, StoreError> {
    let (kind, invocation) = scope_parts(scope);
    sqlx::query("SELECT * FROM runtime.execution_liveness WHERE tenant_id=$1 AND run_id=$2 AND scope_kind=$3 AND scope_invocation_id=$4")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(kind).bind(invocation.into_uuid()).fetch_optional(&mut *tx).await?.as_ref().map(decode).transpose()
}
pub(super) fn decode(row: &PgRow) -> Result<ExecutionRecord, StoreError> {
    let bytes: Vec<u8> = row.try_get("encoded_record")?;
    if row.try_get::<Vec<u8>, _>("record_hash")? != Sha256::digest(&bytes).into_bytes() {
        return Err(StoreError::Corrupt);
    }
    let record = decode_execution_liveness_record(&bytes).map_err(|_| StoreError::Corrupt)?;
    let binding = record.binding();
    let (scope_kind, invocation) = scope_parts(binding.scope);
    let expected_due = record
        .next_due_utc()
        .map_err(|_| StoreError::Corrupt)?
        .map(UtcTimestamp::unix_millis);
    if binding.tenant_id.into_uuid() != row.try_get::<uuid::Uuid, _>("tenant_id")?
        || binding.run_id.into_uuid() != row.try_get::<uuid::Uuid, _>("run_id")?
        || binding.owner_attempt_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("owner_attempt_id")?
        || scope_kind != row.try_get::<i16, _>("scope_kind")?
        || invocation.into_uuid() != row.try_get::<uuid::Uuid, _>("scope_invocation_id")?
        || binding.operation_identity.into_bytes()
            != row.try_get::<Vec<u8>, _>("operation_identity")?.as_slice()
        || encode_u64(binding.owner_fence.get(), "owner_fence")?
            != row.try_get::<i64, _>("owner_fence")?
        || encode_u64(binding.producer_generation.get(), "producer_generation")?
            != row.try_get::<i64, _>("producer_generation")?
        || encode_u64(record.revision(), "revision")? != row.try_get::<i64, _>("revision")?
        || kind(&record) != row.try_get::<i16, _>("record_kind")?
        || record.policy().reference().digest.into_bytes()
            != row.try_get::<Vec<u8>, _>("policy_digest")?.as_slice()
        || expected_due != row.try_get::<Option<i64>, _>("due_at_ms")?
        || record.is_closed() != row.try_get::<bool, _>("closed")?
    {
        return Err(StoreError::Corrupt);
    }
    Ok(record)
}
fn kind(record: &ExecutionRecord) -> i16 {
    match record {
        ExecutionRecord::Checkpoint(_) => 1,
        ExecutionRecord::Recovering(_) => 2,
    }
}
pub(super) async fn insert(
    tx: &mut sqlx::PgConnection,
    record: &ExecutionRecord,
) -> Result<(), StoreError> {
    let binding = record.binding();
    let (scope_kind, invocation) = scope_parts(binding.scope);
    let bytes = encode_execution_liveness_record(record);
    sqlx::query("INSERT INTO runtime.execution_liveness(tenant_id,run_id,scope_kind,scope_invocation_id,operation_identity,owner_attempt_id,owner_fence,producer_generation,revision,record_kind,policy_digest,encoded_record,record_hash,due_at_ms,closed) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)")
        .bind(binding.tenant_id.into_uuid()).bind(binding.run_id.into_uuid()).bind(scope_kind).bind(invocation.into_uuid())
        .bind(binding.operation_identity.into_bytes().to_vec()).bind(binding.owner_attempt_id.into_uuid()).bind(encode_u64(binding.owner_fence.get(),"liveness.counter")?).bind(encode_u64(binding.producer_generation.get(),"liveness.counter")?)
        .bind(encode_u64(record.revision(),"liveness.counter")?).bind(kind(record)).bind(record.policy().reference().digest.into_bytes().to_vec())
        .bind(&bytes).bind(Sha256::digest(&bytes).into_bytes().to_vec()).bind(record.next_due_utc().map_err(|_| StoreError::Invalid)?.map(UtcTimestamp::unix_millis)).bind(record.is_closed()).execute(&mut *tx).await?;
    Ok(())
}
pub(super) async fn update(
    tx: &mut sqlx::PgConnection,
    prior: &ExecutionRecord,
    next: &ExecutionRecord,
) -> Result<(), StoreError> {
    let old = prior.binding();
    let binding = next.binding();
    let (scope_kind, invocation) = scope_parts(old.scope);
    let bytes = encode_execution_liveness_record(next);
    let changed=sqlx::query("UPDATE runtime.execution_liveness SET owner_attempt_id=$1,owner_fence=$2,revision=$3,record_kind=$4,encoded_record=$5,record_hash=$6,due_at_ms=$7,closed=$8 WHERE tenant_id=$9 AND run_id=$10 AND scope_kind=$11 AND scope_invocation_id=$12 AND revision=$13 AND record_kind=$14 AND owner_attempt_id=$15 AND owner_fence=$16 AND NOT closed")
        .bind(binding.owner_attempt_id.into_uuid()).bind(encode_u64(binding.owner_fence.get(),"liveness.counter")?).bind(encode_u64(next.revision(),"liveness.counter")?).bind(kind(next)).bind(&bytes).bind(Sha256::digest(&bytes).into_bytes().to_vec())
        .bind(next.next_due_utc().map_err(|_| StoreError::Invalid)?.map(UtcTimestamp::unix_millis)).bind(next.is_closed())
        .bind(old.tenant_id.into_uuid()).bind(old.run_id.into_uuid()).bind(scope_kind).bind(invocation.into_uuid()).bind(encode_u64(prior.revision(),"liveness.counter")?).bind(kind(prior)).bind(old.owner_attempt_id.into_uuid()).bind(encode_u64(old.owner_fence.get(),"liveness.counter")?).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Err(StoreError::Conflict);
    }
    Ok(())
}
