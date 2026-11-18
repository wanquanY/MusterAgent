//! Content-owner admission fence. Metadata tombstones and cache invalidation
//! commit with the privacy plan; physical deletion remains ContentStore.delete.
use super::{CONTENT_READY, CONTENT_TOMBSTONED, PostgresContentError, decode_content_ref};
use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;
use musteragent_runtime_content::ContentStoreError;
use sqlx::Row;

pub(crate) async fn tombstone_in_transaction(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    refs: &[ContentRef],
) -> Result<(), PostgresContentError> {
    let mut refs = refs.iter().collect::<Vec<_>>();
    refs.sort_by_key(|r| r.content_id);
    for reference in refs {
        let row = sqlx::query("SELECT tenant_id,media_type,byte_length::text AS byte_length,content_hash,state FROM artifact.content_refs WHERE content_id=$1 FOR UPDATE")
            .bind(reference.content_id.into_uuid()).fetch_optional(&mut *tx).await?
            .ok_or(ContentStoreError::NotFound)?;
        if row.try_get::<Option<uuid::Uuid>, _>("tenant_id")? != Some(tenant.into_uuid()) {
            return Err(ContentStoreError::Unauthorized.into());
        }
        if decode_content_ref(reference.content_id, &row)? != *reference {
            return Err(ContentStoreError::Invariant.into());
        }
        let state: i16 = row.try_get("state")?;
        if !matches!(state, CONTENT_READY | CONTENT_TOMBSTONED) {
            return Err(ContentStoreError::Invariant.into());
        }
        sqlx::query("UPDATE artifact.content_refs SET state=$3,updated_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE tenant_id=$1 AND content_id=$2 AND state<>$3")
            .bind(tenant.into_uuid()).bind(reference.content_id.into_uuid()).bind(CONTENT_TOMBSTONED)
            .execute(&mut *tx).await?;
    }
    Ok(())
}
