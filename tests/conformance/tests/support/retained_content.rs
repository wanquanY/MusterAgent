//! Register immutable request/response metadata for storage-only fixtures.
//! These tests do not read blob bytes; tests that do must use PostgresContentStore.
use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;

pub async fn retain(pool: &sqlx::PgPool, tenant_id: TenantId, reference: &ContentRef) {
    sqlx::query(
        "INSERT INTO artifact.content_refs (content_id, tenant_id, media_type,
         byte_length, content_hash, storage_ref, created_at_ms)
         VALUES ($1, $2, $3, $4::text::numeric, $5, $6, 1)",
    )
    .bind(reference.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .bind(format!(
        "fixture://retained-content/{}",
        reference.content_id
    ))
    .execute(pool)
    .await
    .expect("retain canonical fixture content identity");
}
