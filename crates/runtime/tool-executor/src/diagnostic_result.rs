//! Immutable diagnostics use the ordinary content/journal result channel. The
//! display message is a separate projection and never limits diagnostic data.
use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_identity::{ContentId, InvocationId, TenantId, VersionedKey};
use musteragent_loop::LoopPortError;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ErrorCode;

pub async fn persist(
    content: &dyn ContentStore,
    tenant: TenantId,
    invocation: InvocationId,
    schema_key: &str,
    schema: &[u8],
    value: &serde_json::Value,
) -> Result<CapabilityResult, LoopPortError> {
    let invariant = || error(ErrorCode::InternalInvariantViolated, false);
    let bytes = serde_json::to_vec(value).map_err(|_| invariant())?;
    let digest = Sha256::digest(&bytes);
    let id = ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        format!("{schema_key}:{digest}").as_bytes(),
    ))
    .map_err(|_| invariant())?;
    let reference = content
        .put_at(
            id,
            PutContent {
                tenant_id: tenant,
                media_type: MediaType::new("application/json").map_err(|_| invariant())?,
                bytes,
            },
        )
        .await
        .map_err(|failure| match failure {
            ContentStoreError::Unavailable => error(ErrorCode::DependencyUnavailable, true),
            ContentStoreError::HashMismatch | ContentStoreError::LengthMismatch => {
                error(ErrorCode::ContentIntegrityFailed, false)
            }
            ContentStoreError::IdentityConflict => error(ErrorCode::IdempotencyConflict, false),
            _ => invariant(),
        })?;
    Ok(CapabilityResult {
        result_hash: reference.sha256,
        result_ref: reference,
        result_schema: SchemaRef {
            schema_key: VersionedKey::new(schema_key).map_err(|_| invariant())?,
            schema_hash: Sha256::digest(schema),
        },
        provider_request_ref: None,
    })
}
fn error(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}
