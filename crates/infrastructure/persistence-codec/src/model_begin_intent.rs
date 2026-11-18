//! Stable v1 intent encoding shared by both original Model Journal owners.
//! Keep the domain separator and field bytes compatible with SQLite v151.
use crate::CanonicalJsonContextCodec;
use musteragent_content_ref::Sha256;
use musteragent_context::{ContextCodec as _, ModelIteration};
use musteragent_runtime_model::ProviderRecoveryCapability;

#[derive(Debug, thiserror::Error)]
#[error("model begin intent is invalid")]
pub struct ModelBeginIntentError;

/// Binds every effect-relevant begin argument, including the original typed
/// request, its durable content identity, output ids and recovery semantics.
/// This fingerprint is not a grant or a source-authentication proof.
pub fn model_begin_intent_hash(
    iteration: &ModelIteration,
    recovery: ProviderRecoveryCapability,
) -> Result<Sha256, ModelBeginIntentError> {
    let canonical = CanonicalJsonContextCodec
        .encode_model_request(&iteration.request)
        .map_err(|_| ModelBeginIntentError)?;
    let mut bytes = b"musteragent.model-begin-intent.v1\0".to_vec();
    bytes.extend_from_slice(&iteration.iteration.to_be_bytes());
    for id in [
        iteration.invocation_id.into_uuid(),
        iteration.assistant_message_id.into_uuid(),
        iteration.assistant_output_id.into_uuid(),
        iteration.reasoning_output_id.into_uuid(),
        iteration.request_ref.content_id.into_uuid(),
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    let media = iteration.request_ref.media_type.as_str();
    bytes.extend_from_slice(
        &u64::try_from(media.len())
            .map_err(|_| ModelBeginIntentError)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(media.as_bytes());
    bytes.extend_from_slice(&iteration.request_ref.byte_length.to_be_bytes());
    bytes.extend_from_slice(&iteration.request_ref.sha256.into_bytes());
    let recovery: i64 = match recovery {
        ProviderRecoveryCapability::ResumableStream => 1,
        ProviderRecoveryCapability::QueryableResponse => 2,
        ProviderRecoveryCapability::IdempotentRequest => 3,
        ProviderRecoveryCapability::NonResumable => 4,
    };
    bytes.extend_from_slice(&recovery.to_be_bytes());
    bytes.extend_from_slice(&Sha256::digest(&canonical).into_bytes());
    Ok(Sha256::digest(&bytes))
}

/// The original Home snapshot owns the timing mode. Activity requests must be
/// the exact v4 content admitted by the original planner; a wire shape is not
/// sufficient to authorize an activity policy or an explicit business deadline.
pub fn validate_model_begin_timing(
    iteration: &ModelIteration,
    snapshot_schema: &str,
    request_bytes: &[u8],
) -> Result<(), ModelBeginIntentError> {
    let activity = iteration
        .request
        .invocation
        .timing
        .legacy_deadline()
        .is_none();
    if !activity
        || !matches!(
            snapshot_schema,
            musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION
                | musteragent_runtime_memory::MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION
        )
    {
        return Err(ModelBeginIntentError);
    }
    if activity
        && CanonicalJsonContextCodec
            .encode_model_request(&iteration.request)
            .map_err(|_| ModelBeginIntentError)?
            != request_bytes
    {
        return Err(ModelBeginIntentError);
    }
    Ok(())
}
