use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{CommandId, ContentId, CorrelationId, TenantId};
use musteragent_runtime_content::{
    AdmissionContentPlan, AdmissionContentSlot, AdmissionContentSource, AdmissionContentStage,
    ContentStoreError,
};
use musteragent_runtime_contract::{ErrorCode, InputContent, RuntimeError};

use super::RuntimeApplication;
use crate::runtime_error;

impl RuntimeApplication {
    pub(super) async fn prepare_admission_content(
        &self,
        tenant_id: TenantId,
        command_id: CommandId,
        command_fingerprint: Sha256,
        slot: AdmissionContentSlot,
        input: &InputContent,
        correlation_id: CorrelationId,
    ) -> Result<AdmissionContentStage, RuntimeError> {
        let proposed_content_id = match input {
            InputContent::InlineUtf8(_) => derive_content_id(command_id, slot, correlation_id)?,
            InputContent::ContentRef(reference) => reference.content_id,
        };
        let source = match input {
            InputContent::InlineUtf8(value) => AdmissionContentSource::Inline {
                media_type: MediaType::new("text/plain;charset=utf-8")
                    .map_err(|_| admission_invariant(correlation_id))?,
                bytes: value.as_str().as_bytes().to_vec(),
            },
            InputContent::ContentRef(reference) => {
                AdmissionContentSource::CarryForward(reference.clone())
            }
        };
        let plan = AdmissionContentPlan {
            tenant_id,
            command_id,
            command_fingerprint,
            slot,
            proposed_content_id,
            source,
        };
        plan.validate()
            .map_err(|error| map_content_error(error, correlation_id))?;
        let stage = self
            .content
            .prepare_admission(plan.clone())
            .await
            .map_err(|error| map_content_error(error, correlation_id))?;
        stage
            .validate_for(&plan)
            .map_err(|_| admission_invariant(correlation_id))?;
        Ok(stage)
    }
}

fn derive_content_id(
    command_id: CommandId,
    slot: AdmissionContentSlot,
    correlation_id: CorrelationId,
) -> Result<ContentId, RuntimeError> {
    let mut seed = b"musteragent.legacy.runtime.admission-content.v1\0".to_vec();
    seed.extend_from_slice(command_id.into_uuid().as_bytes());
    seed.push(slot.code());
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    ContentId::from_uuid(uuid::Uuid::from_bytes(value))
        .map_err(|_| admission_invariant(correlation_id))
}

fn map_content_error(error: ContentStoreError, correlation_id: CorrelationId) -> RuntimeError {
    let (code, retryable, message) = match error {
        ContentStoreError::TooLarge => (
            ErrorCode::InvalidCommand,
            false,
            "Admission content exceeds the Runtime limit",
        ),
        ContentStoreError::NotFound
        | ContentStoreError::Unauthorized
        | ContentStoreError::LengthMismatch
        | ContentStoreError::HashMismatch
        | ContentStoreError::Invariant => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Admission content identity or immutable bytes failed validation",
        ),
        ContentStoreError::IdentityConflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "Admission content identity was reused with different immutable bytes",
        ),
        ContentStoreError::Unavailable => (
            ErrorCode::DependencyUnavailable,
            true,
            "Admission content staging is unavailable",
        ),
    };
    runtime_error(code, correlation_id, retryable, message)
}

fn admission_invariant(correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::InternalInvariantViolated,
        correlation_id,
        false,
        "Admission content identity derivation failed",
    )
}
