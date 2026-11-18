//! Closed-code and database-identity codecs for durable Tool commits.

use super::*;

pub(super) fn decode_status(value: i16) -> Result<ToolInvocationStatus, ToolCommitError> {
    match value {
        1 => Ok(ToolInvocationStatus::Requested),
        2 => Ok(ToolInvocationStatus::WaitingInteraction),
        3 => Ok(ToolInvocationStatus::Running),
        4 => Ok(ToolInvocationStatus::Succeeded),
        5 => Ok(ToolInvocationStatus::Failed),
        6 => Ok(ToolInvocationStatus::Indeterminate),
        7 => Ok(ToolInvocationStatus::Cancelled),
        _ => Err(ToolCommitError::Corrupt),
    }
}

pub(super) fn decode_effect(value: i16) -> Result<Effect, ToolCommitError> {
    match value {
        1 => Ok(Effect::Read),
        2 => Ok(Effect::WriteReversible),
        3 => Ok(Effect::WriteExternal),
        4 => Ok(Effect::Destructive),
        _ => Err(ToolCommitError::Corrupt),
    }
}

pub(super) const fn effect_code(value: Effect) -> i16 {
    match value {
        Effect::Read => 1,
        Effect::WriteReversible => 2,
        Effect::WriteExternal => 3,
        Effect::Destructive => 4,
    }
}

pub(super) fn decode_approval(value: i16) -> Result<ApprovalRequirement, ToolCommitError> {
    match value {
        1 => Ok(ApprovalRequirement::Never),
        2 => Ok(ApprovalRequirement::ByPolicy),
        3 => Ok(ApprovalRequirement::Always),
        _ => Err(ToolCommitError::Corrupt),
    }
}

pub(super) const fn approval_code(value: ApprovalRequirement) -> i16 {
    match value {
        ApprovalRequirement::Never => 1,
        ApprovalRequirement::ByPolicy => 2,
        ApprovalRequirement::Always => 3,
    }
}

pub(super) fn decode_retry(value: i16) -> Result<ToolRetryContract, ToolCommitError> {
    match value {
        1 => Ok(ToolRetryContract::ReadOnly),
        2 => Ok(ToolRetryContract::IdempotentByKey),
        3 => Ok(ToolRetryContract::QueryBeforeRetry),
        4 => Ok(ToolRetryContract::NotReplayable),
        _ => Err(ToolCommitError::Corrupt),
    }
}

pub(super) const fn retry_code(value: ToolRetryContract) -> i16 {
    match value {
        ToolRetryContract::ReadOnly => 1,
        ToolRetryContract::IdempotentByKey => 2,
        ToolRetryContract::QueryBeforeRetry => 3,
        ToolRetryContract::NotReplayable => 4,
    }
}

pub(super) fn parse_u64(value: String) -> Result<u64, ToolCommitError> {
    value.parse().map_err(|_| ToolCommitError::Corrupt)
}

pub(super) fn require_one(value: u64) -> Result<(), ToolCommitError> {
    if value == 1 {
        Ok(())
    } else {
        Err(ToolCommitError::TransitionConflict)
    }
}

pub(super) fn id<T: DatabaseId>(value: uuid::Uuid) -> Result<T, ToolCommitError> {
    T::from_database(value).map_err(|_| ToolCommitError::Corrupt)
}

pub(super) fn optional_id<T: DatabaseId>(
    value: Option<uuid::Uuid>,
) -> Result<Option<T>, ToolCommitError> {
    value.map(id).transpose()
}

pub(super) trait DatabaseId: Sized {
    fn from_database(value: uuid::Uuid) -> Result<Self, ()>;
}

macro_rules! database_ids {
    ($($type:ty),+ $(,)?) => {
        $(
            impl DatabaseId for $type {
                fn from_database(value: uuid::Uuid) -> Result<Self, ()> {
                    Self::from_uuid(value).map_err(|_| ())
                }
            }
        )+
    };
}

database_ids!(
    ActivityId,
    AttemptId,
    AuthorizationReceiptId,
    CapabilityProviderId,
    ConversationId,
    InteractionId,
    PrincipalId,
    musteragent_identity::RunId,
    TenantId,
    WorkspaceId,
);

pub(super) struct StoredResultMetadata {
    pub schema_key: Option<String>,
    pub schema_hash: Option<Vec<u8>>,
    pub provider_request_ref: Option<OpaqueExternalRef>,
}

pub(super) fn decode_diagnostic_result(
    diagnostic_result_ref: Option<ContentRef>,
    diagnostic_result_hash: Option<Sha256>,
    result_ref: Option<&ContentRef>,
    metadata: StoredResultMetadata,
) -> Result<Option<CapabilityResult>, ToolCommitError> {
    Ok(match (diagnostic_result_ref, diagnostic_result_hash) {
        (Some(event_ref), Some(event_hash)) => {
            let persisted_ref = result_ref.ok_or(ToolCommitError::Corrupt)?;
            if event_ref != *persisted_ref || event_hash != persisted_ref.sha256 {
                return Err(ToolCommitError::Corrupt);
            }
            Some(CapabilityResult {
                result_ref: event_ref,
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new(
                        metadata.schema_key.ok_or(ToolCommitError::Corrupt)?,
                    )
                    .map_err(|_| ToolCommitError::Corrupt)?,
                    schema_hash: Sha256::from_bytes(
                        metadata.schema_hash.ok_or(ToolCommitError::Corrupt)?,
                    )
                    .map_err(|_| ToolCommitError::Corrupt)?,
                },
                result_hash: event_hash,
                provider_request_ref: metadata.provider_request_ref,
            })
        }
        (None, None) if result_ref.is_none() => None,
        _ => return Err(ToolCommitError::Corrupt),
    })
}
