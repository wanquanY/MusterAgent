//! PostgreSQL tool-journal storage codecs, durable identities, and error mapping.

use super::*;

pub(super) async fn authorize_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content: &ContentRef,
) -> Result<(), ToolCommitError> {
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND state = 2
               AND media_type = $3 AND byte_length = $4::bigint
               AND content_hash = $5
             FOR KEY SHARE
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().to_vec())
    .fetch_one(&mut *transaction)
    .await?;
    if valid {
        Ok(())
    } else {
        tracing::error!(
            tenant_id = %tenant_id,
            content_id = %content.content_id,
            media_type = content.media_type.as_str(),
            byte_length = content.byte_length,
            "Tool journal rejected unavailable or mismatched content"
        );
        Err(ToolCommitError::ContentUnavailable)
    }
}

pub(super) async fn advance_run_sequence(
    transaction: &mut sqlx::PgConnection,
    run_id: musteragent_identity::RunId,
    sequence: u64,
) -> Result<(), ToolCommitError> {
    let changed = sqlx::query(
        "UPDATE runtime.runs
         SET last_run_sequence = $2::bigint, run_version = run_version + 1
         WHERE run_id = $1",
    )
    .bind(run_id.into_uuid())
    .bind(sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    require_one(changed.rows_affected())
}

pub(super) fn attempt_fence(
    attempt: &AttemptContext,
    home_epoch: musteragent_identity::HomeEpoch,
) -> AttemptFence {
    AttemptFence {
        run_id: attempt.run_id,
        attempt_id: attempt.attempt_id,
        attempt_number: attempt.attempt_number,
        worker_id: attempt.worker_id.clone(),
        lease_id: attempt.lease_id,
        fencing_token: attempt.fencing_token,
        home_epoch,
    }
}

pub(super) fn deterministic_record_id(
    invocation_id: InvocationId,
    tag: &str,
    index: usize,
) -> Result<LedgerRecordId, ToolCommitError> {
    let mut seed = invocation_id.into_uuid().as_bytes().to_vec();
    seed.extend_from_slice(tag.as_bytes());
    seed.extend_from_slice(&u64::try_from(index)?.to_be_bytes());
    LedgerRecordId::from_uuid(deterministic_uuid(&seed)).map_err(|_| ToolCommitError::Corrupt)
}

fn deterministic_uuid(seed: &[u8]) -> uuid::Uuid {
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}

pub(super) fn protect(
    keys: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    field: &str,
    value: &str,
) -> Result<Vec<u8>, ToolCommitError> {
    let context = format!("tool-invocation:{invocation_id}:{field}");
    Ok(keys.seal(context.as_bytes(), value)?.as_bytes().to_vec())
}

pub(super) fn unprotect(
    keys: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    field: &str,
    protected: Vec<u8>,
) -> Result<OpaqueExternalRef, ToolCommitError> {
    let context = format!("tool-invocation:{invocation_id}:{field}");
    let protected = ProtectedOpaqueRef::from_bytes(protected)?;
    let value = keys.open(context.as_bytes(), &protected)?;
    OpaqueExternalRef::new(value).map_err(|_| ToolCommitError::Corrupt)
}

pub(super) fn content_ref(
    id: uuid::Uuid,
    media_type: String,
    byte_length: String,
    hash: Vec<u8>,
) -> Result<ContentRef, ToolCommitError> {
    Ok(ContentRef {
        content_id: musteragent_identity::ContentId::from_uuid(id)
            .map_err(|_| ToolCommitError::Corrupt)?,
        media_type: MediaType::new(media_type).map_err(|_| ToolCommitError::Corrupt)?,
        byte_length: parse_u64(byte_length)?,
        sha256: Sha256::from_bytes(hash).map_err(|_| ToolCommitError::Corrupt)?,
    })
}

pub(super) fn optional_content_ref(
    id: Option<uuid::Uuid>,
    media_type: Option<String>,
    byte_length: Option<String>,
    hash: Option<Vec<u8>>,
) -> Result<Option<ContentRef>, ToolCommitError> {
    match (id, media_type, byte_length, hash) {
        (None, None, None, None) => Ok(None),
        (Some(id), Some(media), Some(length), Some(hash)) => {
            content_ref(id, media, length, hash).map(Some)
        }
        _ => Err(ToolCommitError::Corrupt),
    }
}

pub(super) fn authorization_receipt_from_pg_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AuthorizationReceipt, ToolCommitError> {
    let receipt = AuthorizationReceipt {
        receipt_id: id::<AuthorizationReceiptId>(row.try_get("receipt_id")?)?,
        tenant_id: id::<TenantId>(row.try_get("tenant_id")?)?,
        principal_id: id::<PrincipalId>(row.try_get("principal_id")?)?,
        conversation_id: id::<ConversationId>(row.try_get("conversation_id")?)?,
        workspace_id: optional_id::<WorkspaceId>(row.try_get("workspace_id")?)?,
        source_interaction_id: id::<InteractionId>(row.try_get("source_interaction_id")?)?,
        capability_key: CapabilityKey::from_versioned_key(
            VersionedKey::new(row.try_get::<String, _>("capability_key")?)
                .map_err(|_| ToolCommitError::Corrupt)?,
        ),
        capability_provider_id: id::<CapabilityProviderId>(row.try_get("capability_provider_id")?)?,
        permission_mode: decode_permission_mode(row.try_get("permission_mode")?)?,
        scope_fingerprint: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("scope_fingerprint")?)
            .map_err(|_| ToolCommitError::Corrupt)?,
        issued_at: UtcTimestamp::from_unix_millis(row.try_get("issued_at_ms")?),
        expires_at: UtcTimestamp::from_unix_millis(row.try_get("expires_at_ms")?),
    };
    receipt.validate().map_err(|_| ToolCommitError::Corrupt)?;
    Ok(receipt)
}

pub(super) fn authorization_receipt_matches_query(
    receipt: &AuthorizationReceipt,
    query: &AuthorizationReceiptQuery,
) -> bool {
    receipt.tenant_id == query.tenant_id
        && receipt.principal_id == query.principal_id
        && receipt.conversation_id == query.conversation_id
        && receipt.workspace_id == query.workspace_id
        && receipt.capability_key == query.capability_key
        && receipt.capability_provider_id == query.capability_provider_id
        && receipt.permission_mode == query.permission_mode
        && receipt.scope_fingerprint == query.scope_fingerprint
        && receipt.expires_at > query.observed_at
}

pub(super) fn authorization_receipt_matches_issuance(
    persisted: &AuthorizationReceipt,
    proposed: &AuthorizationReceipt,
    source_responded_at: UtcTimestamp,
    lifetime_ms: i64,
) -> bool {
    persisted.receipt_id == proposed.receipt_id
        && persisted.tenant_id == proposed.tenant_id
        && persisted.principal_id == proposed.principal_id
        && persisted.conversation_id == proposed.conversation_id
        && persisted.workspace_id == proposed.workspace_id
        && persisted.source_interaction_id == proposed.source_interaction_id
        && persisted.capability_key == proposed.capability_key
        && persisted.capability_provider_id == proposed.capability_provider_id
        && persisted.permission_mode == proposed.permission_mode
        && persisted.scope_fingerprint == proposed.scope_fingerprint
        && persisted.issued_at == source_responded_at
        && persisted
            .expires_at
            .unix_millis()
            .checked_sub(persisted.issued_at.unix_millis())
            == Some(lifetime_ms)
}

pub(super) const fn permission_mode_code(value: PermissionMode) -> i16 {
    match value {
        PermissionMode::Supervised => 1,
        PermissionMode::Trusted => 2,
        PermissionMode::Deny => 3,
        PermissionMode::FullAccess => 4,
        PermissionMode::ServiceManaged => 5,
    }
}

fn decode_permission_mode(value: i16) -> Result<PermissionMode, ToolCommitError> {
    match value {
        1 => Ok(PermissionMode::Supervised),
        2 => Ok(PermissionMode::Trusted),
        3 => Ok(PermissionMode::Deny),
        4 => Ok(PermissionMode::FullAccess),
        5 => Ok(PermissionMode::ServiceManaged),
        _ => Err(ToolCommitError::Corrupt),
    }
}

#[derive(Debug, Error)]
pub(super) enum ToolCommitError {
    #[error("execution liveness rejected Tool transition: {0:?}")]
    Liveness(musteragent_execution_liveness::LivenessJournalError),
    #[error("Tool transition is invalid")]
    InvalidTransition,
    #[error("capability was already requested in this task")]
    CapabilityAlreadyRequested,
    #[error("Tool transition changed concurrently")]
    TransitionConflict,
    #[error("Tool invocation was not found")]
    NotFound,
    #[error("Tool invocation idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("Tool referenced Content is unavailable")]
    ContentUnavailable,
    #[error("Tool durable state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Interaction(#[from] PostgresInteractionError),
    #[error(transparent)]
    Protection(#[from] OpaqueRefProtectionError),
    #[error(transparent)]
    NumericConversion(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    CommitLane(#[from] CommitLaneError),
    #[error("governed PostgreSQL Tool Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Audit(#[from] crate::PostgresSecurityAuditError),
}

impl ToolCommitError {
    pub(super) fn port(self) -> ToolExecutionPortError {
        // The public port deliberately erases adapter details. Preserve a closed
        // reason before that boundary without logging SQL, arguments, or secrets.
        let diagnostic = match &self {
            Self::Liveness(_) => "execution_liveness",
            Self::InvalidTransition => "invalid_transition",
            Self::CapabilityAlreadyRequested => "capability_already_requested",
            Self::TransitionConflict => "transition_conflict",
            Self::NotFound => "not_found",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::ContentUnavailable => "content_unavailable",
            Self::Corrupt => "corrupt",
            Self::Database(_) => "database",
            Self::Ledger(_) => "ledger",
            Self::Execution(_) => "execution",
            Self::Interaction(_) => "interaction",
            Self::Protection(_) => "protection",
            Self::NumericConversion(_) => "numeric_conversion",
            Self::CommitLane(_) => "commit_lane",
            Self::Transaction(_) => "transaction",
            Self::Audit(crate::PostgresSecurityAuditError::FactAuthorityMismatch) => {
                "audit_authority"
            }
            Self::Audit(crate::PostgresSecurityAuditError::IdempotencyConflict) => {
                "audit_idempotency"
            }
            Self::Audit(crate::PostgresSecurityAuditError::SourceActionConflict) => {
                "audit_source_action"
            }
            Self::Audit(crate::PostgresSecurityAuditError::SourceTraceMismatch) => "audit_trace",
            Self::Audit(_) => "audit",
        };
        tracing::error!(
            tool.persistence.error = diagnostic,
            "Tool journal operation failed"
        );
        match self {
            Self::Liveness(error) => ToolExecutionPortError {
                code: error.code,
                retryable: error.retryable,
            },
            Self::Execution(PostgresExecutionError::StaleFence)
            | Self::CommitLane(CommitLaneError::StaleFence) => ToolExecutionPortError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
            },
            Self::Database(error)
            | Self::Ledger(PostgresLedgerError::Database(error))
            | Self::Execution(PostgresExecutionError::Database(error))
            | Self::Interaction(PostgresInteractionError::Sql(error))
            | Self::CommitLane(CommitLaneError::Database(error))
            | Self::Audit(crate::PostgresSecurityAuditError::Database(error)) => {
                database_port(&error)
            }
            Self::Transaction(error)
            | Self::Ledger(PostgresLedgerError::Transaction(error))
            | Self::Execution(PostgresExecutionError::Transaction(error))
            | Self::Interaction(PostgresInteractionError::Transaction(error))
            | Self::CommitLane(CommitLaneError::Transaction(error))
            | Self::Audit(crate::PostgresSecurityAuditError::Transaction(error)) => {
                transaction_port(&error)
            }
            Self::CapabilityAlreadyRequested => ToolExecutionPortError {
                code: ErrorCode::PolicyDenied,
                retryable: false,
            },
            Self::ContentUnavailable => ToolExecutionPortError {
                code: ErrorCode::ContentIntegrityFailed,
                retryable: false,
            },
            Self::NotFound => ToolExecutionPortError {
                code: ErrorCode::CapabilityUnavailable,
                retryable: false,
            },
            Self::InvalidTransition | Self::TransitionConflict | Self::IdempotencyConflict => {
                ToolExecutionPortError {
                    code: ErrorCode::RunTransitionInvalid,
                    retryable: false,
                }
            }
            _ => ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            },
        }
    }
}
