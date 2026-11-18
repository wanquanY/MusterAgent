use std::sync::Arc;

use musteragent_identity::{AttemptId, InteractionId, InvocationId, TenantId, UtcTimestamp};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_runtime_capability::{
    ToolInvocationStore, ToolPersistenceDisposition, ToolTerminalOutcome,
};
use musteragent_storage_protection::{
    OpaqueRefKeyring, OpaqueRefProtectionError, ProtectedOpaqueRef,
};
use musteragent_tool::{
    ToolContractError, ToolInvocation, ToolInvocationStatus, ToolRetryContract,
};
use sqlx::{PgPool, Row as _};
use thiserror::Error;

#[derive(Clone)]
pub struct PostgresToolInvocationStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    reference_keys: Arc<OpaqueRefKeyring>,
}

impl PostgresToolInvocationStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            reference_keys,
        }
    }

    /// The historical projection-only port has no Snapshot/Attempt commit lane.
    /// Activity work is exclusively owned by the fenced Runtime Journal.
    async fn require_legacy_invocation(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<(), PostgresToolInvocationError> {
        let fixed: Option<bool> = sqlx::query_scalar("SELECT activity_timing IS NULL FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(tenant_id.into_uuid()).bind(invocation_id.into_uuid()).fetch_optional(&self.pool).await?;
        match fixed {
            Some(true) => Ok(()),
            Some(false) => Err(PostgresToolInvocationError::FencedLivenessJournalRequired),
            None => Err(PostgresToolInvocationError::InvocationNotFound),
        }
    }

    async fn begin_inner(
        &self,
        tenant_id: TenantId,
        conversation_id: musteragent_identity::ConversationId,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> Result<ToolPersistenceDisposition, PostgresToolInvocationError> {
        if invocation.status != ToolInvocationStatus::Requested
            || invocation.execution_attempt_id.is_some()
            || invocation.result_ref.is_some()
            || invocation.interaction_id.is_some()
        {
            return Err(PostgresToolInvocationError::BeginRequiresRequested);
        }
        if invocation.timing.legacy_deadline().is_none() {
            return Err(PostgresToolInvocationError::FencedLivenessJournalRequired);
        }
        let timing =
            musteragent_persistence_codec::encode_invocation_timing_columns(&invocation.timing);
        let inserted = sqlx::query(
            "INSERT INTO tool.tool_invocations (
                 invocation_id, tenant_id, conversation_id, run_id,
                 requested_by_attempt_id, tool_call_id, tool_ref,
                 capability_provider_id, status, effect, approval_policy,
                 retry_contract, idempotency_key, request_content_id,
                 request_hash, deadline_ms, activity_timing, activity_timing_hash
             ) VALUES (
                 $1, $2, $3, $4,
                 $5, $6, $7,
                 $8, $9, $10, $11,
                 $12, $13, $14,
                 $15, $16, $17, $18
             )
             ON CONFLICT DO NOTHING",
        )
        .bind(invocation.invocation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(invocation.run_id.into_uuid())
        .bind(invocation.requested_by_attempt_id.into_uuid())
        .bind(invocation.tool_call_id.as_str())
        .bind(invocation.tool_key.as_str())
        .bind(invocation.capability_provider_id.into_uuid())
        .bind(status_code(invocation.status))
        .bind(effect_code(invocation.effect))
        .bind(approval_code(invocation.approval))
        .bind(retry_code(retry_contract))
        .bind(invocation.idempotency_key.as_str())
        .bind(invocation.request_ref.content_id.into_uuid())
        .bind(invocation.request_ref.sha256.into_bytes().as_slice())
        .bind(timing.legacy_deadline_ms)
        .bind(&timing.activity)
        .bind(&timing.activity_hash)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(ToolPersistenceDisposition::Applied);
        }

        let row = sqlx::query(
            "SELECT invocation_id, tenant_id, conversation_id, run_id,
                    requested_by_attempt_id, tool_call_id, tool_ref,
                    capability_provider_id, status, effect, approval_policy,
                    retry_contract, idempotency_key, request_content_id,
                    request_hash, deadline_ms, activity_timing, activity_timing_hash
             FROM tool.tool_invocations
             WHERE invocation_id = $1
                OR (tenant_id = $2 AND idempotency_key = $3)
                OR (tenant_id = $2 AND run_id = $4 AND tool_call_id = $5)",
        )
        .bind(invocation.invocation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(invocation.idempotency_key.as_str())
        .bind(invocation.run_id.into_uuid())
        .bind(invocation.tool_call_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(PostgresToolInvocationError::IdempotencyConflict);
        };
        let duplicate = row.try_get::<uuid::Uuid, _>("invocation_id")?
            == invocation.invocation_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("tenant_id")? == tenant_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("conversation_id")? == conversation_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("run_id")? == invocation.run_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("requested_by_attempt_id")?
                == invocation.requested_by_attempt_id.into_uuid()
            && row.try_get::<String, _>("tool_call_id")? == invocation.tool_call_id.as_str()
            && row.try_get::<String, _>("tool_ref")? == invocation.tool_key.as_str()
            && row.try_get::<uuid::Uuid, _>("capability_provider_id")?
                == invocation.capability_provider_id.into_uuid()
            && row.try_get::<i16, _>("status")? == status_code(invocation.status)
            && row.try_get::<i16, _>("effect")? == effect_code(invocation.effect)
            && row.try_get::<i16, _>("approval_policy")? == approval_code(invocation.approval)
            && row.try_get::<i16, _>("retry_contract")? == retry_code(retry_contract)
            && row.try_get::<String, _>("idempotency_key")? == invocation.idempotency_key.as_str()
            && row.try_get::<uuid::Uuid, _>("request_content_id")?
                == invocation.request_ref.content_id.into_uuid()
            && row.try_get::<Vec<u8>, _>("request_hash")?
                == invocation.request_ref.sha256.into_bytes()
            && row.try_get::<Option<i64>, _>("deadline_ms")? == timing.legacy_deadline_ms
            && row.try_get::<Option<Vec<u8>>, _>("activity_timing")? == timing.activity
            && row.try_get::<Option<Vec<u8>>, _>("activity_timing_hash")? == timing.activity_hash;
        if duplicate {
            Ok(ToolPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresToolInvocationError::IdempotencyConflict)
        }
    }

    async fn wait_for_approval_inner(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        interaction_id: InteractionId,
    ) -> Result<ToolPersistenceDisposition, PostgresToolInvocationError> {
        self.require_legacy_invocation(tenant_id, invocation_id)
            .await?;
        let result = sqlx::query(
            "UPDATE tool.tool_invocations
             SET status = 2, interaction_id = $3
             WHERE tenant_id = $1 AND invocation_id = $2 AND activity_timing IS NULL AND status = 1",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(interaction_id.into_uuid())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(ToolPersistenceDisposition::Applied);
        }
        let existing = sqlx::query(
            "SELECT status, interaction_id
             FROM tool.tool_invocations
             WHERE tenant_id = $1 AND invocation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        match existing {
            Some(row)
                if row.try_get::<i16, _>("status")?
                    == status_code(ToolInvocationStatus::WaitingInteraction)
                    && row.try_get::<Option<uuid::Uuid>, _>("interaction_id")?
                        == Some(interaction_id.into_uuid()) =>
            {
                Ok(ToolPersistenceDisposition::Duplicate)
            }
            Some(_) => Err(PostgresToolInvocationError::TransitionConflict),
            None => Err(PostgresToolInvocationError::InvocationNotFound),
        }
    }

    async fn start_inner(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        execution_attempt_id: AttemptId,
        started_at: UtcTimestamp,
    ) -> Result<ToolPersistenceDisposition, PostgresToolInvocationError> {
        self.require_legacy_invocation(tenant_id, invocation_id)
            .await?;
        let result = sqlx::query(
            "UPDATE tool.tool_invocations
             SET status = 3, execution_attempt_id = $3, started_at_ms = $4
             WHERE tenant_id = $1 AND invocation_id = $2 AND activity_timing IS NULL AND status IN (1, 2)",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(execution_attempt_id.into_uuid())
        .bind(started_at.unix_millis())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(ToolPersistenceDisposition::Applied);
        }
        let existing = sqlx::query(
            "SELECT status, execution_attempt_id, started_at_ms
             FROM tool.tool_invocations
             WHERE tenant_id = $1 AND invocation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        match existing {
            Some(row)
                if row.try_get::<i16, _>("status")?
                    == status_code(ToolInvocationStatus::Running)
                    && row.try_get::<Option<uuid::Uuid>, _>("execution_attempt_id")?
                        == Some(execution_attempt_id.into_uuid())
                    && row.try_get::<Option<i64>, _>("started_at_ms")?
                        == Some(started_at.unix_millis()) =>
            {
                Ok(ToolPersistenceDisposition::Duplicate)
            }
            Some(_) => Err(PostgresToolInvocationError::TransitionConflict),
            None => Err(PostgresToolInvocationError::InvocationNotFound),
        }
    }

    async fn complete_inner(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        outcome: ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    ) -> Result<ToolPersistenceDisposition, PostgresToolInvocationError> {
        self.require_legacy_invocation(tenant_id, invocation_id)
            .await?;
        let encoded = EncodedOutcome::new(&self.reference_keys, invocation_id, &outcome)?;
        let allowed_statuses: &[i16] = match outcome {
            ToolTerminalOutcome::Succeeded(_)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. }
            | ToolTerminalOutcome::Indeterminate { .. } => &[3],
            ToolTerminalOutcome::Failed { .. } | ToolTerminalOutcome::Cancelled(_) => &[1, 2, 3],
        };
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::ToolTerminal)
            .await?;
        let update = sqlx::query(
            "UPDATE tool.tool_invocations
             SET status = $3, finished_at_ms = $4, terminal_error = $5,
                 terminal_reason = $6, reconciliation_ref_protected = $7,
                 indeterminate_observed_at_ms = CASE WHEN $3 = 6 THEN $4
                     ELSE indeterminate_observed_at_ms END
             WHERE tenant_id = $1 AND invocation_id = $2
               AND activity_timing IS NULL AND status = ANY($8)",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(encoded.status)
        .bind(finished_at.unix_millis())
        .bind(&encoded.terminal_error)
        .bind(&encoded.terminal_reason)
        .bind(&encoded.reconciliation_ref)
        .bind(allowed_statuses)
        .execute(&mut *transaction)
        .await?;
        if update.rows_affected() == 0 {
            let duplicate = terminal_is_duplicate(
                &mut transaction,
                &self.reference_keys,
                tenant_id,
                invocation_id,
                &outcome,
                finished_at,
            )
            .await?;
            transaction.rollback().await?;
            return if duplicate {
                Ok(ToolPersistenceDisposition::Duplicate)
            } else {
                Err(PostgresToolInvocationError::TransitionConflict)
            };
        }
        let result = outcome.retained_result();
        if let Some(result) = result {
            result.validate()?;
            sqlx::query(
                "INSERT INTO tool.tool_results (
                     invocation_id, result_content_id, result_hash,
                     result_schema_key, result_schema_hash,
                     provider_request_ref_protected, committed_at_ms
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (invocation_id) DO UPDATE SET
                 result_content_id = EXCLUDED.result_content_id, result_hash = EXCLUDED.result_hash,
                 result_schema_key = EXCLUDED.result_schema_key, result_schema_hash = EXCLUDED.result_schema_hash,
                 provider_request_ref_protected = EXCLUDED.provider_request_ref_protected,
                 committed_at_ms = EXCLUDED.committed_at_ms",
            )
            .bind(invocation_id.into_uuid())
            .bind(result.result_ref.content_id.into_uuid())
            .bind(result.result_hash.into_bytes().as_slice())
            .bind(result.result_schema.schema_key.as_str())
            .bind(result.result_schema.schema_hash.into_bytes().as_slice())
            .bind(encoded.provider_request_ref)
            .bind(finished_at.unix_millis())
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE tool.tool_invocations
                 SET result_content_id = $3
                 WHERE tenant_id = $1 AND invocation_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(invocation_id.into_uuid())
            .bind(result.result_ref.content_id.into_uuid())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(ToolPersistenceDisposition::Applied)
    }
}

impl ToolInvocationStore for PostgresToolInvocationStore {
    type Error = PostgresToolInvocationError;

    fn begin(
        &self,
        tenant_id: TenantId,
        conversation_id: musteragent_identity::ConversationId,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ToolPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            self.begin_inner(tenant_id, conversation_id, invocation, retry_contract)
                .await
        })
    }

    fn start(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        execution_attempt_id: AttemptId,
        started_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ToolPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            self.start_inner(tenant_id, invocation_id, execution_attempt_id, started_at)
                .await
        })
    }

    fn wait_for_approval(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        interaction_id: InteractionId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ToolPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            self.wait_for_approval_inner(tenant_id, invocation_id, interaction_id)
                .await
        })
    }

    fn complete(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        outcome: ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ToolPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            self.complete_inner(tenant_id, invocation_id, outcome, finished_at)
                .await
        })
    }
}

struct EncodedOutcome {
    status: i16,
    terminal_error: Option<Vec<u8>>,
    terminal_reason: Option<Vec<u8>>,
    reconciliation_ref: Option<Vec<u8>>,
    provider_request_ref: Option<Vec<u8>>,
}

impl EncodedOutcome {
    fn new(
        keyring: &OpaqueRefKeyring,
        invocation_id: InvocationId,
        outcome: &ToolTerminalOutcome,
    ) -> Result<Self, PostgresToolInvocationError> {
        match outcome {
            ToolTerminalOutcome::Succeeded(result)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. } => {
                result.validate()?;
                Ok(Self {
                    status: status_code(ToolInvocationStatus::Succeeded),
                    terminal_error: None,
                    terminal_reason: None,
                    reconciliation_ref: None,
                    provider_request_ref: protect_optional(
                        keyring,
                        invocation_id,
                        "provider-request",
                        result
                            .provider_request_ref
                            .as_ref()
                            .map(|value| value.as_str()),
                    )?,
                })
            }
            ToolTerminalOutcome::Failed {
                error,
                diagnostic_result,
                ..
            } => {
                if let Some(result) = diagnostic_result {
                    result.validate()?;
                }
                Ok(Self {
                    status: status_code(ToolInvocationStatus::Failed),
                    terminal_error: Some(error.code.as_str().as_bytes().to_vec()),
                    terminal_reason: None,
                    reconciliation_ref: None,
                    provider_request_ref: protect_optional(
                        keyring,
                        invocation_id,
                        "provider-request",
                        diagnostic_result.as_ref().and_then(|result| {
                            result
                                .provider_request_ref
                                .as_ref()
                                .map(|value| value.as_str())
                        }),
                    )?,
                })
            }
            ToolTerminalOutcome::Indeterminate {
                reason,
                reconciliation_ref,
                observed_error,
                diagnostic_result,
            } => Ok(Self {
                status: status_code(ToolInvocationStatus::Indeterminate),
                terminal_error: observed_error
                    .as_ref()
                    .map(|error| error.code.as_str().as_bytes().to_vec()),
                terminal_reason: Some(reason.as_str().as_bytes().to_vec()),
                reconciliation_ref: protect_optional(
                    keyring,
                    invocation_id,
                    "reconciliation",
                    Some(reconciliation_ref.as_str()),
                )?,
                provider_request_ref: protect_optional(
                    keyring,
                    invocation_id,
                    "provider-request",
                    diagnostic_result.as_ref().and_then(|result| {
                        result
                            .provider_request_ref
                            .as_ref()
                            .map(|value| value.as_str())
                    }),
                )?,
            }),
            ToolTerminalOutcome::Cancelled(_) => Ok(Self {
                status: status_code(ToolInvocationStatus::Cancelled),
                terminal_error: None,
                terminal_reason: None,
                reconciliation_ref: None,
                provider_request_ref: None,
            }),
        }
    }
}

async fn terminal_is_duplicate(
    transaction: &mut sqlx::PgConnection,
    keyring: &OpaqueRefKeyring,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    outcome: &ToolTerminalOutcome,
    finished_at: UtcTimestamp,
) -> Result<bool, PostgresToolInvocationError> {
    let row = sqlx::query(
        "SELECT invocation.status, invocation.finished_at_ms,
                invocation.terminal_error, invocation.terminal_reason,
                invocation.reconciliation_ref_protected,
                result.result_content_id, result.result_hash,
                result.result_schema_key, result.result_schema_hash,
                result.provider_request_ref_protected
         FROM tool.tool_invocations AS invocation
         LEFT JOIN tool.tool_results AS result
           ON result.invocation_id = invocation.invocation_id
         WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Err(PostgresToolInvocationError::InvocationNotFound);
    };
    let expected = EncodedOutcome::new(keyring, invocation_id, outcome)?;
    if row.try_get::<i16, _>("status")? != expected.status
        || row.try_get::<Option<i64>, _>("finished_at_ms")? != Some(finished_at.unix_millis())
        || row.try_get::<Option<Vec<u8>>, _>("terminal_error")? != expected.terminal_error
        || row.try_get::<Option<Vec<u8>>, _>("terminal_reason")? != expected.terminal_reason
    {
        return Ok(false);
    }
    let stored_reconciliation = open_optional(
        keyring,
        invocation_id,
        "reconciliation",
        row.try_get("reconciliation_ref_protected")?,
    )?;
    let expected_reconciliation = match outcome {
        ToolTerminalOutcome::Indeterminate {
            reconciliation_ref, ..
        } => Some(reconciliation_ref.as_str()),
        _ => None,
    };
    if stored_reconciliation.as_deref() != expected_reconciliation {
        return Ok(false);
    }
    let expected_result = outcome.retained_result();
    match expected_result {
        Some(result) => {
            let stored_provider = open_optional(
                keyring,
                invocation_id,
                "provider-request",
                row.try_get("provider_request_ref_protected")?,
            )?;
            Ok(row.try_get::<Option<uuid::Uuid>, _>("result_content_id")?
                == Some(result.result_ref.content_id.into_uuid())
                && row.try_get::<Option<Vec<u8>>, _>("result_hash")?
                    == Some(result.result_hash.into_bytes().to_vec())
                && row
                    .try_get::<Option<String>, _>("result_schema_key")?
                    .as_deref()
                    == Some(result.result_schema.schema_key.as_str())
                && row.try_get::<Option<Vec<u8>>, _>("result_schema_hash")?
                    == Some(result.result_schema.schema_hash.into_bytes().to_vec())
                && stored_provider.as_deref()
                    == result
                        .provider_request_ref
                        .as_ref()
                        .map(|value| value.as_str()))
        }
        None => Ok(row
            .try_get::<Option<uuid::Uuid>, _>("result_content_id")?
            .is_none()),
    }
}

fn protect_optional(
    keyring: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    field: &str,
    value: Option<&str>,
) -> Result<Option<Vec<u8>>, PostgresToolInvocationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let context = format!("tool-invocation:{invocation_id}:{field}");
    Ok(Some(
        keyring.seal(context.as_bytes(), value)?.as_bytes().to_vec(),
    ))
}

fn open_optional(
    keyring: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    field: &str,
    value: Option<Vec<u8>>,
) -> Result<Option<String>, PostgresToolInvocationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let context = format!("tool-invocation:{invocation_id}:{field}");
    let protected = ProtectedOpaqueRef::from_bytes(value)?;
    Ok(Some(keyring.open(context.as_bytes(), &protected)?))
}

const fn status_code(value: ToolInvocationStatus) -> i16 {
    match value {
        ToolInvocationStatus::Requested => 1,
        ToolInvocationStatus::WaitingInteraction => 2,
        ToolInvocationStatus::Running => 3,
        ToolInvocationStatus::Succeeded => 4,
        ToolInvocationStatus::Failed => 5,
        ToolInvocationStatus::Indeterminate => 6,
        ToolInvocationStatus::Cancelled => 7,
    }
}

const fn effect_code(value: Effect) -> i16 {
    match value {
        Effect::Read => 1,
        Effect::WriteReversible => 2,
        Effect::WriteExternal => 3,
        Effect::Destructive => 4,
    }
}

const fn approval_code(value: ApprovalRequirement) -> i16 {
    match value {
        ApprovalRequirement::Never => 1,
        ApprovalRequirement::ByPolicy => 2,
        ApprovalRequirement::Always => 3,
    }
}

const fn retry_code(value: ToolRetryContract) -> i16 {
    match value {
        ToolRetryContract::ReadOnly => 1,
        ToolRetryContract::IdempotentByKey => 2,
        ToolRetryContract::QueryBeforeRetry => 3,
        ToolRetryContract::NotReplayable => 4,
    }
}

#[derive(Debug, Error)]
pub enum PostgresToolInvocationError {
    #[error("activity execution requires the original fenced Runtime Journal")]
    FencedLivenessJournalRequired,
    #[error(transparent)]
    ToolContract(#[from] ToolContractError),
    #[error(transparent)]
    CapabilityContract(#[from] musteragent_runtime_capability::CapabilityRuntimeError),
    #[error("Tool invocation begin requires a clean Requested state")]
    BeginRequiresRequested,
    #[error("Tool invocation was not found for this tenant")]
    InvocationNotFound,
    #[error("Tool invocation idempotency identity conflicts with another request")]
    IdempotencyConflict,
    #[error("Tool invocation transition conflicts with its durable state")]
    TransitionConflict,
    #[error(transparent)]
    Protection(#[from] OpaqueRefProtectionError),
    #[error("PostgreSQL Tool persistence failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Tool Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}
