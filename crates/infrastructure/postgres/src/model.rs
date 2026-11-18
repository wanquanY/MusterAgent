use musteragent_identity::UtcTimestamp;
use std::sync::Arc;

use musteragent_identity::InvocationId;
use musteragent_runtime_model::{
    AdmittedModelInvocation, AdmittedModelInvocationStore, AttributedModelUsageFact,
    CompleteModelInvocation, MeteringPublication, ModelContractError, ModelInvocation,
    ModelInvocationPurpose, ModelInvocationRouteRole, ModelInvocationStatus, ModelInvocationStore,
    ModelPersistenceDisposition,
};
use musteragent_storage_protection::{
    OpaqueRefKeyring, OpaqueRefProtectionError, ProtectedOpaqueRef,
};
use sqlx::{PgPool, Row as _};
use thiserror::Error;

const METERING_TOPIC: &str = "musteragent.model-usage.v1";

mod diagnostics;
mod supervision;

#[derive(Clone)]
pub struct PostgresModelInvocationStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
    liveness: Option<Arc<crate::PostgresFencedExecutionLivenessJournal>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PersistedModelInvocationRecovery {
    pub early_provider_cause: Option<musteragent_runtime_model::ProviderErrorCode>,
    pub request_hash: musteragent_content_ref::Sha256,
    pub model_route_ref: musteragent_runtime_model::ModelRouteRef,
    pub state: PersistedModelInvocationRecoveryState,
    pub failure_retryable: bool,
    pub retry_not_before: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PersistedModelInvocationRecoveryState {
    /// The Provider may already have accepted the request, but Runtime has no
    /// durable terminal proof. Recovery must never dispatch it again.
    Indeterminate,
    Succeeded(musteragent_content_ref::ContentRef),
    Failed(musteragent_runtime_contract::ErrorCode),
    Cancelled,
}

impl PostgresModelInvocationStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self::with_security_audit(database, reference_keys, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            reference_keys,
            audit,
            liveness: None,
        }
    }

    /// Loads the immutable response identity of a previously completed
    /// physical invocation. This is the recovery path for deterministic
    /// nested invocations whose caller crashed after completion but before
    /// publishing its enclosing Context/Capability commit.
    pub async fn load_recovery(
        &self,
        invocation_id: InvocationId,
    ) -> Result<Option<PersistedModelInvocationRecovery>, PostgresModelInvocationError> {
        let row = sqlx::query(
            "SELECT invocation.request_hash, invocation.model_route_ref,
                    invocation.status, invocation.terminal_error, invocation.early_provider_cause, invocation.terminal_error_retryable, invocation.retry_not_before_ms,
                    content.content_id, content.media_type,
                    content.byte_length::text AS byte_length,
                    content.content_hash
             FROM runtime.model_invocations AS invocation
             LEFT JOIN artifact.content_refs AS content
               ON content.tenant_id = invocation.tenant_id
              AND content.content_id = invocation.response_content_id
              AND content.state = 2
             WHERE invocation.invocation_id = $1
            ",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state = match row.try_get::<i16, _>("status")? {
            1 => PersistedModelInvocationRecoveryState::Indeterminate,
            2 => PersistedModelInvocationRecoveryState::Succeeded(
                musteragent_content_ref::ContentRef {
                    content_id: musteragent_identity::ContentId::from_uuid(
                        row.try_get::<Option<uuid::Uuid>, _>("content_id")?
                            .ok_or(PostgresModelInvocationError::CorruptState)?,
                    )
                    .map_err(|_| PostgresModelInvocationError::CorruptState)?,
                    media_type: musteragent_content_ref::MediaType::new(
                        row.try_get::<Option<String>, _>("media_type")?
                            .ok_or(PostgresModelInvocationError::CorruptState)?,
                    )
                    .map_err(|_| PostgresModelInvocationError::CorruptState)?,
                    byte_length: row
                        .try_get::<Option<String>, _>("byte_length")?
                        .ok_or(PostgresModelInvocationError::CorruptState)?
                        .parse()
                        .map_err(|_| PostgresModelInvocationError::CorruptState)?,
                    sha256: musteragent_content_ref::Sha256::from_bytes(
                        row.try_get::<Option<Vec<u8>>, _>("content_hash")?
                            .ok_or(PostgresModelInvocationError::CorruptState)?,
                    )?,
                },
            ),
            3 => {
                let bytes = row
                    .try_get::<Option<Vec<u8>>, _>("terminal_error")?
                    .ok_or(PostgresModelInvocationError::CorruptState)?;
                let code = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(musteragent_runtime_contract::ErrorCode::from_safe_code)
                    .ok_or(PostgresModelInvocationError::CorruptState)?;
                PersistedModelInvocationRecoveryState::Failed(code)
            }
            4 => PersistedModelInvocationRecoveryState::Cancelled,
            _ => return Err(PostgresModelInvocationError::CorruptState),
        };
        Ok(Some(PersistedModelInvocationRecovery {
            early_provider_cause: row
                .try_get::<Option<String>, _>("early_provider_cause")?
                .as_deref()
                .map(|cause| {
                    musteragent_runtime_model::ProviderErrorCode::from_alternate_provider_cause(
                        cause,
                    )
                    .ok_or(PostgresModelInvocationError::CorruptState)
                })
                .transpose()?,
            request_hash: musteragent_content_ref::Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("request_hash")?,
            )?,
            model_route_ref: musteragent_runtime_model::ModelRouteRef::from_key(
                musteragent_identity::VersionedKey::new(
                    row.try_get::<String, _>("model_route_ref")?,
                )
                .map_err(|_| PostgresModelInvocationError::CorruptState)?,
            ),
            state,
            retry_not_before: row
                .try_get::<Option<i64>, _>("retry_not_before_ms")?
                .map(UtcTimestamp::from_unix_millis),
            failure_retryable: row
                .try_get::<Option<bool>, _>("terminal_error_retryable")?
                .unwrap_or(false),
        }))
    }

    async fn begin_inner(
        &self,
        invocation: ModelInvocation,
        parent_invocation_id: Option<InvocationId>,
    ) -> Result<ModelPersistenceDisposition, PostgresModelInvocationError> {
        let mut connection = self.pool.acquire().await?;
        self.begin_in_connection(&mut connection, invocation, parent_invocation_id)
            .await
    }

    async fn begin_in_connection(
        &self,
        connection: &mut sqlx::PgConnection,
        invocation: ModelInvocation,
        parent_invocation_id: Option<InvocationId>,
    ) -> Result<ModelPersistenceDisposition, PostgresModelInvocationError> {
        invocation.validate()?;
        if invocation.status != ModelInvocationStatus::Started {
            return Err(PostgresModelInvocationError::BeginRequiresStarted);
        }
        let continuation_input = protect_optional(
            &self.reference_keys,
            invocation.invocation_id,
            "continuation-input",
            invocation
                .continuation_input_ref
                .as_ref()
                .map(|value| value.as_str()),
        )?;
        let result = sqlx::query(
            "INSERT INTO runtime.model_invocations (
                 invocation_id, tenant_id, conversation_id, run_id, attempt_id,
                 purpose, route_role, status, model_route_ref, route_identity_hash,
                 credential_grant_ref,
                 requested_model_key, pricing_version_ref, idempotency_key,
                 request_content_id, request_hash, continuation_input_ref, started_at_ms, parent_invocation_id
             ) VALUES (
                 $1, $2, $3, $4, $5,
                 $6, $7, $8, $9, $10,
                 $11, $12, $13, $14, $15,
                 $16, $17, $18, $19
             )
             ON CONFLICT DO NOTHING",
        )
        .bind(invocation.invocation_id.into_uuid())
        .bind(invocation.tenant_id.into_uuid())
        .bind(invocation.conversation_id.into_uuid())
        .bind(invocation.run_id.into_uuid())
        .bind(invocation.attempt_id.into_uuid())
        .bind(purpose_code(invocation.purpose))
        .bind(route_role_code(invocation.route_role))
        .bind(status_code(invocation.status))
        .bind(invocation.model_route_ref.as_str())
        .bind(invocation.route_identity_hash.into_bytes().to_vec())
        .bind(invocation.credential_grant_ref.as_str())
        .bind(invocation.requested_model_key.as_str())
        .bind(invocation.pricing_version_ref.as_str())
        .bind(invocation.idempotency_key.as_str())
        .bind(invocation.request_ref.content_id.into_uuid())
        .bind(invocation.request_hash.into_bytes().as_slice())
        .bind(continuation_input)
        .bind(invocation.started_at.unix_millis())
        .bind(parent_invocation_id.map(InvocationId::into_uuid))
        .execute(&mut *connection)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(ModelPersistenceDisposition::Applied);
        }
        let existing = sqlx::query(
            "SELECT invocation_id, tenant_id, conversation_id, run_id, attempt_id,
                    purpose, route_role, model_route_ref, route_identity_hash,
                    credential_grant_ref,
                    requested_model_key, pricing_version_ref, request_hash,
                    idempotency_key, request_content_id, started_at_ms, parent_invocation_id
             FROM runtime.model_invocations
             WHERE invocation_id = $1
                OR (tenant_id = $2 AND idempotency_key = $3)",
        )
        .bind(invocation.invocation_id.into_uuid())
        .bind(invocation.tenant_id.into_uuid())
        .bind(invocation.idempotency_key.as_str())
        .fetch_optional(&mut *connection)
        .await?;
        let Some(existing) = existing else {
            return Err(PostgresModelInvocationError::IdempotencyConflict);
        };
        if immutable_identity_matches(&existing, &invocation)?
            && existing.try_get::<Option<uuid::Uuid>, _>("parent_invocation_id")?
                == parent_invocation_id.map(InvocationId::into_uuid)
        {
            Ok(ModelPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresModelInvocationError::IdempotencyConflict)
        }
    }

    async fn complete_inner(
        &self,
        completion: CompleteModelInvocation,
    ) -> Result<ModelPersistenceDisposition, PostgresModelInvocationError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::ModelTerminalUsage)
            .await?;
        let disposition = self
            .complete_in_transaction(&mut transaction, completion, false)
            .await?;
        transaction.commit().await?;
        Ok(disposition)
    }

    pub(crate) async fn complete_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        completion: CompleteModelInvocation,
        supervised: bool,
    ) -> Result<ModelPersistenceDisposition, PostgresModelInvocationError> {
        completion.validate()?;
        let invocation = &completion.invocation;
        if invocation.purpose
            == musteragent_runtime_model::ModelInvocationPurpose::ContextCompaction
        {
            let activity: Option<bool> = sqlx::query_scalar("SELECT activity_timing IS NOT NULL FROM runtime.context_compaction_slots WHERE invocation_id=$1")
                .bind(invocation.invocation_id.into_uuid()).fetch_optional(&mut *transaction).await?;
            if activity.is_some_and(|activity| activity != supervised) {
                return Err(PostgresModelInvocationError::IdempotencyConflict);
            }
        }

        let original = sqlx::query(
            "SELECT invocation_id, tenant_id, conversation_id, run_id, attempt_id,
                    purpose, route_role, model_route_ref, route_identity_hash, credential_grant_ref,
                    requested_model_key, pricing_version_ref, request_hash, idempotency_key,
                    request_content_id, started_at_ms
             FROM runtime.model_invocations WHERE invocation_id=$1 FOR UPDATE",
        )
        .bind(invocation.invocation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresModelInvocationError::TerminalConflict)?;
        if !immutable_identity_matches(&original, invocation)? {
            return Err(PostgresModelInvocationError::IdempotencyConflict);
        }
        let provider_request = protect_optional(
            &self.reference_keys,
            invocation.invocation_id,
            "provider-request",
            invocation
                .provider_request_ref
                .as_ref()
                .map(|value| value.as_str()),
        )?;
        let continuation_output = protect_optional(
            &self.reference_keys,
            invocation.invocation_id,
            "continuation-output",
            invocation
                .continuation_output_ref
                .as_ref()
                .map(|value| value.as_str()),
        )?;
        let usage_blob = invocation.usage.map(encode_usage);
        let terminal_error = invocation
            .error
            .as_ref()
            .map(|error| error.code.as_str().as_bytes().to_vec());
        let update = sqlx::query(
            "UPDATE runtime.model_invocations
             SET status = $2,
                 response_content_id = $3,
                 provider_request_ref = $4,
                 continuation_output_ref = $5,
                 usage_fact = $6,
                 finished_at_ms = $7,
                 terminal_error = $8,
                 response_model_key = $9,
                 terminal_error_retryable = $10, retry_not_before_ms = $11, early_provider_cause = $12
             WHERE invocation_id = $1 AND status = 1",
        )
        .bind(invocation.invocation_id.into_uuid())
        .bind(status_code(invocation.status))
        .bind(
            invocation
                .response_ref
                .as_ref()
                .map(|value| value.content_id.into_uuid()),
        )
        .bind(provider_request)
        .bind(continuation_output)
        .bind(usage_blob)
        .bind(invocation.finished_at.map(|value| value.unix_millis()))
        .bind(terminal_error)
        .bind(
            invocation
                .response_model_key
                .as_ref()
                .map(musteragent_model::ModelKey::as_str),
        )
        .bind(invocation.error.as_ref().map(|error| error.retryable))
        .bind(completion.retry_not_before.map(UtcTimestamp::unix_millis))
        .bind(completion.early_provider_cause.map(|cause| cause.as_str()))
        .execute(&mut *transaction)
        .await?;
        if update.rows_affected() == 0 {
            let duplicate =
                terminal_is_duplicate(transaction, &self.reference_keys, &completion).await?;
            return if duplicate {
                self.audit
                    .append_model_terminal_in_transaction(
                        transaction,
                        invocation.tenant_id,
                        invocation.invocation_id,
                    )
                    .await?;
                Ok(ModelPersistenceDisposition::Duplicate)
            } else {
                Err(PostgresModelInvocationError::TerminalConflict)
            };
        }
        if let Some(fact) = completion.usage_fact.as_ref() {
            if let Some(publication) = completion.metering_publication.as_ref() {
                insert_metering_outbox(transaction, fact, publication).await?;
            }
            insert_usage_fact(transaction, fact, completion.metering_publication.as_ref()).await?;
        }
        self.audit
            .append_model_terminal_in_transaction(
                transaction,
                invocation.tenant_id,
                invocation.invocation_id,
            )
            .await?;
        Ok(ModelPersistenceDisposition::Applied)
    }

    async fn complete_observed(
        &self,
        completion: CompleteModelInvocation,
    ) -> Result<ModelPersistenceDisposition, PostgresModelInvocationError> {
        let invocation_id = completion.invocation.invocation_id;
        self.complete_inner(completion)
            .await
            .inspect_err(|error| diagnostics::completion_failed(invocation_id, error))
    }
}

impl ModelInvocationStore for PostgresModelInvocationStore {
    type Error = PostgresModelInvocationError;

    fn begin(
        &self,
        invocation: ModelInvocation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ModelPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.begin_inner(invocation, None).await })
    }

    fn complete(
        &self,
        completion: CompleteModelInvocation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ModelPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.complete_observed(completion).await })
    }
}

impl AdmittedModelInvocationStore for PostgresModelInvocationStore {
    fn usage_attribution(
        &self,
        run_id: musteragent_identity::RunId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<musteragent_runtime_model::ModelUsageAttribution, Self::Error>,
    > {
        Box::pin(async move { usage_attribution(&mut *self.pool.acquire().await?, run_id).await })
    }

    type Error = PostgresModelInvocationError;

    fn begin_supervised(
        &self,
        request: musteragent_runtime_model::SupervisedModelAdmission,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<
            musteragent_runtime_model::SupervisedModelStart,
            musteragent_runtime_model::SupervisedModelStoreError<Self::Error>,
        >,
    > {
        Box::pin(self.begin_supervised_inner(request))
    }

    fn complete_supervised(
        &self,
        request: musteragent_runtime_model::SupervisedModelCompletion,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<
            ModelPersistenceDisposition,
            musteragent_runtime_model::SupervisedModelStoreError<Self::Error>,
        >,
    > {
        Box::pin(self.complete_supervised_inner(request))
    }

    fn failure_code(error: &Self::Error) -> musteragent_runtime_contract::ErrorCode {
        use crate::database_failure::{PostgresFailureClass, classify};
        use musteragent_runtime_contract::ErrorCode;
        match error {
            Self::Error::Sql(source)
            | Self::Error::Audit(crate::PostgresSecurityAuditError::Database(source))
            | Self::Error::Audit(crate::PostgresSecurityAuditError::Transaction(
                crate::PostgresTransactionError::Database(source),
            ))
            | Self::Error::Transaction(crate::PostgresTransactionError::Database(source))
                if classify(source) == PostgresFailureClass::Unavailable =>
            {
                ErrorCode::DependencyUnavailable
            }
            Self::Error::Transaction(
                crate::PostgresTransactionError::DeadlineExceeded { .. }
                | crate::PostgresTransactionError::CommitOutcomeUnknown { .. },
            )
            | Self::Error::Audit(crate::PostgresSecurityAuditError::Transaction(
                crate::PostgresTransactionError::DeadlineExceeded { .. }
                | crate::PostgresTransactionError::CommitOutcomeUnknown { .. },
            )) => ErrorCode::DependencyUnavailable,
            Self::Error::IdempotencyConflict | Self::Error::TerminalConflict => {
                ErrorCode::IdempotencyConflict
            }
            _ => ErrorCode::InternalInvariantViolated,
        }
    }

    fn recovery_cooldown(
        &self,
        admission: AdmittedModelInvocation,
        now: musteragent_identity::UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_model::ModelRecoveryCooldown>, Self::Error>,
    > {
        Box::pin(async move {
            admission.validate()?;
            let Some(parent) = admission.parent_invocation_id else {
                return Ok(None);
            };
            let invocation = &admission.invocation;
            let row = sqlx::query("SELECT terminal_error, early_provider_cause, retry_not_before_ms FROM runtime.model_invocations WHERE tenant_id = $1 AND run_id = $2 AND parent_invocation_id = $3 AND route_identity_hash = $4 AND status = 3 AND terminal_error_retryable = TRUE AND retry_not_before_ms > $5 ORDER BY retry_not_before_ms DESC LIMIT 1")
                .bind(invocation.tenant_id.into_uuid())
                .bind(invocation.run_id.into_uuid())
                .bind(parent.into_uuid())
                .bind(invocation.route_identity_hash.into_bytes().to_vec())
                .bind(now.unix_millis())
                .fetch_optional(&self.pool).await?;
            row.map(|row| {
                let code = String::from_utf8(row.try_get::<Vec<u8>, _>("terminal_error")?)
                    .map_err(|_| Self::Error::CorruptState)?;
                Ok(musteragent_runtime_model::ModelRecoveryCooldown {
                    early_provider_cause: row.try_get::<Option<String>, _>("early_provider_cause")?.as_deref().map(|cause| musteragent_runtime_model::ProviderErrorCode::from_alternate_provider_cause(cause).ok_or(Self::Error::CorruptState)).transpose()?,
                    code: musteragent_runtime_contract::ErrorCode::from_safe_code(&code)
                        .ok_or(Self::Error::CorruptState)?,
                    not_before: musteragent_identity::UtcTimestamp::from_unix_millis(
                        row.try_get("retry_not_before_ms")?,
                    ),
                })
            })
            .transpose()
        })
    }

    fn begin_admitted(
        &self,
        admission: AdmittedModelInvocation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ModelPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            admission.validate()?;
            self.begin_inner(admission.invocation, admission.parent_invocation_id)
                .await
        })
    }

    fn complete(
        &self,
        completion: CompleteModelInvocation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ModelPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.complete_observed(completion).await })
    }
}

pub(crate) async fn insert_metering_outbox(
    transaction: &mut sqlx::PgConnection,
    fact: &AttributedModelUsageFact,
    publication: &MeteringPublication,
) -> Result<(), PostgresModelInvocationError> {
    sqlx::query(
        "INSERT INTO event_store.outbox (
             message_id, tenant_id, conversation_id, topic, payload,
             payload_hash, committed_at_ms, available_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)",
    )
    .bind(publication.message_id.into_uuid())
    .bind(fact.envelope.tenant_id.into_uuid())
    .bind(fact.envelope.conversation_id.into_uuid())
    .bind(METERING_TOPIC)
    .bind(&publication.payload)
    .bind(publication.payload_hash.into_bytes().as_slice())
    .bind(fact.envelope.observed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_usage_fact(
    transaction: &mut sqlx::PgConnection,
    fact: &AttributedModelUsageFact,
    publication: Option<&MeteringPublication>,
) -> Result<(), PostgresModelInvocationError> {
    if let Some(attribution) = fact.envelope.attribution
        && attribution != usage_attribution(transaction, fact.envelope.run_id).await?
    {
        return Err(PostgresModelInvocationError::CorruptState);
    }
    let usage = &fact.usage;
    sqlx::query(
        "INSERT INTO runtime.model_usage_facts (
             invocation_id, tenant_id, conversation_id, run_id, attempt_id,
             purpose, model_route_ref, credential_grant_ref,
             requested_model_key, response_model_key, pricing_version_ref,
             input_tokens_uncached, output_tokens, reasoning_tokens,
             provider_cache_read_tokens, provider_cache_write_tokens,
             provider_cache_write_1h_tokens, cancelled, total_tokens,
             metering_dedup_key, observed_at_ms, outbox_message_id, parent_run_id, billing_subject
         ) VALUES (
             $1, $2, $3, $4, $5,
             $6, $7, $8, $9, $10, $11,
             $12, $13, $14,
             $15, $16, $17, $18, $19,
             $20, $21, $22, $23, $24
         )",
    )
    .bind(usage.invocation_id.into_uuid())
    .bind(fact.envelope.tenant_id.into_uuid())
    .bind(fact.envelope.conversation_id.into_uuid())
    .bind(fact.envelope.run_id.into_uuid())
    .bind(fact.envelope.attempt_id.into_uuid())
    .bind(purpose_code(usage.purpose))
    .bind(usage.model_route_ref.as_str())
    .bind(usage.credential_grant_ref.as_str())
    .bind(usage.requested_model_key.as_str())
    .bind(usage.response_model_key.as_str())
    .bind(usage.pricing_version_ref.as_str())
    .bind(to_i64(usage.input_tokens)?)
    .bind(to_i64(usage.output_tokens)?)
    .bind(to_i64(usage.reasoning_tokens)?)
    .bind(to_i64(usage.provider_cache_read_tokens)?)
    .bind(to_i64(usage.provider_cache_write_tokens)?)
    .bind(to_i64(usage.provider_cache_write_1h_tokens)?)
    .bind(usage.cancelled)
    .bind(fact.provider_total_tokens.map(to_i64).transpose()?)
    .bind(usage.metering_dedup_key.as_str())
    .bind(fact.envelope.observed_at.unix_millis())
    .bind(publication.map(|value| value.message_id.into_uuid()))
    .bind(
        fact.envelope
            .attribution
            .and_then(|a| a.parent_run_id)
            .map(|id| id.into_uuid()),
    )
    .bind(
        fact.envelope
            .attribution
            .map(|a| a.billing_subject.into_uuid()),
    )
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn terminal_is_duplicate(
    transaction: &mut sqlx::PgConnection,
    keyring: &OpaqueRefKeyring,
    completion: &CompleteModelInvocation,
) -> Result<bool, PostgresModelInvocationError> {
    let invocation = &completion.invocation;
    let row = sqlx::query(
        "SELECT invocation.status, invocation.finished_at_ms,
                invocation.response_content_id, invocation.provider_request_ref,
                invocation.continuation_output_ref, invocation.terminal_error, invocation.early_provider_cause, invocation.terminal_error_retryable, invocation.retry_not_before_ms,
                invocation.response_model_key AS invocation_response_model_key,
                usage.input_tokens_uncached, usage.output_tokens,
                usage.reasoning_tokens, usage.provider_cache_read_tokens,
                usage.provider_cache_write_tokens,
                usage.provider_cache_write_1h_tokens, usage.cancelled,
                usage.total_tokens, usage.metering_dedup_key,
                usage.credential_grant_ref, usage.requested_model_key,
                usage.response_model_key, usage.pricing_version_ref,
                outbox.payload_hash
         FROM runtime.model_invocations AS invocation
         LEFT JOIN runtime.model_usage_facts AS usage
           ON usage.invocation_id = invocation.invocation_id
         LEFT JOIN event_store.outbox AS outbox
           ON outbox.message_id = usage.outbox_message_id
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    if row
        .try_get::<Option<bool>, _>("terminal_error_retryable")?
        .is_some_and(|retryable| {
            Some(retryable) != invocation.error.as_ref().map(|error| error.retryable)
        })
        || row.try_get::<Option<i64>, _>("retry_not_before_ms")?
            != completion.retry_not_before.map(UtcTimestamp::unix_millis)
        || row.try_get::<i16, _>("status")? != status_code(invocation.status)
        || row.try_get::<Option<i64>, _>("finished_at_ms")?
            != invocation.finished_at.map(|value| value.unix_millis())
        || row.try_get::<Option<uuid::Uuid>, _>("response_content_id")?
            != invocation
                .response_ref
                .as_ref()
                .map(|value| value.content_id.into_uuid())
        || row
            .try_get::<Option<String>, _>("early_provider_cause")?
            .as_deref()
            != completion.early_provider_cause.map(|cause| cause.as_str())
        || row.try_get::<Option<Vec<u8>>, _>("terminal_error")?
            != invocation
                .error
                .as_ref()
                .map(|error| error.code.as_str().as_bytes().to_vec())
        || row
            .try_get::<Option<String>, _>("invocation_response_model_key")?
            .as_deref()
            != invocation
                .response_model_key
                .as_ref()
                .map(musteragent_model::ModelKey::as_str)
    {
        return Ok(false);
    }
    let provider_request = open_optional(
        keyring,
        invocation.invocation_id,
        "provider-request",
        row.try_get("provider_request_ref")?,
    )?;
    let continuation_output = open_optional(
        keyring,
        invocation.invocation_id,
        "continuation-output",
        row.try_get("continuation_output_ref")?,
    )?;
    if provider_request.as_deref()
        != invocation
            .provider_request_ref
            .as_ref()
            .map(|value| value.as_str())
        || continuation_output.as_deref()
            != invocation
                .continuation_output_ref
                .as_ref()
                .map(|value| value.as_str())
    {
        return Ok(false);
    }
    match (
        completion.usage_fact.as_ref(),
        completion.metering_publication.as_ref(),
    ) {
        (None, None) => Ok(row
            .try_get::<Option<i64>, _>("input_tokens_uncached")?
            .is_none()),
        (Some(fact), publication) => {
            let usage = &fact.usage;
            Ok(row.try_get::<Option<i64>, _>("input_tokens_uncached")?
                == Some(to_i64(usage.input_tokens)?)
                && row.try_get::<Option<i64>, _>("output_tokens")?
                    == Some(to_i64(usage.output_tokens)?)
                && row.try_get::<Option<i64>, _>("reasoning_tokens")?
                    == Some(to_i64(usage.reasoning_tokens)?)
                && row.try_get::<Option<i64>, _>("provider_cache_read_tokens")?
                    == Some(to_i64(usage.provider_cache_read_tokens)?)
                && row.try_get::<Option<i64>, _>("provider_cache_write_tokens")?
                    == Some(to_i64(usage.provider_cache_write_tokens)?)
                && row.try_get::<Option<i64>, _>("provider_cache_write_1h_tokens")?
                    == Some(to_i64(usage.provider_cache_write_1h_tokens)?)
                && row.try_get::<Option<bool>, _>("cancelled")? == Some(usage.cancelled)
                && row.try_get::<Option<i64>, _>("total_tokens")?
                    == fact.provider_total_tokens.map(to_i64).transpose()?
                && row
                    .try_get::<Option<String>, _>("metering_dedup_key")?
                    .as_deref()
                    == Some(usage.metering_dedup_key.as_str())
                && row
                    .try_get::<Option<String>, _>("credential_grant_ref")?
                    .as_deref()
                    == Some(usage.credential_grant_ref.as_str())
                && row
                    .try_get::<Option<String>, _>("requested_model_key")?
                    .as_deref()
                    == Some(usage.requested_model_key.as_str())
                && row
                    .try_get::<Option<String>, _>("response_model_key")?
                    .as_deref()
                    == Some(usage.response_model_key.as_str())
                && row
                    .try_get::<Option<String>, _>("pricing_version_ref")?
                    .as_deref()
                    == Some(usage.pricing_version_ref.as_str())
                && row.try_get::<Option<Vec<u8>>, _>("payload_hash")?
                    == publication.map(|value| value.payload_hash.into_bytes().to_vec()))
        }
        _ => Ok(false),
    }
}

pub(crate) fn protect_optional(
    keyring: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    field: &str,
    value: Option<&str>,
) -> Result<Option<Vec<u8>>, PostgresModelInvocationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let context = format!("model-invocation:{invocation_id}:{field}");
    Ok(Some(
        keyring.seal(context.as_bytes(), value)?.as_bytes().to_vec(),
    ))
}

pub(crate) fn open_optional(
    keyring: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    field: &str,
    value: Option<Vec<u8>>,
) -> Result<Option<String>, PostgresModelInvocationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let context = format!("model-invocation:{invocation_id}:{field}");
    let protected = ProtectedOpaqueRef::from_bytes(value)?;
    Ok(Some(keyring.open(context.as_bytes(), &protected)?))
}

pub(crate) fn purpose_code(value: ModelInvocationPurpose) -> i16 {
    match value {
        ModelInvocationPurpose::Primary => 1,
        ModelInvocationPurpose::ContextCompaction => 2,
        ModelInvocationPurpose::Finalization => 3,
        ModelInvocationPurpose::Verification => 4,
        ModelInvocationPurpose::EvolutionEvaluation => 5,
        ModelInvocationPurpose::EvolutionInduction => 6,
    }
}

pub(crate) fn route_role_code(value: ModelInvocationRouteRole) -> i16 {
    i16::from(value.canonical_code())
}

pub(crate) fn status_code(value: ModelInvocationStatus) -> i16 {
    match value {
        ModelInvocationStatus::Started => 1,
        ModelInvocationStatus::Succeeded => 2,
        ModelInvocationStatus::Failed => 3,
        ModelInvocationStatus::Cancelled => 4,
    }
}

pub(crate) fn encode_usage(value: musteragent_runtime_model::ModelUsage) -> Vec<u8> {
    let mut output = Vec::with_capacity(57);
    output.extend_from_slice(&value.input_tokens_uncached.to_be_bytes());
    output.extend_from_slice(&value.output_tokens.to_be_bytes());
    output.extend_from_slice(&value.reasoning_tokens.to_be_bytes());
    output.extend_from_slice(&value.provider_cache_read_tokens.to_be_bytes());
    output.extend_from_slice(&value.provider_cache_write_tokens.to_be_bytes());
    output.extend_from_slice(&value.provider_cache_write_1h_tokens.to_be_bytes());
    match value.total_tokens {
        Some(total) => {
            output.push(1);
            output.extend_from_slice(&total.to_be_bytes());
        }
        None => output.push(0),
    }
    output
}

pub(crate) fn decode_usage(
    value: &[u8],
) -> Result<musteragent_runtime_model::ModelUsage, PostgresModelInvocationError> {
    if value.len() != 49 && value.len() != 57 {
        return Err(PostgresModelInvocationError::CorruptState);
    }
    let mut offset = 0_usize;
    let input_tokens_uncached = decode_usage_u64(value, &mut offset)?;
    let output_tokens = decode_usage_u64(value, &mut offset)?;
    let reasoning_tokens = decode_usage_u64(value, &mut offset)?;
    let provider_cache_read_tokens = decode_usage_u64(value, &mut offset)?;
    let provider_cache_write_tokens = decode_usage_u64(value, &mut offset)?;
    let provider_cache_write_1h_tokens = decode_usage_u64(value, &mut offset)?;
    let total_tokens = match value.get(offset) {
        Some(0) if value.len() == 49 => None,
        Some(1) if value.len() == 57 => {
            offset = offset.saturating_add(1);
            Some(decode_usage_u64(value, &mut offset)?)
        }
        _ => return Err(PostgresModelInvocationError::CorruptState),
    };
    let usage = musteragent_runtime_model::ModelUsage {
        input_tokens_uncached,
        output_tokens,
        reasoning_tokens,
        provider_cache_read_tokens,
        provider_cache_write_tokens,
        provider_cache_write_1h_tokens,
        total_tokens,
    };
    usage.validate()?;
    Ok(usage)
}

fn decode_usage_u64(value: &[u8], offset: &mut usize) -> Result<u64, PostgresModelInvocationError> {
    let end = offset.saturating_add(8);
    let bytes: [u8; 8] = value
        .get(*offset..end)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(PostgresModelInvocationError::CorruptState)?;
    *offset = end;
    Ok(u64::from_be_bytes(bytes))
}

fn to_i64(value: u64) -> Result<i64, PostgresModelInvocationError> {
    i64::try_from(value).map_err(|_| PostgresModelInvocationError::NumericRange)
}

#[derive(Debug, Error)]
pub enum PostgresModelInvocationError {
    #[error(transparent)]
    Contract(#[from] ModelContractError),
    #[error("model invocation begin requires Started status")]
    BeginRequiresStarted,
    #[error("model invocation idempotency key conflicts with another request")]
    IdempotencyConflict,
    #[error("model invocation terminal state conflicts with an existing outcome")]
    TerminalConflict,
    #[error("persisted model invocation is corrupt")]
    CorruptState,
    #[error("model usage exceeds PostgreSQL BIGINT accounting range")]
    NumericRange,
    #[error(transparent)]
    Protection(#[from] OpaqueRefProtectionError),
    #[error(transparent)]
    Digest(#[from] musteragent_content_ref::DigestLengthError),
    #[error(transparent)]
    Audit(#[from] crate::PostgresSecurityAuditError),
    #[error("PostgreSQL model persistence failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Model Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}

fn immutable_identity_matches(
    row: &sqlx::postgres::PgRow,
    invocation: &ModelInvocation,
) -> Result<bool, PostgresModelInvocationError> {
    Ok(
        row.try_get::<uuid::Uuid, _>("invocation_id")? == invocation.invocation_id.into_uuid()
            && row.try_get::<String, _>("idempotency_key")? == invocation.idempotency_key.as_str()
            && row.try_get::<uuid::Uuid, _>("request_content_id")?
                == invocation.request_ref.content_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("tenant_id")? == invocation.tenant_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("conversation_id")?
                == invocation.conversation_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("run_id")? == invocation.run_id.into_uuid()
            && row.try_get::<uuid::Uuid, _>("attempt_id")? == invocation.attempt_id.into_uuid()
            && row.try_get::<i16, _>("purpose")? == purpose_code(invocation.purpose)
            && row.try_get::<i16, _>("route_role")? == route_role_code(invocation.route_role)
            && row.try_get::<String, _>("model_route_ref")? == invocation.model_route_ref.as_str()
            && row.try_get::<Vec<u8>, _>("route_identity_hash")?
                == invocation.route_identity_hash.into_bytes()
            && row.try_get::<String, _>("credential_grant_ref")?
                == invocation.credential_grant_ref.as_str()
            && row.try_get::<String, _>("requested_model_key")?
                == invocation.requested_model_key.as_str()
            && row.try_get::<String, _>("pricing_version_ref")?
                == invocation.pricing_version_ref.as_str()
            && row.try_get::<Vec<u8>, _>("request_hash")? == invocation.request_hash.into_bytes()
            && row.try_get::<i64, _>("started_at_ms")? == invocation.started_at.unix_millis(),
    )
}

/// Read frozen derived authority and the original root billing subject.
pub(crate) async fn usage_attribution(
    tx: &mut sqlx::PgConnection,
    run: musteragent_identity::RunId,
) -> Result<musteragent_runtime_model::ModelUsageAttribution, PostgresModelInvocationError> {
    let row=sqlx::query("SELECT r.parent_run_id,COALESCE(a.billing_subject,c.created_by) AS billing_subject FROM runtime.runs r JOIN conversation.conversations c ON c.conversation_id=r.conversation_id LEFT JOIN runtime.derived_run_authorities a ON a.run_id=r.run_id WHERE r.run_id=$1")
        .bind(run.into_uuid()).fetch_one(tx).await?;
    Ok(musteragent_runtime_model::ModelUsageAttribution {
        parent_run_id: row
            .try_get::<Option<uuid::Uuid>, _>("parent_run_id")?
            .map(musteragent_identity::RunId::from_uuid)
            .transpose()
            .map_err(|_| PostgresModelInvocationError::CorruptState)?,
        billing_subject: musteragent_identity::PrincipalId::from_uuid(
            row.try_get("billing_subject")?,
        )
        .map_err(|_| PostgresModelInvocationError::CorruptState)?,
    })
}
