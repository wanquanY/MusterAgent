use musteragent_identity::UtcTimestamp;
use musteragent_runtime_audit::{SecurityAuditTraceId, principal_kind_code};
use musteragent_runtime_contract::{ErrorCode, OpaqueDiagnosticRef, RuntimeError};
use musteragent_runtime_session::{
    AdmissionDenialCommit, AdmissionDenialQuery, AdmissionDenialReceipt, runtime_error,
};
use sqlx::Row as _;

use super::{PostgresRuntimeApplicationStore, database_dependency};
use crate::database_failure::{PostgresFailureClass, classify as classify_database_failure};
use crate::{PostgresSecurityAuditError, PostgresTransactionError};

impl PostgresRuntimeApplicationStore {
    pub(super) async fn load_admission_denial_inner(
        &self,
        query: AdmissionDenialQuery,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> Result<Option<AdmissionDenialReceipt>, RuntimeError> {
        let row = sqlx::query(
            "SELECT conversation_id, command_fingerprint, error_code,
                    retryable, diagnostic_ref, occurred_at_ms
             FROM security_audit.admission_denials
             WHERE tenant_id = $1 AND command_id = $2",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(query.command_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| database_dependency(error, "admission.load_denial", correlation_id))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let conversation_id: uuid::Uuid = row
            .try_get("conversation_id")
            .map_err(|_| dependency(correlation_id))?;
        let fingerprint: Vec<u8> = row
            .try_get("command_fingerprint")
            .map_err(|_| dependency(correlation_id))?;
        if conversation_id != query.conversation_id.into_uuid()
            || fingerprint.as_slice() != query.command_fingerprint.into_bytes().as_slice()
        {
            return Err(conflict(correlation_id));
        }
        decode_receipt(&row, correlation_id).map(Some)
    }

    pub(super) async fn record_admission_denial_inner(
        &self,
        commit: AdmissionDenialCommit,
    ) -> Result<AdmissionDenialReceipt, RuntimeError> {
        let trace_id =
            SecurityAuditTraceId::from_trace_context(&commit.trace_context).map_err(|error| {
                audit_contract_dependency(&error, "admission_denial_trace", commit.correlation_id)
            })?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(|error| {
                audit_transaction_dependency(
                    error,
                    "admission_denial_transaction",
                    commit.correlation_id,
                )
            })?;
        let lock_identity = format!(
            "security-audit-admission-denial:{}:{}",
            commit.actor.tenant_id, commit.command_id
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_identity)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                audit_database_dependency(error, "admission_denial_lock", commit.correlation_id)
            })?;
        sqlx::query(
            "INSERT INTO security_audit.admission_denials (
                 tenant_id, command_id, conversation_id,
                 actor_principal_id, actor_principal_kind,
                 correlation_id, trace_id, command_fingerprint,
                 error_code, retryable, diagnostic_ref, occurred_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (tenant_id, command_id) DO NOTHING",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.command_id.into_uuid())
        .bind(commit.conversation_id.into_uuid())
        .bind(commit.actor.principal_id.into_uuid())
        .bind(principal_kind_code(commit.actor.principal_kind))
        .bind(commit.correlation_id.into_uuid())
        .bind(trace_id.as_bytes().as_slice())
        .bind(commit.command_fingerprint.into_bytes().as_slice())
        .bind(commit.error_code.as_str())
        .bind(commit.retryable)
        .bind(
            commit
                .diagnostic_ref
                .as_ref()
                .map(OpaqueDiagnosticRef::as_str),
        )
        .bind(commit.occurred_at.unix_millis())
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            audit_database_dependency(error, "admission_denial_insert", commit.correlation_id)
        })?;
        let row = sqlx::query(
            "SELECT conversation_id, actor_principal_id, actor_principal_kind,
                    correlation_id, trace_id, command_fingerprint,
                    error_code, retryable, diagnostic_ref, occurred_at_ms
             FROM security_audit.admission_denials
             WHERE tenant_id = $1 AND command_id = $2",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.command_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| {
            audit_database_dependency(error, "admission_denial_verify", commit.correlation_id)
        })?;
        let identity_matches = row
            .try_get::<uuid::Uuid, _>("conversation_id")
            .is_ok_and(|value| value == commit.conversation_id.into_uuid())
            && row
                .try_get::<uuid::Uuid, _>("actor_principal_id")
                .is_ok_and(|value| value == commit.actor.principal_id.into_uuid())
            && row
                .try_get::<i16, _>("actor_principal_kind")
                .is_ok_and(|value| value == principal_kind_code(commit.actor.principal_kind))
            && row
                .try_get::<uuid::Uuid, _>("correlation_id")
                .is_ok_and(|value| value == commit.correlation_id.into_uuid())
            && row
                .try_get::<Vec<u8>, _>("trace_id")
                .is_ok_and(|value| value.as_slice() == trace_id.as_bytes())
            && row
                .try_get::<Vec<u8>, _>("command_fingerprint")
                .is_ok_and(|value| {
                    value.as_slice() == commit.command_fingerprint.into_bytes().as_slice()
                })
            && row
                .try_get::<String, _>("error_code")
                .is_ok_and(|value| value == commit.error_code.as_str())
            && row
                .try_get::<bool, _>("retryable")
                .is_ok_and(|value| value == commit.retryable)
            && row
                .try_get::<Option<String>, _>("diagnostic_ref")
                .is_ok_and(|value| {
                    value.as_deref()
                        == commit
                            .diagnostic_ref
                            .as_ref()
                            .map(OpaqueDiagnosticRef::as_str)
                });
        // `occurred_at_ms` is the first committed observation of the denial,
        // not part of the command's idempotency identity. Concurrent replays
        // can observe the same deterministic denial on adjacent clock ticks;
        // they must return the canonical first receipt instead of conflicting.
        if !identity_matches {
            return Err(conflict(commit.correlation_id));
        }
        let receipt = decode_receipt(&row, commit.correlation_id)?;
        self.audit
            .append_admission_denied_in_transaction(
                &mut transaction,
                commit.actor.tenant_id,
                commit.command_id,
            )
            .await
            .map_err(|error| {
                audit_pipeline_dependency(
                    error,
                    "admission_denial_append_audit",
                    commit.correlation_id,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            audit_transaction_dependency(error, "admission_denial_commit", commit.correlation_id)
        })?;
        Ok(receipt)
    }
}

fn decode_receipt(
    row: &sqlx::postgres::PgRow,
    correlation_id: musteragent_identity::CorrelationId,
) -> Result<AdmissionDenialReceipt, RuntimeError> {
    let code: String = row
        .try_get("error_code")
        .map_err(|_| dependency(correlation_id))?;
    Ok(AdmissionDenialReceipt {
        error_code: ErrorCode::from_safe_code(&code).ok_or_else(|| dependency(correlation_id))?,
        retryable: row
            .try_get("retryable")
            .map_err(|_| dependency(correlation_id))?,
        diagnostic_ref: row
            .try_get::<Option<String>, _>("diagnostic_ref")
            .map_err(|_| dependency(correlation_id))?
            .map(OpaqueDiagnosticRef::new)
            .transpose()
            .map_err(|_| dependency(correlation_id))?,
        occurred_at: UtcTimestamp::from_unix_millis(
            row.try_get("occurred_at_ms")
                .map_err(|_| dependency(correlation_id))?,
        ),
    })
}

fn conflict(correlation_id: musteragent_identity::CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::IdempotencyConflict,
        correlation_id,
        false,
        "Admission denial identity conflicts with canonical command truth",
    )
}

fn dependency(correlation_id: musteragent_identity::CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::DependencyUnavailable,
        correlation_id,
        true,
        "Security Audit admission authority is unavailable",
    )
}

fn audit_database_dependency(
    error: sqlx::Error,
    stage: &'static str,
    correlation_id: musteragent_identity::CorrelationId,
) -> RuntimeError {
    let material_error = match classify_database_failure(&error) {
        PostgresFailureClass::Unavailable => "database_unavailable",
        PostgresFailureClass::Invalid => "database_invalid",
    };
    if let Some(sqlstate) = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
    {
        tracing::error!(
            failure.stage = stage,
            correlation_id = %correlation_id,
            material.error = material_error,
            database.sqlstate = sqlstate.as_ref(),
            error = ?error,
            "Security Audit admission denial database operation failed"
        );
    } else {
        tracing::error!(
            failure.stage = stage,
            correlation_id = %correlation_id,
            material.error = material_error,
            error = ?error,
            "Security Audit admission denial database operation failed"
        );
    }
    dependency(correlation_id)
}

fn audit_pipeline_dependency(
    error: PostgresSecurityAuditError,
    stage: &'static str,
    correlation_id: musteragent_identity::CorrelationId,
) -> RuntimeError {
    match error {
        PostgresSecurityAuditError::Database(error) => {
            audit_database_dependency(error, stage, correlation_id)
        }
        PostgresSecurityAuditError::Transaction(error) => {
            audit_transaction_dependency(error, stage, correlation_id)
        }
        error => {
            tracing::error!(
                failure.stage = stage,
                correlation_id = %correlation_id,
                material.error = "corrupt_state",
                error = ?error,
                "Security Audit admission denial pipeline failed"
            );
            dependency(correlation_id)
        }
    }
}

fn audit_transaction_dependency(
    error: PostgresTransactionError,
    stage: &'static str,
    correlation_id: musteragent_identity::CorrelationId,
) -> RuntimeError {
    let (material_error, sqlstate) = match &error {
        PostgresTransactionError::Policy(_) => ("corrupt_state", None),
        PostgresTransactionError::Database(source) => (
            match classify_database_failure(source) {
                PostgresFailureClass::Unavailable => "database_unavailable",
                PostgresFailureClass::Invalid => "database_invalid",
            },
            source
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code),
        ),
        PostgresTransactionError::DeadlineExceeded { .. } => ("database_unavailable", None),
        PostgresTransactionError::CommitOutcomeUnknown { source } => (
            "database_unavailable",
            source
                .as_ref()
                .and_then(sqlx::Error::as_database_error)
                .and_then(sqlx::error::DatabaseError::code),
        ),
    };
    if let Some(sqlstate) = sqlstate {
        tracing::error!(
            failure.stage = stage,
            correlation_id = %correlation_id,
            material.error = material_error,
            database.sqlstate = sqlstate.as_ref(),
            error = ?error,
            "Security Audit admission denial transaction failed"
        );
    } else {
        tracing::error!(
            failure.stage = stage,
            correlation_id = %correlation_id,
            material.error = material_error,
            error = ?error,
            "Security Audit admission denial transaction failed"
        );
    }
    dependency(correlation_id)
}

fn audit_contract_dependency(
    error: &impl std::fmt::Debug,
    stage: &'static str,
    correlation_id: musteragent_identity::CorrelationId,
) -> RuntimeError {
    tracing::error!(
        failure.stage = stage,
        correlation_id = %correlation_id,
        material.error = "corrupt_state",
        error = ?error,
        "Security Audit admission denial contract failed"
    );
    dependency(correlation_id)
}
