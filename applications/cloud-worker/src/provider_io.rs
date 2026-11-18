use crate::WorkerTraceContextSource;
use musteragent_identity::{TenantId, ToolCallId};
use musteragent_loop::LoopPortError;
use musteragent_model_providers::{ContentResolver, ContentResolverError, ResolvedContent};
use musteragent_run::AttemptContext;
use musteragent_runtime_content::{ContentStore, ContentStoreError};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, TraceContext, TraceParent, TraceState,
};
use secrecy::SecretSlice;
use sqlx::{PgPool, Row as _};
use std::{future::Future, pin::Pin, sync::Arc};

/// Tenant-authorized adapter from Runtime ContentStore to Provider content.
pub struct RuntimeModelContentResolver {
    content: Arc<dyn ContentStore>,
}

impl RuntimeModelContentResolver {
    #[must_use]
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self { content }
    }
}

impl ContentResolver for RuntimeModelContentResolver {
    fn resolve<'a>(
        &'a self,
        tenant_id: TenantId,
        content_ref: &'a musteragent_content_ref::ContentRef,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedContent, ContentResolverError>> + Send + 'a>>
    {
        Box::pin(async move {
            let bytes =
                self.content
                    .read(tenant_id, content_ref)
                    .await
                    .map_err(|error| match error {
                        ContentStoreError::NotFound => ContentResolverError::NotFound,
                        ContentStoreError::Unauthorized
                        | ContentStoreError::LengthMismatch
                        | ContentStoreError::HashMismatch
                        | ContentStoreError::IdentityConflict
                        | ContentStoreError::Invariant => ContentResolverError::Unauthorized,
                        ContentStoreError::TooLarge => ContentResolverError::TooLarge,
                        ContentStoreError::Unavailable => ContentResolverError::Unavailable,
                    })?;
            Ok(ResolvedContent::Inline {
                media_type: content_ref.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            })
        })
    }
}

/// Loads the original ingress trace under the active Attempt fence.
#[derive(Clone)]
pub struct PostgresWorkerTraceContextSource {
    pool: PgPool,
}

impl PostgresWorkerTraceContextSource {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl WorkerTraceContextSource for PostgresWorkerTraceContextSource {
    fn trace_context<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        _call_id: &'a ToolCallId,
    ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT run.traceparent, run.tracestate
                 FROM runtime.runs AS run
                 JOIN runtime.attempts AS attempt
                   ON attempt.tenant_id = run.tenant_id
                  AND attempt.run_id = run.run_id
                 JOIN runtime.leases AS lease
                   ON lease.tenant_id = attempt.tenant_id
                  AND lease.run_id = attempt.run_id
                  AND lease.attempt_id = attempt.attempt_id
                 WHERE run.run_id = $1
                   AND attempt.attempt_id = $2
                   AND attempt.attempt_number = $3
                   AND attempt.worker_id = $4
                   AND attempt.lease_id = $5
                   AND attempt.fencing_token = $6::numeric
                   AND attempt.status = 1
                   AND lease.expires_at_ms >
                       floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            )
            .bind(attempt.run_id.into_uuid())
            .bind(attempt.attempt_id.into_uuid())
            .bind(i32::try_from(attempt.attempt_number.get()).map_err(|_| trace_invariant())?)
            .bind(attempt.worker_id.as_str())
            .bind(attempt.lease_id.into_uuid())
            .bind(attempt.fencing_token.get().to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| trace_unavailable())?
            .ok_or_else(trace_stale)?;
            let traceparent = TraceParent::new(
                row.try_get::<String, _>("traceparent")
                    .map_err(|_| trace_invariant())?,
            )
            .map_err(|_| trace_invariant())?;
            let tracestate = row
                .try_get::<Option<String>, _>("tracestate")
                .map_err(|_| trace_invariant())?
                .map(TraceState::new)
                .transpose()
                .map_err(|_| trace_invariant())?;
            Ok(TraceContext {
                traceparent,
                tracestate,
            })
        })
    }
}

fn trace_stale() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::AttemptLeaseStale,
        retryable: false,
    }
}

fn trace_invariant() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

fn trace_unavailable() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::DependencyUnavailable,
        retryable: true,
    }
}
