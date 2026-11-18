//! Invocation-owned prewrites for Office computation. Reservations survive retries;
//! the original live Attempt owns every write and the final Artifact transaction.
pub(crate) mod reservations;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContentId, InvocationId, TenantId, UtcTimestamp};
use musteragent_run::AttemptContext;
use musteragent_runtime_content::{
    ContentSink, ContentSource, ContentStore, ContentStoreError as Error, InvocationContentLimits,
    InvocationContentStore, MAX_CONTENT_BYTES, PutContent, VerifiedContentReader,
};
use musteragent_runtime_contract::ApplicationFuture;
use std::{collections::BTreeMap, sync::Arc};

pub struct PostgresInvocationContentStore {
    database: crate::PostgresDatabaseRuntime,
    content: Arc<crate::PostgresContentStore>,
    tenant: TenantId,
    attempt: AttemptContext,
    invocation: InvocationId,
    limits: InvocationContentLimits,
}
impl PostgresInvocationContentStore {
    pub fn new(
        database: crate::PostgresDatabaseRuntime,
        content: Arc<crate::PostgresContentStore>,
        tenant: TenantId,
        attempt: AttemptContext,
        invocation: InvocationId,
        limits: InvocationContentLimits,
    ) -> Self {
        Self {
            database,
            content,
            tenant,
            attempt,
            invocation,
            limits,
        }
    }
    async fn authorized(&self) -> Result<crate::GovernedPostgresTransaction<'static>, Error> {
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, &self.attempt)
            .await
            .map_err(|e| match e {
                crate::commit_lane::CommitLaneError::StaleFence => Error::Unauthorized,
                _ => Error::Unavailable,
            })?;
        if state.tenant_id != self.tenant {
            return Err(Error::Unauthorized);
        }
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations t JOIN runtime.runs r ON r.run_id=t.run_id AND r.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND t.invocation_id=$2 AND t.run_id=$3 AND t.status=3 AND t.execution_attempt_id=$4 AND r.cancel_requested_at_ms IS NULL AND (t.deadline_ms IS NULL OR t.deadline_ms > floor(extract(epoch FROM clock_timestamp())*1000)::bigint))")
            .bind(self.tenant.into_uuid()).bind(self.invocation.into_uuid()).bind(self.attempt.run_id.into_uuid()).bind(self.attempt.attempt_id.into_uuid()).fetch_one(&mut *tx).await.map_err(database)?;
        if !valid {
            return Err(Error::Unauthorized);
        }
        crate::execution_liveness::validate_tool_content_access(
            &mut tx,
            &state,
            &self.attempt,
            self.invocation,
        )
        .await
        .map_err(|_| Error::Unauthorized)?;
        Ok(tx)
    }
    pub async fn requested_at(&self) -> Result<UtcTimestamp, Error> {
        let mut tx = self.authorized().await?;
        // Running invocations own a persisted start time. Use that stable
        // timestamp for preparation and retries, never a fresh wall clock.
        let at: i64 = sqlx::query_scalar("SELECT started_at_ms FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(self.tenant.into_uuid()).bind(self.invocation.into_uuid()).fetch_one(&mut *tx).await.map_err(database)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(UtcTimestamp::from_unix_millis(at))
    }
    async fn reserved(
        &self,
        tx: &mut sqlx::PgConnection,
        reference: &ContentRef,
    ) -> Result<(), Error> {
        reservations::reserved(tx, self.invocation, reference).await
    }
}
impl InvocationContentStore for PostgresInvocationContentStore {
    fn reserve_outputs<'a>(
        &'a self,
        tenant: TenantId,
        invocation: InvocationId,
        outputs: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            if tenant != self.tenant || invocation != self.invocation {
                return Err(Error::Unauthorized);
            }
            let mut tx = self.authorized().await?;
            reservations::reserve(
                &mut tx,
                tenant,
                self.attempt.run_id,
                invocation,
                self.limits,
                outputs,
            )
            .await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(())
        })
    }
}
impl ContentStore for PostgresInvocationContentStore {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, Error>> {
        Box::pin(async { Err(Error::Unauthorized) })
    }
    fn put_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, Error>> {
        Box::pin(async move {
            request.validate()?;
            let reference = ContentRef {
                content_id: id,
                media_type: request.media_type,
                byte_length: request.bytes.len() as u64,
                sha256: Sha256::digest(&request.bytes),
            };
            self.put_stream_at(request.tenant_id, &reference, &mut request.bytes.as_slice())
                .await
        })
    }
    fn put_stream_at<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<ContentRef, Error>> {
        Box::pin(async move {
            if tenant != self.tenant || reference.byte_length > MAX_CONTENT_BYTES {
                return Err(Error::Unauthorized);
            }
            let mut tx = self.authorized().await?;
            self.reserved(&mut tx, reference).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            let stored = self
                .content
                .put_content_stream(tenant, reference, source, Some(self.invocation))
                .await?;
            if &stored != reference {
                return Err(Error::IdentityConflict);
            }
            let mut tx = self.authorized().await?;
            reservations::mark_ready(&mut tx, self.invocation, reference).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(stored)
        })
    }
    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(Error::Unauthorized);
            }
            self.content.read(tenant, reference).await
        })
    }
    fn open_verified<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<VerifiedContentReader, Error>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(Error::Unauthorized);
            }
            self.content.open_verified(tenant, reference).await
        })
    }
    fn copy_to<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(Error::Unauthorized);
            }
            self.content.copy_to(tenant, reference, sink).await
        })
    }
}
fn database(error: sqlx::Error) -> Error {
    use crate::database_failure::{PostgresFailureClass, classify};
    let unavailable = classify(&error) == PostgresFailureClass::Unavailable;
    let sqlstate = error.as_database_error().and_then(|e| e.code());
    tracing::warn!(
        content.error = if unavailable {
            "database_unavailable"
        } else {
            "invariant"
        },
        database.sqlstate = sqlstate.as_deref().unwrap_or(""),
        "Invocation content query failed"
    );
    if error
        .as_database_error()
        .is_some_and(|e| e.is_unique_violation())
    {
        Error::IdentityConflict
    } else if unavailable {
        Error::Unavailable
    } else {
        Error::Invariant
    }
}
