//! Source grants belong to the Artifact Owner. Public policy and its CAS remain
//! in the original Backend Share Owner; this port never creates a public URL.
mod codec;
mod lifecycle;
mod prepare;
mod read;

use super::*;
use musteragent_identity::{AgentId, ContentId};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    AgentArtifactContent, ArtifactPublicationAudience, ArtifactPublicationContentAuthorization,
    ArtifactPublicationSourceDescriptor, ArtifactPublicationSourceError as Error,
    ArtifactPublicationSourcePort, ArtifactPublicationSourceRef,
    ArtifactPublicationSourceState as State, ArtifactPublicationSourceView as View,
    MAX_PUBLICATION_DEPENDENCIES, PUBLICATION_PREPARE_TTL_MILLIS,
    PrepareArtifactPublicationSource as Prepare,
};
use sqlx::PgConnection;
use uuid::Uuid;

fn sql(error: sqlx::Error) -> Error {
    tracing::warn!(
        sql_state = error
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref()
            .unwrap_or("none"),
        "Artifact publication source database failure"
    );
    Error::Unavailable
}
fn authority(error: PostgresArtifactError) -> Error {
    match error {
        PostgresArtifactError::NotFound => Error::Forbidden,
        PostgresArtifactError::Sql(e) => sql(e),
        PostgresArtifactError::Transaction(_) => Error::Unavailable,
        _ => Error::Invariant,
    }
}
async fn now(db: &mut PgConnection) -> Result<i64, Error> {
    sqlx::query_scalar("SELECT artifact.dialogue_publication_now_ms()")
        .fetch_one(db)
        .await
        .map_err(sql)
}
impl ArtifactPublicationSourcePort for PostgresArtifactStore {
    fn prepare(&self, request: Prepare) -> ApplicationFuture<'_, Result<View, Error>> {
        Box::pin(prepare::prepare(self, request))
    }
    fn retain(
        &self,
        source: ArtifactPublicationSourceRef,
    ) -> ApplicationFuture<'_, Result<View, Error>> {
        Box::pin(lifecycle::transition(self, source, true))
    }
    fn release(
        &self,
        source: ArtifactPublicationSourceRef,
    ) -> ApplicationFuture<'_, Result<View, Error>> {
        Box::pin(lifecycle::transition(self, source, false))
    }
    fn inspect(
        &self,
        source: ArtifactPublicationSourceRef,
    ) -> ApplicationFuture<'_, Result<View, Error>> {
        Box::pin(read::inspect(self, source))
    }
    fn authorize_read(
        &self,
        source: ArtifactPublicationSourceRef,
        content_index: u32,
    ) -> ApplicationFuture<'_, Result<ArtifactPublicationContentAuthorization, Error>> {
        Box::pin(async move {
            let mut tx = self
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::ReadCommitted,
                )
                .await
                .map_err(|_| Error::Unavailable)?;
            let row = read::load(&mut tx, source, false).await?;
            let view = read::view(&mut tx, &row, true).await?;
            if view.state != State::Retained {
                return Err(match view.state {
                    State::Expired => Error::Expired,
                    _ => Error::Revoked,
                });
            }
            let descriptor = view.descriptor.ok_or(Error::Invariant)?;
            let reference = if content_index == 0 {
                descriptor.root
            } else {
                descriptor
                    .dependencies
                    .get((content_index - 1) as usize)
                    .cloned()
                    .ok_or(Error::Forbidden)?
            };
            let tenant_id = TenantId::from_uuid(row.try_get("tenant_id").map_err(sql)?)
                .map_err(|_| Error::Invariant)?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(ArtifactPublicationContentAuthorization {
                tenant_id,
                reference,
            })
        })
    }
    fn reclaim_expired_prepares(&self, limit: u32) -> ApplicationFuture<'_, Result<u32, Error>> {
        Box::pin(lifecycle::reclaim(self, limit))
    }
}
