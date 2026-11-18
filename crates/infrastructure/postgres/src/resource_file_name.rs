//! Indexed source-identity resolution. The existing Resource commit owns
//! authorization, scope fencing, collision checks, idempotency and the receipt.
use crate::resource::{PostgresResourceStore, encode_scope};
use musteragent_identity::{ArtifactId, ResourceNodeId, ResourceRevision};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ResourceCommandContext, ResourceFileNameTarget, ResourceFileNameTargets, ResourceStoreError,
};
use sqlx::Row;

impl ResourceFileNameTargets for PostgresResourceStore {
    fn resolve<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<ResourceFileNameTarget, ResourceStoreError>> {
        Box::pin(async move {
            context
                .validate()
                .map_err(|_| ResourceStoreError::NodeUnavailable)?;
            let mut transaction = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            crate::resource_file_catalog::authorize(&mut transaction, context).await?;
            let (scope_kind, scope_id) = encode_scope(context.scope());
            let row = sqlx::query(
                "SELECT node.node_id, head.revision
                 FROM resource.nodes node
                 JOIN resource.scope_heads head USING (tenant_id, scope_kind, scope_id)
                 WHERE node.tenant_id=$1 AND node.scope_kind=$2 AND node.scope_id=$3
                   AND node.file_artifact_id=$4 AND node.kind=2 AND node.lifecycle=1",
            )
            .bind(context.tenant_id.into_uuid())
            .bind(scope_kind)
            .bind(scope_id)
            .bind(artifact_id.into_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?
            .ok_or(ResourceStoreError::NodeUnavailable)?;
            let node_id = ResourceNodeId::from_uuid(
                row.try_get("node_id")
                    .map_err(|_| ResourceStoreError::Corrupt)?,
            )
            .map_err(|_| ResourceStoreError::Corrupt)?;
            let revision: i64 = row
                .try_get("revision")
                .map_err(|_| ResourceStoreError::Corrupt)?;
            transaction
                .commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            Ok(ResourceFileNameTarget {
                node_id,
                revision: ResourceRevision::new(
                    u64::try_from(revision).map_err(|_| ResourceStoreError::Corrupt)?,
                ),
            })
        })
    }
}
