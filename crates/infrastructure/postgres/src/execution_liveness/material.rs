//! Resolve immutable Tool material before acquiring the mutation transaction.
//! A prepared value is scoped to one call and exact Snapshot hash; it is not
//! current installation authority and never permits replay or resets a clock.
use super::*;
use musteragent_identity::InvocationId;
use musteragent_runtime_snapshot::{
    ToolDefinitionSnapshot,
    application_catalog::{ApplicationCatalogMaterialError, resolve_tool_catalog},
};
use musteragent_tool::ToolKey;

#[derive(Clone)]
pub(super) struct PreparedTool {
    pub hash: Sha256,
    pub tool: ToolDefinitionSnapshot,
}
impl PostgresFencedExecutionLivenessJournal {
    pub(crate) async fn prepare_tool_material(
        &self,
        owner: &AttemptContext,
        key: Option<&ToolKey>,
        invocation: Option<InvocationId>,
    ) -> Result<Self, LivenessJournalError> {
        self.prepare_material_inner(owner, key, invocation)
            .await
            .map_err(StoreError::port)
    }
    pub(super) async fn prepare_material_inner(
        &self,
        owner: &AttemptContext,
        key: Option<&ToolKey>,
        invocation: Option<InvocationId>,
    ) -> Result<Self, StoreError> {
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, owner).await?;
        let key = match key {
            Some(key) => Some(key.clone()),
            None => match invocation {
                Some(id) => {
                    let value: Option<String> = sqlx::query_scalar("SELECT tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3")
                .bind(state.tenant_id.into_uuid()).bind(owner.run_id.into_uuid()).bind(id.into_uuid()).fetch_optional(&mut *tx).await?;
                    value
                        .map(|value| {
                            musteragent_identity::VersionedKey::new(value)
                                .map(ToolKey::from_versioned_key)
                        })
                        .transpose()
                        .map_err(|_| StoreError::Corrupt)?
                }
                None => None,
            },
        };
        let Some(key) = key else {
            tx.rollback().await?;
            return Ok(self.clone());
        };
        // Preserve the original maintenance/evaluation role boundary before
        // looking in public application sources. A private Tool is not a
        // missing application and may never be enabled by catalog fallback.
        if admission::private_tool(self, &mut tx, &state, owner, key.as_str())
            .await?
            .is_some()
        {
            tx.rollback().await?;
            return Ok(self.clone());
        }
        let (_, bytes, hash) = admission::snapshot_bytes(&mut tx, &state, owner).await?;
        tx.rollback().await?;
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&bytes)
            .map_err(|_| StoreError::Corrupt)?;
        if snapshot.tools.iter().any(|tool| tool.manifest.key == key) {
            return Ok(self.clone());
        }
        snapshot
            .validate_liveness_schema()
            .map_err(|_| StoreError::Invalid)?;
        if snapshot.application_catalogs.is_empty() {
            return Err(StoreError::Invalid);
        }
        let catalog = resolve_tool_catalog(&snapshot, self.application_catalogs.as_deref())
            .await
            .map_err(material_error)?;
        snapshot
            .validate_resolved_tool_catalog(&catalog.catalog)
            .map_err(|_| StoreError::Corrupt)?;
        if catalog.catalog.get_by_key(key.as_str()).is_none() {
            return Err(StoreError::Invalid);
        }
        let tool = catalog
            .executable(&snapshot, self.application_catalogs.as_deref(), &key)
            .await
            .map_err(material_error)?;
        let mut prepared = self.clone();
        prepared.prepared_tool = Some(Arc::new(PreparedTool { hash, tool }));
        Ok(prepared)
    }
    pub(super) fn prepared_tool(
        &self,
        hash: Sha256,
        key: &str,
    ) -> Result<&ToolDefinitionSnapshot, StoreError> {
        let prepared = self
            .prepared_tool
            .as_ref()
            .ok_or(StoreError::MaterialUnavailable)?;
        if prepared.hash != hash || prepared.tool.manifest.key.as_str() != key {
            return Err(StoreError::Invalid);
        }
        Ok(&prepared.tool)
    }
}
fn material_error(error: ApplicationCatalogMaterialError) -> StoreError {
    match error {
        ApplicationCatalogMaterialError::Unavailable => StoreError::MaterialUnavailable,
        ApplicationCatalogMaterialError::Corrupt => StoreError::Corrupt,
    }
}
