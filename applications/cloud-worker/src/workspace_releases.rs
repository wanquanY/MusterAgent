//! Startup publication for the two reference binaries. Execution uses sealed Run material.
use crate::BuildError;
use musteragent_content_ref::MediaType;
use musteragent_identity::ContentId;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_tool::workspace::{WorkspaceContractError, WorkspaceReleaseCatalog};
use std::{fs::File, io::Read, path::Path};

pub fn load_workspace_releases(path: &Path) -> Result<WorkspaceReleaseCatalog, BuildError> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(WorkspaceReleaseCatalog::MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    Ok(WorkspaceReleaseCatalog::from_json(&bytes)?)
}

/// Both hosts reconcile the same immutable identity before accepting work. Reusing a
/// declared release identity with different bytes is a content-store conflict.
pub async fn publish_workspace_releases(
    catalog: &WorkspaceReleaseCatalog,
    store: &dyn ContentStore,
) -> Result<(), BuildError> {
    let tenant_id = musteragent_runtime_skill::official_catalog_scope_tenant_id()?;
    for publication in catalog.publications() {
        let release = publication.release();
        let identity = serde_json::to_vec(&(
            "musteragent.workspace-tool-release.v1",
            &release.plugin_id,
            &release.version,
            &release.release_sha256,
        ))?;
        let id = ContentId::from_uuid(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &identity))?;
        let media_type = MediaType::new("application/json")?;
        let retained = store
            .put_runtime_release_at(
                id,
                PutContent {
                    tenant_id,
                    media_type: media_type.clone(),
                    bytes: publication.bytes().to_vec(),
                },
            )
            .await?;
        if retained.content_id != id
            || retained.media_type != media_type
            || retained.sha256 != publication.sha256()
            || retained.byte_length != publication.bytes().len() as u64
        {
            return Err(WorkspaceContractError.into());
        }
    }
    Ok(())
}
