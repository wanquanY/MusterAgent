#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
enum DuringRead {
    Revoke,
    ChangeHead,
    Rename,
    None,
}
struct State {
    allowed: bool,
    first: ProjectFileVersion,
    current: ProjectFileVersion,
}
#[derive(Clone)]
struct Versions(Arc<Mutex<State>>);
impl ProjectFileVersionStore for Versions {
    fn resolve_version<'a>(
        &'a self,
        _: &'a ResourceCommandContext,
        node: ResourceNodeId,
        version: Option<VersionNumber>,
    ) -> ApplicationFuture<'a, Result<ProjectFileVersion, ResourceStoreError>> {
        Box::pin(async move {
            let state = self.0.lock().unwrap();
            if !state.allowed || node != state.current.node_id {
                return Err(ResourceStoreError::NodeUnavailable);
            }
            if version == Some(state.first.version) {
                return Ok(state.first.clone());
            }
            if version.is_some_and(|value| value != state.current.version) {
                return Err(ResourceStoreError::FileUnavailable);
            }
            Ok(state.current.clone())
        })
    }
}
struct Bytes {
    state: Versions,
    change: DuringRead,
}
impl ContentStore for Bytes {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        panic!("read only")
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            let mut state = self.state.0.lock().unwrap();
            match self.change {
                DuringRead::Revoke => state.allowed = false,
                DuringRead::ChangeHead => {
                    state.current.version = VersionNumber::new(2).unwrap();
                    state.current.artifact_version_id = ArtifactVersionId::generate();
                    state.current.content_ref = content(b"new");
                }
                DuringRead::Rename => state.current.name = "renamed.txt".into(),
                DuringRead::None => {}
            }
            Ok(b"old".to_vec())
        })
    }
}
fn content(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
async fn read(
    change: DuringRead,
    pinned: bool,
) -> Result<ProjectFileReadView, ProjectFileReadError> {
    let file = ProjectFileVersion {
        node_id: ResourceNodeId::generate(),
        artifact_id: ArtifactId::generate(),
        artifact_version_id: ArtifactVersionId::generate(),
        name: "original.txt".into(),
        version: VersionNumber::new(1).unwrap(),
        content_ref: content(b"old"),
    };
    let state = Versions(Arc::new(Mutex::new(State {
        allowed: true,
        first: file.clone(),
        current: file.clone(),
    })));
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let request = ReadProjectFile {
        context: ResourceCommandContext {
            tenant_id: actor.tenant_id,
            actor,
            source: ResourceCommandSource::Workspace {
                workspace_id: WorkspaceId::generate(),
            },
            invocation_id: None,
            tool_key: None,
            now: UtcTimestamp::from_unix_millis(1),
        },
        node_id: file.node_id,
        version: pinned.then_some(file.version),
        offset: 0,
        limit: 100,
        include_outline: false,
    };
    ProjectFileReadApplication::new(state.clone(), Arc::new(Bytes { state, change }))
        .read(request)
        .await
}

#[tokio::test]
async fn revoked_resource_authority_during_content_io_never_releases_bytes() {
    assert_eq!(
        read(DuringRead::Revoke, false).await.unwrap_err(),
        ProjectFileReadError::Store(ResourceStoreError::NodeUnavailable)
    );
}
#[tokio::test]
async fn changed_current_head_during_content_io_requires_a_new_observation() {
    assert_eq!(
        read(DuringRead::ChangeHead, false).await.unwrap_err(),
        ProjectFileReadError::Store(ResourceStoreError::RevisionConflict)
    );
}
#[tokio::test]
async fn explicit_historical_read_remains_pinned_when_current_head_advances() {
    let view = read(DuringRead::ChangeHead, true).await.unwrap();
    assert_eq!(view.file.version.get(), 1);
    assert_eq!(view.content.as_deref(), Some("old"));
}
#[tokio::test]
async fn rename_rechecks_authority_but_does_not_replace_content_identity() {
    let view = read(DuringRead::Rename, false).await.unwrap();
    assert_eq!(view.file.name, "renamed.txt");
    assert_eq!(view.content.as_deref(), Some("old"));
    assert_eq!(
        read(DuringRead::None, false)
            .await
            .unwrap()
            .content
            .as_deref(),
        Some("old")
    );
}
