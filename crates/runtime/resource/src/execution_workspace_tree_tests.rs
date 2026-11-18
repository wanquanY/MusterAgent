use super::*;
use crate::ProjectFileVersion;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_resource::{ResourceName, ResourceNode, ResourceNodeLifecycle, ResourceProvenance};

fn request() -> ReadExecutionWorkspaceTree {
    let tenant_id = TenantId::generate();
    ReadExecutionWorkspaceTree {
        context: ResourceCommandContext {
            tenant_id,
            actor: Actor {
                tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            source: ResourceCommandSource::ExecutionWorkspace(ExecutionWorkspaceBinding {
                execution_workspace_id: ExecutionWorkspaceId::generate(),
                conversation_id: ConversationId::generate(),
                branch_id: None,
                project_id: None,
                home_epoch: HomeEpoch::new(1).unwrap(),
            }),
            invocation_id: None,
            tool_key: None,
            now: UtcTimestamp::from_unix_millis(1),
        },
        maximum_entries: 100,
        maximum_total_bytes: 1_024,
    }
}

fn folder(
    request: &ReadExecutionWorkspaceTree,
    name: &str,
    parent: Option<ResourceNodeId>,
) -> ProjectFileCatalogEntry {
    ProjectFileCatalogEntry {
        production: None,
        node: ResourceNode {
            is_result: false,
            node_id: ResourceNodeId::generate(),
            tenant_id: request.context.tenant_id,
            scope: request.context.scope(),
            parent_node_id: parent,
            original_parent_node_id: None,
            name: ResourceName::new(name).unwrap(),
            kind: ResourceNodeKind::Folder,
            lifecycle: ResourceNodeLifecycle::Active,
            file_artifact_id: None,
            sort_key: name.into(),
            created_at: request.context.now,
            updated_at: request.context.now,
            revision: ResourceRevision::new(1),
            provenance: ResourceProvenance {
                actor: request.context.actor,
                conversation_id: request.context.conversation_id(),
                invocation_id: None,
                tool_key: None,
            },
        },
        file: None,
        file_updated_at: None,
    }
}

fn file(request: &ReadExecutionWorkspaceTree, name: &str, length: u64) -> ProjectFileCatalogEntry {
    let mut entry = folder(request, name, None);
    let artifact_id = ArtifactId::generate();
    entry.node.kind = ResourceNodeKind::File;
    entry.node.file_artifact_id = Some(artifact_id);
    entry.file = Some(ProjectFileVersion {
        node_id: entry.node.node_id,
        artifact_id,
        artifact_version_id: ArtifactVersionId::generate(),
        name: name.into(),
        version: VersionNumber::new(1).unwrap(),
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/octet-stream").unwrap(),
            byte_length: length,
            sha256: Sha256::digest("fixture"),
        },
    });
    entry
}

fn tree(
    request: &ReadExecutionWorkspaceTree,
    entries: Vec<ProjectFileCatalogEntry>,
) -> Result<ExecutionWorkspaceTree, ResourceStoreError> {
    ExecutionWorkspaceTree::from_catalog(request, ResourceRevision::new(1), entries)
}

#[test]
fn rejects_missing_cyclic_file_parents_and_ambiguous_names() {
    let req = request();
    let mut a = folder(&req, "目录", None);
    let b = folder(&req, "子目录", Some(a.node.node_id));
    assert_eq!(
        tree(&req, vec![b.clone()]).unwrap_err(),
        ResourceStoreError::Corrupt
    );
    a.node.parent_node_id = Some(b.node.node_id);
    assert_eq!(
        tree(&req, vec![a, b]).unwrap_err(),
        ResourceStoreError::Corrupt
    );
    let a = file(&req, "data.bin", 1);
    let b = folder(&req, "nested", Some(a.node.node_id));
    assert_eq!(
        tree(&req, vec![a, b]).unwrap_err(),
        ResourceStoreError::Corrupt
    );
    for names in [("data", "data"), ("Data", "data")] {
        assert_eq!(
            tree(
                &req,
                vec![folder(&req, names.0, None), folder(&req, names.1, None)]
            )
            .unwrap_err(),
            ResourceStoreError::Corrupt
        );
    }
}

#[test]
fn bounds_are_all_or_nothing_and_preserve_empty_directories() {
    let mut req = request();
    let root = folder(&req, "分析", None);
    let child = folder(&req, "空目录", Some(root.node.node_id));
    let entries = vec![child, root, file(&req, "data.bin", 1_024)];
    let view = tree(&req, entries.clone()).unwrap();
    assert_eq!(view.total_bytes, 1_024);
    assert_eq!(
        view.entries
            .iter()
            .map(|e| e.path.as_str())
            .collect::<Vec<_>>(),
        ["data.bin", "分析", "分析/空目录"]
    );
    req.maximum_entries = 2;
    assert_eq!(
        tree(&req, entries.clone()).unwrap_err(),
        ResourceStoreError::BoundsExceeded
    );
    req.maximum_entries = 100;
    req.maximum_total_bytes = 1_023;
    assert_eq!(
        tree(&req, entries).unwrap_err(),
        ResourceStoreError::BoundsExceeded
    );
    req.maximum_total_bytes = u64::MAX;
    assert_eq!(
        tree(&req, vec![file(&req, "a", u64::MAX), file(&req, "b", 1)]).unwrap_err(),
        ResourceStoreError::BoundsExceeded
    );
}

#[test]
fn rejects_inconsistent_file_identity_and_unrepresentable_paths() {
    let req = request();
    let mut f = file(&req, "data.bin", 1);
    f.file.as_mut().unwrap().name = "other.bin".into();
    assert_eq!(
        tree(&req, vec![f]).unwrap_err(),
        ResourceStoreError::FileUnavailable
    );
    let mut entries = Vec::new();
    let mut parent = None;
    for _ in 0..17 {
        let item = folder(&req, &"a".repeat(255), parent);
        parent = Some(item.node.node_id);
        entries.push(item);
    }
    assert_eq!(
        tree(&req, entries).unwrap_err(),
        ResourceStoreError::BoundsExceeded
    );
}
