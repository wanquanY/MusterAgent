#![cfg(test)]

#[path = "project/agent_files.rs"]
mod agent_files;
mod support;
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_resource::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use musteragent_runtime_resource::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use support::*;

struct Authority(Mutex<ResourceCommandContext>);
impl ResourceContextAuthority for Authority {
    fn resolve(
        &self,
        q: ResolveResourceContext,
    ) -> ApplicationFuture<'_, Result<ResourceCommandContext, ResourceContextError>> {
        Box::pin(async move {
            let c = self.0.lock().expect("fixture lock").clone();
            assert_eq!(q.actor, c.actor);
            Ok(c)
        })
    }
}
struct Store(
    Mutex<ProjectFileCatalogEntry>,
    Mutex<Sha256>,
    std::sync::atomic::AtomicUsize,
);
impl ProjectFileCatalogStore for Store {
    fn snapshot_files(
        &self,
        q: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        Box::pin(async move {
            let mut page = self.list_files(q).await?;
            page.next_offset = None;
            Ok(page)
        })
    }
    fn resolve_artifact_placement<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<Option<ProjectFileCatalogEntry>, ResourceStoreError>> {
        Box::pin(async move {
            let entry = self.0.lock().unwrap().clone();
            Ok((entry.node.scope == context.scope()
                && entry.node.file_artifact_id == Some(artifact_id))
            .then_some(entry))
        })
    }
    fn list_files(
        &self,
        q: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        Box::pin(async move {
            self.2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let entry = self.0.lock().expect("fixture lock").clone();
            if entry.node.scope != q.context.scope() {
                return Err(ResourceStoreError::NodeUnavailable);
            }
            if q.produced_by.is_some() {
                assert!(
                    q.recursive,
                    "producer discovery includes the bounded subtree"
                );
            }
            Ok(ProjectFileCatalogPage {
                scope: q.context.scope(),
                revision: ResourceRevision::new(1),
                content_revision: *self.1.lock().expect("fixture lock"),
                entries: vec![entry],
                next_offset: Some(q.offset + 1),
            })
        })
    }
    fn resolve_file<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        id: ResourceNodeId,
    ) -> ApplicationFuture<'a, Result<ProjectFileCatalogEntry, ResourceStoreError>> {
        Box::pin(async move {
            let entry = self.0.lock().expect("fixture lock").clone();
            if entry.node.scope != context.scope() || entry.node.node_id != id {
                return Err(ResourceStoreError::NodeUnavailable);
            }
            Ok(entry)
        })
    }
}
struct Coverage(Mutex<VecDeque<ProjectFileSourceCoverage>>);
impl ProjectFileSourceCoverageReader for Coverage {
    fn coverage<'a>(
        &'a self,
        _: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<ProjectFileSourceCoverage, FileQueryError>> {
        Box::pin(async move {
            let mut states = self.0.lock().expect("coverage fixture");
            if states.len() > 1 {
                Ok(states.pop_front().expect("queued coverage"))
            } else {
                Ok(*states.front().expect("current coverage"))
            }
        })
    }
}
struct Project {
    f: Fixture,
    authority: Arc<Authority>,
    store: Arc<Store>,
    app: FileQueryApplication,
    coverage: Arc<Coverage>,
}
impl Project {
    fn new() -> Self {
        let mut f = Fixture::new();
        let resource = ResourceCommandContext {
            tenant_id: f.context.actor.tenant_id,
            actor: f.context.actor,
            source: ResourceCommandSource::Conversation {
                conversation_id: f
                    .context
                    .require_conversation()
                    .expect("conversation fixture"),
                workspace_id: Some(WorkspaceId::generate()),
            },
            invocation_id: None,
            tool_key: None,
            now: f.context.now,
        };
        f.context.project_workspace_id = resource.workspace_id();
        let node_id = ResourceNodeId::generate();
        let artifact_id = ArtifactId::generate();
        let r = f.bytes.add("text/plain", b"project content");
        let entry = ProjectFileCatalogEntry {
            production: None,
            node: ResourceNode {
                is_result: false,
                node_id,
                tenant_id: resource.tenant_id,
                scope: resource.scope(),
                parent_node_id: None,
                original_parent_node_id: None,
                name: ResourceName::new("project.txt").expect("valid fixture and authorized query"),
                kind: ResourceNodeKind::File,
                lifecycle: ResourceNodeLifecycle::Active,
                file_artifact_id: Some(artifact_id),
                sort_key: "a".into(),
                created_at: f.context.now,
                updated_at: f.context.now,
                revision: ResourceRevision::new(1),
                provenance: ResourceProvenance {
                    actor: resource.actor,
                    conversation_id: Some(
                        f.context
                            .require_conversation()
                            .expect("conversation fixture"),
                    ),
                    invocation_id: None,
                    tool_key: None,
                },
            },
            file: Some(ProjectFileVersion {
                node_id,
                artifact_id,
                artifact_version_id: ArtifactVersionId::generate(),
                name: "project.txt".into(),
                version: VersionNumber::new(1).expect("valid fixture and authorized query"),
                content_ref: r,
            }),
            file_updated_at: Some(UtcTimestamp::from_unix_millis(20)),
        };
        let authority = Arc::new(Authority(Mutex::new(resource)));
        let store = Arc::new(Store(
            Mutex::new(entry),
            Mutex::new(Sha256::digest(b"first")),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let coverage = Arc::new(Coverage(Mutex::new(VecDeque::from([
            ProjectFileSourceCoverage {
                revision: 1,
                complete: true,
            },
        ]))));
        let catalog = Arc::new(ManagedResourceFileCatalog::new(
            authority.clone(),
            store.clone(),
            coverage.clone(),
        ));
        let app = FileQueryApplication::new(
            f.catalog.clone(),
            catalog,
            Arc::new(ContentFileReader::new(f.bytes.clone())),
        );
        Self {
            f,
            authority,
            store,
            app,
            coverage,
        }
    }
    async fn page(&self) -> FilePage {
        self.app
            .list(&self.f.context, project_query())
            .await
            .expect("valid fixture and authorized query")
    }
}
fn project_query() -> ListFiles {
    ListFiles {
        scope: FileScope::Project,
        ..query()
    }
}

#[tokio::test]
async fn rename_preserves_file_references_and_reads_but_content_changes_do_not() {
    let p = Project::new();
    let before = p.page().await.entries[0].clone();
    let mut legacy = before.file_ref.clone();
    legacy.revision = legacy.revision.replacen(":file:", ":1:", 1);
    {
        let mut source = p.store.0.lock().expect("fixture lock");
        source.node.name = ResourceName::new("renamed.txt").expect("name");
        source.node.revision = ResourceRevision::new(2);
    }
    let after = p.page().await.entries[0].clone();
    assert_eq!(before.file_ref, after.file_ref);
    assert_eq!(after.name, "renamed.txt");
    for reference in [&before.file_ref, &legacy] {
        let result = p
            .app
            .read(&p.f.context, read(reference.clone()))
            .await
            .expect("read after rename");
        assert_eq!(result.view.text.as_deref(), Some("project content"));
        assert_eq!(&result.view.entry.file_ref, reference);
        assert_eq!(result.view.entry.name, "renamed.txt");
    }
    p.store
        .0
        .lock()
        .expect("fixture lock")
        .file
        .as_mut()
        .expect("file")
        .artifact_version_id = ArtifactVersionId::generate();
    for reference in [&before.file_ref, &legacy] {
        assert_eq!(
            p.app
                .inspect(&p.f.context, reference)
                .await
                .expect_err("old content rejected"),
            FileQueryError::Changed
        );
    }
}

#[tokio::test]
async fn same_application_selects_scope_without_copying_project_files_into_conversation() {
    let p = Project::new();
    let page = p.page().await;
    assert_eq!(page.coverage, FileCoverage::ProjectChildren);
    assert_eq!(page.entries[0].modified_at_ms, 20);
    assert!(
        p.app
            .list(&p.f.context, query())
            .await
            .expect("valid fixture and authorized query")
            .entries
            .is_empty()
    );
    let result = p
        .app
        .read(&p.f.context, read(page.entries[0].file_ref.clone()))
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(result.view.text.as_deref(), Some("project content"));
    assert!(
        p.app
            .list(&p.f.context, query())
            .await
            .expect("valid fixture and authorized query")
            .entries
            .is_empty(),
        "reading does not create a conversation association"
    );
}
#[tokio::test]
async fn version_fingerprint_invalidates_cursor_even_when_tree_revision_is_unchanged() {
    let p = Project::new();
    let old = p.page().await;
    *p.store.1.lock().expect("fixture lock") = Sha256::digest(b"edited-file");
    assert_eq!(
        p.app
            .list(
                &p.f.context,
                ListFiles {
                    cursor: old.next_cursor,
                    ..project_query()
                }
            )
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Changed
    );
}
#[tokio::test]
async fn project_reassignment_invalidates_an_old_node_reference() {
    let p = Project::new();
    let reference = p.page().await.entries[0].file_ref.clone();
    p.authority.0.lock().expect("fixture lock").source = ResourceCommandSource::Conversation {
        conversation_id: p
            .f
            .context
            .require_conversation()
            .expect("conversation fixture"),
        workspace_id: Some(WorkspaceId::generate()),
    };
    assert_eq!(
        p.app
            .inspect(&p.f.context, &reference)
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Denied
    );
}
#[tokio::test]
async fn content_allowlist_does_not_grant_project_directory_enumeration() {
    let p = Project::new();
    let mut context = p.f.context.clone();
    context.project_workspace_id = None;
    context.content_allowlist = Some(vec![
        p.store
            .0
            .lock()
            .expect("valid fixture and authorized query")
            .file
            .as_ref()
            .expect("valid fixture and authorized query")
            .content_ref
            .clone(),
    ]);
    assert_eq!(
        p.app
            .list(&context, project_query())
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Denied
    );
}
#[tokio::test]
async fn file_cannot_be_used_as_a_folder_and_trashed_records_fail_closed() {
    let p = Project::new();
    let reference = p.page().await.entries[0].file_ref.clone();
    assert_eq!(
        p.app
            .list(
                &p.f.context,
                ListFiles {
                    parent: Some(reference.clone()),
                    ..project_query()
                }
            )
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::InvalidQuery
    );
    p.store.0.lock().expect("fixture lock").node.lifecycle = ResourceNodeLifecycle::Trashed;
    assert_eq!(
        p.app
            .inspect(&p.f.context, &reference)
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Integrity
    );
}

#[tokio::test]
async fn pending_backing_output_stays_visible_without_becoming_an_empty_folder() {
    let p = Project::new();
    p.store.0.lock().expect("fixture lock").file = None;
    let page = p.page().await;
    assert_eq!(page.entries.len(), 1);
    assert!(!page.entries[0].is_directory);
    assert_eq!(page.entries[0].availability, FileAvailability::Unavailable);
    assert_eq!(
        p.app
            .read(&p.f.context, read(page.entries[0].file_ref.clone()))
            .await
            .expect_err("unavailable output"),
        FileQueryError::Unavailable
    );
    assert_eq!(*p.f.bytes.reads.lock().expect("fixture lock"), 0);
}

#[tokio::test]
async fn standalone_project_listing_rejects_conversation_scope_without_inventing_membership() {
    let p = Project::new();
    let mut context = p.f.context.clone();
    context.conversation_id = None;
    let target = context.target().expect("standalone workspace");
    assert_eq!(
        query().validate_target(target),
        Err(FileQueryError::InvalidQuery)
    );
    assert_eq!(project_query().validate_target(target), Ok(()));
    assert_eq!(
        p.app.list(&context, query()).await.err(),
        Some(FileQueryError::InvalidQuery)
    );
    assert!(p.app.list(&context, project_query()).await.is_ok());
    assert!(p.app.list(&p.f.context, query()).await.is_ok());
}

#[tokio::test]
async fn standalone_project_queries_keep_real_workspace_identity_and_reject_conversation_reuse() {
    let p = Project::new();
    let conversation_reference = p.page().await.entries[0].file_ref.clone();
    let mut context = p.f.context.clone();
    context.conversation_id = None;
    let page = p
        .app
        .list(&context, project_query())
        .await
        .expect("authorized workspace");
    let reference = &page.entries[0].file_ref;
    assert!(reference.conversation_id.is_none());
    assert_eq!(
        reference.workspace_id,
        context.project_workspace_id.map(|id| id.to_string())
    );
    assert_eq!(
        p.app
            .read(&context, read(reference.clone()))
            .await
            .expect("original content")
            .view
            .text
            .as_deref(),
        Some("project content")
    );
    assert_eq!(
        p.app.inspect(&context, &conversation_reference).await.err(),
        Some(FileQueryError::InvalidReference)
    );
    assert_eq!(
        p.app.inspect(&p.f.context, reference).await.err(),
        Some(FileQueryError::InvalidReference)
    );
    context.project_workspace_id = Some(WorkspaceId::generate());
    assert_eq!(
        p.app.inspect(&context, reference).await.err(),
        Some(FileQueryError::InvalidReference)
    );
}

#[tokio::test]
async fn pending_legacy_sources_cannot_claim_complete_but_known_files_remain_readable() {
    let p = Project::new();
    let file = p.page().await.entries.remove(0).file_ref;
    *p.coverage.0.lock().expect("coverage fixture") = VecDeque::from([ProjectFileSourceCoverage {
        revision: 2,
        complete: false,
    }]);
    assert!(matches!(
        p.app.list(&p.f.context, project_query()).await,
        Err(FileQueryError::Incomplete)
    ));
    assert!(p.app.inspect(&p.f.context, &file).await.is_ok());
}

#[tokio::test]
async fn source_changes_during_page_and_between_pages_invalidate_the_snapshot() {
    let p = Project::new();
    let first = p.page().await;
    *p.coverage.0.lock().expect("coverage fixture") = VecDeque::from([
        ProjectFileSourceCoverage {
            revision: 1,
            complete: true,
        },
        ProjectFileSourceCoverage {
            revision: 2,
            complete: true,
        },
    ]);
    assert!(matches!(
        p.app.list(&p.f.context, project_query()).await,
        Err(FileQueryError::Changed)
    ));
    let mut next = project_query();
    next.cursor = first.next_cursor;
    assert!(matches!(
        p.app.list(&p.f.context, next).await,
        Err(FileQueryError::Changed)
    ));
    assert!(p.app.list(&p.f.context, project_query()).await.is_ok());
}

#[tokio::test]
async fn recursive_search_uses_original_store_and_binds_traversal_to_cursor() {
    let p = Project::new();
    let q = SearchFiles {
        scope: Some(FileScope::Project),
        parent: None,
        file_ref: None,
        query: "project".into(),
        cursor: None,
        limit: 1,
        traversal: FileSearchTraversal::Subtree,
    };
    let page = p
        .app
        .search(&p.f.context, q.clone())
        .await
        .expect("subtree search");
    assert_eq!(page.coverage, FileCoverage::ProjectSubtree);
    assert_eq!(page.hits.len(), 1);
    let cursor = page.next_cursor.expect("continuation");
    let mut continued = q;
    continued.cursor = Some(cursor);
    continued.traversal = FileSearchTraversal::Children;
    assert_eq!(
        p.app
            .search(&p.f.context, continued)
            .await
            .expect_err("traversal binding"),
        FileQueryError::InvalidQuery
    );
}

struct TreeStore(Vec<ProjectFileCatalogEntry>);
#[tokio::test]
async fn result_preview_uses_original_directory_and_rejects_a_different_version() {
    let p = Project::new();
    let primary = p.f.artifact(
        "report.html",
        musteragent_artifact::ArtifactKind::File,
        "text/html",
        b"<img src=figure.svg>",
    );
    let artifact_id = p.f.projection.0.lock().unwrap().artifacts[0].artifact_id;
    {
        let mut entry = p.store.0.lock().unwrap();
        entry.node.name = ResourceName::new("report.html").unwrap();
        entry.node.file_artifact_id = Some(artifact_id);
        let version = entry.file.as_mut().unwrap();
        version.artifact_id = artifact_id;
        version.name = "report.html".into();
        version.content_ref = primary.clone();
    }
    let reference = p.f.first().await;
    let result = p
        .app
        .open_preview(&p.f.context, &reference)
        .await
        .expect("result directory");
    let tree = result.file_tree.expect("original Resource tree");
    assert_eq!(tree.entrypoint, "report.html");
    assert_eq!(tree.files[0].content, primary);
    assert_eq!(tree.files[0].file_ref.scope, FileScope::Project);
    let resource = p
        .app
        .open_preview(&p.f.context, &tree.files[0].file_ref)
        .await
        .expect("same file via Resource");
    assert_eq!(resource.file_tree.unwrap(), tree);
    p.store.0.lock().unwrap().file.as_mut().unwrap().version = VersionNumber::new(2).unwrap();
    assert_eq!(
        p.app
            .open_preview(&p.f.context, &reference)
            .await
            .unwrap_err(),
        FileQueryError::Changed
    );
    p.f.projection.0.lock().unwrap().artifacts.clear();
    assert_eq!(
        p.app
            .open_preview(&p.f.context, &reference)
            .await
            .unwrap_err(),
        FileQueryError::NotFound
    );
}

impl ProjectFileCatalogStore for TreeStore {
    fn snapshot_files(
        &self,
        q: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        self.list_files(q)
    }

    fn list_files(
        &self,
        q: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        Box::pin(async move {
            let end = q.offset.saturating_add(q.limit).min(self.0.len());
            Ok(ProjectFileCatalogPage {
                scope: q.context.scope(),
                revision: ResourceRevision::new(1),
                content_revision: Sha256::digest(b"tree"),
                entries: self.0[q.offset..end].to_vec(),
                next_offset: (end < self.0.len()).then_some(end),
            })
        })
    }
    fn resolve_file<'a>(
        &'a self,
        _: &'a ResourceCommandContext,
        id: ResourceNodeId,
    ) -> ApplicationFuture<'a, Result<ProjectFileCatalogEntry, ResourceStoreError>> {
        Box::pin(async move {
            self.0
                .iter()
                .find(|entry| entry.node.node_id == id)
                .cloned()
                .ok_or(ResourceStoreError::NodeUnavailable)
        })
    }
}
#[tokio::test]
async fn search_skips_directory_only_pages_with_a_separate_metadata_budget() {
    for directory_count in [154, 600] {
        let p = Project::new();
        let file = p.store.0.lock().expect("project store lock").clone();
        let mut entries = Vec::new();
        for i in 0..directory_count {
            let mut folder = file.clone();
            folder.node.node_id = ResourceNodeId::generate();
            folder.node.name = ResourceName::new(format!("folder-{i}")).expect("folder name");
            folder.node.kind = ResourceNodeKind::Folder;
            folder.node.file_artifact_id = None;
            folder.file = None;
            entries.push(folder);
        }
        entries.push(file);
        let app = FileQueryApplication::new(
            p.f.catalog.clone(),
            Arc::new(ManagedResourceFileCatalog::new(
                p.authority,
                Arc::new(TreeStore(entries)),
                p.coverage,
            )),
            Arc::new(ContentFileReader::new(p.f.bytes.clone())),
        );
        let mut q = SearchFiles {
            traversal: FileSearchTraversal::Children,
            scope: Some(FileScope::Project),
            parent: None,
            file_ref: None,
            query: "content".into(),
            cursor: None,
            limit: 20,
        };
        let first = app
            .search(&p.f.context, q.clone())
            .await
            .expect("first search page");
        let result = if directory_count > 512 {
            assert!(!first.complete);
            assert_eq!(first.scanned_bytes, 0);
            q.cursor = Some(first.next_cursor.expect("bounded traversal continuation"));
            app.search(&p.f.context, q)
                .await
                .expect("remaining search page")
        } else {
            first
        };
        assert_eq!(result.hits.len(), 1);
        assert!(result.complete);
        assert_eq!(result.scanned_bytes, 15);
    }
}

#[tokio::test]
async fn preview_describes_large_directory_without_materializing_unrequested_bodies() {
    let p = Project::new();
    let primary = p.f.bytes.add("text/html", b"<h1>small report</h1>");
    let mut report = p.store.0.lock().unwrap().clone();
    report.node.name = ResourceName::new("report.html").unwrap();
    report.file.as_mut().unwrap().name = "report.html".into();
    report.file.as_mut().unwrap().content_ref = primary.clone();
    let mut entries = vec![report.clone()];
    // No such body exists in the content reader. Directory metadata must not
    // read it, or reject the small report because an unrelated file is large.
    let mut unrelated = report.clone();
    unrelated.node.node_id = ResourceNodeId::generate();
    unrelated.node.name = ResourceName::new("unrelated.bin").unwrap();
    let artifact_id = ArtifactId::generate();
    unrelated.node.file_artifact_id = Some(artifact_id);
    let version = unrelated.file.as_mut().unwrap();
    version.node_id = unrelated.node.node_id;
    version.artifact_id = artifact_id;
    version.name = "unrelated.bin".into();
    version.content_ref.byte_length = 365 * 1024 * 1024;
    entries.push(unrelated);
    let app = FileQueryApplication::new(
        p.f.catalog.clone(),
        Arc::new(ManagedResourceFileCatalog::new(
            p.authority,
            Arc::new(TreeStore(entries)),
            p.coverage,
        )),
        Arc::new(ContentFileReader::new(p.f.bytes.clone())),
    );
    let page = app.list(&p.f.context, project_query()).await.unwrap();
    let reference = &page
        .entries
        .iter()
        .find(|entry| entry.name == "report.html")
        .unwrap()
        .file_ref;
    let descriptor = app.open_preview(&p.f.context, reference).await.unwrap();
    assert_eq!(descriptor.primary, primary);
    let tree = descriptor.file_tree.unwrap();
    assert_eq!(tree.entrypoint, "report.html");
    assert_eq!(tree.files.len(), 2);
    assert_eq!(
        tree.files
            .iter()
            .find(|file| file.path == "unrelated.bin")
            .unwrap()
            .content
            .byte_length,
        365 * 1024 * 1024
    );
}

#[tokio::test]
async fn preview_resolves_the_original_resource_for_all_formats_without_listing_directories() {
    use musteragent_artifact::ArtifactKind;
    for mime in [
        "text/plain",
        "text/markdown",
        "application/json",
        "text/html",
    ] {
        let p = Project::new();
        let content =
            p.f.artifact("report", ArtifactKind::File, mime, b"original");
        let selected = p.f.first().await;
        let artifact_id = p.f.projection.0.lock().unwrap().artifacts[0].artifact_id;
        let node_id = {
            let mut stored = p.store.0.lock().unwrap();
            stored.node.file_artifact_id = Some(artifact_id);
            let file = stored.file.as_mut().unwrap();
            file.artifact_id = artifact_id;
            file.content_ref = content.clone();
            stored.node.node_id
        };
        let descriptor = p.app.open_preview(&p.f.context, &selected).await.unwrap();
        let placement = descriptor.resource_ref.expect("original placement");
        assert_eq!(
            placement.source,
            FileSource::Resource {
                node_id: node_id.to_string()
            }
        );
        assert_eq!(placement.conversation_id, selected.conversation_id);
        assert_eq!(descriptor.entry.file_ref, selected);
        assert_eq!(descriptor.primary, content);
        assert_eq!(descriptor.file_tree.is_some(), mime == "text/html");
        // Ordinary text resolves only the indexed placement, not all folders.
        assert_eq!(
            p.store.2.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(mime == "text/html")
        );
        assert_eq!(*p.f.bytes.reads.lock().unwrap(), 0);
        p.store.0.lock().unwrap().file.as_mut().unwrap().version = VersionNumber::new(2).unwrap();
        assert_eq!(
            p.app
                .open_preview(&p.f.context, &selected)
                .await
                .unwrap_err(),
            FileQueryError::Changed
        );
    }
}

#[tokio::test]
async fn standalone_historical_result_does_not_acquire_an_unrelated_edit_target() {
    let p = Project::new();
    p.f.artifact(
        "report.txt",
        musteragent_artifact::ArtifactKind::File,
        "text/plain",
        b"original",
    );
    let selected = p.f.first().await;
    let descriptor = p.app.open_preview(&p.f.context, &selected).await.unwrap();
    assert!(descriptor.resource_ref.is_none());
    assert!(descriptor.file_tree.is_none());
    assert_eq!(p.store.2.load(std::sync::atomic::Ordering::SeqCst), 0);
}
