//! Product source identity and frozen Tool admission; not OS-provider tests.
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn context() -> FileQueryContext {
    FileQueryContext {
        dialogue_source: None,
        actor: Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        conversation_id: Some(ConversationId::generate()),
        project_workspace_id: Some(WorkspaceId::generate()),
        now: UtcTimestamp::from_unix_millis(1),
        authority_revision: "original-project-authority".into(),
        content_allowlist: None,
    }
}
fn entry(context: &FileQueryContext) -> FileEntry {
    FileEntry {
        production: None,
        native_workspace_id: context.project_workspace_id.map(|id| id.to_string()),
        file_ref: FileReference {
            owned_agent_id: None,
            schema: FILE_REFERENCE_SCHEMA.into(),
            conversation_id: context.conversation_id.map(|id| id.to_string()),
            workspace_id: context.reference_workspace_id(),
            scope: FileScope::Project,
            source: FileSource::NativeDocument {
                grant_id: uuid::Uuid::now_v7().to_string(),
                document_id: uuid::Uuid::now_v7().to_string(),
            },
            revision: "provider-observation:1".into(),
        },
        name: "same display name".into(),
        is_directory: false,
        media_type: None,
        byte_length: None,
        modified_at_ms: 0,
        associated_at_ms: None,
        semantic_kind: Some("file".into()),
        availability: FileAvailability::Available,
    }
}

#[test]
fn both_original_targets_keep_native_placement_without_fabricating_a_path() {
    let conversation = context();
    let project = FileQueryContext {
        dialogue_source: None,
        conversation_id: None,
        ..conversation.clone()
    };
    for context in [conversation, project] {
        let file = entry(&context);
        file.validate(&context).expect("original project");
        let json = serde_json::to_value(&file.file_ref).expect("wire");
        assert_eq!(json["source"].as_object().expect("source").len(), 3);
        assert!(json["source"].get("path").is_none());
        assert_eq!(
            serde_json::from_value::<FileReference>(json).expect("identity"),
            file.file_ref
        );
        let mut other = context.clone();
        other.project_workspace_id = Some(WorkspaceId::generate());
        assert!(file.validate(&other).is_err());
        other.project_workspace_id = None;
        assert!(file.validate(&other).is_err());
        let mut misplaced = file;
        misplaced.native_workspace_id = None;
        assert!(
            misplaced
                .validate_target(context.target().expect("target"))
                .is_err()
        );
    }
}

#[test]
fn document_and_grant_ids_are_canonical_opaque_nonzero_uuids() {
    let context = context();
    for value in [
        "",
        "content://provider/tree/root",
        "/private/file",
        "folder/file",
        "00000000-0000-0000-0000-000000000000",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
    ] {
        for grant in [false, true] {
            let mut reference = entry(&context).file_ref;
            let FileSource::NativeDocument {
                grant_id,
                document_id,
            } = &mut reference.source
            else {
                panic!("document source");
            };
            *if grant { grant_id } else { document_id } = value.into();
            assert_eq!(
                reference.validate(&context),
                Err(FileQueryError::InvalidReference)
            );
        }
    }
    let mut reference = entry(&context).file_ref;
    reference.scope = FileScope::Conversation;
    assert_eq!(
        reference.validate(&context),
        Err(FileQueryError::InvalidReference)
    );
}

#[test]
fn raw_locators_or_permission_fields_cannot_be_smuggled_into_a_document_reference() {
    let context = context();
    for key in ["uri", "path", "bookmark", "permissions", "provider_id"] {
        let mut json = serde_json::to_value(entry(&context).file_ref).expect("wire");
        json["source"][key] = serde_json::json!("private");
        assert!(
            serde_json::from_value::<FileReference>(json).is_err(),
            "{key}"
        );
    }
}

#[test]
fn refresh_may_change_name_and_revision_but_never_document_or_grant() {
    let context = context();
    let original = entry(&context);
    let target = context.target().expect("target");
    let mut current = original.clone();
    current.name = "renamed by the provider".into();
    current.file_ref.revision = "provider-observation:2".into();
    current
        .validate_refreshed_inspection(target, &original.file_ref)
        .expect("metadata");
    assert_eq!(
        current.validate_inspection(target, &original.file_ref),
        Err(FileQueryError::Integrity)
    );
    for grant in [true, false] {
        let mut changed = current.clone();
        let FileSource::NativeDocument {
            grant_id,
            document_id,
        } = &mut changed.file_ref.source
        else {
            panic!("document");
        };
        *if grant { grant_id } else { document_id } = uuid::Uuid::now_v7().to_string();
        assert_eq!(
            changed.validate_refreshed_inspection(target, &original.file_ref),
            Err(FileQueryError::Integrity)
        );
    }
}

struct MetadataOwner {
    context: FileQueryContext,
    file: FileEntry,
    denied: AtomicBool,
    revoke_during_list: AtomicBool,
    resolutions: AtomicUsize,
    lists: AtomicUsize,
}
impl MetadataOwner {
    fn new(directory: bool) -> Arc<Self> {
        let context = context();
        let mut file = entry(&context);
        file.is_directory = directory;
        Arc::new(Self {
            context,
            file,
            denied: AtomicBool::new(false),
            revoke_during_list: AtomicBool::new(false),
            resolutions: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
        })
    }
    fn service(self: &Arc<Self>) -> FileQueryService {
        FileQueryService::new(
            self.clone(),
            Arc::new(FileQueryApplication::new(
                self.clone(),
                self.clone(),
                self.clone(),
            )),
        )
    }
    fn access(&self) -> FileQueryAccess {
        FileQueryAccess {
            actor: self.context.actor,
            conversation_id: self.context.conversation_id,
            workspace_id: None,
            run: None,
        }
    }
}
impl FileQueryAuthority for MetadataOwner {
    fn resolve<'a>(
        &'a self,
        _: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async {
            self.resolutions.fetch_add(1, Ordering::SeqCst);
            if self.denied.load(Ordering::SeqCst) {
                return Err(FileQueryError::Denied);
            }
            Ok(self.context.clone())
        })
    }
}
impl FileCatalog for MetadataOwner {
    fn resolve_identity<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        panic!("a published Tool must reject a new source before resolving it")
    }

    fn list<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async {
            self.lists.fetch_add(1, Ordering::SeqCst);
            if self.revoke_during_list.load(Ordering::SeqCst) {
                self.denied.store(true, Ordering::SeqCst);
            }
            Ok(FilePage {
                entries: vec![self.file.clone()],
                next_cursor: None,
                revision: "provider-snapshot:1".into(),
                coverage: FileCoverage::ProjectChildren,
                retraction_status: None,
            })
        })
    }
    fn resolve_current<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        panic!("a published Tool must reject a new source before inspecting or reading it")
    }
}
impl FileContentReader for MetadataOwner {
    fn read<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: ResolvedFile,
        _: ReadFile,
    ) -> ApplicationFuture<'a, Result<FileReadResult, FileQueryError>> {
        panic!("frozen file Tools must never read an unpublished source")
    }
}
fn query() -> ListFiles {
    ListFiles {
        produced_by: None,
        scope: FileScope::Project,
        parent: None,
        cursor: None,
        limit: 100,
        name_query: None,
    }
}

#[tokio::test]
async fn published_tool_inputs_reject_document_sources_before_authority_or_io() {
    let owner = MetadataOwner::new(false);
    let service = owner.service();
    for version in [1, 2] {
        for (operation, input) in [
            (
                "list",
                serde_json::json!({"scope":"project","parent":owner.file.file_ref}),
            ),
            (
                "inspect",
                serde_json::json!({"file_ref":owner.file.file_ref}),
            ),
            ("read", serde_json::json!({"file_ref":owner.file.file_ref})),
            (
                "search",
                serde_json::json!({"file_ref":owner.file.file_ref,"query":"x"}),
            ),
            (
                "search",
                serde_json::json!({"scope":"project","parent":owner.file.file_ref,"query":"x"}),
            ),
        ] {
            assert_eq!(
                service
                    .execute_tool(
                        &owner.access(),
                        &format!("files.{operation}@{version}"),
                        &serde_json::to_vec(&input).expect("input")
                    )
                    .await
                    .expect_err("frozen schema"),
                FileQueryError::Unsupported,
                "{operation}@{version}"
            );
        }
    }
    assert_eq!(owner.resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(owner.lists.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn published_list_and_search_never_drop_rows_or_read_unsupported_sources() {
    for directory in [true, false] {
        let owner = MetadataOwner::new(directory);
        let service = owner.service();
        for version in [1, 2] {
            for (operation, input) in [
                ("list", serde_json::json!({"scope":"project"})),
                ("search", serde_json::json!({"scope":"project","query":"x"})),
            ] {
                assert_eq!(
                    service
                        .execute_tool(
                            &owner.access(),
                            &format!("files.{operation}@{version}"),
                            &serde_json::to_vec(&input).expect("input")
                        )
                        .await
                        .expect_err("frozen schema"),
                    FileQueryError::Unsupported,
                    "{operation}@{version}"
                );
            }
        }
        let page = service
            .list_product(&owner.access(), query())
            .await
            .expect("Product metadata");
        assert_eq!(page.entries, vec![owner.file.clone()]);
    }
}

#[tokio::test]
async fn product_metadata_still_reauthorizes_after_io_and_cannot_grant_access_by_identity() {
    let owner = MetadataOwner::new(false);
    let service = owner.service();
    service
        .list_product(&owner.access(), query())
        .await
        .expect("authorized metadata");
    assert_eq!(owner.resolutions.load(Ordering::SeqCst), 2);
    owner.revoke_during_list.store(true, Ordering::SeqCst);
    assert_eq!(
        service
            .list_product(&owner.access(), query())
            .await
            .expect_err("revoked during IO"),
        FileQueryError::Denied
    );
    let calls = owner.lists.load(Ordering::SeqCst);
    assert_eq!(
        service
            .list_product(&owner.access(), query())
            .await
            .expect_err("still denied"),
        FileQueryError::Denied
    );
    assert_eq!(owner.lists.load(Ordering::SeqCst), calls);
}
