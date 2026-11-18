#![cfg(test)]

mod support;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::{DialogueContextCut, DialogueCursor};
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{Arc, Mutex};
use support::{Bytes, Fixture, read};

struct Resources(Mutex<String>);
impl FileCatalog for Resources {
    fn list<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async {
            Ok(FilePage {
                entries: vec![],
                next_cursor: None,
                revision: self.0.lock().unwrap().clone(),
                coverage: FileCoverage::OwnedAgentResources,
                retraction_status: None,
            })
        })
    }
    fn list_subtree<'a>(
        &'a self,
        c: &'a FileQueryContext,
        q: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        self.list(c, q)
    }
    fn resolve_identity<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Unavailable) })
    }
}
struct Inputs(Vec<ResolvedFile>);
struct Outputs(Vec<ResolvedFile>);
impl DialogueOutputSource for Outputs {
    fn list<'a>(
        &'a self,
        _: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, FileQueryError>> {
        Box::pin(async move {
            let start = (from.seq - 1) as usize;
            let end = (start + limit).min(self.0.len());
            Ok(DialogueInputPage {
                files: self.0[start..end].to_vec(),
                next: (end < self.0.len()).then_some(DialogueInputPosition {
                    seq: end as u64 + 1,
                    ordinal: 0,
                }),
            })
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        _: &'a FileQueryContext,
        identity: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            self.0
                .iter()
                .find(|f| f.entry.file_ref.identity() == *identity)
                .cloned()
                .ok_or(FileQueryError::Denied)
        })
    }
}
impl DialogueInputSource for Inputs {
    fn list<'a>(
        &'a self,
        _: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, FileQueryError>> {
        Box::pin(async move {
            let start = (from.seq - 1) as usize;
            let end = (start + limit).min(self.0.len());
            Ok(DialogueInputPage {
                files: self.0[start..end].to_vec(),
                next: (end < self.0.len()).then_some(DialogueInputPosition {
                    seq: end as u64 + 1,
                    ordinal: 0,
                }),
            })
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        c: &'a FileQueryContext,
        r: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            r.validate(c)?;
            self.0
                .iter()
                .find(|f| f.entry.file_ref.identity() == *r)
                .cloned()
                .ok_or(FileQueryError::Denied)
        })
    }
}
fn setup(
    count: usize,
) -> (
    FileQueryContext,
    Arc<Resources>,
    Arc<Inputs>,
    Arc<Bytes>,
    AgentFileCatalog,
) {
    let fixture = Fixture::new();
    let mut context = fixture.context;
    let agent_id = AgentId::from_uuid(uuid::Uuid::now_v7()).unwrap();
    let cut = DialogueContextCut {
        tenant_id: context.actor.tenant_id,
        owner_id: context.actor.principal_id,
        agent_id,
        dialogue_id: AgentDialogueId::generate(),
        through: DialogueCursor {
            epoch: 1,
            seq: count as u64,
        },
        entry_count: count as u64,
        source_chain_sha256: Sha256::digest(b"frozen inputs"),
    };
    context.dialogue_source = Some(AgentFileQuerySource {
        agent_id,
        invocation_id: InvocationId::generate(),
        tool_key: VersionedKey::new("files.list@4").unwrap(),
        input_view: Some(DialogueInputView {
            delegation: None,
            cut: cut.clone(),
            has_inputs: true,
        }),
    });
    let inputs = Arc::new(Inputs(
        (1..=count)
            .map(|seq| {
                let content = fixture.bytes.add("text/plain", b"accepted file");
                ResolvedFile {
                    artifact: None,
                    dependencies: vec![],
                    content: Some(content.clone()),
                    entry: FileEntry {
                        production: None,
                        native_workspace_id: None,
                        file_ref: FileReference {
                            schema: AGENT_INPUT_FILE_REFERENCE_SCHEMA.to_owned(),
                            scope: FileScope::OwnedAgent,
                            conversation_id: None,
                            workspace_id: None,
                            owned_agent_id: Some(agent_id.to_string()),
                            source: FileSource::DialogueInput {
                                dialogue_id: cut.dialogue_id.to_string(),
                                entry_id: DialogueEntryId::generate().to_string(),
                                epoch: 1,
                                seq: seq as u64,
                                ordinal: 0,
                            },
                            revision: content.sha256.to_string(),
                        },
                        name: format!("input-{seq}.txt"),
                        is_directory: false,
                        media_type: Some("text/plain".into()),
                        byte_length: Some(content.byte_length),
                        modified_at_ms: 10,
                        associated_at_ms: Some(10),
                        semantic_kind: Some("input_attachment".into()),
                        availability: FileAvailability::Available,
                    },
                }
            })
            .collect(),
    ));
    let resources = Arc::new(Resources(Mutex::new("resource-1".into())));
    let catalog = AgentFileCatalog::new(resources.clone(), inputs.clone());
    (context, resources, inputs, fixture.bytes, catalog)
}
fn query() -> ListFiles {
    ListFiles {
        produced_by: None,
        scope: FileScope::OwnedAgent,
        parent: None,
        cursor: None,
        limit: 100,
        name_query: None,
    }
}

#[tokio::test]
async fn published_media_paginates_after_inputs_and_opens_original_binary_without_copying() {
    let (mut context, resources, inputs, bytes, _) = setup(3);
    context.dialogue_source.as_mut().unwrap().tool_key = VersionedKey::new("files.list@6").unwrap();
    let files = inputs
        .0
        .iter()
        .map(|file| {
            let mut file = file.clone();
            let FileSource::DialogueInput {
                dialogue_id,
                entry_id,
                epoch,
                seq,
                ordinal,
            } = file.entry.file_ref.source
            else {
                panic!("input");
            };
            file.entry.file_ref.source = FileSource::DialogueOutput {
                dialogue_id,
                entry_id,
                epoch,
                seq,
                ordinal,
            };
            file.entry.file_ref.schema = AGENT_OUTPUT_FILE_REFERENCE_SCHEMA.into();
            file.entry.name = format!("material-{seq}.mp4");
            let content = bytes.add("video/mp4", b"original immutable movie bytes");
            file.entry.file_ref.revision = format!("delivered:{seq}:{}", content.sha256);
            file.entry.media_type = Some(content.media_type.as_str().into());
            file.entry.byte_length = Some(content.byte_length);
            file.entry.semantic_kind = Some("file".into());
            file.content = Some(content);
            file
        })
        .collect::<Vec<_>>();
    let catalog = Arc::new(
        AgentFileCatalog::new(resources.clone(), inputs)
            .with_outputs(Arc::new(Outputs(files.clone()))),
    );
    let app = FileQueryApplication::new(
        resources.clone(),
        resources,
        Arc::new(ContentFileReader::new(bytes.clone())),
    )
    .with_dialogue_catalog(catalog)
    .with_content_store(bytes);
    let mut q = query();
    let mut found = Vec::new();
    let mut pages = 0;
    loop {
        let page = app.list(&context, q.clone()).await.unwrap();
        pages += 1;
        found.extend(page.entries);
        q.cursor = page.next_cursor;
        if q.cursor.is_none() {
            break;
        }
        assert!(pages <= 3);
    }
    assert_eq!(pages, 3);
    assert_eq!(found.len(), 6);
    let reference = files[1].entry.file_ref.clone();
    let opened = app.open(&context, &reference).await.unwrap();
    assert_eq!(opened.primary, files[1].content.clone().unwrap());
    assert!(opened.artifact_origin.is_none());
    assert!(opened.resource_ref.is_none());
    let original = app
        .read_content(&context, &reference, &opened.primary)
        .await
        .unwrap();
    assert_eq!(original.bytes, b"original immutable movie bytes");
    let mut stale = reference.clone();
    stale.revision.push_str("changed");
    assert_eq!(
        app.open(&context, &stale).await,
        Err(FileQueryError::Changed)
    );
    let mut foreign = reference;
    foreign.owned_agent_id = Some(AgentId::generate().to_string());
    assert!(app.open(&context, &foreign).await.is_err());
}

#[tokio::test]
async fn dialogue_input_full_pagination_and_filtered_empty_progress() {
    let (context, _, _, _, catalog) = setup(129);
    let mut q = query();
    let mut all = vec![];
    let mut pages = 0;
    loop {
        let page = catalog.list(&context, q.clone()).await.unwrap();
        assert_eq!(page.coverage, FileCoverage::OwnedAgentFiles);
        pages += 1;
        all.extend(page.entries);
        q.cursor = page.next_cursor;
        if q.cursor.is_none() {
            break;
        }
        assert!(pages < 5);
    }
    assert_eq!(pages, 3);
    assert_eq!(all.len(), 129);
    assert_eq!(all[128].name, "input-129.txt");
    let mut q = query();
    q.name_query = Some("129".into());
    let first = catalog.list(&context, q.clone()).await.unwrap();
    q.cursor = first.next_cursor;
    let second = catalog.list(&context, q.clone()).await.unwrap();
    assert!(second.entries.is_empty() && second.next_cursor.is_some());
    q.cursor = second.next_cursor;
    let last = catalog.list(&context, q).await.unwrap();
    assert_eq!(last.entries[0].name, "input-129.txt");
    assert!(last.next_cursor.is_none());
}

#[tokio::test]
async fn dialogue_input_cursor_fences_resources_cut_authority_and_old_contracts() {
    let (mut context, resources, _, _, catalog) = setup(129);
    let first = catalog.list(&context, query()).await.unwrap();
    let q = ListFiles {
        cursor: first.next_cursor,
        ..query()
    };
    *resources.0.lock().unwrap() = "resource-2".into();
    assert_eq!(
        catalog.list(&context, q.clone()).await.unwrap_err(),
        FileQueryError::Changed
    );
    *resources.0.lock().unwrap() = "resource-1".into();
    context.authority_revision = "permission-revoked".into();
    assert_eq!(
        catalog.list(&context, q.clone()).await.unwrap_err(),
        FileQueryError::InvalidQuery
    );
    context.authority_revision = "grant-1".into();
    context
        .dialogue_source
        .as_mut()
        .unwrap()
        .input_view
        .as_mut()
        .unwrap()
        .cut
        .source_chain_sha256 = Sha256::digest(b"another cut");
    assert_eq!(
        catalog.list(&context, q).await.unwrap_err(),
        FileQueryError::InvalidQuery
    );
    for old in [
        "files.list@3",
        "files.search@3",
        "files.inspect@3",
        "files.read@4",
    ] {
        context.dialogue_source.as_mut().unwrap().tool_key = VersionedKey::new(old).unwrap();
        assert_eq!(
            catalog.list(&context, query()).await.unwrap_err(),
            FileQueryError::Unsupported
        );
    }
}

#[tokio::test]
async fn dialogue_input_source_media_is_typed_and_exact_refs_never_become_authority() {
    let (mut context, _, inputs, bytes, _) = setup(1);
    context.dialogue_source.as_mut().unwrap().tool_key = VersionedKey::new("files.read@5").unwrap();
    let reader = ContentFileReader::new(bytes.clone());
    for mime in ["image/png", "audio/wav", "video/mp4", "application/pdf"] {
        let mut file = inputs.0[0].clone();
        let content = bytes.add(mime, b"verified original media");
        file.content = Some(content.clone());
        file.entry.media_type = Some(mime.into());
        file.entry.byte_length = Some(content.byte_length);
        file.entry.file_ref.revision = content.sha256.to_string();
        let query = read(file.entry.file_ref.clone());
        let result = reader.read(&context, file, query).await.unwrap();
        assert_eq!(
            result.view.representation,
            FileRepresentation::SourceContent
        );
        assert_eq!(result.authorized_content, vec![content]);
    }
    let mut other = inputs.0[0].entry.file_ref.clone();
    other.owned_agent_id = Some(
        AgentId::from_uuid(uuid::Uuid::now_v7())
            .unwrap()
            .to_string(),
    );
    assert_eq!(
        inputs.resolve(&context, &other).await.unwrap_err(),
        FileQueryError::InvalidReference
    );
}
