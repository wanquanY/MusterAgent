#![allow(dead_code)]
use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::MessageRole;
use musteragent_identity::*;
use musteragent_projection::*;
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::{ApplicationFuture, AttachmentRef, DisplayText, RuntimeError};
use musteragent_runtime_file_query::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Projection(pub Mutex<ConversationProjectionSnapshot>);
impl ConversationProjectionApplication for Projection {
    fn get_head_projection(
        &self,
        q: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        Box::pin(async move {
            let s = self.0.lock().expect("fixture lock").clone();
            assert_eq!(s.conversation_id, q.conversation_id);
            assert_eq!(s.tenant_id, q.actor.tenant_id);
            assert!(q.preferred_snapshot_id.is_none());
            Ok(s)
        })
    }
    fn get_conversation_projection(
        &self,
        _: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        panic!("historical snapshot is not current authority")
    }
    fn get_bounded_head_projection(
        &self,
        _: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        panic!("bounded transcript omits old attachments")
    }
    fn get_product_read_projection_window(
        &self,
        _: GetConversationProjectionWindow,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionWindow, RuntimeError>> {
        panic!("file discovery cannot depend on loaded UI rows")
    }
}
#[derive(Default)]
pub struct Bytes {
    pub values: Mutex<HashMap<ContentId, Vec<u8>>>,
    pub reads: Mutex<usize>,
    pub on_read: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl ContentStore for Bytes {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        panic!("queries are read only")
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        r: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            *self.reads.lock().expect("fixture lock") += 1;
            let hook = self.on_read.lock().expect("fixture lock").clone();
            if let Some(hook) = hook {
                hook();
            }
            self.values
                .lock()
                .expect("valid fixture and authorized query")
                .get(&r.content_id)
                .cloned()
                .ok_or(ContentStoreError::Unavailable)
        })
    }
}
impl Bytes {
    pub fn add(&self, mime: &str, bytes: &[u8]) -> ContentRef {
        let r = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(mime).expect("valid fixture and authorized query"),
            sha256: Sha256::digest(bytes),
            byte_length: bytes.len() as u64,
        };
        self.values
            .lock()
            .expect("valid fixture and authorized query")
            .insert(r.content_id, bytes.to_vec());
        r
    }
}
pub struct Fixture {
    pub context: FileQueryContext,
    pub projection: Arc<Projection>,
    pub bytes: Arc<Bytes>,
    pub catalog: Arc<ConversationFileCatalog>,
    pub application: FileQueryApplication,
}
impl Fixture {
    pub fn new() -> Self {
        let context = FileQueryContext {
            dialogue_source: None,
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            conversation_id: Some(ConversationId::generate()),
            now: UtcTimestamp::from_unix_millis(10),
            authority_revision: "grant-1".to_owned(),
            content_allowlist: None,
            project_workspace_id: None,
        };
        let projection = Arc::new(Projection(Mutex::new(ConversationProjectionSnapshot {
            schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
            snapshot_id: ProjectionSnapshotId::generate(),
            tenant_id: context.actor.tenant_id,
            conversation_id: context
                .require_conversation()
                .expect("conversation fixture"),
            home_epoch: HomeEpoch::new(1).expect("valid fixture and authorized query"),
            through_conversation_seq: LedgerOffset::new(1),
            messages: vec![],
            tool_timeline: vec![],
            context_compactions: vec![],
            pending_interactions: vec![],
            activities: vec![],
            runs: vec![],
            active_runs: vec![],
            artifacts: vec![],
            stream_outputs: vec![],
            content_manifest: vec![],
            checksum: Sha256::digest(b"snapshot"),
            created_at: context.now,
        })));
        let bytes = Arc::new(Bytes::default());
        let catalog = Arc::new(ConversationFileCatalog::new(projection.clone()));
        let application = FileQueryApplication::new(
            catalog.clone(),
            Arc::new(NoProject),
            Arc::new(ContentFileReader::new(bytes.clone())),
        )
        .with_content_store(bytes.clone());
        Self {
            context,
            projection,
            bytes,
            catalog,
            application,
        }
    }
    pub fn attachment(&self, name: &str, mime: &str, body: &[u8]) -> ContentRef {
        let r = self.bytes.add(mime, body);
        self.projection
            .0
            .lock()
            .expect("valid fixture and authorized query")
            .messages
            .push(ProjectedMessage {
                project_task_target: None,
                message_id: MessageId::generate(),
                participant_id: None,
                role: MessageRole::User,
                content_ref: self.bytes.add("text/plain", b"message"),
                reasoning_summaries: vec![],
                status: ProjectedMessageStatus::Committed,
                source_run_id: None,
                committed_at: self.context.now,
                attachment_refs: vec![AttachmentRef {
                    content_ref: r.clone(),
                    display_name: DisplayText::new(name)
                        .expect("valid fixture and authorized query"),
                }],
                requested_skill_refs: vec![],
                imported_projection_ref: None,
                assistant_output: None,
                partial_reason: None,
            });
        r
    }
    pub fn artifact(&self, name: &str, kind: ArtifactKind, mime: &str, body: &[u8]) -> ContentRef {
        let r = self.bytes.add(mime, body);
        self.projection
            .0
            .lock()
            .expect("valid fixture and authorized query")
            .artifacts
            .push(ProjectedArtifact {
                artifact_id: ArtifactId::generate(),
                activity_id: None,
                created_by_run_id: None,
                updated_by_run_id: None,
                presentation_run_id: None,
                kind,
                lifecycle: ArtifactLifecycle::Published,
                current_version: VersionNumber::new(1).expect("valid fixture and authorized query"),
                title: name.to_owned(),
                current_content_ref: r.clone(),
                current_dependency_refs: vec![],
                updated_at: self.context.now,
            });
        r
    }
    pub async fn first(&self) -> FileReference {
        self.application
            .list(&self.context, query())
            .await
            .expect("valid fixture and authorized query")
            .entries[0]
            .file_ref
            .clone()
    }
}
pub fn query() -> ListFiles {
    ListFiles {
        produced_by: None,
        scope: FileScope::Conversation,
        parent: None,
        cursor: None,
        limit: 100,
        name_query: None,
    }
}
pub fn read(reference: FileReference) -> ReadFile {
    ReadFile {
        file_ref: reference,
        offset: 0,
        limit: 1024,
    }
}
pub struct NoProject;
impl FileCatalog for NoProject {
    fn list<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Unavailable) })
    }
    fn resolve_identity<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: &'a musteragent_runtime_file_query::FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Unavailable) })
    }
}
