//! The project panel edits both native and conversation-created Files. Its author
//! is the real Workspace principal in either case; provenance remains unchanged.
#[path = "workspace_principal_expiry.rs"]
mod expiry;

use super::*;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_identity::{PrincipalId, TenantId};
use musteragent_postgres::PostgresWorkspaceResourceRootAuthority;
use musteragent_runtime_resource::{
    CreateResourceFile, EnsureWorkspaceResourceRoot, ResourceCommandContext, ResourceCommandSource,
    TrashResourceNode, WorkspaceResourceRootAuthority,
};
use sqlx::{Acquire, PgPool, Row};

struct Fixture {
    _postgres: IsolatedPostgres,
    pool: PgPool,
    source: running_loop::Fixture,
    content: Arc<dyn ContentStore>,
    artifact_id: ArtifactId,
    context: ResourceCommandContext,
    node_id: ResourceNodeId,
}
impl Fixture {
    async fn new() -> Self {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("isolated PostgreSQL");
        let pool = postgres.pool().await.unwrap();
        musteragent_conformance::migrate(&pool).await.unwrap();
        let (source, _) = running_loop::seed_running_loop(&pool).await;
        let workspace = WorkspaceId::generate();
        let now = running_loop::database_now(&pool).await;
        let source_context = context(
            source.tenant_id,
            source.principal_id,
            source.conversation_id,
            workspace,
            now.unix_millis(),
        );
        PostgresWorkspaceResourceRootAuthority::new(pool.clone())
            .ensure_root(EnsureWorkspaceResourceRoot {
                actor: source_context.actor,
                workspace_id: workspace,
            })
            .await
            .unwrap();
        sqlx::query("UPDATE conversation.conversations SET workspace_id=$1 WHERE tenant_id=$2 AND conversation_id=$3")
            .bind(workspace.into_uuid()).bind(source.tenant_id.into_uuid())
            .bind(source.conversation_id.into_uuid()).execute(&pool).await.unwrap();
        let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(ObjectStoreBlobStore::in_memory()),
            running_loop::keyring(),
        ));
        let base = content
            .put(PutContent {
                tenant_id: source.tenant_id,
                media_type: MediaType::new("text/markdown").unwrap(),
                bytes: b"# Original\n".to_vec(),
            })
            .await
            .unwrap();
        let artifact_id = ArtifactId::generate();
        let node_id = ResourceNodeId::generate();
        ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
            .create_file(CreateResourceFile {
                context: source_context.clone(),
                identity: mutation("conversation-file"),
                expected_revision: ResourceRevision::ZERO,
                node_id,
                parent_node_id: None,
                name: name("report.md"),
                sort_key: "report.md".into(),
                artifact: Artifact {
                    artifact_id,
                    tenant_id: source.tenant_id,
                    source: ArtifactSource::Conversation(source.conversation_id),
                    origin: ArtifactOrigin::Product,
                    kind: ArtifactKind::File,
                    lifecycle: ArtifactLifecycle::Draft,
                    current_version: VersionNumber::new(1).unwrap(),
                    title: "report.md".into(),
                    created_at: now,
                    updated_at: now,
                },
                version: ArtifactVersion {
                    artifact_version_id: ArtifactVersionId::generate(),
                    artifact_id,
                    version: VersionNumber::new(1).unwrap(),
                    content_hash: base.sha256,
                    content_ref: base,
                    dependency_refs: vec![],
                    source_invocation_id: None,
                    committed_by: ArtifactVersionAuthor::Participant(source.requester_id),
                    committed_at: now,
                },
            })
            .await
            .unwrap();
        let mut context = source_context;
        context.source = ResourceCommandSource::Workspace {
            workspace_id: workspace,
        };
        Self {
            _postgres: postgres,
            pool,
            source,
            content,
            artifact_id,
            context,
            node_id,
        }
    }
    fn edits(&self) -> FileEditApplication<PostgresResourceStore> {
        FileEditApplication::new(
            PostgresResourceStore::new(self.pool.clone()),
            self.content.clone(),
        )
    }
    fn begin(&self) -> BeginFileEdit {
        BeginFileEdit {
            context: self.context.clone(),
            session_id: ResourceEditSessionId::generate(),
            file_node_id: self.node_id,
            base_version: VersionNumber::new(1).unwrap(),
            format_adapter: VersionedKey::new("text.utf8@1").unwrap(),
            idempotency_key: IdempotencyKey::new("workspace-principal-begin").unwrap(),
            command_fingerprint: Sha256::digest(b"workspace-principal-begin"),
            expires_at: UtcTimestamp::from_unix_millis(self.context.now.unix_millis() + 60_000),
        }
    }
    async fn prepare(&self) -> FinalizeFileEdit {
        let edits = self.edits();
        let begin = self.begin();
        let opened = edits.begin(begin.clone()).await.unwrap();
        let replay = edits.begin(begin.clone()).await.unwrap();
        assert_eq!(opened.session.session_id, replay.session.session_id);
        let apply = ApplyFileEdits {
            context: self.context.clone(),
            session_id: begin.session_id,
            operation_id: ResourceEditOperationId::generate(),
            idempotency_key: IdempotencyKey::new("workspace-principal-apply").unwrap(),
            expected_session_revision: ResourceRevision::ZERO,
            expected_sequence: 1,
            operations: vec![TextEditOperation::Replace {
                range: TextRange {
                    start_byte: 0,
                    end_byte: 11,
                },
                expected_hash: Sha256::digest(b"# Original\n"),
                text: "# User edit\n".into(),
            }],
        };
        let checkpoint = edits.apply(apply.clone()).await.unwrap();
        assert!(edits.apply(apply).await.unwrap().duplicate);
        FinalizeFileEdit {
            context: self.context.clone(),
            command_id: ResourceEditTerminalCommandId::generate(),
            session_id: begin.session_id,
            artifact_version_id: ArtifactVersionId::generate(),
            committed_by: ArtifactVersionAuthor::ProductPrincipal(self.source.principal_id),
            idempotency_key: IdempotencyKey::new("workspace-principal-finalize").unwrap(),
            expected_session_revision: checkpoint.session_revision,
        }
    }
    async fn version_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id=$1")
            .bind(self.artifact_id.into_uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn workspace_principal_edits_conversation_file_and_replays_without_rewriting_provenance() {
    let f = Fixture::new().await;
    let finalize = f.prepare().await;
    let edits = f.edits();
    let receipt = edits
        .finalize(finalize.clone())
        .await
        .expect("project user can edit a conversation File");
    assert_eq!(receipt.state, EditSessionState::Committed);
    assert_eq!(receipt.file_version, Some(VersionNumber::new(2).unwrap()));
    let replay = edits.finalize(finalize.clone()).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(receipt.artifact_version_id, replay.artifact_version_id);
    assert_eq!(f.version_count().await, 2);
    // Product previews read the exact same committed head as project edits,
    // including files originally created in a Conversation.
    use musteragent_runtime_file_query::{ConversationFileVersions, FileQueryContext};
    let query_context = FileQueryContext {
        dialogue_source: None,
        actor: f.context.actor,
        conversation_id: Some(f.source.conversation_id),
        project_workspace_id: f.context.workspace_id(),
        now: f.context.now,
        authority_revision: "test-current".into(),
        content_allowlist: None,
    };
    let versions = PostgresResourceStore::new(f.pool.clone())
        .current_versions(&query_context, vec![f.artifact_id, ArtifactId::generate()])
        .await
        .unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].artifact_id, f.artifact_id);
    assert_eq!(versions[0].version.get(), 2);
    assert_eq!(
        f.content
            .read(f.source.tenant_id, &versions[0].content_ref)
            .await
            .unwrap(),
        b"# User edit\n"
    );
    let mut wrong_scope = query_context.clone();
    wrong_scope.project_workspace_id = Some(WorkspaceId::generate());
    assert!(
        PostgresResourceStore::new(f.pool.clone())
            .current_versions(&wrong_scope, vec![f.artifact_id])
            .await
            .unwrap()
            .is_empty()
    );
    let row = sqlx::query("SELECT a.conversation_id,a.workspace_id,a.current_version::text, v.committed_by,v.committed_by_principal_id,v.source_invocation_id,v.content_id FROM artifact.artifacts a JOIN artifact.artifact_versions v USING(artifact_id) WHERE a.artifact_id=$1 AND v.version=2")
        .bind(f.artifact_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(
        row.get::<uuid::Uuid, _>("conversation_id"),
        f.source.conversation_id.into_uuid()
    );
    assert!(row.get::<Option<uuid::Uuid>, _>("workspace_id").is_none());
    assert!(row.get::<Option<uuid::Uuid>, _>("committed_by").is_none());
    assert!(
        row.get::<Option<uuid::Uuid>, _>("source_invocation_id")
            .is_none()
    );
    assert_eq!(
        row.get::<uuid::Uuid, _>("committed_by_principal_id"),
        f.source.principal_id.into_uuid()
    );
    let stored = sqlx::query("SELECT committed_by, committed_by_principal_id FROM artifact.artifact_versions WHERE artifact_id=$1 AND version=1")
        .bind(f.artifact_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(
        stored.get::<uuid::Uuid, _>("committed_by"),
        f.source.requester_id.into_uuid()
    );
    assert!(
        stored
            .get::<Option<uuid::Uuid>, _>("committed_by_principal_id")
            .is_none()
    );
    let loaded = edits
        .begin(BeginFileEdit {
            base_version: VersionNumber::new(2).unwrap(),
            session_id: ResourceEditSessionId::generate(),
            idempotency_key: IdempotencyKey::new("next-version").unwrap(),
            command_fingerprint: Sha256::digest(b"next-version"),
            ..f.begin()
        })
        .await
        .unwrap();
    assert_eq!(
        f.content
            .read(f.source.tenant_id, &loaded.checkpoint_ref)
            .await
            .unwrap(),
        b"# User edit\n"
    );
}

#[tokio::test]
async fn workspace_principal_retries_same_checkpoint_after_author_constraint_migration() {
    let f = Fixture::new().await;
    let finalize = f.prepare().await;
    let original: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('artifact.validate_workspace_version_author()'::regprocedure)",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    sqlx::raw_sql("CREATE OR REPLACE FUNCTION artifact.validate_workspace_version_author() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected author constraint'; END $$")
        .execute(&f.pool).await.unwrap();
    let rejected = sqlx::query("INSERT INTO artifact.artifact_versions (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by_principal_id,committed_at_ms) SELECT $1,artifact_id,2,content_id,content_hash,$2,1 FROM artifact.artifact_versions WHERE artifact_id=$3 AND version=1")
        .bind(ArtifactVersionId::generate().into_uuid())
        .bind(f.source.principal_id.into_uuid())
        .bind(f.artifact_id.into_uuid())
        .execute(&f.pool).await.unwrap_err();
    assert_eq!(
        rejected.as_database_error().unwrap().code().as_deref(),
        Some("P0001")
    );
    assert_eq!(
        f.edits().finalize(finalize.clone()).await.unwrap_err(),
        FileEditError::Corrupt,
        "A deterministic trigger invariant is not a transient 503"
    );
    assert_eq!(f.version_count().await, 1);
    let state: i16 =
        sqlx::query_scalar("SELECT state FROM resource.edit_sessions WHERE session_id=$1")
            .bind(finalize.session_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(state, 1, "The failed commit must leave its checkpoint open");
    sqlx::raw_sql(&original).execute(&f.pool).await.unwrap();
    let receipt = f.edits().finalize(finalize.clone()).await.unwrap();
    assert_eq!(receipt.state, EditSessionState::Committed);
    assert_eq!(receipt.file_version, Some(VersionNumber::new(2).unwrap()));
    assert!(f.edits().finalize(finalize).await.unwrap().duplicate);
    assert_eq!(f.version_count().await, 2);
}

#[tokio::test]
async fn workspace_principal_cannot_reuse_edit_handles_with_another_owner_tenant_or_project() {
    let f = Fixture::new().await;
    let finalize = f.prepare().await;
    for mode in ["owner", "tenant", "project"] {
        let mut invalid = finalize.clone();
        invalid.command_id = ResourceEditTerminalCommandId::generate();
        invalid.idempotency_key = IdempotencyKey::new(format!("forged-{mode}")).unwrap();
        match mode {
            "owner" => {
                invalid.context.actor.principal_id = PrincipalId::generate();
                invalid.committed_by =
                    ArtifactVersionAuthor::ProductPrincipal(invalid.context.actor.principal_id);
            }
            "tenant" => {
                invalid.context.tenant_id = TenantId::generate();
                invalid.context.actor.tenant_id = invalid.context.tenant_id;
            }
            _ => {
                let other = WorkspaceId::generate();
                PostgresWorkspaceResourceRootAuthority::new(f.pool.clone())
                    .ensure_root(EnsureWorkspaceResourceRoot {
                        actor: invalid.context.actor,
                        workspace_id: other,
                    })
                    .await
                    .unwrap();
                invalid.context.source = ResourceCommandSource::Workspace {
                    workspace_id: other,
                };
            }
        }
        let error = f.edits().finalize(invalid).await.unwrap_err();
        assert!(
            matches!(
                error,
                FileEditError::SessionConflict
                    | FileEditError::SessionUnavailable
                    | FileEditError::FileUnavailable
            ),
            "{mode}: {error:?}"
        );
        assert_eq!(f.version_count().await, 1);
    }
    assert_eq!(
        f.edits().finalize(finalize).await.unwrap().state,
        EditSessionState::Committed
    );
}

#[tokio::test]
async fn workspace_principal_cannot_finalize_after_project_file_was_trashed() {
    let f = Fixture::new().await;
    let finalize = f.prepare().await;
    ResourceApplication::new(PostgresResourceStore::new(f.pool.clone()))
        .trash_node(TrashResourceNode {
            context: f.context.clone(),
            identity: mutation("trash-before-save"),
            expected_revision: ResourceRevision::new(1),
            node_id: f.node_id,
        })
        .await
        .unwrap();
    let error = f.edits().finalize(finalize).await.unwrap_err();
    assert!(
        matches!(
            error,
            FileEditError::SessionConflict | FileEditError::FileUnavailable
        ),
        "{error:?}"
    );
    assert_eq!(f.version_count().await, 1);
}

#[tokio::test]
async fn workspace_principal_edit_waits_for_current_workspace_authority() {
    let f = Fixture::new().await;
    let finalize = f.prepare().await;
    let workspace = f.context.workspace_id().unwrap();
    sqlx::query(
        "UPDATE resource.workspace_authority SET status=3,ready_at_ms=NULL WHERE tenant_id=$1 AND workspace_id=$2",
    )
    .bind(f.source.tenant_id.into_uuid())
    .bind(workspace.into_uuid())
    .execute(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        f.edits().finalize(finalize.clone()).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    assert_eq!(f.version_count().await, 1);
    sqlx::query(
        "UPDATE resource.workspace_authority SET status=2,ready_at_ms=created_at_ms WHERE tenant_id=$1 AND workspace_id=$2",
    )
    .bind(f.source.tenant_id.into_uuid())
    .bind(workspace.into_uuid())
    .execute(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        f.edits().finalize(finalize).await.unwrap().state,
        EditSessionState::Committed
    );
}

#[tokio::test]
async fn workspace_principal_author_cannot_be_applied_to_an_unbound_non_file_artifact() {
    let f = Fixture::new().await;
    let artifact = ArtifactId::generate();
    sqlx::query("INSERT INTO artifact.artifacts (artifact_id,tenant_id,conversation_id,origin,kind,lifecycle,current_version,title,created_at_ms,updated_at_ms) VALUES ($1,$2,$3,3,1,1,1,'Note manifest',1,1)")
        .bind(artifact.into_uuid()).bind(f.source.tenant_id.into_uuid()).bind(f.source.conversation_id.into_uuid()).execute(&f.pool).await.unwrap();
    let denied = sqlx::query("INSERT INTO artifact.artifact_versions (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by_principal_id,committed_at_ms) SELECT $1,$2,1,content_id,content_hash,$3,1 FROM artifact.artifact_versions WHERE artifact_id=$4 AND version=1")
        .bind(ArtifactVersionId::generate().into_uuid()).bind(artifact.into_uuid()).bind(f.source.principal_id.into_uuid())
        .bind(f.artifact_id.into_uuid()).execute(&f.pool).await;
    assert!(
        denied.is_err(),
        "A Workspace principal is not a general Conversation Artifact author"
    );
}

/// Wait for a PostgreSQL-observed blocking edge, not a guessed scheduling delay.
/// Every caller arranges exactly one new waiter for the supplied backend.
async fn blocked_backend(pool: &PgPool, blocker: i32, stage: &str) -> i32 {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(pid) = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
            )
            .bind(blocker)
            .fetch_optional(pool)
            .await
            .unwrap()
            {
                return pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("database lock barrier not reached: {stage}, blocker {blocker}"))
}

#[tokio::test]
async fn workspace_principal_inflight_replay_survives_trash_after_original_commit() {
    let f = Fixture::new().await;
    let finalize = f.prepare().await;
    // Keep the fixture-only gate outside the four-connection application pool;
    // F1, F2, the source barrier and the lock observer use those four slots.
    let mut gate = f.pool.acquire().await.unwrap().detach();
    let gate_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut gate)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(772097)")
        .execute(&mut gate)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION artifact.pause_finalize_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(772097); RETURN NEW; END $$; CREATE TRIGGER pause_finalize_fixture BEFORE INSERT ON artifact.artifact_versions FOR EACH ROW WHEN (NEW.version = 2) EXECUTE FUNCTION artifact.pause_finalize_fixture();")
        .execute(&f.pool).await.unwrap();
    let first_edits = f.edits();
    let first_command = finalize.clone();
    let first = tokio::spawn(async move { first_edits.finalize(first_command).await });
    let first_pid = blocked_backend(&f.pool, gate_pid, "first finalize awaiting gate").await;

    // F1 already holds the source/node/session locks but has not committed its
    // receipt. Queue an exclusive source-table barrier, then F2's shared lock.
    // PostgreSQL's blocking graph proves F2 passed its first receipt lookup.
    // A table barrier is deliberate: a shared row lock can bypass a queued row
    // writer. EXCLUSIVE blocks FOR SHARE while allowing ordinary trigger reads.
    let mut source_barrier = f.pool.acquire().await.unwrap();
    let barrier_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *source_barrier)
        .await
        .unwrap();
    let (held_sender, held_receiver) = tokio::sync::oneshot::channel();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
    let barrier = tokio::spawn(async move {
        let mut tx = source_barrier.begin().await.unwrap();
        sqlx::query("LOCK TABLE conversation.conversations IN EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await
            .unwrap();
        held_sender.send(()).unwrap();
        release_receiver.await.unwrap();
        tx.commit().await.unwrap();
    });
    assert_eq!(
        blocked_backend(&f.pool, first_pid, "exclusive source barrier").await,
        barrier_pid
    );
    let second_edits = f.edits();
    let second_command = finalize.clone();
    let second = tokio::spawn(async move { second_edits.finalize(second_command).await });
    let second_pid = blocked_backend(&f.pool, barrier_pid, "replay awaiting source barrier").await;
    assert_ne!(second_pid, first_pid);

    sqlx::query("SELECT pg_advisory_unlock(772097)")
        .execute(&mut gate)
        .await
        .unwrap();
    let committed = first.await.unwrap().unwrap();
    held_receiver.await.unwrap();
    ResourceApplication::new(PostgresResourceStore::new(f.pool.clone()))
        .trash_node(TrashResourceNode {
            context: f.context.clone(),
            identity: mutation("trash-after-first-finalize"),
            expected_revision: ResourceRevision::new(1),
            node_id: f.node_id,
        })
        .await
        .unwrap();
    release_sender.send(()).unwrap();
    barrier.await.unwrap();
    let replay = second
        .await
        .unwrap()
        .expect("in-flight retry must recover the exact committed receipt");
    assert!(replay.duplicate);
    assert_eq!(replay.artifact_version_id, committed.artifact_version_id);
    assert_eq!(replay.file_version, Some(VersionNumber::new(2).unwrap()));
    assert_eq!(f.version_count().await, 2);

    // Receipt recovery is exact and retains the current principal authorization.
    let mut changed = finalize.clone();
    changed.artifact_version_id = ArtifactVersionId::generate();
    assert_eq!(
        f.edits().finalize(changed).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    let mut unauthorized = finalize;
    unauthorized.context.actor.principal_id = PrincipalId::generate();
    unauthorized.committed_by =
        ArtifactVersionAuthor::ProductPrincipal(unauthorized.context.actor.principal_id);
    assert_eq!(
        f.edits().finalize(unauthorized).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    assert_eq!(f.version_count().await, 2);
}
