#![cfg(test)]

#[path = "support/file_events.rs"]
mod file_events;
#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_postgres::*;
use musteragent_resource::ResourceName;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_resource::*;
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use std::sync::Arc;

fn folder(context: &ResourceCommandContext, revision: u64, name: &str) -> CreateResourceFolder {
    let id = ResourceChangeSetId::generate();
    CreateResourceFolder {
        context: context.clone(),
        identity: ResourceMutationIdentity {
            change_set_id: id,
            idempotency_key: IdempotencyKey::new(id.to_string()).unwrap(),
            command_fingerprint: Sha256::digest(id.to_string()),
        },
        expected_revision: ResourceRevision::new(revision),
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: ResourceName::new(name).unwrap(),
        sort_key: name.into(),
    }
}

#[tokio::test]
async fn preparation_uses_original_fence_freezes_baseline_and_serializes_all_writers() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal =
        PostgresFencedToolInvocationJournal::new(pool.clone(), codec.clone(), codec, keys);
    let first = resource_support::start_resource_tool(
        &pool, &content, &journal, &attempt, fixture, "first",
    )
    .await;
    let second = resource_support::start_resource_tool(
        &pool, &content, &journal, &attempt, fixture, "second",
    )
    .await;
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let now = running_loop::database_now(&pool).await;
    let binding = PostgresExecutionWorkspaceAuthority::new(pool.clone())
        .ensure(EnsureExecutionWorkspace {
            actor,
            conversation_id: fixture.conversation_id,
            expected_branch_id: None,
            expected_home_epoch: HomeEpoch::new(1).unwrap(),
            now,
        })
        .await
        .unwrap()
        .binding;
    let context = ResourceCommandContext {
        tenant_id: actor.tenant_id,
        actor,
        source: ResourceCommandSource::ExecutionWorkspace(binding),
        invocation_id: None,
        tool_key: None,
        now,
    };
    let store = PostgresResourceStore::new(pool.clone());
    store
        .create_folder(&folder(&context, 0, "原始目录"))
        .await
        .unwrap();
    let mut request = PrepareExecutionWorkspace {
        context: ResourceCommandContext {
            invocation_id: Some(first),
            tool_key: Some(VersionedKey::new("resource.folder.create@1").unwrap()),
            ..context.clone()
        },
        attempt: attempt.clone(),
        request_fingerprint: Sha256::digest(b"immutable program+mounts"),
        maximum_entries: 100,
        maximum_total_bytes: 10000,
        cwd: None,
        environment: ExecutionWorkspaceEnvironmentChange::Keep,
        selection_ref: None,
        project: ExecutionWorkspaceProjectChange::Keep,
    };
    let (one, duplicate) = tokio::join!(
        store.prepare_execution(request.clone()),
        store.prepare_execution(request.clone())
    );
    let one = one.expect("original fenced preparation");
    assert_eq!(one, duplicate.expect("concurrent replay is identical"));
    assert_eq!(one.revision.get(), 1);
    assert_eq!(one.entries[0].path, "原始目录");
    assert_eq!(
        store
            .create_folder(&folder(&context, 1, "editor conflict"))
            .await
            .unwrap_err(),
        ResourceStoreError::WriterBusy,
        "unconfirmed execution is not a revision conflict"
    );
    let mut competing = request.clone();
    competing.context.invocation_id = Some(second);
    assert_eq!(
        store
            .prepare_execution(competing.clone())
            .await
            .unwrap_err(),
        ExecutionWorkspacePreparationError::WriterBusy
    );
    let mut changed = request.clone();
    changed.request_fingerprint = Sha256::digest(b"different program");
    assert_eq!(
        store.prepare_execution(changed).await.unwrap_err(),
        ExecutionWorkspacePreparationError::RequestConflict
    );
    // Simulate a Resource commit in the same invocation, then loss of its ACK.
    store
        .create_folder(&folder(&request.context, 1, "执行新增"))
        .await
        .unwrap();
    let reopened = PostgresResourceStore::new(pool.clone())
        .prepare_execution(request.clone())
        .await
        .unwrap();
    assert_eq!(
        one, reopened,
        "recovery cannot rebase onto the newly written tree"
    );
    request.attempt.fencing_token = FencingToken::new(attempt.fencing_token.get() + 1).unwrap();
    assert_eq!(
        store.prepare_execution(request.clone()).await.unwrap_err(),
        ExecutionWorkspacePreparationError::Unavailable
    );
    // A proven failed candidate releases writer qualification; the immutable baseline remains.
    sqlx::query(
        "UPDATE tool.tool_invocations SET status=5,finished_at_ms=$2 WHERE invocation_id=$1",
    )
    .bind(first.into_uuid())
    .bind(now.unix_millis())
    .execute(&pool)
    .await
    .unwrap();
    let next = store.prepare_execution(competing.clone()).await.unwrap();
    assert_eq!(next.entries.len(), 2);
    sqlx::query("UPDATE runtime.runs SET cancel_requested_at_ms=$2 WHERE run_id=$1")
        .bind(fixture.run_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.prepare_execution(competing).await.unwrap_err(),
        ExecutionWorkspacePreparationError::Unavailable
    );
    let retained: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resource.execution_workspace_snapshot_entries WHERE invocation_id=$1",
    )
    .bind(first.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        retained, 1,
        "cancellation does not delete recovery evidence"
    );
}

#[tokio::test]
async fn complete_workspace_changes_commit_atomically_and_keep_file_identity() {
    use musteragent_content_ref::MediaType;
    use musteragent_content_ref::SchemaRef;
    use musteragent_runtime_capability::CapabilityResult;
    use musteragent_runtime_content::PutContent;
    use std::collections::BTreeMap;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let keys = running_loop::keyring();
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        codec.clone(),
        codec.clone(),
        keys.clone(),
    );
    let committer = PostgresFencedResourceToolCommitter::new(pool.clone(), codec, keys);
    let store = PostgresResourceStore::new(pool.clone());
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let make_file = |path: &str, reference: &musteragent_content_ref::ContentRef| {
        let mut entry =
            WorkspaceManifestEntry::file(path.into(), "unused".into(), false, reference);
        entry.identity = None;
        entry
    };
    let text = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"original".to_vec(),
        })
        .await
        .unwrap();
    let edited = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"edited".to_vec(),
        })
        .await
        .unwrap();
    let mut first_identity = None;
    let mut previous_manifest = None;
    for round in 0..2 {
        let invocation = resource_support::start_resource_tool(
            &pool,
            &content,
            &journal,
            &attempt,
            fixture,
            &format!("round{round}"),
        )
        .await;
        let now = running_loop::database_now(&pool).await;
        let context = ResourceCommandContext {
            tenant_id: fixture.tenant_id,
            actor,
            source: ResourceCommandSource::Conversation {
                conversation_id: fixture.conversation_id,
                workspace_id: None,
            },
            invocation_id: Some(invocation),
            tool_key: Some(VersionedKey::new("resource.folder.create@1").unwrap()),
            now,
        };
        let snapshot = store
            .prepare_execution(PrepareExecutionWorkspace {
                context: context.clone(),
                attempt: attempt.clone(),
                request_fingerprint: Sha256::digest(format!("round{round}")),
                maximum_entries: 100,
                maximum_total_bytes: 100000,
                cwd: None,
                environment: ExecutionWorkspaceEnvironmentChange::Keep,
                selection_ref: None,
                project: ExecutionWorkspaceProjectChange::Keep,
            })
            .await
            .unwrap();
        let manifest = WorkspaceManifest::from_snapshot(&snapshot, snapshot.cwd.clone()).unwrap();
        let mut entries = manifest.entries.clone();
        let mut operations = vec![];
        let mut outputs = BTreeMap::new();
        if round == 0 {
            entries.push(WorkspaceManifestEntry::directory(
                "work/目录".into(),
                None,
                false,
            ));
            entries.push(WorkspaceManifestEntry::directory(
                "work/空目录".into(),
                None,
                false,
            ));
            for path in ["work/目录/a.txt", "work/目录/delete.txt", "work/b.txt"] {
                entries.push(make_file(path, &text));
                outputs.insert(path.into(), text.clone());
            }
        } else {
            first_identity = manifest
                .entries
                .iter()
                .find(|e| e.path == "work/目录/a.txt")
                .unwrap()
                .identity
                .clone();
            // Swap files through a temporary path, rename their parent, remove one
            // sibling, then replace file bytes through the usual temporary-file pattern.
            operations = vec![
                WorkspaceOperation::Rename {
                    from: "work/目录/a.txt".into(),
                    to: "work/tmp.txt".into(),
                },
                WorkspaceOperation::Rename {
                    from: "work/b.txt".into(),
                    to: "work/目录/a.txt".into(),
                },
                WorkspaceOperation::Rename {
                    from: "work/tmp.txt".into(),
                    to: "work/b.txt".into(),
                },
                WorkspaceOperation::Remove {
                    path: "work/目录/delete.txt".into(),
                },
                WorkspaceOperation::Rename {
                    from: "work/目录".into(),
                    to: "work/重命名".into(),
                },
                WorkspaceOperation::Replace {
                    path: "work/b.txt".into(),
                },
            ];
            entries.retain(|e| e.path != "work/目录/delete.txt");
            for entry in &mut entries {
                entry.path = match entry.path.as_str() {
                    "work/目录/a.txt" => "work/b.txt".into(),
                    "work/b.txt" => "work/重命名/a.txt".into(),
                    "work/目录" => "work/重命名".into(),
                    _ => entry.path.clone(),
                };
                if entry.path == "work/b.txt" {
                    entry.sha256 = Some(edited.sha256.to_string());
                    entry.byte_length = Some(edited.byte_length);
                }
            }
            entries.push(WorkspaceManifestEntry::directory(
                "work/new".into(),
                None,
                false,
            ));
            entries.push(make_file("work/new/out.txt", &edited));
            outputs.insert("work/b.txt".into(), edited.clone());
            outputs.insert("work/new/out.txt".into(), edited.clone());
        }
        let delta = WorkspaceDelta {
            schema_version: WORKSPACE_DELTA_VERSION.into(),
            base_revision: manifest.revision.clone(),
            execution: WorkspaceExecutionOutcome::Succeeded,
            entries,
            operations,
        };
        let private_context = ResourceCommandContext {
            source: ResourceCommandSource::ExecutionWorkspace(snapshot.binding),
            ..context
        };
        let changes = plan_execution_workspace_changes(
            private_context.clone(),
            running_loop::run_context(fixture),
            &snapshot,
            &manifest,
            &delta,
            &outputs,
        )
        .unwrap()
        .unwrap();
        let result_ref = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("application/json").unwrap(),
                bytes: br#"{"status":"committed"}"#.to_vec(),
            })
            .await
            .unwrap();
        let command = CommitPreparedResourceTool {
            run_context: running_loop::run_context(fixture),
            invocation_id: invocation,
            result: CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("resource.folder.create.output@1").unwrap(),
                    schema_hash: Sha256::digest(b"resource.folder.create.output.v1"),
                },
                provider_request_ref: None,
            },
            mutation: changes,
            committed_at: now,
        };
        if round == 1 {
            sqlx::raw_sql("CREATE FUNCTION tool.reject_workspace_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$; CREATE TRIGGER reject_workspace_result BEFORE INSERT ON tool.tool_results FOR EACH ROW EXECUTE FUNCTION tool.reject_workspace_result();").execute(&pool).await.unwrap();
            assert!(committer.commit(&attempt, command.clone()).await.is_err());
            let unchanged = store
                .read_tree(ReadExecutionWorkspaceTree {
                    context: private_context.clone(),
                    maximum_entries: 100,
                    maximum_total_bytes: 100000,
                })
                .await
                .unwrap();
            assert_eq!(unchanged.entries.len(), snapshot.entries.len());
            assert_eq!(unchanged.revision, snapshot.revision);
            assert!(
                unchanged
                    .entries
                    .iter()
                    .any(|e| e.path == "目录/delete.txt")
            );
            sqlx::raw_sql("DROP TRIGGER reject_workspace_result ON tool.tool_results; DROP FUNCTION tool.reject_workspace_result();").execute(&pool).await.unwrap();
        }
        let committed = committer.commit(&attempt, command.clone()).await.unwrap();
        assert!(!committed.duplicate);
        let duplicate = committer.commit(&attempt, command).await.unwrap();
        assert!(duplicate.duplicate);
        let tree = store
            .read_tree(ReadExecutionWorkspaceTree {
                context: private_context,
                maximum_entries: 100,
                maximum_total_bytes: 100000,
            })
            .await
            .unwrap();
        assert_eq!(tree.entries.len(), delta.entries.len() - 2);
        if round == 1 {
            let file = tree
                .entries
                .iter()
                .find(|e| e.path == "b.txt")
                .unwrap()
                .resource
                .file
                .as_ref()
                .unwrap();
            assert_eq!(file.node_id.to_string(), first_identity.clone().unwrap());
            assert_eq!(file.version.get(), 2);
            assert_eq!(file.content_ref.sha256, edited.sha256);
            assert!(
                !tree
                    .entries
                    .iter()
                    .any(|e| e.path.starts_with("目录") || e.path.contains(".mw-"))
            );
        }
        // FileQuery enumerates the identical committed Resource tree and pins
        // all relative dependencies to one exact revision for preview/export.
        use musteragent_runtime_file_query::{
            FileCatalog, FileQueryContext, FileScope, FileSource, ManagedResourceFileCatalog,
        };
        let catalog = ManagedResourceFileCatalog::execution_workspace(
            Arc::new(PostgresResourceContextAuthority::new(pool.clone())),
            Arc::new(PostgresResourceStore::new(pool.clone())),
        );
        let query_context = FileQueryContext {
            dialogue_source: None,
            actor,
            conversation_id: Some(fixture.conversation_id),
            now,
            authority_revision: "1".into(),
            content_allowlist: None,
            project_workspace_id: None,
        };
        let selected = tree
            .entries
            .iter()
            .find(|entry| entry.path == "b.txt")
            .unwrap();
        let file = selected.resource.file.as_ref().unwrap();
        let listed = catalog
            .list_subtree(
                &query_context,
                musteragent_runtime_file_query::ListFiles {
                    produced_by: None,
                    scope: FileScope::Work,
                    parent: None,
                    name_query: None,
                    cursor: None,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        let reference = listed
            .entries
            .iter()
            .find(|entry| {
                entry.file_ref.source
                    == FileSource::Resource {
                        node_id: file.node_id.to_string(),
                    }
            })
            .unwrap()
            .file_ref
            .clone();
        let exact = catalog
            .file_tree(&query_context, &reference)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact.entrypoint, "b.txt");
        assert_eq!(
            exact.files.len(),
            tree.entries
                .iter()
                .filter(|e| e.resource.file.is_some())
                .count()
        );
        assert!(
            exact
                .files
                .iter()
                .all(|e| e.file_ref.scope == FileScope::Work)
        );
        let mut outsider = query_context.clone();
        outsider.actor.principal_id = PrincipalId::generate();
        assert!(
            catalog.file_tree(&outsider, &reference).await.is_err(),
            "private working content remains private"
        );
        previous_manifest = Some(manifest);
    }
    assert!(first_identity.is_some());
    assert!(previous_manifest.is_some());
}

#[tokio::test]
async fn selected_project_subtree_and_private_files_share_one_atomic_commit() {
    use musteragent_content_ref::{MediaType, SchemaRef};
    use musteragent_runtime_capability::CapabilityResult;
    use musteragent_runtime_content::PutContent;
    use std::collections::BTreeMap;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let project = WorkspaceId::generate();
    sqlx::query("UPDATE conversation.conversations SET workspace_id=$2 WHERE conversation_id=$1")
        .bind(fixture.conversation_id.into_uuid())
        .bind(project.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let keys = running_loop::keyring();
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        codec.clone(),
        codec.clone(),
        keys.clone(),
    );
    let committer = PostgresFencedResourceToolCommitter::new(pool.clone(), codec, keys);
    let invocation = resource_support::start_resource_tool_with_contract(
        &pool,
        &content,
        &journal,
        &attempt,
        fixture,
        "project",
        "data.python.run@11",
        br#"{"code":"pass"}"#.to_vec(),
    )
    .await;
    let now = running_loop::database_now(&pool).await;
    let context = ResourceCommandContext {
        tenant_id: fixture.tenant_id,
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        source: ResourceCommandSource::Conversation {
            conversation_id: fixture.conversation_id,
            workspace_id: Some(project),
        },
        invocation_id: None,
        tool_key: None,
        now,
    };
    let store = PostgresResourceStore::new(pool.clone());
    let selected = store
        .create_folder(&folder(&context, 0, "selected"))
        .await
        .unwrap();
    store
        .create_folder(&folder(&context, 1, "not-mounted"))
        .await
        .unwrap();
    let request = PrepareExecutionWorkspace {
        context: ResourceCommandContext {
            invocation_id: Some(invocation),
            tool_key: Some(VersionedKey::new("data.python.run@11").unwrap()),
            ..context.clone()
        },
        attempt: attempt.clone(),
        request_fingerprint: Sha256::digest(b"project"),
        maximum_entries: 100,
        maximum_total_bytes: 100000,
        cwd: Some("project".into()),
        environment: ExecutionWorkspaceEnvironmentChange::Keep,
        selection_ref: None,
        project: ExecutionWorkspaceProjectChange::Select {
            root_node_id: Some(selected.node.node_id),
            writable: true,
        },
    };
    let snapshot = store.prepare_execution(request.clone()).await.unwrap();
    assert!(snapshot.project.as_ref().unwrap().entries.is_empty());
    let baseline = WorkspaceManifest::from_snapshot(&snapshot, snapshot.cwd.clone()).unwrap();
    baseline.validate().unwrap();
    assert!(
        !baseline
            .entries
            .iter()
            .any(|e| e.path.contains("not-mounted"))
    );
    let body = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"both scopes".to_vec(),
        })
        .await
        .unwrap();
    let mut entries = baseline.entries.clone();
    let mut outputs = BTreeMap::new();
    for path in ["work/private.txt", "project/shared.txt"] {
        let mut entry = WorkspaceManifestEntry::file(path.into(), "new".into(), false, &body);
        entry.identity = None;
        entries.push(entry);
        outputs.insert(path.into(), body.clone());
    }
    let delta = WorkspaceDelta {
        schema_version: WORKSPACE_DELTA_VERSION.into(),
        base_revision: baseline.revision.clone(),
        execution: WorkspaceExecutionOutcome::Succeeded,
        entries,
        operations: vec![],
    };
    let private = ResourceCommandContext {
        source: ResourceCommandSource::ExecutionWorkspace(snapshot.binding),
        ..request.context.clone()
    };
    let mutation = plan_execution_workspace_changes(
        private.clone(),
        running_loop::run_context(fixture),
        &snapshot,
        &baseline,
        &delta,
        &outputs,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        mutation,
        PreparedResourceMutation::ChangeSets { .. }
    ));
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").unwrap(),
            bytes: br#"{"status":"committed"}"#.to_vec(),
        })
        .await
        .unwrap();
    let command = CommitPreparedResourceTool {
        run_context: running_loop::run_context(fixture),
        invocation_id: invocation,
        mutation,
        committed_at: now,
        result: CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("data.python.run.output@11").unwrap(),
                schema_hash: Sha256::digest(b"result"),
            },
            provider_request_ref: None,
        },
    };
    sqlx::raw_sql("CREATE FUNCTION tool.reject_workspace_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$; CREATE TRIGGER reject_workspace_result BEFORE INSERT ON tool.tool_results FOR EACH ROW EXECUTE FUNCTION tool.reject_workspace_result();").execute(&pool).await.unwrap();
    assert!(committer.commit(&attempt, command.clone()).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.nodes WHERE kind=2")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "both Resource scopes rolled back");
    sqlx::raw_sql("DROP TRIGGER reject_workspace_result ON tool.tool_results; DROP FUNCTION tool.reject_workspace_result();").execute(&pool).await.unwrap();
    committer.commit(&attempt, command.clone()).await.unwrap();
    assert!(committer.commit(&attempt, command).await.unwrap().duplicate);
    let placed: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT parent_node_id FROM resource.nodes WHERE name='shared.txt'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        placed,
        Some(selected.node.node_id.into_uuid()),
        "project writes stay under the selected root"
    );
    let tree = store
        .read_tree(ReadExecutionWorkspaceTree {
            context: private,
            maximum_entries: 100,
            maximum_total_bytes: 100000,
        })
        .await
        .unwrap();
    assert_eq!(tree.entries[0].path, "private.txt");
}
