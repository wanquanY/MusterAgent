use super::*;

#[path = "production.rs"]
mod production;

fn agent_files() -> Project {
    let mut p = Project::new();
    let agent_id = AgentId::from_uuid(uuid::Uuid::now_v7()).expect("Agent identity");
    let invocation_id = InvocationId::generate();
    let tool_key = VersionedKey::new("files.list@3").expect("released key");
    p.f.context.project_workspace_id = None;
    p.f.context.dialogue_source = Some(AgentFileQuerySource {
        input_view: None,
        agent_id,
        invocation_id,
        tool_key: tool_key.clone(),
    });
    {
        let mut resource = p.authority.0.lock().expect("authority");
        resource.source = ResourceCommandSource::DialogueExecution {
            conversation_id: p.f.context.require_conversation().expect("private context"),
            agent_id,
        };
        resource.invocation_id = Some(invocation_id);
        resource.tool_key = Some(tool_key);
        p.store.0.lock().expect("store").node.scope = resource.scope();
    }
    p.app = FileQueryApplication::new(
        p.f.catalog.clone(),
        p.f.catalog.clone(),
        Arc::new(ContentFileReader::new(p.f.bytes.clone())),
    )
    .with_dialogue_catalog(Arc::new(ManagedResourceFileCatalog::for_owned_agent(
        p.authority.clone(),
        p.store.clone(),
    )));
    p
}
fn dialogue_query() -> ListFiles {
    ListFiles {
        scope: FileScope::OwnedAgent,
        ..query()
    }
}

#[tokio::test]
async fn agent_file_identity_and_cursor_survive_a_new_authorized_task_context() {
    let mut p = agent_files();
    let first = p
        .app
        .list(&p.f.context, dialogue_query())
        .await
        .expect("first task");
    assert_eq!(first.coverage, FileCoverage::OwnedAgentResources);
    let reference = first.entries[0].file_ref.clone();
    assert_eq!(reference.schema, AGENT_FILE_REFERENCE_SCHEMA);
    assert!(reference.conversation_id.is_none() && reference.workspace_id.is_none());
    let next_conversation = ConversationId::generate();
    let next_invocation = InvocationId::generate();
    p.f.context.conversation_id = Some(next_conversation);
    p.f.context
        .dialogue_source
        .as_mut()
        .expect("agent_files")
        .invocation_id = next_invocation;
    {
        let mut owner = p.authority.0.lock().expect("authority");
        owner.source = ResourceCommandSource::DialogueExecution {
            conversation_id: next_conversation,
            agent_id: p
                .f
                .context
                .dialogue_source
                .as_ref()
                .expect("agent_files")
                .agent_id,
        };
        owner.invocation_id = Some(next_invocation);
    }
    let next = p
        .app
        .list(
            &p.f.context,
            ListFiles {
                cursor: first.next_cursor,
                ..dialogue_query()
            },
        )
        .await
        .expect("continue stable directory in another task");
    assert_eq!(next.entries[0].file_ref, reference);
    let result = p
        .app
        .read(&p.f.context, read(reference))
        .await
        .expect("read original file");
    assert_eq!(result.view.text.as_deref(), Some("project content"));
}

#[tokio::test]
async fn dialogue_references_cannot_select_another_agent_or_a_public_conversation() {
    let p = agent_files();
    let reference = p
        .app
        .list(&p.f.context, dialogue_query())
        .await
        .expect("list")
        .entries[0]
        .file_ref
        .clone();
    let mut other = reference.clone();
    other.owned_agent_id = Some(
        AgentId::from_uuid(uuid::Uuid::now_v7())
            .expect("other Agent")
            .to_string(),
    );
    assert_eq!(
        p.app
            .inspect(&p.f.context, &other)
            .await
            .expect_err("other Agent"),
        FileQueryError::InvalidReference
    );
    other = reference.clone();
    other.conversation_id = p.f.context.conversation_id.map(|id| id.to_string());
    assert_eq!(
        p.app
            .inspect(&p.f.context, &other)
            .await
            .expect_err("mixed identity"),
        FileQueryError::InvalidReference
    );
    let mut public = p.f.context.clone();
    public.dialogue_source = None;
    assert_eq!(
        p.app
            .inspect(&public, &reference)
            .await
            .expect_err("ordinary context"),
        FileQueryError::InvalidReference
    );
    p.authority.0.lock().expect("authority").invocation_id = Some(InvocationId::generate());
    assert_eq!(
        p.app
            .inspect(&p.f.context, &reference)
            .await
            .expect_err("revoked invocation"),
        FileQueryError::Denied
    );
    assert_eq!(*p.f.bytes.reads.lock().expect("reads"), 0);
}

#[tokio::test]
async fn agent_file_content_update_invalidates_only_the_exact_revision() {
    let p = agent_files();
    let old = p
        .app
        .list(&p.f.context, dialogue_query())
        .await
        .expect("list")
        .entries[0]
        .file_ref
        .clone();
    let updated = p.f.bytes.add("text/plain", b"updated content");
    {
        let mut store = p.store.0.lock().expect("store");
        let file = store.file.as_mut().expect("file");
        file.artifact_version_id = ArtifactVersionId::generate();
        file.content_ref = updated;
    }
    assert_eq!(
        p.app
            .read(&p.f.context, read(old.clone()))
            .await
            .expect_err("old observation"),
        FileQueryError::Changed
    );
    let current = p
        .app
        .refresh_reference(&p.f.context, &old)
        .await
        .expect("explicit refresh");
    assert_eq!(current.file_ref.source, old.source);
    assert_eq!(current.file_ref.owned_agent_id, old.owned_agent_id);
    assert_ne!(current.file_ref.revision, old.revision);
    let read = p
        .app
        .read(&p.f.context, read(current.file_ref))
        .await
        .expect("current bytes");
    assert_eq!(read.view.text.as_deref(), Some("updated content"));
}

#[tokio::test]
async fn successor_tool_schemas_validate_actual_dialogue_entries_and_coverage() {
    use musteragent_tool::BuiltinToolCatalog;
    let p = agent_files();
    let list_input = serde_json::json!({"scope":"owned_agent"});
    let page = p
        .app
        .list(&p.f.context, dialogue_query())
        .await
        .expect("list");
    let reference = page.entries[0].file_ref.clone();
    let inspected = p
        .app
        .inspect(&p.f.context, &reference)
        .await
        .expect("inspect");
    let read_result = p
        .app
        .read(&p.f.context, read(reference.clone()))
        .await
        .expect("read");
    let file_input = serde_json::json!({"file_ref":reference});
    let search_input = serde_json::json!({"file_ref":reference, "query":"content"});
    let searched = p
        .app
        .search(
            &p.f.context,
            serde_json::from_value(search_input.clone()).expect("search query"),
        )
        .await
        .expect("search");
    use musteragent_tool::{ProductExecutionTarget, compiled_product_release};
    let publication = compiled_product_release(ProductId::KERNEL);
    for catalog in [
        ProductExecutionTarget::Cloud,
        ProductExecutionTarget::Device,
    ]
    .map(|target| BuiltinToolCatalog::for_product_release(&publication, target).unwrap())
    {
        for (operation, input, output) in [
            (
                "list",
                list_input.clone(),
                serde_json::to_value(&page).unwrap(),
            ),
            (
                "inspect",
                file_input.clone(),
                serde_json::to_value(&inspected).unwrap(),
            ),
            (
                "read",
                file_input.clone(),
                serde_json::json!({"view":read_result.view,"visual_content":[]}),
            ),
            (
                "search",
                search_input.clone(),
                serde_json::to_value(&searched).unwrap(),
            ),
        ] {
            let key = format!("files.{operation}");
            let definition = catalog
                .definitions()
                .iter()
                .find(|tool| tool.key.logical_key() == key)
                .expect("current release");
            for (schema, value) in [
                (&definition.input_schema_json, input),
                (&definition.output_schema_json, output),
            ] {
                let schema: serde_json::Value = serde_json::from_slice(schema).unwrap();
                let validator = jsonschema::validator_for(&schema).unwrap();
                let errors = validator
                    .iter_errors(&value)
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>();
                assert!(errors.is_empty(), "{key}: {errors:?}");
            }
        }
    }
}
