use super::*;

fn producer(p: &Project) -> AgentFileProduction {
    AgentFileProduction {
        execution_home: FileExecutionHome::Native,
        agent_id: p
            .f
            .context
            .dialogue_source
            .as_ref()
            .unwrap()
            .agent_id
            .to_string(),
        dialogue_id: uuid::Uuid::now_v7().to_string(),
        intent_id: uuid::Uuid::now_v7().to_string(),
        task_id: None,
        run_id: RunId::generate().to_string(),
        invocation_id: InvocationId::generate().to_string(),
        committed_at_ms: 20,
    }
}
fn selected(origin: &AgentFileProduction) -> ListFiles {
    ListFiles {
        produced_by: Some(FileProductionFilter::Run {
            run_id: origin.run_id.clone(),
        }),
        ..dialogue_query()
    }
}

#[tokio::test]
async fn producer_selects_the_exact_current_version_not_the_initial_node_author() {
    let p = agent_files();
    let origin = producer(&p);
    let old_invocation = InvocationId::generate();
    {
        let mut current = p.store.0.lock().unwrap();
        current.node.provenance.invocation_id = Some(old_invocation);
        current.production = Some(origin.clone());
    }
    let page = p.app.list(&p.f.context, selected(&origin)).await.unwrap();
    assert_eq!(page.entries[0].production, Some(origin.clone()));
    assert_ne!(origin.invocation_id, old_invocation.to_string());
    let inspected = p
        .app
        .inspect(&p.f.context, &page.entries[0].file_ref)
        .await
        .unwrap();
    assert_eq!(inspected.production, Some(origin));
    assert_eq!(*p.f.bytes.reads.lock().unwrap(), 0);
}

#[tokio::test]
async fn producer_cursor_cannot_be_reused_for_another_run_or_unfiltered_query() {
    let p = agent_files();
    let origin = producer(&p);
    p.store.0.lock().unwrap().production = Some(origin.clone());
    let page = p.app.list(&p.f.context, selected(&origin)).await.unwrap();
    for mut query in [dialogue_query(), selected(&producer(&p))] {
        query.cursor.clone_from(&page.next_cursor);
        assert_eq!(
            p.app.list(&p.f.context, query).await.unwrap_err(),
            FileQueryError::InvalidQuery
        );
    }
    // Even the same producer advancing the current version invalidates the
    // source snapshot; names and the original node author cannot pin a page.
    *p.store.1.lock().unwrap() = Sha256::digest(b"new current version and producer");
    let mut query = selected(&origin);
    query.cursor = page.next_cursor;
    assert_eq!(
        p.app.list(&p.f.context, query).await.unwrap_err(),
        FileQueryError::Changed
    );
}

#[tokio::test]
async fn catalog_cannot_ignore_a_producer_filter_or_invent_cross_agent_provenance() {
    let p = agent_files();
    let origin = producer(&p);
    // The fixture intentionally returns a row without applying the selector.
    // The application boundary rejects the whole page instead of filtering it
    // after pagination or attributing the node's initial creator.
    for invalid in [None, Some(producer(&p))] {
        p.store.0.lock().unwrap().production = invalid;
        assert_eq!(
            p.app
                .list(&p.f.context, selected(&origin))
                .await
                .unwrap_err(),
            FileQueryError::Integrity
        );
    }
    let mut foreign = origin;
    foreign.agent_id = uuid::Uuid::now_v7().to_string();
    p.store.0.lock().unwrap().production = Some(foreign);
    assert_eq!(
        p.app
            .list(&p.f.context, dialogue_query())
            .await
            .unwrap_err(),
        FileQueryError::Integrity
    );
    p.store.0.lock().unwrap().production = None;
    assert!(
        p.app
            .list(&p.f.context, dialogue_query())
            .await
            .unwrap()
            .entries[0]
            .production
            .is_none()
    );
}

#[tokio::test]
async fn non_dialogue_storage_explicitly_rejects_producer_selection() {
    let p = Project::new();
    let query = ListFiles {
        produced_by: Some(FileProductionFilter::Run {
            run_id: RunId::generate().to_string(),
        }),
        ..project_query()
    };
    assert_eq!(
        p.app.list(&p.f.context, query).await.unwrap_err(),
        FileQueryError::Unsupported
    );
    assert_eq!(p.store.2.load(std::sync::atomic::Ordering::SeqCst), 0);
}

struct QueryAuthority(FileQueryContext);
impl FileQueryAuthority for QueryAuthority {
    fn resolve<'a>(
        &'a self,
        _: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

#[tokio::test]
async fn old_tool_contracts_project_production_and_reject_even_null_new_input() {
    for version in [3, 4, 5] {
        let mut p = agent_files();
        let origin = producer(&p);
        p.store.0.lock().unwrap().production = Some(origin.clone());
        let key = format!("files.list@{version}");
        let versioned_key = VersionedKey::new(&key).unwrap();
        p.f.context.dialogue_source.as_mut().unwrap().tool_key = versioned_key.clone();
        p.authority.0.lock().unwrap().tool_key = Some(versioned_key);
        let access = FileQueryAccess {
            actor: p.f.context.actor,
            conversation_id: p.f.context.conversation_id,
            workspace_id: None,
            run: None,
        };
        let service = FileQueryService::new(Arc::new(QueryAuthority(p.f.context)), Arc::new(p.app));
        let (page, _) = service
            .execute_tool(&access, &key, br#"{"scope":"owned_agent"}"#)
            .await
            .unwrap();
        assert_eq!(page["entries"][0].get("production").is_some(), version == 5);
        let inspection =
            serde_json::to_vec(&serde_json::json!({"file_ref":page["entries"][0]["file_ref"]}))
                .unwrap();
        let (entry, _) = service
            .execute_tool(&access, &format!("files.inspect@{version}"), &inspection)
            .await
            .unwrap();
        assert_eq!(entry.get("production").is_some(), version == 5);
        if version == 5 {
            // Frozen v5 output schemas are identical on Cloud and Device.
            // Keep testing those contracts without retaining a product release.
            for (tool, output, schema) in [
                (
                    "files.list@5",
                    &page,
                    include_str!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../../../contracts/fixtures/files-list-v5-output.schema.json"
                    )),
                ),
                (
                    "files.inspect@5",
                    &entry,
                    include_str!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../../../contracts/fixtures/files-inspect-v5-output.schema.json"
                    )),
                ),
            ] {
                let schema: serde_json::Value = serde_json::from_str(schema).unwrap();
                let validator = jsonschema::validator_for(&schema).unwrap();
                let errors = validator
                    .iter_errors(output)
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>();
                assert!(errors.is_empty(), "{tool}: {errors:?}");
            }
        }
        let (read, _) = service
            .execute_tool(
                &access,
                &format!("files.read@{}", version.min(4) + 1),
                &inspection,
            )
            .await
            .unwrap();
        assert!(
            read["view"]["entry"].get("production").is_none(),
            "read contracts are unchanged"
        );
        if version < 5 {
            for extra in [
                serde_json::Value::Null,
                serde_json::json!({"kind":"run","run_id":origin.run_id}),
            ] {
                let input = serde_json::to_vec(
                    &serde_json::json!({"scope":"owned_agent","produced_by":extra}),
                )
                .unwrap();
                assert_eq!(
                    service
                        .execute_tool(&access, &key, &input)
                        .await
                        .unwrap_err(),
                    FileQueryError::InvalidQuery
                );
            }
        }
    }
}
