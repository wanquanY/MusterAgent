mod support;
use musteragent_runtime_file_query::*;
use std::cell::Cell;
#[test]
fn immutable_metadata_paging_constructs_only_one_page_and_rejects_changed_or_foreign_cursors() {
    let fixture = support::Fixture::new();
    let context = &fixture.context;
    let constructed = Cell::new(0);
    let entry = |index: usize| {
        constructed.set(constructed.get() + 1);
        Ok(FileEntry {
            production: None,
            native_workspace_id: None,
            file_ref: FileReference {
                owned_agent_id: None,
                schema: FILE_REFERENCE_SCHEMA.into(),
                conversation_id: context.conversation_id.map(|id| id.to_string()),
                workspace_id: None,
                scope: FileScope::Project,
                source: FileSource::Hosted {
                    project_id: "10000000-0000-4000-8000-000000000001".into(),
                    path: format!("file-{index}.txt"),
                },
                revision: format!("v{index}"),
            },
            name: format!("file-{index}.txt"),
            is_directory: false,
            media_type: Some("text/plain".into()),
            byte_length: Some(1),
            modified_at_ms: 0,
            associated_at_ms: None,
            semantic_kind: None,
            availability: FileAvailability::Available,
        })
    };
    let mut query = ListFiles {
        produced_by: None,
        scope: FileScope::Project,
        parent: None,
        cursor: None,
        limit: 100,
        name_query: None,
    };
    let first = paginate_file_snapshot(context, &query, "original tree".into(), 0..100_000, entry)
        .expect("first page");
    assert_eq!(constructed.get(), 100);
    assert_eq!(first.entries.len(), 100);
    query.cursor = first.next_cursor;
    let second = paginate_file_snapshot(context, &query, "original tree".into(), 0..100_000, entry)
        .expect("next page");
    assert_eq!(second.entries[0].name, "file-100.txt");
    assert_eq!(constructed.get(), 200);
    assert_eq!(
        paginate_file_snapshot(context, &query, "new tree".into(), 0..100_000, entry)
            .expect_err("changed snapshot"),
        FileQueryError::Changed
    );
    assert_eq!(constructed.get(), 200);
    query.name_query = Some("other selection".into());
    assert_eq!(
        paginate_file_snapshot(context, &query, "original tree".into(), 0..100_000, entry)
            .expect_err("foreign cursor"),
        FileQueryError::InvalidQuery
    );
    assert_eq!(constructed.get(), 200);
}

#[tokio::test]
async fn async_snapshot_uses_same_cursor_and_resolves_only_selected_entries() {
    let fixture = support::Fixture::new();
    let context = &fixture.context;
    let calls = Cell::new(0);
    let entry = |i: usize| FileEntry {
        production: None,
        native_workspace_id: None,
        file_ref: FileReference {
            owned_agent_id: None,
            schema: FILE_REFERENCE_SCHEMA.into(),
            conversation_id: context.conversation_id.map(|id| id.to_string()),
            workspace_id: None,
            scope: FileScope::Project,
            source: FileSource::Hosted {
                project_id: "10000000-0000-4000-8000-000000000001".into(),
                path: format!("{i}.txt"),
            },
            revision: "v1".into(),
        },
        name: format!("{i}.txt"),
        is_directory: false,
        media_type: None,
        byte_length: None,
        modified_at_ms: 0,
        associated_at_ms: None,
        semantic_kind: None,
        availability: FileAvailability::Available,
    };
    let mut query = ListFiles {
        produced_by: None,
        scope: FileScope::Project,
        parent: None,
        cursor: None,
        limit: 3,
        name_query: None,
    };
    let sync = paginate_file_snapshot(context, &query, "complete".into(), 0..100, |i| Ok(entry(i)))
        .expect("sync page");
    let resolve = |i| {
        calls.set(calls.get() + 1);
        std::future::ready(Ok(entry(i)))
    };
    let first = paginate_file_snapshot_async(context, &query, "complete".into(), 0..100, resolve)
        .await
        .expect("async page");
    assert_eq!(first, sync);
    assert_eq!(calls.get(), 3);
    query.cursor = first.next_cursor;
    let second = paginate_file_snapshot_async(context, &query, "complete".into(), 0..100, resolve)
        .await
        .expect("continuation");
    assert_eq!(second.entries[0].name, "3.txt");
    assert_eq!(calls.get(), 6);
    assert_eq!(
        paginate_file_snapshot_async(context, &query, "changed".into(), 0..100, resolve)
            .await
            .expect_err("changed"),
        FileQueryError::Changed
    );
    assert_eq!(calls.get(), 6);
    query.name_query = Some("foreign".into());
    assert_eq!(
        paginate_file_snapshot_async(context, &query, "complete".into(), 0..100, resolve)
            .await
            .expect_err("foreign"),
        FileQueryError::InvalidQuery
    );
    assert_eq!(calls.get(), 6);
}

#[tokio::test]
async fn async_snapshot_resolution_failure_never_returns_a_partial_page() {
    let f = support::Fixture::new();
    let query = ListFiles {
        produced_by: None,
        scope: FileScope::Project,
        parent: None,
        cursor: None,
        limit: 3,
        name_query: None,
    };
    assert_eq!(
        paginate_file_snapshot_async(&f.context, &query, "snapshot".into(), 0..100, |_| {
            std::future::ready(Err(FileQueryError::Denied))
        })
        .await
        .expect_err("original authority rejected"),
        FileQueryError::Denied
    );
}
