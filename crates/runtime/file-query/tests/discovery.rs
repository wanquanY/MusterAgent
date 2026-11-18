mod support;
use musteragent_artifact::ArtifactKind;
use musteragent_identity::{ConversationId, VersionNumber};
use musteragent_projection::ProjectedMessageStatus;
use musteragent_runtime_file_query::*;
use support::*;

#[tokio::test]
async fn discovery_combines_effective_attachments_and_outputs_without_reading_bodies() {
    let f = Fixture::new();
    f.attachment("same.md", "text/markdown", b"input");
    f.artifact("same.md", ArtifactKind::File, "text/markdown", b"output");
    let page = f
        .application
        .list(&f.context, query())
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(page.entries.len(), 2);
    assert_ne!(
        page.entries[0].file_ref.source,
        page.entries[1].file_ref.source
    );
    assert_eq!(*f.bytes.reads.lock().expect("fixture lock"), 0);
    assert_eq!(page.coverage, FileCoverage::ProjectedConversation);
}
#[tokio::test]
async fn rewind_removes_attachment_and_old_reference_cannot_read_retained_bytes() {
    let f = Fixture::new();
    f.attachment("input.txt", "text/plain", b"private");
    let reference = f.first().await;
    f.projection
        .0
        .lock()
        .expect("fixture lock")
        .messages
        .clear();
    assert!(
        f.application
            .list(&f.context, query())
            .await
            .expect("valid fixture and authorized query")
            .entries
            .is_empty()
    );
    assert_eq!(
        f.application
            .read(&f.context, read(reference))
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::NotFound
    );
    assert_eq!(*f.bytes.reads.lock().expect("fixture lock"), 0);
}
#[tokio::test]
async fn effective_artifact_version_is_pinned_and_stale_reads_fail() {
    let f = Fixture::new();
    f.artifact("plan.md", ArtifactKind::File, "text/plain", b"version1");
    let reference = f.first().await;
    let changed = f.bytes.add("text/plain", b"version2");
    {
        let mut s = f.projection.0.lock().expect("fixture lock");
        s.artifacts[0].current_content_ref = changed;
        s.artifacts[0].current_version = VersionNumber::new(2).expect("valid fixture version");
    }
    let current = f
        .application
        .refresh_reference(&f.context, &reference)
        .await
        .expect("refresh the same artifact identity to its effective version");
    assert_eq!(current.file_ref.source, reference.source);
    assert_ne!(current.file_ref.revision, reference.revision);
    assert_eq!(
        f.application
            .inspect(&f.context, &reference)
            .await
            .expect_err("the prior artifact revision cannot be opened as current"),
        FileQueryError::Changed
    );
    assert!(
        f.application
            .inspect(&f.context, &current.file_ref)
            .await
            .is_ok()
    );
    assert_eq!(
        f.application
            .read(&f.context, read(reference))
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Changed
    );
}
#[tokio::test]
async fn page_cursor_rejects_rewind_grant_change_and_query_change() {
    let mut f = Fixture::new();
    for i in 0..3 {
        f.attachment(&format!("file{i}"), "text/plain", b"file");
    }
    let q = ListFiles {
        limit: 1,
        ..query()
    };
    let page = f
        .application
        .list(&f.context, q.clone())
        .await
        .expect("valid fixture and authorized query");
    let next = ListFiles {
        cursor: page.next_cursor,
        ..q
    };
    assert_eq!(
        f.application
            .list(&f.context, next.clone())
            .await
            .expect("valid fixture and authorized query")
            .entries
            .len(),
        1
    );
    assert_eq!(
        f.application
            .list(
                &f.context,
                ListFiles {
                    name_query: Some("file".into()),
                    ..next.clone()
                }
            )
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::InvalidQuery
    );
    f.context.authority_revision = "grant2".into();
    assert_eq!(
        f.application
            .list(&f.context, next.clone())
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::InvalidQuery
    );
    f.context.authority_revision = "grant-1".into();
    f.projection.0.lock().expect("fixture lock").messages.pop();
    assert_eq!(
        f.application
            .list(&f.context, next)
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Changed
    );
}
#[tokio::test]
async fn restricted_context_cannot_discover_or_resolve_omitted_content() {
    let mut f = Fixture::new();
    let allowed = f.attachment("allowed", "text/plain", b"a");
    f.attachment("private", "text/plain", b"b");
    let private = f
        .application
        .list(&f.context, query())
        .await
        .expect("valid fixture and authorized query")
        .entries
        .into_iter()
        .find(|e| e.name == "private")
        .expect("valid fixture and authorized query")
        .file_ref;
    f.context.content_allowlist = Some(vec![allowed]);
    let page = f
        .application
        .list(&f.context, query())
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.coverage, FileCoverage::RestrictedConversation);
    assert_eq!(
        f.application
            .inspect(&f.context, &private)
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::NotFound
    );
}
#[tokio::test]
async fn malformed_scope_and_cross_conversation_references_are_rejected_before_io() {
    let f = Fixture::new();
    f.attachment("file", "text/plain", b"file");
    let reference = f.first().await;
    let mut other = reference.clone();
    other.conversation_id = Some(ConversationId::generate().to_string());
    assert_eq!(
        f.application
            .inspect(&f.context, &other)
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::InvalidReference
    );
    other = reference;
    other.scope = FileScope::Project;
    assert_eq!(
        f.application
            .inspect(&f.context, &other)
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::InvalidReference
    );
    let page = f
        .application
        .list(
            &f.context,
            ListFiles {
                scope: FileScope::Project,
                ..query()
            },
        )
        .await;
    assert_eq!(
        page.expect_err("reject invalid or stale file access"),
        FileQueryError::Unavailable
    );
}
#[tokio::test]
async fn streaming_message_does_not_publish_uncommitted_attachments() {
    let f = Fixture::new();
    f.attachment("pending", "text/plain", b"input");
    f.projection.0.lock().expect("fixture lock").messages[0].status =
        ProjectedMessageStatus::Streaming;
    assert!(
        f.application
            .list(&f.context, query())
            .await
            .expect("valid fixture and authorized query")
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn failed_output_is_explicit_and_duplicate_message_relation_is_not_duplicated() {
    let f = Fixture::new();
    f.attachment("input", "text/plain", b"same");
    f.artifact(
        "failed output",
        ArtifactKind::File,
        "text/plain",
        b"partial",
    );
    {
        let mut p = f.projection.0.lock().expect("fixture lock");
        let duplicate = p.messages[0].attachment_refs[0].clone();
        p.messages[0].attachment_refs.push(duplicate);
        p.artifacts[0].lifecycle = musteragent_artifact::ArtifactLifecycle::Failed;
    }
    let page = f
        .application
        .list(&f.context, query())
        .await
        .expect("list source states");
    assert_eq!(page.entries.len(), 2);
    assert!(
        page.entries
            .iter()
            .any(|e| e.availability == FileAvailability::Failed)
    );
}

#[tokio::test]
async fn allowlist_order_does_not_change_a_permission_bound_cursor() {
    let mut f = Fixture::new();
    let one = f.attachment("one", "text/plain", b"one");
    let two = f.attachment("two", "text/plain", b"two");
    f.context.content_allowlist = Some(vec![one.clone(), two.clone()]);
    let q = ListFiles {
        limit: 1,
        ..query()
    };
    let page = f
        .application
        .list(&f.context, q.clone())
        .await
        .expect("first page");
    f.context.content_allowlist = Some(vec![two, one]);
    assert_eq!(
        f.application
            .list(
                &f.context,
                ListFiles {
                    cursor: page.next_cursor,
                    ..q
                }
            )
            .await
            .expect("equivalent grant")
            .entries
            .len(),
        1
    );
}
