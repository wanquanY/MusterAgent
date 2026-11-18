mod support;
use musteragent_runtime_file_query::*;
use support::*;
fn query(term: &str) -> SearchFiles {
    SearchFiles {
        traversal: FileSearchTraversal::Children,
        scope: Some(FileScope::Conversation),
        parent: None,
        file_ref: None,
        query: term.into(),
        cursor: None,
        limit: 2,
    }
}
#[tokio::test]
async fn utf8_matches_crossing_windows_have_exact_offsets_and_no_duplicate_pages() {
    let f = Fixture::new();
    let text = format!("{}跨界匹配 word word word", "a".repeat(65530));
    f.attachment("large.txt", "text/plain", text.as_bytes());
    let page = f
        .application
        .search(&f.context, query("跨界匹配"))
        .await
        .expect("search across read windows");
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].start_byte, 65530);
    assert!(page.complete);
    let mut q = query("word");
    let first = f
        .application
        .search(&f.context, q.clone())
        .await
        .expect("first page");
    assert_eq!(first.hits.len(), 2);
    q.cursor = first.next_cursor;
    assert!(q.cursor.is_some());
    let second = f
        .application
        .search(&f.context, q)
        .await
        .expect("next page");
    assert_eq!(second.hits.len(), 1);
    assert!(second.hits[0].start_byte > first.hits[1].end_byte);
    assert!(second.complete);
}
#[tokio::test]
async fn continuation_is_bound_to_query_and_current_effective_sources() {
    let f = Fixture::new();
    f.attachment("file.txt", "text/plain", b"word word word");
    let mut q = query("word");
    q.cursor = f
        .application
        .search(&f.context, q.clone())
        .await
        .expect("page")
        .next_cursor;
    let mut wrong = q.clone();
    wrong.query = "other".into();
    assert_eq!(
        f.application
            .search(&f.context, wrong)
            .await
            .expect_err("query binding"),
        FileQueryError::InvalidQuery
    );
    f.attachment("new.txt", "text/plain", b"word");
    assert_eq!(
        f.application
            .search(&f.context, q)
            .await
            .expect_err("snapshot binding"),
        FileQueryError::Changed
    );
}
#[tokio::test]
async fn unsupported_sources_are_reported_and_never_count_as_complete_search() {
    let f = Fixture::new();
    f.attachment("audio.mp3", "audio/mpeg", b"binary");
    let page = f
        .application
        .search(&f.context, query("absent"))
        .await
        .expect("coverage result");
    assert!(page.hits.is_empty());
    assert_eq!(page.unsearched.len(), 1);
    assert!(!page.complete);
    assert!(page.next_cursor.is_none());
}
#[tokio::test]
async fn search_budget_returns_continuation_without_false_exhaustion() {
    let f = Fixture::new();
    f.attachment(
        "large.txt",
        "text/plain",
        "a".repeat(2 * 1024 * 1024).as_bytes(),
    );
    let page = f
        .application
        .search(&f.context, query("missing"))
        .await
        .expect("bounded query");
    assert!(page.next_cursor.is_some());
    assert!(page.scanned_bytes <= 1024 * 1024);
    assert!(!page.complete);
    assert!(page.hits.is_empty());
}
#[tokio::test]
async fn exact_file_search_never_visits_other_conversation_files() {
    let f = Fixture::new();
    f.attachment("chosen.txt", "text/plain", b"chosen match");
    let reference = f.first().await;
    f.attachment("another.txt", "text/plain", b"another match");
    let page = f
        .application
        .search(
            &f.context,
            SearchFiles {
                scope: None,
                file_ref: Some(reference.clone()),
                ..query("match")
            },
        )
        .await
        .expect("one source");
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].file_ref, reference);
}

#[tokio::test]
async fn committed_draft_and_archived_files_remain_readable_and_searchable() {
    use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
    for lifecycle in [ArtifactLifecycle::Draft, ArtifactLifecycle::Archived] {
        let f = Fixture::new();
        f.artifact(
            "working.md",
            ArtifactKind::File,
            "text/markdown",
            b"committed draft content",
        );
        f.projection.0.lock().expect("projection").artifacts[0].lifecycle = lifecycle;
        let page = f
            .application
            .search(&f.context, query("draft"))
            .await
            .expect("committed source");
        assert_eq!(page.hits.len(), 1);
        assert_eq!(page.hits[0].start_byte, 10);
        assert!(page.complete);
        assert!(page.unsearched.is_empty());
    }
}

#[tokio::test]
async fn recursive_search_is_explicit_and_cannot_fall_back_to_children() {
    let f = Fixture::new();
    let mut q = query("match");
    q.traversal = FileSearchTraversal::Subtree;
    assert_eq!(
        f.application
            .search(&f.context, q.clone())
            .await
            .expect_err("conversation is a relation view"),
        FileQueryError::InvalidQuery
    );
    q.scope = Some(FileScope::Project);
    let mut context = f.context.clone();
    context.project_workspace_id = Some(musteragent_identity::WorkspaceId::generate());
    assert_eq!(
        f.application
            .search(&context, q)
            .await
            .expect_err("no fabricated recursive provider"),
        FileQueryError::Unsupported
    );
}

#[tokio::test]
async fn small_files_are_scanned_across_catalog_pages_without_empty_intermediate_results() {
    let f = Fixture::new();
    for i in 0..12 {
        f.attachment(&format!("file-{i:02}.txt"), "text/plain", b"needle");
    }
    let mut q = query("needle");
    q.limit = 20;
    let page = f
        .application
        .search(&f.context, q)
        .await
        .expect("multi page scan");
    assert_eq!(page.hits.len(), 12);
    assert_eq!(page.scanned_bytes, 72);
    assert!(page.complete);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn unsupported_catalog_is_bounded_and_can_be_fully_traversed() {
    let f = Fixture::new();
    for n in 0..140 {
        f.attachment(&format!("audio-{n}.mp3"), "audio/mpeg", b"binary");
    }
    let mut request = query("missing");
    let mut total = 0;
    loop {
        let page = f
            .application
            .search(&f.context, request.clone())
            .await
            .expect("bounded unsupported catalog search");
        assert!(page.unsearched.len() <= 16);
        assert!(
            !page.complete,
            "unsupported files never become complete coverage"
        );
        total += page.unsearched.len();
        request.cursor = page.next_cursor;
        if request.cursor.is_none() {
            break;
        }
        assert!(total < 140, "continuation must make progress");
    }
    assert_eq!(total, 140);
}
