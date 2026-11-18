mod support;
use musteragent_artifact::ArtifactKind;
use musteragent_runtime_file_query::*;
use std::sync::Arc;
use support::*;

#[tokio::test]
async fn utf8_reads_have_bounded_monotonic_continuations_and_reconstruct_exact_text() {
    let f = Fixture::new();
    let body = "中英文一起🙂\nlast";
    f.attachment("notes.txt", "text/plain", body.as_bytes());
    let reference = f.first().await;
    let mut offset = 0;
    let mut result = String::new();
    loop {
        let page = f
            .application
            .read(
                &f.context,
                ReadFile {
                    file_ref: reference.clone(),
                    offset,
                    limit: 5,
                },
            )
            .await
            .expect("valid fixture and authorized query")
            .view;
        assert!(
            page.text
                .as_ref()
                .expect("valid fixture and authorized query")
                .len()
                <= 5
        );
        result.push_str(
            page.text
                .as_ref()
                .expect("valid fixture and authorized query"),
        );
        match page.next_offset {
            Some(next) => {
                assert!(next > offset);
                offset = next;
            }
            None => break,
        }
    }
    assert_eq!(result, body);
    assert_eq!(
        f.application
            .read(
                &f.context,
                ReadFile {
                    file_ref: reference,
                    offset: 0,
                    limit: 1
                }
            )
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::InvalidQuery
    );
}
#[tokio::test]
async fn empty_text_is_distinct_from_unsupported_binary() {
    let f = Fixture::new();
    f.attachment("empty.txt", "text/plain", b"");
    let view = f
        .application
        .read(&f.context, read(f.first().await))
        .await
        .expect("valid fixture and authorized query")
        .view;
    assert_eq!(view.text.as_deref(), Some(""));
    assert_eq!(view.representation, FileRepresentation::SourceText);
    let f = Fixture::new();
    f.attachment("binary", "application/octet-stream", &[0, 255, 0]);
    let view = f
        .application
        .read(&f.context, read(f.first().await))
        .await
        .expect("valid fixture and authorized query")
        .view;
    assert_eq!(view.text, None);
    assert_eq!(view.extraction_status, "parser_unavailable");
}
#[tokio::test]
async fn retraction_during_asynchronous_content_read_discards_result() {
    let f = Fixture::new();
    f.attachment("file", "text/plain", b"secret");
    let reference = f.first().await;
    let p = f.projection.clone();
    *f.bytes.on_read.lock().expect("fixture lock") = Some(Arc::new(move || {
        p.0.lock().expect("fixture lock").messages.clear();
    }));
    assert_eq!(
        f.application
            .read(&f.context, read(reference))
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::NotFound
    );
}
#[tokio::test]
async fn structured_artifact_manifest_is_never_presented_as_document_text() {
    let f = Fixture::new();
    f.artifact(
        "deck",
        ArtifactKind::Presentation,
        "application/json",
        br#"{"internal":"manifest"}"#,
    );
    assert_eq!(
        f.application
            .read(&f.context, read(f.first().await))
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Unsupported
    );
    assert_eq!(*f.bytes.reads.lock().expect("fixture lock"), 0);
}
#[tokio::test]
async fn content_integrity_is_checked_even_for_a_store_that_does_not_verify() {
    let f = Fixture::new();
    let r = f.attachment("file", "text/plain", b"safe");
    f.bytes
        .values
        .lock()
        .expect("valid fixture and authorized query")
        .insert(r.content_id, b"evil".to_vec());
    assert_eq!(
        f.application
            .read(&f.context, read(f.first().await))
            .await
            .expect_err("reject invalid or stale file access"),
        FileQueryError::Integrity
    );
}
#[tokio::test]
async fn visual_content_returns_explicit_authorized_material_not_a_text_claim() {
    let f = Fixture::new();
    let r = f.attachment("image", "image/png", b"test-image-payload");
    let result = f
        .application
        .read(&f.context, read(f.first().await))
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(result.authorized_content, vec![r]);
    assert!(result.view.text.is_none());
    assert_eq!(
        result.view.representation,
        FileRepresentation::VisualContent
    );
}
