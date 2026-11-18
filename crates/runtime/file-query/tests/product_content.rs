mod support;
use musteragent_runtime_file_query::*;
use std::sync::Arc;
use support::*;

#[tokio::test]
async fn primary_and_exact_dependencies_are_read_without_a_synthetic_artifact() {
    let f = Fixture::new();
    let primary = f.attachment("hello.txt", "text/plain", b"original bytes");
    let reference = f.first().await;
    let descriptor = f
        .application
        .open(&f.context, &reference)
        .await
        .expect("open");
    assert_eq!(descriptor.primary, primary);
    assert!(descriptor.dependencies.is_empty());
    let read = f
        .application
        .read_content(&f.context, &reference, &primary)
        .await
        .expect("original read");
    assert_eq!(read.bytes, b"original bytes");
    assert!(matches!(
        read.descriptor.entry.file_ref.source,
        FileSource::Attachment { .. }
    ));
    let foreign = f.bytes.add("text/plain", b"other private content");
    assert_eq!(
        f.application
            .read_content(&f.context, &reference, &foreign)
            .await
            .expect_err("not a bearer content grant"),
        FileQueryError::Denied
    );
}

#[tokio::test]
async fn source_identity_and_effective_version_bound_dependency_bytes() {
    let f = Fixture::new();
    let old = f.artifact(
        "deck",
        musteragent_artifact::ArtifactKind::Presentation,
        "application/json",
        b"{}",
    );
    let asset = f.bytes.add("image/png", b"original page");
    f.projection.0.lock().expect("lock").artifacts[0]
        .current_dependency_refs
        .push(asset.clone());
    let selected = f.first().await;
    let read = f
        .application
        .read_content(&f.context, &selected, &asset)
        .await
        .expect("exact governed asset");
    assert_eq!(read.descriptor.primary, old);
    assert_eq!(read.bytes, b"original page");
    f.projection.0.lock().expect("lock").artifacts[0].current_content_ref =
        f.bytes.add("application/json", b"new version");
    assert_eq!(
        f.application
            .read_content(&f.context, &selected, &asset)
            .await
            .expect_err("no latest-head substitution"),
        FileQueryError::Changed
    );
}

#[tokio::test]
async fn rewind_during_product_download_rejects_buffered_bytes() {
    let f = Fixture::new();
    let r = f.attachment("file", "text/plain", b"secret");
    let selected = f.first().await;
    let p = f.projection.clone();
    *f.bytes.on_read.lock().expect("lock") =
        Some(Arc::new(move || p.0.lock().expect("lock").messages.clear()));
    assert_eq!(
        f.application
            .read_content(&f.context, &selected, &r)
            .await
            .expect_err("revoked association"),
        FileQueryError::NotFound
    );
}

#[tokio::test]
async fn original_content_corruption_and_restricted_grants_fail_closed() {
    let mut f = Fixture::new();
    let r = f.attachment("file", "text/plain", b"safe");
    let selected = f.first().await;
    f.bytes
        .values
        .lock()
        .expect("lock")
        .insert(r.content_id, b"evil".to_vec());
    assert_eq!(
        f.application
            .read_content(&f.context, &selected, &r)
            .await
            .expect_err("integrity"),
        FileQueryError::Integrity
    );
    f.context.content_allowlist = Some(vec![]);
    assert_eq!(
        f.application
            .open(&f.context, &selected)
            .await
            .expect_err("child grant"),
        FileQueryError::NotFound
    );
}
