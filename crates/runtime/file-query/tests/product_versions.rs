mod support;
use musteragent_artifact::ArtifactKind;
use musteragent_identity::VersionNumber;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{Arc, Mutex};
use support::*;

struct Versions(Mutex<Vec<ConversationFileVersion>>);
impl ConversationFileVersions for Versions {
    fn current_versions<'a>(
        &'a self,
        _: &'a FileQueryContext,
        ids: Vec<musteragent_identity::ArtifactId>,
    ) -> ApplicationFuture<'a, Result<Vec<ConversationFileVersion>, FileQueryError>> {
        Box::pin(async move {
            Ok(self
                .0
                .lock()
                .expect("product version fixture lock")
                .iter()
                .filter(|version| ids.contains(&version.artifact_id))
                .cloned()
                .collect())
        })
    }
}

#[tokio::test]
async fn product_follows_saved_file_versions_without_rewriting_history_or_reviving_relations() {
    let f = Fixture::new();
    f.artifact("notes.md", ArtifactKind::File, "text/markdown", b"original");
    let original = f.first().await;
    let artifact_id = f
        .projection
        .0
        .lock()
        .expect("projection fixture lock")
        .artifacts[0]
        .artifact_id;
    let versions = Arc::new(Versions(Mutex::new(vec![])));
    let product = FileQueryApplication::new(
        Arc::new(
            ConversationFileCatalog::new(f.projection.clone())
                .with_product_versions(versions.clone()),
        ),
        Arc::new(NoProject),
        Arc::new(ContentFileReader::new(f.bytes.clone())),
    )
    .with_content_store(f.bytes.clone());
    for version in 2..=3 {
        let content = f
            .bytes
            .add("text/markdown", format!("saved {version}").as_bytes());
        *versions.0.lock().expect("product version fixture lock") = vec![ConversationFileVersion {
            artifact_id,
            version: VersionNumber::new(version).expect("positive fixture version"),
            content_ref: content.clone(),
            updated_at: f.context.now,
        }];
        let current = product
            .refresh_reference(&f.context, &original)
            .await
            .expect("refresh metadata to the current product version");
        assert_eq!(current.file_ref.source, original.source);
        assert_eq!(
            current.file_ref.revision,
            format!("{version}:{}", content.sha256)
        );
        assert_eq!(
            product
                .inspect(&f.context, &original)
                .await
                .expect_err("stale inspection must fail"),
            FileQueryError::Changed
        );
        let descriptor = product
            .open(&f.context, &current.file_ref)
            .await
            .expect("open the explicitly refreshed version");
        assert_eq!(descriptor.primary, content);
        assert_eq!(
            descriptor
                .artifact_origin
                .expect("original artifact provenance")
                .version
                .get(),
            version
        );
        let page = product
            .list(&f.context, query())
            .await
            .expect("current product catalog");
        assert_eq!(page.entries[0].file_ref, current.file_ref);
        // The same snapshot still means version 1 for history and sealed tools.
        assert_eq!(
            f.application
                .inspect(&f.context, &original)
                .await
                .expect("history retains its sealed original version")
                .file_ref,
            original
        );
    }
    let mut restricted = f.context.clone();
    restricted.content_allowlist = Some(vec![]);
    assert_eq!(
        product
            .list(&restricted, query())
            .await
            .expect_err("restricted content remains denied"),
        FileQueryError::Denied
    );
    f.projection
        .0
        .lock()
        .expect("projection fixture lock")
        .artifacts
        .clear();
    assert!(
        product
            .list(&f.context, query())
            .await
            .expect("catalog after original relation removal")
            .entries
            .is_empty()
    );
    assert_eq!(
        product
            .refresh_reference(&f.context, &original)
            .await
            .expect_err("metadata refresh cannot revive a removed relation"),
        FileQueryError::NotFound
    );
}
