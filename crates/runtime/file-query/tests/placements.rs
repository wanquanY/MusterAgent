mod support;
use musteragent_artifact::ArtifactKind;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{Arc, Mutex};
use support::*;

struct Placements(Mutex<ConversationFilePlacement>);
impl ConversationFilePlacements for Placements {
    fn placements<'a>(
        &'a self,
        _: &'a FileQueryContext,
        ids: Vec<musteragent_identity::ArtifactId>,
    ) -> ApplicationFuture<'a, Result<Vec<ConversationFilePlacement>, FileQueryError>> {
        Box::pin(async move {
            let item = self.0.lock().expect("placement lock").clone();
            Ok(if ids.contains(&item.artifact_id) {
                vec![item]
            } else {
                vec![]
            })
        })
    }
}
#[tokio::test]
async fn names_and_trash_follow_placement_without_changing_versions_or_reviving_retracted_relations()
 {
    let f = Fixture::new();
    f.artifact(
        "old.md",
        ArtifactKind::File,
        "text/plain",
        b"pinned contents",
    );
    let id = f.projection.0.lock().expect("projection lock").artifacts[0].artifact_id;
    let placement = Arc::new(Placements(Mutex::new(ConversationFilePlacement {
        artifact_id: id,
        name: "renamed.md".into(),
        available: true,
    })));
    let catalog =
        ConversationFileCatalog::new(f.projection.clone()).with_placements(placement.clone());
    let original = f.first().await;
    let renamed = catalog
        .list(&f.context, query())
        .await
        .expect("renamed catalog");
    assert_eq!(renamed.entries[0].name, "renamed.md");
    assert_eq!(renamed.entries[0].file_ref, original);
    placement.0.lock().unwrap().available = false;
    assert!(
        catalog
            .list(&f.context, query())
            .await
            .expect("hidden catalog")
            .entries
            .is_empty()
    );
    assert!(catalog.resolve(&f.context, &original).await.is_err());
    placement.0.lock().unwrap().available = true;
    assert_eq!(
        catalog
            .list(&f.context, query())
            .await
            .expect("visible catalog")
            .entries
            .len(),
        1
    );
    f.projection
        .0
        .lock()
        .expect("projection lock")
        .artifacts
        .clear();
    assert!(
        catalog
            .list(&f.context, query())
            .await
            .expect("retracted catalog")
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn saved_files_are_listed_and_readable_until_trash_or_rewind() {
    let f = Fixture::new();
    let bytes = b"<html><img src='assets/front.png'></html>";
    f.artifact("report.html", ArtifactKind::File, "text/html", bytes);
    let original = f.first().await;
    let id = f.projection.0.lock().unwrap().artifacts[0].artifact_id;
    let placement = Arc::new(Placements(Mutex::new(ConversationFilePlacement {
        artifact_id: id,
        name: "report.html".into(),
        available: true,
    })));
    let catalog = Arc::new(
        ConversationFileCatalog::new(f.projection.clone()).with_placements(placement.clone()),
    );
    let app = FileQueryApplication::new(
        catalog.clone(),
        Arc::new(NoProject),
        Arc::new(ContentFileReader::new(f.bytes.clone())),
    )
    .with_content_store(f.bytes.clone());
    let listed = app.list(&f.context, query()).await.unwrap();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].file_ref, original);
    let entry = app
        .resolve_identity(&f.context, &original.identity())
        .await
        .unwrap();
    let descriptor = app.open(&f.context, &entry.file_ref).await.unwrap();
    assert_eq!(
        app.read_content(&f.context, &entry.file_ref, &descriptor.primary)
            .await
            .unwrap()
            .bytes,
        bytes
    );
    // A rename changes presentation, not access or the versioned file identity.
    placement.0.lock().unwrap().name = "renamed.html".into();
    assert_eq!(
        app.resolve_identity(&f.context, &original.identity())
            .await
            .unwrap()
            .name,
        "renamed.html"
    );
    assert_eq!(
        app.list(&f.context, query()).await.unwrap().entries.len(),
        1
    );
    placement.0.lock().unwrap().available = false;
    assert!(matches!(
        app.resolve_identity(&f.context, &original.identity()).await,
        Err(FileQueryError::NotFound)
    ));
    assert!(
        app.read_content(&f.context, &entry.file_ref, &descriptor.primary)
            .await
            .is_err()
    );
    placement.0.lock().unwrap().available = true;
    f.projection.0.lock().unwrap().artifacts.clear();
    assert!(matches!(
        app.resolve_identity(&f.context, &original.identity()).await,
        Err(FileQueryError::NotFound)
    ));
}
