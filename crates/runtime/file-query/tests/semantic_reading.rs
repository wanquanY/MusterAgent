mod support;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_content_ref::ContentRef;
use musteragent_identity::*;
use musteragent_runtime_artifact::*;
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};
use musteragent_runtime_file_query::*;
use std::sync::Arc;
use support::*;

struct Catalog(ArtifactView);
impl ArtifactApplication for Catalog {
    fn get_artifact(
        &self,
        query: GetArtifact,
    ) -> ApplicationFuture<'_, Result<ArtifactView, RuntimeError>> {
        Box::pin(async move {
            assert_eq!(query.artifact_id, self.0.artifact.artifact_id);
            Ok(self.0.clone())
        })
    }
    fn list_conversation_artifacts(
        &self,
        _: ListConversationArtifacts,
    ) -> ApplicationFuture<'_, Result<ArtifactPage, RuntimeError>> {
        panic!("effective projection is the discovery owner")
    }
}
fn application(
    f: &Fixture,
    kind: ArtifactKind,
    body: &[u8],
    dependencies: Vec<ContentRef>,
    newer_body: Option<&[u8]>,
) -> FileQueryApplication {
    let content = f.artifact("artifact", kind, "application/json", body);
    let mut snapshot = f.projection.0.lock().expect("fixture projection");
    let artifact = snapshot.artifacts.first_mut().expect("fixture artifact");
    artifact.current_dependency_refs = dependencies.clone();
    let version = ArtifactVersion {
        artifact_version_id: ArtifactVersionId::generate(),
        artifact_id: artifact.artifact_id,
        version: artifact.current_version,
        content_hash: content.sha256,
        content_ref: content,
        dependency_refs: dependencies,
        source_invocation_id: None,
        committed_by: ArtifactVersionAuthor::Participant(ParticipantId::generate()),
        committed_at: f.context.now,
    };
    let mut versions = vec![version.clone()];
    if let Some(bytes) = newer_body {
        let content = f.bytes.add("application/json", bytes);
        versions.push(ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            version: VersionNumber::new(2).expect("version"),
            content_hash: content.sha256,
            content_ref: content,
            ..version
        });
    }
    let owner = Arc::new(Catalog(ArtifactView {
        artifact: Artifact {
            artifact_id: artifact.artifact_id,
            tenant_id: f.context.actor.tenant_id,
            source: ArtifactSource::Conversation(
                f.context
                    .require_conversation()
                    .expect("conversation fixture"),
            ),
            origin: ArtifactOrigin::Product,
            kind,
            lifecycle: ArtifactLifecycle::Published,
            current_version: VersionNumber::new(versions.len() as u64).expect("version"),
            title: "artifact".into(),
            created_at: f.context.now,
            updated_at: f.context.now,
        },
        versions,
    }));
    FileQueryApplication::new(
        f.catalog.clone(),
        Arc::new(NoProject),
        Arc::new(ContentFileReader::new(f.bytes.clone()).with_artifacts(owner)),
    )
}
fn stored(reference: &ContentRef) -> serde_json::Value {
    serde_json::json!({"content_id": reference.content_id.to_string(), "media_type": reference.media_type.as_str(),
        "byte_length": reference.byte_length, "sha256": reference.sha256.to_string()})
}
#[tokio::test]
async fn image_reads_only_committed_assets_and_uses_visual_material() {
    let f = Fixture::new();
    let image = f.bytes.add("image/png", b"generated image bytes");
    let body = serde_json::to_vec(&serde_json::json!({"source_kind":"image_generation_record", "source_id":"source", "task_id":"task", "title":"image", "prompt":"test", "asset_urls":["https://never-fetch.invalid/image"],
        "assets":[{"content_ref":stored(&image),"delivery_url":null}], "thumbnail_url":null,"provider_request_ref":null,"provider_model":null})).expect("manifest");
    let app = application(&f, ArtifactKind::Image, &body, vec![image.clone()], None);
    let result = app
        .read(&f.context, read(f.first().await))
        .await
        .expect("semantic image");
    assert_eq!(result.authorized_content, vec![image]);
    assert_eq!(result.view.text, None);
    assert_eq!(result.view.resolved_start, 0);
    assert_eq!(result.view.resolved_end, 1);
}
#[tokio::test]
async fn image_read_uses_the_effective_version_after_the_catalog_head_advances() {
    let f = Fixture::new();
    let effective = f.bytes.add("image/png", b"effective image");
    let newer = f.bytes.add("image/png", b"rewound image");
    let manifest = |image: &ContentRef| {
        serde_json::to_vec(&serde_json::json!({
            "source_kind": "image_generation_record", "source_id": "source",
            "task_id": "task", "title": "image", "prompt": "test", "asset_urls": [],
            "assets": [{"content_ref": stored(image), "delivery_url": null}],
            "thumbnail_url": null, "provider_request_ref": null, "provider_model": null
        }))
        .unwrap()
    };
    let app = application(
        &f,
        ArtifactKind::Image,
        &manifest(&effective),
        vec![effective.clone()],
        Some(&manifest(&newer)),
    );
    let result = app.read(&f.context, read(f.first().await)).await.unwrap();
    assert_eq!(result.authorized_content, vec![effective]);
    assert_eq!(
        result.view.representation,
        FileRepresentation::VisualContent
    );
    assert_eq!(result.view.text, None);
}
#[tokio::test]
async fn undeclared_media_asset_is_denied_even_if_manifest_has_a_content_reference() {
    let f = Fixture::new();
    let image = f.bytes.add("image/png", b"private");
    let body = serde_json::to_vec(&serde_json::json!({"source_kind":"image_generation_record", "source_id":"source", "task_id":"task", "title":"image", "prompt":"test", "asset_urls":[],
        "assets":[{"content_ref":stored(&image),"delivery_url":null}], "thumbnail_url":null,"provider_request_ref":null,"provider_model":null})).expect("manifest");
    let app = application(&f, ArtifactKind::Image, &body, vec![], None);
    assert!(app.read(&f.context, read(f.first().await)).await.is_err());
    assert_eq!(
        *f.bytes.reads.lock().expect("reads"),
        1,
        "must not fetch the undeclared image"
    );
}
#[tokio::test]
async fn legacy_url_only_media_is_explicitly_unsupported_and_never_fetched() {
    let f = Fixture::new();
    let body = br#"{"source_kind":"image_generation_record","source_id":"source","task_id":"task","title":"image","prompt":"test","asset_urls":["https://never-fetch.invalid/image"]}"#;
    let app = application(&f, ArtifactKind::Image, body, vec![], None);
    let result = app
        .read(&f.context, read(f.first().await))
        .await
        .expect("explicit unsupported");
    assert_eq!(result.view.representation, FileRepresentation::Unsupported);
    assert_eq!(result.view.text, None);
    assert!(result.authorized_content.is_empty());
}
