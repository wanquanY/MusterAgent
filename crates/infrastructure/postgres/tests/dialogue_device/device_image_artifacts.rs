//! Native admitted custody with real canonical Content, Artifact and Resource
//! transactions. No Cloud Run, Tool invocation or producer surrogate is seeded.
use super::*;

const PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 4, 0,
    0, 0, 181, 28, 12, 2, 0, 0, 0, 11, 73, 68, 65, 84, 120, 218, 99, 252, 255, 31, 0, 3, 3, 2, 0,
    239, 154, 63, 89, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];

fn image_root(references: &[ContentRef]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "source_kind":"image_generation_record", "source_id":"image-owner-record",
        "task_id":"image-owner-task", "title":"原图交付", "prompt":"a green square",
        "asset_urls":[], "thumbnail_url":null, "provider_request_ref":null, "provider_model":null,
        "assets": references.iter().map(|r| serde_json::json!({
            "content_ref": {"content_id":r.content_id.to_string(),"media_type":r.media_type.as_str(),
                "byte_length":r.byte_length,"sha256":r.sha256.into_bytes().iter().map(|v|format!("{v:02x}")).collect::<String>()},
            "delivery_url":null
        })).collect::<Vec<_>>()
    })).unwrap()
}
fn change_image(e: &mut Export, dependencies: &[ContentRef]) {
    let bytes = image_root(dependencies);
    let root = reference("application/json", &bytes);
    let entry = &mut e.manifest.entries[0];
    entry.kind = ArtifactKind::Image;
    entry.title = "原图交付".into();
    entry.producer.tool_key = VersionedKey::new("artifact.image.create@1").unwrap();
    entry.version.content_hash = root.sha256;
    entry.version.content_ref = root.clone();
    entry.version.dependency_refs = dependencies.to_vec();
    e.contents = DeviceArtifactContents::new(
        &e.delivery,
        &e.manifest,
        vec![DeviceArtifactContent {
            content_ref: root,
            bytes,
        }],
    )
    .unwrap();
}
fn redeliver(e: &Export) -> Export {
    let delivery = DeviceToolDelivery::new(
        e.delivery.binding().clone(),
        InvocationId::generate(),
        "artifacts.deliver@2".into(),
        e.delivery.request_ref().clone(),
        e.delivery.request_bytes().to_vec(),
    )
    .unwrap();
    let mut manifest = e.manifest.clone();
    manifest.request_fingerprint = delivery.fingerprint();
    let contents =
        DeviceArtifactContents::new(&delivery, &manifest, e.contents.entries().to_vec()).unwrap();
    Export {
        delivery,
        manifest,
        contents,
    }
}
impl Fixture {
    fn image_directory_query(&self) -> AgentArtifactDirectoryQuery {
        AgentArtifactDirectoryQuery {
            actor: self.actor,
            agent_id: self.custody.source.scope.agent_id,
            cursor: None,
            limit: 10,
            origin: None,
            file_types: vec!["image".into()],
        }
    }
    async fn image_content(&self, tenant: TenantId) -> ContentRef {
        self.content
            .put_at(
                ContentId::generate(),
                PutContent {
                    tenant_id: tenant,
                    media_type: MediaType::new("image/png").unwrap(),
                    bytes: PNG.to_vec(),
                },
            )
            .await
            .unwrap()
    }
    fn image_export(&self, dependencies: &[ContentRef]) -> Export {
        let mut export = redeliver(&self.export(None, false));
        change_image(&mut export, dependencies);
        export
    }
}

#[tokio::test]
async fn device_image_manifest_publishes_canonical_dependencies_read_replay_and_gc() {
    let f = Fixture::admitted().await;
    let image = f.image_content(f.actor.tenant_id).await;
    let e = f.image_export(std::slice::from_ref(&image));
    assert_eq!(e.delivery.tool_key(), "artifacts.deliver@2");
    assert_eq!(
        e.contents.entries().len(),
        1,
        "only JSON root crosses transport"
    );
    assert_eq!(
        e.contents.entries()[0].content_ref.media_type.as_str(),
        "application/json"
    );
    let (receipt, replay) = tokio::join!(f.publish(&e), f.publish(&e));
    assert_eq!(receipt, replay);
    assert_eq!(f.lookup(&e).await.unwrap(), Some(receipt.clone()));
    let entry = receipt.delivery_receipt.entry_ids[0];
    let dialogue = f.custody.source.scope.dialogue_id;
    let descriptor = f
        .artifacts()
        .describe(f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    assert_eq!(descriptor.kind, ArtifactKind::Image);
    assert!(
        !descriptor.history_supported,
        "Image is not editable TXT history"
    );
    let listed = f
        .artifacts()
        .list(f.image_directory_query(), CorrelationId::generate())
        .await
        .unwrap();
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].entry_id, entry);
    assert_eq!(listed.items[0].artifact.artifact_id, descriptor.artifact_id);
    assert_eq!(listed.items[0].artifact.kind, ArtifactKind::Image);
    match &descriptor.current_version.content {
        AgentArtifactContent::Ready { root, dependencies } => {
            assert_eq!(*root, e.manifest.entries[0].version.content_ref);
            assert_eq!(dependencies, &vec![image.clone()]);
        }
        _ => panic!("published image must be readable"),
    }
    let version = canonical_version(&f, &e).await;
    assert_eq!(
        f.artifacts()
            .authorize_content(
                f.actor,
                dialogue,
                entry,
                version,
                image.clone(),
                CorrelationId::generate()
            )
            .await
            .unwrap(),
        image
    );
    assert_eq!(
        f.content.read(f.actor.tenant_id, &image).await.unwrap(),
        PNG
    );
    let unrelated = f.image_content(f.actor.tenant_id).await;
    assert!(
        f.artifacts()
            .authorize_content(
                f.actor,
                dialogue,
                entry,
                version,
                unrelated.clone(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms=0 WHERE tenant_id=$1")
        .bind(f.actor.tenant_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    f.content
        .sweep_unreferenced_content(
            UtcTimestamp::from_unix_millis(now(&f.pool).await),
            ContentGcPolicy::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        f.content.read(f.actor.tenant_id, &image).await.unwrap(),
        PNG,
        "original Artifact dependency pins retain canonical image bytes"
    );
    assert!(f.content.read(f.actor.tenant_id, &unrelated).await.is_err());
    let facts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT count(*) FROM runtime.runs), (SELECT count(*) FROM tool.tool_invocations),
        (SELECT count(*) FROM artifact.artifact_version_dependencies),
        (SELECT count(*) FROM resource.nodes WHERE scope_kind=4),
        (SELECT count(*) FROM resource.file_versions)",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        facts,
        (0, 0, 1, 0, 0),
        "an Image manifest is neither File bytes nor a synthetic Resource node"
    );
    let roots = PostgresPrivacyErasureStore::new(f.pool.clone())
        .device_artifact_erasure_roots(f.actor, Some(f.custody.source.scope.agent_id))
        .await
        .unwrap();
    assert!(roots.contains(&e.manifest.entries[0].version.content_ref));
    assert!(roots.contains(&image));
    assert!(roots.contains(&receipt.delivery_receipt.result_ref));
    let changed_custody = sqlx::query(
        "UPDATE artifact.device_versions SET device_id=$1 WHERE artifact_version_id=$2",
    )
    .bind(DeviceId::generate().into_uuid())
    .bind(version.into_uuid())
    .execute(&f.pool)
    .await;
    assert!(
        changed_custody.is_err(),
        "Image must retain original immutable custody guard"
    );
    // A later Content tombstone revokes reads but must not erase the committed ACK.
    f.content.delete(f.actor.tenant_id, &image).await.unwrap();
    assert!(
        f.artifacts()
            .authorize_content(
                f.actor,
                dialogue,
                entry,
                version,
                image,
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    assert_eq!(f.lookup(&e).await.unwrap(), Some(receipt));
}

#[tokio::test]
async fn device_image_node_free_reads_recheck_account_and_artifact_lifecycle() {
    let f = Fixture::admitted().await;
    let image = f.image_content(f.actor.tenant_id).await;
    let e = f.image_export(std::slice::from_ref(&image));
    let receipt = f.publish(&e).await;
    let entry = receipt.delivery_receipt.entry_ids[0];
    let dialogue = f.custody.source.scope.dialogue_id;
    let reader = f.artifacts();
    let descriptor = reader
        .describe(f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    let outsider = Actor {
        principal_id: PrincipalId::generate(),
        ..f.actor
    };
    assert!(
        reader
            .describe(outsider, dialogue, entry, CorrelationId::generate())
            .await
            .is_err()
    );
    let mut foreign_query = f.image_directory_query();
    foreign_query.actor = outsider;
    assert!(
        reader
            .list(foreign_query, CorrelationId::generate())
            .await
            .is_err()
    );
    assert!(
        reader
            .authorize_content(
                outsider,
                dialogue,
                entry,
                descriptor.current_version.artifact_version_id,
                image.clone(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );

    // The original Artifact lifecycle remains authoritative even without a
    // Resource node. Exercise the persisted archived state at the read owner.
    sqlx::query("UPDATE artifact.artifacts SET lifecycle=3 WHERE tenant_id=$1 AND artifact_id=$2")
        .bind(f.actor.tenant_id.into_uuid())
        .bind(descriptor.artifact_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(
        reader
            .describe(f.actor, dialogue, entry, CorrelationId::generate())
            .await
            .is_err()
    );
    assert!(
        reader
            .authorize_content(
                f.actor,
                dialogue,
                entry,
                descriptor.current_version.artifact_version_id,
                image,
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .list(f.image_directory_query(), CorrelationId::generate())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let another = redeliver(&e);
    assert_eq!(
        f.publisher
            .publish_device_artifacts(
                f.actor,
                &f.target,
                &another.delivery,
                &another.manifest,
                &another.contents
            )
            .await,
        Err(DialogueToolPublicationError::ReferenceUnavailable)
    );
    assert_eq!(
        f.lookup(&e).await.unwrap(),
        Some(receipt),
        "the committed ACK is historical evidence, not current read authority"
    );
}

#[tokio::test]
async fn device_image_missing_foreign_changed_and_deleted_dependencies_fail_before_commit() {
    let f = Fixture::admitted().await;
    let ready = f.image_content(f.actor.tenant_id).await;
    let foreign = f.image_content(TenantId::generate()).await;
    let deleted = f.image_content(f.actor.tenant_id).await;
    f.content.delete(f.actor.tenant_id, &deleted).await.unwrap();
    let mut hash = ready.clone();
    hash.sha256 = Sha256::digest(b"not the canonical image");
    let mut length = ready.clone();
    length.byte_length += 1;
    let mut mime = ready.clone();
    mime.media_type = MediaType::new("image/jpeg").unwrap();
    for dependency in [
        reference("image/png", PNG),
        foreign,
        hash,
        length,
        mime,
        deleted,
    ] {
        let e = f.image_export(&[dependency]);
        assert_eq!(
            f.publisher
                .publish_device_artifacts(f.actor, &f.target, &e.delivery, &e.manifest, &e.contents)
                .await,
            Err(DialogueToolPublicationError::ReferenceUnavailable)
        );
        assert_eq!(f.lookup(&e).await.unwrap(), None);
    }
    let e = f.image_export(&[ready.clone(), reference("image/png", PNG)]);
    assert!(
        f.publisher
            .publish_device_artifacts(f.actor, &f.target, &e.delivery, &e.manifest, &e.contents)
            .await
            .is_err()
    );
    let facts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT count(*) FROM agent.dialogue_tool_publication_groups),
        (SELECT count(*) FROM artifact.device_versions),
        (SELECT count(*) FROM artifact.artifact_version_dependencies),
        (SELECT count(*) FROM resource.nodes)",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        facts,
        (0, 0, 0, 0),
        "partial closure must not publish or place any artifact"
    );
    assert_eq!(
        f.content.read(f.actor.tenant_id, &ready).await.unwrap(),
        PNG
    );
}

#[tokio::test]
async fn device_image_source_version_reuse_binds_exact_kind_and_dependency_closure() {
    let f = Fixture::admitted().await;
    let image = f.image_content(f.actor.tenant_id).await;
    let e = f.image_export(std::slice::from_ref(&image));
    let first = f.publish(&e).await;
    let next = redeliver(&e);
    let repeated = f.publish(&next).await;
    assert_ne!(
        first.delivery_receipt.entry_ids,
        repeated.delivery_receipt.entry_ids
    );
    assert_eq!(
        canonical_version(&f, &e).await,
        canonical_version(&f, &next).await
    );
    let different = f.image_content(f.actor.tenant_id).await;
    let mut changed = redeliver(&e);
    change_image(&mut changed, &[different]);
    assert_eq!(
        f.publisher
            .publish_device_artifacts(
                f.actor,
                &f.target,
                &changed.delivery,
                &changed.manifest,
                &changed.contents
            )
            .await,
        Err(DialogueToolPublicationError::Forbidden),
        "same source version cannot be rebound to other bytes"
    );
    let mut changed_kind = redeliver(&e);
    let bytes = b"cannot reinterpret the committed image as a TXT file".to_vec();
    let root = reference("text/plain", &bytes);
    changed_kind.manifest.entries[0].kind = ArtifactKind::File;
    changed_kind.manifest.entries[0].version.content_ref = root.clone();
    changed_kind.manifest.entries[0].version.content_hash = root.sha256;
    changed_kind.manifest.entries[0]
        .version
        .dependency_refs
        .clear();
    changed_kind.contents = DeviceArtifactContents::new(
        &changed_kind.delivery,
        &changed_kind.manifest,
        vec![DeviceArtifactContent {
            content_ref: root,
            bytes,
        }],
    )
    .unwrap();
    assert!(
        f.publisher
            .publish_device_artifacts(
                f.actor,
                &f.target,
                &changed_kind.delivery,
                &changed_kind.manifest,
                &changed_kind.contents
            )
            .await
            .is_err()
    );
    assert_eq!(count(&f.pool, "dialogue_tool_publication_groups").await, 2);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact.device_versions")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(f.lookup(&e).await.unwrap(), Some(first));
}

/// Advance authorization through its real owner before any admission ACK. The
/// immutable first proof is deliberately not rewritten by this operation.
async fn renewed_fixture() -> (Fixture, Sha256, Sha256) {
    let f = Fixture::new().await;
    let mut claims = grant(&f.custody, now(&f.pool).await);
    claims.expires_at = UtcTimestamp::from_unix_millis(now(&f.pool).await + 1_000);
    let old = f
        .queue
        .bind_device_grant(f.claim, sign(claims))
        .await
        .unwrap();
    let old_hash = Sha256::digest(&old.encoded_proof);
    let remaining = old.grant.claims.expires_at.unix_millis() - now(&f.pool).await;
    if remaining >= 0 {
        tokio::time::sleep(Duration::from_millis(remaining as u64 + 5)).await;
    }
    let mut successor = old.grant.claims.clone();
    successor.issued_at = UtcTimestamp::from_unix_millis(now(&f.pool).await);
    successor.expires_at = successor.issued_at.checked_add_millis(60_000).unwrap();
    let next = f
        .queue
        .reauthorize_device_grant(
            f.claim,
            &f.target,
            old_hash,
            f.custody.source_digest,
            sign(successor),
        )
        .await
        .unwrap();
    let next_hash = Sha256::digest(&next.encoded_proof);
    assert_ne!(old_hash, next_hash);
    assert_eq!(next.source, old.source);
    let state: (Vec<u8>, i64, Option<i64>) = sqlx::query_as(
        "SELECT d.signed_grant_hash,h.current_revision,h.admitted_revision
         FROM agent.device_admissions d JOIN agent.device_grant_heads h USING(intent_id)
         WHERE d.intent_id=$1",
    )
    .bind(f.binding.intent_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(state, (old_hash.into_bytes().to_vec(), 2, None));
    (f, old_hash, next_hash)
}

#[tokio::test]
async fn device_image_and_txt_read_use_admitted_successor_grant_not_first_proof() {
    let (mut f, old_hash, admitted_hash) = renewed_fixture().await;
    f.binding.grant_proof_hash = admitted_hash;
    let mut foreign = f.binding.clone();
    foreign.grant_proof_hash = Sha256::digest(b"not any authorization of this intent");
    assert!(
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &foreign)
            .await
            .is_err()
    );
    f.queue
        .acknowledge_device_admission(f.actor, &f.target, &f.binding)
        .await
        .unwrap();
    let mut unadmitted = f.binding.clone();
    unadmitted.grant_proof_hash = old_hash;
    assert!(
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &unadmitted)
            .await
            .is_err(),
        "a historical grant is not interchangeable with the acknowledged grant"
    );
    let image = f.image_content(f.actor.tenant_id).await;
    for e in [
        f.export(None, false),
        f.image_export(std::slice::from_ref(&image)),
    ] {
        let receipt = f.publish(&e).await;
        assert_eq!(f.lookup(&e).await.unwrap(), Some(receipt.clone()));
        let entry = receipt.delivery_receipt.entry_ids[0];
        let dialogue = f.custody.source.scope.dialogue_id;
        let descriptor = f
            .artifacts()
            .describe(f.actor, dialogue, entry, CorrelationId::generate())
            .await
            .unwrap();
        assert_eq!(descriptor.kind, e.manifest.entries[0].kind);
        let reference = if descriptor.kind == ArtifactKind::Image {
            image.clone()
        } else {
            e.manifest.entries[0].version.content_ref.clone()
        };
        f.artifacts()
            .authorize_content(
                f.actor,
                dialogue,
                entry,
                descriptor.current_version.artifact_version_id,
                reference.clone(),
                CorrelationId::generate(),
            )
            .await
            .unwrap();
        assert!(
            !f.content
                .read(f.actor.tenant_id, &reference)
                .await
                .unwrap()
                .is_empty()
        );
        let mut wrong = f.actor;
        wrong.principal_id = PrincipalId::generate();
        assert!(
            f.artifacts()
                .describe(wrong, dialogue, entry, CorrelationId::generate())
                .await
                .is_err()
        );
        let wrong_delivery = DeviceToolDelivery::new(
            unadmitted.clone(),
            InvocationId::generate(),
            e.delivery.tool_key().to_owned(),
            e.delivery.request_ref().clone(),
            e.delivery.request_bytes().to_vec(),
        )
        .unwrap();
        let mut wrong_manifest = e.manifest.clone();
        wrong_manifest.request_fingerprint = wrong_delivery.fingerprint();
        let wrong_contents = DeviceArtifactContents::new(
            &wrong_delivery,
            &wrong_manifest,
            e.contents.entries().to_vec(),
        )
        .unwrap();
        assert!(
            f.publisher
                .publish_device_artifacts(
                    f.actor,
                    &f.target,
                    &wrong_delivery,
                    &wrong_manifest,
                    &wrong_contents
                )
                .await
                .is_err()
        );
    }
    let proof: (Vec<u8>, i64, i64) = sqlx::query_as(
        "SELECT d.signed_grant_hash,h.current_revision,h.admitted_revision
         FROM agent.device_admissions d JOIN agent.device_grant_heads h USING(intent_id)
         WHERE d.intent_id=$1",
    )
    .bind(f.binding.intent_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(proof, (old_hash.into_bytes().to_vec(), 2, 2));
    assert_eq!(count(&f.pool, "dialogue_tool_publication_groups").await, 2);
}

#[tokio::test]
async fn device_image_read_uses_late_old_ack_instead_of_unadmitted_current_head() {
    let (mut f, admitted_hash, unadmitted_hash) = renewed_fixture().await;
    // Native may already have admitted v1 while its ACK was lost. Cloud offered
    // v2 before learning that fact; recovery must pin actual admission v1.
    f.binding.grant_proof_hash = admitted_hash;
    f.queue
        .acknowledge_device_admission(f.actor, &f.target, &f.binding)
        .await
        .unwrap();
    let head: (i64, i64) = sqlx::query_as("SELECT current_revision,admitted_revision FROM agent.device_grant_heads WHERE intent_id=$1")
        .bind(f.binding.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(head, (2, 1));
    let mut wrong = f.binding.clone();
    wrong.grant_proof_hash = unadmitted_hash;
    assert!(
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &wrong)
            .await
            .is_err()
    );
    let image = f.image_content(f.actor.tenant_id).await;
    let e = f.image_export(std::slice::from_ref(&image));
    let receipt = f.publish(&e).await;
    let entry = receipt.delivery_receipt.entry_ids[0];
    let dialogue = f.custody.source.scope.dialogue_id;
    let descriptor = f
        .artifacts()
        .describe(f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    f.artifacts()
        .authorize_content(
            f.actor,
            dialogue,
            entry,
            descriptor.current_version.artifact_version_id,
            image,
            CorrelationId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(f.lookup(&e).await.unwrap(), Some(receipt));
}
