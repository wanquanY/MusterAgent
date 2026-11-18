//! PG conformance for the original Device publication/Artifact/Content owners.
//! The signed custody fixture substitutes only the trusted Native boundary;
//! bytes, migration, transactions, read authorization and GC are real owners.
use super::*;
use musteragent_artifact::{ArtifactKind, ArtifactVersion, ArtifactVersionAuthor};
use musteragent_postgres::{ContentGcPolicy, PostgresArtifactStore, PostgresPrivacyErasureStore};
use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
use musteragent_runtime_privacy::*;
use std::collections::BTreeSet;

struct Export {
    delivery: DeviceToolDelivery,
    manifest: DeviceArtifactExportManifest,
    contents: DeviceArtifactContents,
}
impl Fixture {
    async fn admitted() -> Self {
        let mut f = Self::new().await;
        f.bind().await;
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &f.binding)
            .await
            .unwrap();
        f
    }
    fn export(&self, previous: Option<&Export>, caption: bool) -> Export {
        let source = &self.custody.source;
        let artifact =
            previous.map_or_else(ArtifactId::generate, |p| p.manifest.entries[0].artifact_id);
        let number = previous.map_or(1, |p| p.manifest.entries[0].version.version.get() + 1);
        let bytes =
            format!("Device-produced UTF-8 TXT, version {number}.\n客户反馈验收。\n").into_bytes();
        let root = reference("text/plain", &bytes);
        let producer = InvocationId::generate();
        let call = ToolCallId::new(format!("producer_txt_{number}")).unwrap();
        let mut arguments = serde_json::json!({"purpose":"reply", "artifacts":[{
            "artifact_ref":{"kind":"artifact","id":artifact.to_string()},
            "artifact_tool_call_id":call.as_str()}]});
        if caption {
            arguments["caption"] = "Your file is ready.".into();
        }
        let request = serde_json::to_vec(&arguments).unwrap();
        let delivery = DeviceToolDelivery::new(
            self.binding.clone(),
            InvocationId::generate(),
            ARTIFACTS_DELIVER_TOOL.into(),
            reference("application/json", &request),
            request,
        )
        .unwrap();
        let created = previous.map_or(UtcTimestamp::from_unix_millis(100), |p| {
            p.manifest.entries[0].created_at
        });
        let committed = UtcTimestamp::from_unix_millis(100 + number as i64);
        let entry = DeviceArtifactExport {
            role: DeviceArtifactExportRole::Delivery,
            artifact_id: artifact,
            kind: ArtifactKind::File,
            title: "验收备注.txt".into(),
            source_activity_id: previous.map_or_else(ActivityId::generate, |p| {
                p.manifest.entries[0].source_activity_id
            }),
            source_created_by_run_id: self.binding.run_id,
            source_conversation_id: source.identities.conversation_id,
            created_at: created,
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id: artifact,
                version: VersionNumber::new(number).unwrap(),
                content_ref: root.clone(),
                dependency_refs: vec![],
                content_hash: root.sha256,
                source_invocation_id: Some(producer),
                committed_by: ArtifactVersionAuthor::Participant(source.identities.executor),
                committed_at: committed,
            },
            producer: DeviceArtifactProducer {
                invocation_id: producer,
                tool_call_id: call,
                tool_key: VersionedKey::new("file.write@1").unwrap(),
                result_ref: reference("application/json", br#"{"ok":true}"#),
                terminal_record_id: LedgerRecordId::generate(),
                terminal_record_hash: Sha256::digest(b"fixture terminal"),
                terminal_run_sequence: RunSeq::new(number).unwrap(),
                terminal_at: committed,
            },
        };
        let manifest = DeviceArtifactExportManifest {
            request_fingerprint: delivery.fingerprint(),
            scope: DeviceArtifactExportScope {
                tenant_id: self.actor.tenant_id,
                owner_id: self.actor.principal_id,
                agent_id: source.scope.agent_id,
                dialogue_id: source.scope.dialogue_id,
                conversation_id: source.identities.conversation_id,
            },
            entries: vec![entry],
        };
        let contents = DeviceArtifactContents::new(
            &delivery,
            &manifest,
            vec![DeviceArtifactContent {
                content_ref: root,
                bytes,
            }],
        )
        .unwrap();
        Export {
            delivery,
            manifest,
            contents,
        }
    }
    async fn publish(&self, e: &Export) -> DeviceArtifactDeliveryReceipt {
        self.publisher
            .publish_device_artifacts(
                self.actor,
                &self.target,
                &e.delivery,
                &e.manifest,
                &e.contents,
            )
            .await
            .unwrap()
    }
    fn artifacts(&self) -> PostgresArtifactStore {
        let id = DottedKey::new("test.device.artifacts").unwrap();
        PostgresArtifactStore::new(
            self.pool.clone(),
            Arc::new(OpaqueRefKeyring::new(id.clone(), [(id, [51; 32])]).unwrap()),
        )
    }
    async fn lookup(
        &self,
        e: &Export,
    ) -> Result<Option<DeviceArtifactDeliveryReceipt>, DialogueToolPublicationError> {
        self.publisher
            .lookup_device_artifact_delivery(self.actor, &self.target, &e.delivery, &e.manifest)
            .await
    }
}
fn reference(media: &str, bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media).unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

#[tokio::test]
async fn device_artifact_txt_publish_read_history_exact_replay_and_no_cloud_surrogates() {
    let f = Fixture::admitted().await;
    let e1 = f.export(None, true);
    assert_eq!(f.lookup(&e1).await.unwrap(), None);
    let (a, b) = tokio::join!(f.publish(&e1), f.publish(&e1));
    assert_eq!(a, b);
    a.validate_for(&e1.delivery, &e1.manifest).unwrap();
    assert_eq!(f.lookup(&e1).await.unwrap(), Some(a.clone()));
    let entry = *a.delivery_receipt.entry_ids.last().unwrap();
    let dialogue = f.custody.source.scope.dialogue_id;
    let reader = f.artifacts();
    let described = reader
        .describe(f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    assert!(described.history_supported);
    let v1 = described.current_version.artifact_version_id;
    let root = &e1.manifest.entries[0].version.content_ref;
    assert_eq!(
        reader
            .authorize_content(
                f.actor,
                dialogue,
                entry,
                v1,
                root.clone(),
                CorrelationId::generate()
            )
            .await
            .unwrap(),
        *root
    );
    assert_eq!(
        f.content.read(f.actor.tenant_id, root).await.unwrap(),
        e1.contents.entries()[0].bytes
    );
    assert_eq!(
        described.current_version.author,
        AgentArtifactAuthor::Agent {
            agent_id: f.custody.source.scope.agent_id,
            display_name: f.custody.source.configuration.profile.display_name.clone(),
        }
    );
    let e2 = f.export(Some(&e1), false);
    let second = f.publish(&e2).await;
    let current = reader
        .describe(f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    assert_eq!(current.artifact_id, described.artifact_id);
    assert_eq!(current.delivered_version_id, v1);
    assert_eq!(current.current_version.version.get(), 2);
    let history = reader
        .history(
            AgentArtifactHistoryQuery {
                actor: f.actor,
                dialogue_id: dialogue,
                entry_id: entry,
                at_version: None,
                before_version: None,
                limit: 20,
            },
            CorrelationId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(history.versions.len(), 2);
    assert_eq!(history.versions[1].artifact_version_id, v1);
    assert_eq!(
        f.publish(&e1).await,
        a,
        "old delivery must not rewind the canonical head"
    );
    assert_eq!(f.lookup(&e2).await.unwrap(), Some(second));
    let facts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT count(*) FROM artifact.artifacts),(SELECT count(*) FROM artifact.device_versions),
        (SELECT count(*) FROM runtime.runs),(SELECT count(*) FROM tool.tool_invocations),
        (SELECT count(*) FROM artifact.artifact_versions WHERE source_invocation_id IS NOT NULL)",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(facts, (1, 2, 0, 0, 0));
    assert!(
        sqlx::query("UPDATE artifact.device_versions SET source_version=999")
            .execute(&f.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM artifact.device_versions")
            .execute(&f.pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn device_artifact_wrong_commitment_custody_owner_permission_and_expired_target_fail_closed()
{
    let mut f = Fixture::with_permission(false, Some("full_access")).await;
    f.bind().await;
    f.queue
        .acknowledge_device_admission(f.actor, &f.target, &f.binding)
        .await
        .unwrap();
    let e = f.export(None, false);
    let receipt = f.publish(&e).await;
    let mut changed = e.manifest.clone();
    changed.entries[0].title = "different.txt".into();
    assert_ne!(
        changed.commitment(&e.delivery).unwrap(),
        receipt.export_commitment
    );
    assert!(
        f.publisher
            .lookup_device_artifact_delivery(f.actor, &f.target, &e.delivery, &changed)
            .await
            .is_err()
    );
    assert!(
        f.publisher
            .publish_device_artifacts(f.actor, &f.target, &e.delivery, &changed, &e.contents)
            .await
            .is_err()
    );
    for same_tenant in [false, true] {
        let mut foreign = actor();
        if same_tenant {
            foreign.tenant_id = f.actor.tenant_id;
        }
        let authority = target(
            foreign,
            f.binding.device_id,
            f.binding.installation_id,
            now(&f.pool).await,
        );
        assert!(
            f.publisher
                .lookup_device_artifact_delivery(foreign, &authority, &e.delivery, &e.manifest)
                .await
                .is_err()
        );
        assert!(
            f.artifacts()
                .describe(
                    foreign,
                    f.custody.source.scope.dialogue_id,
                    receipt.delivery_receipt.entry_ids[0],
                    CorrelationId::generate()
                )
                .await
                .is_err()
        );
    }
    let expired = target(
        f.actor,
        f.binding.device_id,
        f.binding.installation_id,
        now(&f.pool).await - 400_000,
    );
    assert!(
        f.publisher
            .lookup_device_artifact_delivery(f.actor, &expired, &e.delivery, &e.manifest)
            .await
            .is_err()
    );
    let mut wrong = e.manifest.clone();
    wrong.scope.agent_id = AgentId::generate();
    assert!(
        f.publisher
            .lookup_device_artifact_delivery(f.actor, &f.target, &e.delivery, &wrong)
            .await
            .is_err()
    );
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_revision=permission_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
        .bind(f.custody.source.scope.agent_id.into_uuid()).execute(&f.pool).await.unwrap();
    assert_eq!(
        f.lookup(&e).await.unwrap(),
        Some(receipt),
        "a revision with the same permission preset is not a revocation"
    );
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_mode='supervised',permission_revision=permission_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
        .bind(f.custody.source.scope.agent_id.into_uuid()).execute(&f.pool).await.unwrap();
    assert!(
        f.lookup(&e).await.is_err(),
        "current permission revocation also fences receipt lookup"
    );
    assert!(
        f.publisher
            .publish_device_artifacts(f.actor, &f.target, &e.delivery, &e.manifest, &e.contents)
            .await
            .is_err()
    );
    assert_eq!(count(&f.pool, "dialogue_tool_publication_groups").await, 1);
}

#[tokio::test]
async fn device_artifact_tombstone_preserves_ack_gc_retains_owned_content_and_reclaims_unowned_bytes()
 {
    let f = Fixture::admitted().await;
    let e = f.export(None, true);
    let receipt = f.publish(&e).await;
    let orphan = f
        .content
        .put(PutContent {
            tenant_id: f.actor.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"uncommitted orphan".to_vec(),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms=0 WHERE tenant_id=$1")
        .bind(f.actor.tenant_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    let report = f
        .content
        .sweep_unreferenced_content(
            UtcTimestamp::from_unix_millis(now(&f.pool).await),
            ContentGcPolicy::default(),
        )
        .await
        .unwrap();
    assert!(report.reclaimed >= 1);
    assert!(f.content.read(f.actor.tenant_id, &orphan).await.is_err());
    assert_eq!(
        f.content
            .read(f.actor.tenant_id, &receipt.delivery_receipt.result_ref)
            .await
            .unwrap(),
        receipt.delivery_receipt.result_bytes
    );
    let root = &e.manifest.entries[0].version.content_ref;
    f.content.delete(f.actor.tenant_id, root).await.unwrap();
    assert!(f.content.read(f.actor.tenant_id, root).await.is_err());
    assert_eq!(
        f.lookup(&e).await.unwrap(),
        Some(receipt.clone()),
        "ordinary root tombstone must not erase an already committed fact"
    );
    assert!(
        f.artifacts()
            .authorize_content(
                f.actor,
                f.custody.source.scope.dialogue_id,
                *receipt.delivery_receipt.entry_ids.last().unwrap(),
                canonical_version(&f, &e).await,
                root.clone(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    assert!(
        f.content
            .put_stream_at(
                f.actor.tenant_id,
                root,
                &mut e.contents.entries()[0].bytes.as_slice()
            )
            .await
            .is_err()
    );
}

async fn canonical_version(f: &Fixture, e: &Export) -> ArtifactVersionId {
    let id: uuid::Uuid = sqlx::query_scalar(
        "SELECT artifact_version_id FROM artifact.device_versions WHERE source_version_id=$1",
    )
    .bind(
        e.manifest.entries[0]
            .version
            .artifact_version_id
            .into_uuid(),
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    ArtifactVersionId::from_uuid(id).unwrap()
}

#[path = "device_artifact_privacy.rs"]
mod privacy;

#[path = "device_image_artifacts.rs"]
mod images;

#[path = "device_artifacts_batch.rs"]
mod batch;
