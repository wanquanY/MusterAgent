use super::*;
use musteragent_artifact::{ArtifactKind, ArtifactVersion, ArtifactVersionAuthor};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;

fn fixture() -> (DeviceToolDelivery, DeviceArtifactExportManifest) {
    let ids = [ArtifactId::generate(), ArtifactId::generate()];
    let bytes = serde_json::to_vec(&serde_json::json!({"purpose":"reply","artifacts":ids.iter().enumerate().map(|(i,id)|
        serde_json::json!({"artifact_ref":{"kind":"artifact","id":id.to_string()},"artifact_tool_call_id":format!("native-{i}")})
    ).collect::<Vec<_>>()})).expect("valid test fixture");
    let request_ref = reference("application/json", &bytes);
    let delivery = DeviceToolDelivery::new(
        DeviceDeliveryBinding {
            work_item: None,
            intent_id: UserIntentId::generate(),
            run_id: RunId::generate(),
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            source_digest: Sha256::digest(b"source"),
            snapshot_hash: Sha256::digest(b"snapshot"),
            grant_proof_hash: Sha256::digest(b"grant"),
        },
        InvocationId::generate(),
        "artifacts.deliver@2".into(),
        request_ref,
        bytes,
    )
    .expect("valid test fixture");
    let scope = DeviceArtifactExportScope {
        tenant_id: TenantId::generate(),
        owner_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
        dialogue_id: AgentDialogueId::generate(),
        conversation_id: ConversationId::generate(),
    };
    let entries = ids
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            let producer = InvocationId::generate();
            let at = UtcTimestamp::from_unix_millis(100);
            let mut content = reference(
                if i == 0 {
                    "video/mp4"
                } else {
                    "application/zip"
                },
                b"whole-source-content",
            );
            content.byte_length = (12 + i) as u64 * 1024 * 1024;
            DeviceArtifactExport {
                role: DeviceArtifactExportRole::Delivery,
                artifact_id: id,
                kind: ArtifactKind::File,
                title: format!("original-{i}"),
                source_activity_id: ActivityId::generate(),
                source_created_by_run_id: delivery.binding().run_id,
                source_conversation_id: scope.conversation_id,
                created_at: at,
                version: ArtifactVersion {
                    artifact_version_id: ArtifactVersionId::generate(),
                    artifact_id: id,
                    version: VersionNumber::new(1).expect("valid test fixture"),
                    content_hash: content.sha256,
                    content_ref: content,
                    dependency_refs: vec![],
                    source_invocation_id: Some(producer),
                    committed_by: ArtifactVersionAuthor::Participant(ParticipantId::generate()),
                    committed_at: at,
                },
                producer: DeviceArtifactProducer {
                    invocation_id: producer,
                    tool_call_id: ToolCallId::new(format!("native-{i}"))
                        .expect("valid test fixture"),
                    tool_key: VersionedKey::new("device.workspace.file.publish@1")
                        .expect("valid test fixture"),
                    result_ref: reference("application/json", b"{}"),
                    terminal_record_id: LedgerRecordId::generate(),
                    terminal_record_hash: Sha256::digest(b"terminal"),
                    terminal_run_sequence: RunSeq::new(3).expect("valid test fixture"),
                    terminal_at: at,
                },
            }
        })
        .collect();
    let manifest = DeviceArtifactExportManifest {
        request_fingerprint: delivery.fingerprint(),
        scope,
        entries,
    };
    (delivery, manifest)
}
fn reference(mime: &str, bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(mime).expect("valid test fixture"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
#[test]
fn large_binary_deliveries_use_staging_without_inline_bytes_or_mutating_source_versions() {
    let (delivery, manifest) = fixture();
    let staged = DeviceArtifactContents::staged(&delivery, &manifest).expect("valid test fixture");
    assert!(staged.entries().is_empty());
    staged
        .validate_for(&delivery, &manifest)
        .expect("valid test fixture");
    let mut changed = manifest.clone();
    changed.entries[0]
        .version
        .dependency_refs
        .push(reference("image/png", b"unselected"));
    assert!(staged.validate_for(&delivery, &changed).is_err());
    let mut changed = manifest.clone();
    changed.entries[0].kind = ArtifactKind::Video;
    assert!(staged.validate_for(&delivery, &changed).is_err());
}
#[test]
fn chunks_are_bound_to_original_delivery_content_and_contiguous_transport_positions() {
    let (delivery, manifest) = fixture();
    let reference = &manifest.entries[0].version.content_ref;
    let bytes = vec![7; DEVICE_ARTIFACT_CHUNK_BYTES];
    let first = device_artifact_chunk_reference(&delivery, &manifest, reference, 0, &bytes)
        .expect("valid test fixture");
    assert_eq!(
        device_artifact_chunk_reference(&delivery, &manifest, reference, 0, &bytes)
            .expect("valid test fixture"),
        first
    );
    assert_ne!(
        device_artifact_chunk_reference(
            &delivery,
            &manifest,
            reference,
            DEVICE_ARTIFACT_CHUNK_BYTES as u64,
            &bytes
        )
        .expect("valid test fixture")
        .content_id,
        first.content_id
    );
    assert!(device_artifact_chunk_reference(&delivery, &manifest, reference, 1, &bytes).is_err());
    assert!(
        device_artifact_chunk_reference(&delivery, &manifest, reference, 0, &bytes[..10]).is_err()
    );
    let mut alien = reference.clone();
    alien.content_id = ContentId::generate();
    assert!(device_artifact_chunk_reference(&delivery, &manifest, &alien, 0, &bytes).is_err());
    let mut substituted = manifest.clone();
    substituted.entries[0].version.content_ref.sha256 = Sha256::digest(b"different");
    substituted.entries[0].version.content_hash = substituted.entries[0].version.content_ref.sha256;
    assert_ne!(
        device_artifact_chunk_id(
            &delivery,
            &substituted,
            &substituted.entries[0].version.content_ref,
            0
        )
        .expect("valid test fixture"),
        first.content_id
    );
}
