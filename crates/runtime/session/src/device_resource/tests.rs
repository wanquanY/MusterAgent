use super::*;
use crate::{
    DeviceArtifactExportRole, DeviceArtifactProducer, DeviceToolDelivery, DeviceToolDeliveryReceipt,
};
use musteragent_content_ref::MediaType;
use musteragent_run::WorkerId;

fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("valid test fixture"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
fn command() -> DeviceResourceCommand {
    let request_bytes = br#"{"name":"document.txt","content":"hello"}"#.to_vec();
    let admission = DeviceDeliveryBinding {
        work_item: None,
        intent_id: UserIntentId::generate(),
        run_id: RunId::generate(),
        device_id: DeviceId::generate(),
        installation_id: DeviceInstallationId::generate(),
        source_digest: Sha256::digest(b"source"),
        snapshot_hash: Sha256::digest(b"snapshot"),
        grant_proof_hash: Sha256::digest(b"grant"),
    };
    DeviceResourceCommand::new(
        admission.clone(),
        DeviceResourceInvocation {
            run_context: RunContext {
                tenant_id: TenantId::generate(),
                conversation_id: ConversationId::generate(),
                activity_id: ActivityId::generate(),
                activity_kind: musteragent_activity::ActivityKind::Chat,
                run_id: admission.run_id,
                executor_participant_id: ParticipantId::generate(),
                requested_by_participant_id: ParticipantId::generate(),
                parent_run_id: None,
                delegation_id: None,
                execution_scope: ExecutionScope::DevicePrivate {
                    device_id: admission.device_id,
                    workspace_id: None,
                },
                control_home: RuntimeHome::Device {
                    device_id: admission.device_id,
                },
                execution_home: RuntimeHome::Device {
                    device_id: admission.device_id,
                },
                home_epoch: HomeEpoch::new(1).expect("valid test fixture"),
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
            },
            timing: InvocationExecutionTiming::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(500),
            },
            invocation_id: InvocationId::generate(),
            requested_by_attempt_id: AttemptId::generate(),
            tool_call_id: ToolCallId::new("create-file").expect("valid test fixture"),
            tool_key: ToolKey::from_versioned_key(
                VersionedKey::new("resource.file.create@3").expect("valid test fixture"),
            ),
            capability_key: CapabilityKey::from_versioned_key(
                VersionedKey::new("resource.file.create@3").expect("valid test fixture"),
            ),
            provider_id: CapabilityProviderId::generate(),
            idempotency_key: IdempotencyKey::new("same-file-create").expect("valid test fixture"),
            request_ref: reference(&request_bytes),
            request_schema: SchemaRef {
                schema_key: VersionedKey::new("resource.file.create.request@3")
                    .expect("valid test fixture"),
                schema_hash: Sha256::digest(b"request-schema"),
            },
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("resource.file.create.result@3")
                    .expect("valid test fixture"),
                schema_hash: Sha256::digest(b"result-schema"),
            },
            authorized_content: vec![],
            memory: None,
        },
        request_bytes,
    )
    .expect("valid test fixture")
}
fn attempt(command: &DeviceResourceCommand) -> AttemptContext {
    AttemptContext {
        attempt_id: AttemptId::generate(),
        run_id: command.admission().run_id,
        attempt_number: AttemptNumber::new(1).expect("valid test fixture"),
        worker_id: WorkerId::new("native-device").expect("valid test fixture"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("valid test fixture"),
        lease_expires_at: UtcTimestamp::from_unix_millis(200),
    }
}
fn receipt(command: &DeviceResourceCommand) -> DeviceResourceReceipt {
    let result_bytes = br#"{"file_ref":{"kind":"file_node","id":"placeholder"}}"#.to_vec();
    DeviceResourceReceipt {
        command_commitment: command.commitment(),
        invocation_id: command.invocation().invocation_id,
        operation: command.operation(),
        outcome: DeviceResourceOutcome::Succeeded(DeviceResourceSuccess {
            result_ref: reference(&result_bytes),
            result_schema: command.invocation().result_schema.clone(),
            result_bytes,
            authorized_content: vec![],
            produced_versions: vec![DeviceCanonicalFileVersion {
                node_id: ResourceNodeId::generate(),
                artifact_id: ArtifactId::generate(),
                artifact_version_id: ArtifactVersionId::generate(),
                version: VersionNumber::new(1).expect("valid test fixture"),
                content_ref: ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new("text/plain").expect("valid test fixture"),
                    byte_length: 5,
                    sha256: Sha256::digest(b"hello"),
                },
                dependency_refs: vec![],
            }],
        }),
        read_dependencies: vec![],
        committed_at: UtcTimestamp::from_unix_millis(100),
    }
}
#[test]
fn renewed_attempt_keeps_command_and_receipt_identity_but_changes_current_proof() {
    let command = command();
    let receipt = receipt(&command);
    let old = attempt(&command);
    let first =
        DeviceResourceRequest::new(command.clone(), old.clone()).expect("valid test fixture");
    let next = DeviceResourceRequest::new(
        command.clone(),
        AttemptContext {
            attempt_id: AttemptId::generate(),
            attempt_number: AttemptNumber::new(2).expect("valid test fixture"),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(2).expect("valid test fixture"),
            lease_expires_at: UtcTimestamp::from_unix_millis(400),
            ..old
        },
    )
    .expect("valid test fixture");
    assert_eq!(first.command().commitment(), next.command().commitment());
    assert_ne!(
        first.proof_digest().expect("valid test fixture"),
        next.proof_digest().expect("valid test fixture")
    );
    assert_eq!(
        receipt
            .commitment(first.command())
            .expect("valid test fixture"),
        receipt
            .commitment(next.command())
            .expect("valid test fixture")
    );
    assert!(
        first
            .validate_at(UtcTimestamp::from_unix_millis(200))
            .is_err()
    );
    assert!(
        next.validate_at(UtcTimestamp::from_unix_millis(200))
            .is_ok()
    );
    assert_eq!(
        DeviceResourceRequest::decode(&next.encode().expect("valid test fixture"))
            .expect("valid test fixture"),
        next
    );
}
#[test]
fn changed_body_schema_provider_or_admission_cannot_reuse_receipt() {
    let command = command();
    let receipt = receipt(&command);
    for kind in 0..4 {
        let mut admission = command.admission().clone();
        let mut invocation = command.invocation().clone();
        let mut bytes = command.request_bytes().to_vec();
        match kind {
            0 => {
                bytes.push(b' ');
                invocation.request_ref.byte_length = bytes.len() as u64;
                invocation.request_ref.sha256 = Sha256::digest(&bytes);
            }
            1 => invocation.request_schema.schema_hash = Sha256::digest(b"changed-schema"),
            2 => invocation.provider_id = CapabilityProviderId::generate(),
            _ => admission.grant_proof_hash = Sha256::digest(b"another-grant"),
        }
        let changed =
            DeviceResourceCommand::new(admission, invocation, bytes).expect("valid test fixture");
        assert_ne!(changed.commitment(), command.commitment());
        assert!(receipt.validate_for(&changed).is_err());
    }
}
#[test]
fn canonical_encoding_rejects_unknown_fields_duplicates_and_alternate_json() {
    let command = command();
    let bytes = command.encode().expect("valid test fixture");
    assert_eq!(
        DeviceResourceCommand::decode(&bytes).expect("valid test fixture"),
        command
    );
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("valid test fixture");
    value["extra"] = true.into();
    assert!(
        DeviceResourceCommand::decode(&serde_json::to_vec(&value).expect("valid test fixture"))
            .is_err()
    );
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert!(DeviceResourceCommand::decode(&changed).is_err());
    let original = String::from_utf8(bytes).expect("valid test fixture");
    let duplicate = original.replacen(
        "{",
        "{\"schema\":\"musteragent.device-resource-command@1\",",
        1,
    );
    assert!(DeviceResourceCommand::decode(duplicate.as_bytes()).is_err());
}
#[test]
fn original_bytes_are_not_reinterpreted_and_unknown_tool_is_rejected() {
    let command = command();
    let mut invocation = command.invocation().clone();
    invocation.tool_key = ToolKey::from_versioned_key(
        VersionedKey::new("artifact.file.create@1").expect("valid test fixture"),
    );
    assert!(
        DeviceResourceCommand::new(
            command.admission().clone(),
            invocation,
            command.request_bytes().to_vec()
        )
        .is_err()
    );
    let mut invocation = command.invocation().clone();
    invocation.request_ref.sha256 = Sha256::digest(b"different bytes");
    assert!(
        DeviceResourceCommand::new(
            command.admission().clone(),
            invocation,
            command.request_bytes().to_vec()
        )
        .is_err()
    );
}
#[test]
fn reads_cannot_masquerade_as_version_producers() {
    let create = command();
    let mut i = create.invocation().clone();
    i.tool_key =
        ToolKey::from_versioned_key(VersionedKey::new("files.read@5").expect("valid test fixture"));
    i.capability_key = CapabilityKey::from_versioned_key(
        VersionedKey::new("files.read@5").expect("valid test fixture"),
    );
    let read = DeviceResourceCommand::new(
        create.admission().clone(),
        i,
        create.request_bytes().to_vec(),
    )
    .expect("valid test fixture");
    let mut receipt = receipt(&read);
    assert!(receipt.validate_for(&read).is_err());
    let DeviceResourceOutcome::Succeeded(success) = &mut receipt.outcome else {
        panic!("expected success")
    };
    success.produced_versions.clear();
    assert!(receipt.validate_for(&read).is_ok());
}
#[test]
fn canonical_publication_requires_exact_current_successful_producer_and_typed_ack() {
    let command = command();
    let receipt = receipt(&command);
    let version = receipt
        .success()
        .expect("valid test fixture")
        .produced_versions[0]
        .clone();
    let producer = DeviceArtifactProducer {
        invocation_id: command.invocation().invocation_id,
        tool_call_id: command.invocation().tool_call_id.clone(),
        tool_key: VersionedKey::new(command.invocation().tool_key.as_str())
            .expect("valid test fixture"),
        result_ref: receipt
            .success()
            .expect("valid test fixture")
            .result_ref
            .clone(),
        terminal_record_id: LedgerRecordId::generate(),
        terminal_record_hash: Sha256::digest(b"terminal"),
        terminal_run_sequence: RunSeq::new(10).expect("valid test fixture"),
        terminal_at: UtcTimestamp::from_unix_millis(110),
    };
    let entry = DeviceResourceDeliveryReference::new(
        DeviceArtifactExportRole::Delivery,
        &command,
        &receipt,
        version.clone(),
        producer,
    )
    .expect("valid test fixture");
    let bytes = serde_json::to_vec(&serde_json::json!({"purpose":"reply","artifacts":[{"artifact_ref":{"kind":"file_node","id":version.node_id.to_string()},"artifact_tool_call_id":command.invocation().tool_call_id.as_str()}]})).expect("valid test fixture");
    let delivery = DeviceToolDelivery::new(
        command.admission().clone(),
        InvocationId::generate(),
        "artifacts.deliver@1".into(),
        reference(&bytes),
        bytes,
    )
    .expect("valid test fixture");
    let manifest = DeviceResourceDeliveryManifest {
        request_fingerprint: delivery.fingerprint(),
        entries: vec![entry],
    };
    manifest
        .validate_for(&delivery)
        .expect("valid test fixture");
    assert_eq!(
        DeviceResourceDeliveryManifest::decode(
            &manifest.encode(&delivery).expect("valid test fixture"),
            &delivery
        )
        .expect("valid test fixture"),
        manifest
    );
    let mut stale = manifest.clone();
    stale.entries[0].admission.run_id = RunId::generate();
    assert!(stale.validate_for(&delivery).is_err());
    let mut wrong = manifest.clone();
    wrong.entries[0].producer.result_ref = reference(b"{}");
    assert!(
        wrong.entries[0]
            .validate_against(&command, &receipt)
            .is_err()
    );
    let bytes = delivery.result_bytes().expect("valid test fixture");
    let mut result_ref = reference(&bytes);
    result_ref.content_id = ContentId::from_uuid(uuid::Uuid::new_v5(
        &delivery.invocation_id().into_uuid(),
        b"public-tool-receipt@1",
    ))
    .expect("valid test fixture");
    let ordinary = DeviceToolDeliveryReceipt {
        invocation_id: delivery.invocation_id(),
        fingerprint: delivery.fingerprint(),
        entry_ids: vec![DialogueEntryId::generate()],
        result_ref,
        result_bytes: bytes,
    };
    ordinary
        .validate_for(&delivery)
        .expect("valid test fixture");
    let mut ack = DeviceResourceDeliveryReceipt {
        publication: ordinary,
        resource_commitment: manifest.commitment(&delivery).expect("valid test fixture"),
    };
    ack.validate_for(&delivery, &manifest)
        .expect("valid test fixture");
    ack.resource_commitment = delivery.fingerprint();
    assert!(ack.validate_for(&delivery, &manifest).is_err());
}

#[test]
fn read_dependencies_are_bound_retention_facts_without_adding_authorization() {
    let command = command();
    let mut receipt = receipt(&command);
    let original = receipt.commitment(&command).expect("valid test fixture");
    let reference = reference(b"private source");
    receipt.read_dependencies.push(reference.clone());
    assert_ne!(
        receipt.commitment(&command).expect("valid test fixture"),
        original
    );
    assert!(
        receipt
            .success()
            .expect("valid test fixture")
            .authorized_content
            .is_empty()
    );
    let encoded = receipt.encode(&command).expect("valid test fixture");
    assert_eq!(
        DeviceResourceReceipt::decode(&encoded, &command).expect("valid test fixture"),
        receipt
    );
    let encoded: serde_json::Value = serde_json::from_slice(&encoded).expect("valid test fixture");
    assert_eq!(
        encoded["read_dependencies"][0]["id"],
        reference.content_id.to_string()
    );
    assert_eq!(
        encoded["read_dependencies"][0]["sha256"],
        reference.sha256.to_string()
    );
    receipt.read_dependencies.push(reference);
    assert!(receipt.validate_for(&command).is_err());
    receipt.read_dependencies.truncate(1);
    receipt.read_dependencies[0] = command.invocation().request_ref.clone();
    receipt.read_dependencies[0].sha256 = Sha256::digest(b"conflicting identity");
    assert!(receipt.validate_for(&command).is_err());
}
#[test]
fn read_dependency_set_requires_canonical_order_and_complete_field() {
    let command = command();
    let mut receipt = receipt(&command);
    receipt.read_dependencies = vec![reference(b"one"), reference(b"two")];
    receipt.read_dependencies.sort_by_key(|r| r.content_id);
    assert!(receipt.validate_for(&command).is_ok());
    let bytes = receipt.encode(&command).expect("valid test fixture");
    receipt.read_dependencies.reverse();
    assert!(receipt.validate_for(&command).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("valid test fixture");
    value
        .as_object_mut()
        .expect("valid test fixture")
        .remove("read_dependencies");
    assert!(
        DeviceResourceReceipt::decode(
            &serde_json::to_vec(&value).expect("valid test fixture"),
            &command
        )
        .is_err()
    );
}
#[test]
fn durable_rejection_cannot_carry_success_content_or_publish_a_version() {
    let command = command();
    let success = receipt(&command);
    let mut rejected = success.clone();
    rejected.outcome = DeviceResourceOutcome::Rejected(DeviceResourceRejectionCode::Conflict);
    assert!(rejected.success().is_none());
    assert_eq!(
        rejected.rejection(),
        Some(DeviceResourceRejectionCode::Conflict)
    );
    assert_ne!(
        rejected.commitment(&command).expect("valid test fixture"),
        success.commitment(&command).expect("valid test fixture")
    );
    let bytes = rejected.encode(&command).expect("valid test fixture");
    assert_eq!(
        DeviceResourceReceipt::decode(&bytes, &command).expect("valid test fixture"),
        rejected
    );
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("valid test fixture");
    assert_eq!(
        value["outcome"],
        serde_json::json!({"kind":"rejected", "code":"conflict"})
    );
    value["outcome"]["result_ref"] = serde_json::json!({});
    assert!(
        DeviceResourceReceipt::decode(
            &serde_json::to_vec(&value).expect("valid test fixture"),
            &command
        )
        .is_err()
    );
    let version = success
        .success()
        .expect("valid test fixture")
        .produced_versions[0]
        .clone();
    let producer = DeviceArtifactProducer {
        invocation_id: command.invocation().invocation_id,
        tool_call_id: command.invocation().tool_call_id.clone(),
        tool_key: VersionedKey::new(command.invocation().tool_key.as_str())
            .expect("valid test fixture"),
        result_ref: success
            .success()
            .expect("valid test fixture")
            .result_ref
            .clone(),
        terminal_record_id: LedgerRecordId::generate(),
        terminal_record_hash: Sha256::digest(b"terminal"),
        terminal_run_sequence: RunSeq::new(10).expect("valid test fixture"),
        terminal_at: UtcTimestamp::from_unix_millis(110),
    };
    assert!(
        DeviceResourceDeliveryReference::new(
            DeviceArtifactExportRole::Delivery,
            &command,
            &rejected,
            version,
            producer
        )
        .is_err()
    );
}

#[test]
fn new_write_releases_keep_exact_invocation_identity_under_stable_operation() {
    let base = command();
    for (old, new) in [
        ("resource.file.create@3", "resource.file.create@4"),
        ("resource.file.edit.begin@4", "resource.file.edit.begin@5"),
        ("resource.file.edit.append@3", "resource.file.edit.append@4"),
        ("resource.file.edit.patch@5", "resource.file.edit.patch@6"),
        (
            "resource.file.edit.finalize@3",
            "resource.file.edit.finalize@4",
        ),
        ("resource.file.edit.abort@3", "resource.file.edit.abort@4"),
        ("resource.folder.create@4", "resource.folder.create@5"),
        ("resource.node.rename@4", "resource.node.rename@5"),
        ("resource.node.move@3", "resource.node.move@4"),
        ("resource.node.trash@3", "resource.node.trash@4"),
        ("resource.node.restore@3", "resource.node.restore@4"),
        (
            "resource.change-set.commit@5",
            "resource.change-set.commit@6",
        ),
    ] {
        let with_key = |key: &str| {
            let mut invocation = base.invocation().clone();
            invocation.tool_key =
                ToolKey::from_versioned_key(VersionedKey::new(key).expect("valid test fixture"));
            invocation.capability_key = CapabilityKey::from_versioned_key(
                VersionedKey::new(key).expect("valid test fixture"),
            );
            DeviceResourceCommand::new(
                base.admission().clone(),
                invocation,
                base.request_bytes().to_vec(),
            )
            .expect("valid test fixture")
        };
        let old = with_key(old);
        let new = with_key(new);
        assert_eq!(old.operation(), new.operation());
        assert_ne!(old.commitment(), new.commitment());
        assert_eq!(
            DeviceResourceCommand::decode(&new.encode().expect("valid test fixture"))
                .expect("valid test fixture"),
            new
        );
        let rejected = DeviceResourceReceipt {
            command_commitment: new.commitment(),
            invocation_id: new.invocation().invocation_id,
            operation: new.operation(),
            outcome: DeviceResourceOutcome::Rejected(DeviceResourceRejectionCode::Conflict),
            read_dependencies: vec![],
            committed_at: UtcTimestamp::from_unix_millis(100),
        };
        assert_eq!(
            DeviceResourceReceipt::decode(
                &rejected.encode(&new).expect("valid test fixture"),
                &new
            )
            .expect("valid test fixture"),
            rejected
        );
        assert!(rejected.validate_for(&old).is_err());
        let mut mismatched = new.invocation().clone();
        mismatched.capability_key = old.invocation().capability_key.clone();
        assert!(
            DeviceResourceCommand::new(
                new.admission().clone(),
                mismatched,
                new.request_bytes().to_vec()
            )
            .is_err()
        );
    }
    assert!(DeviceResourceOperation::EditResume.is_read_only());
    assert!(!DeviceResourceOperation::EditBegin.is_read_only());
    assert!(DeviceResourceOperation::from_tool_key("resource.file.create@5").is_err());
}
#[test]
fn publication_producer_must_keep_actual_tool_release_not_just_operation() {
    let base = command();
    let mut invocation = base.invocation().clone();
    invocation.tool_key = ToolKey::from_versioned_key(
        VersionedKey::new("resource.file.create@4").expect("valid test fixture"),
    );
    invocation.capability_key = CapabilityKey::from_versioned_key(
        VersionedKey::new("resource.file.create@4").expect("valid test fixture"),
    );
    let command = DeviceResourceCommand::new(
        base.admission().clone(),
        invocation,
        base.request_bytes().to_vec(),
    )
    .expect("valid test fixture");
    let receipt = receipt(&command);
    let producer = DeviceArtifactProducer {
        invocation_id: command.invocation().invocation_id,
        tool_call_id: command.invocation().tool_call_id.clone(),
        tool_key: VersionedKey::new("resource.file.create@4").expect("valid test fixture"),
        result_ref: receipt
            .success()
            .expect("valid test fixture")
            .result_ref
            .clone(),
        terminal_record_id: LedgerRecordId::generate(),
        terminal_record_hash: Sha256::digest(b"terminal"),
        terminal_run_sequence: RunSeq::new(10).expect("valid test fixture"),
        terminal_at: UtcTimestamp::from_unix_millis(110),
    };
    let mut reference = DeviceResourceDeliveryReference::new(
        DeviceArtifactExportRole::Delivery,
        &command,
        &receipt,
        receipt
            .success()
            .expect("valid test fixture")
            .produced_versions[0]
            .clone(),
        producer,
    )
    .expect("valid test fixture");
    reference.producer.tool_key =
        VersionedKey::new("resource.file.create@3").expect("valid test fixture");
    assert!(reference.validate_against(&command, &receipt).is_err());
}

#[test]
fn forward_read_releases_preserve_readonly_operation_and_exact_command_identity() {
    let base = command();
    for (old_key, new_key) in [
        ("resource.file.read@4", "resource.file.read@5"),
        ("resource.file.edit.resume@4", "resource.file.edit.resume@5"),
        ("resource.search@3", "resource.search@4"),
        ("resource.tree.list@3", "resource.tree.list@4"),
    ] {
        let with_key = |key: &str| {
            let mut invocation = base.invocation().clone();
            invocation.tool_key =
                ToolKey::from_versioned_key(VersionedKey::new(key).expect("valid test fixture"));
            invocation.capability_key = CapabilityKey::from_versioned_key(
                VersionedKey::new(key).expect("valid test fixture"),
            );
            DeviceResourceCommand::new(
                base.admission().clone(),
                invocation,
                base.request_bytes().to_vec(),
            )
            .expect("valid test fixture")
        };
        let old = with_key(old_key);
        let new = with_key(new_key);
        assert_eq!(old.operation(), new.operation());
        assert!(new.operation().is_read_only());
        assert!(!new.operation().can_produce_versions());
        assert_ne!(old.commitment(), new.commitment());
        assert_eq!(
            DeviceResourceCommand::decode(&new.encode().expect("valid test fixture"))
                .expect("valid test fixture"),
            new
        );
    }
}

#[test]
fn physical_workspace_is_versioned_and_committed_without_changing_legacy_bytes() {
    let original = command();
    let legacy_bytes = original.encode().expect("valid test fixture");
    assert!(!String::from_utf8_lossy(&legacy_bytes).contains("workspace_id"));
    assert_eq!(
        DeviceResourceCommand::decode(&legacy_bytes)
            .expect("valid test fixture")
            .encode()
            .expect("valid test fixture"),
        legacy_bytes
    );
    let first = WorkspaceId::generate();
    let mut invocation = original.invocation().clone();
    invocation.run_context.execution_scope = ExecutionScope::DevicePrivate {
        device_id: original.admission().device_id,
        workspace_id: Some(first),
    };
    let scoped = DeviceResourceCommand::new(
        original.admission().clone(),
        invocation.clone(),
        original.request_bytes().to_vec(),
    )
    .expect("valid test fixture");
    let encoded = scoped.encode().expect("valid test fixture");
    assert!(String::from_utf8_lossy(&encoded).contains("resource-command@2"));
    assert_eq!(
        DeviceResourceCommand::decode(&encoded).expect("valid test fixture"),
        scoped
    );
    assert_ne!(scoped.commitment(), original.commitment());
    let previous = receipt(&original);
    assert!(previous.commitment(&scoped).is_err());
    invocation.run_context.execution_scope = ExecutionScope::DevicePrivate {
        device_id: original.admission().device_id,
        workspace_id: Some(WorkspaceId::generate()),
    };
    let other = DeviceResourceCommand::new(
        original.admission().clone(),
        invocation,
        original.request_bytes().to_vec(),
    )
    .expect("valid test fixture");
    assert_ne!(other.commitment(), scoped.commitment());
    let downgraded = String::from_utf8(encoded)
        .expect("valid test fixture")
        .replace("resource-command@2", "resource-command@1");
    assert!(DeviceResourceCommand::decode(downgraded.as_bytes()).is_err());
    let upgraded = String::from_utf8(legacy_bytes)
        .expect("valid test fixture")
        .replace("resource-command@1", "resource-command@2");
    assert!(DeviceResourceCommand::decode(upgraded.as_bytes()).is_err());
    let request =
        DeviceResourceRequest::new(scoped.clone(), attempt(&scoped)).expect("valid test fixture");
    assert_eq!(
        DeviceResourceRequest::decode(&request.encode().expect("valid test fixture"))
            .expect("valid test fixture"),
        request
    );
}

#[test]
fn native_memory_context_and_approval_only_mode_survive_exact_transport() {
    let original = command();
    let mut invocation = original.invocation().clone();
    invocation.tool_key = musteragent_tool::ToolKey::from_versioned_key(
        VersionedKey::new("user_memory.remember@2").expect("valid test fixture"),
    );
    invocation.capability_key = CapabilityKey::from_versioned_key(
        VersionedKey::new("user_memory.remember@2").expect("valid test fixture"),
    );
    invocation.memory = Some(DeviceMemoryContext {
        evidence_record_id: musteragent_identity::LedgerRecordId::generate(),
        approval_interaction_id: None,
        user_origin: None,
    });
    let command = DeviceResourceCommand::new(
        original.admission().clone(),
        invocation.clone(),
        original.request_bytes().to_vec(),
    )
    .expect("valid test fixture");
    assert_eq!(
        DeviceResourceCommand::decode(&command.encode().expect("valid test fixture"))
            .expect("valid test fixture"),
        command
    );
    let request = DeviceResourceRequest::new(command.clone(), attempt(&command))
        .expect("valid test fixture")
        .for_memory_approval()
        .expect("valid test fixture");
    assert_eq!(
        DeviceResourceRequest::decode(&request.encode().expect("valid test fixture"))
            .expect("valid test fixture"),
        request
    );
    invocation.memory = None;
    assert!(
        DeviceResourceCommand::new(
            original.admission().clone(),
            invocation,
            original.request_bytes().to_vec()
        )
        .is_err()
    );
}

#[path = "task_tests.rs"]
mod tasks;
