use super::*;
fn effect(index: usize) -> NativeFileRetraction {
    NativeFileRetraction {
        schema: "musteragent.native-file-retraction.v1".into(),
        tenant_id: TenantId::generate().to_string(),
        principal_id: PrincipalId::generate().to_string(),
        device_id: DeviceId::generate().to_string(),
        conversation_id: ConversationId::generate().to_string(),
        home_epoch: "1".into(),
        baseline_offset: "42".into(),
        trigger_message_id: MessageId::generate().to_string(),
        source_run_id: RunId::generate().to_string(),
        workspace_id: WorkspaceId::generate().to_string(),
        invocation_id: InvocationId::generate().to_string(),
        path: format!("file-{index}.txt"),
        sha256: "a".repeat(64),
    }
}
#[test]
fn thousands_of_effects_fit_bounded_deterministic_outbox_batches() {
    let effects = (0..4096).map(effect).collect::<Vec<_>>();
    let command = CommandId::generate();
    let forward = NativeFileRetractionBatch::outboxes(command, effects.clone()).expect("batches");
    let reverse = NativeFileRetractionBatch::outboxes(command, effects.into_iter().rev().collect())
        .expect("batches");
    assert_eq!(forward, reverse);
    assert_eq!(forward.len(), 32);
    assert!(forward.len() <= MAX_APPEND_OUTBOX_MESSAGES);
    assert!(
        forward
            .iter()
            .map(|outbox| outbox.payload.len())
            .sum::<usize>()
            < musteragent_ledger::MAX_APPEND_PAYLOAD_BYTES
    );
    for outbox in forward {
        assert_eq!(
            NativeFileRetractionBatch::decode(&outbox)
                .expect("decode")
                .effects
                .len(),
            128
        );
    }
}
#[test]
fn invalid_and_duplicate_receipts_never_enter_a_batch() {
    let valid = effect(0);
    assert!(
        NativeFileRetractionBatch::outboxes(
            CommandId::generate(),
            vec![valid.clone(), valid.clone()]
        )
        .is_none()
    );
    for path in [
        "/absolute",
        "../parent",
        ".musteragent/private",
        "dir//file",
        "dir/../file",
        "dir\\file",
    ] {
        let mut invalid = valid.clone();
        invalid.path = path.into();
        assert!(
            NativeFileRetractionBatch::outboxes(CommandId::generate(), vec![invalid]).is_none()
        );
    }
    let mut outbox = NativeFileRetractionBatch::outboxes(CommandId::generate(), vec![valid])
        .expect("batch")
        .remove(0);
    outbox.payload.push(b' ');
    assert!(NativeFileRetractionBatch::decode(&outbox).is_none());
}
