use super::*;
use musteragent_content_ref::MediaType;
use musteragent_execution_context::DIALOGUE_PUBLISH_TOOL;
use musteragent_identity::ContentId;

fn request() -> DeviceToolDelivery {
    let bytes = br#"{"text":"The requested report is ready.","purpose":"reply"}"#.to_vec();
    DeviceToolDelivery::new(
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
        DIALOGUE_PUBLISH_TOOL.into(),
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("valid test fixture"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        },
        bytes,
    )
    .expect("valid test fixture")
}
fn receipt(delivery: &DeviceToolDelivery) -> DeviceToolDeliveryReceipt {
    let bytes = delivery.result_bytes().expect("valid test fixture");
    DeviceToolDeliveryReceipt {
        invocation_id: delivery.invocation_id(),
        fingerprint: delivery.fingerprint(),
        entry_ids: vec![DialogueEntryId::generate()],
        result_ref: ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &delivery.invocation_id().into_uuid(),
                b"public-tool-receipt@1",
            ))
            .expect("valid test fixture"),
            media_type: MediaType::new("application/json").expect("valid test fixture"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        },
        result_bytes: bytes,
    }
}
#[test]
fn original_tool_decoder_rejects_arbitrary_text_unregistered_tools_and_tampered_content() {
    let d = request();
    for (key, bytes) in [
        (DIALOGUE_PUBLISH_TOOL, b"I have completed it".to_vec()),
        ("dialogue.publish@2", d.request_bytes().to_vec()),
        (
            DIALOGUE_PUBLISH_TOOL,
            br#"{"text":"changed","purpose":"reply"}"#.to_vec(),
        ),
    ] {
        assert!(
            DeviceToolDelivery::new(
                d.binding.clone(),
                d.invocation_id,
                key.into(),
                d.request_ref.clone(),
                bytes
            )
            .is_err()
        );
    }
    let mut missing = d.binding.clone();
    missing.source_digest = Sha256::from_bytes(vec![0; 32]).expect("valid test fixture");
    assert!(
        DeviceToolDelivery::new(
            missing,
            d.invocation_id,
            d.tool_key.clone(),
            d.request_ref.clone(),
            d.request_bytes.clone()
        )
        .is_err()
    );
}
#[test]
fn receipt_is_exact_across_retries_and_does_not_ack_a_different_custody_or_invocation() {
    let d = request();
    let r = receipt(&d);
    r.validate_for(&d).expect("valid test fixture");
    r.validate_for(&d.clone()).expect("valid test fixture");
    for case in 0..7 {
        let mut b = d.binding.clone();
        let invocation = if case == 6 {
            InvocationId::generate()
        } else {
            d.invocation_id
        };
        match case {
            0 => b.intent_id = UserIntentId::generate(),
            1 => b.run_id = RunId::generate(),
            2 => b.device_id = DeviceId::generate(),
            3 => b.installation_id = DeviceInstallationId::generate(),
            4 => b.snapshot_hash = Sha256::digest(b"different snapshot"),
            5 => b.grant_proof_hash = Sha256::digest(b"different grant"),
            _ => (),
        }
        let other = DeviceToolDelivery::new(
            b,
            invocation,
            d.tool_key.clone(),
            d.request_ref.clone(),
            d.request_bytes.clone(),
        )
        .expect("valid test fixture");
        assert!(r.validate_for(&other).is_err(), "custody case {case}");
    }
}
#[test]
fn acked_result_requires_real_entry_identity_exact_result_bytes_and_stable_content_identity() {
    let d = request();
    for case in 0..5 {
        let mut r = receipt(&d);
        match case {
            0 => r.entry_ids.clear(),
            1 => r.entry_ids.push(r.entry_ids[0]),
            2 => r.result_bytes = br#"{"committed":true}"#.to_vec(),
            3 => r.result_ref.content_id = ContentId::generate(),
            _ => r.fingerprint = Sha256::digest(b"another publication"),
        }
        assert!(r.validate_for(&d).is_err(), "receipt case {case}");
    }
}
