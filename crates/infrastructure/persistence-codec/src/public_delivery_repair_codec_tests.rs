use crate::public_delivery_repair_codec::*;
use crate::wire;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;
use prost::Message;
pub(crate) fn rejection() -> PublicDeliveryRejectionV1 {
    let bytes = b"Original useful answer";
    PublicDeliveryRejectionV1 {
        candidate: PublicDeliveryCandidateProof {
            invocation_id: InvocationId::generate(),
            assistant_message_id: MessageId::generate(),
            assistant_output_id: OutputId::generate(),
            ledger_cutoff: ConversationSeq::new(8).unwrap(),
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(bytes),
            },
        },
        diagnostics: vec![
            PublicDeliveryDiagnostic::new(PublicDeliveryDiagnosticKind::MalformedJson, "/")
                .unwrap(),
        ],
        disposition: PublicDeliveryRepairDisposition::FormatOnly,
        cut: PublicDeliveryRepairCut {
            progress_sequence: Some(ConversationSeq::new(2).unwrap()),
            control_sequence: Some(ConversationSeq::new(4).unwrap()),
        },
    }
}
#[test]
fn private_repair_codec_roundtrips_exact_source_diagnostics_and_pinned_contract() {
    let value = rejection();
    let bytes = encode_public_delivery_rejection(&value).unwrap();
    assert_eq!(decode_public_delivery_rejection(&bytes).unwrap(), value);
    let binding = value.binding().unwrap();
    let bytes = encode_public_delivery_repair_binding(&binding).unwrap();
    assert_eq!(
        decode_public_delivery_repair_binding(&bytes).unwrap(),
        binding
    );
    let output = PublicDeliveryUnreplayableOutput {
        invocation_id: value.candidate.invocation_id,
        assistant_message_id: value.candidate.assistant_message_id,
        assistant_output_id: value.candidate.assistant_output_id,
        ledger_cutoff: value.candidate.ledger_cutoff,
        byte_length: MAX_PUBLIC_DELIVERY_BYTES as u64 + 1,
        sha256: Sha256::digest(b"too large original frames"),
        cut: value.cut,
    };
    let bytes = encode_public_delivery_unreplayable(&output).unwrap();
    assert_eq!(decode_public_delivery_unreplayable(&bytes).unwrap(), output);
}
#[test]
fn private_repair_codec_rejects_unknown_duplicate_noncanonical_and_legacy_fields() {
    let value = rejection();
    let bytes = encode_public_delivery_rejection(&value).unwrap();
    let mut unknown = bytes.clone();
    unknown.extend([0xf8, 0x07, 0x01]);
    assert!(decode_public_delivery_rejection(&unknown).is_err());
    let mut duplicate = bytes.clone();
    duplicate.extend_from_slice(&bytes);
    assert!(decode_public_delivery_rejection(&duplicate).is_err());
    assert!(
        decode_public_delivery_rejection(&vec![0; MAX_PUBLIC_REPAIR_METADATA_BYTES + 1]).is_err()
    );
    let mut w = public_delivery_rejection_message(&value).unwrap();
    w.diagnostics[0].path = "/private-token".into();
    assert!(decode_public_delivery_rejection(&w.encode_to_vec()).is_err());
    let mut w = public_delivery_rejection_message(&value).unwrap();
    w.disposition = "prerequisite_recovery".into();
    assert!(decode_public_delivery_rejection(&w.encode_to_vec()).is_err());
    let mut w = public_delivery_repair_binding_message(&value.binding().unwrap()).unwrap();
    w.contract_ref = PublicDeliveryContract::V5.key().into();
    assert!(decode_public_delivery_repair_binding(&w.encode_to_vec()).is_err());
    let mut w = public_delivery_repair_binding_message(&value.binding().unwrap()).unwrap();
    w.repair_spec_sha256 = Some(wire::Sha256 {
        value: Sha256::digest(b"different-policy").into_bytes().to_vec(),
    });
    assert!(decode_public_delivery_repair_binding(&w.encode_to_vec()).is_err());
}
