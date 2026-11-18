#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;

fn rejection() -> PublicDeliveryRejectionV1 {
    let bytes = b"# A useful original answer";
    PublicDeliveryRejectionV1 {
        candidate: PublicDeliveryCandidateProof {
            invocation_id: InvocationId::generate(),
            assistant_message_id: MessageId::generate(),
            assistant_output_id: OutputId::generate(),
            ledger_cutoff: ConversationSeq::new(7).unwrap(),
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                sha256: Sha256::digest(bytes),
                byte_length: bytes.len() as u64,
                media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
            },
        },
        diagnostics: vec![
            PublicDeliveryDiagnostic::new(PublicDeliveryDiagnosticKind::MalformedJson, "/")
                .unwrap(),
        ],
        disposition: PublicDeliveryRepairDisposition::FormatOnly,
        cut: PublicDeliveryRepairCut {
            progress_sequence: None,
            control_sequence: Some(ConversationSeq::new(3).unwrap()),
        },
    }
}
#[test]
fn v6_pins_repair_without_rewriting_legacy_schema() {
    assert_eq!(
        PublicDeliveryContract::V6.schema(),
        PublicDeliveryContract::V5.schema()
    );
    assert_ne!(
        PublicDeliveryContract::V6.prompt(),
        PublicDeliveryContract::V5.prompt()
    );
    let p = PublicDeliveryContract::V6.prompt();
    assert!(p.contains(&public_delivery_repair_hash().to_string()));
    assert!(p.contains(PUBLIC_DELIVERY_REPAIR_REF));
    // The existing provenance reader consumes the complete suffix as JSON.
    assert_eq!(
        p.split_once("\nSchema:\n").unwrap().1,
        PublicDeliveryContract::V6.schema()
    );
}
#[test]
fn repair_binding_pins_diagnostics_source_and_separate_episode_control_cut() {
    let original = rejection();
    let binding = original.binding().unwrap();
    binding.validate().unwrap();
    assert_eq!(
        binding.diagnostic_hash,
        public_delivery_diagnostics_hash(&original.diagnostics).unwrap()
    );
    let mut changed = binding.clone();
    changed.contract_prompt_sha256 = Sha256::digest(b"wrong");
    assert!(changed.validate().is_err());
    let mut changed = binding.clone();
    changed.rejection_invocation_id = InvocationId::generate();
    assert!(changed.validate().is_err());
    let mut changed = original.clone();
    changed.cut.control_sequence = Some(ConversationSeq::new(8).unwrap());
    assert!(changed.validate().is_err());
    assert_eq!(original.cut.progress_sequence, None);
    let mut changed = original.clone();
    changed.cut.control_sequence = None;
    assert_eq!(
        changed.cut.progress_sequence,
        original.cut.progress_sequence
    );
    assert_ne!(changed.binding().unwrap(), binding);
}
#[test]
fn reference_loss_is_never_format_only_and_paths_never_leak_values() {
    for bytes in [
        br#"{"deliveries":[{"kind":"interaction","purpose":"clarification","interaction_id":"private-placeholder"}]}"#.as_slice(),
        br#"{"deliveries":[{"kind":"artifact","purpose":"result","artifact_ref":{"kind":"artifact","id":"private-placeholder"},"artifact_tool_call_id":"call-a"}]}"#.as_slice(),
        br#"{"deliveries":[{"kind":"partner_created","purpose":"result","creation_command_id":"private-placeholder","partner_tool_call_id":"call-a"}]}"#.as_slice(),
        br#"{"deliveries":[{"kind":"interaction","purpose":"clarification"}]}"#.as_slice(),
    ] {
        let failure=parse_public_delivery_v6(bytes).unwrap_err();
        assert_eq!(PublicDeliveryRepairDisposition::for_diagnostics(&failure.diagnostics).unwrap(),PublicDeliveryRepairDisposition::PrerequisiteRecovery);
        assert!(failure.diagnostics.iter().all(|v|!v.path().contains("private-placeholder")));
    }
    assert!(
        PublicDeliveryDiagnostic::new(
            PublicDeliveryDiagnosticKind::UnexpectedProperty,
            "/secret-token"
        )
        .is_err()
    );
    assert!(
        PublicDeliveryDiagnostic::new(
            PublicDeliveryDiagnosticKind::InvalidType,
            "/deliveries/016/text"
        )
        .is_err()
    );
}
#[test]
fn strict_acceptance_and_duplicates_use_original_parser() {
    let valid = br#"{"deliveries":[{"kind":"message","purpose":"reply","text":"3127"}]}"#;
    assert_eq!(
        parse_public_delivery_v6(valid).unwrap(),
        PublicDeliveryContract::V5.parse(valid).unwrap()
    );
    for bytes in [
        br#"{"deliveries":[{"kind":"message","purpose":"reply","text":"one","text":"two"}]}"#.as_slice(),
        br#"{"deliveries":[{"kind":"message","purpose":"reply","text":"one","secret-token":"secret-value"}]}"#.as_slice(),
        br#"{"deliveries":[{"kind":"message","purpose":"reply"}]}"#.as_slice(),
        b"# Plain Markdown",
        b"{\"deliveries\":[",
    ] {
        assert!(PublicDeliveryContract::V5.parse(bytes).is_err());
        let failure=parse_public_delivery_v6(bytes).unwrap_err();
        assert!(failure.diagnostics.len()<=MAX_DELIVERY_DIAGNOSTICS);
        public_delivery_diagnostics_hash(&failure.diagnostics).unwrap();
        assert!(failure.diagnostics.iter().all(|v|!v.path().contains("secret")));
    }
}
#[test]
fn candidate_bounds_are_complete_bytes_never_a_truncated_replay() {
    let mut value = rejection();
    value.candidate.content_ref.byte_length = MAX_PUBLIC_DELIVERY_BYTES as u64;
    assert!(value.validate().is_ok());
    value.candidate.content_ref.byte_length += 1;
    assert!(value.validate().is_err());
    let output = PublicDeliveryUnreplayableOutput {
        invocation_id: value.candidate.invocation_id,
        assistant_message_id: value.candidate.assistant_message_id,
        assistant_output_id: value.candidate.assistant_output_id,
        ledger_cutoff: value.candidate.ledger_cutoff,
        byte_length: value.candidate.content_ref.byte_length,
        sha256: value.candidate.content_ref.sha256,
        cut: value.cut,
    };
    assert!(output.validate().is_ok());
    let mut invalid = output;
    invalid.byte_length = MAX_PUBLIC_DELIVERY_BYTES as u64;
    assert!(invalid.validate().is_err());
}
