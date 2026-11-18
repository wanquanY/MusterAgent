#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;

fn proof(bytes: &[u8]) -> PublicDeliveryCandidateProof {
    PublicDeliveryCandidateProof {
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
    }
}
fn cut() -> PublicDeliveryRepairCut {
    PublicDeliveryRepairCut {
        progress_sequence: None,
        control_sequence: None,
    }
}
#[test]
fn v7_seals_strict_failure_policy_with_unchanged_typed_payload() {
    let v = PublicDeliveryContract::V7;
    assert_eq!(PublicDeliveryContract::CURRENT, v);
    assert_eq!(v.schema(), PublicDeliveryContract::V5.schema());
    assert_eq!(v.hash(), PublicDeliveryContract::V6.hash());
    let prompt = v.prompt();
    assert!(prompt.contains(PUBLIC_DELIVERY_RECOVERY_V7_REF));
    assert!(prompt.contains(&public_delivery_recovery_v7_hash().to_string()));
    assert_eq!(prompt.split_once("\nSchema:\n").unwrap().1, v.schema());
    assert_eq!(
        PublicDeliveryContract::from_prompt_ref(&proof(prompt.as_bytes()).content_ref),
        Some(v)
    );
    assert_ne!(prompt, PublicDeliveryContract::V6.prompt());
    assert!(PUBLIC_DELIVERY_RECOVERY_V7_SPEC.contains("terminal_without_rewrite"));
}
#[test]
fn only_complete_strict_envelope_is_accepted_no_aliases_or_normalization() {
    let valid = r#"{"deliveries":[{"kind":"message","purpose":"reply","text":"A useful answer"}]}"#;
    let bytes = valid.as_bytes();
    assert_eq!(
        parse_public_delivery_v7(bytes).unwrap(),
        PublicDeliveryContract::V6.parse(bytes).unwrap()
    );
    for raw in ["# A useful answer".to_string(), format!("```json\n{valid}\n```"),
        r#"{"kind":"message","purpose":"reply","text":"A useful answer"}"#.into(),
        r#"[{"kind":"message","purpose":"reply","text":"A useful answer"}]"#.into(),
        r#"{"deliveries":[{"kind":"message","purpose":"reply","text":"A","text":"B"}]}"#.into(),
        r#"{"deliveries":[{"kind":"interaction","purpose":"required_decision","interaction_id":"invalid"},{"kind":"message","purpose":"reply"}]}"#.into(),
        r#"{"deliveries":[{"kind":"message","purpose":"reply","text":"A"}],"deliveries":[]}"#.into() ] {
        let failure = parse_public_delivery_v7(raw.as_bytes()).unwrap_err();
        let terminal = PublicDeliveryFormatRejection { candidate: proof(raw.as_bytes()), diagnostics: failure.diagnostics.clone(), cut: cut() };
        terminal.validate().unwrap();
        // Even a bounded diagnostic headed by reference syntax is not recovery.
        let legacy = PublicDeliveryRejectionV1 { candidate: terminal.candidate, diagnostics: failure.diagnostics.clone(),
            disposition: PublicDeliveryRepairDisposition::for_diagnostics(&failure.diagnostics).unwrap(), cut: cut() };
        assert!(legacy.binding_for(PublicDeliveryContract::V7).is_err(), "{raw}");
    }
}
#[test]
fn v7_recovery_requires_authoritative_unavailable_diagnostic_and_distinct_policy_identity() {
    let bytes = r#"{"deliveries":[{"kind":"interaction","purpose":"required_decision","interaction_id":"019b2f80-0000-7000-8000-000000000002"}]}"#.as_bytes();
    assert!(parse_public_delivery_v7(bytes).is_ok());
    let repair = PublicDeliveryRejectionV1 {
        candidate: proof(bytes),
        diagnostics: vec![
            PublicDeliveryDiagnostic::new(
                PublicDeliveryDiagnosticKind::ReferenceUnavailable,
                "/deliveries/0/interaction_id",
            )
            .unwrap(),
        ],
        disposition: PublicDeliveryRepairDisposition::PrerequisiteRecovery,
        cut: cut(),
    };
    let binding = repair.binding_for(PublicDeliveryContract::V7).unwrap();
    binding.validate().unwrap();
    assert_ne!(binding, repair.binding().unwrap());
    let mut forged = binding.clone();
    forged.disposition = PublicDeliveryRepairDisposition::FormatOnly;
    assert!(forged.validate().is_err());
    let mut forged = binding.clone();
    forged.repair_spec_ref = VersionedKey::new(PUBLIC_DELIVERY_REPAIR_REF).unwrap();
    assert!(forged.validate().is_err());
    let mut mixed = repair;
    mixed.diagnostics.push(
        PublicDeliveryDiagnostic::new(
            PublicDeliveryDiagnosticKind::MissingRequired,
            "/deliveries/1/text",
        )
        .unwrap(),
    );
    mixed.diagnostics.sort();
    assert!(mixed.binding_for(PublicDeliveryContract::V7).is_err());
}
