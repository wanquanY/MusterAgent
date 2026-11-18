//! One canonical private codec for Journal, ContextPlan and Home persistence.
//! Decode proves structure and immutable V6 material identity, never authority.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
use musteragent_identity::{ConversationSeq, VersionedKey};
use prost::Message;
use std::num::NonZeroUsize;

pub const MAX_PUBLIC_REPAIR_METADATA_BYTES: usize = 16 * 1024;
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("invalid private Dialogue delivery repair data")]
pub struct PublicRepairCodecError;
type Result<T> = std::result::Result<T, PublicRepairCodecError>;
fn checked<T, E>(v: std::result::Result<T, E>) -> Result<T> {
    v.map_err(|_| PublicRepairCodecError)
}
fn required<T>(v: Option<T>) -> Result<T> {
    v.ok_or(PublicRepairCodecError)
}
fn id<T: std::str::FromStr>(v: Option<String>) -> Result<T> {
    checked(required(v)?.parse())
}
fn hash(v: Option<wire::Sha256>) -> Result<Sha256> {
    checked(Sha256::from_bytes(required(v)?.value))
}
fn hash_message(v: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: v.into_bytes().to_vec(),
    }
}
fn seq(v: u64) -> Result<ConversationSeq> {
    checked(ConversationSeq::new(v))
}
fn cut(v: Option<wire::PublicDeliveryRepairCutV1>) -> Result<PublicDeliveryRepairCut> {
    let v = required(v)?;
    Ok(PublicDeliveryRepairCut {
        progress_sequence: v.progress_sequence.map(seq).transpose()?,
        control_sequence: v.control_sequence.map(seq).transpose()?,
    })
}
fn cut_message(v: PublicDeliveryRepairCut) -> wire::PublicDeliveryRepairCutV1 {
    wire::PublicDeliveryRepairCutV1 {
        progress_sequence: v.progress_sequence.map(ConversationSeq::get),
        control_sequence: v.control_sequence.map(ConversationSeq::get),
    }
}
fn proof(v: Option<wire::PublicDeliveryCandidateProofV1>) -> Result<PublicDeliveryCandidateProof> {
    let v = required(v)?;
    let result = PublicDeliveryCandidateProof {
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        assistant_message_id: id(v.assistant_message_id.map(|v| v.value))?,
        assistant_output_id: id(v.assistant_output_id.map(|v| v.value))?,
        ledger_cutoff: seq(v.ledger_cutoff)?,
        content_ref: checked(crate::wire_common::decode_content_ref(required(
            v.content_ref,
        )?))?,
    };
    checked(result.validate())?;
    Ok(result)
}
fn proof_message(v: &PublicDeliveryCandidateProof) -> Result<wire::PublicDeliveryCandidateProofV1> {
    checked(v.validate())?;
    Ok(proof_fields(v))
}
fn proof_fields(v: &PublicDeliveryCandidateProof) -> wire::PublicDeliveryCandidateProofV1 {
    wire::PublicDeliveryCandidateProofV1 {
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        assistant_message_id: Some(wire::MessageId {
            value: v.assistant_message_id.to_string(),
        }),
        assistant_output_id: Some(wire::OutputId {
            value: v.assistant_output_id.to_string(),
        }),
        ledger_cutoff: v.ledger_cutoff.get(),
        content_ref: Some(crate::encode_content_ref(&v.content_ref)),
    }
}
pub fn public_delivery_rejection_message(
    v: &PublicDeliveryRejectionV1,
) -> Result<wire::PublicDeliveryRejectionV1> {
    checked(v.validate())?;
    Ok(wire::PublicDeliveryRejectionV1 {
        candidate: Some(proof_message(&v.candidate)?),
        diagnostics: v
            .diagnostics
            .iter()
            .map(|v| wire::PublicDeliveryDiagnosticV1 {
                kind: v.kind().as_str().into(),
                path: v.path().into(),
            })
            .collect(),
        disposition: v.disposition.as_str().into(),
        cut: Some(cut_message(v.cut)),
    })
}
pub fn public_delivery_rejection_from_message(
    v: wire::PublicDeliveryRejectionV1,
) -> Result<PublicDeliveryRejectionV1> {
    let result = PublicDeliveryRejectionV1 {
        candidate: proof(v.candidate)?,
        diagnostics: v
            .diagnostics
            .into_iter()
            .map(|v| {
                checked(PublicDeliveryDiagnostic::new(
                    required(PublicDeliveryDiagnosticKind::parse(&v.kind))?,
                    v.path,
                ))
            })
            .collect::<Result<_>>()?,
        disposition: required(PublicDeliveryRepairDisposition::parse(&v.disposition))?,
        cut: cut(v.cut)?,
    };
    checked(result.validate())?;
    Ok(result)
}
pub fn public_delivery_repair_binding_message(
    v: &PublicDeliveryRepairBinding,
) -> Result<wire::PublicDeliveryRepairBindingV1> {
    checked(v.validate())?;
    Ok(public_delivery_repair_binding_fields(v))
}
/// Lossless field projection for the original infallible domain-event encoder.
/// This does not validate or authorize a binding. Command/persistence encoders
/// and all decoders retain the checked codec above and below this projection.
pub fn public_delivery_repair_binding_fields(
    v: &PublicDeliveryRepairBinding,
) -> wire::PublicDeliveryRepairBindingV1 {
    wire::PublicDeliveryRepairBindingV1 {
        contract_ref: v.contract_ref.as_str().into(),
        contract_prompt_sha256: Some(hash_message(v.contract_prompt_sha256)),
        repair_spec_ref: v.repair_spec_ref.as_str().into(),
        repair_spec_sha256: Some(hash_message(v.repair_spec_sha256)),
        rejection_invocation_id: Some(wire::InvocationId {
            value: v.rejection_invocation_id.to_string(),
        }),
        candidate: Some(proof_fields(&v.candidate)),
        diagnostic_hash: Some(hash_message(v.diagnostic_hash)),
        disposition: v.disposition.as_str().into(),
        cut: Some(cut_message(v.cut)),
    }
}
pub fn public_delivery_repair_binding_from_message(
    v: wire::PublicDeliveryRepairBindingV1,
) -> Result<PublicDeliveryRepairBinding> {
    let result = PublicDeliveryRepairBinding {
        contract_ref: checked(VersionedKey::new(v.contract_ref))?,
        contract_prompt_sha256: hash(v.contract_prompt_sha256)?,
        repair_spec_ref: checked(VersionedKey::new(v.repair_spec_ref))?,
        repair_spec_sha256: hash(v.repair_spec_sha256)?,
        rejection_invocation_id: id(v.rejection_invocation_id.map(|v| v.value))?,
        candidate: proof(v.candidate)?,
        diagnostic_hash: hash(v.diagnostic_hash)?,
        disposition: required(PublicDeliveryRepairDisposition::parse(&v.disposition))?,
        cut: cut(v.cut)?,
    };
    checked(result.validate())?;
    Ok(result)
}
pub fn public_delivery_unreplayable_message(
    v: &PublicDeliveryUnreplayableOutput,
) -> Result<wire::PublicDeliveryUnreplayableOutputV1> {
    checked(v.validate())?;
    Ok(wire::PublicDeliveryUnreplayableOutputV1 {
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        assistant_message_id: Some(wire::MessageId {
            value: v.assistant_message_id.to_string(),
        }),
        assistant_output_id: Some(wire::OutputId {
            value: v.assistant_output_id.to_string(),
        }),
        ledger_cutoff: v.ledger_cutoff.get(),
        byte_length: v.byte_length,
        sha256: Some(hash_message(v.sha256)),
        cut: Some(cut_message(v.cut)),
    })
}
pub fn public_delivery_unreplayable_from_message(
    v: wire::PublicDeliveryUnreplayableOutputV1,
) -> Result<PublicDeliveryUnreplayableOutput> {
    let result = PublicDeliveryUnreplayableOutput {
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        assistant_message_id: id(v.assistant_message_id.map(|v| v.value))?,
        assistant_output_id: id(v.assistant_output_id.map(|v| v.value))?,
        ledger_cutoff: seq(v.ledger_cutoff)?,
        byte_length: v.byte_length,
        sha256: hash(v.sha256)?,
        cut: cut(v.cut)?,
    };
    checked(result.validate())?;
    Ok(result)
}

pub fn public_delivery_format_rejection_message(
    v: &PublicDeliveryFormatRejection,
) -> Result<wire::PublicDeliveryFormatRejectionV1> {
    checked(v.validate())?;
    Ok(wire::PublicDeliveryFormatRejectionV1 {
        candidate: Some(proof_message(&v.candidate)?),
        diagnostics: v
            .diagnostics
            .iter()
            .map(|d| wire::PublicDeliveryDiagnosticV1 {
                kind: d.kind().as_str().into(),
                path: d.path().into(),
            })
            .collect(),
        cut: Some(cut_message(v.cut)),
    })
}
pub fn public_delivery_format_rejection_from_message(
    v: wire::PublicDeliveryFormatRejectionV1,
) -> Result<PublicDeliveryFormatRejection> {
    let result = PublicDeliveryFormatRejection {
        candidate: proof(v.candidate)?,
        diagnostics: v
            .diagnostics
            .into_iter()
            .map(|d| {
                checked(PublicDeliveryDiagnostic::new(
                    required(PublicDeliveryDiagnosticKind::parse(&d.kind))?,
                    d.path,
                ))
            })
            .collect::<Result<_>>()?,
        cut: cut(v.cut)?,
    };
    checked(result.validate())?;
    Ok(result)
}
fn encode(message: &impl Message, name: &str) -> Result<Vec<u8>> {
    if message.encoded_len() > MAX_PUBLIC_REPAIR_METADATA_BYTES {
        return Err(PublicRepairCodecError);
    }
    let bytes = message.encode_to_vec();
    preflight(&bytes, name)?;
    Ok(bytes)
}
fn preflight(bytes: &[u8], name: &str) -> Result<()> {
    checked(crate::bounded_protobuf::check(
        bytes,
        NonZeroUsize::new(MAX_PUBLIC_REPAIR_METADATA_BYTES).expect("nonzero"),
        name,
        crate::bounded_protobuf::Profile::Journal,
    ))
}
macro_rules! codec {
    ($encode:ident,$decode:ident,$domain:ty,$wire:ty,$to:ident,$from:ident,$name:literal) => {
        pub fn $encode(value: &$domain) -> Result<Vec<u8>> {
            encode(&$to(value)?, $name)
        }
        pub fn $decode(bytes: &[u8]) -> Result<$domain> {
            preflight(bytes, $name)?;
            let result = $from(checked(<$wire>::decode(bytes))?)?;
            if $encode(&result)? != bytes {
                return Err(PublicRepairCodecError);
            }
            Ok(result)
        }
    };
}
codec!(
    encode_public_delivery_repair_binding,
    decode_public_delivery_repair_binding,
    PublicDeliveryRepairBinding,
    wire::PublicDeliveryRepairBindingV1,
    public_delivery_repair_binding_message,
    public_delivery_repair_binding_from_message,
    "musteragent.v1.PublicDeliveryRepairBindingV1"
);
codec!(
    encode_public_delivery_rejection,
    decode_public_delivery_rejection,
    PublicDeliveryRejectionV1,
    wire::PublicDeliveryRejectionV1,
    public_delivery_rejection_message,
    public_delivery_rejection_from_message,
    "musteragent.v1.PublicDeliveryRejectionV1"
);
codec!(
    encode_public_delivery_unreplayable,
    decode_public_delivery_unreplayable,
    PublicDeliveryUnreplayableOutput,
    wire::PublicDeliveryUnreplayableOutputV1,
    public_delivery_unreplayable_message,
    public_delivery_unreplayable_from_message,
    "musteragent.v1.PublicDeliveryUnreplayableOutputV1"
);

codec!(
    encode_public_delivery_format_rejection,
    decode_public_delivery_format_rejection,
    PublicDeliveryFormatRejection,
    wire::PublicDeliveryFormatRejectionV1,
    public_delivery_format_rejection_message,
    public_delivery_format_rejection_from_message,
    "musteragent.v1.PublicDeliveryFormatRejectionV1"
);
