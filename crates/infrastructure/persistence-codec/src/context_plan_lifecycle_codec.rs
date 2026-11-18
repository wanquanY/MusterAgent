//! Canonical prepared-plan lineage codec; structure is never dispatch authority.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_context::{
    ContextPlanControlCut, ContextPlanHeadRef, ContextPlanLineage, ContextPlanSelection,
};
use musteragent_identity::ConversationSeq;
use musteragent_run::RunControlKind;
use prost::Message;
use std::num::{NonZeroU32, NonZeroUsize};

/// Maximum canonical encoded size of a lineage or selection value.
pub const MAX_CONTEXT_PLAN_LINEAGE_BYTES: usize = 4096;
/// A malformed, noncanonical, oversized or invalid context-plan lifecycle value.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("invalid context plan lineage")]
pub struct ContextPlanLineageCodecError;
type Result<T> = std::result::Result<T, ContextPlanLineageCodecError>;
fn checked<T, E>(v: std::result::Result<T, E>) -> Result<T> {
    v.map_err(|_| ContextPlanLineageCodecError)
}
fn required<T>(v: Option<T>) -> Result<T> {
    v.ok_or(ContextPlanLineageCodecError)
}
fn id<T: std::str::FromStr>(v: Option<String>) -> Result<T> {
    checked(required(v)?.parse())
}
fn generation(v: u32) -> Result<NonZeroU32> {
    required(NonZeroU32::new(v).filter(|v| v.get() <= i32::MAX as u32))
}
fn control(v: wire::ContextPlanControlCutV1) -> Result<ContextPlanControlCut> {
    Ok(ContextPlanControlCut {
        sequence: checked(ConversationSeq::new(v.sequence))?,
        record_id: id(v.record_id.map(|v| v.value))?,
        kind: match v.kind {
            1 => RunControlKind::Steering,
            2 => RunControlKind::FollowUp,
            _ => return Err(ContextPlanLineageCodecError),
        },
    })
}
fn control_fields(v: ContextPlanControlCut) -> wire::ContextPlanControlCutV1 {
    wire::ContextPlanControlCutV1 {
        sequence: v.sequence.get(),
        record_id: Some(wire::LedgerRecordId {
            value: v.record_id.to_string(),
        }),
        kind: match v.kind {
            RunControlKind::Steering => 1,
            RunControlKind::FollowUp => 2,
        },
    }
}
fn head(v: wire::ContextPlanHeadRefV1) -> Result<ContextPlanHeadRef> {
    Ok(ContextPlanHeadRef {
        snapshot_id: id(v.snapshot_id.map(|v| v.value))?,
        generation: generation(v.generation)?,
        checksum: checked(Sha256::from_bytes(required(v.checksum)?.value))?,
    })
}
fn head_fields(v: ContextPlanHeadRef) -> wire::ContextPlanHeadRefV1 {
    wire::ContextPlanHeadRefV1 {
        snapshot_id: Some(wire::ContextSnapshotId {
            value: v.snapshot_id.to_string(),
        }),
        generation: v.generation.get(),
        checksum: Some(wire::Sha256 {
            value: v.checksum.into_bytes().to_vec(),
        }),
    }
}
/// Lossless projection for the original infallible Ledger encoder.
pub fn context_plan_lineage_fields(v: &ContextPlanLineage) -> wire::ContextPlanLineageV1 {
    wire::ContextPlanLineageV1 {
        schema_version: 1,
        generation: v.generation.get(),
        predecessor: v.predecessor.map(head_fields),
        control: v.control.map(control_fields),
    }
}
/// Validate lineage before projecting it into the durable wire message.
pub fn context_plan_lineage_message(v: &ContextPlanLineage) -> Result<wire::ContextPlanLineageV1> {
    checked(v.validate())?;
    Ok(context_plan_lineage_fields(v))
}
/// Decode and validate the complete predecessor and control lineage.
pub fn context_plan_lineage_from_message(
    v: wire::ContextPlanLineageV1,
) -> Result<ContextPlanLineage> {
    if v.schema_version != 1 {
        return Err(ContextPlanLineageCodecError);
    }
    let result = ContextPlanLineage {
        generation: generation(v.generation)?,
        predecessor: v.predecessor.map(head).transpose()?,
        control: v.control.map(control).transpose()?,
    };
    checked(result.validate())?;
    Ok(result)
}
/// Validate the expected context head and source control before encoding.
pub fn context_plan_selection_message(
    v: &ContextPlanSelection,
) -> Result<wire::ContextPlanSelectionV1> {
    checked(v.validate())?;
    Ok(wire::ContextPlanSelectionV1 {
        schema_version: 1,
        expected_head: v.expected_head.map(head_fields),
        source_control: v.source_control.map(control_fields),
        lineage: v
            .lineage
            .as_ref()
            .map(context_plan_lineage_message)
            .transpose()?,
    })
}
/// Decode a selection and validate its relationship to the expected head.
pub fn context_plan_selection_from_message(
    v: wire::ContextPlanSelectionV1,
) -> Result<ContextPlanSelection> {
    if v.schema_version != 1 {
        return Err(ContextPlanLineageCodecError);
    }
    let result = ContextPlanSelection {
        expected_head: v.expected_head.map(head).transpose()?,
        source_control: v.source_control.map(control).transpose()?,
        lineage: v
            .lineage
            .map(context_plan_lineage_from_message)
            .transpose()?,
    };
    checked(result.validate())?;
    Ok(result)
}
fn preflight(bytes: &[u8], name: &str) -> Result<()> {
    checked(crate::bounded_protobuf::check(
        bytes,
        NonZeroUsize::new(MAX_CONTEXT_PLAN_LINEAGE_BYTES).expect("nonzero"),
        name,
        crate::bounded_protobuf::Profile::Journal,
    ))
}
macro_rules! codec {
    ($encode:ident,$decode:ident,$domain:ty,$wire:ty,$to:ident,$from:ident,$name:literal) => {
        /// Encode a validated value within the canonical lifecycle size bound.
        pub fn $encode(value: &$domain) -> Result<Vec<u8>> {
            let message = $to(value)?;
            if message.encoded_len() > MAX_CONTEXT_PLAN_LINEAGE_BYTES {
                return Err(ContextPlanLineageCodecError);
            }
            let bytes = message.encode_to_vec();
            preflight(&bytes, $name)?;
            Ok(bytes)
        }
        /// Decode bounded canonical bytes, rejecting unknown or duplicate fields.
        pub fn $decode(bytes: &[u8]) -> Result<$domain> {
            preflight(bytes, $name)?;
            let result = $from(checked(<$wire>::decode(bytes))?)?;
            if $encode(&result)? != bytes {
                return Err(ContextPlanLineageCodecError);
            }
            Ok(result)
        }
    };
}
codec!(
    encode_context_plan_lineage,
    decode_context_plan_lineage,
    ContextPlanLineage,
    wire::ContextPlanLineageV1,
    context_plan_lineage_message,
    context_plan_lineage_from_message,
    "musteragent.v1.ContextPlanLineageV1"
);
codec!(
    encode_context_plan_selection,
    decode_context_plan_selection,
    ContextPlanSelection,
    wire::ContextPlanSelectionV1,
    context_plan_selection_message,
    context_plan_selection_from_message,
    "musteragent.v1.ContextPlanSelectionV1"
);
#[cfg(test)]
mod tests;
