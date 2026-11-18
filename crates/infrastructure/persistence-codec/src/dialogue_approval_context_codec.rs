//! Closed read-only lifecycle observation. Never decoded as Tool authority.
use crate::{dialogue_codec::*, dialogue_context_codec, wire};
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
use std::num::NonZeroU64;
type Error = DialogueCodecError;
pub fn encode(v: &DialogueApprovalContext) -> wire::DialogueApprovalContext {
    wire::DialogueApprovalContext {
        frozen_cut: Some(dialogue_context_codec::cut(&v.frozen_cut)),
        from_seq: v.from_seq,
        observations: v
            .observations
            .iter()
            .map(|o| wire::DialogueApprovalContextObservation {
                entry_id: Some(wire::DialogueEntryId {
                    value: o.entry_id.to_string(),
                }),
                sequence: o.sequence,
                interaction_id: Some(wire::InteractionId {
                    value: o.interaction_id.to_string(),
                }),
                run_id: Some(wire::RunId {
                    value: o.run_id.to_string(),
                }),
                visible_revision: o.visible_revision.get(),
                control_revision: o.control_revision.into_bytes().to_vec(),
                kind: match o.kind {
                    DialogueApprovalContextKind::Approval => 1,
                    DialogueApprovalContextKind::Confirmation => 2,
                },
                state: match o.state {
                    DialogueApprovalContextState::Pending => 1,
                    DialogueApprovalContextState::Approved => 2,
                    DialogueApprovalContextState::Denied => 3,
                    DialogueApprovalContextState::Expired => 4,
                    DialogueApprovalContextState::Cancelled => 5,
                    DialogueApprovalContextState::ResolvedDecisionUnavailable => 6,
                },
            })
            .collect(),
    }
}
pub fn decode(v: wire::DialogueApprovalContext) -> Result<DialogueApprovalContext, Error> {
    let v = DialogueApprovalContext {
        frozen_cut: dialogue_context_codec::decode_cut(required(v.frozen_cut)?)?,
        from_seq: v.from_seq,
        observations: v
            .observations
            .into_iter()
            .map(|o| {
                Ok(DialogueApprovalObservation {
                    entry_id: id(o.entry_id.map(|v| v.value))?,
                    sequence: o.sequence,
                    interaction_id: id(o.interaction_id.map(|v| v.value))?,
                    run_id: id(o.run_id.map(|v| v.value))?,
                    visible_revision: NonZeroU64::new(o.visible_revision).ok_or(Error::Corrupt)?,
                    control_revision: Sha256::from_bytes(o.control_revision)
                        .map_err(|_| Error::Corrupt)?,
                    kind: match o.kind {
                        1 => DialogueApprovalContextKind::Approval,
                        2 => DialogueApprovalContextKind::Confirmation,
                        _ => return Err(Error::Corrupt),
                    },
                    state: match o.state {
                        1 => DialogueApprovalContextState::Pending,
                        2 => DialogueApprovalContextState::Approved,
                        3 => DialogueApprovalContextState::Denied,
                        4 => DialogueApprovalContextState::Expired,
                        5 => DialogueApprovalContextState::Cancelled,
                        6 => DialogueApprovalContextState::ResolvedDecisionUnavailable,
                        _ => return Err(Error::Corrupt),
                    },
                })
            })
            .collect::<Result<_, Error>>()?,
    };
    v.validate().map_err(|_| Error::Corrupt)?;
    Ok(v)
}
