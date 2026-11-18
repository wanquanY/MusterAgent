//! Bounded wire observations and controls for the original Device Interaction.
//! Decoding validates the contract; custody and consent remain Owner authority.
use crate::{device_publication_codec as admission, dialogue_codec::*, encode_content_ref, wire};
use musteragent_approval::{InteractionKind, InteractionStatus, InteractionView};
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::{ConversationSeq, RunSeq, VersionedKey};
use musteragent_policy::Effect;
use musteragent_runtime_contract::{CommandAck, CommandDisposition, ErrorCode};
use musteragent_runtime_session::*;
use prost::Message;

type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
pub const PROJECTION_LIMIT: usize = 1152 * 1024;
pub const COMMAND_LIMIT: usize = 40 * 1024;
pub const ACK_LIMIT: usize = 64 * 1024;
fn hash(v: Option<wire::Sha256>) -> Result<Sha256, Error> {
    let h = crate::submission_decode::sha256(v, "native.interaction.hash").map_err(|_| BAD)?;
    if h.into_bytes() == [0; 32] {
        return Err(BAD);
    }
    Ok(h)
}
fn sha(v: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: v.into_bytes().to_vec(),
    }
}
fn effect(v: Effect) -> &'static str {
    match v {
        Effect::Read => "read",
        Effect::WriteReversible => "write_reversible",
        Effect::WriteExternal => "write_external",
        Effect::Destructive => "destructive",
    }
}
fn decode_effect(v: &str) -> Result<Effect, Error> {
    match v {
        "read" => Ok(Effect::Read),
        "write_reversible" => Ok(Effect::WriteReversible),
        "write_external" => Ok(Effect::WriteExternal),
        "destructive" => Ok(Effect::Destructive),
        _ => Err(BAD),
    }
}
pub fn interaction(v: &InteractionView) -> wire::InteractionView {
    wire::InteractionView {
        interaction_id: Some(wire::InteractionId {
            value: v.interaction_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: v.run_id.to_string(),
        }),
        kind: match v.kind {
            InteractionKind::Approval => wire::InteractionKind::Approval,
            InteractionKind::Clarification => wire::InteractionKind::Clarification,
            InteractionKind::Confirmation => wire::InteractionKind::Confirmation,
            InteractionKind::Selection => wire::InteractionKind::Selection,
            InteractionKind::Credential => wire::InteractionKind::Credential,
            InteractionKind::OAuth => wire::InteractionKind::Oauth,
        } as i32,
        status: match v.status {
            InteractionStatus::Pending => wire::InteractionStatus::Pending,
            InteractionStatus::Responded => wire::InteractionStatus::Responded,
            InteractionStatus::Expired => wire::InteractionStatus::Expired,
            InteractionStatus::Cancelled => wire::InteractionStatus::Cancelled,
        } as i32,
        prompt_ref: Some(encode_content_ref(&v.prompt_ref)),
        response_schema: Some(wire::SchemaRef {
            schema_key: v.response_schema.schema_key.as_str().to_owned(),
            schema_hash: Some(sha(v.response_schema.schema_hash)),
        }),
        requested_at: Some(timestamp(v.requested_at)),
        expires_at: v.expires_at.map(timestamp),
        responded_at: v.responded_at.map(timestamp),
    }
}
pub fn decode_interaction(v: Option<wire::InteractionView>) -> Result<InteractionView, Error> {
    let v = required(v)?;
    let schema = required(v.response_schema)?;
    Ok(InteractionView {
        interaction_id: id(v.interaction_id.map(|v| v.value))?,
        run_id: id(v.run_id.map(|v| v.value))?,
        kind: match wire::InteractionKind::try_from(v.kind).map_err(|_| BAD)? {
            wire::InteractionKind::Approval => InteractionKind::Approval,
            wire::InteractionKind::Clarification => InteractionKind::Clarification,
            wire::InteractionKind::Confirmation => InteractionKind::Confirmation,
            wire::InteractionKind::Selection => InteractionKind::Selection,
            wire::InteractionKind::Credential => InteractionKind::Credential,
            wire::InteractionKind::Oauth => InteractionKind::OAuth,
            _ => return Err(BAD),
        },
        status: match wire::InteractionStatus::try_from(v.status).map_err(|_| BAD)? {
            wire::InteractionStatus::Pending => InteractionStatus::Pending,
            wire::InteractionStatus::Responded => InteractionStatus::Responded,
            wire::InteractionStatus::Expired => InteractionStatus::Expired,
            wire::InteractionStatus::Cancelled => InteractionStatus::Cancelled,
            _ => return Err(BAD),
        },
        prompt_ref: content(required(v.prompt_ref)?)?,
        response_schema: SchemaRef {
            schema_key: VersionedKey::new(schema.schema_key).map_err(|_| BAD)?,
            schema_hash: hash(schema.schema_hash)?,
        },
        requested_at: time(v.requested_at)?,
        expires_at: v.expires_at.map(|t| time(Some(t))).transpose()?,
        responded_at: v.responded_at.map(|t| time(Some(t))).transpose()?,
    })
}
pub fn projection(
    v: &NativeDialogueInteractionProjection,
) -> wire::NativeDialogueInteractionProjection {
    use wire::native_dialogue_interaction_projection::Resolution as R;
    wire::NativeDialogueInteractionProjection {
        admission: Some(admission::binding(&v.admission)),
        conversation_id: Some(wire::ConversationId {
            value: v.conversation_id.to_string(),
        }),
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        interaction: Some(interaction(&v.interaction)),
        prompt_content: v.prompt_bytes.clone(),
        observed_run_sequence: v.observed_run_sequence.get(),
        control_open: v.control_open,
        approval_policy: v.approval_policy.as_ref().map(|p| {
            wire::NativeDialogueInteractionApprovalPolicy {
                policy_sha256: Some(sha(p.policy_sha256)),
                tool_key: p.tool_key.clone(),
                capability_key: p.capability_key.clone(),
                provider_id: Some(wire::CapabilityProviderId {
                    value: p.provider_id.to_string(),
                }),
                effect: effect(p.effect).to_owned(),
            }
        }),
        resolution: v.resolution.as_ref().map(|r| match r {
            DialogueInteractionResolution::Answer { reference, bytes } => {
                R::Answer(wire::DialogueInteractionAnswer {
                    content_ref: Some(encode_content_ref(reference)),
                    content: bytes.clone(),
                })
            }
            DialogueInteractionResolution::Approved => {
                R::Approved(wire::DialogueInteractionDecision {})
            }
            DialogueInteractionResolution::Denied => {
                R::Denied(wire::DialogueInteractionDecision {})
            }
        }),
    }
}
pub fn decode_projection(
    v: Option<wire::NativeDialogueInteractionProjection>,
) -> Result<NativeDialogueInteractionProjection, Error> {
    use wire::native_dialogue_interaction_projection::Resolution as R;
    let v = required(v)?;
    if v.encoded_len() > PROJECTION_LIMIT {
        return Err(BAD);
    }
    let p = NativeDialogueInteractionProjection {
        admission: admission::decode_binding(v.admission)?,
        conversation_id: id(v.conversation_id.map(|v| v.value))?,
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        interaction: decode_interaction(v.interaction)?,
        prompt_bytes: v.prompt_content,
        observed_run_sequence: RunSeq::new(v.observed_run_sequence).map_err(|_| BAD)?,
        control_open: v.control_open,
        approval_policy: v
            .approval_policy
            .map(|p| {
                Ok(NativeDialogueInteractionApprovalPolicy {
                    policy_sha256: hash(p.policy_sha256)?,
                    tool_key: p.tool_key,
                    capability_key: p.capability_key,
                    provider_id: id(p.provider_id.map(|v| v.value))?,
                    effect: decode_effect(&p.effect)?,
                })
            })
            .transpose()?,
        resolution: v
            .resolution
            .map(|r| {
                Ok(match r {
                    R::Answer(a) => DialogueInteractionResolution::Answer {
                        reference: content(required(a.content_ref)?)?,
                        bytes: a.content,
                    },
                    R::Approved(_) => DialogueInteractionResolution::Approved,
                    R::Denied(_) => DialogueInteractionResolution::Denied,
                })
            })
            .transpose()?,
    };
    p.validate().map_err(|_| BAD)?;
    Ok(p)
}
pub fn projection_bytes(v: &NativeDialogueInteractionProjection) -> Vec<u8> {
    projection(v).encode_to_vec()
}
pub fn decode_projection_bytes(bytes: &[u8]) -> Result<NativeDialogueInteractionProjection, Error> {
    if bytes.is_empty() || bytes.len() > PROJECTION_LIMIT {
        return Err(BAD);
    }
    let v = wire::NativeDialogueInteractionProjection::decode(bytes).map_err(|_| BAD)?;
    if v.encode_to_vec() != bytes {
        return Err(BAD);
    }
    decode_projection(Some(v))
}
pub fn command(v: &NativeDialogueInteractionCommand) -> wire::NativeDialogueInteractionCommand {
    use wire::native_dialogue_interaction_command::Response as R;
    wire::NativeDialogueInteractionCommand {
        command_id: Some(wire::CommandId {
            value: v.command_id.to_string(),
        }),
        interaction_id: Some(wire::InteractionId {
            value: v.interaction_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: v.target.dialogue_id.to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: v.target.entry_id.to_string(),
        }),
        expected_control_revision: Some(sha(v.target.expected_control_revision)),
        admission: Some(admission::binding(&v.admission)),
        response: Some(match &v.response {
            DialogueInteractionControlResponse::Answer(bytes) => R::Answer(bytes.clone()),
            DialogueInteractionControlResponse::CompleteCapability(bytes) => {
                R::Capability(bytes.clone())
            }
            DialogueInteractionControlResponse::ApproveOnce => {
                R::ApproveOnce(wire::DialogueInteractionDecision {})
            }
            DialogueInteractionControlResponse::Deny => {
                R::Deny(wire::DialogueInteractionDecision {})
            }
            DialogueInteractionControlResponse::Cancel => {
                R::Cancel(wire::DialogueInteractionDecision {})
            }
        }),
    }
}
pub fn decode_command(
    v: Option<wire::NativeDialogueInteractionCommand>,
) -> Result<NativeDialogueInteractionCommand, Error> {
    use wire::native_dialogue_interaction_command::Response as R;
    let v = required(v)?;
    if v.encoded_len() > COMMAND_LIMIT {
        return Err(BAD);
    }
    let result = NativeDialogueInteractionCommand {
        command_id: id(v.command_id.map(|v| v.value))?,
        interaction_id: id(v.interaction_id.map(|v| v.value))?,
        target: DialogueInteractionTarget {
            dialogue_id: id(v.dialogue_id.map(|v| v.value))?,
            entry_id: id(v.entry_id.map(|v| v.value))?,
            expected_control_revision: hash(v.expected_control_revision)?,
        },
        admission: admission::decode_binding(v.admission)?,
        response: match required(v.response)? {
            R::Answer(bytes) => DialogueInteractionControlResponse::Answer(bytes),
            R::Capability(bytes) => DialogueInteractionControlResponse::CompleteCapability(bytes),
            R::ApproveOnce(_) => DialogueInteractionControlResponse::ApproveOnce,
            R::Deny(_) => DialogueInteractionControlResponse::Deny,
            R::Cancel(_) => DialogueInteractionControlResponse::Cancel,
        },
    };
    result.response.validate().map_err(|_| BAD)?;
    Ok(result)
}
pub fn publication(
    v: &NativeDialogueInteractionPublicationReceipt,
) -> wire::NativeDialogueInteractionPublicationReceipt {
    wire::NativeDialogueInteractionPublicationReceipt {
        interaction_id: Some(wire::InteractionId {
            value: v.interaction_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: v.dialogue_id.to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: v.entry_id.to_string(),
        }),
        projection_sha256: Some(sha(v.projection_sha256)),
        control_revision: Some(sha(v.control_revision)),
    }
}
pub fn decode_publication(
    v: Option<wire::NativeDialogueInteractionPublicationReceipt>,
) -> Result<NativeDialogueInteractionPublicationReceipt, Error> {
    let v = required(v)?;
    Ok(NativeDialogueInteractionPublicationReceipt {
        interaction_id: id(v.interaction_id.map(|v| v.value))?,
        dialogue_id: id(v.dialogue_id.map(|v| v.value))?,
        entry_id: id(v.entry_id.map(|v| v.value))?,
        projection_sha256: hash(v.projection_sha256)?,
        control_revision: hash(v.control_revision)?,
    })
}
pub fn command_ack(v: &CommandAck) -> wire::CommandAck {
    wire::CommandAck {
        command_id: Some(wire::CommandId {
            value: v.command_id.to_string(),
        }),
        disposition: match v.disposition {
            CommandDisposition::Committed => wire::CommandDisposition::Committed,
            CommandDisposition::Duplicate => wire::CommandDisposition::Duplicate,
        } as i32,
        committed_at: Some(timestamp(v.committed_at)),
        first_conversation_seq: v.first_conversation_seq.map(|s| s.get()),
        last_conversation_seq: v.last_conversation_seq.map(|s| s.get()),
        resource_refs: v
            .resource_refs
            .iter()
            .map(crate::wire_common::resource_ref)
            .collect(),
    }
}
pub fn decode_command_ack(v: Option<wire::CommandAck>) -> Result<CommandAck, Error> {
    let v = required(v)?;
    if v.encoded_len() > ACK_LIMIT || v.resource_refs.len() > 256 {
        return Err(BAD);
    }
    let ack = CommandAck {
        command_id: id(v.command_id.map(|v| v.value))?,
        disposition: match wire::CommandDisposition::try_from(v.disposition).map_err(|_| BAD)? {
            wire::CommandDisposition::Committed => CommandDisposition::Committed,
            wire::CommandDisposition::Duplicate => CommandDisposition::Duplicate,
            _ => return Err(BAD),
        },
        committed_at: time(v.committed_at)?,
        first_conversation_seq: v
            .first_conversation_seq
            .map(ConversationSeq::new)
            .transpose()
            .map_err(|_| BAD)?,
        last_conversation_seq: v
            .last_conversation_seq
            .map(ConversationSeq::new)
            .transpose()
            .map_err(|_| BAD)?,
        resource_refs: v
            .resource_refs
            .into_iter()
            .map(crate::wire_common::decode_resource_ref)
            .collect::<Result<_, _>>()
            .map_err(|_| BAD)?,
    };
    ack.validate().map_err(|_| BAD)?;
    Ok(ack)
}
pub fn rejection(v: ErrorCode) -> wire::NativeDialogueInteractionRejected {
    wire::NativeDialogueInteractionRejected {
        code: crate::error_codec::error_code(v) as i32,
    }
}
pub fn decode_rejection(v: wire::NativeDialogueInteractionRejected) -> Result<ErrorCode, Error> {
    crate::error_codec::decode_error_code(v.code).map_err(|_| BAD)
}
pub fn control(v: &DialogueInteractionControlReceipt) -> wire::DialogueInteractionControlReceipt {
    let (delivery, owner_ack) = match &v.delivery {
        DialogueInteractionControlDelivery::Queued => {
            (wire::DialogueInteractionControlDelivery::Queued, None)
        }
        DialogueInteractionControlDelivery::Applied(ack) => (
            wire::DialogueInteractionControlDelivery::Applied,
            Some(command_ack(ack)),
        ),
        DialogueInteractionControlDelivery::Rejected(_) => {
            (wire::DialogueInteractionControlDelivery::Rejected, None)
        }
    };
    wire::DialogueInteractionControlReceipt {
        command_id: Some(wire::CommandId {
            value: v.command_id.to_string(),
        }),
        delivery: delivery as i32,
        owner_ack,
    }
}

pub fn command_cursor(
    value: NativeDialogueInteractionCommandCursor,
) -> wire::NativeDialogueInteractionCommandCursor {
    wire::NativeDialogueInteractionCommandCursor {
        queued_at_ms: value.queued_at.unix_millis(),
        command_id: Some(wire::CommandId {
            value: value.command_id.to_string(),
        }),
    }
}
pub fn decode_command_cursor(
    value: Option<wire::NativeDialogueInteractionCommandCursor>,
) -> Result<Option<NativeDialogueInteractionCommandCursor>, Error> {
    value
        .map(|v| {
            if v.queued_at_ms <= 0 {
                return Err(BAD);
            }
            Ok(NativeDialogueInteractionCommandCursor {
                queued_at: musteragent_identity::UtcTimestamp::from_unix_millis(v.queued_at_ms),
                command_id: id(v.command_id.map(|v| v.value))?,
            })
        })
        .transpose()
}

pub fn installation_namespace(value: NativeDialogueInteractionInstallationNamespace) -> i32 {
    match value {
        NativeDialogueInteractionInstallationNamespace::DialogueOrigin => 1,
        NativeDialogueInteractionInstallationNamespace::NativeClient => 2,
    }
}
pub fn decode_installation_namespace(
    value: i32,
) -> Result<NativeDialogueInteractionInstallationNamespace, Error> {
    match value {
        1 => Ok(NativeDialogueInteractionInstallationNamespace::DialogueOrigin),
        2 => Ok(NativeDialogueInteractionInstallationNamespace::NativeClient),
        _ => Err(BAD),
    }
}
