//! Canonical account-owned execution scalar codecs shared by RPC and durable receipts.
//! They validate structure; authorization remains in the application owner.
use crate::{encode_content_ref, wire};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_definition::owned_agent::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_policy::PermissionMode;
use prost_types::Timestamp;
use std::{num::NonZeroU64, str::FromStr};
#[derive(Debug, thiserror::Error, Clone, Copy, Eq, PartialEq)]
pub enum DialogueCodecError {
    #[error("invalid dialogue contract")]
    Corrupt,
}
type Error = DialogueCodecError;
mod input_attachment;
pub use input_attachment::*;
mod reply;
pub use reply::*;

pub fn task_target(value: AgentTaskTarget) -> wire::AgentTaskTarget {
    wire::AgentTaskTarget {
        work_item_id: value.work_item_id.to_string(),
        goal_revision: value.goal_revision,
        control_revision: value.control_revision,
    }
}
pub fn decode_task_target(value: wire::AgentTaskTarget) -> Result<AgentTaskTarget, Error> {
    let target = AgentTaskTarget {
        work_item_id: id(Some(value.work_item_id))?,
        goal_revision: value.goal_revision,
        control_revision: value.control_revision,
    };
    target.validate().map_err(|_| Error::Corrupt)?;
    Ok(target)
}
pub fn required<T>(value: Option<T>) -> Result<T, Error> {
    value.ok_or(Error::Corrupt)
}
pub fn id<T: FromStr>(value: Option<String>) -> Result<T, Error> {
    required(value)?.parse().map_err(|_| Error::Corrupt)
}
pub fn nz(value: u64) -> Result<NonZeroU64, Error> {
    NonZeroU64::new(value).ok_or(Error::Corrupt)
}
pub fn timestamp(value: UtcTimestamp) -> Timestamp {
    Timestamp {
        seconds: value.unix_millis().div_euclid(1000),
        nanos: (value.unix_millis().rem_euclid(1000) as i32) * 1_000_000,
    }
}
pub fn time(value: Option<Timestamp>) -> Result<UtcTimestamp, Error> {
    crate::scalar::timestamp(value).map_err(|_| Error::Corrupt)
}
pub fn content(value: wire::ContentRef) -> Result<ContentRef, Error> {
    crate::wire_common::decode_content_ref(value).map_err(|_| Error::Corrupt)
}
pub fn cursor(value: DialogueCursor) -> wire::AgentDialogueCursor {
    wire::AgentDialogueCursor {
        epoch: value.epoch,
        seq: value.seq,
    }
}
pub fn decode_cursor(value: Option<wire::AgentDialogueCursor>) -> Result<DialogueCursor, Error> {
    let v = required(value)?;
    let c = DialogueCursor {
        epoch: v.epoch,
        seq: v.seq,
    };
    c.validate().map_err(|_| Error::Corrupt)?;
    Ok(c)
}
pub fn intent(value: &UserIntent) -> wire::DialogueUserIntent {
    wire::DialogueUserIntent {
        intent_id: Some(wire::UserIntentId {
            value: value.intent_id.to_string(),
        }),
        command_id: Some(wire::CommandId {
            value: value.command_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: value.dialogue_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        owner_id: Some(wire::PrincipalId {
            value: value.owner_id.to_string(),
        }),
        source_entry_id: Some(wire::DialogueEntryId {
            value: value.source_entry_id.to_string(),
        }),
        content_ref: Some(encode_content_ref(&value.content_ref)),
        origin: Some(wire::DialogueInputOrigin {
            surface: match &value.origin.surface {
                DialogueInputSurface::Desktop => 1,
                DialogueInputSurface::Web => 2,
                DialogueInputSurface::Mobile => 3,
                DialogueInputSurface::Schedule => 5,
                DialogueInputSurface::Capability => 6,
            },
            installation_id: value
                .origin
                .installation_id
                .map(|v| wire::DeviceInstallationId {
                    value: v.to_string(),
                }),
        }),
        observed_cursor: Some(cursor(value.observed_cursor)),
        task_target: value.task_target.map(task_target),
        reply_target: value.reply_binding.as_ref().map(|v| reply_target(v.target)),
        reply_binding: value.reply_binding.as_ref().map(reply_binding),
        reply_to_intent_id: value.reply_to_intent_id.map(|v| wire::UserIntentId {
            value: v.to_string(),
        }),
        accepted_cursor: Some(cursor(value.accepted_cursor)),
        accepted_at: Some(timestamp(value.accepted_at)),
        input_attachments: value
            .input_attachments
            .iter()
            .map(input_attachment)
            .collect(),
    }
}
pub fn decode_intent(value: wire::DialogueUserIntent) -> Result<UserIntent, Error> {
    let origin = required(value.origin)?;
    let origin = DialogueInputOrigin {
        surface: match origin.surface {
            1 => DialogueInputSurface::Desktop,
            2 => DialogueInputSurface::Web,
            3 => DialogueInputSurface::Mobile,
            5 => DialogueInputSurface::Schedule,
            6 => DialogueInputSurface::Capability,
            _ => return Err(Error::Corrupt),
        },
        installation_id: origin
            .installation_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
    };
    origin.validate().map_err(|_| Error::Corrupt)?;
    let reply_target = value.reply_target.map(decode_reply_target).transpose()?;
    let reply_binding = value.reply_binding.map(decode_reply_binding).transpose()?;
    if reply_target != reply_binding.as_ref().map(|v| v.target) {
        return Err(Error::Corrupt);
    }
    let decoded = UserIntent {
        intent_id: id(value.intent_id.map(|v| v.value))?,
        command_id: id(value.command_id.map(|v| v.value))?,
        dialogue_id: id(value.dialogue_id.map(|v| v.value))?,
        tenant_id: id(value.tenant_id.map(|v| v.value))?,
        owner_id: id(value.owner_id.map(|v| v.value))?,
        source_entry_id: id(value.source_entry_id.map(|v| v.value))?,
        content_ref: content(required(value.content_ref)?)?,
        input_attachments: decode_input_attachments(value.input_attachments)?,
        origin,
        observed_cursor: decode_cursor(value.observed_cursor)?,
        reply_to_intent_id: value
            .reply_to_intent_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        task_target: value.task_target.map(decode_task_target).transpose()?,
        reply_binding,
        accepted_cursor: decode_cursor(value.accepted_cursor)?,
        accepted_at: time(value.accepted_at)?,
    };
    if let Some(binding) = &decoded.reply_binding {
        if binding.dialogue_id != decoded.dialogue_id {
            return Err(Error::Corrupt);
        }
        binding
            .cursor
            .require_at_or_before(decoded.observed_cursor)
            .map_err(|_| Error::Corrupt)?;
    }
    Ok(decoded)
}
pub fn configuration(value: &OwnedAgentConfiguration) -> wire::OwnedAgentConfiguration {
    wire::OwnedAgentConfiguration {
        agent_id: Some(wire::AgentId {
            value: value.agent_id.to_string(),
        }),
        manifest_revision: value.manifest_revision.get(),
        profile: Some(wire::OwnedAgentProfile {
            revision: value.profile.revision.get(),
            display_name: value.profile.display_name.clone(),
            description: value.profile.description.clone(),
            avatar_ref: value.profile.avatar_ref.as_ref().map(encode_content_ref),
        }),
        instructions_revision: value.instructions_revision.get(),
        instructions_ref: value.instructions_ref.as_ref().map(encode_content_ref),
        permission_revision: value.permission_revision.get(),
        permission_mode: value.permission_mode as i32,
    }
}
pub fn decode_configuration(
    value: wire::OwnedAgentConfiguration,
) -> Result<OwnedAgentConfiguration, Error> {
    let p = required(value.profile)?;
    let result = OwnedAgentConfiguration {
        agent_id: id(value.agent_id.map(|v| v.value))?,
        manifest_revision: nz(value.manifest_revision)?,
        profile: OwnedAgentProfile {
            revision: nz(p.revision)?,
            display_name: p.display_name,
            description: p.description,
            avatar_ref: p.avatar_ref.map(content).transpose()?,
        },
        instructions_revision: nz(value.instructions_revision)?,
        instructions_ref: value.instructions_ref.map(content).transpose()?,
        permission_revision: nz(value.permission_revision)?,
        permission_mode: match value.permission_mode {
            1 => PermissionMode::Supervised,
            2 => PermissionMode::Trusted,
            4 => PermissionMode::FullAccess,
            _ => return Err(Error::Corrupt),
        },
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    Ok(result)
}
pub fn space(s: &OwnerSpace) -> wire::OwnerSpace {
    wire::OwnerSpace {
        space_id: Some(wire::OwnerSpaceId {
            value: s.space_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: s.tenant_id.to_string(),
        }),
        owner_id: Some(wire::PrincipalId {
            value: s.owner_id.to_string(),
        }),
        product: "musteragent".into(),
        default_agent_id: s.default_agent_id.map(|v| wire::AgentId {
            value: v.to_string(),
        }),
        revision: s.revision.get(),
        created_at: Some(timestamp(s.created_at)),
    }
}
pub fn decode_space(s: wire::OwnerSpace) -> Result<OwnerSpace, Error> {
    if s.product != "musteragent" {
        return Err(Error::Corrupt);
    }
    Ok(OwnerSpace {
        space_id: id(s.space_id.map(|v| v.value))?,
        tenant_id: id(s.tenant_id.map(|v| v.value))?,
        owner_id: id(s.owner_id.map(|v| v.value))?,
        product: OwnerSpaceProduct::MusterAgent,
        default_agent_id: s.default_agent_id.map(|v| id(Some(v.value))).transpose()?,
        revision: nz(s.revision)?,
        created_at: time(s.created_at)?,
    })
}
pub fn agent(a: &OwnedAgent) -> wire::OwnedAgentInstance {
    wire::OwnedAgentInstance {
        agent_id: Some(wire::AgentId {
            value: a.agent_id.to_string(),
        }),
        space_id: Some(wire::OwnerSpaceId {
            value: a.space_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: a.dialogue_id.to_string(),
        }),
        published_version_id: Some(wire::AgentVersionId {
            value: a.published_version_id.to_string(),
        }),
        status: match a.status {
            OwnedAgentStatus::Active => 1,
            OwnedAgentStatus::Archived => 2,
            OwnedAgentStatus::Deleted => 3,
        },
        revision: a.revision.get(),
        created_at: Some(timestamp(a.created_at)),
        updated_at: Some(timestamp(a.updated_at)),
    }
}
pub fn decode_agent(a: wire::OwnedAgentInstance) -> Result<OwnedAgent, Error> {
    Ok(OwnedAgent {
        agent_id: id(a.agent_id.map(|v| v.value))?,
        space_id: id(a.space_id.map(|v| v.value))?,
        dialogue_id: id(a.dialogue_id.map(|v| v.value))?,
        published_version_id: id(a.published_version_id.map(|v| v.value))?,
        status: match a.status {
            1 => OwnedAgentStatus::Active,
            2 => OwnedAgentStatus::Archived,
            3 => OwnedAgentStatus::Deleted,
            _ => return Err(Error::Corrupt),
        },
        revision: nz(a.revision)?,
        created_at: time(a.created_at)?,
        updated_at: time(a.updated_at)?,
    })
}
pub fn dialogue(d: &AgentDialogue) -> wire::AgentDialogue {
    wire::AgentDialogue {
        dialogue_id: Some(wire::AgentDialogueId {
            value: d.dialogue_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: d.agent_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: d.tenant_id.to_string(),
        }),
        owner_id: Some(wire::PrincipalId {
            value: d.owner_id.to_string(),
        }),
        head: Some(cursor(d.head)),
        created_at: Some(timestamp(d.created_at)),
    }
}
pub fn decode_dialogue(d: wire::AgentDialogue) -> Result<AgentDialogue, Error> {
    Ok(AgentDialogue {
        dialogue_id: id(d.dialogue_id.map(|v| v.value))?,
        agent_id: id(d.agent_id.map(|v| v.value))?,
        tenant_id: id(d.tenant_id.map(|v| v.value))?,
        owner_id: id(d.owner_id.map(|v| v.value))?,
        head: decode_cursor(d.head)?,
        created_at: time(d.created_at)?,
    })
}
pub fn entry(e: &AgentDialogueEntry) -> wire::AgentDialogueEntry {
    use wire::agent_dialogue_entry::Content;
    let author = e.author_agent_id.map(|v| wire::AgentId {
        value: v.to_string(),
    });
    let content = match &e.content {
        AgentDialogueContent::Erased {
            original_source_payload_sha256,
        } => Content::Erased(wire::AgentDialogueErased {
            original_source_payload_sha256: original_source_payload_sha256.into_bytes().to_vec(),
        }),
        AgentDialogueContent::PartnerCreated(partner) => {
            Content::PartnerCreated(wire::AgentDialoguePartnerCreated {
                creation_command_id: Some(wire::CommandId {
                    value: partner.creation_command_id.to_string(),
                }),
                agent_id: Some(wire::AgentId {
                    value: partner.agent_id.to_string(),
                }),
                dialogue_id: Some(wire::AgentDialogueId {
                    value: partner.dialogue_id.to_string(),
                }),
                display_name: partner.display_name.clone(),
                description: partner.description.clone(),
                avatar_ref: partner.avatar_ref.as_ref().map(encode_content_ref),
                profile_revision: partner.profile_revision.get(),
            })
        }
        AgentDialogueContent::UserMessage {
            content_ref,
            input_attachments,
        } => Content::UserMessage(wire::AgentDialogueText {
            content_ref: Some(encode_content_ref(content_ref)),
            author_agent_id: author.clone(),
            input_attachments: input_attachments.iter().map(attachment).collect(),
        }),
        AgentDialogueContent::AgentMessage { content_ref } => {
            Content::AgentMessage(wire::AgentDialogueText {
                content_ref: Some(encode_content_ref(content_ref)),
                author_agent_id: author.clone(),
                input_attachments: Vec::new(),
            })
        }
        AgentDialogueContent::Interaction { interaction_id } => {
            Content::Interaction(wire::AgentDialogueInteraction {
                interaction_id: Some(wire::InteractionId {
                    value: interaction_id.to_string(),
                }),
            })
        }
        AgentDialogueContent::Artifact {
            artifact_id,
            artifact_version_id,
        } => Content::Artifact(wire::AgentDialogueArtifact {
            artifact_id: Some(wire::ArtifactId {
                value: artifact_id.to_string(),
            }),
            version_id: Some(wire::ArtifactVersionId {
                value: artifact_version_id.to_string(),
            }),
        }),
    };
    wire::AgentDialogueEntry {
        entry_id: Some(wire::DialogueEntryId {
            value: e.entry_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: e.tenant_id.to_string(),
        }),
        owner_id: Some(wire::PrincipalId {
            value: e.owner_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: e.dialogue_id.to_string(),
        }),
        cursor: Some(cursor(e.cursor)),
        source_intent_id: e.source_intent_id.map(|v| wire::UserIntentId {
            value: v.to_string(),
        }),
        visible_revision: e.visible_revision.get(),
        created_at: Some(timestamp(e.created_at)),
        author_agent_id: author,
        content: Some(content),
        presentation: match e.presentation {
            AgentDialoguePresentation::Standard => wire::AgentDialogueEntryPresentation::Standard,
            AgentDialoguePresentation::Progress => wire::AgentDialogueEntryPresentation::Progress,
        } as i32,
    }
}
pub fn decode_entry(e: wire::AgentDialogueEntry) -> Result<AgentDialogueEntry, Error> {
    use wire::agent_dialogue_entry::Content;
    let envelope_author = e.author_agent_id.map(|v| id(Some(v.value))).transpose()?;
    let (body, nested_author) = match required(e.content)? {
        Content::Erased(v) => (
            AgentDialogueContent::Erased {
                original_source_payload_sha256: Sha256::from_bytes(
                    v.original_source_payload_sha256,
                )
                .map_err(|_| Error::Corrupt)?,
            },
            None,
        ),
        Content::PartnerCreated(v) => {
            let partner = AgentDialoguePartnerCreated {
                creation_command_id: id(v.creation_command_id.map(|v| v.value))?,
                agent_id: id(v.agent_id.map(|v| v.value))?,
                dialogue_id: id(v.dialogue_id.map(|v| v.value))?,
                display_name: v.display_name,
                description: v.description,
                avatar_ref: v.avatar_ref.map(content).transpose()?,
                profile_revision: nz(v.profile_revision)?,
            };
            PublicDeliveryContent::PartnerCreated(partner.clone())
                .validate(DialoguePublicationPurpose::Result)
                .map_err(|_| Error::Corrupt)?;
            (AgentDialogueContent::PartnerCreated(partner), None)
        }
        Content::UserMessage(v) => (
            AgentDialogueContent::UserMessage {
                content_ref: content(required(v.content_ref)?)?,
                input_attachments: decode_attachments(v.input_attachments)?,
            },
            v.author_agent_id.map(|v| id(Some(v.value))).transpose()?,
        ),
        Content::AgentMessage(v) if v.input_attachments.is_empty() => (
            AgentDialogueContent::AgentMessage {
                content_ref: content(required(v.content_ref)?)?,
            },
            v.author_agent_id.map(|v| id(Some(v.value))).transpose()?,
        ),
        Content::Interaction(v) => (
            AgentDialogueContent::Interaction {
                interaction_id: id(v.interaction_id.map(|v| v.value))?,
            },
            None,
        ),
        Content::Artifact(v) => (
            AgentDialogueContent::Artifact {
                artifact_id: id(v.artifact_id.map(|v| v.value))?,
                artifact_version_id: id(v.version_id.map(|v| v.value))?,
            },
            None,
        ),
        Content::AgentMessage(_) => return Err(Error::Corrupt),
    };
    if envelope_author.is_some() && nested_author.is_some() && envelope_author != nested_author {
        return Err(Error::Corrupt);
    }
    let author_agent_id = envelope_author.or(nested_author);
    if author_agent_id.is_some()
        && matches!(&body, AgentDialogueContent::UserMessage { input_attachments, .. } if !input_attachments.is_empty())
    {
        return Err(Error::Corrupt);
    }
    Ok(AgentDialogueEntry {
        entry_id: id(e.entry_id.map(|v| v.value))?,
        tenant_id: id(e.tenant_id.map(|v| v.value))?,
        owner_id: id(e.owner_id.map(|v| v.value))?,
        dialogue_id: id(e.dialogue_id.map(|v| v.value))?,
        cursor: decode_cursor(e.cursor)?,
        source_intent_id: e.source_intent_id.map(|v| id(Some(v.value))).transpose()?,
        author_agent_id,
        visible_revision: nz(e.visible_revision)?,
        created_at: time(e.created_at)?,
        content: body,
        presentation: match wire::AgentDialogueEntryPresentation::try_from(e.presentation)
            .map_err(|_| Error::Corrupt)?
        {
            wire::AgentDialogueEntryPresentation::Progress => AgentDialoguePresentation::Progress,
            _ => AgentDialoguePresentation::Standard,
        },
    })
}
