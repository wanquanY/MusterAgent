//! Canonical conversation Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ConversationCreated(value) => {
            Ok(DomainEvent::Conversation(ConversationEvent::Created {
                kind: conversation_kind(value.kind)?,
                control_home: runtime_home(value.control_home)?,
                workspace_id: optional_id(value.workspace_id.map(|item| item.value))?,
            }))
        }
        Event::ConversationWorkspaceAssigned(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::WorkspaceAssigned {
                workspace_id: required_id(value.workspace_id.map(|item| item.value))?,
                assignment_command_id: required_id(
                    value.assignment_command_id.map(|item| item.value),
                )?,
            },
        )),
        Event::ConversationArchived(_) => {
            Ok(DomainEvent::Conversation(ConversationEvent::Archived))
        }
        Event::ConversationUnarchived(_) => {
            Ok(DomainEvent::Conversation(ConversationEvent::Unarchived))
        }
        Event::ConversationTitleChanged(value) => {
            let proposal_record_id = optional_id(value.proposal_record_id.map(|item| item.value))?;
            let origin = match wire::ConversationTitleOrigin::try_from(value.origin) {
                Ok(wire::ConversationTitleOrigin::User) if proposal_record_id.is_none() => {
                    ConversationTitleOrigin::User
                }
                Ok(wire::ConversationTitleOrigin::FirstUserMessage)
                    if proposal_record_id.is_none() =>
                {
                    ConversationTitleOrigin::FirstUserMessage
                }
                Ok(wire::ConversationTitleOrigin::AcceptedProposal) => {
                    ConversationTitleOrigin::AcceptedProposal {
                        proposal_record_id: proposal_record_id.ok_or_else(corrupt)?,
                    }
                }
                _ => return Err(corrupt()),
            };
            Ok(DomainEvent::Conversation(ConversationEvent::TitleChanged {
                title: DisplayText::new(value.title).map_err(|_| corrupt())?,
                origin,
                set_by: required_id(value.set_by.map(|item| item.value))?,
            }))
        }
        Event::ParticipantJoined(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::ParticipantJoined {
                kind: participant_kind(value.kind)?,
                principal_ref: optional_id(value.principal_ref.map(|value| value.value))?,
                agent_ref: optional_id(value.agent_ref.map(|value| value.value))?,
                service_ref: optional_text(value.service_ref)
                    .map(|value| {
                        musteragent_identity::DottedKey::new(value)
                            .map(ServiceRef::from_key)
                            .map_err(|_| corrupt())
                    })
                    .transpose()?,
                display_snapshot: ParticipantDisplaySnapshot {
                    display_name: DisplayText::new(value.display_name).map_err(|_| corrupt())?,
                    avatar_ref: value.avatar_ref.map(content_ref).transpose()?,
                },
            },
        )),
        Event::ParticipantLeft(_) => Ok(DomainEvent::Conversation(
            ConversationEvent::ParticipantLeft,
        )),
        Event::MessageAccepted(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::MessageAccepted {
                project_task_target: value
                    .project_task_target
                    .map(crate::project_task_codec::decode_target)
                    .transpose()
                    .map_err(|_| corrupt())?,
                message_id: required_id(value.message_id.map(|value| value.value))?,
                role: message_role(value.role)?,
                content_ref: content_ref(value.content_ref.ok_or_else(corrupt)?)?,
                attachment_refs: value
                    .attachment_refs
                    .into_iter()
                    .map(|value| {
                        Ok(AttachmentRef {
                            content_ref: content_ref(value.content_ref.ok_or_else(corrupt)?)?,
                            display_name: DisplayText::new(value.display_name)
                                .map_err(|_| corrupt())?,
                        })
                    })
                    .collect::<Result<_, _>>()?,
                requested_skill_refs: value
                    .requested_skill_refs
                    .into_iter()
                    .map(|reference| {
                        musteragent_identity::VersionedKey::new(reference).map_err(|_| corrupt())
                    })
                    .collect::<Result<_, _>>()?,
            },
        )),
        Event::MessageCommitted(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::MessageCommitted {
                message_id: required_id(value.message_id.map(|value| value.value))?,
                role: message_role(value.role)?,
                content_ref: content_ref(value.content_ref.ok_or_else(corrupt)?)?,
                content_hash: sha256(value.content_hash)?,
                content_bytes: value.content_bytes,
            },
        )),
        Event::MessagePartiallyCommitted(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::MessagePartiallyCommitted {
                message_id: required_id(value.message_id.map(|value| value.value))?,
                content_ref: content_ref(value.content_ref.ok_or_else(corrupt)?)?,
                content_hash: sha256(value.content_hash)?,
                content_bytes: value.content_bytes,
                reason: partial_message_reason(value.reason)?,
            },
        )),
        Event::MessageImported(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::MessageImported {
                message_id: required_id(value.message_id.map(|value| value.value))?,
                role: message_role(value.role)?,
                content_ref: content_ref(value.content_ref.ok_or_else(corrupt)?)?,
                projection_ref: value.projection_ref.map(content_ref).transpose()?,
                attachment_refs: value
                    .attachment_refs
                    .into_iter()
                    .map(|reference| {
                        Ok(AttachmentRef {
                            content_ref: content_ref(reference.content_ref.ok_or_else(corrupt)?)?,
                            display_name: DisplayText::new(reference.display_name)
                                .map_err(|_| corrupt())?,
                        })
                    })
                    .collect::<Result<Vec<_>, SubscriptionError>>()?,
                legacy_message_id: SafeDisplayText::new(value.legacy_message_id)
                    .map_err(|_| corrupt())?,
                source_sequence: value.source_sequence,
                source_payload_hash: sha256(value.source_payload_hash)?,
            },
        )),
        Event::MessageRetracted(value) => Ok(DomainEvent::Conversation(
            ConversationEvent::MessageRetracted {
                message_id: required_id(value.message_id.map(|value| value.value))?,
                reason: SafeDisplayText::new(value.reason).map_err(|_| corrupt())?,
                source_run_id: optional_id(value.source_run_id.map(|value| value.value))?,
            },
        )),
        _ => Err(corrupt()),
    }
}
