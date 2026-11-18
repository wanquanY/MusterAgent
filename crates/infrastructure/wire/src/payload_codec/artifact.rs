//! Canonical artifact Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ArtifactCreated(value) => Ok(DomainEvent::Artifact(ArtifactEvent::Created {
            artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
            kind: artifact_kind(value.kind)?,
            title: DisplayText::new(value.title).map_err(|_| corrupt())?,
            source_invocation_id: optional_id(value.source_invocation_id.map(|value| value.value))?,
        })),
        Event::ArtifactVersionCommitted(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::VersionCommitted {
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
                version: VersionNumber::new(value.version).map_err(|_| corrupt())?,
                content_ref: content_ref(value.content_ref.ok_or_else(corrupt)?)?,
                dependency_refs: value
                    .dependency_refs
                    .into_iter()
                    .map(content_ref)
                    .collect::<Result<Vec<_>, _>>()?,
                content_hash: sha256(value.content_hash)?,
                source_invocation_id: optional_id(
                    value.source_invocation_id.map(|value| value.value),
                )?,
            }))
        }
        Event::NoteAuthoringStarted(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::NoteAuthoringStarted {
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
                session_id: required_id(value.session_id.map(|value| value.value))?,
                base_revision: VersionNumber::new(value.base_revision).map_err(|_| corrupt())?,
            }))
        }
        Event::NoteSegmentCommitted(value) => {
            let artifact_id = required_id(value.artifact_id.map(|value| value.value))?;
            let session_id = required_id(value.session_id.map(|value| value.value))?;
            let segment_id = required_id(value.segment_id.map(|value| value.value))?;
            let content_hash = sha256(value.content_hash)?;
            let event = if let Some(reference) = value.content_ref {
                ArtifactEvent::NoteSegmentCommitted {
                    artifact_id,
                    session_id,
                    segment_id,
                    ordinal: value.ordinal,
                    content_ref: content_ref(reference)?,
                    content_hash,
                    character_count: value.character_count,
                    utf8_byte_count: value.utf8_byte_count,
                }
            } else {
                ArtifactEvent::NoteSegmentCommittedV1 {
                    artifact_id,
                    session_id,
                    segment_id,
                    ordinal: value.ordinal,
                    content_hash,
                    character_count: value.character_count,
                    utf8_byte_count: value.utf8_byte_count,
                }
            };
            Ok(DomainEvent::Artifact(event))
        }
        Event::NoteVersionFinalized(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::NoteVersionFinalized {
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
                session_id: required_id(value.session_id.map(|value| value.value))?,
                version: VersionNumber::new(value.version).map_err(|_| corrupt())?,
                block_count: value.block_count,
                character_count: value.character_count,
                utf8_byte_count: value.utf8_byte_count,
            }))
        }
        Event::ArtifactFailed(value) => Ok(DomainEvent::Artifact(ArtifactEvent::Failed {
            artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
            error: error_info(value.error)?,
        })),
        Event::ArtifactEditProposed(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::EditProposed {
                proposal_id: required_id(value.proposal_id.map(|value| value.value))?,
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
                base_version: VersionNumber::new(value.base_version).map_err(|_| corrupt())?,
                proposal_ref: content_ref(value.proposal_ref.ok_or_else(corrupt)?)?,
                proposal_hash: sha256(value.proposal_hash)?,
            }))
        }
        Event::ArtifactEditApplied(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::EditApplied {
                proposal_id: required_id(value.proposal_id.map(|value| value.value))?,
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
                version: VersionNumber::new(value.version).map_err(|_| corrupt())?,
                applied_by: required_id(value.applied_by.map(|value| value.value))?,
            }))
        }
        Event::ArtifactEditRejected(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::EditRejected {
                proposal_id: required_id(value.proposal_id.map(|value| value.value))?,
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
                rejected_by: required_id(value.rejected_by.map(|value| value.value))?,
            }))
        }
        Event::ArtifactEditExpired(value) => {
            Ok(DomainEvent::Artifact(ArtifactEvent::EditExpired {
                proposal_id: required_id(value.proposal_id.map(|value| value.value))?,
                artifact_id: required_id(value.artifact_id.map(|value| value.value))?,
            }))
        }
        _ => Err(corrupt()),
    }
}
