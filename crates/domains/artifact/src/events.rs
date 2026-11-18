use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactProposalId, InvocationId, NoteAuthoringSessionId, NoteSegmentId,
    ParticipantId, VersionNumber,
};
use musteragent_runtime_contract::{DisplayText, ErrorInfo};

use crate::ArtifactKind;

/// Artifact-owned facts. A proposed edit never mutates a committed version.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArtifactEvent {
    Created {
        artifact_id: ArtifactId,
        kind: ArtifactKind,
        title: DisplayText,
        source_invocation_id: Option<InvocationId>,
    },
    VersionCommitted {
        artifact_id: ArtifactId,
        version: VersionNumber,
        content_ref: ContentRef,
        dependency_refs: Vec<ContentRef>,
        content_hash: Sha256,
        /// Tool invocation that produced this exact version. Product-authored
        /// edits leave this empty, while Runtime tools must retain it so a
        /// projected tool row can reopen every durable output without parsing
        /// tool-specific result JSON.
        source_invocation_id: Option<InvocationId>,
    },
    /// A recoverable authoring branch was opened over a canonical Note
    /// revision. This follows the empty/draft revision commit in the same
    /// Ledger append.
    NoteAuthoringStarted {
        artifact_id: ArtifactId,
        session_id: NoteAuthoringSessionId,
        base_revision: VersionNumber,
    },
    /// Historical Note segment fact written before the durable segment
    /// `ContentRef` became part of the canonical Ledger contract. It remains a
    /// first-class replay variant so an additive wire-field upgrade never
    /// makes an already committed conversation unreadable. New writers must
    /// only emit [`Self::NoteSegmentCommitted`].
    NoteSegmentCommittedV1 {
        artifact_id: ArtifactId,
        session_id: NoteAuthoringSessionId,
        segment_id: NoteSegmentId,
        ordinal: u64,
        content_hash: Sha256,
        character_count: u64,
        utf8_byte_count: u64,
    },
    /// One exact bounded segment became durable on the private branch.
    NoteSegmentCommitted {
        artifact_id: ArtifactId,
        session_id: NoteAuthoringSessionId,
        segment_id: NoteSegmentId,
        ordinal: u64,
        content_ref: ContentRef,
        content_hash: Sha256,
        character_count: u64,
        utf8_byte_count: u64,
    },
    /// The branch was atomically published as a canonical Note revision.
    NoteVersionFinalized {
        artifact_id: ArtifactId,
        session_id: NoteAuthoringSessionId,
        version: VersionNumber,
        block_count: u64,
        character_count: u64,
        utf8_byte_count: u64,
    },
    Failed {
        artifact_id: ArtifactId,
        error: ErrorInfo,
    },
    EditProposed {
        proposal_id: ArtifactProposalId,
        artifact_id: ArtifactId,
        base_version: VersionNumber,
        proposal_ref: ContentRef,
        proposal_hash: Sha256,
    },
    EditApplied {
        proposal_id: ArtifactProposalId,
        artifact_id: ArtifactId,
        version: VersionNumber,
        applied_by: ParticipantId,
    },
    EditRejected {
        proposal_id: ArtifactProposalId,
        artifact_id: ArtifactId,
        rejected_by: ParticipantId,
    },
    EditExpired {
        proposal_id: ArtifactProposalId,
        artifact_id: ArtifactId,
    },
}
