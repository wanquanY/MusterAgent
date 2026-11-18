//! Durable contracts for recoverable, block-addressed Note authoring.
//!
//! An authoring session is a private branch over one canonical Note revision.
//! Segment commits advance only that branch; they do not create user-visible
//! Artifact versions. A later fenced finalize operation publishes an immutable
//! manifest and its blocks as one new canonical Artifact version.

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ConversationId, InvocationId, NoteAuthoringSessionId,
    NoteBlockId, NoteSegmentId, RunId, TenantId, UtcTimestamp, VersionNumber,
};
use thiserror::Error;

/// Canonical media type for one UTF-8 Markdown authoring segment.
pub const NOTE_SEGMENT_MEDIA_TYPE: &str = "text/markdown;charset=utf-8";

/// One Tool call remains well below common model and transport output limits.
pub const MAX_NOTE_SEGMENT_CHARACTERS: u64 = 32_000;
/// Byte quota is independently reachable below the four-byte scalar ceiling;
/// 32,000 CJK scalars fit while an emoji-heavy segment must flush earlier.
pub const MAX_NOTE_SEGMENT_UTF8_BYTES: u64 = 96 * 1_024;
/// Product and Agent writers share the same one-million-character Note quota.
pub const MAX_NOTE_AGGREGATE_CHARACTERS: u64 = 1_000_000;
/// The aggregate byte quota covers the worst normal multi-byte production mix.
pub const MAX_NOTE_AGGREGATE_UTF8_BYTES: u64 = 4 * 1_024 * 1_024;
/// Matches the governed Artifact-version dependency closure bound.
pub const MAX_NOTE_BLOCKS: u64 = 128;
/// Stable section keys are navigation identities, not unbounded user content.
pub const MAX_NOTE_SECTION_KEY_CHARACTERS: usize = 200;

const _: () = {
    assert!(MAX_NOTE_SEGMENT_CHARACTERS < MAX_NOTE_AGGREGATE_CHARACTERS);
    assert!(MAX_NOTE_SEGMENT_UTF8_BYTES < MAX_NOTE_SEGMENT_CHARACTERS * 4);
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NoteAuthoringSessionStatus {
    Open,
    Finalizing,
    Completed,
    Conflicted,
    Failed,
    Cancelled,
}

impl NoteAuthoringSessionStatus {
    #[must_use]
    pub const fn accepts_segments(self) -> bool {
        matches!(self, Self::Open)
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Open | Self::Finalizing)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NoteBlockKind {
    Paragraph,
    Heading,
    List,
    Code,
    Quote,
    Table,
    Divider,
}

/// Runtime-private recovery aggregate for one Note authoring branch.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NoteAuthoringSession {
    pub session_id: NoteAuthoringSessionId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub artifact_id: ArtifactId,
    pub base_revision: VersionNumber,
    pub owner_run_id: RunId,
    pub owner_invocation_id: InvocationId,
    pub status: NoteAuthoringSessionStatus,
    pub next_segment_ordinal: u64,
    pub segment_count: u64,
    pub durable_character_count: u64,
    pub durable_utf8_byte_count: u64,
    pub plan_ref: Option<ContentRef>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl NoteAuthoringSession {
    pub fn validate(&self) -> Result<(), NoteAuthoringContractError> {
        if self.next_segment_ordinal != self.segment_count || self.segment_count > MAX_NOTE_BLOCKS {
            return Err(NoteAuthoringContractError::InvalidSequence);
        }
        if self.durable_character_count > MAX_NOTE_AGGREGATE_CHARACTERS {
            return Err(NoteAuthoringContractError::AggregateCharacterQuota);
        }
        if self.durable_utf8_byte_count > MAX_NOTE_AGGREGATE_UTF8_BYTES {
            return Err(NoteAuthoringContractError::AggregateByteQuota);
        }
        if self.updated_at < self.created_at {
            return Err(NoteAuthoringContractError::InvalidTimestamp);
        }
        if self
            .plan_ref
            .as_ref()
            .is_some_and(|reference| reference.media_type.as_str() != "application/json")
        {
            return Err(NoteAuthoringContractError::InvalidPlanContent);
        }
        Ok(())
    }

    /// Validates an append without mutating the caller-owned session value.
    pub fn validate_append(
        &self,
        segment: &NoteSegmentReceipt,
    ) -> Result<(), NoteAuthoringContractError> {
        self.validate()?;
        segment.validate()?;
        if !self.status.accepts_segments() {
            return Err(NoteAuthoringContractError::SessionNotOpen);
        }
        if segment.session_id != self.session_id
            || segment.tenant_id != self.tenant_id
            || segment.conversation_id != self.conversation_id
            || segment.artifact_id != self.artifact_id
        {
            return Err(NoteAuthoringContractError::ScopeMismatch);
        }
        if segment.ordinal != self.next_segment_ordinal {
            return Err(NoteAuthoringContractError::InvalidSequence);
        }
        if self.segment_count >= MAX_NOTE_BLOCKS {
            return Err(NoteAuthoringContractError::BlockQuota);
        }
        if self
            .durable_character_count
            .checked_add(segment.character_count)
            .is_none_or(|total| total > MAX_NOTE_AGGREGATE_CHARACTERS)
        {
            return Err(NoteAuthoringContractError::AggregateCharacterQuota);
        }
        if self
            .durable_utf8_byte_count
            .checked_add(segment.utf8_byte_count)
            .is_none_or(|total| total > MAX_NOTE_AGGREGATE_UTF8_BYTES)
        {
            return Err(NoteAuthoringContractError::AggregateByteQuota);
        }
        Ok(())
    }
}

/// Immutable receipt proving that one exact segment is durable in a branch.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NoteSegmentReceipt {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub artifact_id: ArtifactId,
    pub session_id: NoteAuthoringSessionId,
    pub segment_id: NoteSegmentId,
    pub ordinal: u64,
    pub block_kind: NoteBlockKind,
    pub section_key: Option<String>,
    pub content_ref: ContentRef,
    pub content_sha256: Sha256,
    pub character_count: u64,
    pub utf8_byte_count: u64,
    pub source_invocation_id: InvocationId,
    pub committed_at: UtcTimestamp,
}

impl NoteSegmentReceipt {
    pub fn validate(&self) -> Result<(), NoteAuthoringContractError> {
        if self.ordinal >= MAX_NOTE_BLOCKS {
            return Err(NoteAuthoringContractError::BlockQuota);
        }
        if self.character_count == 0 {
            return Err(NoteAuthoringContractError::EmptySegment);
        }
        if self.character_count > MAX_NOTE_SEGMENT_CHARACTERS {
            return Err(NoteAuthoringContractError::SegmentCharacterQuota);
        }
        if self.utf8_byte_count == 0
            || self.utf8_byte_count > MAX_NOTE_SEGMENT_UTF8_BYTES
            || self.utf8_byte_count != self.content_ref.byte_length
        {
            return Err(NoteAuthoringContractError::SegmentByteQuota);
        }
        if self.content_ref.media_type.as_str() != NOTE_SEGMENT_MEDIA_TYPE {
            return Err(NoteAuthoringContractError::InvalidSegmentContent);
        }
        if self.content_sha256 != self.content_ref.sha256 {
            return Err(NoteAuthoringContractError::ContentIdentityMismatch);
        }
        validate_section_key(self.section_key.as_deref())
    }
}

/// Immutable structural-sharing edge from a Note manifest revision to a block.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NoteVersionBlock {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub block_id: NoteBlockId,
    pub ordinal: u64,
    pub block_kind: NoteBlockKind,
    pub section_key: Option<String>,
    pub content_ref: ContentRef,
    pub character_count: u64,
    pub utf8_byte_count: u64,
}

impl NoteVersionBlock {
    pub fn validate(&self) -> Result<(), NoteAuthoringContractError> {
        if self.ordinal >= MAX_NOTE_BLOCKS {
            return Err(NoteAuthoringContractError::BlockQuota);
        }
        if self.character_count == 0 || self.character_count > MAX_NOTE_AGGREGATE_CHARACTERS {
            return Err(NoteAuthoringContractError::InvalidBlock);
        }
        if self.utf8_byte_count == 0
            || self.utf8_byte_count > MAX_NOTE_AGGREGATE_UTF8_BYTES
            || self.utf8_byte_count != self.content_ref.byte_length
        {
            return Err(NoteAuthoringContractError::InvalidBlock);
        }
        if self.content_ref.media_type.as_str() != NOTE_SEGMENT_MEDIA_TYPE {
            return Err(NoteAuthoringContractError::InvalidSegmentContent);
        }
        validate_section_key(self.section_key.as_deref())
    }
}

fn validate_section_key(value: Option<&str>) -> Result<(), NoteAuthoringContractError> {
    if value.is_some_and(|key| {
        key.trim().is_empty() || key.chars().count() > MAX_NOTE_SECTION_KEY_CHARACTERS
    }) {
        Err(NoteAuthoringContractError::InvalidSectionKey)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum NoteAuthoringContractError {
    #[error("Note authoring identity is outside the owning tenant, Conversation or Artifact")]
    ScopeMismatch,
    #[error("Note authoring segment order is not contiguous")]
    InvalidSequence,
    #[error("Note authoring session is not open for segment commits")]
    SessionNotOpen,
    #[error("Note authoring segment must not be empty")]
    EmptySegment,
    #[error("Note authoring segment exceeds its Unicode scalar quota")]
    SegmentCharacterQuota,
    #[error("Note authoring segment violates its UTF-8 byte quota or identity")]
    SegmentByteQuota,
    #[error("Note authoring session exceeds its Unicode scalar quota")]
    AggregateCharacterQuota,
    #[error("Note authoring session exceeds its UTF-8 byte quota")]
    AggregateByteQuota,
    #[error("Note authoring session exceeds its immutable block quota")]
    BlockQuota,
    #[error("Note authoring content does not use the canonical segment media type")]
    InvalidSegmentContent,
    #[error("Note authoring ContentRef and digest disagree")]
    ContentIdentityMismatch,
    #[error("Note authoring section key is empty or too long")]
    InvalidSectionKey,
    #[error("Note authoring plan must be application/json")]
    InvalidPlanContent,
    #[error("Note authoring timestamps move backwards")]
    InvalidTimestamp,
    #[error("Note version block is empty or violates aggregate bounds")]
    InvalidBlock,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        ArtifactId, ContentId, ConversationId, InvocationId, NoteAuthoringSessionId, NoteSegmentId,
        RunId, TenantId, UtcTimestamp, VersionNumber,
    };

    use super::{
        MAX_NOTE_AGGREGATE_CHARACTERS, MAX_NOTE_SEGMENT_CHARACTERS, MAX_NOTE_SEGMENT_UTF8_BYTES,
        NOTE_SEGMENT_MEDIA_TYPE, NoteAuthoringContractError, NoteAuthoringSession,
        NoteAuthoringSessionStatus, NoteBlockKind, NoteSegmentReceipt,
    };

    fn fixture() -> (NoteAuthoringSession, NoteSegmentReceipt) {
        let tenant_id = TenantId::generate();
        let conversation_id = ConversationId::generate();
        let artifact_id = ArtifactId::generate();
        let session_id = NoteAuthoringSessionId::generate();
        let bytes = "中文🙂".as_bytes();
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(NOTE_SEGMENT_MEDIA_TYPE).expect("media type"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(bytes),
        };
        let session = NoteAuthoringSession {
            session_id,
            tenant_id,
            conversation_id,
            artifact_id,
            base_revision: VersionNumber::new(1).expect("revision"),
            owner_run_id: RunId::generate(),
            owner_invocation_id: InvocationId::generate(),
            status: NoteAuthoringSessionStatus::Open,
            next_segment_ordinal: 0,
            segment_count: 0,
            durable_character_count: 0,
            durable_utf8_byte_count: 0,
            plan_ref: None,
            created_at: UtcTimestamp::from_unix_millis(1),
            updated_at: UtcTimestamp::from_unix_millis(1),
        };
        let segment = NoteSegmentReceipt {
            tenant_id,
            conversation_id,
            artifact_id,
            session_id,
            segment_id: NoteSegmentId::generate(),
            ordinal: 0,
            block_kind: NoteBlockKind::Paragraph,
            section_key: None,
            content_sha256: reference.sha256,
            content_ref: reference,
            character_count: 3,
            utf8_byte_count: bytes.len() as u64,
            source_invocation_id: InvocationId::generate(),
            committed_at: UtcTimestamp::from_unix_millis(2),
        };
        (session, segment)
    }

    #[test]
    fn append_measures_unicode_scalars_and_utf8_bytes_independently() {
        let (session, segment) = fixture();
        assert_eq!(session.validate_append(&segment), Ok(()));
        assert!(segment.utf8_byte_count > segment.character_count);
    }

    #[test]
    fn append_rejects_order_scope_and_aggregate_overflow() {
        let (mut session, mut segment) = fixture();
        segment.ordinal = 1;
        assert_eq!(
            session.validate_append(&segment),
            Err(NoteAuthoringContractError::InvalidSequence)
        );
        segment.ordinal = 0;
        segment.conversation_id = ConversationId::generate();
        assert_eq!(
            session.validate_append(&segment),
            Err(NoteAuthoringContractError::ScopeMismatch)
        );
        segment.conversation_id = session.conversation_id;
        session.durable_character_count = MAX_NOTE_AGGREGATE_CHARACTERS;
        assert_eq!(
            session.validate_append(&segment),
            Err(NoteAuthoringContractError::AggregateCharacterQuota)
        );
    }

    #[test]
    fn segment_limit_is_not_the_aggregate_limit() {
        let (session, mut segment) = fixture();
        segment.character_count = MAX_NOTE_SEGMENT_CHARACTERS + 1;
        assert_eq!(
            session.validate_append(&segment),
            Err(NoteAuthoringContractError::SegmentCharacterQuota)
        );
        segment.character_count = MAX_NOTE_SEGMENT_CHARACTERS;
        segment.utf8_byte_count = MAX_NOTE_SEGMENT_UTF8_BYTES + 1;
        segment.content_ref.byte_length = segment.utf8_byte_count;
        assert_eq!(
            session.validate_append(&segment),
            Err(NoteAuthoringContractError::SegmentByteQuota)
        );
    }
}
