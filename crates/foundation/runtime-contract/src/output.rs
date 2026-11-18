use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{FrameId, MessageId, OutputId};
use thiserror::Error;

const MAX_INLINE_FRAME_BYTES: usize = 64 * 1_024;

/// Bounded persisted UTF-8 frame, distinct from command input.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct InlineFrame(String);

impl InlineFrame {
    pub fn new(value: impl Into<String>) -> Result<Self, InlineFrameError> {
        let value = value.into();
        if value.is_empty() {
            return Err(InlineFrameError::Empty);
        }
        if value.len() > MAX_INLINE_FRAME_BYTES {
            return Err(InlineFrameError::TooLong {
                actual_bytes: value.len(),
            });
        }
        if value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(InlineFrameError::ControlCharacter);
        }
        Ok(Self(value))
    }

    /// Creates a zero-byte terminal marker. It is valid only when the owning
    /// [`OutputFrame`] has `final_for_output=true`.
    #[must_use]
    pub fn final_marker() -> Self {
        Self(String::new())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn byte_length(&self) -> u64 {
        self.0.len() as u64
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum InlineFrameError {
    #[error("inline frame cannot be empty")]
    Empty,
    #[error("inline frame exceeds 64 KiB: {actual_bytes} bytes")]
    TooLong { actual_bytes: usize },
    #[error("inline frame contains an unsupported control character")]
    ControlCharacter,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OutputChannel {
    AssistantText,
    ReasoningSummary,
    ToolArguments,
    ToolProgress,
    ArtifactContent,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum FrameContent {
    InlineUtf8(InlineFrame),
    ContentRef(ContentRef),
}

impl FrameContent {
    #[must_use]
    pub fn byte_length(&self) -> u64 {
        match self {
            Self::InlineUtf8(value) => value.byte_length(),
            Self::ContentRef(reference) => reference.byte_length,
        }
    }

    #[must_use]
    pub fn content_hash(&self) -> Sha256 {
        match self {
            Self::InlineUtf8(value) => Sha256::digest(value.as_str().as_bytes()),
            Self::ContentRef(reference) => reference.sha256,
        }
    }
}

/// Durable coalesced stream frame sharing the Conversation Ledger sequence.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OutputFrame {
    pub frame_id: FrameId,
    pub output_id: OutputId,
    pub message_id: Option<MessageId>,
    pub channel: OutputChannel,
    pub frame_index: u64,
    pub byte_offset_start: u64,
    pub byte_offset_end: u64,
    pub content: FrameContent,
    pub content_hash: Sha256,
    pub final_for_output: bool,
}

impl OutputFrame {
    pub fn validate(&self) -> Result<(), OutputFrameError> {
        if self.content.byte_length() == 0 && !self.final_for_output {
            return Err(OutputFrameError::EmptyNonTerminalFrame);
        }
        let expected_end = self
            .byte_offset_start
            .checked_add(self.content.byte_length())
            .ok_or(OutputFrameError::OffsetOverflow)?;
        if self.byte_offset_end != expected_end {
            return Err(OutputFrameError::InvalidByteRange);
        }
        if self.content_hash != self.content.content_hash() {
            return Err(OutputFrameError::ContentHashMismatch);
        }
        if self.channel == OutputChannel::AssistantText && self.message_id.is_none() {
            return Err(OutputFrameError::AssistantMessageMissing);
        }
        Ok(())
    }
}

/// Incremental validator for one output stream.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OutputSequence {
    output_id: OutputId,
    next_frame_index: u64,
    next_byte_offset: u64,
    sealed: bool,
}

impl OutputSequence {
    #[must_use]
    pub const fn new(output_id: OutputId) -> Self {
        Self {
            output_id,
            next_frame_index: 0,
            next_byte_offset: 0,
            sealed: false,
        }
    }

    pub fn accept(&mut self, frame: &OutputFrame) -> Result<(), OutputFrameError> {
        frame.validate()?;
        if frame.output_id != self.output_id {
            return Err(OutputFrameError::OutputIdentityMismatch);
        }
        if frame.frame_index != self.next_frame_index {
            return Err(OutputFrameError::FrameIndexDiscontinuity);
        }
        if frame.byte_offset_start != self.next_byte_offset {
            return Err(OutputFrameError::ByteOffsetDiscontinuity);
        }
        if self.sealed {
            // Older PostgreSQL recovery writers could durably replay an empty
            // terminal marker after the Provider terminal batch had already
            // sealed the stream. This exact, contiguous re-seal is content
            // idempotent and must remain readable. Every other append after a
            // terminal frame is still rejected.
            if !frame.final_for_output
                || !matches!(
                    &frame.content,
                    FrameContent::InlineUtf8(value) if value.byte_length() == 0
                )
            {
                return Err(OutputFrameError::OutputAlreadyFinal);
            }
            self.next_frame_index = self
                .next_frame_index
                .checked_add(1)
                .ok_or(OutputFrameError::FrameIndexOverflow)?;
            return Ok(());
        }
        self.next_frame_index = self
            .next_frame_index
            .checked_add(1)
            .ok_or(OutputFrameError::FrameIndexOverflow)?;
        self.next_byte_offset = frame.byte_offset_end;
        self.sealed = frame.final_for_output;
        Ok(())
    }

    #[must_use]
    pub const fn is_sealed(self) -> bool {
        self.sealed
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum OutputFrameError {
    #[error("a zero-byte output frame is allowed only as a terminal marker")]
    EmptyNonTerminalFrame,
    #[error("output frame byte offset overflow")]
    OffsetOverflow,
    #[error("output frame byte range does not match content length")]
    InvalidByteRange,
    #[error("output frame content hash does not match its content")]
    ContentHashMismatch,
    #[error("assistant text frame requires a message identity")]
    AssistantMessageMissing,
    #[error("cannot append after final_for_output")]
    OutputAlreadyFinal,
    #[error("output frame belongs to another output")]
    OutputIdentityMismatch,
    #[error("output frame index is not contiguous")]
    FrameIndexDiscontinuity,
    #[error("output frame byte offset is not contiguous")]
    ByteOffsetDiscontinuity,
    #[error("output frame index overflow")]
    FrameIndexOverflow,
}
