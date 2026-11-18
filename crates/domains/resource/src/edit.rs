use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ArtifactId, ResourceEditSessionId, ResourceRevision, UtcTimestamp, VersionNumber,
};
use thiserror::Error;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EditSessionState {
    Open,
    Finalizing,
    Committed,
    Aborted,
    Conflicted,
    Expired,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditSession {
    pub session_id: ResourceEditSessionId,
    pub file_artifact_id: ArtifactId,
    pub base_version: VersionNumber,
    pub state: EditSessionState,
    pub session_revision: ResourceRevision,
    pub next_sequence: u64,
    pub checkpoint_hash: Option<Sha256>,
    pub expires_at: UtcTimestamp,
}

impl EditSession {
    pub fn validate(&self) -> Result<(), TextEditError> {
        if self.next_sequence == 0 {
            return Err(TextEditError::InvalidSequence);
        }
        if matches!(self.state, EditSessionState::Committed) && self.checkpoint_hash.is_none() {
            return Err(TextEditError::MissingCheckpoint);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TextRange {
    pub start_byte: usize,
    pub end_byte: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TextAnchor {
    pub byte_offset: usize,
    pub expected_before: Option<Sha256>,
    pub expected_after: Option<Sha256>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TextEditOperation {
    Insert {
        anchor: TextAnchor,
        text: String,
    },
    Replace {
        range: TextRange,
        expected_hash: Sha256,
        text: String,
    },
    Delete {
        range: TextRange,
        expected_hash: Sha256,
    },
}

/// Applies a complete operation set to one immutable UTF-8 base.
///
/// Every range/hash is validated against the same base before any mutation is
/// visible. Operations must be non-overlapping; execution runs back-to-front
/// so byte offsets remain stable and the outcome is atomic.
pub fn apply_text_operations(
    base: &str,
    operations: &[TextEditOperation],
) -> Result<String, TextEditError> {
    if operations.is_empty() {
        return Err(TextEditError::EmptyOperationSet);
    }
    let mut normalized = operations
        .iter()
        .map(|operation| validate_operation(base, operation))
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort_by_key(|operation| (operation.start, operation.end));
    for adjacent in normalized.windows(2) {
        if adjacent[0].end > adjacent[1].start
            || (adjacent[0].start == adjacent[0].end
                && adjacent[1].start == adjacent[1].end
                && adjacent[0].start == adjacent[1].start)
        {
            return Err(TextEditError::OverlappingOperations);
        }
    }
    let mut output = base.to_owned();
    for operation in normalized.into_iter().rev() {
        output.replace_range(operation.start..operation.end, &operation.replacement);
    }
    Ok(output)
}

struct NormalizedOperation {
    start: usize,
    end: usize,
    replacement: String,
}

fn validate_operation(
    base: &str,
    operation: &TextEditOperation,
) -> Result<NormalizedOperation, TextEditError> {
    match operation {
        TextEditOperation::Insert { anchor, text } => {
            validate_boundary(base, anchor.byte_offset)?;
            if text.is_empty() {
                return Err(TextEditError::EmptyReplacement);
            }
            if let Some(expected) = anchor.expected_before {
                let prefix = &base.as_bytes()[..anchor.byte_offset];
                if Sha256::digest(prefix) != expected {
                    return Err(TextEditError::HashMismatch);
                }
            }
            if let Some(expected) = anchor.expected_after {
                let suffix = &base.as_bytes()[anchor.byte_offset..];
                if Sha256::digest(suffix) != expected {
                    return Err(TextEditError::HashMismatch);
                }
            }
            Ok(NormalizedOperation {
                start: anchor.byte_offset,
                end: anchor.byte_offset,
                replacement: text.clone(),
            })
        }
        TextEditOperation::Replace {
            range,
            expected_hash,
            text,
        } => {
            validate_range(base, *range)?;
            if text.is_empty() {
                return Err(TextEditError::EmptyReplacement);
            }
            validate_hash(base, *range, *expected_hash)?;
            Ok(NormalizedOperation {
                start: range.start_byte,
                end: range.end_byte,
                replacement: text.clone(),
            })
        }
        TextEditOperation::Delete {
            range,
            expected_hash,
        } => {
            validate_range(base, *range)?;
            validate_hash(base, *range, *expected_hash)?;
            Ok(NormalizedOperation {
                start: range.start_byte,
                end: range.end_byte,
                replacement: String::new(),
            })
        }
    }
}

fn validate_hash(base: &str, range: TextRange, expected: Sha256) -> Result<(), TextEditError> {
    let actual = Sha256::digest(&base.as_bytes()[range.start_byte..range.end_byte]);
    (actual == expected)
        .then_some(())
        .ok_or(TextEditError::HashMismatch)
}

fn validate_range(base: &str, range: TextRange) -> Result<(), TextEditError> {
    if range.start_byte >= range.end_byte || range.end_byte > base.len() {
        return Err(TextEditError::InvalidRange);
    }
    validate_boundary(base, range.start_byte)?;
    validate_boundary(base, range.end_byte)
}

fn validate_boundary(base: &str, offset: usize) -> Result<(), TextEditError> {
    if offset > base.len() || !base.is_char_boundary(offset) {
        Err(TextEditError::InvalidUtf8Boundary)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum TextEditError {
    #[error("Edit operation set cannot be empty")]
    EmptyOperationSet,
    #[error("Edit range is invalid")]
    InvalidRange,
    #[error("Edit offset is not a UTF-8 boundary")]
    InvalidUtf8Boundary,
    #[error("Edit precondition hash does not match")]
    HashMismatch,
    #[error("Edit operations overlap")]
    OverlappingOperations,
    #[error("Insert/replace content cannot be empty")]
    EmptyReplacement,
    #[error("Edit sequence must start at one")]
    InvalidSequence,
    #[error("Committed Edit Session must have a checkpoint")]
    MissingCheckpoint,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::Sha256;

    use super::{TextAnchor, TextEditError, TextEditOperation, TextRange, apply_text_operations};

    #[test]
    fn applies_non_overlapping_operations_atomically() {
        let base = "hello world";
        let result = apply_text_operations(
            base,
            &[
                TextEditOperation::Replace {
                    range: TextRange {
                        start_byte: 0,
                        end_byte: 5,
                    },
                    expected_hash: Sha256::digest(b"hello"),
                    text: "Hello".to_owned(),
                },
                TextEditOperation::Insert {
                    anchor: TextAnchor {
                        byte_offset: base.len(),
                        expected_before: Some(Sha256::digest(base.as_bytes())),
                        expected_after: Some(Sha256::digest(b"")),
                    },
                    text: "!".to_owned(),
                },
            ],
        )
        .expect("edit");
        assert_eq!(result, "Hello world!");
    }

    #[test]
    fn rejects_a_single_bad_precondition_without_output() {
        let result = apply_text_operations(
            "hello",
            &[TextEditOperation::Delete {
                range: TextRange {
                    start_byte: 0,
                    end_byte: 5,
                },
                expected_hash: Sha256::digest(b"other"),
            }],
        );
        assert_eq!(result, Err(TextEditError::HashMismatch));
    }
}
