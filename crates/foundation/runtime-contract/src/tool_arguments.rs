use std::str::FromStr;

use thiserror::Error;

/// Maximum number of schema violations retained for one rejected model Tool
/// action. The Runtime persists only bounded property paths, never argument
/// values, so a correction remains useful without becoming a second copy of
/// potentially sensitive model output.
pub const MAX_TOOL_ARGUMENT_VALIDATION_ISSUES: usize = 3;

/// Returns whether a rejection issue collection is already in its canonical
/// durable form: bounded, strictly sorted, and duplicate-free.
#[must_use]
pub fn tool_argument_validation_issues_are_canonical(
    issues: &[ToolArgumentValidationIssue],
) -> bool {
    issues.len() <= MAX_TOOL_ARGUMENT_VALIDATION_ISSUES
        && issues.windows(2).all(|pair| pair[0] < pair[1])
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ToolArgumentValidationIssueKind {
    GenerationTokenLimit,
    MalformedJson,
    ObjectRequired,
    MissingRequired,
    UnexpectedProperty,
    InvalidValue,
}

impl ToolArgumentValidationIssueKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GenerationTokenLimit => "generation_token_limit",
            Self::MalformedJson => "malformed_json",
            Self::ObjectRequired => "object_required",
            Self::MissingRequired => "missing_required",
            Self::UnexpectedProperty => "unexpected_property",
            Self::InvalidValue => "invalid_value",
        }
    }
}

impl FromStr for ToolArgumentValidationIssueKind {
    type Err = ToolArgumentValidationIssueKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "generation_token_limit" => Ok(Self::GenerationTokenLimit),
            "malformed_json" => Ok(Self::MalformedJson),
            "object_required" => Ok(Self::ObjectRequired),
            "missing_required" => Ok(Self::MissingRequired),
            "unexpected_property" => Ok(Self::UnexpectedProperty),
            "invalid_value" => Ok(Self::InvalidValue),
            _ => Err(ToolArgumentValidationIssueKindParseError),
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("Tool argument validation issue kind is not recognized")]
pub struct ToolArgumentValidationIssueKindParseError;

/// A value-free, model-correctable JSON-Schema violation.
///
/// Paths are deliberately restricted to the same bounded JSON-Pointer subset
/// produced by Runtime schema validation. This type is safe to persist and
/// project into a private correction turn because it cannot contain argument
/// values, provider responses, credentials, model identities, or prices.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ToolArgumentValidationIssue {
    kind: ToolArgumentValidationIssueKind,
    path: String,
}

impl ToolArgumentValidationIssue {
    pub fn new(
        kind: ToolArgumentValidationIssueKind,
        path: impl Into<String>,
    ) -> Result<Self, ToolArgumentValidationIssueError> {
        let path = path.into();
        if path.is_empty()
            || path.len() > 256
            || !path.starts_with('/')
            || !path.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'~')
            })
        {
            return Err(ToolArgumentValidationIssueError);
        }
        Ok(Self { kind, path })
    }

    #[must_use]
    pub const fn kind(&self) -> ToolArgumentValidationIssueKind {
        self.kind
    }

    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("Tool argument validation issue path is not a bounded safe JSON Pointer")]
pub struct ToolArgumentValidationIssueError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_issue_accepts_only_value_free_safe_paths() {
        let issue = ToolArgumentValidationIssue::new(
            ToolArgumentValidationIssueKind::UnexpectedProperty,
            "/reference_images/0/label",
        )
        .expect("safe path");
        assert_eq!(issue.path(), "/reference_images/0/label");
        assert_eq!(issue.kind().as_str(), "unexpected_property");

        assert!(
            ToolArgumentValidationIssue::new(
                ToolArgumentValidationIssueKind::InvalidValue,
                "/prompt/secret value",
            )
            .is_err()
        );
        assert!(
            ToolArgumentValidationIssue::new(
                ToolArgumentValidationIssueKind::InvalidValue,
                "relative/path",
            )
            .is_err()
        );

        let duplicate = vec![issue.clone(), issue];
        assert!(!tool_argument_validation_issues_are_canonical(&duplicate));
    }
}
