use musteragent_content_ref::ContentRef;
use musteragent_policy::PermissionMode;
use thiserror::Error;

use crate::DisplayText;

const MAX_INLINE_INPUT_BYTES: usize = 64 * 1_024;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct InlineInput(String);

impl InlineInput {
    pub fn new(value: impl Into<String>) -> Result<Self, InlineInputError> {
        let value = value.into();
        if value.is_empty() {
            return Err(InlineInputError::Empty);
        }
        if value.len() > MAX_INLINE_INPUT_BYTES {
            return Err(InlineInputError::TooLong {
                actual_bytes: value.len(),
            });
        }
        if value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(InlineInputError::ControlCharacter);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum InlineInputError {
    #[error("inline input cannot be empty")]
    Empty,
    #[error("inline input exceeds 64 KiB: {actual_bytes} bytes")]
    TooLong { actual_bytes: usize },
    #[error("inline input contains an unsupported control character")]
    ControlCharacter,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum InputContent {
    InlineUtf8(InlineInput),
    ContentRef(ContentRef),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AttachmentRef {
    pub content_ref: ContentRef,
    pub display_name: DisplayText,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ModelClass(String);

impl ModelClass {
    /// Accepts an exact Admin model key or a route-group key.
    ///
    /// Model registry keys are provider-facing identifiers rather than Musteragent
    /// dotted logical keys. Real values include uppercase letters, slashes,
    /// colons and version punctuation, so narrowing this value to `DottedKey`
    /// silently makes exact model selection impossible.
    pub fn new(value: impl Into<String>) -> Result<Self, ModelClassError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ModelClassError::Empty);
        }
        if value.len() > 255 {
            return Err(ModelClassError::TooLong);
        }
        if !value
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric())
            || !value.chars().all(|character| {
                character.is_ascii_alphanumeric()
                    || matches!(character, '.' | '-' | '_' | '/' | ':' | '@' | '+')
            })
        {
            return Err(ModelClassError::InvalidGrammar);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ModelClassError {
    #[error("model selection cannot be empty")]
    Empty,
    #[error(
        "model selection must start with an ASCII letter or digit and contain only ASCII letters, digits, '.', '-', '_', '/', ':', '@' or '+'"
    )]
    InvalidGrammar,
    #[error("model selection exceeds 255 bytes")]
    TooLong,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReasoningEffort {
    None,
    Enabled,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Ultra,
}

impl ReasoningEffort {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Enabled => "enabled",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
            Self::Ultra => "ultra",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "enabled" => Some(Self::Enabled),
            "minimal" => Some(Self::Minimal),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::XHigh),
            "max" => Some(Self::Max),
            "ultra" => Some(Self::Ultra),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct RequestedFeatures {
    pub reasoning_summary: bool,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub permission_mode: PermissionMode,
}

impl Default for RequestedFeatures {
    fn default() -> Self {
        Self {
            reasoning_summary: false,
            reasoning_effort: None,
            permission_mode: PermissionMode::Supervised,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelClass, ModelClassError};

    #[test]
    fn model_selection_accepts_real_registry_and_route_group_keys() {
        for value in [
            "chat.balanced",
            "kimi-k3",
            "MiniMax-M2.7",
            "x-ai/grok-4.1-fast",
            "anthropic.claude-opus-4-6-v1:0",
            "deployment@2026-07-31",
        ] {
            assert_eq!(
                ModelClass::new(value)
                    .unwrap_or_else(|error| panic!("{value} rejected: {error}"))
                    .as_str(),
                value
            );
        }
    }

    #[test]
    fn model_selection_rejects_ambiguous_or_unbounded_values() {
        assert_eq!(ModelClass::new(""), Err(ModelClassError::Empty));
        assert_eq!(
            ModelClass::new(" model"),
            Err(ModelClassError::InvalidGrammar)
        );
        assert_eq!(
            ModelClass::new("model?query"),
            Err(ModelClassError::InvalidGrammar)
        );
        assert_eq!(
            ModelClass::new("m".repeat(256)),
            Err(ModelClassError::TooLong)
        );
    }
}
