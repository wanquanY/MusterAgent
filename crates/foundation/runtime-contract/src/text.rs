use thiserror::Error;

const DISPLAY_TEXT_MAX_BYTES: usize = 512;
const SAFE_DISPLAY_TEXT_MAX_BYTES: usize = 1_024;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DisplayText(String);

impl DisplayText {
    pub fn new(value: impl Into<String>) -> Result<Self, TextContractError> {
        let value = value.into();
        validate_text(&value, DISPLAY_TEXT_MAX_BYTES)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SafeDisplayText(String);

impl SafeDisplayText {
    pub fn new(value: impl Into<String>) -> Result<Self, TextContractError> {
        let value = value.into();
        validate_text(&value, SAFE_DISPLAY_TEXT_MAX_BYTES)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_text(value: &str, maximum_bytes: usize) -> Result<(), TextContractError> {
    if value.is_empty() {
        return Err(TextContractError::Empty);
    }
    if value.len() > maximum_bytes {
        return Err(TextContractError::TooLong {
            maximum_bytes,
            actual_bytes: value.len(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(TextContractError::ControlCharacter);
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum TextContractError {
    #[error("display text cannot be empty")]
    Empty,
    #[error("display text exceeds {maximum_bytes} bytes: {actual_bytes}")]
    TooLong {
        maximum_bytes: usize,
        actual_bytes: usize,
    },
    #[error("display text cannot contain control characters")]
    ControlCharacter,
}
