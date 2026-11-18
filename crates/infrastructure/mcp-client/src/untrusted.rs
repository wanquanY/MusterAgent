//! Wrappers for server-controlled data.

/// Text that originated from an MCP server (tool descriptions, error
/// messages, server instructions, resource URIs).
///
/// It is retained byte-for-byte (apart from explicit, reported truncation)
/// and intentionally implements neither `Display` nor `Deref<Target = str>`:
/// a caller must opt in with [`UntrustedText::as_untrusted_str`] and is then
/// responsible for presenting it to a model as quoted data, never as
/// instructions.
#[derive(Clone, Eq, PartialEq, Hash, Default)]
pub struct UntrustedText {
    text: String,
    original_bytes: usize,
}

impl UntrustedText {
    /// Retains at most `limit` bytes, cutting on a UTF-8 boundary.
    #[must_use]
    pub fn bounded(value: &str, limit: usize) -> Self {
        let original_bytes = value.len();
        if value.len() <= limit {
            return Self {
                text: value.to_owned(),
                original_bytes,
            };
        }
        let mut end = limit;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            text: value[..end].to_owned(),
            original_bytes,
        }
    }

    #[must_use]
    pub fn as_untrusted_str(&self) -> &str {
        &self.text
    }

    /// True when the retained text is shorter than what the server sent.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.original_bytes > self.text.len()
    }

    #[must_use]
    pub fn original_bytes(&self) -> usize {
        self.original_bytes
    }
}

impl std::fmt::Debug for UntrustedText {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UntrustedText")
            .field("bytes", &self.text.len())
            .field("original_bytes", &self.original_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::UntrustedText;

    #[test]
    fn truncation_is_on_char_boundary_and_reported() {
        let text = UntrustedText::bounded("zé", 2);
        assert_eq!(text.as_untrusted_str(), "z");
        assert!(text.is_truncated());
        assert_eq!(text.original_bytes(), 3);
        assert!(!UntrustedText::bounded("abc", 3).is_truncated());
        assert!(!format!("{text:?}").contains('z'));
    }
}
