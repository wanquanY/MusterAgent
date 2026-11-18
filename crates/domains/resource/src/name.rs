use thiserror::Error;
use unicode_normalization::UnicodeNormalization as _;

pub const MAX_RESOURCE_NAME_BYTES: usize = 255;

/// Canonical user-visible node name and its collision key.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResourceName {
    display: String,
    comparison_key: String,
}

impl ResourceName {
    /// A generated document title is not a path. Preserve its readable text
    /// within the file-system byte bound and reserve space for disambiguation.
    pub fn generated_file(
        title: &str,
        extension: &str,
        ordinal: u32,
    ) -> Result<Self, ResourceNameError> {
        let extension = extension.trim_start_matches('.');
        if extension.is_empty()
            || extension.len() > 12
            || !extension.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err(ResourceNameError::Invalid);
        }
        let normalized: String = title
            .nfc()
            .map(|c| {
                if c.is_control() || c == '/' || c == '\\' {
                    ' '
                } else {
                    c
                }
            })
            .collect();
        let mut stem = normalized.trim().to_owned();
        let suffix = format!(".{extension}");
        if stem
            .get(stem.len().saturating_sub(suffix.len())..)
            .is_some_and(|tail| tail.eq_ignore_ascii_case(&suffix))
        {
            stem.truncate(stem.len() - suffix.len());
        }
        let suffix = if ordinal > 1 {
            format!(" ({ordinal}){suffix}")
        } else {
            suffix
        };
        while stem.len() + suffix.len() > MAX_RESOURCE_NAME_BYTES {
            stem.pop();
        }
        let stem = stem.trim();
        Self::new(format!(
            "{}{suffix}",
            if stem.is_empty() {
                "Presentation"
            } else {
                stem
            }
        ))
    }

    /// A generated folder reserves the stable identity suffix before truncating
    /// a user title. Titles cannot inject paths or exhaust the suffix budget.
    pub fn generated_folder(title: &str, suffix: &str) -> Result<Self, ResourceNameError> {
        if suffix.len() >= MAX_RESOURCE_NAME_BYTES {
            return Err(ResourceNameError::Invalid);
        }
        let normalized: String = title
            .nfc()
            .map(|c| {
                if c.is_control() || c == '/' || c == '\\' {
                    ' '
                } else {
                    c
                }
            })
            .collect();
        let mut stem = normalized.trim().to_owned();
        if stem.is_empty() {
            stem = "Folder".to_owned();
        }
        while stem.len() + suffix.len() > MAX_RESOURCE_NAME_BYTES {
            stem.pop();
        }
        Self::new(format!("{}{suffix}", stem.trim()))
    }

    pub fn new(value: impl Into<String>) -> Result<Self, ResourceNameError> {
        let value = value.into();
        if value.is_empty()
            || value.trim() != value
            || value == "."
            || value == ".."
            || value.len() > MAX_RESOURCE_NAME_BYTES
            || value.chars().any(|character| {
                character.is_control() || character == '/' || character == '\\' || character == '\0'
            })
        {
            return Err(ResourceNameError::Invalid);
        }
        let normalized = value.nfc().collect::<String>();
        if normalized != value {
            return Err(ResourceNameError::NotNormalized);
        }
        let comparison_key = normalized
            .chars()
            .flat_map(char::to_lowercase)
            .collect::<String>()
            .nfc()
            .collect();
        Ok(Self {
            display: normalized,
            comparison_key,
        })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.display
    }

    #[must_use]
    pub fn comparison_key(&self) -> &str {
        &self.comparison_key
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceNameError {
    #[error("Resource node name is invalid")]
    Invalid,
    #[error("Resource node name must be Unicode NFC")]
    NotNormalized,
}

#[cfg(test)]
mod tests {
    use super::{ResourceName, ResourceNameError};

    #[test]
    fn rejects_path_and_non_canonical_names() {
        for invalid in ["", " a", "a ", ".", "..", "a/b", "a\\b", "a\n"] {
            assert_eq!(ResourceName::new(invalid), Err(ResourceNameError::Invalid));
        }
        assert_eq!(
            ResourceName::new("e\u{301}.md"),
            Err(ResourceNameError::NotNormalized)
        );
    }

    #[test]
    fn creates_stable_case_insensitive_collision_key() {
        let upper = ResourceName::new("Main.PY").expect("name");
        let lower = ResourceName::new("main.py").expect("name");
        assert_eq!(upper.comparison_key(), lower.comparison_key());
        assert_eq!(upper.as_str(), "Main.PY");
    }

    #[test]
    fn generated_titles_are_safe_bounded_filenames_without_losing_the_extension() {
        assert_eq!(
            ResourceName::generated_file("  e\u{301}/项目\\总结\n.PPTX ", "pptx", 2)
                .expect("filename")
                .as_str(),
            "é 项目 总结 (2).pptx"
        );
        let long = ResourceName::generated_file(&"成果".repeat(200), "pptx", 100)
            .expect("bounded filename");
        assert!(long.as_str().len() <= super::MAX_RESOURCE_NAME_BYTES);
        assert!(long.as_str().ends_with(" (100).pptx"));
        assert_eq!(
            ResourceName::generated_file("\n/\\", "pptx", 1)
                .expect("empty title")
                .as_str(),
            "Presentation.pptx"
        );
        assert!(ResourceName::generated_file("title", &"x".repeat(300), 1).is_err());
    }
}
