use musteragent_content_ref::ContentRef;
use musteragent_identity::{ArtifactId, ArtifactVersionId, VersionNumber, VersionedKey};
use thiserror::Error;

pub const VERSIONED_FILE_SCHEMA: &str = "musteragent.versioned-file/1";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VersionedFileDocument {
    pub file_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub version: VersionNumber,
    pub content_ref: ContentRef,
    pub encoding: Option<&'static str>,
    pub format_adapter: VersionedKey,
    pub line_index_ref: Option<ContentRef>,
    pub outline_ref: Option<ContentRef>,
}

impl VersionedFileDocument {
    pub fn validate(&self) -> Result<(), VersionedFileContractError> {
        if self.content_ref.byte_length == 0 {
            return Err(VersionedFileContractError::EmptyContent);
        }
        if let Some(encoding) = self.encoding
            && encoding != "utf-8"
        {
            return Err(VersionedFileContractError::InvalidEncoding);
        }
        for derived in [&self.line_index_ref, &self.outline_ref]
            .into_iter()
            .flatten()
        {
            if derived.content_id == self.content_ref.content_id {
                return Err(VersionedFileContractError::DerivedContentAliasesBody);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum VersionedFileContractError {
    #[error("Versioned file content cannot be empty")]
    EmptyContent,
    #[error("Text file encoding must be UTF-8")]
    InvalidEncoding,
    #[error("Derived file index cannot alias the file body")]
    DerivedContentAliasesBody,
}
