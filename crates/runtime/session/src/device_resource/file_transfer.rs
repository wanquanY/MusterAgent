//! Original file bytes travel on a bounded data channel, never in Tool results.
//! Every chunk retains the original frozen invocation and current Native fence.
use super::*;
use musteragent_runtime_file_query::FileReference;
use serde::Deserialize;

pub const DEVICE_FILE_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceFileImportInput {
    pub file_ref: Option<FileReference>,
    #[serde(default, deserialize_with = "artifact_id")]
    pub artifact_id: Option<ArtifactId>,
    /// Select an exact declared dependency for a structured media Artifact.
    /// Omit for the original file itself. This is never an independent grant.
    #[serde(default, deserialize_with = "content_id")]
    pub content_id: Option<ContentId>,
    pub path: String,
}
impl DeviceFileImportInput {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let value: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
        if (value.file_ref.is_some() && value.artifact_id.is_some())
            || (value.file_ref.is_none()
                && value.artifact_id.is_none()
                && value.content_id.is_none())
        {
            return Err(Error::InvalidArguments);
        }
        let path = std::path::Path::new(&value.path);
        if value.path.is_empty()
            || value.path.len() > 4096
            || path.is_absolute()
            || path
                .components()
                .any(|p| !matches!(p, std::path::Component::Normal(_)))
            || value.path.split('/').any(|p| p == ".musteragent")
        {
            return Err(Error::InvalidArguments);
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceFileChunk {
    pub command_commitment: Sha256,
    pub content_ref: ContentRef,
    pub offset: u64,
    pub data: Vec<u8>,
    /// A separate final observation is required, including for an empty file.
    pub eof: bool,
}
impl DeviceFileChunk {
    pub fn validate(&self, request: &DeviceResourceRequest, offset: u64) -> Result<(), Error> {
        if request.command().operation() != DeviceResourceOperation::WorkspaceFileImport
            || self.command_commitment != request.command().commitment()
            || self.offset != offset
            || offset > self.content_ref.byte_length
            || self.content_ref.byte_length > musteragent_runtime_content::MAX_CONTENT_BYTES
            || self.data.len() > DEVICE_FILE_CHUNK_BYTES
            || self.eof != (offset == self.content_ref.byte_length)
            || (self.eof && !self.data.is_empty())
            || (!self.eof
                && (self.data.is_empty()
                    || offset
                        .checked_add(self.data.len() as u64)
                        .is_none_or(|end| end > self.content_ref.byte_length)))
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}

fn content_id<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<ContentId>, D::Error> {
    let value = Option::<String>::deserialize(d)?;
    value
        .map(|s| s.parse().map_err(serde::de::Error::custom))
        .transpose()
}

fn artifact_id<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<ArtifactId>, D::Error> {
    let value = Option::<String>::deserialize(d)?;
    value
        .map(|s| s.parse().map_err(serde::de::Error::custom))
        .transpose()
}
