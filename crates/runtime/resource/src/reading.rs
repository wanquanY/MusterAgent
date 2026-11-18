use std::sync::Arc;

use musteragent_content_ref::ContentRef;
use musteragent_identity::{ArtifactId, ArtifactVersionId, ResourceNodeId, VersionNumber};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;

use crate::file_parsing::ParsedFileContent;
use crate::{ResourceCommandContext, ResourceStoreError};

pub const DEFAULT_PROJECT_FILE_READ_BYTES: usize = 32 * 1024;
pub const MAX_PROJECT_FILE_READ_BYTES: usize = 64 * 1024;
pub const MAX_PROJECT_FILE_OUTLINE_ENTRIES: usize = 512;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectFileVersion {
    pub node_id: ResourceNodeId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub name: String,
    pub version: VersionNumber,
    pub content_ref: ContentRef,
}

/// Exact authorized resource identity carried from preparation into the fenced
/// content commit. A file reference alone never grants write authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PreparedResourceFileReplacement {
    pub context: ResourceCommandContext,
    pub file: ProjectFileVersion,
}

pub trait ProjectFileVersionStore: Send + Sync {
    /// Resolve the original managed placement; never infer node identity from an Artifact ID.
    fn resolve_artifact<'a>(
        &'a self,
        _context: &'a ResourceCommandContext,
        _artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<ProjectFileVersion, ResourceStoreError>> {
        Box::pin(async { Err(ResourceStoreError::FileUnavailable) })
    }

    fn resolve_version<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        node_id: ResourceNodeId,
        version: Option<VersionNumber>,
    ) -> ApplicationFuture<'a, Result<ProjectFileVersion, ResourceStoreError>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReadProjectFile {
    pub context: ResourceCommandContext,
    pub node_id: ResourceNodeId,
    pub version: Option<VersionNumber>,
    pub offset: usize,
    pub limit: usize,
    pub include_outline: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectFileOutlineEntry {
    pub kind: String,
    pub title: String,
    pub depth: u8,
    pub start_byte: usize,
    pub end_byte: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectFileReadView {
    pub quality: crate::ExtractionQuality,
    pub file: ProjectFileVersion,
    pub content: Option<String>,
    pub content_representation: ProjectFileContentRepresentation,
    pub extraction_status: ProjectFileExtractionStatus,
    pub parser: Option<ProjectFileParserIdentity>,
    pub coordinate_space: ProjectFileReadCoordinateSpace,
    pub resolved_start_byte: usize,
    pub resolved_end_byte: usize,
    pub truncated: bool,
    pub continuation_offset: Option<usize>,
    pub outline: Vec<ProjectFileOutlineEntry>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProjectFileContentRepresentation {
    SourceText,
    ExtractedText,
    Binary,
}

impl ProjectFileContentRepresentation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceText => "source_text",
            Self::ExtractedText => "extracted_text",
            Self::Binary => "binary",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProjectFileExtractionStatus {
    NotNeeded,
    Available,
    PartiallyAvailable,
    ParserUnavailable,
    InvalidTextEncoding,
    ParserFailed,
    OcrRequired,
    OutputTooLarge,
}

impl ProjectFileExtractionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotNeeded => "not_needed",
            Self::Available => "available",
            Self::PartiallyAvailable => "partially_ready",
            Self::ParserUnavailable => "parser_unavailable",
            Self::InvalidTextEncoding => "invalid_text_encoding",
            Self::ParserFailed => "parser_failed",
            Self::OcrRequired => "ocr_required",
            Self::OutputTooLarge => "parser_output_too_large",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProjectFileReadCoordinateSpace {
    SourceUtf8,
    ExtractedUtf8,
    None,
}

impl ProjectFileReadCoordinateSpace {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceUtf8 => "source_utf8",
            Self::ExtractedUtf8 => "extracted_utf8",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectFileParserIdentity {
    pub key: String,
    pub revision: String,
}

pub trait ProjectFileReadApplicationPort: Send + Sync {
    fn read(
        &self,
        request: ReadProjectFile,
    ) -> ApplicationFuture<'_, Result<ProjectFileReadView, ProjectFileReadError>>;
}

pub struct ProjectFileReadApplication<S> {
    store: S,
    content: Arc<dyn ContentStore>,
}

impl<S> ProjectFileReadApplication<S>
where
    S: ProjectFileVersionStore,
{
    #[must_use]
    pub fn new(store: S, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn read(
        &self,
        request: ReadProjectFile,
    ) -> Result<ProjectFileReadView, ProjectFileReadError> {
        request
            .context
            .validate()
            .map_err(|_| ProjectFileReadError::IdentityMismatch)?;
        if request.limit == 0 || request.limit > MAX_PROJECT_FILE_READ_BYTES {
            return Err(ProjectFileReadError::InvalidRange);
        }
        let file = self
            .store
            .resolve_version(&request.context, request.node_id, request.version)
            .await?;
        if file.node_id != request.node_id
            || request
                .version
                .is_some_and(|version| version != file.version)
        {
            return Err(ProjectFileReadError::IdentityMismatch);
        }
        let read = read_stored_file_content(
            self.content.as_ref(),
            request.context.tenant_id,
            &file.content_ref,
            request.offset,
            request.limit,
            request.include_outline,
        )
        .await?;
        // Content IO/parser work may outlive membership or the selected head.
        // Re-resolve through the original owner before releasing any bytes.
        let current = self
            .store
            .resolve_version(&request.context, request.node_id, request.version)
            .await?;
        if current.node_id != file.node_id
            || current.artifact_id != file.artifact_id
            || current.artifact_version_id != file.artifact_version_id
            || current.version != file.version
            || current.content_ref != file.content_ref
        {
            return Err(ResourceStoreError::RevisionConflict.into());
        }
        Ok(ProjectFileReadView {
            quality: read.quality,
            file: current,
            content: read.content,
            content_representation: read.content_representation,
            extraction_status: read.extraction_status,
            parser: read.parser,
            coordinate_space: read.coordinate_space,
            resolved_start_byte: read.resolved_start_byte,
            resolved_end_byte: read.resolved_end_byte,
            truncated: read.truncated,
            continuation_offset: read.continuation_offset,
            outline: read.outline,
        })
    }
}

impl<S> ProjectFileReadApplicationPort for ProjectFileReadApplication<S>
where
    S: ProjectFileVersionStore,
{
    fn read(
        &self,
        request: ReadProjectFile,
    ) -> ApplicationFuture<'_, Result<ProjectFileReadView, ProjectFileReadError>> {
        Box::pin(ProjectFileReadApplication::read(self, request))
    }
}

/// A bounded semantic view of one already-authorized immutable content object.
/// Authorization belongs to the calling application, and bytes are verified
/// here before the existing format parser sees them.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileContentReadView {
    pub quality: crate::ExtractionQuality,
    pub content: Option<String>,
    pub content_representation: ProjectFileContentRepresentation,
    pub extraction_status: ProjectFileExtractionStatus,
    pub parser: Option<ProjectFileParserIdentity>,
    pub coordinate_space: ProjectFileReadCoordinateSpace,
    pub resolved_start_byte: usize,
    pub resolved_end_byte: usize,
    pub truncated: bool,
    pub continuation_offset: Option<usize>,
    pub outline: Vec<ProjectFileOutlineEntry>,
}

pub async fn read_file_content(
    content_ref: &ContentRef,
    bytes: Vec<u8>,
    offset: usize,
    limit: usize,
    include_outline: bool,
) -> Result<FileContentReadView, ProjectFileReadError> {
    if limit == 0 || limit > MAX_PROJECT_FILE_READ_BYTES {
        return Err(ProjectFileReadError::InvalidRange);
    }
    musteragent_runtime_content::verify_content(content_ref, &bytes)
        .map_err(|_| ProjectFileReadError::ContentIntegrity)?;
    let parsed = ParsedFileContent::parse(content_ref, bytes).await?;
    parsed.window(offset, limit, include_outline)
}

/// Reuses only exact immutable parser material from the Content owner's
/// bounded cache, after a fresh authorized read. No file-query-local cache.
pub async fn read_stored_file_content(
    store: &dyn ContentStore,
    tenant: musteragent_identity::TenantId,
    reference: &ContentRef,
    offset: usize,
    limit: usize,
    include_outline: bool,
) -> Result<FileContentReadView, ProjectFileReadError> {
    if limit == 0 || limit > MAX_PROJECT_FILE_READ_BYTES {
        return Err(ProjectFileReadError::InvalidRange);
    }
    let batch = musteragent_runtime_content::ContentBatch::load(
        store,
        tenant,
        std::slice::from_ref(reference),
    )
    .await?;
    let parsed = batch
        .decode_async(reference, "resource.file-parsed@4", |bytes| {
            ParsedFileContent::parse(reference, bytes.to_vec())
        })
        .await?;
    parsed.window(offset, limit, include_outline)
}

pub(crate) fn outline(media_type: &str, text: &str) -> Vec<ProjectFileOutlineEntry> {
    let mut entries = Vec::new();
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if entries.len() == MAX_PROJECT_FILE_OUTLINE_ENTRIES {
            break;
        }
        let body = line.trim_end_matches(['\r', '\n']);
        let trimmed = body.trim_start();
        let indentation = body.len().saturating_sub(trimmed.len());
        let symbol = if media_type.starts_with("text/markdown") {
            markdown_symbol(trimmed)
        } else {
            source_symbol(media_type, trimmed, indentation)
        };
        if let Some((kind, title, depth)) = symbol {
            entries.push(ProjectFileOutlineEntry {
                kind: kind.to_owned(),
                title: title.chars().take(256).collect(),
                depth,
                start_byte: offset,
                end_byte: offset + body.len(),
            });
        }
        offset += line.len();
    }
    entries
}

fn markdown_symbol(line: &str) -> Option<(&'static str, String, u8)> {
    let hashes = line.bytes().take_while(|byte| *byte == b'#').count();
    if !(1..=6).contains(&hashes) || line.as_bytes().get(hashes) != Some(&b' ') {
        return None;
    }
    let title = line[hashes + 1..].trim();
    (!title.is_empty()).then(|| ("heading", title.to_owned(), hashes as u8))
}

fn source_symbol(
    media_type: &str,
    line: &str,
    indentation: usize,
) -> Option<(&'static str, String, u8)> {
    let depth = u8::try_from((indentation / 2).min(31)).unwrap_or(31);
    if media_type.contains("python") {
        for (prefix, kind) in [
            ("async def ", "function"),
            ("def ", "function"),
            ("class ", "class"),
        ] {
            if let Some(rest) = line.strip_prefix(prefix) {
                return symbol_name(rest, kind, depth);
            }
        }
    }
    if media_type.contains("javascript")
        || media_type.contains("typescript")
        || media_type.contains("jsx")
        || media_type.contains("tsx")
    {
        for (prefix, kind) in [
            ("export default class ", "class"),
            ("export class ", "class"),
            ("class ", "class"),
            ("export async function ", "function"),
            ("export function ", "function"),
            ("async function ", "function"),
            ("function ", "function"),
            ("interface ", "interface"),
            ("type ", "type"),
        ] {
            if let Some(rest) = line.strip_prefix(prefix) {
                return symbol_name(rest, kind, depth);
            }
        }
    }
    if media_type.contains("html") {
        for level in 1..=6 {
            let prefix = format!("<h{level}");
            if line.starts_with(&prefix) {
                return Some(("heading", strip_markup(line), level));
            }
        }
    }
    if media_type.contains("css") && line.ends_with('{') {
        let title = line.trim_end_matches('{').trim();
        if !title.is_empty() && !title.starts_with('@') {
            return Some(("selector", title.to_owned(), depth));
        }
    }
    None
}

fn symbol_name(rest: &str, kind: &'static str, depth: u8) -> Option<(&'static str, String, u8)> {
    let end = rest
        .find(['(', '{', ':', '<', '=', ' '])
        .unwrap_or(rest.len());
    let title = rest[..end].trim();
    (!title.is_empty()).then(|| (kind, title.to_owned(), depth))
}

fn strip_markup(line: &str) -> String {
    let mut title = String::new();
    let mut inside = false;
    for character in line.chars() {
        match character {
            '<' => inside = true,
            '>' => inside = false,
            _ if !inside => title.push(character),
            _ => {}
        }
    }
    title.trim().to_owned()
}

fn next_char_boundary(text: &str, mut offset: usize) -> usize {
    while offset < text.len() && !text.is_char_boundary(offset) {
        offset += 1;
    }
    offset
}

fn previous_char_boundary(text: &str, mut offset: usize) -> usize {
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum ProjectFileReadError {
    #[error("Project File identity is invalid")]
    IdentityMismatch,
    #[error("Project File read range is invalid")]
    InvalidRange,
    #[error("Project File body is not UTF-8")]
    InvalidEncoding,
    #[error("Project File content is unavailable")]
    ContentUnavailable,
    #[error("File bytes do not match the immutable content identity")]
    ContentIntegrity,
    #[error(transparent)]
    Store(#[from] ResourceStoreError),
}

/// Shared byte-coordinate selection for source and extracted UTF-8 text.
pub fn utf8_read_window(
    text: &str,
    offset: usize,
    limit: usize,
) -> Result<(usize, usize), ProjectFileReadError> {
    if offset > text.len() || limit == 0 {
        return Err(ProjectFileReadError::InvalidRange);
    }
    let start = next_char_boundary(text, offset);
    let end = previous_char_boundary(text, start.saturating_add(limit).min(text.len()));
    if end == start && start < text.len() {
        return Err(ProjectFileReadError::InvalidRange);
    }
    Ok((start, end))
}

impl From<musteragent_runtime_content::ContentStoreError> for ProjectFileReadError {
    fn from(error: musteragent_runtime_content::ContentStoreError) -> Self {
        match error {
            musteragent_runtime_content::ContentStoreError::HashMismatch
            | musteragent_runtime_content::ContentStoreError::LengthMismatch
            | musteragent_runtime_content::ContentStoreError::IdentityConflict
            | musteragent_runtime_content::ContentStoreError::Invariant => Self::ContentIntegrity,
            _ => Self::ContentUnavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{markdown_symbol, source_symbol};

    #[test]
    fn outline_adapters_extract_stable_markdown_and_source_symbols() {
        assert_eq!(
            markdown_symbol("## Architecture"),
            Some(("heading", "Architecture".to_owned(), 2))
        );
        assert_eq!(
            source_symbol("text/x-python", "async def build(value):", 0),
            Some(("function", "build".to_owned(), 0))
        );
        assert_eq!(
            source_symbol("text/typescript", "export class ProjectTree {", 0),
            Some(("class", "ProjectTree".to_owned(), 0))
        );
    }
}
