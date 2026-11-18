use std::sync::Arc;

use musteragent_artifact::{FileRepresentation, file_capability};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ResourceNodeId, TenantId, UtcTimestamp, VersionNumber,
};
use musteragent_resource::ResourceScope;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

pub const RESOURCE_SEARCH_PARSER_KEY: &str = "resource.direct-text";
pub const RESOURCE_SEARCH_PARSER_REVISION: &str = "1";
pub const MAX_RESOURCE_SEARCH_CHUNK_BYTES: usize = 4 * 1024;
pub(crate) const MAX_EXTRACTED_TEXT_BYTES: usize = 32 * 1024 * 1024;
const EXTRACTED_MARKDOWN_MEDIA_TYPE: &str = "text/markdown";
const EXTRACTED_PLAIN_TEXT_MEDIA_TYPE: &str = "text/plain";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceSearchBackfillSource {
    pub tenant_id: TenantId,
    pub scope: ResourceScope,
    pub node_id: ResourceNodeId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub file_version: VersionNumber,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedResourceSearchBackfill {
    pub source: ResourceSearchBackfillSource,
    pub worker_id: String,
    pub attempt_count: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceSearchChunkDocument {
    pub chunk_id: uuid::Uuid,
    pub chunk_ordinal: u32,
    pub text: String,
    pub start_byte: u64,
    pub end_byte: u64,
    pub line_start: u32,
    pub line_end: u32,
    pub section_path: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceSearchManifestDocument {
    pub manifest_id: uuid::Uuid,
    pub source: ResourceSearchBackfillSource,
    pub parser_key: String,
    pub parser_revision: String,
    pub chunks: Vec<ResourceSearchChunkDocument>,
}

pub trait ResourceSearchBackfillStore: Send + Sync {
    fn claim(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<
        '_,
        Result<Option<ClaimedResourceSearchBackfill>, ResourceSearchIndexError>,
    >;

    fn complete(
        &self,
        claim: ClaimedResourceSearchBackfill,
        document: ResourceSearchManifestDocument,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ResourceSearchIndexError>>;

    fn mark_terminal(
        &self,
        claim: ClaimedResourceSearchBackfill,
        error_code: &'static str,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ResourceSearchIndexError>>;

    fn fail(
        &self,
        claim: ClaimedResourceSearchBackfill,
        error_code: &'static str,
        retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ResourceSearchIndexError>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResourceSearchBackfillRun {
    Idle,
    Indexed {
        artifact_version_id: ArtifactVersionId,
        chunk_count: usize,
    },
    Unsupported {
        artifact_version_id: ArtifactVersionId,
    },
    Deferred {
        artifact_version_id: ArtifactVersionId,
        attempt_count: u16,
    },
}

pub struct ResourceSearchBackfiller<S> {
    store: S,
    content: Arc<dyn ContentStore>,
}

impl<S> ResourceSearchBackfiller<S>
where
    S: ResourceSearchBackfillStore,
{
    #[must_use]
    pub fn new(store: S, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn run_once(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> Result<ResourceSearchBackfillRun, ResourceSearchIndexError> {
        let Some(claim) = self.store.claim(worker_id, now, lease_expires_at).await? else {
            return Ok(ResourceSearchBackfillRun::Idle);
        };
        let artifact_version_id = claim.source.artifact_version_id;
        let bytes = match self
            .content
            .read(claim.source.tenant_id, &claim.source.content_ref)
            .await
        {
            Ok(bytes) => bytes,
            Err(_) => {
                let attempt_count = claim.attempt_count;
                self.store
                    .fail(claim, "content_unavailable", retry_at)
                    .await?;
                return Ok(ResourceSearchBackfillRun::Deferred {
                    artifact_version_id,
                    attempt_count,
                });
            }
        };
        let parsed = match extract_search_text(&claim.source.content_ref, bytes).await {
            Ok(parsed) => parsed,
            Err(error) => {
                self.store
                    .mark_terminal(claim, error.as_code(), now)
                    .await?;
                return Ok(ResourceSearchBackfillRun::Unsupported {
                    artifact_version_id,
                });
            }
        };
        let chunks = chunk_text(artifact_version_id, &parsed.text);
        let chunk_count = chunks.len();
        let document = ResourceSearchManifestDocument {
            manifest_id: artifact_version_id.into_uuid(),
            source: claim.source.clone(),
            parser_key: parsed.parser_key.to_owned(),
            parser_revision: parsed.parser_revision.to_owned(),
            chunks,
        };
        self.store.complete(claim, document, now).await?;
        Ok(ResourceSearchBackfillRun::Indexed {
            artifact_version_id,
            chunk_count,
        })
    }
}

pub(crate) struct ParsedSearchText {
    pub(crate) quality: crate::ExtractionQuality,
    pub(crate) text: String,
    pub(crate) parser_key: &'static str,
    pub(crate) parser_revision: &'static str,
    pub(crate) media_type: &'static str,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum SearchExtractionError {
    ParserUnavailable,
    InvalidTextEncoding,
    ParserFailed,
    OcrRequired,
    OutputTooLarge,
}

impl SearchExtractionError {
    pub(crate) const fn as_code(self) -> &'static str {
        match self {
            Self::ParserUnavailable => "parser_unavailable",
            Self::InvalidTextEncoding => "invalid_text_encoding",
            Self::ParserFailed => "parser_failed",
            Self::OcrRequired => "ocr_required",
            Self::OutputTooLarge => "parser_output_too_large",
        }
    }
}

pub(crate) async fn extract_search_text(
    content_ref: &ContentRef,
    bytes: Vec<u8>,
) -> Result<ParsedSearchText, SearchExtractionError> {
    if file_capability(&content_ref.media_type).representation == FileRepresentation::Text {
        return bounded_extracted_text(
            String::from_utf8(bytes).map_err(|_| SearchExtractionError::InvalidTextEncoding)?,
        )
        .map(|text| ParsedSearchText {
            quality: Default::default(),
            text,
            parser_key: RESOURCE_SEARCH_PARSER_KEY,
            parser_revision: RESOURCE_SEARCH_PARSER_REVISION,
            media_type: EXTRACTED_PLAIN_TEXT_MEDIA_TYPE,
        });
    }
    let material = crate::parser_process::extract(content_ref.media_type.as_str(), bytes).await?;
    let (parser_key, parser_revision, media_type) =
        parser_identity(content_ref.media_type.as_str())?;
    Ok(ParsedSearchText {
        text: material.text,
        quality: material.quality,
        parser_key,
        parser_revision,
        media_type,
    })
}

pub(crate) fn parser_identity(
    media: &str,
) -> Result<(&'static str, &'static str, &'static str), SearchExtractionError> {
    match media.split(';').next().unwrap_or("").trim() {
        "application/pdf" => Ok(("resource.pdf-text", "1", EXTRACTED_MARKDOWN_MEDIA_TYPE)),
        _ => Err(SearchExtractionError::ParserUnavailable),
    }
}

pub(crate) fn extract_document_sync(
    media_type: &str,
    bytes: Vec<u8>,
) -> Result<ParsedSearchText, SearchExtractionError> {
    let essence = media_type
        .split(';')
        .next()
        .ok_or(SearchExtractionError::ParserUnavailable)?
        .trim()
        .to_ascii_lowercase();
    if essence == "application/pdf" {
        return crate::pdf_reading::extract(bytes);
    }
    Err(SearchExtractionError::ParserUnavailable)
}

fn bounded_extracted_text(text: String) -> Result<String, SearchExtractionError> {
    if text.trim().is_empty() {
        return Err(SearchExtractionError::OcrRequired);
    }
    if text.len() > MAX_EXTRACTED_TEXT_BYTES {
        return Err(SearchExtractionError::OutputTooLarge);
    }
    Ok(text)
}

#[cfg(test)]
fn extract_pdf_text(bytes: Vec<u8>) -> Result<String, SearchExtractionError> {
    crate::pdf_reading::extract(bytes).map(|parsed| parsed.text)
}
#[cfg(test)]
use crate::pdf_reading::map_error as map_pdf_error;

fn chunk_text(
    artifact_version_id: ArtifactVersionId,
    text: &str,
) -> Vec<ResourceSearchChunkDocument> {
    if text.is_empty() {
        return Vec::new();
    }
    let lines = text.split_inclusive('\n').collect::<Vec<_>>();
    let mut chunks = Vec::new();
    let mut chunk_start = 0usize;
    let mut chunk_text = String::new();
    let mut chunk_line_start = 1u32;
    let mut current_offset = 0usize;
    let mut current_line = 1u32;
    let mut section_path = Vec::<String>::new();
    let mut chunk_sections = Vec::<String>::new();

    for line in lines {
        if let Some((depth, title)) = markdown_heading(line) {
            section_path.truncate(depth.saturating_sub(1));
            section_path.push(title);
        }
        if !chunk_text.is_empty()
            && chunk_text.len().saturating_add(line.len()) > MAX_RESOURCE_SEARCH_CHUNK_BYTES
        {
            chunks.push(chunk_document(
                artifact_version_id,
                chunks.len(),
                chunk_start,
                current_offset,
                chunk_line_start,
                current_line.saturating_sub(1),
                std::mem::take(&mut chunk_text),
                std::mem::take(&mut chunk_sections),
            ));
            chunk_start = current_offset;
            chunk_line_start = current_line;
        }
        if chunk_text.is_empty() {
            chunk_sections = section_path.clone();
        }
        append_bounded_line(
            artifact_version_id,
            line,
            &mut chunks,
            &mut chunk_text,
            &mut chunk_start,
            &mut current_offset,
            &mut chunk_line_start,
            current_line,
            &chunk_sections,
        );
        current_line = current_line.saturating_add(1);
    }
    if !chunk_text.is_empty() {
        chunks.push(chunk_document(
            artifact_version_id,
            chunks.len(),
            chunk_start,
            current_offset,
            chunk_line_start,
            current_line.saturating_sub(1),
            chunk_text,
            chunk_sections,
        ));
    }
    chunks
}

#[allow(clippy::too_many_arguments)]
fn append_bounded_line(
    artifact_version_id: ArtifactVersionId,
    line: &str,
    chunks: &mut Vec<ResourceSearchChunkDocument>,
    chunk_text: &mut String,
    chunk_start: &mut usize,
    current_offset: &mut usize,
    chunk_line_start: &mut u32,
    current_line: u32,
    section_path: &[String],
) {
    let mut remaining = line;
    while !remaining.is_empty() {
        let capacity = MAX_RESOURCE_SEARCH_CHUNK_BYTES.saturating_sub(chunk_text.len());
        let take = previous_char_boundary(remaining, capacity.min(remaining.len()));
        if take == 0 && !chunk_text.is_empty() {
            chunks.push(chunk_document(
                artifact_version_id,
                chunks.len(),
                *chunk_start,
                *current_offset,
                *chunk_line_start,
                current_line,
                std::mem::take(chunk_text),
                section_path.to_vec(),
            ));
            *chunk_start = *current_offset;
            *chunk_line_start = current_line;
            continue;
        }
        let take = if take == 0 { remaining.len() } else { take };
        chunk_text.push_str(&remaining[..take]);
        *current_offset = current_offset.saturating_add(take);
        remaining = &remaining[take..];
        if !remaining.is_empty() {
            chunks.push(chunk_document(
                artifact_version_id,
                chunks.len(),
                *chunk_start,
                *current_offset,
                *chunk_line_start,
                current_line,
                std::mem::take(chunk_text),
                section_path.to_vec(),
            ));
            *chunk_start = *current_offset;
            *chunk_line_start = current_line;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn chunk_document(
    artifact_version_id: ArtifactVersionId,
    ordinal: usize,
    start_byte: usize,
    end_byte: usize,
    line_start: u32,
    line_end: u32,
    text: String,
    section_path: Vec<String>,
) -> ResourceSearchChunkDocument {
    let mut identity = artifact_version_id.into_uuid().as_bytes().to_vec();
    identity.extend_from_slice(&u64::try_from(ordinal).unwrap_or(u64::MAX).to_be_bytes());
    let digest = musteragent_content_ref::Sha256::digest(identity).into_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ResourceSearchChunkDocument {
        chunk_id: uuid::Uuid::from_bytes(bytes),
        chunk_ordinal: u32::try_from(ordinal).unwrap_or(u32::MAX),
        text,
        start_byte: u64::try_from(start_byte).unwrap_or(u64::MAX),
        end_byte: u64::try_from(end_byte).unwrap_or(u64::MAX),
        line_start,
        line_end,
        section_path,
    }
}

fn markdown_heading(line: &str) -> Option<(usize, String)> {
    let trimmed = line.trim();
    let depth = trimmed.bytes().take_while(|byte| *byte == b'#').count();
    if !(1..=6).contains(&depth) || trimmed.as_bytes().get(depth) != Some(&b' ') {
        return None;
    }
    let title = trimmed[depth + 1..].trim();
    (!title.is_empty()).then(|| (depth, title.to_owned()))
}

fn previous_char_boundary(text: &str, mut offset: usize) -> usize {
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceSearchIndexError {
    #[error("Resource search index dependency is unavailable")]
    Unavailable,
    #[error("Resource search index lease is stale")]
    StaleLease,
    #[error("Resource search index state is corrupt")]
    Corrupt,
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_RESOURCE_SEARCH_CHUNK_BYTES, SearchExtractionError, chunk_text, extract_pdf_text,
        map_pdf_error,
    };
    use musteragent_identity::ArtifactVersionId;

    #[test]
    fn chunker_is_bounded_and_preserves_utf8_content() {
        let source = format!("# 标题\n{}\n", "正文🙂".repeat(2_000));
        let chunks = chunk_text(ArtifactVersionId::generate(), &source);
        assert!(chunks.len() > 1);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.text.len() <= MAX_RESOURCE_SEARCH_CHUNK_BYTES)
        );
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<String>(),
            source
        );
        assert!(chunks.iter().all(|chunk| chunk.section_path == ["标题"]));
    }

    #[test]
    fn pdf_parser_extracts_page_text_with_stable_page_sections() {
        let stream = "BT /F1 12 Tf 72 720 Td (Searchable project source) Tj ET";
        let objects = [
            "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_owned(),
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
            format!("<< /Length {} >>\nstream\n{stream}\nendstream", stream.len()),
        ];
        let mut pdf = b"%PDF-1.4\n".to_vec();
        let mut offsets = Vec::new();
        for (index, object) in objects.iter().enumerate() {
            offsets.push(pdf.len());
            pdf.extend_from_slice(format!("{} 0 obj\n{}\nendobj\n", index + 1, object).as_bytes());
        }
        let xref = pdf.len();
        pdf.extend_from_slice(
            format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
        );
        for offset in offsets {
            pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
        }
        pdf.extend_from_slice(
            format!(
                "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
                objects.len() + 1
            )
            .as_bytes(),
        );

        let text = extract_pdf_text(pdf).expect("extract PDF text");
        assert!(text.starts_with("# Page 1\n"));
        assert!(text.contains("Searchable project source"));
    }

    #[test]
    fn pdf_decompression_limit_is_reported_as_output_too_large() {
        assert_eq!(
            map_pdf_error(lopdf::Error::Decompress(
                lopdf::DecompressError::MemoryLimitExceeded { limit: 1024 },
            )),
            SearchExtractionError::OutputTooLarge
        );
    }
}
