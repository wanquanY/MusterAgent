//! One format decoder for Resource and unified file readers. Derived material
//! is keyed by exact ContentRef and parser contract revision in the existing
//! Content cache; authorization and retention are checked before cache lookup.
use crate::reading::*;
use musteragent_artifact::{FileRepresentation, file_capability};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::RetainedContentMaterial;

pub(crate) struct ParsedFileContent {
    text: Option<String>,
    quality: crate::ExtractionQuality,
    representation: ProjectFileContentRepresentation,
    status: ProjectFileExtractionStatus,
    parser: Option<ProjectFileParserIdentity>,
    outline_media_type: String,
}
impl RetainedContentMaterial for ParsedFileContent {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.text.as_ref().map_or(0, String::capacity)
            + self.outline_media_type.capacity()
            + self.quality.retained_bytes()
            + self
                .parser
                .as_ref()
                .map_or(0, |p| p.key.capacity() + p.revision.capacity())
    }
}
impl ParsedFileContent {
    pub(crate) async fn parse(
        reference: &ContentRef,
        bytes: Vec<u8>,
    ) -> Result<Self, ProjectFileReadError> {
        musteragent_runtime_content::verify_content(reference, &bytes)
            .map_err(|_| ProjectFileReadError::ContentIntegrity)?;
        if file_capability(&reference.media_type).representation == FileRepresentation::Text {
            return Ok(Self {
                quality: Default::default(),
                text: Some(
                    String::from_utf8(bytes).map_err(|_| ProjectFileReadError::InvalidEncoding)?,
                ),
                representation: ProjectFileContentRepresentation::SourceText,
                status: ProjectFileExtractionStatus::NotNeeded,
                parser: None,
                outline_media_type: reference.media_type.as_str().into(),
            });
        }
        match crate::search_indexing::extract_search_text(reference, bytes).await {
            Ok(parsed) => Ok(Self {
                text: Some(parsed.text),
                representation: ProjectFileContentRepresentation::ExtractedText,
                status: if parsed.quality.issues.is_empty() {
                    ProjectFileExtractionStatus::Available
                } else {
                    ProjectFileExtractionStatus::PartiallyAvailable
                },
                quality: parsed.quality,
                parser: Some(ProjectFileParserIdentity {
                    key: parsed.parser_key.into(),
                    revision: parsed.parser_revision.into(),
                }),
                outline_media_type: parsed.media_type.into(),
            }),
            Err(error) => Ok(Self {
                quality: Default::default(),
                text: None,
                representation: ProjectFileContentRepresentation::Binary,
                status: match error.as_code() {
                    "parser_unavailable" => ProjectFileExtractionStatus::ParserUnavailable,
                    "invalid_text_encoding" => ProjectFileExtractionStatus::InvalidTextEncoding,
                    "parser_failed" => ProjectFileExtractionStatus::ParserFailed,
                    "ocr_required" => ProjectFileExtractionStatus::OcrRequired,
                    "parser_output_too_large" => ProjectFileExtractionStatus::OutputTooLarge,
                    _ => ProjectFileExtractionStatus::ParserFailed,
                },
                parser: None,
                outline_media_type: String::new(),
            }),
        }
    }
    pub(crate) fn window(
        &self,
        offset: usize,
        limit: usize,
        include_outline: bool,
    ) -> Result<FileContentReadView, ProjectFileReadError> {
        let (start, end, continuation_offset) = match &self.text {
            Some(text) => {
                let (start, end) = utf8_read_window(text, offset, limit)?;
                (start, end, (end < text.len()).then_some(end))
            }
            None => (0, 0, None),
        };
        Ok(FileContentReadView {
            quality: self.quality.clone(),
            content: self.text.as_ref().map(|text| text[start..end].into()),
            content_representation: self.representation,
            extraction_status: self.status,
            parser: self.parser.clone(),
            coordinate_space: match self.representation {
                ProjectFileContentRepresentation::SourceText => {
                    ProjectFileReadCoordinateSpace::SourceUtf8
                }
                ProjectFileContentRepresentation::ExtractedText => {
                    ProjectFileReadCoordinateSpace::ExtractedUtf8
                }
                ProjectFileContentRepresentation::Binary => ProjectFileReadCoordinateSpace::None,
            },
            resolved_start_byte: start,
            resolved_end_byte: end,
            truncated: continuation_offset.is_some(),
            continuation_offset,
            outline: if include_outline {
                self.text.as_ref().map_or_else(Vec::new, |text| {
                    super::reading::outline(&self.outline_media_type, text)
                })
            } else {
                vec![]
            },
        })
    }
}
