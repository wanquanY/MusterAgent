//! Source-owned document material, selected by the shared Runtime reader.
use crate::{FileQueryContext, FileQueryError};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::ExtractionQuality;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DocumentRepresentation {
    pub state: String,
    pub source_sha256: String,
    pub text: Option<String>,
    pub representation_revision: Option<String>,
    pub parser_revision: Option<String>,
    pub resolved_start: usize,
    pub resolved_end: usize,
    pub next_offset: Option<usize>,
    pub quality: ExtractionQuality,
}

pub trait UploadedDocumentReader: Send + Sync {
    fn read<'a>(
        &'a self,
        context: &'a FileQueryContext,
        source: &'a ContentRef,
        offset: usize,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DocumentRepresentation, FileQueryError>>;
}

impl DocumentRepresentation {
    pub(crate) fn validate(
        &self,
        source: &ContentRef,
        offset: usize,
        limit: usize,
    ) -> Result<(), FileQueryError> {
        if self.source_sha256 != source.sha256.to_string() {
            return Err(FileQueryError::Integrity);
        }
        let readable = matches!(self.state.as_str(), "ready" | "partially_ready");
        if readable {
            let text = self.text.as_ref().ok_or(FileQueryError::Integrity)?;
            if text.len() > limit
                || self.resolved_start != offset
                || self.resolved_end.checked_sub(self.resolved_start) != Some(text.len())
                || self
                    .next_offset
                    .is_some_and(|next| next != self.resolved_end || next <= offset)
                || self
                    .representation_revision
                    .as_ref()
                    .is_none_or(|revision| revision.is_empty())
                || self
                    .parser_revision
                    .as_ref()
                    .is_none_or(|revision| revision.is_empty())
            {
                return Err(FileQueryError::Integrity);
            }
        } else if !matches!(
            self.state.as_str(),
            "preparing" | "failed" | "unsupported" | "unavailable" | "not_owned"
        ) || self.text.is_some()
            || self.next_offset.is_some()
        {
            return Err(FileQueryError::Integrity);
        }
        let pages = &self.quality.pages_with_text;
        if pages
            .iter()
            .any(|page| *page == 0 || self.quality.pages_total.is_some_and(|total| *page > total))
            || pages.windows(2).any(|pair| pair[0] >= pair[1])
            || self.quality.issues.iter().any(|issue| {
                issue.code.is_empty()
                    || issue.occurrences == 0
                    || issue.page.is_some_and(|page| {
                        page == 0 || self.quality.pages_total.is_some_and(|total| page > total)
                    })
            })
        {
            return Err(FileQueryError::Integrity);
        }
        Ok(())
    }
}
