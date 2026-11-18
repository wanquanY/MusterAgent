//! Bounded, read-only child history. No inbox acknowledgement or control authority.
use musteragent_content_ref::ContentRef;
use musteragent_identity::{Actor, AttemptId, InvocationId, RunId};
use musteragent_ledger::LedgerPayload;
use musteragent_runtime_content::{ContentStore, ContentStoreError, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

mod originals;
pub use originals::*;
mod projection;
pub use projection::public_entry;

#[derive(Debug, Clone)]
pub struct HistoryAccess {
    pub actor: Actor,
    pub current_run_id: RunId,
    /// Tool callers must prove the live attempt and pending invocation. UI readers
    /// use current participant/branch authority without acquiring execution rights.
    pub tool_fence: Option<(AttemptId, InvocationId)>,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryView {
    #[default]
    Summary,
    Timeline,
    Detail,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryInput {
    #[serde(default)]
    pub view: HistoryView,
    pub handle: Option<String>,
    pub record_id: Option<String>,
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default = "default_bytes")]
    pub max_bytes: usize,
    #[serde(default)]
    pub offset: usize,
}
fn default_limit() -> usize {
    20
}
fn default_bytes() -> usize {
    16 * 1024
}
impl HistoryInput {
    pub fn validate(&self) -> Result<(), HistoryError> {
        if !(1..=100).contains(&self.limit)
            || !(4096..=65536).contains(&self.max_bytes)
            || self.cursor.as_ref().is_some_and(|value| value.len() > 4096)
            || (self.view == HistoryView::Timeline && self.handle.is_none())
            || (self.view == HistoryView::Detail) != self.record_id.is_some()
            || (self.view != HistoryView::Detail && self.offset != 0)
        {
            return Err(HistoryError::Invalid);
        }
        if self
            .handle
            .as_ref()
            .is_some_and(|id| id.parse::<musteragent_identity::DelegationId>().is_err())
            || self
                .record_id
                .as_ref()
                .is_some_and(|id| id.parse::<musteragent_identity::LedgerRecordId>().is_err())
        {
            return Err(HistoryError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCursor {
    pub schema: String,
    pub tenant: String,
    pub conversation: String,
    pub current_run: String,
    pub branch: Option<String>,
    pub head: u64,
    pub view: HistoryView,
    pub handle: Option<String>,
    pub after: u64,
}
impl HistoryCursor {
    pub fn decode(input: &HistoryInput) -> Result<Option<Self>, HistoryError> {
        input
            .cursor
            .as_ref()
            .map(|value| serde_json::from_str(value).map_err(|_| HistoryError::Invalid))
            .transpose()
    }
    pub fn validate_against(&self, scope: &Self) -> Result<(), HistoryError> {
        if self.schema != "musteragent.delegation-history-cursor.v1"
            || self.tenant != scope.tenant
            || self.conversation != scope.conversation
            || self.current_run != scope.current_run
            || self.branch != scope.branch
            || self.view != scope.view
            || self.handle != scope.handle
            || self.head > scope.head
            || self.after > self.head
        {
            return Err(HistoryError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub result_invocation: Option<musteragent_identity::InvocationId>,
    pub record_id: String,
    pub sequence: u64,
    pub kind: &'static str,
    pub label: String,
    pub handle: String,
    pub source_run_id: Option<String>,
    pub content: Option<ContentRef>,
    pub content_available: bool,
}
#[derive(Debug, Clone)]
pub struct HistorySlice {
    pub cursor: HistoryCursor,
    pub entries: Vec<HistoryEntry>,
    pub has_more: bool,
}
/// Implementations must freshly authorize membership, current Run, source parent,
/// active branch and each selected record in one consistent storage transaction.
pub trait DelegationHistorySource: Send + Sync {
    fn originals<'a>(
        &'a self,
        _access: &'a HistoryAccess,
        _content: Arc<dyn ContentStore>,
        _page: u64,
    ) -> ApplicationFuture<'a, Result<OriginalPage, HistoryError>> {
        Box::pin(async { Err(HistoryError::Unavailable) })
    }
    fn read<'a>(
        &'a self,
        access: &'a HistoryAccess,
        input: &'a HistoryInput,
    ) -> ApplicationFuture<'a, Result<HistorySlice, HistoryError>>;
}
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum HistoryError {
    #[error("Invalid history page or stale cursor; start a new query")]
    Invalid,
    #[error(
        "History record is absent or not visible under current authority; use a Runtime source record_id, not an artifact, content, or generation record ID"
    )]
    Denied,
    #[error("History storage is unavailable")]
    Unavailable,
    #[error("History evidence is inconsistent")]
    Corrupt,
}

#[derive(Debug, Serialize)]
pub struct HistoryPage {
    pub view: HistoryView,
    pub revision: u64,
    pub items: Vec<HistoryItem>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct HistoryItem {
    pub record_id: String,
    pub sequence: u64,
    pub kind: String,
    pub label: String,
    pub handle: String,
    pub source_run_id: Option<String>,
    pub content: Option<HistoryContent>,
}
#[derive(Debug, Serialize)]
pub struct HistoryContent {
    pub content_id: String,
    pub sha256: String,
    pub byte_length: u64,
    pub availability: &'static str,
    pub text: Option<String>,
    pub offset: usize,
    pub next_offset: Option<usize>,
    pub complete: bool,
}

pub struct DelegationHistoryReader {
    source: Arc<dyn DelegationHistorySource>,
    content: Arc<dyn ContentStore>,
}
impl DelegationHistoryReader {
    pub fn new(source: Arc<dyn DelegationHistorySource>, content: Arc<dyn ContentStore>) -> Self {
        Self { source, content }
    }
    pub async fn read(
        &self,
        access: &HistoryAccess,
        input: &HistoryInput,
    ) -> Result<HistoryPage, HistoryError> {
        input.validate()?;
        let slice = self.source.read(access, input).await?;
        let mut cursor = slice.cursor.clone();
        let mut page = HistoryPage {
            view: input.view,
            revision: cursor.head,
            items: Vec::new(),
            has_more: slice.has_more,
            next_cursor: None,
        };
        for entry in &slice.entries {
            let mut item = HistoryItem {
                record_id: entry.record_id.clone(),
                sequence: entry.sequence,
                kind: entry.kind.to_owned(),
                label: entry.label.clone(),
                handle: entry.handle.clone(),
                source_run_id: entry.source_run_id.clone(),
                content: entry.content.as_ref().map(|reference| HistoryContent {
                    content_id: reference.content_id.to_string(),
                    sha256: reference.sha256.to_string(),
                    byte_length: reference.byte_length,
                    availability: "reference",
                    text: None,
                    offset: 0,
                    next_offset: None,
                    complete: false,
                }),
            };
            if input.view == HistoryView::Detail {
                let reference = entry.content.as_ref().ok_or(HistoryError::Invalid)?;
                let output = item.content.as_mut().ok_or(HistoryError::Corrupt)?;
                let bytes = if entry.content_available {
                    self.content.read(access.actor.tenant_id, reference).await
                } else {
                    Err(ContentStoreError::NotFound)
                };
                match bytes {
                    Ok(bytes) => {
                        verify_content(reference, &bytes).map_err(|_| HistoryError::Corrupt)?;
                        let text =
                            std::str::from_utf8(&bytes).map_err(|_| HistoryError::Invalid)?;
                        if input.offset > text.len() || !text.is_char_boundary(input.offset) {
                            return Err(HistoryError::Invalid);
                        }
                        let mut end = text
                            .len()
                            .min(input.offset.saturating_add(input.max_bytes / 6));
                        while !text.is_char_boundary(end) {
                            end -= 1;
                        }
                        output.availability = "available";
                        output.text = Some(text[input.offset..end].to_owned());
                        output.offset = input.offset;
                        output.next_offset = (end < text.len()).then_some(end);
                        output.complete = input.offset == 0 && end == text.len();
                    }
                    Err(ContentStoreError::NotFound) => {
                        output.availability = "deleted_or_unavailable"
                    }
                    Err(ContentStoreError::Unauthorized) => return Err(HistoryError::Denied),
                    Err(ContentStoreError::Unavailable) => return Err(HistoryError::Unavailable),
                    Err(_) => return Err(HistoryError::Corrupt),
                }
            }
            page.items.push(item);
            // Reserve space for the fixed-size scope cursor and page envelope.
            if serde_json::to_vec(&page)
                .map_err(|_| HistoryError::Corrupt)?
                .len()
                + 1024
                > input.max_bytes
            {
                page.items.pop();
                if page.items.is_empty() {
                    return Err(HistoryError::Invalid);
                }
                page.has_more = true;
                break;
            }
            cursor.after = entry.sequence;
        }
        if page.items.len() == slice.entries.len() {
            cursor.after = slice.cursor.after;
        }
        page.next_cursor = page
            .has_more
            .then(|| serde_json::to_string(&cursor))
            .transpose()
            .map_err(|_| HistoryError::Corrupt)?;
        // Hydration may have awaited storage. Recheck current authority before publishing.
        let mut recheck = input.clone();
        let mut proof_cursor = slice.cursor.clone();
        proof_cursor.after = HistoryCursor::decode(input)?.map_or(0, |cursor| cursor.after);
        recheck.cursor =
            Some(serde_json::to_string(&proof_cursor).map_err(|_| HistoryError::Corrupt)?);
        let verified = self.source.read(access, &recheck).await?;
        for item in &page.items {
            let original = slice
                .entries
                .iter()
                .find(|entry| entry.record_id == item.record_id)
                .ok_or(HistoryError::Corrupt)?;
            if !verified.entries.iter().any(|entry| {
                entry.record_id == original.record_id
                    && entry.sequence == original.sequence
                    && entry.handle == original.handle
                    && entry.source_run_id == original.source_run_id
                    && entry.content == original.content
                    && entry.content_available == original.content_available
            }) {
                return Err(HistoryError::Denied);
            }
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests;
