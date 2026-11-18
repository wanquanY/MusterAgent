//! Bounded exact-text search over the same versioned readers. Each continuation
//! pins the source revision, query, traversal and authority. Recursive search
//! is a distinct provider query, never inferred from child pagination.
use crate::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use musteragent_content_ref::Sha256;
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileSearchTraversal {
    #[default]
    Children,
    Subtree,
}
const CATALOG_PAGE: usize = 8;
const READ_BUDGET: usize = 16;
// Directory-only pages are an internal traversal detail. Bound metadata work
// separately from content reads so a large tree still yields a continuation.
const CATALOG_BUDGET: usize = 64;
const SEARCH_BYTES: usize = 64 * 1024;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFiles {
    #[serde(default)]
    pub traversal: FileSearchTraversal,
    pub scope: Option<FileScope>,
    pub parent: Option<FileReference>,
    pub file_ref: Option<FileReference>,
    pub query: String,
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}
const fn default_limit() -> usize {
    20
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct FileSearchHit {
    pub file_ref: FileReference,
    pub name: String,
    pub representation: FileRepresentation,
    pub parser_revision: Option<String>,
    pub start_byte: usize,
    pub end_byte: usize,
    pub excerpt: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct FileSearchGap {
    pub file_ref: FileReference,
    pub reason: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct FileSearchPage {
    pub hits: Vec<FileSearchHit>,
    pub unsearched: Vec<FileSearchGap>,
    pub next_cursor: Option<String>,
    pub revision: String,
    pub coverage: FileCoverage,
    pub scanned_bytes: usize,
    /// True only at exhaustion with no skipped content in any preceding page.
    pub complete: bool,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Position {
    version: u8,
    query: String,
    revision: String,
    catalog_cursor: Option<String>,
    index: usize,
    offset: usize,
    gaps: bool,
}
impl FileQueryApplication {
    pub async fn search(
        &self,
        context: &FileQueryContext,
        query: SearchFiles,
    ) -> Result<FileSearchPage, FileQueryError> {
        self.search_observed(
            context,
            query,
            &crate::activity::QueryActivity::new(None),
            crate::source_contract::FileSourceContract::Product,
        )
        .await
    }
    pub(crate) async fn search_observed(
        &self,
        context: &FileQueryContext,
        query: SearchFiles,
        activity: &crate::activity::QueryActivity<'_>,
        contract: crate::source_contract::FileSourceContract,
    ) -> Result<FileSearchPage, FileQueryError> {
        activity.check_stop()?;
        if query.query.trim().is_empty()
            || query.query.len() > 512
            || query.limit == 0
            || query.limit > 100
            || query.scope.is_some() == query.file_ref.is_some()
            || (query.file_ref.is_some() && query.parent.is_some())
            || (query.traversal == FileSearchTraversal::Subtree
                && (!matches!(
                    query.scope,
                    Some(FileScope::Project | FileScope::Work | FileScope::OwnedAgent)
                ) || query.file_ref.is_some()))
        {
            return Err(FileQueryError::InvalidQuery);
        }
        let scope = query
            .scope
            .or_else(|| query.file_ref.as_ref().map(|r| r.scope))
            .ok_or(FileQueryError::InvalidQuery)?;
        let list = ListFiles {
            produced_by: None,
            scope,
            parent: query.parent.clone(),
            cursor: None,
            limit: CATALOG_PAGE,
            name_query: None,
        };
        list.validate(context)?;
        for reference in query.parent.iter().chain(query.file_ref.iter()) {
            contract.reference(reference)?;
        }
        let fingerprint = Sha256::digest(
            serde_json::to_vec(&(
                crate::cursor::fingerprint(context, &list)?,
                &query.file_ref,
                &query.query,
                query.traversal,
            ))
            .map_err(|_| FileQueryError::Integrity)?,
        )
        .to_string();
        let mut position = match &query.cursor {
            Some(cursor) => {
                if cursor.len() > 8192 {
                    return Err(FileQueryError::InvalidQuery);
                }
                let saved: Position = serde_json::from_slice(
                    &URL_SAFE_NO_PAD
                        .decode(cursor)
                        .map_err(|_| FileQueryError::InvalidQuery)?,
                )
                .map_err(|_| FileQueryError::InvalidQuery)?;
                if saved.version != 1 || saved.query != fingerprint {
                    return Err(FileQueryError::InvalidQuery);
                }
                saved
            }
            None => Position {
                version: 1,
                query: fingerprint,
                revision: String::new(),
                catalog_cursor: None,
                index: 0,
                offset: 0,
                gaps: false,
            },
        };
        let mut hits = Vec::new();
        let mut gaps = Vec::new();
        let mut scanned_bytes = 0;
        let mut reads = 0;
        let mut catalog_pages = 0;
        let page = loop {
            activity.check_stop()?;
            let page = if let Some(reference) = &query.file_ref {
                if position.catalog_cursor.is_some() || position.index != 0 {
                    return Err(FileQueryError::InvalidQuery);
                }
                FilePage {
                    retraction_status: None,
                    entries: vec![self.inspect(context, reference).await?],
                    next_cursor: None,
                    revision: reference.revision.clone(),
                    coverage: match scope {
                        FileScope::OwnedAgent => {
                            if matches!(reference.source, crate::FileSource::DialogueInput { .. }) {
                                FileCoverage::OwnedAgentFiles
                            } else {
                                FileCoverage::OwnedAgentResources
                            }
                        }
                        FileScope::Project => FileCoverage::ProjectChildren,
                        FileScope::Work => FileCoverage::WorkChildren,
                        FileScope::Conversation if context.content_allowlist.is_some() => {
                            FileCoverage::RestrictedConversation
                        }
                        FileScope::Conversation => FileCoverage::ProjectedConversation,
                    },
                }
            } else {
                self.search_catalog(
                    context,
                    query.traversal,
                    ListFiles {
                        cursor: position.catalog_cursor.clone(),
                        ..list.clone()
                    },
                )
                .await?
            };
            activity.completed()?;
            // Check every row, including directories and unavailable entries,
            // before reading any content from this page. Unsupported identities
            // cannot be silently skipped and reported as a complete search.
            for entry in &page.entries {
                contract.reference(&entry.file_ref)?;
            }
            if !position.revision.is_empty() && position.revision != page.revision {
                return Err(FileQueryError::Changed);
            }
            position.revision = page.revision.clone();
            if position.index > page.entries.len() {
                return Err(FileQueryError::InvalidQuery);
            }
            while position.index < page.entries.len()
                && reads < READ_BUDGET
                && hits.len() < query.limit
                && gaps.len() < 128
            {
                let entry = &page.entries[position.index];
                if entry.is_directory {
                    if query.file_ref.is_some() {
                        return Err(FileQueryError::DirectorySearchTarget);
                    }
                    position.index += 1;
                    position.offset = 0;
                    continue;
                }
                // Draft/archived lifecycle does not remove a committed source
                // version. Only unavailable sources lack readable content.
                if entry.availability == FileAvailability::Unavailable {
                    gaps.push(FileSearchGap {
                        file_ref: entry.file_ref.clone(),
                        reason: "source_not_available".into(),
                    });
                    position.index += 1;
                    position.offset = 0;
                    continue;
                }
                let read = self
                    .read(
                        context,
                        ReadFile {
                            file_ref: entry.file_ref.clone(),
                            offset: position.offset,
                            limit: SEARCH_BYTES,
                        },
                    )
                    .await?;
                activity.completed()?;
                reads += 1;
                let Some(text) = read.view.text else {
                    gaps.push(FileSearchGap {
                        file_ref: entry.file_ref.clone(),
                        reason: read.view.extraction_status,
                    });
                    position.index += 1;
                    position.offset = 0;
                    continue;
                };
                if read.view.extraction_status == "partially_ready"
                    && !gaps.iter().any(|gap| gap.file_ref == entry.file_ref)
                {
                    gaps.push(FileSearchGap {
                        file_ref: entry.file_ref.clone(),
                        reason: "partially_ready".into(),
                    });
                }
                scanned_bytes += text.len();
                // Leave a query-sized overlap so a match crossing a UTF-8 window
                // boundary is observed once on the next read. No mid-codepoint cuts.
                let overlap = if read.view.next_offset.is_some() {
                    let mut end = text.len().saturating_sub(query.query.len());
                    while end > 0 && !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    end
                } else {
                    text.len()
                };
                if overlap == 0 && read.view.next_offset.is_some() {
                    return Err(FileQueryError::Integrity);
                }
                let mut consumed = overlap;
                for (offset, _) in text.match_indices(&query.query) {
                    if offset >= overlap && read.view.next_offset.is_some() {
                        break;
                    }
                    let end = offset + query.query.len();
                    let mut excerpt_start = offset.saturating_sub(96);
                    while !text.is_char_boundary(excerpt_start) {
                        excerpt_start += 1;
                    }
                    let mut excerpt_end = end.saturating_add(160).min(text.len());
                    while !text.is_char_boundary(excerpt_end) {
                        excerpt_end -= 1;
                    }
                    hits.push(FileSearchHit {
                        file_ref: entry.file_ref.clone(),
                        name: entry.name.clone(),
                        representation: read.view.representation,
                        parser_revision: read.view.parser_revision.clone(),
                        start_byte: read.view.resolved_start + offset,
                        end_byte: read.view.resolved_start + end,
                        excerpt: text[excerpt_start..excerpt_end].into(),
                    });
                    if hits.len() == query.limit {
                        consumed = end;
                        break;
                    }
                }
                if consumed < text.len() || read.view.next_offset.is_some() {
                    position.offset = read.view.resolved_start + consumed;
                } else {
                    position.index += 1;
                    position.offset = 0;
                }
            }
            // Multi-file search must not mix changes occurring between individual reads.
            if query.file_ref.is_none() {
                let fresh = self
                    .search_catalog(
                        context,
                        query.traversal,
                        ListFiles {
                            cursor: position.catalog_cursor.clone(),
                            ..list.clone()
                        },
                    )
                    .await?;
                activity.completed()?;
                if fresh.revision != page.revision {
                    return Err(FileQueryError::Changed);
                }
            }
            catalog_pages += 1;
            if position.index < page.entries.len()
                || page.next_cursor.is_none()
                || reads >= READ_BUDGET
                || hits.len() >= query.limit
                || gaps.len() >= 128
                || catalog_pages >= CATALOG_BUDGET
            {
                break page;
            }
            position.catalog_cursor = page.next_cursor;
            position.index = 0;
            position.offset = 0;
        };
        position.gaps |= !gaps.is_empty();
        let exhausted = position.index == page.entries.len() && page.next_cursor.is_none();
        if position.index == page.entries.len() {
            position.catalog_cursor = page.next_cursor;
            position.index = 0;
        }
        let next_cursor = if exhausted {
            None
        } else {
            Some(
                URL_SAFE_NO_PAD
                    .encode(serde_json::to_vec(&position).map_err(|_| FileQueryError::Integrity)?),
            )
        };
        Ok(FileSearchPage {
            hits,
            unsearched: gaps,
            next_cursor,
            revision: page.revision,
            coverage: page.coverage,
            scanned_bytes,
            complete: exhausted && !position.gaps,
        })
    }
}
