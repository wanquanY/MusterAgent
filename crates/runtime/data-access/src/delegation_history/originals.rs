//! Keyword search reads original authorized messages. Cursors are positions,
//! never an authorization grant, and every page is selected again by its owner.
use super::*;
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchPosition {
    pub page: u64,
    pub item: usize,
    pub offset: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalSearch {
    pub query: String,
    pub cursor: Option<SearchPosition>,
}
pub struct OriginalText {
    pub role: &'static str,
    pub text: String,
}
pub struct OriginalPage {
    pub items: Vec<OriginalText>,
    pub next: Option<u64>,
}
#[derive(Serialize)]
pub struct SearchHit {
    role: &'static str,
    text: String,
    offset: usize,
    complete: bool,
}
#[derive(Serialize)]
pub struct SearchResponse {
    matches: Vec<SearchHit>,
    next_cursor: Option<SearchPosition>,
}

impl DelegationHistoryReader {
    pub async fn search(
        &self,
        access: &HistoryAccess,
        input: OriginalSearch,
    ) -> Result<SearchResponse, HistoryError> {
        let query = input.query.trim();
        let cursor = input.cursor.unwrap_or(SearchPosition {
            page: 1,
            item: 0,
            offset: 0,
        });
        if query.is_empty()
            || query.chars().count() > 512
            || cursor.page == 0
            || access.tool_fence.is_none()
        {
            return Err(HistoryError::Invalid);
        }
        let page = self
            .source
            .originals(access, self.content.clone(), cursor.page)
            .await?;
        page.find(query, cursor)
    }
}

impl OriginalPage {
    fn find(self, query: &str, cursor: SearchPosition) -> Result<SearchResponse, HistoryError> {
        const PAGE_BYTES: usize = 16384;
        const MAX_MATCHES: usize = 16;
        if cursor.item > self.items.len() || (cursor.item == self.items.len() && cursor.offset != 0)
        {
            return Err(HistoryError::Invalid);
        }
        let mut matches = Vec::new();
        let mut left = PAGE_BYTES;
        let needle = query.to_lowercase();
        for (item, original) in self.items.iter().enumerate().skip(cursor.item) {
            let offset = if item == cursor.item {
                cursor.offset
            } else {
                0
            };
            if !original.text.is_char_boundary(offset) {
                return Err(HistoryError::Invalid);
            }
            if !original.text.to_lowercase().contains(&needle) {
                continue;
            }
            if left < 4 || matches.len() == MAX_MATCHES {
                return Ok(SearchResponse {
                    matches,
                    next_cursor: Some(SearchPosition {
                        item,
                        offset,
                        ..cursor
                    }),
                });
            }
            let end = original
                .text
                .floor_char_boundary(offset.saturating_add(left).min(original.text.len()));
            let complete = end == original.text.len();
            matches.push(SearchHit {
                role: original.role,
                text: original.text[offset..end].into(),
                offset,
                complete,
            });
            left -= end - offset;
            if !complete {
                return Ok(SearchResponse {
                    matches,
                    next_cursor: Some(SearchPosition {
                        item,
                        offset: end,
                        ..cursor
                    }),
                });
            }
        }
        Ok(SearchResponse {
            matches,
            next_cursor: self.next.map(|page| SearchPosition {
                page,
                item: 0,
                offset: 0,
            }),
        })
    }
}

impl HistoryInput {
    pub fn original_scope() -> Self {
        Self {
            view: HistoryView::Summary,
            handle: None,
            record_id: None,
            cursor: None,
            limit: 1,
            max_bytes: 16384,
            offset: 0,
        }
    }
    pub fn original_record(record: String) -> Self {
        Self {
            view: HistoryView::Detail,
            record_id: Some(record),
            ..Self::original_scope()
        }
    }
}
/// Reuse the authorized detail reader for Work's original record. Only public
/// message and result bodies can become keyword-search results.
pub async fn original_record_text(
    entry: &HistoryEntry,
    store: &dyn ContentStore,
    access: &HistoryAccess,
) -> Result<Option<OriginalText>, HistoryError> {
    let role = match (entry.kind, entry.label.as_str()) {
        ("message", "User input") => "user",
        ("message", _) => "assistant",
        ("result", _) => "tool",
        _ => return Ok(None),
    };
    let Some(reference) = entry.content.as_ref().filter(|_| entry.content_available) else {
        return Ok(None);
    };
    let bytes = match store.read(access.actor.tenant_id, reference).await {
        Ok(bytes) => bytes,
        Err(ContentStoreError::NotFound) => return Ok(None),
        Err(ContentStoreError::Unauthorized) => return Err(HistoryError::Denied),
        Err(_) => return Err(HistoryError::Unavailable),
    };
    verify_content(reference, &bytes).map_err(|_| HistoryError::Corrupt)?;
    let Ok(text) = String::from_utf8(bytes) else {
        return Ok(None);
    };
    Ok(Some(OriginalText { role, text }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start() -> SearchPosition {
        SearchPosition {
            page: 1,
            item: 0,
            offset: 0,
        }
    }
    fn page(text: &str) -> OriginalPage {
        OriginalPage {
            items: vec![OriginalText {
                role: "user",
                text: text.to_owned(),
            }],
            next: Some(65),
        }
    }

    #[test]
    fn unicode_hits_resume_at_the_exact_byte_until_the_original_is_complete() {
        let original = "🌳Cedar枝".repeat(2000);
        let first = page(&original).find("cedar", start()).unwrap();
        let hit = &first.matches[0];
        assert!(!hit.complete);
        let cursor = first.next_cursor.unwrap();
        assert_eq!(cursor.offset, hit.text.len());
        let last = page(&original).find("CEDAR", cursor).unwrap();
        assert_eq!(last.matches[0].offset, hit.text.len());
        assert!(last.matches[0].complete);
        assert_eq!(hit.text.clone() + &last.matches[0].text, original);
        assert_eq!(last.next_cursor.unwrap().page, 65);
    }
    #[test]
    fn only_valid_source_offsets_are_accepted_even_with_an_empty_page() {
        for (item, offset) in [(0, 1), (0, 200), (1, 1), (2, 0)] {
            assert!(matches!(
                page("🌳").find(
                    "🌳",
                    SearchPosition {
                        item,
                        offset,
                        ..start()
                    }
                ),
                Err(HistoryError::Invalid)
            ));
        }
        let empty = page("另一个词").find("not present", start()).unwrap();
        assert!(empty.matches.is_empty());
        assert_eq!(empty.next_cursor.unwrap().page, 65);
    }
}
