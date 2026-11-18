//! User-facing management reads over the original immutable Memory contents.
//! No management projection changes Runtime retrieval visibility.
use std::{num::NonZeroU16, sync::Arc};

use musteragent_content_ref::ContentRef;
use musteragent_identity::{Actor, AgentId, MemoryId, MemoryVersionId, UtcTimestamp};
use musteragent_memory::MemorySource;
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::ApplicationFuture;

const MAX_MEMORY_BYTES: u64 = 256 * 1024;
const SUMMARY_CHARACTERS: usize = 512;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentMemoryManagementIdentity {
    pub actor: Actor,
    pub agent_id: AgentId,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ManagedMemoryLifecycle {
    Active,
    Revoked,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ManagedMemoryCursor {
    pub updated_at: UtcTimestamp,
    pub memory_id: MemoryId,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListManagedMemories {
    pub identity: AgentMemoryManagementIdentity,
    pub lifecycle: ManagedMemoryLifecycle,
    pub observed_at: UtcTimestamp,
    pub after: Option<ManagedMemoryCursor>,
    pub limit: NonZeroU16,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedMemoryHead {
    pub source: MemorySource,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub lifecycle: ManagedMemoryLifecycle,
    pub updated_at: UtcTimestamp,
    pub content_ref: ContentRef,
    pub content_erased: bool,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedMemoryHeadPage {
    pub heads: Vec<ManagedMemoryHead>,
    pub next_cursor: Option<ManagedMemoryCursor>,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ManagedMemoryContentState {
    Ready,
    Unavailable,
    Invalid,
    Erased,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedMemory {
    pub agent_id: AgentId,
    pub source: MemorySource,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub lifecycle: ManagedMemoryLifecycle,
    pub updated_at: UtcTimestamp,
    pub text: String,
    pub text_truncated: bool,
    pub content_state: ManagedMemoryContentState,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedMemoryPage {
    pub items: Vec<ManagedMemory>,
    pub next_cursor: Option<ManagedMemoryCursor>,
}
#[derive(Debug, thiserror::Error)]
pub enum MemoryManagementError {
    #[error("Memory management requires the owning user")]
    Unauthorized,
    #[error("Memory is unavailable")]
    NotFound,
    #[error("Memory changed; reload its current version")]
    Conflict,
    #[error("Memory management query is invalid")]
    InvalidQuery,
    #[error("Memory management storage is unavailable")]
    Store,
}

/// The implementation must check original Agent ownership together with each
/// head, not trust an Agent UUID supplied by a presentation client.
pub trait AgentMemoryManagementStore: Send + Sync {
    fn list_managed_memories(
        &self,
        query: ListManagedMemories,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHeadPage, MemoryManagementError>>;
    fn read_managed_memory(
        &self,
        identity: AgentMemoryManagementIdentity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHead, MemoryManagementError>>;
    /// Rechecks current ownership, exact immutable heads/lifecycles and erasure
    /// fences after Content IO. A changed head must never return stale text.
    fn validate_managed_memory_heads(
        &self,
        identity: AgentMemoryManagementIdentity,
        heads: Vec<ManagedMemoryHead>,
    ) -> ApplicationFuture<'_, Result<Vec<bool>, MemoryManagementError>>;
}

pub struct AgentMemoryManagementService<S> {
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
}
impl<S: AgentMemoryManagementStore> AgentMemoryManagementService<S> {
    pub fn new(store: Arc<S>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn list(
        &self,
        query: ListManagedMemories,
    ) -> Result<ManagedMemoryPage, MemoryManagementError> {
        if query.limit.get() > 50
            || query
                .after
                .is_some_and(|c| c.updated_at > query.observed_at)
        {
            return Err(MemoryManagementError::InvalidQuery);
        }
        let identity = query.identity;
        let page = self.store.list_managed_memories(query).await?;
        let items = self.materialize(identity, &page.heads, true).await?;
        Ok(ManagedMemoryPage {
            items,
            next_cursor: page.next_cursor,
        })
    }

    pub async fn read(
        &self,
        identity: AgentMemoryManagementIdentity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> Result<ManagedMemory, MemoryManagementError> {
        let head = self
            .store
            .read_managed_memory(identity, memory_id, expected)
            .await?;
        self.materialize(identity, &[head], false)
            .await?
            .into_iter()
            .next()
            .ok_or(MemoryManagementError::Conflict)
    }

    async fn materialize(
        &self,
        identity: AgentMemoryManagementIdentity,
        heads: &[ManagedMemoryHead],
        summary: bool,
    ) -> Result<Vec<ManagedMemory>, MemoryManagementError> {
        materialize_heads(
            &*self.content,
            identity.actor.tenant_id,
            heads,
            summary,
            self.store
                .validate_managed_memory_heads(identity, heads.to_vec()),
        )
        .await
        .map(|items| {
            items
                .into_iter()
                .map(|item| ManagedMemory {
                    agent_id: identity.agent_id,
                    source: item.head.source,
                    memory_id: item.head.memory_id,
                    memory_version_id: item.head.memory_version_id,
                    lifecycle: item.head.lifecycle,
                    updated_at: item.head.updated_at,
                    text: item.text,
                    text_truncated: item.text_truncated,
                    content_state: item.content_state,
                })
                .collect()
        })
    }
}

pub(crate) struct MaterializedMemory {
    pub head: ManagedMemoryHead,
    pub text: String,
    pub text_truncated: bool,
    pub content_state: ManagedMemoryContentState,
}
/// Shared Memory content owner. The validation future is polled only after the
/// bounded batch Content IO; both scopes must then revalidate exact heads.
pub(crate) async fn materialize_heads(
    content: &dyn ContentStore,
    tenant_id: musteragent_identity::TenantId,
    heads: &[ManagedMemoryHead],
    summary: bool,
    validate: impl std::future::Future<Output = Result<Vec<bool>, MemoryManagementError>>,
) -> Result<Vec<MaterializedMemory>, MemoryManagementError> {
    let refs: Vec<_> = heads
        .iter()
        .filter(|h| !h.content_erased && h.content_ref.byte_length <= MAX_MEMORY_BYTES)
        .map(|h| h.content_ref.clone())
        .collect();
    // One bounded batch read, never a client-side checkout per list row.
    let payloads = content.read_batch(tenant_id, &refs).await;
    let payloads = match payloads {
        Ok(values) if values.len() == refs.len() => Some(values),
        _ => None,
    };
    let valid = validate.await?;
    if valid.len() != heads.len() {
        return Err(MemoryManagementError::Store);
    }
    let mut content_index = 0;
    let mut items = Vec::with_capacity(heads.len());
    for (head, valid) in heads.iter().zip(valid) {
        let payload = if !head.content_erased && head.content_ref.byte_length <= MAX_MEMORY_BYTES {
            let value = payloads.as_ref().and_then(|p| p.get(content_index));
            content_index += 1;
            value
        } else {
            None
        };
        if !valid {
            continue;
        }
        let decoded = if head.content_erased {
            Err(ManagedMemoryContentState::Erased)
        } else if head.content_ref.byte_length > MAX_MEMORY_BYTES {
            Err(ManagedMemoryContentState::Invalid)
        } else {
            payload
                .ok_or(ManagedMemoryContentState::Unavailable)
                .and_then(|bytes| display_text(head.source, &head.content_ref, bytes))
        };
        let (text, content_state) = match decoded {
            Ok(text) => (text, ManagedMemoryContentState::Ready),
            Err(state) => (String::new(), state),
        };
        let text_truncated = summary && text.chars().count() > SUMMARY_CHARACTERS;
        let text = if text_truncated {
            text.chars().take(SUMMARY_CHARACTERS).collect()
        } else {
            text
        };
        items.push(MaterializedMemory {
            head: head.clone(),
            text,
            text_truncated,
            content_state,
        });
    }
    Ok(items)
}

fn display_text(
    source: MemorySource,
    reference: &ContentRef,
    bytes: &[u8],
) -> Result<String, ManagedMemoryContentState> {
    let invalid = ManagedMemoryContentState::Invalid;
    verify_content(reference, bytes).map_err(|_| invalid)?;
    // A governed import preserves the original remember document. Provenance
    // is not a content format; the owner already authorized this published head.
    // Still require the exact document schema, never an arbitrary `content` field.
    if matches!(
        source,
        MemorySource::UserAuthored | MemorySource::GovernedImport
    ) && reference.media_type.as_str() == "application/json"
    {
        return crate::user_memory_document::UserMemoryDocument::decode(bytes)
            .map(|document| document.content)
            .map_err(|_| invalid);
    }
    if crate::is_experience_claim_media_type(reference.media_type.as_str()) {
        return crate::ExperienceClaimDocument::decode(bytes)
            .map(|claim| claim.claim)
            .map_err(|_| invalid);
    }
    match reference
        .media_type
        .as_str()
        .split(';')
        .next()
        .map(str::trim)
    {
        Some("text/plain" | "text/markdown") => {
            String::from_utf8(bytes.to_vec()).map_err(|_| invalid)
        }
        _ => Err(invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{MediaType, Sha256};
    use musteragent_identity::*;
    use musteragent_runtime_content::{ContentStoreError, PutContent};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Store {
        head: ManagedMemoryHead,
        changed: Arc<AtomicBool>,
    }
    impl AgentMemoryManagementStore for Store {
        fn list_managed_memories(
            &self,
            _: ListManagedMemories,
        ) -> ApplicationFuture<'_, Result<ManagedMemoryHeadPage, MemoryManagementError>> {
            Box::pin(async {
                Ok(ManagedMemoryHeadPage {
                    heads: vec![self.head.clone()],
                    next_cursor: None,
                })
            })
        }
        fn read_managed_memory(
            &self,
            _: AgentMemoryManagementIdentity,
            _: MemoryId,
            _: MemoryVersionId,
        ) -> ApplicationFuture<'_, Result<ManagedMemoryHead, MemoryManagementError>> {
            Box::pin(async { Ok(self.head.clone()) })
        }
        fn validate_managed_memory_heads(
            &self,
            _: AgentMemoryManagementIdentity,
            heads: Vec<ManagedMemoryHead>,
        ) -> ApplicationFuture<'_, Result<Vec<bool>, MemoryManagementError>> {
            Box::pin(async move { Ok(vec![!self.changed.load(Ordering::SeqCst); heads.len()]) })
        }
    }
    struct Content {
        bytes: Vec<u8>,
        changed: Arc<AtomicBool>,
        invalidate_during_read: bool,
    }
    impl ContentStore for Content {
        fn put(
            &self,
            _: PutContent,
        ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
            Box::pin(async { Err(ContentStoreError::Unavailable) })
        }
        fn read<'a>(
            &'a self,
            _: TenantId,
            _: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
            Box::pin(async move {
                if self.invalidate_during_read {
                    self.changed.store(true, Ordering::SeqCst);
                }
                Ok(self.bytes.clone())
            })
        }
    }
    fn fixture(
        bytes: Vec<u8>,
        media: &str,
        invalidate: bool,
    ) -> (
        AgentMemoryManagementService<Store>,
        AgentMemoryManagementIdentity,
        ManagedMemoryHead,
    ) {
        let head = ManagedMemoryHead {
            source: MemorySource::UserAuthored,
            content_erased: false,
            memory_id: MemoryId::generate(),
            memory_version_id: MemoryVersionId::generate(),
            lifecycle: ManagedMemoryLifecycle::Active,
            updated_at: UtcTimestamp::from_unix_millis(1),
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(media).unwrap(),
                sha256: Sha256::digest(&bytes),
                byte_length: bytes.len() as u64,
            },
        };
        let changed = Arc::new(AtomicBool::new(false));
        let app = AgentMemoryManagementService::new(
            Arc::new(Store {
                head: head.clone(),
                changed: changed.clone(),
            }),
            Arc::new(Content {
                bytes,
                changed,
                invalidate_during_read: invalidate,
            }),
        );
        let identity = AgentMemoryManagementIdentity {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            agent_id: AgentId::generate(),
        };
        (app, identity, head)
    }
    #[tokio::test]
    async fn erased_current_head_retains_identity_without_reading_private_content() {
        let (mut app, identity, mut head) =
            fixture(b"private preference".to_vec(), "text/plain", true);
        head.content_erased = true;
        Arc::get_mut(&mut app.store).unwrap().head = head.clone();
        let item = app
            .read(identity, head.memory_id, head.memory_version_id)
            .await
            .unwrap();
        assert_eq!(item.memory_version_id, head.memory_version_id);
        assert_eq!(item.content_state, ManagedMemoryContentState::Erased);
        assert!(item.text.is_empty());
        assert!(!item.text_truncated);
        // Content::read would set this flag. An erased head must never reach it.
        assert!(!app.store.changed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn late_revocation_erasure_or_owner_change_never_returns_materialized_text() {
        let (app, identity, head) = fixture(b"private content".to_vec(), "text/plain", true);
        assert!(matches!(
            app.read(identity, head.memory_id, head.memory_version_id)
                .await,
            Err(MemoryManagementError::Conflict)
        ));
    }
    #[tokio::test]
    async fn summaries_are_unicode_bounded_and_detail_keeps_original_text() {
        let text = "记忆".repeat(400);
        let (app, identity, head) = fixture(text.as_bytes().to_vec(), "text/markdown", false);
        let page = app
            .list(ListManagedMemories {
                identity,
                lifecycle: ManagedMemoryLifecycle::Active,
                observed_at: UtcTimestamp::from_unix_millis(100),
                after: None,
                limit: NonZeroU16::new(1).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(page.items[0].text.chars().count(), 512);
        assert!(page.items[0].text_truncated);
        assert_eq!(
            app.read(identity, head.memory_id, head.memory_version_id)
                .await
                .unwrap()
                .text,
            text
        );
    }
    #[tokio::test]
    async fn untrusted_non_memory_documents_do_not_become_memory_text() {
        let (app, identity, head) = fixture(
            b"{\"secret\":\"not a claim\"}".to_vec(),
            "application/json",
            false,
        );
        let item = app
            .read(identity, head.memory_id, head.memory_version_id)
            .await
            .unwrap();
        assert_eq!(item.content_state, ManagedMemoryContentState::Invalid);
        assert!(item.text.is_empty());
    }

    #[tokio::test]
    async fn original_user_memory_document_is_displayed_without_changing_content() {
        let text = format!("{}\n\"偏好\"", "中".repeat(600));
        let bytes = crate::user_memory_document::UserMemoryDocument {
            fact_key: "preference.reply".into(),
            content: text.clone(),
        }
        .encode()
        .unwrap();
        let (app, identity, head) = fixture(bytes, "application/json", false);
        let page = app
            .list(ListManagedMemories {
                identity,
                lifecycle: ManagedMemoryLifecycle::Active,
                observed_at: UtcTimestamp::from_unix_millis(100),
                after: None,
                limit: NonZeroU16::new(1).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            page.items[0].content_state,
            ManagedMemoryContentState::Ready
        );
        assert_eq!(page.items[0].text.chars().count(), 512);
        assert!(page.items[0].text_truncated);
        let detail = app
            .read(identity, head.memory_id, head.memory_version_id)
            .await
            .unwrap();
        assert_eq!(detail.text, text);
        assert!(!detail.text_truncated);
        assert_eq!(detail.memory_version_id, head.memory_version_id);
    }

    #[test]
    fn json_memory_display_requires_original_source_contract_and_integrity() {
        let bytes = br#"{"fact_key":"preference.language","content":"Chinese"}"#;
        let (_, _, head) = fixture(bytes.to_vec(), "application/json", false);
        let invalid = Err(ManagedMemoryContentState::Invalid);
        assert_eq!(
            display_text(MemorySource::VerifiedExperience, &head.content_ref, bytes),
            invalid
        );
        assert_eq!(
            display_text(MemorySource::GovernedImport, &head.content_ref, bytes),
            Ok("Chinese".into())
        );
        assert_eq!(display_text(head.source, &head.content_ref, b"{}"), invalid);
        for bytes in [
            br#"{"content":"Chinese"}"#.as_slice(),
            br#"{"fact_key":"aa","content":"Chinese","extra":true}"#,
            br#"{"fact_key":"aa","content":"Chinese","content":"other"}"#,
            br#"{"fact_key":"bad key","content":"Chinese"}"#,
            br#"{"fact_key":"aa","content":" "}"#,
            br#"{"fact_key":"aa","content":false}"#,
            b"not json",
        ] {
            let (_, _, head) = fixture(bytes.to_vec(), "application/json", false);
            assert_eq!(display_text(head.source, &head.content_ref, bytes), invalid);
            assert_eq!(
                display_text(MemorySource::GovernedImport, &head.content_ref, bytes),
                invalid
            );
        }
    }
}
