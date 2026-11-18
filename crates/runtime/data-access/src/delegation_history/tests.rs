use super::*;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{
    ContentId, ConversationId, LedgerRecordId, PrincipalId, PrincipalKind, TenantId,
};
use musteragent_runtime_content::PutContent;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Fixture {
    slice: HistorySlice,
    bytes: Vec<u8>,
    reads: AtomicUsize,
    revoke_on_hydration: AtomicBool,
    revoked: AtomicBool,
}
impl DelegationHistorySource for Fixture {
    fn read<'a>(
        &'a self,
        _access: &'a HistoryAccess,
        input: &'a HistoryInput,
    ) -> ApplicationFuture<'a, Result<HistorySlice, HistoryError>> {
        Box::pin(async move {
            if self.revoked.load(Ordering::SeqCst) {
                return Err(HistoryError::Denied);
            }
            if let Some(cursor) = HistoryCursor::decode(input)? {
                cursor.validate_against(&self.slice.cursor)?;
            }
            Ok(self.slice.clone())
        })
    }
}
impl ContentStore for Fixture {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { panic!("history must not write") })
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.revoke_on_hydration.load(Ordering::SeqCst) {
                self.revoked.store(true, Ordering::SeqCst);
            }
            Ok(self.bytes.clone())
        })
    }
}
fn fixture(available: bool) -> (Arc<Fixture>, HistoryAccess, HistoryInput) {
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let current_run_id = RunId::generate();
    let bytes = "约束🦀\n\"精准内容\"".repeat(1000).into_bytes();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("fixture media type"),
        sha256: Sha256::digest(&bytes),
        byte_length: bytes.len() as u64,
    };
    let record_id = LedgerRecordId::generate().to_string();
    let input = serde_json::from_value(
        serde_json::json!({"view":"detail", "record_id":record_id,"max_bytes":4096}),
    )
    .expect("history input");
    let scope = HistoryCursor {
        schema: "musteragent.delegation-history-cursor.v1".into(),
        tenant: actor.tenant_id.to_string(),
        conversation: ConversationId::generate().to_string(),
        current_run: current_run_id.to_string(),
        branch: None,
        head: 10,
        view: HistoryView::Detail,
        handle: None,
        after: 5,
    };
    let fixture = Arc::new(Fixture {
        slice: HistorySlice {
            cursor: scope,
            has_more: false,
            entries: vec![HistoryEntry {
                result_invocation: None,
                record_id,
                sequence: 5,
                kind: "result",
                label: "Tool result".into(),
                handle: String::new(),
                source_run_id: Some(current_run_id.to_string()),
                content: Some(reference),
                content_available: available,
            }],
        },
        bytes,
        reads: AtomicUsize::new(0),
        revoke_on_hydration: AtomicBool::new(false),
        revoked: AtomicBool::new(false),
    });
    (
        fixture,
        HistoryAccess {
            actor,
            current_run_id,
            tool_fence: None,
        },
        input,
    )
}
#[tokio::test]
async fn utf8_pages_are_bounded_exact_and_explicitly_partial() {
    let (fixture, access, mut input) = fixture(true);
    let reader = DelegationHistoryReader::new(fixture.clone(), fixture.clone());
    let mut restored = String::new();
    loop {
        let page = reader.read(&access, &input).await.expect("history page");
        assert!(serde_json::to_vec(&page).expect("encoded page").len() <= input.max_bytes);
        let content = page.items[0].content.as_ref().expect("content descriptor");
        assert_eq!(content.offset, input.offset);
        assert!(!content.complete);
        restored.push_str(content.text.as_ref().expect("visible text"));
        match content.next_offset {
            Some(next) => {
                assert!(next > input.offset);
                input.offset = next;
            }
            None => break,
        }
    }
    assert_eq!(restored.as_bytes(), fixture.bytes);
    input.offset = 1;
    assert!(matches!(
        reader.read(&access, &input).await,
        Err(HistoryError::Invalid)
    ));
}
#[tokio::test]
async fn deletion_does_not_read_cached_bytes_and_revocation_during_hydration_fails_closed() {
    let (deleted, access, input) = fixture(false);
    let page = DelegationHistoryReader::new(deleted.clone(), deleted.clone())
        .read(&access, &input)
        .await
        .expect("deleted metadata");
    let content = page.items[0].content.as_ref().expect("content descriptor");
    assert_eq!(content.availability, "deleted_or_unavailable");
    assert!(content.text.is_none());
    assert_eq!(deleted.reads.load(Ordering::SeqCst), 0);
    let (changed, access, input) = fixture(true);
    changed.revoke_on_hydration.store(true, Ordering::SeqCst);
    assert!(matches!(
        DelegationHistoryReader::new(changed.clone(), changed.clone())
            .read(&access, &input)
            .await,
        Err(HistoryError::Denied)
    ));
    assert_eq!(changed.reads.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn summary_is_metadata_only_and_corrupt_content_is_not_published() {
    let (mut fixture, access, mut input) = fixture(true);
    input.view = HistoryView::Summary;
    input.record_id = None;
    Arc::get_mut(&mut fixture)
        .expect("exclusive fixture")
        .slice
        .cursor
        .view = HistoryView::Summary;
    let page = DelegationHistoryReader::new(fixture.clone(), fixture.clone())
        .read(&access, &input)
        .await
        .expect("summary page");
    assert!(
        page.items[0]
            .content
            .as_ref()
            .expect("summary descriptor")
            .text
            .is_none()
    );
    assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
    input.view = HistoryView::Detail;
    input.record_id = Some(fixture.slice.entries[0].record_id.clone());
    let state = Arc::get_mut(&mut fixture).expect("exclusive fixture");
    state.slice.cursor.view = HistoryView::Detail;
    state.bytes[0] = b'x';
    assert!(matches!(
        DelegationHistoryReader::new(fixture.clone(), fixture)
            .read(&access, &input)
            .await,
        Err(HistoryError::Corrupt)
    ));
}
