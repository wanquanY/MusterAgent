use std::collections::VecDeque;
#[path = "subscription/byte_budget.rs"]
mod byte_budget;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures_util::StreamExt;
use musteragent_conversation::{
    ActivateConversationContextBranch, ArchiveConversation, ConversationApplication,
    ConversationCatalogApplication, ConversationCatalogChange, ConversationEvent, ConversationKind,
    ConversationLifecycle, ConversationPage, ConversationSummary, ConversationView,
    CreateConversation, EditAndResubmitMessage, ForkConversationContext, GetConversation,
    ListConversations, RetractMessage, SetConversationTitle, SubmitMessage,
    SubscribeConversationCatalog, UnarchiveConversation,
};
use musteragent_event_envelope::{LedgerCursor, LedgerMetadata, LedgerRecordKind, Producer};
use musteragent_identity::{
    Actor, ConversationId, ConversationSeq, CorrelationId, DottedKey, HomeEpoch, IdempotencyKey,
    LedgerOffset, LedgerRecordId, PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    CanonicalLedgerRecord, DomainEvent, LedgerApplication, LedgerPayload, ListConversationRecords,
    RecordPage, SubscribeConversationRecords,
};
use musteragent_projection::{
    CatalogChangeSubscription, ChangeFeed, ChangeSubscription,
    ConversationCatalogSubscriptionPolicy, ConversationCatalogSubscriptionService,
    LedgerRecordSource, LedgerSubscriptionService, SubscriptionError, SubscriptionErrorMapper,
    SubscriptionPolicy,
};
use musteragent_runtime_contract::{
    ApplicationFuture, CommandAck, CommandEnvelope, DisplayText, ErrorCode, ErrorDetails,
    RuntimeError, SafeDisplayText,
};
use tokio::sync::Notify;

#[tokio::test]
async fn ten_thousand_record_replay_is_paged_and_ordered() {
    let fixture = Fixture::new(10_001);
    let service = fixture.service(127, 8);
    let mut stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    let mut seen = 0_u64;
    while seen < 10_001 {
        let record = stream
            .next()
            .await
            .unwrap_or_else(|| panic!("stream ended at {seen}"))
            .unwrap_or_else(|error| panic!("record failed: {error:?}"));
        seen += 1;
        assert_eq!(record.metadata.conversation_seq.get(), seen);
    }
    drop(stream);
    service.shutdown().await;
    assert!(fixture.source.calls.load(Ordering::SeqCst) >= 79);
    assert_eq!(fixture.source.maximum_requested.load(Ordering::SeqCst), 127);
}

#[tokio::test]
async fn a_slow_consumer_applies_bounded_backpressure_before_another_page() {
    let fixture = Fixture::new(1_000);
    let service = fixture.service(128, 4);
    let stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        fixture.source.calls.load(Ordering::SeqCst),
        1,
        "the producer must block on the bounded channel instead of preloading pages"
    );
    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_a_blocked_send_while_the_receiver_remains_alive() {
    let fixture = Fixture::new(1_000);
    let service = fixture.service(128, 4);
    let stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        fixture.source.calls.load(Ordering::SeqCst),
        1,
        "the live receiver must keep the producer blocked on the bounded channel"
    );
    tokio::time::timeout(Duration::from_secs(1), service.shutdown())
        .await
        .unwrap_or_else(|_| panic!("shutdown did not cancel the blocked channel send"));

    drop(stream);
}

#[tokio::test]
async fn shutdown_cancels_a_pending_source_query_while_the_receiver_remains_alive() {
    let fixture = Fixture::new(0);
    let source = Arc::new(NeverCompletesSource::default());
    let service = LedgerSubscriptionService::new(
        SubscriptionPolicy {
            page_byte_limit: 8 * 1_024 * 1_024,
            pending_byte_limit: 32 * 1_024 * 1_024,
            page_size: 128,
            channel_capacity: 4,
            poll_interval: Duration::from_millis(100),
        },
        source.clone(),
        fixture.changes.clone(),
        Arc::new(ErrorMapper),
    )
    .unwrap_or_else(|error| panic!("invalid fixture policy: {error}"));
    let stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    tokio::time::timeout(Duration::from_secs(1), async {
        while source.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("subscription task did not enter the pending source query"));
    tokio::time::timeout(Duration::from_secs(1), service.shutdown())
        .await
        .unwrap_or_else(|_| panic!("shutdown did not cancel the pending source query"));

    drop(stream);
}

#[tokio::test]
async fn a_notification_racing_with_the_empty_page_is_replayed_from_the_cursor() {
    let fixture = Fixture::new(0);
    let service = fixture.service(32, 4);
    let mut stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    fixture
        .source
        .append(record(fixture.tenant_id, fixture.conversation_id, 1));
    fixture.changes.notify();
    let received = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap_or_else(|_| panic!("live record deadline exceeded"))
        .unwrap_or_else(|| panic!("stream ended"))
        .unwrap_or_else(|error| panic!("record failed: {error:?}"));
    assert_eq!(received.metadata.conversation_seq.get(), 1);
    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn a_record_committed_without_a_notification_is_recovered_by_bounded_polling() {
    let fixture = Fixture::new(0);
    let service = fixture.service(32, 4);
    let mut stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    tokio::time::sleep(Duration::from_millis(20)).await;
    fixture
        .source
        .append(record(fixture.tenant_id, fixture.conversation_id, 1));
    let received = tokio::time::timeout(Duration::from_millis(350), stream.next())
        .await
        .unwrap_or_else(|_| panic!("periodic Cursor poll did not recover the committed record"))
        .unwrap_or_else(|| panic!("stream ended"))
        .unwrap_or_else(|error| panic!("record failed: {error:?}"));
    assert_eq!(received.metadata.conversation_seq.get(), 1);
    assert!(
        fixture.source.calls.load(Ordering::SeqCst) <= 4,
        "bounded polling must not become a busy loop"
    );
    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn a_record_committed_before_subscription_needs_no_notification_to_be_observed() {
    let fixture = Fixture::new(0);
    fixture
        .source
        .append(record(fixture.tenant_id, fixture.conversation_id, 1));
    let service = fixture.service(32, 4);
    let mut stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .unwrap_or_else(|error| panic!("subscription failed: {error:?}"));

    let received = tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .unwrap_or_else(|_| panic!("initial Cursor backfill deadline exceeded"))
        .unwrap_or_else(|| panic!("stream ended"))
        .unwrap_or_else(|error| panic!("record failed: {error:?}"));
    assert_eq!(received.metadata.conversation_seq.get(), 1);
    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn a_page_with_a_sequence_gap_is_rejected_fail_closed() {
    let fixture = Fixture::new(2);
    fixture.source.mutate(1, |record| {
        record.metadata.conversation_seq =
            ConversationSeq::new(3).unwrap_or_else(|error| panic!("{error}"));
    });
    let service = fixture.service(32, 4);

    assert!(
        service
            .list_conversation_records(fixture.list_query(32))
            .await
            .is_err()
    );
    service.shutdown().await;
}

#[tokio::test]
async fn a_page_crossing_a_tenant_boundary_is_rejected_fail_closed() {
    let fixture = Fixture::new(1);
    fixture.source.mutate(0, |record| {
        record.metadata.tenant_id = TenantId::generate();
    });
    let service = fixture.service(32, 4);

    assert!(
        service
            .list_conversation_records(fixture.list_query(32))
            .await
            .is_err()
    );
    service.shutdown().await;
}

#[tokio::test]
async fn catalog_changes_are_owner_authorized_and_loss_is_a_resync_hint() {
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let actor = Actor {
        tenant_id,
        principal_id,
        principal_kind: PrincipalKind::User,
    };
    let authorized_id = ConversationId::generate();
    let unauthorized_id = ConversationId::generate();
    let conversations = Arc::new(AuthorizedConversations::new(actor, authorized_id));
    conversations.set_visible(false);
    let changes = Arc::new(MemoryChanges::default());
    let service = ConversationCatalogSubscriptionService::new(
        ConversationCatalogSubscriptionPolicy {
            channel_capacity: 2,
        },
        conversations.clone(),
        changes.clone(),
        Arc::new(ErrorMapper),
    )
    .unwrap_or_else(|error| panic!("invalid catalog fixture policy: {error}"));
    let mut stream = service
        .subscribe_conversation_catalog(SubscribeConversationCatalog { actor })
        .await
        .unwrap_or_else(|error| panic!("catalog subscription failed: {error:?}"));

    assert_eq!(
        stream
            .next()
            .await
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::ResyncRequired),
        "subscription establishment must close the list/subscribe race"
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err(),
        "an idle catalog subscription must not force periodic full resyncs"
    );

    changes.notify_catalog(Some(unauthorized_id));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err(),
        "a global change hint must not leak an unauthorized Conversation id"
    );

    conversations.set_visible(true);
    changes.notify_catalog(Some(authorized_id));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap_or_else(|_| panic!("authorized catalog change deadline exceeded"))
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::Upsert(conversations.summary()))
    );

    conversations.set_latest_sequence(1);
    changes.notify_catalog(Some(authorized_id));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err(),
        "cursor-only Ledger movement must not publish a catalog delta"
    );

    conversations.set_active_run_count(1);
    changes.notify_catalog(Some(authorized_id));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap_or_else(|_| panic!("semantic catalog change deadline exceeded"))
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::Upsert(conversations.summary()))
    );

    conversations.set_visible(false);
    changes.notify_catalog(Some(authorized_id));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap_or_else(|_| panic!("catalog removal deadline exceeded"))
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::Remove(authorized_id))
    );

    changes.notify_catalog(None);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap_or_else(|_| panic!("catalog resync deadline exceeded"))
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::ResyncRequired)
    );
    assert_eq!(conversations.list_calls.load(Ordering::SeqCst), 7);

    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn catalog_baseline_suppresses_cursor_noise_and_authorizes_removals() {
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let conversation_id = ConversationId::generate();
    let conversations = Arc::new(AuthorizedConversations::new(actor, conversation_id));
    let changes = Arc::new(MemoryChanges::default());
    let service = ConversationCatalogSubscriptionService::new(
        ConversationCatalogSubscriptionPolicy {
            channel_capacity: 2,
        },
        conversations.clone(),
        changes.clone(),
        Arc::new(ErrorMapper),
    )
    .unwrap_or_else(|error| panic!("invalid catalog fixture policy: {error}"));
    let mut stream = service
        .subscribe_conversation_catalog(SubscribeConversationCatalog { actor })
        .await
        .unwrap_or_else(|error| panic!("catalog subscription failed: {error:?}"));

    assert_eq!(
        stream
            .next()
            .await
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::ResyncRequired)
    );

    conversations.set_latest_sequence(1);
    changes.notify_catalog(Some(conversation_id));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err(),
        "the authorized baseline must suppress first cursor-only noise"
    );

    conversations.set_visible(false);
    changes.notify_catalog(Some(conversation_id));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap_or_else(|_| panic!("catalog removal deadline exceeded"))
            .transpose()
            .unwrap_or_else(|error| panic!("{error:?}")),
        Some(ConversationCatalogChange::Remove(conversation_id)),
        "a baseline row may be removed without leaking an unauthorized id"
    );
    assert_eq!(conversations.list_calls.load(Ordering::SeqCst), 3);

    drop(stream);
    service.shutdown().await;
}

struct Fixture {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    source: Arc<MemorySource>,
    changes: Arc<MemoryChanges>,
}

impl Fixture {
    fn new(count: u64) -> Self {
        let tenant_id = TenantId::generate();
        let conversation_id = ConversationId::generate();
        let records = (1..=count)
            .map(|sequence| record(tenant_id, conversation_id, sequence))
            .collect();
        Self {
            tenant_id,
            principal_id: PrincipalId::generate(),
            conversation_id,
            source: Arc::new(MemorySource::new(records)),
            changes: Arc::new(MemoryChanges::default()),
        }
    }

    fn service(&self, page_size: usize, channel_capacity: usize) -> LedgerSubscriptionService {
        LedgerSubscriptionService::new(
            SubscriptionPolicy {
                page_byte_limit: 8 * 1_024 * 1_024,
                pending_byte_limit: 32 * 1_024 * 1_024,
                page_size,
                channel_capacity,
                poll_interval: Duration::from_millis(100),
            },
            self.source.clone(),
            self.changes.clone(),
            Arc::new(ErrorMapper),
        )
        .unwrap_or_else(|error| panic!("invalid fixture policy: {error}"))
    }

    fn query(&self) -> SubscribeConversationRecords {
        SubscribeConversationRecords {
            actor: Actor {
                tenant_id: self.tenant_id,
                principal_id: self.principal_id,
                principal_kind: PrincipalKind::User,
            },
            conversation_id: self.conversation_id,
            cursor: LedgerCursor {
                conversation_id: self.conversation_id,
                home_epoch: HomeEpoch::new(1).unwrap_or_else(|error| panic!("{error}")),
                after_conversation_seq: LedgerOffset::ZERO,
            },
        }
    }

    fn list_query(&self, limit: usize) -> ListConversationRecords {
        let query = self.query();
        ListConversationRecords {
            actor: query.actor,
            conversation_id: query.conversation_id,
            cursor: query.cursor,
            limit,
        }
    }
}

struct MemorySource {
    records: RwLock<Vec<CanonicalLedgerRecord>>,
    calls: AtomicUsize,
    maximum_requested: AtomicUsize,
}

impl MemorySource {
    fn new(records: Vec<CanonicalLedgerRecord>) -> Self {
        Self {
            records: RwLock::new(records),
            calls: AtomicUsize::new(0),
            maximum_requested: AtomicUsize::new(0),
        }
    }

    fn append(&self, value: CanonicalLedgerRecord) {
        self.records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(value);
    }

    fn mutate(&self, index: usize, mutate: impl FnOnce(&mut CanonicalLedgerRecord)) {
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        mutate(&mut records[index]);
    }
}

impl LedgerRecordSource for MemorySource {
    fn list(
        &self,
        _actor: Actor,
        cursor: LedgerCursor,
        limit: usize,
        max_page_bytes: usize,
    ) -> ApplicationFuture<'_, Result<RecordPage, SubscriptionError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.maximum_requested.fetch_max(limit, Ordering::SeqCst);
        Box::pin(async move {
            let records = self
                .records
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let start = usize::try_from(cursor.after_conversation_seq.get())
                .map_err(|_| SubscriptionError::InvalidQuery)?;
            let remaining = records.len().saturating_sub(start);
            let admitted = musteragent_ledger::LedgerReadBudget::new(max_page_bytes)
                .map_err(|_| SubscriptionError::InvalidQuery)?
                .admitted_prefix(std::iter::repeat_n(1_024, remaining), limit)
                .map_err(|_| SubscriptionError::ReadBudgetExceeded)?;
            let end = start.saturating_add(admitted).min(records.len());
            let page = records[start..end].to_vec();
            let next = page.last().map_or(cursor.after_conversation_seq, |record| {
                LedgerOffset::from(record.metadata.conversation_seq)
            });
            let next_cursor = LedgerCursor {
                after_conversation_seq: next,
                ..cursor
            };
            Ok(RecordPage {
                records: page,
                next_cursor,
                has_more: end < records.len(),
                projection_checkpoint: next_cursor,
            })
        })
    }
}

#[derive(Default)]
struct NeverCompletesSource {
    calls: AtomicUsize,
}

impl LedgerRecordSource for NeverCompletesSource {
    fn list(
        &self,
        _actor: Actor,
        _cursor: LedgerCursor,
        _limit: usize,
        _max_page_bytes: usize,
    ) -> ApplicationFuture<'_, Result<RecordPage, SubscriptionError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        })
    }
}

struct AuthorizedConversations {
    actor: Actor,
    conversation_id: ConversationId,
    list_calls: AtomicUsize,
    latest_sequence: AtomicUsize,
    active_run_count: AtomicUsize,
    visible: AtomicBool,
}

impl AuthorizedConversations {
    fn new(actor: Actor, conversation_id: ConversationId) -> Self {
        Self {
            actor,
            conversation_id,
            list_calls: AtomicUsize::new(0),
            latest_sequence: AtomicUsize::new(0),
            active_run_count: AtomicUsize::new(0),
            visible: AtomicBool::new(true),
        }
    }

    fn set_latest_sequence(&self, value: usize) {
        self.latest_sequence.store(value, Ordering::SeqCst);
    }

    fn set_active_run_count(&self, value: usize) {
        self.active_run_count.store(value, Ordering::SeqCst);
    }

    fn set_visible(&self, value: bool) {
        self.visible.store(value, Ordering::SeqCst);
    }

    fn summary(&self) -> ConversationSummary {
        let latest_sequence = u64::try_from(self.latest_sequence.load(Ordering::SeqCst))
            .unwrap_or_else(|error| panic!("latest sequence conversion failed: {error}"));
        let active_run_count = u32::try_from(self.active_run_count.load(Ordering::SeqCst))
            .unwrap_or_else(|error| panic!("active run count conversion failed: {error}"));
        ConversationSummary {
            conversation_id: self.conversation_id,
            tenant_id: self.actor.tenant_id,
            workspace_id: None,
            kind: ConversationKind::Direct,
            title: Some(
                DisplayText::new("Authorized Conversation")
                    .unwrap_or_else(|error| panic!("{error}")),
            ),
            lifecycle: ConversationLifecycle::Active,
            latest_cursor: LedgerCursor {
                conversation_id: self.conversation_id,
                home_epoch: HomeEpoch::new(1).unwrap_or_else(|error| panic!("{error}")),
                after_conversation_seq: LedgerOffset::new(latest_sequence),
            },
            has_active_run: active_run_count > 0,
            operational: musteragent_conversation::ConversationOperationalSummary {
                active_run_count,
                running_run_count: active_run_count,
                ..Default::default()
            },
            created_at: UtcTimestamp::from_unix_millis(1),
            updated_at: UtcTimestamp::from_unix_millis(1),
        }
    }

    fn unauthorized(&self) -> RuntimeError {
        RuntimeError {
            schema_version: DottedKey::new("musteragent.runtime.error.v2")
                .unwrap_or_else(|error| panic!("{error}")),
            code: ErrorCode::ParticipantNotAuthorized,
            category: ErrorCode::ParticipantNotAuthorized.category(),
            message: SafeDisplayText::new("Conversation is not visible to this actor")
                .unwrap_or_else(|error| panic!("{error}")),
            retryable: false,
            retry_after_ms: None,
            resource_ref: None,
            correlation_id: CorrelationId::generate(),
            details: Box::new(ErrorDetails::None),
        }
    }
}

impl ConversationApplication for AuthorizedConversations {
    fn create_conversation(
        &self,
        _command: CommandEnvelope<CreateConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn recover_conversation_creation(
        &self,
        _command: CommandEnvelope<CreateConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn submit_message(
        &self,
        _command: CommandEnvelope<SubmitMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn retract_message(
        &self,
        _command: CommandEnvelope<RetractMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn fork_context(
        &self,
        _command: CommandEnvelope<ForkConversationContext>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn activate_context_branch(
        &self,
        _command: CommandEnvelope<ActivateConversationContextBranch>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn edit_and_resubmit_message(
        &self,
        _command: CommandEnvelope<EditAndResubmitMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn archive_conversation(
        &self,
        _command: CommandEnvelope<ArchiveConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn unarchive_conversation(
        &self,
        _command: CommandEnvelope<UnarchiveConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn set_conversation_title(
        &self,
        _command: CommandEnvelope<SetConversationTitle>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not execute commands") })
    }

    fn get_conversation(
        &self,
        _query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        Box::pin(async { unreachable!("catalog fixture does not read full views") })
    }

    fn list_conversations(
        &self,
        query: ListConversations,
    ) -> ApplicationFuture<'_, Result<ConversationPage, RuntimeError>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if query.actor != self.actor
                || query
                    .conversation_id
                    .is_some_and(|value| value != self.conversation_id)
            {
                return Err(self.unauthorized());
            }
            if !self.visible.load(Ordering::SeqCst) {
                return Ok(ConversationPage {
                    conversations: Vec::new(),
                    next_cursor: None,
                });
            }
            Ok(ConversationPage {
                conversations: vec![self.summary()],
                next_cursor: None,
            })
        })
    }
}

#[derive(Default)]
struct MemoryChanges {
    notify: Arc<Notify>,
    catalog_notify: Arc<Notify>,
    catalog_changes: Arc<Mutex<VecDeque<Option<ConversationId>>>>,
}

impl MemoryChanges {
    fn notify(&self) {
        self.notify.notify_one();
    }

    fn notify_catalog(&self, conversation_id: Option<ConversationId>) {
        self.catalog_changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(conversation_id);
        self.catalog_notify.notify_one();
    }
}

impl ChangeFeed for MemoryChanges {
    fn subscribe(
        &self,
        _conversation_id: ConversationId,
    ) -> Result<Arc<dyn ChangeSubscription>, SubscriptionError> {
        Ok(Arc::new(MemoryWatcher {
            notify: Arc::clone(&self.notify),
        }))
    }

    fn subscribe_catalog(&self) -> Result<Arc<dyn CatalogChangeSubscription>, SubscriptionError> {
        Ok(Arc::new(MemoryCatalogWatcher {
            notify: Arc::clone(&self.catalog_notify),
            changes: Arc::clone(&self.catalog_changes),
        }))
    }
}

struct MemoryWatcher {
    notify: Arc<Notify>,
}

impl ChangeSubscription for MemoryWatcher {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), SubscriptionError>> {
        Box::pin(async move {
            self.notify.notified().await;
            Ok(())
        })
    }
}

struct MemoryCatalogWatcher {
    notify: Arc<Notify>,
    changes: Arc<Mutex<VecDeque<Option<ConversationId>>>>,
}

impl CatalogChangeSubscription for MemoryCatalogWatcher {
    fn wait(&self) -> ApplicationFuture<'_, Result<Option<ConversationId>, SubscriptionError>> {
        Box::pin(async move {
            loop {
                if let Some(value) = self
                    .changes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front()
                {
                    return Ok(value);
                }
                self.notify.notified().await;
            }
        })
    }
}

struct ErrorMapper;

impl SubscriptionErrorMapper for ErrorMapper {
    fn map(&self, _error: SubscriptionError) -> RuntimeError {
        RuntimeError {
            schema_version: DottedKey::new("musteragent.runtime.error.v2")
                .unwrap_or_else(|error| panic!("{error}")),
            code: ErrorCode::DependencyUnavailable,
            category: ErrorCode::DependencyUnavailable.category(),
            message: SafeDisplayText::new("Ledger subscription unavailable")
                .unwrap_or_else(|error| panic!("{error}")),
            retryable: true,
            retry_after_ms: Some(100),
            resource_ref: None,
            correlation_id: CorrelationId::generate(),
            details: Box::new(ErrorDetails::None),
        }
    }
}

fn record(
    tenant_id: TenantId,
    conversation_id: ConversationId,
    sequence: u64,
) -> CanonicalLedgerRecord {
    CanonicalLedgerRecord {
        metadata: LedgerMetadata {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .unwrap_or_else(|error| panic!("{error}")),
            record_id: LedgerRecordId::generate(),
            record_kind: LedgerRecordKind::DomainEvent,
            tenant_id,
            conversation_id,
            conversation_seq: ConversationSeq::new(sequence)
                .unwrap_or_else(|error| panic!("{error}")),
            activity_id: None,
            run_id: None,
            run_seq: None,
            attempt_id: None,
            participant_id: None,
            home_epoch: HomeEpoch::new(1).unwrap_or_else(|error| panic!("{error}")),
            producer: Producer::RuntimeApi,
            occurred_at: UtcTimestamp::from_unix_millis(
                i64::try_from(sequence).unwrap_or(i64::MAX),
            ),
            committed_at: UtcTimestamp::from_unix_millis(
                i64::try_from(sequence).unwrap_or(i64::MAX),
            ),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            idempotency_key: IdempotencyKey::new(format!("record:{sequence}"))
                .unwrap_or_else(|error| panic!("{error}")),
            payload_hash: musteragent_content_ref::Sha256::digest(sequence.to_be_bytes()),
        },
        payload: LedgerPayload::DomainEvent(DomainEvent::Conversation(ConversationEvent::Archived)),
    }
}
