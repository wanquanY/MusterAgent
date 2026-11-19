use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::StreamExt as _;
use musteragent_conversation::ConversationLifecycle;
use musteragent_identity::{
    Actor, ConversationId, CorrelationId, DottedKey, PrincipalId, PrincipalKind, TenantId,
};
use musteragent_projection::{
    AcknowledgeConversationSeenThrough, CatalogActivityPosition, ConversationCatalogChangeV2,
    ConversationCatalogInvalidationBatchV2, ConversationCatalogInvalidationFeed,
    ConversationCatalogInvalidationSubscription, ConversationCatalogInvalidationV2,
    ConversationCatalogPageV2, ConversationCatalogSourceError,
    ConversationCatalogSourceErrorMapper, ConversationCatalogV2Application,
    ConversationCatalogV2Policy, ConversationCatalogV2Source, ConversationReadReceiptAck,
    ListConversationCatalogV2, SharedConversationCatalogV2Application,
    SubscribeConversationCatalogV2,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorDetails, RuntimeError, SafeDisplayText,
};
use tokio::sync::Notify;

#[tokio::test]
async fn committed_invalidation_hint_reconciles_before_periodic_fallback() {
    let source = Arc::new(CountingSource::default());
    let feed = Arc::new(MemoryInvalidationFeed::default());
    let service = SharedConversationCatalogV2Application::new(
        ConversationCatalogV2Policy {
            invalidation_page_size: 64,
            channel_capacity: 4,
            poll_interval: Duration::from_secs(30),
        },
        source.clone(),
        Arc::new(ErrorMapper),
    )
    .unwrap_or_else(|error| panic!("invalid policy: {error}"))
    .with_invalidation_feed(feed.clone());
    let mut stream = service
        .subscribe_conversation_catalog_v2(query())
        .await
        .unwrap_or_else(|error| panic!("subscribe failed: {error:?}"));

    assert!(matches!(
        stream.next().await.transpose(),
        Ok(Some(ConversationCatalogChangeV2::ResyncRequired { .. }))
    ));
    assert_eq!(source.reads.load(Ordering::SeqCst), 1);

    feed.notify.notify_one();
    wait_for_reads(&source, 2).await;

    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn periodic_reconciliation_remains_authoritative_without_a_hint_feed() {
    let source = Arc::new(CountingSource::default());
    let service = SharedConversationCatalogV2Application::new(
        ConversationCatalogV2Policy {
            invalidation_page_size: 64,
            channel_capacity: 4,
            poll_interval: Duration::from_millis(20),
        },
        source.clone(),
        Arc::new(ErrorMapper),
    )
    .unwrap_or_else(|error| panic!("invalid policy: {error}"));
    let mut stream = service
        .subscribe_conversation_catalog_v2(query())
        .await
        .unwrap_or_else(|error| panic!("subscribe failed: {error:?}"));

    assert!(stream.next().await.is_some());
    wait_for_reads(&source, 2).await;

    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn a_single_hint_drains_every_committed_page_without_watchdog_delay() {
    let source = Arc::new(CountingSource {
        behavior: SourceBehavior::Paged,
        ..Default::default()
    });
    let feed = Arc::new(MemoryInvalidationFeed::default());
    let service = SharedConversationCatalogV2Application::new(
        ConversationCatalogV2Policy {
            invalidation_page_size: 1,
            channel_capacity: 1,
            poll_interval: Duration::from_secs(120),
        },
        source.clone(),
        Arc::new(ErrorMapper),
    )
    .unwrap()
    .with_invalidation_feed(feed.clone());
    let mut stream = service
        .subscribe_conversation_catalog_v2(query())
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Ok(ConversationCatalogChangeV2::ResyncRequired { .. }))
    ));
    feed.notify.notify_one();
    for expected in 1..=3 {
        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap();
        assert!(
            matches!(event, Some(Ok(ConversationCatalogChangeV2::Remove { position, .. })) if position.get() == expected)
        );
    }
    assert_eq!(source.reads.load(Ordering::SeqCst), 4);
    drop(stream);
    service.shutdown().await;
}

async fn wait_for_reads(source: &CountingSource, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while source.reads.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("source did not reach {expected} reads"));
}

fn query() -> SubscribeConversationCatalogV2 {
    SubscribeConversationCatalogV2 {
        actor: Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        lifecycle: ConversationLifecycle::Active,
    }
}

#[derive(Default)]
struct CountingSource {
    reads: AtomicUsize,
    lists: AtomicUsize,
    initial_entered: Notify,
    initial_release: Notify,
    block_initial: bool,
    behavior: SourceBehavior,
}

#[derive(Default, Clone, Copy)]
enum SourceBehavior {
    #[default]
    Empty,
    Remove,
    Paged,
    ReadError,
    ListError,
}

impl ConversationCatalogV2Source for CountingSource {
    fn list(
        &self,
        _query: ListConversationCatalogV2,
    ) -> ApplicationFuture<'_, Result<ConversationCatalogPageV2, ConversationCatalogSourceError>>
    {
        Box::pin(async move {
            self.lists.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                SourceBehavior::Remove | SourceBehavior::Paged => Ok(ConversationCatalogPageV2 {
                    snapshot_position: CatalogActivityPosition::new(1),
                    conversations: Vec::new(),
                    next_cursor: None,
                }),
                SourceBehavior::ListError => Err(ConversationCatalogSourceError::Unavailable),
                _ => panic!("fixture must not list rows without invalidations"),
            }
        })
    }

    fn acknowledge(
        &self,
        _command: AcknowledgeConversationSeenThrough,
    ) -> ApplicationFuture<'_, Result<ConversationReadReceiptAck, ConversationCatalogSourceError>>
    {
        Box::pin(async { unreachable!("fixture never acknowledges receipts") })
    }

    fn read_invalidations(
        &self,
        _actor: Actor,
        after: CatalogActivityPosition,
        _limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<ConversationCatalogInvalidationBatchV2, ConversationCatalogSourceError>,
    > {
        Box::pin(async move {
            let read = self.reads.fetch_add(1, Ordering::SeqCst);
            if read == 0 && self.block_initial {
                self.initial_entered.notify_one();
                self.initial_release.notified().await;
            }
            if read > 0 && matches!(self.behavior, SourceBehavior::ReadError) {
                return Err(ConversationCatalogSourceError::Unavailable);
            }
            if matches!(self.behavior, SourceBehavior::Paged) && read > 0 {
                return Ok(ConversationCatalogInvalidationBatchV2 {
                    invalidations: vec![ConversationCatalogInvalidationV2 {
                        position: CatalogActivityPosition::new(after.get() + 1),
                        conversation_id: ConversationId::generate(),
                    }],
                    latest_position: CatalogActivityPosition::new(3),
                });
            }
            let invalidations = if read > 0
                && matches!(
                    self.behavior,
                    SourceBehavior::Remove | SourceBehavior::ListError
                ) {
                vec![ConversationCatalogInvalidationV2 {
                    position: CatalogActivityPosition::new(1),
                    conversation_id: ConversationId::generate(),
                }]
            } else {
                Vec::new()
            };
            Ok(ConversationCatalogInvalidationBatchV2 {
                invalidations,
                latest_position: CatalogActivityPosition::new(0),
            })
        })
    }
}

#[derive(Default)]
struct MemoryInvalidationFeed {
    notify: Arc<Notify>,
    wakes: Arc<AtomicUsize>,
    fail: bool,
}

impl ConversationCatalogInvalidationFeed for MemoryInvalidationFeed {
    fn subscribe(
        &self,
        _tenant_id: TenantId,
    ) -> Result<Arc<dyn ConversationCatalogInvalidationSubscription>, ConversationCatalogSourceError>
    {
        Ok(Arc::new(MemoryInvalidationSubscription {
            notify: self.notify.clone(),
            wakes: self.wakes.clone(),
            fail: self.fail,
        }))
    }
}

struct MemoryInvalidationSubscription {
    notify: Arc<Notify>,
    wakes: Arc<AtomicUsize>,
    fail: bool,
}

impl ConversationCatalogInvalidationSubscription for MemoryInvalidationSubscription {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), ConversationCatalogSourceError>> {
        Box::pin(async move {
            self.notify.notified().await;
            self.wakes.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(ConversationCatalogSourceError::Unavailable)
            } else {
                Ok(())
            }
        })
    }
}

struct ErrorMapper;

impl ConversationCatalogSourceErrorMapper for ErrorMapper {
    fn map(&self, _error: ConversationCatalogSourceError) -> RuntimeError {
        RuntimeError {
            schema_version: DottedKey::new("musteragent.runtime.error.v2")
                .unwrap_or_else(|error| panic!("{error}")),
            code: ErrorCode::DependencyUnavailable,
            category: ErrorCode::DependencyUnavailable.category(),
            message: SafeDisplayText::new("Conversation Catalog unavailable")
                .unwrap_or_else(|error| panic!("{error}")),
            retryable: true,
            retry_after_ms: Some(100),
            resource_ref: None,
            correlation_id: CorrelationId::generate(),
            details: Box::new(ErrorDetails::None),
        }
    }
}

impl musteragent_projection::WorkspaceCatalogActivitySource for CountingSource {
    fn workspace_activity(
        &self,
        _query: musteragent_projection::ListWorkspaceCatalogActivity,
    ) -> ApplicationFuture<
        '_,
        Result<
            musteragent_projection::WorkspaceCatalogActivityBatch,
            ConversationCatalogSourceError,
        >,
    > {
        Box::pin(async { unreachable!("invalidation fixture does not query workspace activity") })
    }
}

fn lifecycle_service(
    source: Arc<CountingSource>,
    feed: Arc<MemoryInvalidationFeed>,
) -> Arc<SharedConversationCatalogV2Application> {
    Arc::new(
        SharedConversationCatalogV2Application::new(
            ConversationCatalogV2Policy {
                invalidation_page_size: 64,
                channel_capacity: 1,
                poll_interval: Duration::from_secs(30),
            },
            source,
            Arc::new(ErrorMapper),
        )
        .expect("valid bounded policy")
        .with_invalidation_feed(feed),
    )
}

#[tokio::test]
async fn shutdown_releases_backpressured_data_and_each_error_delivery() {
    for (behavior, feed_fails) in [
        (SourceBehavior::Remove, false),
        (SourceBehavior::ReadError, false),
        (SourceBehavior::ListError, false),
        (SourceBehavior::Empty, true),
    ] {
        let source = Arc::new(CountingSource {
            behavior,
            ..Default::default()
        });
        let feed = Arc::new(MemoryInvalidationFeed {
            fail: feed_fails,
            ..Default::default()
        });
        let service = lifecycle_service(source.clone(), feed.clone());
        let mut stream = service
            .subscribe_conversation_catalog_v2(query())
            .await
            .expect("subscribe");
        // Keep the capacity-one initial resync unread. Every branch's next
        // message must face backpressure while the receiver remains alive.
        feed.notify.notify_one();
        if !feed_fails {
            wait_for_reads(&source, 2).await;
            if matches!(behavior, SourceBehavior::Remove | SourceBehavior::ListError) {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while source.lists.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("row read reached");
            }
        } else {
            tokio::time::timeout(Duration::from_secs(1), async {
                while feed.wakes.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("failed hint delivery reached after initial resync was queued");
        }
        tokio::time::timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("shutdown must not depend on the UI consuming the stream");
        assert!(matches!(
            stream.next().await,
            Some(Ok(ConversationCatalogChangeV2::ResyncRequired { .. }))
        ));
        assert!(stream.next().await.is_none());
        assert!(
            service
                .subscribe_conversation_catalog_v2(query())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn shutdown_joins_admitted_initial_read_and_prevents_late_subscription() {
    let source = Arc::new(CountingSource {
        block_initial: true,
        ..Default::default()
    });
    let service = lifecycle_service(source.clone(), Arc::new(MemoryInvalidationFeed::default()));
    let subscriber = service.clone();
    let subscribe =
        tokio::spawn(async move { subscriber.subscribe_conversation_catalog_v2(query()).await });
    source.initial_entered.notified().await;
    let closer = service.clone();
    let mut close = tokio::spawn(async move { closer.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut close)
            .await
            .is_err(),
        "shutdown must join an admitted source read before storage can be closed"
    );
    source.initial_release.notify_one();
    assert!(
        subscribe.await.expect("subscriber task").is_err(),
        "no successful subscription after retirement"
    );
    tokio::time::timeout(Duration::from_secs(1), close)
        .await
        .expect("drained shutdown")
        .expect("closer task");
    assert!(
        service
            .subscribe_conversation_catalog_v2(query())
            .await
            .is_err()
    );
    assert_eq!(
        source.reads.load(Ordering::SeqCst),
        1,
        "closed service must not touch source"
    );
}

#[tokio::test]
async fn list_validates_and_trims_title_query_before_forwarding_to_source() {
    struct CapturingListSource {
        captured: std::sync::Mutex<Vec<ListConversationCatalogV2>>,
    }

    impl ConversationCatalogV2Source for CapturingListSource {
        fn list(
            &self,
            query: ListConversationCatalogV2,
        ) -> ApplicationFuture<'_, Result<ConversationCatalogPageV2, ConversationCatalogSourceError>>
        {
            self.captured.lock().expect("captured lock").push(query);
            Box::pin(async {
                Ok(ConversationCatalogPageV2 {
                    snapshot_position: CatalogActivityPosition::ZERO,
                    conversations: Vec::new(),
                    next_cursor: None,
                })
            })
        }

        fn acknowledge(
            &self,
            _command: AcknowledgeConversationSeenThrough,
        ) -> ApplicationFuture<'_, Result<ConversationReadReceiptAck, ConversationCatalogSourceError>>
        {
            Box::pin(async { unreachable!() })
        }

        fn read_invalidations(
            &self,
            _actor: Actor,
            _after: CatalogActivityPosition,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<ConversationCatalogInvalidationBatchV2, ConversationCatalogSourceError>,
        > {
            Box::pin(async { unreachable!() })
        }
    }

    impl musteragent_projection::WorkspaceCatalogActivitySource for CapturingListSource {
        fn workspace_activity(
            &self,
            _query: musteragent_projection::ListWorkspaceCatalogActivity,
        ) -> ApplicationFuture<
            '_,
            Result<
                musteragent_projection::WorkspaceCatalogActivityBatch,
                ConversationCatalogSourceError,
            >,
        > {
            Box::pin(async { unreachable!() })
        }
    }

    let source = Arc::new(CapturingListSource {
        captured: std::sync::Mutex::new(Vec::new()),
    });
    let service = SharedConversationCatalogV2Application::new(
        ConversationCatalogV2Policy {
            invalidation_page_size: 64,
            channel_capacity: 4,
            poll_interval: Duration::from_secs(30),
        },
        source.clone(),
        Arc::new(ErrorMapper),
    )
    .expect("valid policy");
    let actor = query().actor;

    for invalid in ["", "   \t  ", &"x".repeat(101)] {
        assert!(
            service
                .list_conversation_catalog_v2(
                    ListConversationCatalogV2::new(actor, ConversationLifecycle::Active, 10)
                        .with_title_query(Some(invalid)),
                )
                .await
                .is_err()
        );
    }
    assert!(
        source.captured.lock().expect("captured lock").is_empty(),
        "invalid title queries must be rejected before touching the persistence source"
    );

    service
        .list_conversation_catalog_v2(
            ListConversationCatalogV2::new(actor, ConversationLifecycle::Active, 10)
                .with_workspace_filter(musteragent_projection::CatalogWorkspaceFilter::Unassigned)
                .with_title_query(Some("  Budget 100%  ")),
        )
        .await
        .expect("trimmed query succeeds");
    let captured = source.captured.lock().expect("captured lock");
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].workspace_filter,
        musteragent_projection::CatalogWorkspaceFilter::Unassigned
    );
    assert_eq!(captured[0].title_query.as_deref(), Some("Budget 100%"));
}
