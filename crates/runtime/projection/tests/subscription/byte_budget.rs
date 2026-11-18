use super::*;

fn budgeted(fixture: &Fixture, pending_bytes: usize) -> LedgerSubscriptionService {
    LedgerSubscriptionService::new(
        SubscriptionPolicy {
            page_size: 128,
            channel_capacity: 128,
            page_byte_limit: 4_096,
            pending_byte_limit: pending_bytes + 4_096,
            poll_interval: Duration::from_secs(1),
        },
        fixture.source.clone(),
        fixture.changes.clone(),
        Arc::new(ErrorMapper),
    )
    .expect("valid byte policy")
}

async fn calls(source: &MemorySource, minimum: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while source.calls.load(Ordering::SeqCst) < minimum {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source query started");
}

#[tokio::test]
async fn byte_budget_stops_prefetch_before_channel_count_and_releases_on_last_record() {
    let fixture = Fixture::new(100);
    let service = budgeted(&fixture, 8_192);
    let mut stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("stream");
    calls(&fixture.source, 2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(fixture.source.calls.load(Ordering::SeqCst), 2);
    let first = stream.next().await.expect("first").expect("record");
    assert_eq!(first.metadata.conversation_seq.get(), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        fixture.source.calls.load(Ordering::SeqCst),
        2,
        "half-consumed page retains its budget"
    );
    assert_eq!(
        stream
            .next()
            .await
            .expect("second")
            .expect("record")
            .metadata
            .conversation_seq
            .get(),
        2
    );
    calls(&fixture.source, 3).await;
    for expected in 3..=100 {
        assert_eq!(
            stream
                .next()
                .await
                .expect("ordered record")
                .expect("record")
                .metadata
                .conversation_seq
                .get(),
            expected
        );
    }
    drop(stream);
    service.shutdown().await;
}

#[tokio::test]
async fn subscribers_share_one_budget_and_dropping_one_unblocks_the_other() {
    let fixture = Fixture::new(100);
    let service = budgeted(&fixture, 4_096);
    let first = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("first");
    calls(&fixture.source, 1).await;
    let mut second = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("second");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        fixture.source.calls.load(Ordering::SeqCst),
        1,
        "no second source allocation without a reservation"
    );
    drop(first);
    calls(&fixture.source, 2).await;
    assert_eq!(
        second
            .next()
            .await
            .expect("record")
            .expect("valid")
            .metadata
            .conversation_seq
            .get(),
        1
    );
    drop(second);
    service.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_budget_wait_without_requiring_receiver_drop() {
    let fixture = Fixture::new(100);
    let service = budgeted(&fixture, 4_096);
    let first = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("first");
    calls(&fixture.source, 1).await;
    let second = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("second");
    tokio::time::timeout(Duration::from_secs(1), service.shutdown())
        .await
        .expect("shutdown drains producer tasks");
    drop((first, second));
}

#[tokio::test]
async fn bounded_list_accepts_short_nonempty_pages_without_fabricating_a_cursor() {
    let fixture = Fixture::new(5);
    let service = budgeted(&fixture, 4_096);
    let query = fixture.query();
    let first = service
        .list_conversation_records(ListConversationRecords {
            actor: query.actor,
            conversation_id: query.conversation_id,
            cursor: query.cursor,
            limit: 128,
        })
        .await
        .expect("short page");
    assert_eq!(first.records.len(), 2);
    assert!(first.has_more);
    assert_eq!(first.next_cursor.after_conversation_seq.get(), 2);
    assert_eq!(first.next_cursor, first.projection_checkpoint);
    service.shutdown().await;
}

#[test]
fn invalid_budgets_are_rejected_before_service_admission() {
    for (page, pending) in [
        (0, 4_096),
        (1_023, 4_096),
        (4_096, 2_048),
        (4_096, 4_096),
        (4_096, usize::MAX),
    ] {
        assert!(
            SubscriptionPolicy {
                page_size: 128,
                channel_capacity: 128,
                page_byte_limit: page,
                pending_byte_limit: pending,
                poll_interval: Duration::from_secs(1),
            }
            .validate()
            .is_err()
        );
    }
}

#[tokio::test]
async fn full_tail_cannot_starve_a_short_page_query_or_hold_its_owner_lock() {
    let fixture = Fixture::new(100);
    let service = budgeted(&fixture, 4_096);
    let stream = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("stream");
    calls(&fixture.source, 1).await;
    let query = fixture.query();
    let list = service.list_conversation_records(ListConversationRecords {
        actor: query.actor,
        conversation_id: query.conversation_id,
        cursor: query.cursor,
        limit: 128,
    });
    let page = tokio::time::timeout(Duration::from_secs(1), list)
        .await
        .expect("reserved query page cannot depend on draining the tail")
        .expect("page");
    assert_eq!(page.records.len(), 2);
    service.shutdown().await;
    drop(stream);
}

#[tokio::test]
async fn reservation_covers_source_io_before_any_record_is_queued() {
    let fixture = Fixture::new(0);
    let source = Arc::new(NeverCompletesSource::default());
    let service = LedgerSubscriptionService::new(
        SubscriptionPolicy {
            page_size: 128,
            channel_capacity: 128,
            page_byte_limit: 4_096,
            pending_byte_limit: 8_192,
            poll_interval: Duration::from_secs(1),
        },
        source.clone(),
        fixture.changes.clone(),
        Arc::new(ErrorMapper),
    )
    .expect("service");
    let first = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("first");
    tokio::time::timeout(Duration::from_secs(1), async {
        while source.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first query started");
    let second = service
        .subscribe_conversation_records(fixture.query())
        .await
        .expect("second");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(1), service.shutdown())
        .await
        .expect("cancels in-flight query and budget waiter");
    drop((first, second));
}

#[tokio::test]
async fn shutdown_drains_the_query_lane_and_its_waiter() {
    let fixture = Fixture::new(0);
    let source = Arc::new(NeverCompletesSource::default());
    let service = LedgerSubscriptionService::new(
        SubscriptionPolicy {
            page_size: 128,
            channel_capacity: 128,
            page_byte_limit: 4_096,
            pending_byte_limit: 8_192,
            poll_interval: Duration::from_secs(1),
        },
        source.clone(),
        fixture.changes.clone(),
        Arc::new(ErrorMapper),
    )
    .expect("service");
    let query = fixture.query();
    let request = ListConversationRecords {
        actor: query.actor,
        conversation_id: query.conversation_id,
        cursor: query.cursor,
        limit: 128,
    };
    let first = service.list_conversation_records(request);
    let second = service.list_conversation_records(request);
    tokio::pin!(first, second);
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        1,
        "one reserved query page"
    );
    let (_, first, second) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(service.shutdown(), first, second)
    })
    .await
    .expect("shutdown retires both admitted queries");
    assert!(first.is_err() && second.is_err());
}
