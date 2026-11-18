use super::*;

fn recovery_keyring(
    active: &str,
    keys: impl IntoIterator<Item = (&'static str, [u8; 32])>,
) -> Arc<ProtectedContentKeyring> {
    Arc::new(
        ProtectedContentKeyring::new(
            DottedKey::new(active).expect("active key ID"),
            keys.into_iter()
                .map(|(key_id, key)| (DottedKey::new(key_id).expect("key ID"), key)),
        )
        .expect("recovery keyring"),
    )
}

pub(super) async fn run_completion_intent_key_rotation_requires_the_referenced_generation() {
    for retain_old_key in [true, false] {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start PostgreSQL");
        let pool = postgres.pool().await.expect("connect PostgreSQL");
        migrate(&pool).await.expect("migrate");
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let provider = Arc::new(SingleCompletionProvider::new());
        let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
        let old_keys =
            recovery_keyring("runtime.recovery.old", [("runtime.recovery.old", [41; 32])]);
        let (first, _) = build_loop_with_owners(
            &pool,
            fixture,
            provider.clone(),
            FaultMode::FinishUnavailable,
            Arc::new(NoTools),
            blobs.clone(),
            old_keys,
        )
        .await;
        assert_eq!(
            first
                .execute(
                    attempt.clone(),
                    attempt_telemetry(fixture, &attempt),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopExit::Interrupted {
                cause: musteragent_loop::CancellationCause::SystemShutdown,
            }
        );
        let rotated = if retain_old_key {
            recovery_keyring(
                "runtime.recovery.next",
                [
                    ("runtime.recovery.old", [41; 32]),
                    ("runtime.recovery.next", [42; 32]),
                ],
            )
        } else {
            recovery_keyring(
                "runtime.recovery.next",
                [("runtime.recovery.next", [42; 32])],
            )
        };
        let (replacement, _) = build_loop_with_owners(
            &pool,
            fixture,
            provider.clone(),
            FaultMode::None,
            Arc::new(NoTools),
            blobs,
            rotated,
        )
        .await;
        let exit = replacement
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await;
        if retain_old_key {
            assert!(matches!(exit, AgentLoopExit::Completed { .. }));
        } else {
            assert_eq!(exit, AgentLoopExit::AlreadyTerminal);
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let durable: (i16, i16, i64, i64, i16, i16) = sqlx::query_as(
            "SELECT invocation.status, run.status,
                    (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                    (SELECT count(*) FROM event_store.outbox
                       WHERE topic = 'musteragent.model-usage.v1'),
                    intent.lifecycle, content.state
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run USING (run_id)
             JOIN runtime.model_completion_intents AS intent USING (invocation_id)
             JOIN artifact.content_refs AS content USING (content_id)
             WHERE invocation.run_id = $1",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read key rotation outcome");
        assert_eq!(
            durable,
            if retain_old_key {
                (2, 5, 1, 1, 5, 3)
            } else {
                (3, 6, 1, 1, 5, 3)
            }
        );
        pool.close().await;
    }
}

#[derive(Debug, Copy, Clone)]
enum IntentAttack {
    MetadataTamper,
    CrossTenantRef,
    CrossInvocationCiphertext,
}

pub(super) async fn run_intent_tamper_and_tenant_or_invocation_transplant_fail_closed() {
    for attack in [
        IntentAttack::MetadataTamper,
        IntentAttack::CrossTenantRef,
        IntentAttack::CrossInvocationCiphertext,
    ] {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start PostgreSQL");
        let pool = postgres.pool().await.expect("connect PostgreSQL");
        migrate(&pool).await.expect("migrate");
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let provider = Arc::new(SingleCompletionProvider::new());
        let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
        let (first, _) = build_loop_with_storage(
            &pool,
            fixture,
            provider.clone(),
            FaultMode::FinishUnavailable,
            Arc::new(NoTools),
            blobs.clone(),
        )
        .await;
        assert_eq!(
            first
                .execute(
                    attempt.clone(),
                    attempt_telemetry(fixture, &attempt),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopExit::Interrupted {
                cause: musteragent_loop::CancellationCause::SystemShutdown,
            }
        );
        let invocation_uuid: uuid::Uuid = sqlx::query_scalar(
            "SELECT invocation_id FROM runtime.model_invocations WHERE run_id = $1",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read invocation ID");
        let invocation_id = InvocationId::from_uuid(invocation_uuid).expect("invocation ID");
        let keys = keyring();
        let content = PostgresContentStore::new(pool.clone(), blobs.clone(), keys);
        match attack {
            IntentAttack::MetadataTamper => {
                sqlx::query(
                    "UPDATE runtime.model_completion_intents
                     SET key_version = 'runtime.recovery.tampered'
                     WHERE invocation_id = $1",
                )
                .bind(invocation_uuid)
                .execute(&pool)
                .await
                .expect("tamper key metadata");
            }
            IntentAttack::CrossTenantRef => {
                let foreign = content
                    .put(PutContent {
                        tenant_id: TenantId::generate(),
                        media_type: MediaType::new(
                            "application/vnd.musteragent.legacy.model-completion-intent-v3+encrypted",
                        )
                        .expect("media type"),
                        bytes: b"foreign-tenant-ciphertext".to_vec(),
                    })
                    .await
                    .expect("store foreign-tenant transplant");
                sqlx::query(
                    "UPDATE runtime.model_completion_intents
                     SET content_id = $2, media_type = $3, content_hash = $4,
                         content_bytes = $5::numeric
                     WHERE invocation_id = $1",
                )
                .bind(invocation_uuid)
                .bind(foreign.content_id.into_uuid())
                .bind(foreign.media_type.as_str())
                .bind(foreign.sha256.into_bytes().as_slice())
                .bind(foreign.byte_length.to_string())
                .execute(&pool)
                .await
                .expect("transplant foreign tenant ref");
                sqlx::query(
                    "UPDATE runtime.model_invocations
                     SET completion_intent_content_id = $2 WHERE invocation_id = $1",
                )
                .bind(invocation_uuid)
                .bind(foreign.content_id.into_uuid())
                .execute(&pool)
                .await
                .expect("link foreign tenant ref");
            }
            IntentAttack::CrossInvocationCiphertext => {
                let plaintext = b"foreign invocation completion intent";
                let plaintext_hash = Sha256::digest(plaintext);
                let foreign_invocation = InvocationId::generate();
                let context = format!(
                    "musteragent.legacy.model-completion-intent.v2:tenant={}:run={}:invocation={foreign_invocation}:schema=3:hash={plaintext_hash}:bytes={}",
                    fixture.tenant_id,
                    fixture.run_id,
                    plaintext.len(),
                );
                let recovery_keys = running_loop::recovery_keyring();
                let protected = recovery_keys
                    .seal(context.as_bytes(), plaintext)
                    .expect("seal foreign invocation intent");
                let foreign = content
                    .put(PutContent {
                        tenant_id: fixture.tenant_id,
                        media_type: MediaType::new(
                            "application/vnd.musteragent.legacy.model-completion-intent-v3+encrypted",
                        )
                        .expect("media type"),
                        bytes: protected.as_bytes().to_vec(),
                    })
                    .await
                    .expect("store foreign invocation ciphertext");
                sqlx::query(
                    "UPDATE runtime.model_completion_intents
                     SET content_id = $2, media_type = $3, content_hash = $4,
                         content_bytes = $5::numeric, plaintext_hash = $6,
                         plaintext_bytes = $7::numeric, key_version = $8
                     WHERE invocation_id = $1",
                )
                .bind(invocation_uuid)
                .bind(foreign.content_id.into_uuid())
                .bind(foreign.media_type.as_str())
                .bind(foreign.sha256.into_bytes().as_slice())
                .bind(foreign.byte_length.to_string())
                .bind(plaintext_hash.into_bytes().as_slice())
                .bind(plaintext.len().to_string())
                .bind(recovery_keys.active_key_id().as_str())
                .execute(&pool)
                .await
                .expect("transplant foreign invocation ciphertext");
                sqlx::query(
                    "UPDATE runtime.model_invocations
                     SET completion_intent_content_id = $2 WHERE invocation_id = $1",
                )
                .bind(invocation_uuid)
                .bind(foreign.content_id.into_uuid())
                .execute(&pool)
                .await
                .expect("link foreign invocation ciphertext");
            }
        }

        let (replacement, _) = build_loop_with_storage(
            &pool,
            fixture,
            provider.clone(),
            FaultMode::None,
            Arc::new(NoTools),
            blobs,
        )
        .await;
        assert_eq!(
            replacement
                .execute(
                    attempt.clone(),
                    attempt_telemetry(fixture, &attempt),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopExit::AlreadyTerminal,
            "{attack:?}"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1, "{attack:?}");
        let terminal: (i16, i16, i64, i64, i16) = sqlx::query_as(
            "SELECT invocation.status, run.status,
                    (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                    (SELECT count(*) FROM event_store.outbox
                       WHERE topic = 'musteragent.model-usage.v1'),
                    intent.lifecycle
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run USING (run_id)
             JOIN runtime.model_completion_intents AS intent USING (invocation_id)
             WHERE invocation.invocation_id = $2",
        )
        .bind(fixture.run_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read fail-closed transplant state");
        assert_eq!(
            (terminal.0, terminal.1, terminal.2, terminal.3),
            (3, 6, 1, 1)
        );
        assert_eq!(
            terminal.4,
            if matches!(attack, IntentAttack::CrossTenantRef) {
                4
            } else {
                5
            },
            "{attack:?}"
        );
        pool.close().await;
    }
}

pub(super) async fn run_successful_materialization_unlinks_intent_before_retryable_crypto_erasure()
{
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(SingleCompletionProvider::new());
    let gated = Arc::new(DeleteGateBlobStore::new());
    let blobs: Arc<dyn BlobStore> = gated.clone();
    let (runtime, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert!(matches!(
        runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let pending: (uuid::Uuid, bool, bool, i16, i16, bool) = sqlx::query_as(
        "SELECT invocation.invocation_id,
                invocation.completion_intent_content_id IS NULL,
                invocation.completion_intent_protected IS NULL,
                intent.lifecycle, content.state,
                content.storage_ref_protected IS NOT NULL
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_completion_intents AS intent USING (invocation_id)
         JOIN artifact.content_refs AS content USING (content_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read pending crypto erasure");
    assert_eq!(
        (pending.1, pending.2, pending.3, pending.4, pending.5),
        (true, true, 4, 3, true)
    );

    gated.unavailable.store(false, Ordering::SeqCst);
    let keys = keyring();
    let content: Arc<dyn ContentStore> =
        Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys.clone()));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresAgentLoopJournal::new(
        pool.clone(),
        content,
        codec.clone(),
        codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
    );
    let invocation_id = InvocationId::from_uuid(pending.0).expect("invocation ID");
    sqlx::query(
        "UPDATE runtime.model_completion_intents
         SET erase_next_attempt_at_ms = 0 WHERE invocation_id = $1",
    )
    .bind(pending.0)
    .execute(&pool)
    .await
    .expect("expire retry backoff");
    let cleanup_cancel = tokio_util::sync::CancellationToken::new();
    let cleanup_owner = uuid::Uuid::now_v7();
    assert_eq!(
        journal
            .purge_pending_completion_intents(cleanup_owner, 128, &cleanup_cancel)
            .await
            .expect("replacement owner drains the erasure queue"),
        1
    );
    assert_eq!(
        journal
            .purge_pending_completion_intents(cleanup_owner, 128, &cleanup_cancel)
            .await
            .expect("empty erasure queue is idempotent"),
        0
    );
    assert!(
        journal
            .purge_completion_intent(invocation_id)
            .await
            .expect("idempotent crypto erasure")
    );
    let erased: (i16, i16, bool, bool) = sqlx::query_as(
        "SELECT intent.lifecycle, content.state,
                content.storage_ref IS NULL,
                content.storage_ref_protected IS NULL
         FROM runtime.model_completion_intents AS intent
         JOIN artifact.content_refs AS content USING (content_id)
         WHERE intent.invocation_id = $1",
    )
    .bind(pending.0)
    .fetch_one(&pool)
    .await
    .expect("read crypto-erased intent");
    assert_eq!(erased, (5, 3, true, true));
    pool.close().await;
}

pub(super) async fn run_unavailable_blob_staging_is_reconciled_without_orphan_or_redispatch() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let gated = Arc::new(DeleteGateBlobStore::staging_fault());
    let provider = Arc::new(SingleCompletionProvider {
        calls: AtomicUsize::new(0),
        staging_fault: Some(gated.clone()),
    });
    let blobs: Arc<dyn BlobStore> = gated.clone();
    let (runtime, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert_eq!(
        runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let staged: (i16, i16, bool, i16, bool) = sqlx::query_as(
        "SELECT invocation.status, intent.lifecycle,
                invocation.completion_intent_content_id IS NULL,
                content.state, content.storage_ref_protected IS NULL
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_completion_intents AS intent USING (invocation_id)
         JOIN artifact.content_refs AS content USING (content_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read staged intent orphan candidate");
    assert_eq!(staged, (1, 1, true, 1, true));

    gated.put_unavailable.store(false, Ordering::SeqCst);
    let (replacement, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert_eq!(
        replacement
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::AlreadyTerminal
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let keys = keyring();
    let cleanup_content: Arc<dyn ContentStore> =
        Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys.clone()));
    let cleanup_codec = Arc::new(ProtobufLedgerPayloadCodec);
    let cleanup_journal = PostgresAgentLoopJournal::new(
        pool.clone(),
        cleanup_content,
        cleanup_codec.clone(),
        cleanup_codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
    );
    let drained = cleanup_journal
        .purge_pending_completion_intents(
            uuid::Uuid::now_v7(),
            1,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("drain staged orphan");
    assert!(
        drained <= 1,
        "terminal fast-path may already have erased it"
    );
    let erased: (i16, i16, i16, i16, bool, bool, i64, i64) = sqlx::query_as(
        "SELECT invocation.status, run.status, intent.lifecycle, content.state,
                content.storage_ref IS NULL,
                content.storage_ref_protected IS NULL,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                (SELECT count(*) FROM event_store.outbox
                   WHERE topic = 'musteragent.model-usage.v1')
         FROM runtime.model_invocations AS invocation
         JOIN runtime.runs AS run USING (run_id)
         JOIN runtime.model_completion_intents AS intent USING (invocation_id)
         JOIN artifact.content_refs AS content USING (content_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read reconciled staged intent");
    assert_eq!(erased, (3, 6, 5, 3, true, true, 1, 1));
    pool.close().await;
}

pub(super) async fn run_missing_exact_terminal_intent_fails_closed_without_provider_redispatch() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(ToolThenFinalProvider::new());
    let (first_runtime, _) = build_loop_with_exposed_tools(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::StageUnavailable,
        Arc::new(NoTools),
        Arc::new(MemoryBlobs::in_memory()),
        running_loop::recovery_keyring(),
        vec![exposed_tool_definition("exact_recovery_tool")],
    )
    .await;

    assert_eq!(
        first_runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let unconfirmed: (i16, Option<i16>, bool, i64) = sqlx::query_as(
        "SELECT status, terminal_stop_reason,
                completion_intent_content_id IS NOT NULL,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1)
         FROM runtime.model_invocations WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read terminal without intent");
    assert_eq!(unconfirmed, (1, Some(2), false, 0));

    let (replacement, _) = build_loop(&pool, fixture, provider.clone(), FaultMode::None).await;
    assert_eq!(
        replacement
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::AlreadyTerminal
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let terminal: (i16, i16, i16, i64, i64) = sqlx::query_as(
        "SELECT invocation.status, run.status, checkpoint.phase,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                (SELECT count(*) FROM event_store.outbox
                  WHERE topic = 'musteragent.model-usage.v1')
         FROM runtime.model_invocations AS invocation
         JOIN runtime.runs AS run USING (run_id)
         JOIN runtime.loop_checkpoints AS checkpoint USING (run_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read fail-closed terminal");
    assert_eq!(terminal, (3, 6, 6, 1, 1));
    pool.close().await;
}
