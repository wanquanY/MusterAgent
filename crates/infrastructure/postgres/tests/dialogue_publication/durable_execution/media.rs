use super::*;
use musteragent_runtime_artifact::*;
use musteragent_runtime_capability::{
    CapabilityOutcome, CapabilityReconciliation, OperationContext,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Provider {
    operation: musteragent_tool::BackendOperation,
    manifest: musteragent_capability::CapabilityManifest,
    source: BackendMediaSourceProof,
    outcome: AtomicUsize,
    submits: AtomicUsize,
    polls: AtomicUsize,
    fetches: AtomicUsize,
    fail_first_fetch: bool,
    content: Arc<dyn ContentStore>,
}
impl Provider {
    fn result(&self) -> BackendMediaProviderOutcome {
        let reason = SafeDisplayText::new("awaiting provider reconciliation")
            .expect("valid durable execution fixture");
        let reference = self.reconciliation_identity(
            InvocationId::from_uuid(self.source.source_id)
                .expect("valid durable execution fixture"),
        );
        match self.outcome.load(Ordering::SeqCst) {
            0 => BackendMediaProviderOutcome::Completed(self.source.clone()),
            1 => BackendMediaProviderOutcome::Active {
                source: self.source.clone(),
                phase: BackendMediaPhase::Running,
                reconciliation_ref: reference,
            },
            2 => BackendMediaProviderOutcome::Pending {
                reason,
                reconciliation_ref: reference,
            },
            3 => {
                let code = ErrorCode::CapabilityUnavailable;
                BackendMediaProviderOutcome::Failed {
                    source: self.source.clone(),
                    error: ErrorInfo {
                        code,
                        category: code.category(),
                        safe_message: SafeDisplayText::new("provider rejected generation")
                            .expect("valid durable execution fixture"),
                        retryable: false,
                        dependency_ref: None,
                        diagnostic_ref: None,
                    },
                    retry_disposition: RetryDisposition::DoNotRetry,
                }
            }
            _ => panic!("invalid test scenario"),
        }
    }
}
impl BackendMediaCapabilityProvider for Provider {
    fn operation(&self) -> &musteragent_tool::BackendOperation {
        &self.operation
    }
    fn manifest(&self) -> &musteragent_capability::CapabilityManifest {
        &self.manifest
    }
    fn reconciliation_identity(&self, id: InvocationId) -> OpaqueExternalRef {
        OpaqueExternalRef::new(format!("media:{id}")).expect("valid durable execution fixture")
    }
    fn invoke_media(
        &self,
        _: CapabilityInvocation,
    ) -> ApplicationFuture<'_, BackendMediaProviderOutcome> {
        Box::pin(async move {
            self.submits.fetch_add(1, Ordering::SeqCst);
            self.result()
        })
    }
    fn reconcile_media(
        &self,
        _: CapabilityReconciliation,
    ) -> ApplicationFuture<'_, BackendMediaProviderOutcome> {
        Box::pin(async move {
            self.polls.fetch_add(1, Ordering::SeqCst);
            self.result()
        })
    }
    fn materialize_media(
        &self,
        i: CapabilityInvocation,
        mut source: BackendMediaSourceProof,
        _: Option<OperationContext>,
    ) -> ApplicationFuture<'_, Result<BackendMediaSourceProof, MediaMaterializationError>> {
        Box::pin(async move {
            let n = self.fetches.fetch_add(1, Ordering::SeqCst);
            if self.fail_first_fetch && n == 0 {
                return Err(MediaMaterializationError::Unavailable);
            }
            if source.assets.is_empty() {
                for (ordinal, url) in source.asset_urls.iter().enumerate() {
                    source.assets.push(BackendMediaAssetProof {
                        content_ref: ContentRef {
                            content_id: media_asset_content_id(i.invocation_id, ordinal as u16),
                            media_type: MediaType::new("video/mp4")
                                .expect("valid durable execution fixture"),
                            byte_length: b"deterministic media bytes".len() as u64,
                            sha256: Sha256::digest(b"deterministic media bytes"),
                        },
                        delivery_url: Some(url.clone()),
                        content: vec![],
                        dimensions: None,
                    });
                }
            }
            for asset in &source.assets {
                let stored = self
                    .content
                    .put_at(
                        asset.content_ref.content_id,
                        PutContent {
                            tenant_id: i.tenant_id,
                            media_type: asset.content_ref.media_type.clone(),
                            bytes: b"deterministic media bytes".to_vec(),
                        },
                    )
                    .await
                    .map_err(|_| MediaMaterializationError::Unavailable)?;
                if stored != asset.content_ref {
                    return Err(MediaMaterializationError::InvalidContent);
                }
            }
            Ok(source)
        })
    }
}
fn reconcile(c: &Call, provider: &Provider) -> CapabilityReconciliation {
    CapabilityReconciliation {
        actor: c.invocation.actor,
        tenant_id: c.invocation.tenant_id,
        conversation_id: c.invocation.conversation_id,
        run_id: c.invocation.run_id,
        workspace_id: None,
        execution_attempt_id: c.attempt.attempt_id,
        execution_attempt_number: c.attempt.attempt_number,
        invocation_id: c.invocation.invocation_id,
        provider_id: c.invocation.capability_provider_id,
        idempotency_key: c.invocation.idempotency_key.clone(),
        result_schema: c.invocation.result_schema.clone(),
        reconciliation_ref: provider.reconciliation_identity(c.invocation.invocation_id),
        deadline: c.attempt.lease_expires_at,
        trace_context: c.invocation.trace_context.clone(),
        execution_grant_proof: None,
    }
}
fn provider(
    f: &Fixture,
    c: &Call,
    kind: BackendMediaSourceKind,
    outcome: u8,
    fail_first_fetch: bool,
) -> Arc<Provider> {
    let bytes = b"deterministic media bytes";
    let musteragent_tool::CapabilityExecutionTarget::BackendNamed(operation) =
        &c.material.tool.execution_target
    else {
        panic!("expected a media backend")
    };
    Arc::new(Provider {
        operation: operation.clone(),
        manifest: c.manifest.clone(),
        source: BackendMediaSourceProof {
            kind,
            source_id: c.invocation.invocation_id.into_uuid(),
            task_id: format!("job-{}", c.invocation.invocation_id),
            title: "Generated media".into(),
            prompt: "A neutral test scene".into(),
            asset_urls: vec![],
            assets: vec![BackendMediaAssetProof {
                content_ref: ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new(
                        if kind == BackendMediaSourceKind::ImageGenerationRecord {
                            "image/png"
                        } else {
                            "video/mp4"
                        },
                    )
                    .expect("valid durable execution fixture"),
                    byte_length: bytes.len() as u64,
                    sha256: Sha256::digest(bytes),
                },
                delivery_url: None,
                content: vec![],
                dimensions: Some((32, 32)),
            }],
            thumbnail_url: None,
            provider_request_ref: Some(
                OpaqueExternalRef::new("provider-job").expect("valid durable execution fixture"),
            ),
            failure_code: None,
            provider_model: Some("test-model".into()),
        },
        outcome: AtomicUsize::new(outcome as usize),
        submits: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
        fetches: AtomicUsize::new(0),
        fail_first_fetch,
        content: f.content.clone().expect("valid durable execution fixture"),
    })
}
fn handler(
    f: &Fixture,
    p: Arc<Provider>,
) -> musteragent_tool_executor::MediaArtifactCapabilityHandler<PostgresFencedArtifactToolCommitter>
{
    musteragent_tool_executor::MediaArtifactCapabilityHandler::new(
        p,
        Arc::new(musteragent_postgres::PostgresMediaGenerationJournal::new(
            f.pool.clone(),
        )),
        BuiltinArtifactTools::new(f.content.clone().expect("valid durable execution fixture")),
        Arc::new(PostgresFencedArtifactToolCommitter::new(
            f.pool.clone(),
            Arc::new(TestLedgerPayloadEncoder),
            keys(),
        )),
        Arc::new(Clock(UtcTimestamp::from_unix_millis(1000))),
    )
}
#[tokio::test]
async fn media_states_and_recoverable_materialization_are_durable() {
    let f = setup().await;
    for (outcome, kind, fail) in [
        (1, BackendMediaSourceKind::ImageGenerationRecord, false),
        (2, BackendMediaSourceKind::ImageGenerationRecord, false),
        (3, BackendMediaSourceKind::ImageGenerationRecord, false),
        (0, BackendMediaSourceKind::ImageGenerationRecord, false),
        (0, BackendMediaSourceKind::VideoGenerationRecord, true),
    ] {
        let key = if kind == BackendMediaSourceKind::ImageGenerationRecord {
            "artifact.image.create@1"
        } else {
            "artifact.video.create@1"
        };
        let mut c = call(&f, key, json!({"prompt":"A neutral test scene"})).await;
        let mut p = provider(&f, &c, kind, outcome, fail);
        if fail {
            let source = &mut Arc::get_mut(&mut p)
                .expect("valid durable execution fixture")
                .source;
            source.assets.clear();
            source.asset_urls = vec!["https://media.example.invalid/original.mp4".into()];
        }
        c.durable.prebound_reconciliation_ref =
            Some(p.reconciliation_identity(c.invocation.invocation_id));
        let h = handler(&f, p.clone());
        let out = h
            .invoke(&c.attempt, &c.material, &c.durable, c.invocation.clone())
            .await
            .expect("valid durable execution fixture");
        match outcome {
            1 | 2 => assert!(matches!(
                out,
                DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate { .. })
            )),
            3 => assert!(matches!(
                out,
                DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed { .. })
            )),
            0 => {
                if fail {
                    assert!(matches!(
                        out,
                        DispatchedCapabilityOutcome::Provider(
                            CapabilityOutcome::Indeterminate { .. }
                        )
                    ));
                } else {
                    assert_eq!(result(&f, out).await["data"]["status"], "completed");
                }
                // A new handler/store represents a worker restart. Reconciliation
                // uses recorded metadata and fetches assets without polling/submitting.
                let h = handler(&f, p.clone());
                let recovered = h
                    .reconcile(&c.attempt, &c.material, &c.durable, reconcile(&c, &p))
                    .await
                    .expect("valid durable execution fixture");
                assert!(
                    matches!(
                        recovered,
                        musteragent_tool_executor::DispatchedReconciliationOutcome::Committed(_)
                    ),
                    "{recovered:?}"
                );
                let again = h
                    .invoke(&c.attempt, &c.material, &c.durable, c.invocation.clone())
                    .await
                    .expect("valid durable execution fixture");
                assert_eq!(result(&f, again).await["artifact"]["version"], 1);
                assert_eq!(p.submits.load(Ordering::SeqCst), 1);
                assert_eq!(p.polls.load(Ordering::SeqCst), 0);
                let versions: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM artifact.artifact_versions WHERE source_invocation_id=$1",
                )
                .bind(c.invocation.invocation_id.into_uuid())
                .fetch_one(&f.pool)
                .await
                .expect("valid durable execution fixture");
                assert_eq!(versions, 1);
            }
            _ => unreachable!(),
        }
        if outcome != 0 {
            let observed = h
                .reconcile(&c.attempt, &c.material, &c.durable, reconcile(&c, &p))
                .await
                .expect("valid durable execution fixture");
            use musteragent_runtime_capability::ReconciliationOutcome as R;
            use musteragent_tool_executor::DispatchedReconciliationOutcome as D;
            assert!(matches!(
                (outcome, observed),
                (1 | 2, D::Provider(R::StillIndeterminate { .. }))
                    | (3, D::Provider(R::ProvenFailed { .. }))
            ));
            assert_eq!(p.fetches.load(Ordering::SeqCst), 0);
            if outcome == 1 || outcome == 2 {
                p.outcome.store(0, Ordering::SeqCst);
                let recovered = handler(&f, p.clone())
                    .reconcile(&c.attempt, &c.material, &c.durable, reconcile(&c, &p))
                    .await
                    .expect("valid durable execution fixture");
                assert!(matches!(recovered, D::Committed(_)), "{recovered:?}");
                assert_eq!(
                    p.submits.load(Ordering::SeqCst),
                    1,
                    "reconciliation never resubmits"
                );
            }
        }
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact.media_generation_results")
        .fetch_one(&f.pool)
        .await
        .expect("valid durable execution fixture");
    assert_eq!(count, 4);
    assert!(
        sqlx::query("DELETE FROM artifact.media_generation_results")
            .execute(&f.pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn media_journal_rejects_stale_fences_conflicts_and_foreign_queries() {
    let f = setup().await;
    let mut c = call(
        &f,
        "artifact.image.create@1",
        json!({"prompt":"A neutral scene"}),
    )
    .await;
    let p = provider(
        &f,
        &c,
        BackendMediaSourceKind::ImageGenerationRecord,
        0,
        false,
    );
    c.durable.prebound_reconciliation_ref =
        Some(p.reconciliation_identity(c.invocation.invocation_id));
    let media = handler(&f, p.clone());
    let mut substituted = c.material.clone();
    for (name, kind) in [
        ("Sample.Other", Some("image")),
        (p.operation.as_str(), Some("video")),
    ] {
        substituted.tool.execution_target =
            musteragent_tool::CapabilityExecutionTarget::parse(name, kind).unwrap();
        assert!(
            media
                .invoke(&c.attempt, &substituted, &c.durable, c.invocation.clone())
                .await
                .is_err()
        );
        assert!(
            media
                .reconcile(&c.attempt, &substituted, &c.durable, reconcile(&c, &p))
                .await
                .is_err()
        );
    }
    assert_eq!(p.submits.load(Ordering::SeqCst), 0);
    assert_eq!(p.polls.load(Ordering::SeqCst), 0);
    let journal = musteragent_postgres::PostgresMediaGenerationJournal::new(f.pool.clone());
    let fact = RecordedMediaResult {
        source: p.source.clone(),
        observed_at: now(&f).await,
    };
    let stale = c.attempt.clone();
    c.attempt = advance_fence(&f, &stale).await;
    assert_eq!(
        journal.record(&stale, &c.invocation, fact.clone()).await,
        Err(MediaJournalError::StaleFence)
    );
    assert_eq!(
        journal
            .load(&c.invocation)
            .await
            .expect("valid durable execution fixture"),
        None
    );
    assert_eq!(
        journal
            .record(&c.attempt, &c.invocation, fact.clone())
            .await
            .expect("valid durable execution fixture"),
        fact
    );
    let mut later = fact.clone();
    later.observed_at = UtcTimestamp::from_unix_millis(fact.observed_at.unix_millis() + 1);
    assert_eq!(
        journal
            .record(&c.attempt, &c.invocation, later)
            .await
            .expect("valid durable execution fixture"),
        fact,
        "first observation time is frozen"
    );
    let mut changed = fact.clone();
    changed.source.task_id = "different-job".into();
    assert_eq!(
        journal.record(&c.attempt, &c.invocation, changed).await,
        Err(MediaJournalError::Conflict)
    );
    let mut foreign = c.invocation.clone();
    foreign.actor.principal_id = PrincipalId::generate();
    assert_eq!(
        journal.load(&foreign).await,
        Err(MediaJournalError::Conflict)
    );
    let mut query = reconcile(&c, &p);
    query.invocation_id = InvocationId::generate();
    assert!(
        handler(&f, p.clone())
            .reconcile(&c.attempt, &c.material, &c.durable, query)
            .await
            .is_err()
    );
    assert_eq!(p.polls.load(Ordering::SeqCst), 0);
    assert_eq!(p.fetches.load(Ordering::SeqCst), 0);
}
