pub(super) use super::execution_liveness_support as support;
use super::*;
use musteragent_conformance::fixtures::liveness_admission as admission;
use musteragent_execution_liveness::{
    Closure, ExecutionRecord, FencedExecutionLivenessJournal, Phase,
};
use musteragent_postgres::PostgresFencedExecutionLivenessJournal;
use musteragent_runtime_contract::execution_liveness::ExecutionScope as LivenessScope;
use musteragent_wire::ProtobufSnapshotCodec;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Handoff {
    Original,
    Successor,
    Cancel,
    ReadyShutdown,
    ReadyUserStop,
    ReadyUserStopping,
    ReadyMissingTerminal,
    SemanticRejected,
    SemanticUnknown,
    SemanticCancelled,
}

pub(super) async fn semantic_rejection_cancelled() {
    Box::pin(scenario(false, false, Handoff::SemanticCancelled)).await;
}
pub(super) async fn semantic_rejection_terminal() {
    Box::pin(scenario(false, false, Handoff::SemanticRejected)).await;
}
pub(super) async fn semantic_rejection_unknown() {
    Box::pin(scenario(false, false, Handoff::SemanticUnknown)).await;
}

pub(super) async fn run() {
    scenario(false, false, Handoff::Cancel).await;
    scenario(false, false, Handoff::Original).await;
    scenario(false, false, Handoff::Successor).await;
    scenario(false, true, Handoff::Original).await;
}
pub(super) async fn ready_shutdown() {
    scenario(false, false, Handoff::ReadyShutdown).await;
}
pub(super) async fn ready_stopping_denied() {
    scenario(false, false, Handoff::ReadyUserStopping).await;
}
pub(super) async fn ready_stop_denied() {
    scenario(false, false, Handoff::ReadyUserStop).await;
}
pub(super) async fn ready_missing_terminal() {
    scenario(false, false, Handoff::ReadyMissingTerminal).await;
}
pub(super) async fn confirmed_cancellation() {
    scenario(true, false, Handoff::Original).await;
}

async fn scenario(cancel: bool, restart: bool, handoff: Handoff) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, mut owner) = seed_running_loop(&pool).await;
    let initial = support::record(fixture.tenant_id, &owner);
    let clock = Arc::new(support::ManualHomeClock::new(support::clock(&initial, 0)));
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let content: Arc<dyn ContentStore> = content;
    let (snapshot, _, _) = admission::fixture(
        content.clone(),
        run_context(fixture),
        &owner,
        Arc::new(initial.policy().clone()),
    )
    .await;
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec())
        .bind(&snapshot.encoded_content).bind(fixture.runtime_snapshot_id.into_uuid()).execute(&pool).await.expect("full Snapshot");
    let bare = super::support::bare_loop_journal(
        &pool,
        content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keyring(),
        running_loop::recovery_keyring(),
    );
    let journal = audited_loop_journal(
        &pool,
        content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keyring(),
        running_loop::recovery_keyring(),
    )
    .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), clock.clone());
    let liveness = PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        clock.clone(),
    );

    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"canonical model liveness fixture".to_vec(),
        })
        .await
        .expect("request");
    let mut iteration = model_iteration(fixture, owner.clone(), request_ref, 1);
    iteration.request.invocation.timing =
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(
            musteragent_runtime_contract::execution_liveness::ActivityExecutionTiming {
                policy: snapshot
                    .content
                    .execution_policy
                    .liveness
                    .expect("Run profile")
                    .model_policy()
                    .reference()
                    .clone(),
                business_deadline: None,
            },
        );
    freeze_request(content.as_ref(), &mut iteration).await;
    let scope = LivenessScope::Invocation(iteration.invocation_id);
    let baseline = count(&pool).await;
    let mut invalid_timing = iteration.clone();
    invalid_timing.request.invocation.timing =
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(1000),
        };
    freeze_request(content.as_ref(), &mut invalid_timing).await;
    assert!(
        journal
            .begin_model(
                &owner,
                &invalid_timing,
                ProviderRecoveryCapability::NonResumable
            )
            .await
            .is_err(),
        "v14 rejects legacy model timing"
    );
    invalid_timing = iteration.clone();
    if let musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(
        timing,
    ) = &mut invalid_timing.request.invocation.timing
    {
        timing.policy.digest = Sha256::digest(b"different policy");
    }
    freeze_request(content.as_ref(), &mut invalid_timing).await;
    assert!(
        journal
            .begin_model(
                &owner,
                &invalid_timing,
                ProviderRecoveryCapability::NonResumable
            )
            .await
            .is_err(),
        "exact content cannot substitute the sealed model policy"
    );
    invalid_timing = iteration.clone();
    invalid_timing
        .request
        .instructions
        .push(ModelText::new("unsealed change").expect("text"));
    assert!(
        journal
            .begin_model(
                &owner,
                &invalid_timing,
                ProviderRecoveryCapability::NonResumable
            )
            .await
            .is_err(),
        "typed request must match original v4 content"
    );
    assert_eq!(count(&pool).await, baseline);

    assert!(
        bare.begin_model(&owner, &iteration, ProviderRecoveryCapability::NonResumable)
            .await
            .is_err(),
        "v14 cannot start without original Home supervision"
    );
    sqlx::query("CREATE FUNCTION fail_model_liveness() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected liveness failure'; END $$").execute(&pool).await.expect("fault");
    sqlx::query("CREATE TRIGGER fail_liveness_insert BEFORE INSERT ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION fail_model_liveness()").execute(&pool).await.expect("fault");
    assert!(
        journal
            .begin_model(&owner, &iteration, ProviderRecoveryCapability::NonResumable)
            .await
            .is_err()
    );
    assert_eq!(
        count(&pool).await,
        baseline,
        "model and phase facts roll back together"
    );
    assert!(liveness.load(&owner, scope).await.expect("read").is_none());
    assert_eq!(
        journal
            .load_checkpoint(&owner)
            .await
            .expect("checkpoint")
            .phase,
        LoopPhase::ReadyForModel
    );
    sqlx::query("DROP TRIGGER fail_liveness_insert ON runtime.execution_liveness")
        .execute(&pool)
        .await
        .expect("remove fault");
    journal
        .begin_model(&owner, &iteration, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("atomic begin");
    let started = liveness
        .load(&owner, scope)
        .await
        .expect("read")
        .expect("starting");
    assert_eq!(started.revision(), 1);
    assert_eq!(count(&pool).await, baseline + 2, "original start and phase");

    clock.set(support::clock(&initial, 5));
    journal
        .begin_model(&owner, &iteration, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("exact begin replay");
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(started.clone())
    );
    assert_eq!(count(&pool).await, baseline + 2);
    let mut changed_begin = iteration.clone();
    changed_begin.assistant_output_id = OutputId::generate();
    assert!(
        journal
            .begin_model(
                &owner,
                &changed_begin,
                ProviderRecoveryCapability::NonResumable
            )
            .await
            .is_err()
    );
    changed_begin = iteration.clone();
    changed_begin
        .request
        .instructions
        .push(ModelText::new("changed original instruction").expect("text"));
    assert!(
        journal
            .begin_model(
                &owner,
                &changed_begin,
                ProviderRecoveryCapability::NonResumable
            )
            .await
            .is_err()
    );
    assert!(
        journal
            .begin_model(
                &owner,
                &iteration,
                ProviderRecoveryCapability::IdempotentRequest
            )
            .await
            .is_err()
    );
    journal
        .mark_model_dispatched(&owner, iteration.invocation_id)
        .await
        .expect("dispatch");
    if handoff == Handoff::SemanticUnknown {
        super::model_rejection::unknown(
            super::model_rejection::ModelJournals {
                pool: &pool,
                journal: &journal,
                live: &liveness,
            },
            &owner,
            &iteration,
            scope,
            &clock,
            &initial,
        )
        .await;
        return;
    }
    clock.set(support::clock(&initial, 10));
    // Trusted native completion can arrive while the original canonical batch
    // is queued. Its fixed drain remains eligible for that batch and terminal.
    let accepted = liveness
        .mutate(
            &owner,
            scope,
            started.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::Started {
                    observed_at: support::clock(&initial, 10).monotonic,
                },
            ),
        )
        .await
        .expect("accepted")
        .next;
    let draining = liveness
        .mutate(
            &owner,
            scope,
            accepted.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::ExecutionEnded {
                    observed_at: support::clock(&initial, 10).monotonic,
                },
            ),
        )
        .await
        .expect("native end")
        .next;

    let usage = musteragent_runtime_model::ModelUsage {
        input_tokens_uncached: 20,
        total_tokens: Some(20),
        ..Default::default()
    };
    journal
        .append_model_events(
            &owner,
            ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: 1,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("liveness-model"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::Usage(usage),
                    ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::Stop,
                    },
                ],
            },
        )
        .await
        .expect("canonical terminal batch");
    if matches!(
        handoff,
        Handoff::SemanticRejected | Handoff::SemanticCancelled
    ) {
        super::model_rejection::terminal(
            super::model_rejection::ModelJournals {
                pool: &pool,
                journal: &journal,
                live: &liveness,
            },
            &owner,
            &iteration,
            scope,
            usage,
            handoff == Handoff::SemanticCancelled,
        )
        .await;
        return;
    }
    let completion = ModelCompletionCommit {
        invocation_id: iteration.invocation_id,
        kind: ModelCompletionKind::EmptyResponse,
        tool_calls: vec![],
        reasoning_replay: None,
        usage: Some(usage),
        provider_response_ref: provider_ref("liveness-model"),
        response_model_key: response_model_key(),
        continuation_ref: None,
    };
    let before_seal = count(&pool).await;
    sqlx::query("CREATE TRIGGER fail_result_seal BEFORE UPDATE ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION fail_model_liveness()").execute(&pool).await.expect("fault");
    assert!(
        journal
            .stage_model_completion(&owner, completion.clone())
            .await
            .is_err()
    );
    assert_eq!(
        count(&pool).await,
        before_seal,
        "result and closure roll back together"
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(draining)
    );
    sqlx::query("DROP TRIGGER fail_result_seal ON runtime.execution_liveness")
        .execute(&pool)
        .await
        .expect("clear fault");
    if matches!(
        handoff,
        Handoff::ReadyShutdown
            | Handoff::ReadyUserStop
            | Handoff::ReadyUserStopping
            | Handoff::ReadyMissingTerminal
    ) {
        use musteragent_execution_liveness::{ControlStop, Event, RecordMutation};
        let state = liveness.load(&owner, scope).await.unwrap().unwrap();
        clock.set(support::clock(&initial, 30));
        let reason = if matches!(handoff, Handoff::ReadyUserStop | Handoff::ReadyUserStopping) {
            ControlStop::UserCancelled
        } else {
            ControlStop::SystemShutdown
        };
        let stopping = liveness
            .mutate(
                &owner,
                scope,
                state.revision(),
                RecordMutation::Event(Event::Stop(reason)),
            )
            .await
            .unwrap()
            .next;
        clock.set(support::clock(&initial, 31));
        let stopped = if handoff == Handoff::ReadyUserStopping {
            stopping
        } else {
            liveness
                .mutate(
                    &owner,
                    scope,
                    stopping.revision(),
                    RecordMutation::Event(Event::StopConfirmed),
                )
                .await
                .unwrap()
                .next
        };
        let lifecycle:(i16,Option<uuid::Uuid>)=sqlx::query_as("SELECT i.lifecycle,m.completion_intent_content_id FROM runtime.model_completion_intents i JOIN runtime.model_invocations m USING(invocation_id) WHERE i.invocation_id=$1")
            .bind(iteration.invocation_id.into_uuid()).fetch_one(&pool).await.unwrap();
        assert_eq!(
            lifecycle,
            (2, None),
            "crash leaves the exact ready protected result unlinked"
        );
        if handoff == Handoff::ReadyMissingTerminal {
            sqlx::query("UPDATE runtime.model_invocations SET terminal_stop_reason=NULL WHERE invocation_id=$1").bind(iteration.invocation_id.into_uuid()).execute(&pool).await.unwrap();
        }
        sqlx::query("UPDATE runtime.leases SET expires_at_ms=0 WHERE run_id=$1")
            .bind(owner.run_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms=0 WHERE attempt_id=$1")
            .bind(owner.attempt_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
        let scheduler =
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        let ClaimOutcome::Claimed(claimed) = scheduler
            .claim_one(
                &WorkerId::new("ready-recovery").unwrap(),
                Duration::from_secs(120),
            )
            .await
            .unwrap()
        else {
            panic!("claim")
        };
        let successor = AttemptContext {
            attempt_id: claimed.fence.attempt_id,
            run_id: claimed.fence.run_id,
            attempt_number: claimed.fence.attempt_number,
            worker_id: claimed.fence.worker_id,
            lease_id: claimed.fence.lease_id,
            fencing_token: claimed.fence.fencing_token,
            lease_expires_at: claimed.lease_expires_at,
        };
        assert!(
            journal
                .mark_model_dispatched(&owner, iteration.invocation_id)
                .await
                .is_err()
        );
        let checkpoint = journal.load_checkpoint(&successor).await.unwrap();
        let recovered = journal
            .recover_checkpoint(
                &successor,
                checkpoint,
                ModelRecoveryConstraints {
                    recovery_policy:
                        musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
                },
            )
            .await;
        if handoff == Handoff::ReadyShutdown {
            let recovered =
                recovered.expect("adopt exact ready terminal across shutdown and owner change");
            assert_eq!(recovered.checkpoint.phase, LoopPhase::ReadyForModel);
            assert_eq!(recovered.checkpoint.total_tokens, 20);
            let before = count(&pool).await;
            assert_eq!(
                journal.finish_model(&successor, completion).await.unwrap(),
                recovered.checkpoint
            );
            assert_eq!(
                count(&pool).await,
                before,
                "replay cannot repeat usage or output"
            );
        } else if matches!(handoff, Handoff::ReadyUserStop | Handoff::ReadyUserStopping) {
            let error = recovered.expect_err("user stop is not shutdown result adoption");
            assert_eq!(error.code, ErrorCode::InvalidCommand);
            assert!(!error.retryable);
        } else {
            // A physical shutdown is not proof of canonical provider completion.
            assert!(
                !matches!(recovered,Ok(ref value) if value.recovered_model_completion.is_some())
            );
        }
        let encoded: Vec<u8> = sqlx::query_scalar(
            "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id=$1",
        )
        .bind(iteration.invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            musteragent_wire::decode_execution_liveness_record(&encoded).unwrap(),
            stopped,
            "physical closure is immutable, including when recovery closes the Run lease"
        );
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime.model_invocations WHERE run_id=$1")
                .bind(owner.run_id.into_uuid())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1, "recovery never dispatches a replacement model");
        return;
    }
    journal
        .stage_model_completion(&owner, completion.clone())
        .await
        .expect("retained original result");
    let sealed = liveness
        .load(&owner, scope)
        .await
        .expect("read")
        .expect("sealed");
    assert!(
        matches!(&sealed, ExecutionRecord::Checkpoint(c) if c.state().phase() == Phase::Closed(Closure::TerminalObserved))
    );
    assert!(
        journal
            .mark_model_dispatched(&owner, iteration.invocation_id)
            .await
            .is_err(),
        "sealed results never authorize redispatch"
    );
    let observation = journal
        .load_model_observation(&owner, iteration.invocation_id)
        .await
        .expect("observation");
    assert!(
        journal
            .finish_failed_model(
                &owner,
                musteragent_loop::ModelFailureCommit {
                    observation,
                    provider_failure: musteragent_loop::ModelFailure {
                        code: ErrorCode::DependencyTimeout,
                        retryable: true
                    },
                    recovery_policy:
                        musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
                    retry_after_ms: None,
                    resolution: musteragent_loop::ModelFailureResolution::RetryNewAttempt,
                }
            )
            .await
            .is_err(),
        "a late physical timeout cannot discard the retained exact result"
    );
    let before_terminal = count(&pool).await;
    sqlx::query("CREATE TRIGGER fail_liveness_update BEFORE UPDATE ON runtime.model_invocations FOR EACH ROW WHEN (NEW.status = 2) EXECUTE FUNCTION fail_model_liveness()").execute(&pool).await.expect("fault");
    assert!(
        journal
            .finish_model(&owner, completion.clone())
            .await
            .is_err()
    );
    assert_eq!(
        count(&pool).await,
        before_terminal,
        "terminal Ledger and usage roll back"
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(sealed)
    );
    assert!(matches!(
        journal
            .load_checkpoint(&owner)
            .await
            .expect("in flight")
            .phase,
        LoopPhase::ModelInFlight { .. }
    ));
    sqlx::query("DROP TRIGGER fail_liveness_update ON runtime.model_invocations")
        .execute(&pool)
        .await
        .expect("remove fault");
    if handoff == Handoff::Cancel {
        sqlx::query("UPDATE runtime.runs SET cancel_requested_at_ms=1 WHERE run_id=$1")
            .bind(owner.run_id.into_uuid())
            .execute(&pool)
            .await
            .expect("cancel request");
        assert!(
            journal
                .finish_model(&owner, completion.clone())
                .await
                .is_err(),
            "cancelled Run cannot publish a staged success"
        );
        let cancelled = journal
            .acknowledge_cancellation(&owner, None)
            .await
            .expect("cancel sealed result");
        assert_eq!(cancelled.phase, LoopPhase::Cancelled);
        let encoded: Vec<u8> = sqlx::query_scalar(
            "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id = $1",
        )
        .bind(iteration.invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("retained physical history");
        let retained = musteragent_wire::decode_execution_liveness_record(&encoded).expect("scope");
        assert!(
            matches!(&retained, ExecutionRecord::Checkpoint(c) if c.state().phase() == Phase::Closed(Closure::TerminalObserved)),
            "cancellation does not rewrite physical history"
        );
        return;
    }
    if handoff == Handoff::Successor {
        sqlx::query("UPDATE runtime.leases SET expires_at_ms=0 WHERE run_id=$1")
            .bind(owner.run_id.into_uuid())
            .execute(&pool)
            .await
            .expect("expire old lease");
        sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms=0 WHERE attempt_id=$1")
            .bind(owner.attempt_id.into_uuid())
            .execute(&pool)
            .await
            .expect("expire old attempt");
        let scheduler =
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        let ClaimOutcome::Claimed(claimed) = scheduler
            .claim_one(
                &WorkerId::new("model-recovery-successor").expect("worker"),
                Duration::from_secs(120),
            )
            .await
            .expect("claim replacement")
        else {
            panic!("expired run must be claimable")
        };
        let successor = AttemptContext {
            attempt_id: claimed.fence.attempt_id,
            run_id: claimed.fence.run_id,
            attempt_number: claimed.fence.attempt_number,
            worker_id: claimed.fence.worker_id,
            lease_id: claimed.fence.lease_id,
            fencing_token: claimed.fence.fencing_token,
            lease_expires_at: claimed.lease_expires_at,
        };
        assert!(successor.fencing_token.get() > owner.fencing_token.get());
        assert!(
            journal
                .finish_model(&owner, completion.clone())
                .await
                .is_err(),
            "stale owner cannot publish a sealed result"
        );
        owner = successor;
        iteration.request.invocation.attempt_context = owner.clone();
    }
    let before_terminal = count(&pool).await;
    let checkpoint = journal
        .load_checkpoint(&owner)
        .await
        .expect("recoverable checkpoint");
    let finished = journal
        .recover_checkpoint(
            &owner,
            checkpoint,
            ModelRecoveryConstraints {
                recovery_policy:
                    musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
            },
        )
        .await
        .expect("publish exact retained result after failed business commit")
        .checkpoint;
    assert_eq!(finished.phase, LoopPhase::ReadyForModel);
    assert_eq!(finished.total_tokens, 20);
    let closed = liveness
        .load(&owner, scope)
        .await
        .expect("read")
        .expect("closed");
    assert!(
        matches!(&closed, ExecutionRecord::Checkpoint(c) if c.state().phase() == Phase::Closed(Closure::TerminalObserved))
    );
    let after_terminal = count(&pool).await;
    // Reply loss uses the original completion result, without another usage
    // fact, another supervision revision or a renewed activity clock.
    clock.set(support::clock(&initial, 50));
    let duplicate = journal
        .finish_model(&owner, completion.clone())
        .await
        .expect("terminal replay");
    assert_eq!(duplicate, finished);
    assert_eq!(count(&pool).await, after_terminal);
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(closed)
    );
    assert_eq!(
        after_terminal - before_terminal,
        2,
        "business publication commits model terminal and usage; physical result was already sealed"
    );
    let mut changed_completion = completion.clone();
    changed_completion.kind = ModelCompletionKind::AssistantContinuation;
    assert!(
        journal
            .finish_model(&owner, changed_completion)
            .await
            .is_err(),
        "same usage does not bless a different result kind"
    );
    assert_eq!(count(&pool).await, after_terminal);

    // The original recovery owner may close an invocation proven never
    // dispatched; this also closes its startup scope in that same transaction.
    let mut unused = iteration.clone();
    unused.iteration = 2;
    unused.invocation_id = InvocationId::generate();
    unused.request.invocation.invocation_id = unused.invocation_id;
    unused.assistant_message_id = MessageId::generate();
    unused.assistant_output_id = OutputId::generate();
    unused.reasoning_output_id = OutputId::generate();
    freeze_request(content.as_ref(), &mut unused).await;
    let checkpoint = journal
        .begin_model(&owner, &unused, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("undispatched begin");
    let recovered = journal
        .recover_checkpoint(
            &owner,
            checkpoint,
            ModelRecoveryConstraints {
                recovery_policy:
                    musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
            },
        )
        .await
        .expect("original undispatched recovery");
    assert_eq!(recovered.checkpoint.phase, LoopPhase::ReadyForModel);
    assert!(
        liveness
            .load(&owner, LivenessScope::Invocation(unused.invocation_id))
            .await
            .expect("read")
            .expect("retained")
            .is_closed()
    );

    let mut public = iteration.clone();
    public.iteration = 3;
    public.invocation_id = InvocationId::generate();
    public.request.invocation.invocation_id = public.invocation_id;
    public.assistant_message_id = MessageId::generate();
    public.assistant_output_id = OutputId::generate();
    public.reasoning_output_id = OutputId::generate();
    freeze_request(content.as_ref(), &mut public).await;
    journal
        .begin_model(&owner, &public, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("public begin");
    journal
        .mark_model_dispatched(&owner, public.invocation_id)
        .await
        .expect("public dispatch");
    journal
        .append_model_events(
            &owner,
            ModelEventBatch {
                invocation_id: public.invocation_id,
                iteration: 3,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("public"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new("Visible result").expect("text"),
                    },
                    ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::Stop,
                    },
                ],
            },
        )
        .await
        .expect("public response");
    let public_result = ModelCompletionCommit {
        invocation_id: public.invocation_id,
        kind: ModelCompletionKind::AssistantResponse,
        tool_calls: vec![],
        reasoning_replay: None,
        usage: None,
        provider_response_ref: provider_ref("public"),
        response_model_key: response_model_key(),
        continuation_ref: None,
    };
    journal
        .stage_model_completion(&owner, public_result.clone())
        .await
        .expect("public intent");
    journal
        .finish_model(&owner, public_result)
        .await
        .expect("public terminal");
    let payload: Vec<u8> = sqlx::query_scalar("SELECT e.encoded_event FROM conversation.messages m
        JOIN event_store.ledger_records r ON r.tenant_id=m.tenant_id AND r.conversation_id=m.conversation_id AND r.conversation_seq=m.committed_sequence
        JOIN event_store.domain_events e ON e.record_id=r.record_id WHERE m.message_id=$1")
        .bind(public.assistant_message_id.into_uuid()).fetch_one(&pool).await.expect("original public message sequence");
    let payload = musteragent_projection::LedgerPayloadDecoder::decode(
        &ProtobufLedgerPayloadCodec,
        musteragent_event_envelope::LedgerRecordKind::DomainEvent,
        &payload,
    )
    .expect("decode public fact");
    assert!(
        matches!(payload, musteragent_ledger::LedgerPayload::DomainEvent(
        musteragent_ledger::DomainEvent::Conversation(musteragent_conversation::ConversationEvent::MessageCommitted { message_id, .. }))
        if message_id == public.assistant_message_id),
        "message sequence must not point to the appended liveness fact"
    );

    // New physical invocation, same original Run/Attempt. A committed stop
    // fences both its control-only batches and its visible text output.
    let mut second = iteration;
    second.iteration = 4;
    second.invocation_id = InvocationId::generate();
    second.request.invocation.invocation_id = second.invocation_id;
    second.assistant_message_id = MessageId::generate();
    second.assistant_output_id = OutputId::generate();
    second.reasoning_output_id = OutputId::generate();
    freeze_request(content.as_ref(), &mut second).await;
    journal
        .begin_model(&owner, &second, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("second begin");
    journal
        .mark_model_dispatched(&owner, second.invocation_id)
        .await
        .expect("second dispatch");
    journal
        .append_model_events(
            &owner,
            ModelEventBatch {
                invocation_id: second.invocation_id,
                iteration: second.iteration,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("unknown-model"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new("Durable partial answer").expect("text"),
                    },
                    ModelStreamEvent::Usage(usage),
                ],
            },
        )
        .await
        .expect("durable prefix before stop");
    let scope = LivenessScope::Invocation(second.invocation_id);
    let before_stop = liveness
        .load(&owner, scope)
        .await
        .expect("read")
        .expect("starting");
    let before_unproven_cancel = count(&pool).await;
    assert!(
        journal
            .acknowledge_cancellation(&owner, None)
            .await
            .is_err(),
        "Run cancellation cannot skip physical model stop proof"
    );
    assert_eq!(count(&pool).await, before_unproven_cancel);
    clock.set(support::clock(&initial, 60));
    let stopped = liveness
        .mutate(
            &owner,
            scope,
            before_stop.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::Stop(
                    musteragent_execution_liveness::ControlStop::UserCancelled,
                ),
            ),
        )
        .await
        .expect("original committed stop")
        .next;
    let stopped_count = count(&pool).await;
    assert!(
        journal
            .mark_model_dispatched(&owner, second.invocation_id)
            .await
            .is_err(),
        "old dispatch receipt cannot authorize a stopped operation"
    );
    for events in [
        vec![ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("late"),
            response_model_key: response_model_key(),
        }],
        vec![ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: ModelText::new("must not publish").expect("text"),
        }],
    ] {
        assert!(
            journal
                .append_model_events(
                    &owner,
                    ModelEventBatch {
                        invocation_id: second.invocation_id,
                        iteration: 4,
                        batch_index: 0,
                        events
                    }
                )
                .await
                .is_err()
        );
    }
    assert_eq!(
        count(&pool).await,
        stopped_count,
        "no late public or control fact"
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(stopped.clone())
    );
    let durable_observation = journal
        .load_model_observation(&owner, second.invocation_id)
        .await
        .expect("durable prefix remains readable after stop");
    assert!(
        journal
            .load_model_observation(&owner, InvocationId::generate())
            .await
            .is_err(),
        "observation cannot cross original invocation"
    );
    let mut stale_owner = owner.clone();
    stale_owner.attempt_id = musteragent_identity::AttemptId::generate();
    assert!(
        journal
            .load_model_observation(&stale_owner, second.invocation_id)
            .await
            .is_err(),
        "observation requires original fenced owner"
    );
    if cancel {
        assert!(
            journal
                .acknowledge_cancellation(&owner, None)
                .await
                .is_err(),
            "pending stop is not confirmation"
        );
        clock.set(support::clock(&initial, 70));
        let confirmed = liveness
            .mutate(
                &owner,
                scope,
                stopped.revision(),
                musteragent_execution_liveness::RecordMutation::Event(
                    musteragent_execution_liveness::Event::StopConfirmed,
                ),
            )
            .await
            .expect("physical stop proof")
            .next;
        let before_cancel = count(&pool).await;
        sqlx::query("CREATE TRIGGER fail_cancelled_model_terminal BEFORE UPDATE ON runtime.runs FOR EACH ROW EXECUTE FUNCTION fail_model_liveness()").execute(&pool).await.expect("fault");
        assert!(
            journal
                .acknowledge_cancellation(&owner, None)
                .await
                .is_err()
        );
        assert_eq!(
            count(&pool).await,
            before_cancel,
            "model output sealing and cancellation roll back together"
        );
        sqlx::query("DROP TRIGGER fail_cancelled_model_terminal ON runtime.runs")
            .execute(&pool)
            .await
            .expect("remove fault");
        let result = journal
            .acknowledge_cancellation(&owner, None)
            .await
            .expect("proven model cancellation");
        assert_eq!(result.phase, LoopPhase::Cancelled);
        let encoded: Vec<u8> = sqlx::query_scalar(
            "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id = $1",
        )
        .bind(second.invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("retained scope");
        assert_eq!(
            musteragent_wire::decode_execution_liveness_record(&encoded).expect("scope"),
            confirmed
        );
        return;
    }
    let unknown_commit = musteragent_loop::ModelFailureCommit {
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
        observation: musteragent_loop::ModelInvocationObservation {
            invocation_id: second.invocation_id,
            provider_response_ref: Some(provider_ref("unknown-model")),
            response_model_key: Some(response_model_key()),
            usage: Some(usage),
            public_output_observed: true,
            replay_barrier_observed: true,
        },
        provider_failure: musteragent_loop::ModelFailure {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        },
        retry_after_ms: None,
        resolution: musteragent_loop::ModelFailureResolution::RetryNewAttempt,
    };
    assert_eq!(unknown_commit.observation, durable_observation);
    assert!(
        journal
            .finish_failed_model(&owner, unknown_commit.clone())
            .await
            .is_err(),
        "a pending stop cannot be claimed as a closed unknown outcome"
    );
    assert_eq!(count(&pool).await, stopped_count);
    clock.set(support::clock(&initial, 1000));
    let unconfirmed = liveness
        .mutate(
            &owner,
            scope,
            stopped.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::CheckDue,
            ),
        )
        .await
        .expect("finite cleanup without proof")
        .next;
    assert!(matches!(&unconfirmed, ExecutionRecord::Checkpoint(c)
        if matches!(c.state().phase(), Phase::Closed(Closure::StopUnconfirmed(_)))));
    let before_unknown = count(&pool).await;
    let mut invalid = unknown_commit.clone();
    invalid.observation.public_output_observed = false;
    assert!(
        journal.finish_failed_model(&owner, invalid).await.is_err(),
        "durable output cannot be erased"
    );
    assert_eq!(count(&pool).await, before_unknown);
    sqlx::query("CREATE TRIGGER fail_unknown_model_terminal BEFORE UPDATE ON runtime.runs FOR EACH ROW EXECUTE FUNCTION fail_model_liveness()").execute(&pool).await.expect("fault");
    assert!(
        journal
            .finish_failed_model(&owner, unknown_commit.clone())
            .await
            .is_err()
    );
    assert_eq!(
        count(&pool).await,
        before_unknown,
        "partial output sealing and terminal facts roll back together"
    );
    let model_status: i16 =
        sqlx::query_scalar("SELECT status FROM runtime.model_invocations WHERE invocation_id = $1")
            .bind(second.invocation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("model status");
    assert_eq!(
        model_status, 1,
        "failed terminal transaction leaves original invocation in flight"
    );
    sqlx::query("DROP TRIGGER fail_unknown_model_terminal ON runtime.runs")
        .execute(&pool)
        .await
        .expect("remove fault");
    let result = if restart {
        sqlx::query("UPDATE runtime.leases SET expires_at_ms=0 WHERE run_id=$1")
            .bind(owner.run_id.into_uuid())
            .execute(&pool)
            .await
            .expect("expire old lease");
        sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms=0 WHERE attempt_id=$1")
            .bind(owner.attempt_id.into_uuid())
            .execute(&pool)
            .await
            .expect("expire old attempt");
        let scheduler =
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        let ClaimOutcome::Claimed(claimed) = scheduler
            .claim_one(
                &WorkerId::new("model-recovery-successor").expect("worker"),
                Duration::from_secs(120),
            )
            .await
            .expect("claim replacement")
        else {
            panic!("expired run must be claimable")
        };
        let successor = AttemptContext {
            attempt_id: claimed.fence.attempt_id,
            run_id: claimed.fence.run_id,
            attempt_number: claimed.fence.attempt_number,
            worker_id: claimed.fence.worker_id,
            lease_id: claimed.fence.lease_id,
            fencing_token: claimed.fence.fencing_token,
            lease_expires_at: claimed.lease_expires_at,
        };
        assert!(successor.fencing_token.get() > owner.fencing_token.get());
        assert!(
            journal
                .finish_failed_model(&owner, unknown_commit)
                .await
                .is_err(),
            "former owner cannot seal a call after losing the run fence"
        );
        let checkpoint = journal
            .load_checkpoint(&successor)
            .await
            .expect("successor checkpoint");
        let recovered = journal
            .recover_checkpoint(
                &successor,
                checkpoint,
                ModelRecoveryConstraints {
                    recovery_policy:
                        musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
                },
            )
            .await
            .expect("successor seals original closed model without endless lease reclaim");
        assert!(recovered.replacement_attempt_scheduled);
        recovered.checkpoint
    } else {
        journal
            .finish_failed_model(&owner, unknown_commit)
            .await
            .expect("original atomic unknown outcome")
    };
    assert_eq!(result.phase, LoopPhase::ReadyForModel);
    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id = $1",
    )
    .bind(second.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("retained scope");
    assert_eq!(
        musteragent_wire::decode_execution_liveness_record(&encoded).expect("scope"),
        unconfirmed,
        "local terminalization never replaces unknown remote outcome with stop proof"
    );
    let model_status: i16 =
        sqlx::query_scalar("SELECT status FROM runtime.model_invocations WHERE invocation_id = $1")
            .bind(second.invocation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("model status");
    assert_eq!(model_status, 3);
}

async fn count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM event_store.domain_events")
        .fetch_one(pool)
        .await
        .expect("Ledger count")
}

pub(super) async fn freeze_request(content: &dyn ContentStore, iteration: &mut ModelIteration) {
    let bytes = musteragent_context::ContextCodec::encode_model_request(
        &musteragent_wire::CanonicalJsonContextCodec,
        &iteration.request,
    )
    .expect("canonical model request");
    iteration.request_ref = content
        .put(PutContent {
            tenant_id: iteration.request.invocation.run_context.tenant_id,
            media_type: MediaType::new(musteragent_runtime_model::MODEL_REQUEST_MEDIA_TYPE)
                .expect("media"),
            bytes,
        })
        .await
        .expect("freeze request");
}
