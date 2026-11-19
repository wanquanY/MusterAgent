//! Storage conformance uses real PostgreSQL; opaque event bytes test the
//! injected codec boundary and atomic Ledger/Outbox ownership.
use super::*;
use musteragent_identity::*;
use musteragent_run::WorkerId;
use musteragent_runtime_capability::SandboxRouteCommit;
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_sandbox::*;
use musteragent_testkit::IsolatedPostgres;

pub(super) struct StorageCodec;
impl LedgerPayloadEncoder for StorageCodec {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        format!("{payload:?}").into_bytes()
    }
}
impl LedgerPayloadDecoder for StorageCodec {
    fn decode(
        &self,
        _: LedgerRecordKind,
        _: &[u8],
    ) -> Result<LedgerPayload, musteragent_projection::SubscriptionError> {
        Err(musteragent_projection::SubscriptionError::CorruptRecord)
    }
}
pub(super) struct Fixture {
    _database: IsolatedPostgres,
    pub(super) pool: PgPool,
    pub(super) actor: Actor,
    pub(super) attempt: AttemptContext,
    pub(super) invocation: InvocationId,
    pub(super) journal: PostgresFencedToolInvocationJournal,
}
impl Fixture {
    pub(super) async fn new() -> Self {
        let database = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start pgvector");
        let pool = database.pool().await.expect("connect disposable database");
        crate::migrate(&pool).await.expect("baseline");
        let owner = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let agent = AgentId::generate();
        let conversation = ConversationId::generate();
        let executor = ParticipantId::generate();
        let requester = ParticipantId::generate();
        let run = RunId::generate();
        let activity = ActivityId::generate();
        let snapshot = RuntimeSnapshotId::generate();
        let attempt = AttemptContext {
            run_id: run,
            attempt_id: AttemptId::generate(),
            attempt_number: AttemptNumber::new(1).unwrap(),
            worker_id: WorkerId::new("sandbox-routing-test").unwrap(),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(1).unwrap(),
            lease_expires_at: UtcTimestamp::from_unix_millis(i64::MAX),
        };
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("INSERT INTO agent.agent_definitions(agent_id,agent_key,built_in,lifecycle,display_name,created_at_ms,updated_at_ms) VALUES($1,$2,false,1,'Test Agent',1,1)")
            .bind(agent.into_uuid()).bind(format!("test-{agent}")).execute(&mut *tx).await.unwrap();
        sqlx::query(
            "INSERT INTO conversation.conversations
        (conversation_id,tenant_id,kind,purpose,control_home_kind,home_epoch,lifecycle,created_by,
         created_at_ms,updated_at_ms,execution_owner_participant_id)
         VALUES($1,$2,1,'standard',1,1,1,$3,1,1,$4)",
        )
        .bind(conversation.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(owner.principal_id.into_uuid())
        .bind(executor.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversation.participants
        (participant_id,tenant_id,conversation_id,kind,agent_ref,display_name,joined_at_ms)
        VALUES($1,$2,$3,2,$4,'Muster',1)",
        )
        .bind(executor.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation.into_uuid())
        .bind(agent.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversation.participants
        (participant_id,tenant_id,conversation_id,kind,principal_ref,display_name,joined_at_ms)
        VALUES($1,$2,$3,1,$4,'User',1)",
        )
        .bind(requester.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation.into_uuid())
        .bind(owner.principal_id.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("INSERT INTO event_store.ledger_streams
        (tenant_id,conversation_id,home_epoch,last_sequence,retained_from_sequence,created_at_ms,updated_at_ms)
        VALUES($1,$2,1,0,1,1,1)")
        .bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO runtime.runs
        (run_id,tenant_id,conversation_id,activity_id,activity_kind,executor_participant_id,
         requested_by_participant_id,execution_scope_kind,control_home_kind,execution_home_kind,
         home_epoch,runtime_snapshot_id,admitted_sequence,status,next_attempt_number,admitted_at_ms,started_at_ms,traceparent)
        VALUES($1,$2,$3,$4,1,$5,$6,1,1,1,1,$7,1,2,2,1,1,'00-00000000000000000000000000000001-0000000000000001-00')")
        .bind(run.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(activity.into_uuid())
        .bind(executor.into_uuid()).bind(requester.into_uuid()).bind(snapshot.into_uuid())
        .execute(&mut *tx).await.unwrap();
        let now: i64 =
            sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO runtime.attempts
        (attempt_id,tenant_id,run_id,attempt_number,trigger,worker_id,lease_id,fencing_token,
         lease_expires_at_ms,status,claimed_at_ms) VALUES($1,$2,$3,1,1,$4,$5,1,$6,1,$7)",
        )
        .bind(attempt.attempt_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(run.into_uuid())
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(now + 60_000)
        .bind(now)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runtime.leases
        (run_id,tenant_id,attempt_id,worker_id,lease_id,fencing_token,expires_at_ms,renewed_at_ms)
        VALUES($1,$2,$3,$4,$5,1,$6,$7)",
        )
        .bind(run.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(now + 60_000)
        .bind(now)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let key = DottedKey::new("test.sandbox-routing").unwrap();
        let journal = PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            Arc::new(StorageCodec),
            Arc::new(StorageCodec),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [19; 32])]).unwrap()),
        );
        let payload = StorageCodec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
            RunEvent::Admitted {
                runtime_snapshot_id: snapshot,
                execution_home: musteragent_event_envelope::RuntimeHome::Cloud,
                parent_run_id: None,
                delegation_id: None,
                input_message_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            },
        )));
        journal
            .ledger
            .append(AppendRequest {
                tenant_id: owner.tenant_id,
                conversation_id: conversation,
                expected_home_epoch: HomeEpoch::new(1).unwrap(),
                expected_offset: LedgerOffset::new(0),
                idempotency_key: IdempotencyKey::new(format!("admitted-{run}")).unwrap(),
                command_fingerprint: Sha256::digest(&payload),
                outbox: vec![],
                records: vec![ProposedRecord {
                    schema_version: DottedKey::new(RECORD_SCHEMA).unwrap(),
                    record_id: LedgerRecordId::generate(),
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(activity),
                    run_id: Some(run),
                    run_seq: Some(RunSeq::new(1).unwrap()),
                    attempt_id: None,
                    participant_id: Some(requester),
                    producer: Producer::Worker,
                    occurred_at: UtcTimestamp::from_unix_millis(1),
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }],
            })
            .await
            .unwrap();
        let request = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        sqlx::query("INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,'application/json',2,$3,$4,1)")
            .bind(request.content_id.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(request.sha256.into_bytes().to_vec()).bind(format!("test://{}",request.content_id)).execute(&pool).await.unwrap();
        let invocation = ToolInvocation {
            invocation_id: InvocationId::generate(),
            run_id: run,
            requested_by_attempt_id: attempt.attempt_id,
            execution_attempt_id: None,
            tool_call_id: ToolCallId::new("sandbox-call").unwrap(),
            tool_key: ToolKey::from_versioned_key(VersionedKey::new("sandbox.exec@1").unwrap()),
            capability_provider_id: CapabilityProviderId::generate(),
            status: ToolInvocationStatus::Requested,
            effect: Effect::Read,
            approval: ApprovalRequirement::Never,
            idempotency_key: IdempotencyKey::new("sandbox-test").unwrap(),
            request_ref: request,
            result_ref: None,
            interaction_id: None,
            timing: InvocationExecutionTiming::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(now + 60_000),
            },
        };
        let (tx, state) = journal.begin_commit_lane(&attempt).await.unwrap();
        let append = journal
            .append_for_event(
                &state,
                &attempt,
                invocation.invocation_id,
                "requested",
                UtcTimestamp::from_unix_millis(now),
                DomainEvent::Tool(ToolEvent::InvocationRequested {
                    invocation_id: invocation.invocation_id,
                    tool_call_id: invocation.tool_call_id.clone(),
                    tool_key: invocation.tool_key.clone(),
                    effect: invocation.effect,
                    request_ref: invocation.request_ref.clone(),
                    timing: invocation.timing.clone(),
                }),
            )
            .unwrap();
        let BeginAppend::Ready(mut prepared) = journal
            .ledger
            .begin_append_in_transaction(tx, &append)
            .await
            .unwrap()
        else {
            panic!("new request")
        };
        insert_invocation(
            prepared.transaction_mut(),
            owner.tenant_id,
            conversation,
            &invocation,
            ToolRetryContract::ReadOnly,
        )
        .await
        .unwrap();
        advance_run_sequence(
            prepared.transaction_mut(),
            run,
            state.next_sequence(1).unwrap(),
        )
        .await
        .unwrap();
        prepared.commit(&append).await.unwrap();
        sqlx::query("UPDATE tool.tool_invocations SET status=3,execution_attempt_id=$2,started_at_ms=$3 WHERE invocation_id=$1")
            .bind(invocation.invocation_id.into_uuid()).bind(attempt.attempt_id.into_uuid()).bind(now).execute(&pool).await.unwrap();
        Self {
            _database: database,
            pool,
            actor: owner,
            attempt,
            invocation: invocation.invocation_id,
            journal,
        }
    }
    pub(super) fn commit(&self, executor: SandboxExecutor) -> SandboxRouteCommit {
        SandboxRouteCommit {
            invocation_id: self.invocation,
            route: SandboxRouteRecord {
                decision: SandboxRouteDecision {
                    executor,
                    compatibility: SandboxCompatibility::BrowserCompatible,
                    reason: match executor {
                        SandboxExecutor::Browser => SandboxRouteReason::BrowserClientOnline,
                        SandboxExecutor::Server => SandboxRouteReason::NoOnlineBrowserClient,
                    },
                },
                decided_at: UtcTimestamp::from_unix_millis(1234),
            },
        }
    }
}
