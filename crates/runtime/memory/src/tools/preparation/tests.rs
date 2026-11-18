use crate::write_planner::CompiledMemoryWritePlanner;
mod planner_tests;
use super::*;
use musteragent_capability::CapabilityKey;
use musteragent_content_ref::SchemaRef;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::*;
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_runtime_contract::{TraceContext, TraceParent};
use musteragent_tool::ToolKey;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[path = "write_journal_tests.rs"]
mod write_journal_tests;

#[derive(Default)]
struct Store {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<(TenantId, ContentId), (ContentRef, Vec<u8>)>,
    writes: usize,
    lookup_error: Option<ContentStoreError>,
    lose_ack: bool,
    competing_time: Option<i64>,
    invalid_ack: bool,
}

impl ContentStore for Store {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { panic!("preparation must use stable identities") })
    }

    fn put_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.writes += 1;
            let mut bytes = request.bytes;
            let mut competing = false;
            if request.media_type.as_str() == MEDIA
                && let Some(time) = state.competing_time.take()
            {
                let mut plan: PreparedProposal = serde_json::from_slice(&bytes).unwrap();
                plan.prepared_at_ms = time;
                bytes = encode(&plan).unwrap();
                competing = true;
            }
            let reference = ContentRef {
                content_id: id,
                media_type: request.media_type,
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(&bytes),
            };
            if let Some((old, _)) = state.objects.get(&(request.tenant_id, id)) {
                if *old != reference {
                    return Err(ContentStoreError::IdentityConflict);
                }
            } else {
                state
                    .objects
                    .insert((request.tenant_id, id), (reference.clone(), bytes));
            }
            if competing {
                return Err(ContentStoreError::IdentityConflict);
            }
            if std::mem::take(&mut state.lose_ack) {
                return Err(ContentStoreError::Unavailable);
            }
            let mut ack = reference;
            if state.invalid_ack {
                ack.sha256 = Sha256::digest(b"invalid acknowledgement");
            }
            Ok(ack)
        })
    }

    fn find_prepared_at(
        &self,
        tenant: TenantId,
        id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, ContentStoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            if let Some(error) = state.lookup_error.take() {
                return Err(error);
            }
            Ok(state.objects.get(&(tenant, id)).map(|(r, _)| r.clone()))
        })
    }

    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.state
                .lock()
                .unwrap()
                .objects
                .get(&(tenant, reference.content_id))
                .map(|(_, bytes)| bytes.clone())
                .ok_or(ContentStoreError::NotFound)
        })
    }
}

fn fixture() -> (MemoryToolContext, Vec<u8>) {
    let bytes = br#"{"scope":"agent_profile","kind":"semantic","claim":"remember this claim","conditions":["in this context"],"confidence":0.9,"sensitivity":"private"}"#.to_vec();
    let tenant_id = TenantId::generate();
    let actor = Actor {
        tenant_id,
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let run_context = RunContext {
        tenant_id,
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        activity_kind: musteragent_activity::ActivityKind::Chat,
        run_id: RunId::generate(),
        executor_participant_id: ParticipantId::generate(),
        requested_by_participant_id: ParticipantId::generate(),
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant { tenant_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).unwrap(),
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
    };
    let now = UtcTimestamp::from_unix_millis(100);
    let schema = SchemaRef {
        schema_key: VersionedKey::new("proposal.schema@1").unwrap(),
        schema_hash: Sha256::digest(b"schema"),
    };
    let invocation = CapabilityInvocation {
        actor,
        tenant_id,
        conversation_id: run_context.conversation_id,
        run_id: run_context.run_id,
        requested_by_attempt_id: AttemptId::generate(),
        execution_attempt_id: AttemptId::generate(),
        execution_attempt_number: AttemptNumber::new(1).unwrap(),
        invocation_id: InvocationId::generate(),
        tool_call_id: ToolCallId::new("proposal").unwrap(),
        tool_key: ToolKey::from_versioned_key(VersionedKey::new("experience.propose@3").unwrap()),
        capability_key: CapabilityKey::from_versioned_key(
            VersionedKey::new("experience.propose@3").unwrap(),
        ),
        capability_provider_id: CapabilityProviderId::generate(),
        idempotency_key: IdempotencyKey::new("proposal").unwrap(),
        request_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        },
        request_schema: schema.clone(),
        result_schema: schema,
        authorized_content: vec![],
        workspace_id: None,
        timing: InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(10_000),
        },
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .unwrap(),
            tracestate: None,
        },
        execution_grant_proof: None,
    };
    (
        MemoryToolContext {
            invocation,
            run_context,
            actor,
            agent_id: AgentId::generate(),
            approval_interaction_id: None,
            user_authored_origin: None,
            evidence_record_id: LedgerRecordId::generate(),
            now,
        },
        bytes,
    )
}

#[tokio::test]
async fn later_attempt_restores_identical_candidate_claim_result_and_original_time() {
    let (mut context, bytes) = fixture();
    let store = Store::default();
    let first = prepare_proposal(
        &store,
        &CompiledMemoryWritePlanner,
        context.clone(),
        &bytes,
        None,
    )
    .await
    .unwrap();
    context.now = UtcTimestamp::from_unix_millis(500);
    context.invocation.execution_attempt_id = AttemptId::generate();
    context.invocation.execution_attempt_number = AttemptNumber::new(2).unwrap();
    let second = prepare_proposal(
        &store,
        &CompiledMemoryWritePlanner,
        context.clone(),
        &bytes,
        None,
    )
    .await
    .unwrap();
    assert_eq!(first, second);
    let MemoryToolPreparation::Commit { mutation, result } = second else {
        panic!("commit")
    };
    let MemoryToolMutation::Propose(candidate) = *mutation else {
        panic!("proposal")
    };
    assert_eq!(candidate.created_at.unix_millis(), 100);
    assert_eq!(
        candidate.claim_ref.content_id,
        identity(context.invocation.invocation_id, "proposal-claim")
    );
    assert_eq!(
        result.result_ref.content_id,
        identity(context.invocation.invocation_id, "proposal-result")
    );
    assert_eq!(candidate.status, CandidateStatus::Candidate);
    // Only private Content exists; no Memory/Evolution storage is even provided.
    assert_eq!(store.state.lock().unwrap().objects.len(), 3);
}

#[tokio::test]
async fn lost_ack_and_competing_preparation_recover_the_first_persisted_plan() {
    for competing in [false, true] {
        let (context, bytes) = fixture();
        let store = Store::default();
        {
            let mut state = store.state.lock().unwrap();
            state.lose_ack = !competing;
            state.competing_time = competing.then_some(50);
        }
        let decision =
            proposal_decision(&store, &CompiledMemoryWritePlanner, &context, &bytes, None)
                .await
                .unwrap();
        assert_eq!(
            decision.prepared_at.unix_millis(),
            if competing { 50 } else { 100 }
        );
        assert_eq!(store.state.lock().unwrap().writes, 1);
    }
}

#[tokio::test]
async fn original_binding_and_request_cannot_be_replaced_during_recovery() {
    let (context, bytes) = fixture();
    let store = Store::default();
    proposal_decision(&store, &CompiledMemoryWritePlanner, &context, &bytes, None)
        .await
        .unwrap();
    for variant in 0..10 {
        let mut changed = context.clone();
        match variant {
            0 => {
                changed.actor.principal_id = PrincipalId::generate();
                changed.invocation.actor = changed.actor;
            }
            1 => changed.agent_id = AgentId::generate(),
            2 => {
                changed.run_context.run_id = RunId::generate();
                changed.invocation.run_id = changed.run_context.run_id;
            }
            3 => changed.evidence_record_id = LedgerRecordId::generate(),
            4 => changed.approval_interaction_id = Some(InteractionId::generate()),
            5 => changed.invocation.request_ref.content_id = ContentId::generate(),
            6 => changed.invocation.result_schema.schema_hash = Sha256::digest(b"other schema"),
            7 => changed.run_context.runtime_snapshot_id = RuntimeSnapshotId::generate(),
            8 => changed.invocation.capability_provider_id = CapabilityProviderId::generate(),
            _ => changed.invocation.request_schema.schema_hash = Sha256::digest(b"other schema"),
        }
        assert!(
            matches!(
                proposal_decision(&store, &CompiledMemoryWritePlanner, &changed, &bytes, None)
                    .await,
                Err(MemoryToolError::InvalidResult)
            ),
            "variant {variant}"
        );
    }
    assert!(
        proposal_decision(
            &store,
            &CompiledMemoryWritePlanner,
            &context,
            b"replaced request",
            None
        )
        .await
        .is_err()
    );
    assert_eq!(store.state.lock().unwrap().writes, 1);
}

#[tokio::test]
async fn corrupt_envelopes_references_and_foreign_implementation_never_regenerate() {
    for variant in 0..8 {
        let (context, bytes) = fixture();
        let store = Store::default();
        proposal_decision(&store, &CompiledMemoryWritePlanner, &context, &bytes, None)
            .await
            .unwrap();
        {
            let mut state = store.state.lock().unwrap();
            let (reference, body) = state.objects.values_mut().next().unwrap();
            match variant {
                0 => reference.content_id = ContentId::generate(),
                1 => reference.byte_length = MAX_BYTES as u64 + 1,
                2 => reference.media_type = MediaType::new("application/json").unwrap(),
                3 => body.push(b' '),
                _ => {
                    let mut prepared: PreparedProposal = serde_json::from_slice(body).unwrap();
                    match variant {
                        4 => prepared.implementation = "compiled:other@2".into(),
                        5 => prepared.binding.agent_id = AgentId::generate().into_uuid(),
                        6 => {
                            let WritePlan::ProposeV3(input) = &mut prepared.business.plan else {
                                panic!("v3")
                            };
                            input.claim = "changed meaning".into();
                        }
                        _ => prepared.prepared_at_ms = 101,
                    }
                    *body = encode(&prepared).unwrap();
                    reference.byte_length = body.len() as u64;
                    reference.sha256 = Sha256::digest(body);
                }
            }
        }
        assert!(
            proposal_decision(&store, &CompiledMemoryWritePlanner, &context, &bytes, None)
                .await
                .is_err(),
            "variant {variant}"
        );
        assert_eq!(store.state.lock().unwrap().writes, 1);
    }
}

#[tokio::test]
async fn unavailable_tombstone_and_unauthorized_lookup_are_not_absence() {
    for error in [
        ContentStoreError::Unavailable,
        ContentStoreError::NotFound,
        ContentStoreError::Unauthorized,
        ContentStoreError::Invariant,
    ] {
        let (context, bytes) = fixture();
        let store = Store::default();
        let transient = matches!(error, ContentStoreError::Unavailable);
        store.state.lock().unwrap().lookup_error = Some(error);
        let result =
            proposal_decision(&store, &CompiledMemoryWritePlanner, &context, &bytes, None).await;
        if transient {
            assert!(matches!(result, Err(MemoryToolError::Content)));
        } else {
            assert!(matches!(result, Err(MemoryToolError::InvalidResult)));
        }
        assert_eq!(store.state.lock().unwrap().writes, 0);
    }
}

#[tokio::test]
async fn malformed_write_acknowledgement_cannot_become_a_result() {
    let (context, bytes) = fixture();
    let store = Store::default();
    store.state.lock().unwrap().invalid_ack = true;
    assert!(matches!(
        prepare_proposal(&store, &CompiledMemoryWritePlanner, context, &bytes, None).await,
        Err(MemoryToolError::InvalidResult)
    ));
}
