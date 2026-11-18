//! Real migrated PostgreSQL transaction test; no model or frontend substitute.
use super::*;
use crate::dialogue_fixture as fixture;
use crate::{PostgresAgentDialogueStore, PostgresOwnedAgentStore, migrate};
use fixture::{actor, content, input, prepared, register_content};
use musteragent_content_ref::MediaType;
use musteragent_execution_context::{AgentTaskTarget, DialogueInputSurface, UserIntent};
use musteragent_identity::*;
use musteragent_ledger::AttemptFence;
use musteragent_run::WorkerId;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_session::*;
use musteragent_testkit::IsolatedPostgres;
use musteragent_work_item::*;
use std::sync::Arc;
mod batch;
mod source_failure;
mod stop;

struct PublishedAgent {
    pub(crate) agent_id: AgentId,
    pub(crate) agent_version: AgentVersionId,
}

#[tokio::test]
async fn capability_terminal_stages_one_original_context_input_without_a_user_message() {
    assert_original_context_successor("capability.request@1", false, 1).await;
}

#[tokio::test]
async fn named_capability_terminal_preserves_the_original_single_context_successor() {
    assert_original_context_successor("capability.request@2", false, 1).await;
}

#[tokio::test]
async fn selected_task_capability_continuations_keep_scope_across_two_original_completions() {
    assert_original_context_successor("capability.request@2", true, 2).await;
}

async fn assert_original_context_successor(tool_ref: &str, selected: bool, hops: usize) {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let activation = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let mut request = input(owner, activation.agent.dialogue_id);
    if selected {
        register_content(&pool, owner, &request.content_ref).await;
        let parent = PostgresAgentDialogueStore::new(pool.clone())
            .accept(request)
            .await
            .unwrap()
            .intent;
        let target = selected_task(
            &pool,
            owner,
            activation.agent.space_id,
            activation.agent.agent_id,
            &parent,
        )
        .await;
        request = input(owner, activation.agent.dialogue_id);
        request.task_target = Some(target);
        request.observed_cursor = parent.accepted_cursor;
        request.accepted_at = UtcTimestamp::from_unix_millis(350);
    }
    register_content(&pool, owner, &request.content_ref).await;
    let intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(request)
        .await
        .unwrap()
        .intent;
    let mut current = intent.clone();
    for _ in 0..hops {
        current = complete_original_context(
            pool.clone(),
            owner,
            activation.agent.agent_id,
            activation.agent.published_version_id,
            current,
            tool_ref,
            if selected { 2 } else { 1 },
        )
        .await;
    }
    let mut tx = pool.begin().await.unwrap();
    let facts = verified_root(&mut tx, owner, activation.agent.agent_id, &current)
        .await
        .unwrap();
    assert_eq!(facts.current_intent, current);
    assert_eq!(facts.root_intent, intent);
    assert!(facts.is_direct_user_root);
    let by_id = verified_root_id(&mut tx, owner, activation.agent.agent_id, current.intent_id)
        .await
        .unwrap();
    assert_eq!(by_id.current_intent, facts.current_intent);
    assert_eq!(by_id.root_intent, facts.root_intent);
    let direct = verified_root_id(&mut tx, owner, activation.agent.agent_id, intent.intent_id)
        .await
        .unwrap();
    assert_eq!(direct.root_intent, intent);
    assert!(direct.is_direct_user_root);
    if hops == 2 {
        assert!(matches!(
            source::verified_root_with_budget(
                &mut tx,
                owner,
                activation.agent.agent_id,
                current.intent_id,
                1
            )
            .await,
            Err(DialogueStoreError::Unavailable)
        ));
        assert_eq!(
            source::verified_root_with_budget(
                &mut tx,
                owner,
                activation.agent.agent_id,
                current.intent_id,
                2
            )
            .await
            .unwrap()
            .root_intent,
            intent
        );
    }
    assert!(
        verified_root(&mut tx, actor(), activation.agent.agent_id, &current)
            .await
            .is_err()
    );
    assert!(
        verified_root(&mut tx, owner, AgentId::generate(), &current)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    // Simulate retained corrupt/old material without changing the public input
    // or its canonical receipt. Such a row must not be interpreted as consent.
    if selected {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE agent.dialogue_user_intents SET task_target=NULL WHERE intent_id=$1")
            .bind(current.intent_id.into_uuid())
            .execute(&mut *tx)
            .await
            .unwrap();
        let bad = source::load(&mut tx, owner, activation.agent.agent_id, current.intent_id)
            .await
            .unwrap();
        let bad = crate::dialogue_codec::intent_row(&bad).unwrap();
        assert!(matches!(
            verified_root(&mut tx, owner, activation.agent.agent_id, &bad).await,
            Err(DialogueStoreError::Corrupt)
        ));
        tx.rollback().await.unwrap();
    }
    if selected {
        // A retained continuation with lost scope must not undo the next real
        // completed Interaction or terminal. Its single new input remains
        // available for the preparation owner to record a public failure.
        sqlx::query("UPDATE agent.dialogue_user_intents SET task_target=NULL WHERE intent_id=$1")
            .bind(current.intent_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let bad = source::load(&mut tx, owner, activation.agent.agent_id, current.intent_id)
            .await
            .unwrap();
        let bad = crate::dialogue_codec::intent_row(&bad).unwrap();
        tx.rollback().await.unwrap();
        let next = complete_original_context(
            pool.clone(),
            owner,
            activation.agent.agent_id,
            activation.agent.published_version_id,
            bad,
            tool_ref,
            2,
        )
        .await;
        let mut tx = pool.begin().await.unwrap();
        assert!(matches!(
            verified_root_id(&mut tx, owner, activation.agent.agent_id, next.intent_id).await,
            Err(DialogueStoreError::Corrupt)
        ));
        let pending: String = sqlx::query_scalar(
            "SELECT status FROM agent.dialogue_intent_admissions WHERE intent_id=$1",
        )
        .bind(next.intent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(pending, "pending");
        tx.rollback().await.unwrap();
        source_failure::settle_corrupt_successor(
            &pool,
            owner,
            activation.agent.agent_id,
            next.intent_id,
        )
        .await;
    }
    let mut tx = pool.begin().await.unwrap();
    let changed = content("not the user's original request");
    sqlx::query("UPDATE agent.dialogue_user_intents SET content_ref=$2 WHERE intent_id=$1")
        .bind(current.intent_id.into_uuid())
        .bind(crate::dialogue_codec::content_bytes(&changed))
        .execute(&mut *tx)
        .await
        .unwrap();
    let bad = source::load(&mut tx, owner, activation.agent.agent_id, current.intent_id)
        .await
        .unwrap();
    let bad = crate::dialogue_codec::intent_row(&bad).unwrap();
    assert!(matches!(
        verified_root(&mut tx, owner, activation.agent.agent_id, &bad).await,
        Err(DialogueStoreError::Corrupt)
    ));
    tx.rollback().await.unwrap();
}

async fn complete_original_context(
    pool: sqlx::PgPool,
    owner: Actor,
    agent_id: AgentId,
    agent_version: AgentVersionId,
    intent: UserIntent,
    tool_ref: &str,
    expected_messages: i64,
) -> UserIntent {
    complete_original_context_control(
        pool,
        owner,
        PublishedAgent {
            agent_id,
            agent_version,
        },
        intent,
        tool_ref,
        expected_messages,
        None,
    )
    .await
    .expect("uncontrolled successor")
}
async fn complete_original_context_control(
    pool: sqlx::PgPool,
    owner: Actor,
    PublishedAgent {
        agent_id,
        agent_version,
    }: PublishedAgent,
    intent: UserIntent,
    tool_ref: &str,
    expected_messages: i64,
    timing: Option<stop::Timing>,
) -> Option<UserIntent> {
    let next_seq: i64 =
        sqlx::query_scalar("SELECT head_seq+1 FROM agent.agent_dialogues WHERE dialogue_id=$1")
            .bind(intent.dialogue_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    let conversation = ConversationId::generate();
    let executor = ParticipantId::generate();
    let requester = ParticipantId::generate();
    let run = RunId::generate();
    let activity = ActivityId::generate();
    let fence = AttemptFence {
        run_id: run,
        attempt_id: AttemptId::generate(),
        attempt_number: AttemptNumber::new(1).unwrap(),
        worker_id: WorkerId::new("dialogue-publication-test").unwrap(),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).unwrap(),
        home_epoch: HomeEpoch::new(1).unwrap(),
    };
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO conversation.conversations
        (conversation_id,tenant_id,kind,purpose,control_home_kind,home_epoch,lifecycle,created_by,
         created_at_ms,updated_at_ms,execution_owner_participant_id)
         VALUES($1,$2,4,'dialogue_intent_coordination',1,1,1,$3,1,1,$4)",
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
    .bind(agent_id.into_uuid())
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
    sqlx::query("INSERT INTO agent.dialogue_sessions
        (conversation_id,tenant_id,owner_id,agent_id,dialogue_id,source_intent_id,purpose,
         context_epoch,context_seq,created_at_ms,pinned_agent_version_id,permission_revision,manifest_revision,instructions_revision)
        VALUES($1,$2,$3,$4,$5,$6,'intent_coordination',1,$8,1,$7,1,1,1)")
        .bind(conversation.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid())
        .bind(agent_id.into_uuid()).bind(intent.dialogue_id.into_uuid()).bind(intent.intent_id.into_uuid())
        .bind(agent_version.into_uuid()).bind(intent.accepted_cursor.seq as i64).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO runtime.runs
        (run_id,tenant_id,conversation_id,activity_id,activity_kind,executor_participant_id,
         requested_by_participant_id,execution_scope_kind,control_home_kind,execution_home_kind,
         home_epoch,runtime_snapshot_id,admitted_sequence,status,next_attempt_number,admitted_at_ms,started_at_ms,traceparent)
        VALUES($1,$2,$3,$4,1,$5,$6,1,1,1,1,$7,1,2,2,1,1,'00-00000000000000000000000000000001-0000000000000001-00')")
        .bind(run.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(activity.into_uuid())
        .bind(executor.into_uuid()).bind(requester.into_uuid()).bind(RuntimeSnapshotId::generate().into_uuid())
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
    .bind(fence.attempt_id.into_uuid())
    .bind(owner.tenant_id.into_uuid())
    .bind(run.into_uuid())
    .bind(fence.worker_id.as_str())
    .bind(fence.lease_id.into_uuid())
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
    .bind(fence.attempt_id.into_uuid())
    .bind(fence.worker_id.as_str())
    .bind(fence.lease_id.into_uuid())
    .bind(now + 60_000)
    .bind(now)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // Establish the original ledger through its canonical append owner. This
    // fixture tests storage atomicity, not the model/protobuf rendering layer.
    let payload = format!(
        "{:?}",
        musteragent_run::RunEvent::Completed {
            assistant_message_id: None,
            result_refs: vec![]
        }
    )
    .into_bytes();
    crate::PostgresLedgerStore::new(pool.clone())
        .append(musteragent_ledger::AppendRequest {
            tenant_id: owner.tenant_id,
            conversation_id: conversation,
            expected_home_epoch: HomeEpoch::new(1).unwrap(),
            expected_offset: LedgerOffset::new(0),
            idempotency_key: IdempotencyKey::new(format!("capability-test:{run}")).unwrap(),
            command_fingerprint: musteragent_content_ref::Sha256::digest(&payload),
            records: vec![musteragent_ledger::ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").unwrap(),
                record_id: LedgerRecordId::generate(),
                record_kind: musteragent_event_envelope::LedgerRecordKind::DomainEvent,
                activity_id: Some(activity),
                run_id: Some(run),
                run_seq: Some(RunSeq::new(1).unwrap()),
                attempt_id: Some(fence.attempt_id),
                participant_id: Some(executor),
                producer: musteragent_event_envelope::Producer::Worker,
                occurred_at: UtcTimestamp::from_unix_millis(3),
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: musteragent_content_ref::Sha256::digest(&payload),
                payload,
            }],
            outbox: vec![],
        })
        .await
        .unwrap();
    let stop_request = if let Some(timing) = timing {
        Some(stop::observe(&pool, owner, agent_id, intent.dialogue_id, timing).await)
    } else {
        None
    };
    let prompt = content("capability prompt");
    register_content(&pool, owner, &prompt).await;
    let interaction = InteractionId::generate();
    let invocation = InvocationId::generate();
    let entry = DialogueEntryId::generate();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO tool.tool_invocations(invocation_id,tenant_id,conversation_id,run_id,requested_by_attempt_id,execution_attempt_id,tool_call_id,tool_ref,capability_provider_id,status,effect,approval_policy,retry_contract,idempotency_key,request_content_id,request_hash,interaction_id,deadline_ms) VALUES($1,$2,$3,$4,$5,$5,'capability',$11,$6,4,1,1,1,$7,$8,$9,$10,1000)")
        .bind(invocation.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(run.into_uuid()).bind(fence.attempt_id.into_uuid())
        .bind(CapabilityProviderId::generate().into_uuid()).bind(invocation.to_string()).bind(prompt.content_id.into_uuid()).bind(prompt.sha256.into_bytes().as_slice()).bind(interaction.into_uuid()).bind(tool_ref).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO approval.interactions(interaction_id,tenant_id,conversation_id,activity_id,run_id,requested_by_invocation_id,kind,status,prompt_content_id,response_schema_key,response_schema_hash,requested_at_ms) VALUES($1,$2,$3,$4,$5,$6,6,1,$7,'capability.completion@1',$8,1)")
        .bind(interaction.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(activity.into_uuid()).bind(run.into_uuid()).bind(invocation.into_uuid()).bind(prompt.content_id.into_uuid()).bind([7u8;32].as_slice()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO agent.agent_dialogue_entries(entry_id,tenant_id,owner_id,dialogue_id,epoch,seq,kind,source_intent_id,author_agent_id,interaction_id,visible_revision,created_at_ms) VALUES($1,$2,$3,$4,1,$8,'interaction',$5,$6,$7,1,1)")
        .bind(entry.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid()).bind(intent.dialogue_id.into_uuid()).bind(intent.intent_id.into_uuid()).bind(agent_id.into_uuid()).bind(interaction.into_uuid()).bind(next_seq).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE agent.agent_dialogues SET head_seq=$2 WHERE dialogue_id=$1")
        .bind(intent.dialogue_id.into_uuid())
        .bind(next_seq)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent.dialogue_publications(tenant_id,owner_id,agent_id,dialogue_id,delivery_id,entry_id,source_intent_id,source_conversation_id,source_run_id,purpose,permission_revision,payload_sha256,created_at_ms,source_kind,source_interaction_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'required_decision',1,$10,1,'interaction_owner',$11)")
        .bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid()).bind(agent_id.into_uuid()).bind(intent.dialogue_id.into_uuid()).bind(CommandId::generate().into_uuid()).bind(entry.into_uuid()).bind(intent.intent_id.into_uuid()).bind(conversation.into_uuid()).bind(run.into_uuid()).bind([9u8;32].as_slice()).bind(interaction.into_uuid()).execute(&mut *tx).await.unwrap();
    assert!(!completed(&mut tx, owner.tenant_id, run).await.unwrap());
    stage_successor(&mut tx, owner.tenant_id, run)
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.dialogue_user_intents WHERE continuation_interaction_id=$1",
    )
    .bind(interaction.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        before, 0,
        "a pending card is not a completed availability fact"
    );
    sqlx::query("UPDATE approval.interactions SET status=2,responded_by=$2,responded_at_ms=2 WHERE interaction_id=$1").bind(interaction.into_uuid()).bind(owner.principal_id.into_uuid()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO approval.interaction_responses(interaction_id,response_kind,secure_response_ref,response_hash,responded_by,responded_at_ms) VALUES($1,3,'capability-ready',$2,$3,2)").bind(interaction.into_uuid()).bind([5u8;32].as_slice()).bind(owner.principal_id.into_uuid()).execute(&mut *tx).await.unwrap();
    assert!(completed(&mut tx, owner.tenant_id, run).await.unwrap());
    sqlx::query("UPDATE runtime.runs SET status=5,finished_at_ms=3 WHERE run_id=$1")
        .bind(run.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    stage_successor(&mut tx, owner.tenant_id, run)
        .await
        .unwrap();
    stage_successor(&mut tx, owner.tenant_id, run)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    if matches!(timing, Some(stop::Timing::BeforeSuccessor)) {
        stop::assert_suppressed(&pool, interaction, run).await;
        return None;
    }
    let row=sqlx::query("SELECT i.*,q.status AS queue_status FROM agent.dialogue_user_intents i JOIN agent.dialogue_intent_admissions q USING(intent_id) WHERE continuation_interaction_id=$1").bind(interaction.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        row.get::<uuid::Uuid, _>("source_entry_id"),
        entry.into_uuid()
    );
    assert_eq!(
        row.get::<uuid::Uuid, _>("reply_to_intent_id"),
        intent.intent_id.into_uuid()
    );
    assert_eq!(row.get::<String, _>("origin_surface"), "capability");
    assert_eq!(row.get::<String, _>("queue_status"), "pending");
    assert_eq!(row.get::<i16, _>("input_attachment_count"), 0);
    assert_eq!(
        crate::dialogue_codec::content_from_bytes(row.get("content_ref")).unwrap(),
        intent.content_ref
    );
    let messages:i64=sqlx::query_scalar("SELECT count(*) FROM agent.agent_dialogue_entries WHERE kind='user_message' AND dialogue_id=$1").bind(intent.dialogue_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        messages, expected_messages,
        "server completion must never forge a user message"
    );
    let inputs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.dialogue_user_intents WHERE continuation_interaction_id=$1",
    )
    .bind(interaction.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        inputs, 1,
        "terminal replay must preserve the original single admission"
    );
    let terminal_status: i16 =
        sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
            .bind(run.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        terminal_status, 5,
        "source proof failure must not roll back real completion"
    );
    let successor = crate::dialogue_codec::intent_row(&row).unwrap();
    assert_eq!(successor.origin.surface, DialogueInputSurface::Capability);
    assert_eq!(successor.task_target, intent.task_target);
    if let Some(request) = stop_request {
        stop::assert_late_successor(&pool, request, &successor).await;
    }
    Some(successor)
}

async fn selected_task(
    pool: &sqlx::PgPool,
    actor: Actor,
    space_id: OwnerSpaceId,
    agent_id: AgentId,
    parent: &UserIntent,
) -> AgentTaskTarget {
    let key = DottedKey::new("test.capability.root").unwrap();
    let content = crate::PostgresContentStore::new(
        pool.clone(),
        Arc::new(crate::PostgresTieredBlobStore::new(
            pool.clone(),
            Arc::new(fixture::MemoryBlobs::default()),
        )),
        Arc::new(
            musteragent_storage_protection::OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])])
                .unwrap(),
        ),
    );
    let mut request = CreateAgentWorkItem {
        actor,
        scope: OwnedAgentWorkItemScope {
            space_id,
            owner_id: actor.principal_id,
            agent_id,
        },
        command_id: CommandId::generate(),
        source_dialogue_id: parent.dialogue_id,
        source_intent_id: parent.intent_id,
        split_key: "selected-capability-target".into(),
        parent_work_item_id: None,
        spec: WorkItemSpec {
            objective: "Prepare the review".into(),
            success_criteria: vec!["Cite the sources".into()],
            dependencies: vec![],
            priority: WorkItemPriority::Normal,
            evidence_boundary: "User inputs only".into(),
            follow_up_of: None,
        },
        goal_content_ref: fixture::content("staging"),
        created_at: UtcTimestamp::from_unix_millis(300),
    };
    request.goal_content_ref = content
        .put_reclaimable_at(
            work_item_goal_content_id(request.work_item_id(), request.command_id),
            PutContent {
                tenant_id: actor.tenant_id,
                media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE).unwrap(),
                bytes: work_item_goal_bytes(&request.spec),
            },
        )
        .await
        .unwrap();
    let id = request.work_item_id();
    crate::PostgresWorkItemAuthority::new(pool.clone())
        .create(request)
        .await
        .unwrap();
    AgentTaskTarget {
        work_item_id: id,
        goal_revision: 1,
        control_revision: 1,
    }
}

#[tokio::test]
async fn scheduled_capability_completion_preserves_background_origin_without_user_consent() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap()
        .agent;
    let schedules = crate::PostgresOwnerScheduleStore::new(pool.clone());
    let key = DottedKey::new("test.capability.schedule").unwrap();
    let content = crate::PostgresContentStore::new(
        pool.clone(),
        Arc::new(crate::PostgresTieredBlobStore::new(
            pool.clone(),
            Arc::new(fixture::MemoryBlobs::default()),
        )),
        Arc::new(
            musteragent_storage_protection::OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])])
                .unwrap(),
        ),
    );
    let schedule = schedules
        .change(ChangeOwnerSchedule {
            actor: owner,
            agent_id: agent.agent_id,
            command_id: CommandId::generate(),
            schedule_id: uuid::Uuid::now_v7(),
            expected_revision: 0,
            action: OwnerScheduleAction::Create,
            definition: Some(OwnerScheduleDefinition {
                title: "Daily review".into(),
                instructions: "Summarize progress".into(),
                timezone: "Asia/Shanghai".into(),
                timing: musteragent_goal::ScheduleSpec::Cron {
                    expression: "* * * * *".into(),
                },
            }),
            now: UtcTimestamp::from_unix_millis(1700000000000),
        })
        .await
        .unwrap();
    assert!(
        schedules
            .dispatch_one(
                schedule.schedule_id,
                schedule.next_run_at.unwrap(),
                &content
            )
            .await
            .unwrap()
    );
    let row = sqlx::query("SELECT i.* FROM agent.owner_schedule_occurrences o JOIN agent.dialogue_user_intents i USING(intent_id) WHERE o.schedule_id=$1 AND o.status='accepted'")
        .bind(schedule.schedule_id).fetch_one(&pool).await.unwrap();
    let intent = crate::dialogue_codec::intent_row(&row).unwrap();
    let current = complete_original_context_control(
        pool.clone(),
        owner,
        PublishedAgent {
            agent_id: agent.agent_id,
            agent_version: agent.published_version_id,
        },
        intent.clone(),
        "capability.request@2",
        1,
        Some(stop::Timing::AfterSuccessor),
    )
    .await
    .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let facts = verified_root(&mut tx, owner, agent.agent_id, &current)
        .await
        .unwrap();
    assert_eq!(
        facts.current_intent.origin.surface,
        DialogueInputSurface::Capability
    );
    assert_eq!(facts.root_intent, intent);
    assert_eq!(
        facts.root_intent.origin.surface,
        DialogueInputSurface::Schedule
    );
    assert!(
        !facts.is_direct_user_root,
        "a completed capability does not convert scheduled work into user consent"
    );
    // The original schedule is still known even though it used the historical
    // public user_message projection. Source kind is never inferred from role.
    assert!(
        !verified_root(&mut tx, owner, agent.agent_id, &intent)
            .await
            .unwrap()
            .is_direct_user_root
    );
    let page = verified_roots_page(
        &mut tx,
        owner,
        agent.agent_id,
        &[intent.intent_id, current.intent_id],
    )
    .await
    .unwrap();
    assert_eq!(page.len(), 2);
    for value in page {
        assert!(!value.is_direct_user_root);
        assert_eq!(value.root_intent, intent);
    }
    tx.rollback().await.unwrap();
}
