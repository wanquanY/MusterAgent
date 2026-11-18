mod fixture_artifact;
mod result_reply;

use super::*;
use musteragent_ledger::AttemptFence;
use musteragent_postgres::{PostgresAccountStore, PostgresDialoguePublicationStore};
use musteragent_run::WorkerId;

struct Fixture {
    _pg: IsolatedPostgres,
    pool: PgPool,
    owner: Actor,
    activation: OwnedAgentActivationReceipt,
    content: Option<std::sync::Arc<musteragent_postgres::PostgresContentStore>>,
    intent: UserIntent,
    command: PublicEntryAppend,
}

/// Storage conformance fixture installs actual canonical identities, with the
/// same FK, scope and live-lease constraints as admission. It does not call a
/// model and makes no claim that the end-to-end execution adapter is tested.
async fn fixture() -> Fixture {
    fixture_with_reply_contract(false).await
}
async fn fixture_with_reply_contract(current: bool) -> Fixture {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let fixture_content = current.then(|| super::receipt::attachment_fixture::content_store(&pool));
    let activation = if let Some(content) = fixture_content.as_ref() {
        super::receipt::attachment_fixture::activate_with_content(&pool, owner, content.clone())
            .await
    } else {
        PostgresOwnedAgentStore::new(pool.clone())
            .activate(prepared(&pool, owner, true).await)
            .await
            .unwrap()
    };
    let input = input(owner, activation.agent.dialogue_id);
    register_content(&pool, owner, &input.content_ref).await;
    let intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(input)
        .await
        .unwrap()
        .intent;
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
    .bind(activation.agent.agent_id.into_uuid())
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
        VALUES($1,$2,$3,$4,$5,$6,'intent_coordination',1,1,1,$7,1,1,1)")
        .bind(conversation.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid())
        .bind(activation.agent.agent_id.into_uuid()).bind(intent.dialogue_id.into_uuid()).bind(intent.intent_id.into_uuid())
        .bind(activation.agent.published_version_id.into_uuid()).execute(&mut *tx).await.unwrap();
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
    let body = content("useful answer");
    register_content(&pool, owner, &body).await;
    let fixture = Fixture {
        _pg: pg,
        pool,
        owner,
        activation,
        content: fixture_content,
        intent,
        command: PublicEntryAppend {
            delivery_id: CommandId::generate(),
            fence,
            expected_last_run_sequence: RunSeq::new(1).unwrap(),
            expected_permission_revision: NonZeroU64::MIN,
            purpose: DialoguePublicationPurpose::Reply,
            content: PublicDeliveryContent::Message(body),
        },
    };
    fixture_artifact::append_admission(&fixture, &fixture.command.fence).await;
    fixture
}

#[tokio::test]
async fn explicit_delivery_commits_one_public_entry_and_one_account_event() {
    let f = fixture().await;
    let store = PostgresDialoguePublicationStore::new(f.pool.clone());
    let (a, b) = tokio::join!(
        store.append(f.command.clone()),
        store.append(f.command.clone())
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.entry, b.entry);
    assert_ne!(a.replayed, b.replayed);
    assert_eq!(a.entry.author_agent_id, Some(f.activation.agent.agent_id));
    assert_eq!(a.entry.source_intent_id, Some(f.intent.intent_id));
    assert_eq!(a.entry.cursor, DialogueCursor { epoch: 1, seq: 2 });
    assert_eq!(count(&f.pool, "dialogue_publications").await, 1);
    assert_eq!(count(&f.pool, "agent_dialogue_entries").await, 2);
    let tail = PostgresAccountStore::new(f.pool.clone())
        .read_changes(
            f.owner,
            AccountCursor {
                space_id: f.activation.space.space_id,
                epoch: 1,
                seq: 2,
            },
            10,
        )
        .await
        .unwrap();
    assert_eq!(tail.changes.len(), 1);
    assert_eq!(
        tail.changes[0].change,
        AccountChange::PublicEntryPublished(a.entry.clone())
    );
    let history = PostgresAgentDialogueStore::new(f.pool.clone())
        .read_history(
            f.owner,
            f.intent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        history.entries[1].client_command_id, None,
        "an Agent output is not a replay of the user's optimistic input"
    );
    assert_eq!(history.entries[1].entry, a.entry);
    let mut changed = f.command.clone();
    changed.purpose = DialoguePublicationPurpose::ImportantUpdate;
    assert_eq!(
        store.append(changed).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::IdempotencyConflict
        ))
    );
    // A committed delivery is recoverable after its original Attempt ended.
    sqlx::query("DELETE FROM runtime.leases WHERE run_id=$1")
        .bind(f.command.fence.run_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(store.append(f.command).await.unwrap().replayed);
}

#[tokio::test]
async fn publication_rejects_stale_fence_wrong_scope_missing_files_and_revoked_permission() {
    let f = fixture().await;
    let store = PostgresDialoguePublicationStore::new(f.pool.clone());
    let mut bad = f.command.clone();
    bad.fence.fencing_token = FencingToken::new(2).unwrap();
    assert_eq!(
        store.append(bad).await,
        Err(DialoguePublicationError::StaleFence)
    );
    let mut bad = f.command.clone();
    bad.fence.run_id = RunId::generate();
    assert_eq!(
        store.append(bad).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::Unavailable
        ))
    );
    let mut bad = f.command.clone();
    bad.content = PublicDeliveryContent::Message(content("not stored"));
    assert_eq!(
        store.append(bad).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::Unavailable
        ))
    );
    let mut bad = f.command.clone();
    bad.content = PublicDeliveryContent::Artifact {
        artifact_id: ArtifactId::generate(),
        artifact_version_id: ArtifactVersionId::generate(),
    };
    assert_eq!(
        store.append(bad).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::Unavailable
        ))
    );
    sqlx::query(
        "UPDATE agent.owned_agent_configurations SET permission_revision=2 WHERE agent_id=$1",
    )
    .bind(f.activation.agent.agent_id.into_uuid())
    .execute(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        store.append(f.command.clone()).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::RevisionConflict
        ))
    );
    sqlx::query(
        "UPDATE agent.owned_agent_configurations SET permission_revision=1 WHERE agent_id=$1",
    )
    .bind(f.activation.agent.agent_id.into_uuid())
    .execute(&f.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE runtime.runs SET cancel_requested_at_ms=1 WHERE run_id=$1")
        .bind(f.command.fence.run_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    assert_eq!(
        store.append(f.command).await,
        Err(DialoguePublicationError::Cancelled)
    );
    assert_eq!(count(&f.pool, "dialogue_publications").await, 0);
    assert_eq!(count(&f.pool, "agent_dialogue_entries").await, 1);
    assert_eq!(count(&f.pool, "owner_outbox").await, 2);
}

#[tokio::test]
async fn real_artifact_and_interaction_references_publish_without_fabricated_cards() {
    let f = fixture().await;
    let store = PostgresDialoguePublicationStore::new(f.pool.clone());
    let (artifact, version, bytes) =
        fixture_artifact::committed_artifact(&f, &f.command.fence).await;
    let mut command = fixture_artifact::current_command(&f).await;
    command.purpose = DialoguePublicationPurpose::Result;
    command.content = PublicDeliveryContent::Artifact {
        artifact_id: artifact,
        artifact_version_id: version,
    };
    let card = store.append(command.clone()).await.unwrap();
    assert_eq!(card.entry.content, command.content.public_content());
    // A real object is still insufficient when it belongs to another branch.
    // Source identity is immutable, so create a distinct object, never rewrite it.
    let foreign_artifact = ArtifactId::generate();
    let foreign_version = ArtifactVersionId::generate();
    sqlx::query("INSERT INTO artifact.artifacts
        (artifact_id,tenant_id,conversation_id,activity_id,created_by_run_id,kind,lifecycle,current_version,title,created_at_ms,updated_at_ms)
        VALUES($1,$2,$3,$4,$5,1,2,1,'Other report.md',1,1)")
        .bind(foreign_artifact.into_uuid()).bind(f.owner.tenant_id.into_uuid())
        .bind(ConversationId::generate().into_uuid()).bind(ActivityId::generate().into_uuid())
        .bind(RunId::generate().into_uuid()).execute(&f.pool).await.unwrap();
    sqlx::query("INSERT INTO artifact.artifact_versions
        (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by,committed_at_ms)
        VALUES($1,$2,1,$3,$4,$5,1)")
        .bind(foreign_version.into_uuid()).bind(foreign_artifact.into_uuid()).bind(bytes.content_id.into_uuid())
        .bind(bytes.sha256.into_bytes().to_vec()).bind(ParticipantId::generate().into_uuid())
        .execute(&f.pool).await.unwrap();
    command.delivery_id = CommandId::generate();
    command.content = PublicDeliveryContent::Artifact {
        artifact_id: foreign_artifact,
        artifact_version_id: foreign_version,
    };
    assert_eq!(
        store.append(command).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::Unavailable
        ))
    );

    let interaction = InteractionId::generate();
    let prompt = content("Which date works?");
    register_content(&f.pool, f.owner, &prompt).await;
    let invocation = InvocationId::generate();
    sqlx::query("INSERT INTO tool.tool_invocations
        (invocation_id,tenant_id,conversation_id,run_id,requested_by_attempt_id,tool_call_id,tool_ref,
         capability_provider_id,status,effect,approval_policy,retry_contract,idempotency_key,
         request_content_id,request_hash,deadline_ms)
        SELECT $1,tenant_id,conversation_id,run_id,$2,'question','request_user_input@1',$3,
         1,1,1,1,$4,$5,$6,60000 FROM runtime.runs WHERE run_id=$7")
        .bind(invocation.into_uuid()).bind(f.command.fence.attempt_id.into_uuid())
        .bind(CapabilityProviderId::generate().into_uuid()).bind(invocation.to_string())
        .bind(prompt.content_id.into_uuid()).bind(prompt.sha256.into_bytes().to_vec())
        .bind(f.command.fence.run_id.into_uuid()).execute(&f.pool).await.unwrap();
    sqlx::query("INSERT INTO approval.interactions
        (interaction_id,tenant_id,conversation_id,activity_id,run_id,requested_by_invocation_id,kind,status,
         prompt_content_id,response_schema_key,response_schema_hash,requested_at_ms)
        SELECT $1,tenant_id,conversation_id,activity_id,run_id,$2,1,1,$3,'answer.v1',$4,1
        FROM runtime.runs WHERE run_id=$5")
        .bind(interaction.into_uuid()).bind(invocation.into_uuid()).bind(prompt.content_id.into_uuid())
        .bind(Sha256::digest("response schema").into_bytes().to_vec()).bind(f.command.fence.run_id.into_uuid())
        .execute(&f.pool).await.unwrap();
    let mut question = fixture_artifact::current_command(&f).await;
    question.delivery_id = CommandId::generate();
    question.purpose = DialoguePublicationPurpose::RequiredDecision;
    question.content = PublicDeliveryContent::Interaction(interaction);
    let card = store.append(question.clone()).await.unwrap();
    assert_eq!(
        card.entry.content,
        AgentDialogueContent::Interaction {
            interaction_id: interaction
        }
    );
    sqlx::query("UPDATE approval.interactions SET status=3,closed_at_ms=2 WHERE interaction_id=$1")
        .bind(interaction.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    question.delivery_id = CommandId::generate();
    assert_eq!(
        store.append(question).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::Unavailable
        ))
    );
    assert_eq!(count(&f.pool, "dialogue_publications").await, 2);
}

#[tokio::test]
async fn runtime_artifacts_without_successful_invocation_are_not_published() {
    let f = fixture().await;
    let store = PostgresDialoguePublicationStore::new(f.pool.clone());
    let invocation = fixture_artifact::requested_invocation(&f, &f.command.fence).await;
    // Preserve the original malformed positive fixture as a negative. A content
    // row, Artifact/version, or merely requested/running Tool is not a result.
    for source in [None, Some(invocation)] {
        let (artifact_id, artifact_version_id) =
            fixture_artifact::uncommitted_artifact(&f, source).await;
        let mut command = fixture_artifact::current_command(&f).await;
        command.content = PublicDeliveryContent::Artifact {
            artifact_id,
            artifact_version_id,
        };
        command.purpose = DialoguePublicationPurpose::Result;
        assert_eq!(
            store.append(command.clone()).await,
            Err(DialoguePublicationError::Store(
                DialogueStoreError::Unavailable
            ))
        );
        if source.is_some() {
            fixture_artifact::start_invocation(&f, &f.command.fence, invocation).await;
            command.delivery_id = CommandId::generate();
            assert_eq!(
                store.append(command).await,
                Err(DialoguePublicationError::Store(
                    DialogueStoreError::Unavailable
                ))
            );
        }
    }
    assert_eq!(count(&f.pool, "dialogue_publications").await, 0);
    assert_eq!(count(&f.pool, "agent_dialogue_entries").await, 1);
    assert_eq!(count(&f.pool, "owner_outbox").await, 2);
}

#[tokio::test]
async fn successful_artifact_from_another_run_of_the_same_agent_is_not_published() {
    let f = fixture().await;
    let foreign = fixture_artifact::another_run(&f).await;
    let (artifact_id, artifact_version_id, _) =
        fixture_artifact::committed_artifact(&f, &foreign).await;
    let owned: bool = sqlx::query_scalar("SELECT resource.agent_artifact_owned($1,$2,$3)")
        .bind(f.owner.tenant_id.into_uuid())
        .bind(f.activation.agent.agent_id.into_uuid())
        .bind(artifact_id.into_uuid())
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert!(
        owned,
        "rejection must isolate Run provenance, not missing Agent ownership"
    );
    let mut command = fixture_artifact::current_command(&f).await;
    command.purpose = DialoguePublicationPurpose::Result;
    command.content = PublicDeliveryContent::Artifact {
        artifact_id,
        artifact_version_id,
    };
    let store = PostgresDialoguePublicationStore::new(f.pool.clone());
    assert_eq!(
        store.append(command).await,
        Err(DialoguePublicationError::Store(
            DialogueStoreError::Unavailable
        ))
    );
    assert_eq!(count(&f.pool, "dialogue_publications").await, 0);
    assert_eq!(count(&f.pool, "agent_dialogue_entries").await, 1);
    assert_eq!(count(&f.pool, "owner_outbox").await, 2);
}
