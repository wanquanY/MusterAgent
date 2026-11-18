//! Same process, connection pool and snapshot service; deterministic authority.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::{DialogueCursor, DialogueInputOrigin, DialogueInputSurface};
use musteragent_identity::ProductId;
use musteragent_postgres::{
    PostgresAdmissionStore, PostgresAgentDialogueStore, PostgresDialogueExecutionStore,
};
use musteragent_runtime_application::*;

#[tokio::test]
async fn private_admission_does_not_change_conversation_replay_or_pinned_materials() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("shared admission fixture");
    let pool = pg.pool().await.expect("shared admission fixture");
    migrate(&pool).await.expect("shared admission fixture");
    let key = DottedKey::new("shared.product.test").expect("shared admission fixture");
    let keys = Arc::new(
        OpaqueRefKeyring::new(key.clone(), [(key, [38; 32])]).expect("shared admission fixture"),
    );
    let blobs = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    );
    let content: Arc<dyn ContentStore> = Arc::new(blobs.clone());
    let (store, interactions) = production_audit_stores(&pool, keys, blobs.clone());
    let now = UtcTimestamp::from_unix_millis(1_780_000_000_000);
    let owner = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    publish_host_agent(&pool, owner, content.as_ref()).await;
    let compose = || {
        Arc::new(RuntimeApplication::new(
            musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
            RuntimeApplicationPorts {
                store: store.clone(),
                snapshots: Arc::new(TestSnapshotResolver::production(&pool, content.clone())),
                content: content.clone(),
                encoder: Arc::new(ProtobufLedgerPayloadCodec),
                clock: Arc::new(MovingClock::new(now)),
                grant_authenticator: Arc::new(TestGrantAuthenticator),
                interactions: interactions.clone(),
            },
        ))
    };
    let runtime = compose();
    let conversation = ConversationId::generate();
    runtime
        .create_conversation(
            CommandEnvelope::new(
                metadata(CommandId::generate(), owner, "shared-create", now, None),
                CreateConversation {
                    conversation_id: conversation,
                    workspace_id: None,
                    kind: ConversationKind::Direct,
                    title: None,
                    control_home: RuntimeHome::Cloud,
                },
            )
            .expect("shared admission fixture"),
        )
        .await
        .expect("shared admission fixture");
    let view = runtime
        .get_conversation(GetConversation {
            actor: owner,
            conversation_id: conversation,
        })
        .await
        .expect("shared admission fixture");
    let participant = view
        .participants
        .iter()
        .find(|p| p.kind == ParticipantKind::User)
        .expect("shared admission fixture")
        .participant_id;
    let command_id = CommandId::generate();
    let run_id = derive_admission_run_id(command_id).expect("shared admission fixture");
    let command = CommandEnvelope::new(
        metadata(
            command_id,
            owner,
            "shared-work",
            now,
            Some(ConversationGuard {
                conversation_id: conversation,
                expected_home_epoch: view.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(view.conversation.next_conversation_seq.get() - 1)
                        .expect("shared admission fixture"),
                ),
            }),
        ),
        SubmitMessage {
            project_task_target: None,
            message_id: MessageId::generate(),
            participant_id: participant,
            content: InputContent::InlineUtf8(
                InlineInput::new("Keep my original conversation contract")
                    .expect("shared admission fixture"),
            ),
            attachment_refs: vec![],
            requested_model_class: ModelClass::new("chat.balanced")
                .expect("shared admission fixture"),
            requested_features: RequestedFeatures::default(),
            execution_scope: ExecutionScope::Tenant {
                tenant_id: owner.tenant_id,
            },
        },
    )
    .expect("shared admission fixture");
    runtime
        .submit_message_authorized(
            command.clone(),
            signed_test_grant(owner, run_id, now),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("shared admission fixture");
    let conversation_bytes = snapshot_bytes(&pool, run_id).await;
    let conversation_snapshot = ProtobufSnapshotCodec
        .decode_runtime_snapshot(&conversation_bytes)
        .expect("shared admission fixture");
    assert_eq!(
        conversation_snapshot.product_release,
        Some(musteragent_tool::compiled_product_release(
            ProductId::KERNEL
        ))
    );
    let active = super::private_execution::activate_with_foundation(
        &pool,
        owner,
        content.as_ref(),
        now,
        musteragent_conformance::fixtures::agent::system_version(1, "assistant-base@1"),
    )
    .await;
    let input = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: MediaType::new("text/plain").expect("shared admission fixture"),
            bytes: b"Private execution in the same runtime".to_vec(),
        })
        .await
        .expect("shared admission fixture");
    let intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(AcceptDialogueIntent {
            reply_target: None,

            actor: owner,
            command_id: CommandId::generate(),
            dialogue_id: active.agent.dialogue_id,
            content_ref: input,
            input_attachments: vec![],
            origin: DialogueInputOrigin {
                surface: DialogueInputSurface::Web,
                installation_id: None,
            },
            observed_cursor: DialogueCursor { epoch: 1, seq: 0 },
            reply_to_intent_id: None,
            task_target: None,
            accepted_at: now,
        })
        .await
        .expect("shared admission fixture")
        .intent;
    let dialogue = PostgresDialogueExecutionStore::new(
        PostgresAdmissionStore::new(pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .with_content_store(blobs),
    );
    let preparation = dialogue
        .prepare(owner, intent.intent_id)
        .await
        .expect("shared admission fixture");
    let dialogue_run = preparation
        .identities
        .run_id()
        .expect("shared admission fixture");
    let grant = super::private_execution::private_grant(owner, dialogue_run, now);
    let commit = runtime
        .prepare_dialogue_admission(
            preparation,
            AdmissionSelection::web(BTreeSet::new()),
            grant,
            now.checked_add_millis(30_000)
                .expect("shared admission fixture"),
            metadata(CommandId::generate(), owner, "shared-private", now, None).trace_context,
        )
        .await
        .expect("shared admission fixture");
    dialogue
        .admit(commit)
        .await
        .expect("shared admission fixture");
    let private_bytes = snapshot_bytes(&pool, dialogue_run).await;
    let private = ProtobufSnapshotCodec
        .decode_runtime_snapshot(&private_bytes)
        .expect("shared admission fixture");
    assert_eq!(
        private.product_release,
        Some(musteragent_tool::compiled_product_release(
            ProductId::KERNEL
        ))
    );
    assert_ne!(
        conversation_snapshot.agent, private.agent,
        "private Agent selection cannot replace the conversation Agent"
    );
    assert_ne!(conversation_snapshot.prompt, private.prompt);
    assert_eq!(
        conversation_snapshot.policy.permission_mode,
        signed_test_grant(owner, run_id, now)
            .claims
            .permission_mode
            .expect("ordinary permission")
    );
    assert_eq!(
        private.policy.permission_mode,
        musteragent_policy::PermissionMode::ServiceManaged
    );
    drop(runtime);
    let restarted = compose();
    let replay = restarted
        .submit_message_authorized(
            command,
            signed_test_grant(owner, run_id, now),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("shared admission fixture");
    assert_eq!(replay.disposition, CommandDisposition::Duplicate);
    assert_eq!(snapshot_bytes(&pool, run_id).await, conversation_bytes);
    assert_eq!(snapshot_bytes(&pool, dialogue_run).await, private_bytes);
    assert_ne!(
        Sha256::digest(&conversation_bytes),
        Sha256::digest(&private_bytes)
    );
    pool.close().await;
}

async fn snapshot_bytes(pool: &sqlx::PgPool, run: musteragent_identity::RunId) -> Vec<u8> {
    sqlx::query_scalar("SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE run_id=$1")
        .bind(run.into_uuid())
        .fetch_one(pool)
        .await
        .expect("shared admission fixture")
}
