use super::*;
use musteragent_definition::{
    AgentDefinition, AgentDefinitionStore, AgentKind, AgentLifecycle, PublishedAgentVersionPointer,
};
use musteragent_definition::{owned_agent::*, publication::OwnedAgentVersionDraft};
use musteragent_execution_context::{DialogueCursor, DialogueInputOrigin, DialogueInputSurface};
use musteragent_identity::{
    AgentDialogueId, AgentId, AgentKey, AgentVersionId, ContentId, LedgerOffset, OwnerSpaceId,
    VersionNumber,
};
use musteragent_postgres::{
    PostgresAdmissionStore, PostgresAgentDefinitionStore, PostgresAgentDialogueStore,
    PostgresDialogueExecutionStore, PostgresOwnedAgentStore,
};
use musteragent_runtime_application::*;

pub(super) async fn activate(
    pool: &sqlx::PgPool,
    owner: Actor,
    content: &dyn ContentStore,
    _store: &PostgresRuntimeApplicationStore,
    now: UtcTimestamp,
) -> OwnedAgentActivationReceipt {
    publish_host_agent(pool, owner, content).await;
    let foundation =
        musteragent_conformance::fixtures::agent::system_version(1, "assistant-base@1");
    activate_with_foundation(pool, owner, content, now, foundation).await
}

pub(super) async fn activate_with_foundation(
    pool: &sqlx::PgPool,
    owner: Actor,
    content: &dyn ContentStore,
    now: UtcTimestamp,
    foundation: musteragent_definition::AgentVersion,
) -> OwnedAgentActivationReceipt {
    let mat = foundation
        .material
        .as_ref()
        .expect("private execution fixture");
    let policy = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: mat.policy_content_ref.media_type.clone(),
            bytes: content
                .read(owner.tenant_id, &mat.policy_content_ref)
                .await
                .expect("private execution fixture"),
        })
        .await
        .expect("private execution fixture");
    let memory_policy = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: mat.memory_policy_content_ref.media_type.clone(),
            bytes: content
                .read(owner.tenant_id, &mat.memory_policy_content_ref)
                .await
                .expect("private execution fixture"),
        })
        .await
        .expect("private execution fixture");
    let id = AgentId::generate();
    let definition = AgentDefinition {
        agent_id: id,
        agent_key: AgentKey::new(format!("dialogue-{id}")).expect("private execution fixture"),
        system_slot: None,
        display_name: "Fixture agent".into(),
        kind: AgentKind::Custom,
        built_in: false,
        deletable: true,
        replaceable: true,
        default_for_product: false,
        lifecycle: AgentLifecycle::Active,
    };
    let persona = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: MediaType::new("text/plain").expect("private execution fixture"),
            bytes: b"Synthetic fixture instructions.".to_vec(),
        })
        .await
        .expect("private execution fixture");
    let version = OwnedAgentVersionDraft {
        agent_id: id,
        agent_version_id: AgentVersionId::generate(),
        version: VersionNumber::new(1).expect("private execution fixture"),
        supersedes: None,
        persona_version: VersionNumber::new(1).expect("private execution fixture"),
        persona_content_ref: persona,
        base_skill_refs: foundation.base_skill_refs.clone(),
        policy_ref: foundation.policy_ref.clone(),
        policy_content_ref: policy,
        memory_policy_ref: foundation.memory_policy_ref.clone(),
        memory_policy_content_ref: memory_policy,
        user_memory_collection_id: None,
        long_term_memory_collection_id: None,
        model_requirement_ref: foundation.model_requirement_ref.clone(),
        supported_runtime_contract: foundation.supported_runtime_contract.clone(),
        release_channel: foundation.release_channel,
        published_at: now,
        activated_at: now,
    }
    .prepare(&definition, ContentId::generate())
    .expect("private execution fixture");
    let reference = &version
        .version
        .material
        .as_ref()
        .expect("private execution fixture")
        .manifest_ref;
    content
        .put_at(
            reference.content_id,
            PutContent {
                tenant_id: owner.tenant_id,
                media_type: reference.media_type.clone(),
                bytes: version.manifest_bytes,
            },
        )
        .await
        .expect("private execution fixture");
    let registry = PostgresAgentDefinitionStore::new(pool.clone());
    registry
        .insert_definition(definition.clone())
        .await
        .expect("private execution fixture");
    registry
        .insert_version(version.version.clone())
        .await
        .expect("private execution fixture");
    registry
        .compare_and_swap_published(
            id,
            None,
            PublishedAgentVersionPointer {
                agent_id: id,
                agent_version_id: version.version.agent_version_id,
                version: 1,
                published_at: now,
                published_by: owner.principal_id,
            },
        )
        .await
        .expect("private execution fixture");
    let instructions = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: MediaType::new("text/plain").expect("private execution fixture"),
            bytes: b"Answer concisely in Chinese.".to_vec(),
        })
        .await
        .expect("private execution fixture");
    let configuration = OwnedAgentConfiguration::initial(
        id,
        "Fixture agent".into(),
        "".into(),
        None,
        Some(instructions),
    )
    .expect("private execution fixture");
    let space_id = OwnerSpaceId::generate();
    PostgresOwnedAgentStore::new(pool.clone())
        .activate(OwnedAgentActivation {
            actor: owner,
            command_id: CommandId::generate(),
            space_id,
            agent: OwnedAgent {
                agent_id: id,
                space_id,
                dialogue_id: AgentDialogueId::generate(),
                published_version_id: version.version.agent_version_id,
                status: OwnedAgentStatus::Active,
                revision: NonZeroU64::MIN,
                created_at: now,
                updated_at: now,
            },
            configuration,
            definition,
            version: version.version,
            published_pointer_version: NonZeroU64::MIN,
            make_default: true,
        })
        .await
        .expect("private execution fixture")
}

#[tokio::test]
async fn dialogue_admission_is_atomic_pinned_replayable_and_private() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("private execution fixture");
    let pool = pg.pool().await.expect("private execution fixture");
    migrate(&pool).await.expect("private execution fixture");
    let kid = DottedKey::new("dialogue.test.key").expect("private execution fixture");
    let keys = Arc::new(
        OpaqueRefKeyring::new(kid.clone(), [(kid, [51; 32])]).expect("private execution fixture"),
    );
    let blobs = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    );
    let content: Arc<dyn ContentStore> = Arc::new(blobs.clone());
    let (store, interactions) = production_audit_stores(&pool, keys, blobs.clone());
    let owner = Actor {
        tenant_id: TenantId::from_uuid(
            uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000002")
                .expect("private execution fixture"),
        )
        .expect("private execution fixture"),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let now = UtcTimestamp::from_unix_millis(1_780_000_000_000);
    let active = activate(&pool, owner, content.as_ref(), store.as_ref(), now).await;
    PostgresOwnedAgentStore::new(pool.clone())
        .configure(ConfigureOwnedAgent {
            actor: owner,
            command_id: CommandId::generate(),
            agent_id: active.agent.agent_id,
            expected_revision: NonZeroU64::MIN,
            change: AgentConfigurationChange::Permission {
                mode: musteragent_policy::PermissionMode::FullAccess,
            },
            origin: DialogueInputOrigin {
                surface: DialogueInputSurface::Desktop,
                installation_id: Some(musteragent_identity::DeviceInstallationId::generate()),
            },
            changed_at: now,
        })
        .await
        .expect("private execution fixture");

    let dialogue = PostgresDialogueExecutionStore::new(
        PostgresAdmissionStore::new(pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .with_content_store(blobs),
    );
    let input = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: MediaType::new("text/plain").expect("private execution fixture"),
            bytes: b"Summarize my feedback.".to_vec(),
        })
        .await
        .expect("private execution fixture");
    let intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(AcceptDialogueIntent {
            reply_target: None,

            input_attachments: vec![],
            actor: owner,
            command_id: CommandId::generate(),
            dialogue_id: active.agent.dialogue_id,
            content_ref: input,
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
        .expect("private execution fixture")
        .intent;
    assert!(
        dialogue
            .lookup(owner, intent.intent_id)
            .await
            .expect("private execution fixture")
            .is_none()
    );
    let preparation = dialogue
        .prepare(owner, intent.intent_id)
        .await
        .expect("private execution fixture");
    assert_eq!(preparation.agent.agent_id, active.agent.agent_id);
    let other = Actor {
        principal_id: PrincipalId::generate(),
        ..owner
    };
    assert_eq!(
        dialogue
            .prepare(other, intent.intent_id)
            .await
            .expect_err("rejected private execution fixture"),
        DialogueStoreError::Unavailable
    );
    let ids = musteragent_runtime_admission::AdmissionIdentities::derive(
        preparation.identities.command_id,
    )
    .expect("private execution fixture");
    let clock = Arc::new(MovingClock::new(now));
    let runtime = Arc::new(RuntimeApplication::new(
        musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
        musteragent_runtime_application::RuntimeApplicationPorts {
            store: store.clone(),
            snapshots: Arc::new(TestSnapshotResolver::production(&pool, content.clone())),
            content: content.clone(),
            encoder: Arc::new(ProtobufLedgerPayloadCodec),
            clock: clock.clone(),
            grant_authenticator: Arc::new(TestGrantAuthenticator),
            interactions,
        },
    ));
    let commit = runtime
        .prepare_dialogue_admission(
            preparation.clone(),
            AdmissionSelection::web(BTreeSet::new()),
            private_grant(owner, ids.run_id, now),
            now.checked_add_millis(30_000)
                .expect("private execution fixture"),
            metadata(CommandId::generate(), owner, "trace", now, None).trace_context,
        )
        .await
        .expect("private execution fixture");
    let snapshot = ProtobufSnapshotCodec
        .decode_runtime_snapshot(&commit.admission.batch.runtime_snapshot.encoded_snapshot)
        .expect("private execution fixture");
    validate_dialogue_snapshot_source(&snapshot, &preparation).expect("private execution fixture");
    assert!(
        preparation.product_release.is_none(),
        "ordinary input has no frozen publication"
    );
    assert_eq!(
        snapshot.product_release,
        private_grant(owner, ids.run_id, now).claims.product_release
    );
    // A frozen source can never silently switch to another publication. Its
    // source commitment is independently checked along with the release pin.
    let mut pinned = preparation.clone();
    pinned.product_release = snapshot.product_release.clone();
    pinned.intent.origin.surface = DialogueInputSurface::Schedule;
    let pinned_commit = runtime
        .prepare_dialogue_admission(
            pinned.clone(),
            AdmissionSelection::web(BTreeSet::new()),
            private_grant(owner, ids.run_id, now),
            now.checked_add_millis(30_000).expect("deadline"),
            metadata(CommandId::generate(), owner, "trace", now, None).trace_context,
        )
        .await
        .expect("prepare frozen publication");
    let mut pinned_snapshot = ProtobufSnapshotCodec
        .decode_runtime_snapshot(
            &pinned_commit
                .admission
                .batch
                .runtime_snapshot
                .encoded_snapshot,
        )
        .expect("decode pinned material");
    validate_dialogue_snapshot_source(&pinned_snapshot, &pinned).expect("exact frozen publication");
    pinned_snapshot.product_release = None;
    assert_eq!(
        validate_dialogue_snapshot_source(&pinned_snapshot, &pinned),
        Err(ErrorCode::RuntimeSnapshotInvalid)
    );
    assert_eq!(
        preparation.configuration.permission_mode,
        musteragent_policy::PermissionMode::FullAccess
    );
    assert_eq!(
        snapshot.policy.permission_mode,
        musteragent_policy::PermissionMode::ServiceManaged
    );
    let source_hash =
        dialogue_execution_source_hash(&preparation).expect("private execution fixture");
    assert_eq!(
        source_hash,
        dialogue_execution_source_hash(&preparation.clone()).expect("private execution fixture")
    );
    let mut altered_source = preparation.clone();
    altered_source
        .configuration
        .instructions_ref
        .as_mut()
        .expect("private execution fixture")
        .content_id = ContentId::generate();
    assert_ne!(
        source_hash,
        dialogue_execution_source_hash(&altered_source).expect("private execution fixture")
    );

    assert!(
        snapshot
            .prompt
            .sections
            .iter()
            .any(|s| s.section_key.as_str() == "agent.instructions")
    );
    let mut forged = commit.clone();
    forged.preparation.configuration.permission_revision =
        NonZeroU64::new(3).expect("private execution fixture");
    assert_eq!(
        dialogue
            .admit(forged)
            .await
            .expect_err("rejected private execution fixture"),
        DialogueStoreError::RevisionConflict
    );
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&pool)
        .await
        .expect("private execution fixture");
    assert_eq!(before, 0);
    let mut altered_snapshot = snapshot.clone();
    altered_snapshot.prompt.sections.retain(|section| {
        section.section_key.as_str() != musteragent_runtime_session::EXECUTION_SOURCE_SECTION
    });
    assert!(validate_dialogue_snapshot_source(&altered_snapshot, &preparation).is_err());
    let next_input = content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: MediaType::new("text/plain").expect("private execution fixture"),
            bytes: b"Also prepare a separate summary.".to_vec(),
        })
        .await
        .expect("private execution fixture");
    let second_intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(AcceptDialogueIntent {
            reply_target: None,

            input_attachments: vec![],
            actor: owner,
            command_id: CommandId::generate(),
            dialogue_id: active.agent.dialogue_id,
            content_ref: next_input,
            origin: DialogueInputOrigin {
                surface: DialogueInputSurface::Web,
                installation_id: None,
            },
            observed_cursor: intent.accepted_cursor,
            reply_to_intent_id: None,
            task_target: None,
            accepted_at: now,
        })
        .await
        .expect("private execution fixture")
        .intent;
    let second = dialogue
        .prepare(owner, second_intent.intent_id)
        .await
        .expect("private execution fixture");
    assert_ne!(
        second.identities.conversation_id,
        preparation.identities.conversation_id
    );
    assert_eq!(second.context_cut.through.seq, 1);
    assert_eq!(second.context_cut.entry_count, 1);
    assert_eq!(preparation.context_cut.through.seq, 0);
    assert_ne!(
        second.context_cut.source_chain_sha256,
        preparation.context_cut.source_chain_sha256
    );
    // Later input does not invalidate the accepted cursor of an earlier task.
    let first = dialogue
        .admit(commit.clone())
        .await
        .expect("private execution fixture");
    let again = dialogue
        .admit(commit)
        .await
        .expect("private execution fixture");
    assert_eq!(first.ledger.last_sequence, again.ledger.last_sequence);
    assert_eq!(
        dialogue
            .lookup(owner, intent.intent_id)
            .await
            .expect("private execution fixture")
            .expect("private execution fixture")
            .run_id,
        ids.run_id
    );
    assert!(
        runtime
            .get_conversation(GetConversation {
                actor: owner,
                conversation_id: preparation.identities.conversation_id
            })
            .await
            .is_err()
    );
    // Actual user decision membership cannot override the native purpose.
    let user:uuid::Uuid=sqlx::query_scalar("SELECT participant_id FROM conversation.participants WHERE conversation_id=$1 AND kind=1 AND principal_ref=$2")
        .bind(preparation.identities.conversation_id.into_uuid()).bind(owner.principal_id.into_uuid()).fetch_one(&pool).await.expect("private execution fixture");
    assert_eq!(user, preparation.identities.user.into_uuid());
    assert!(
        runtime
            .get_conversation(GetConversation {
                actor: owner,
                conversation_id: preparation.identities.conversation_id
            })
            .await
            .is_err()
    );
    assert!(
        runtime
            .get_run(musteragent_run::GetRun {
                actor: owner,
                run_id: ids.run_id
            })
            .await
            .is_err()
    );
    let visible = store
        .list_conversations(musteragent_conversation::ListConversations {
            actor: owner,
            conversation_id: None,
            workspace_id: None,
            lifecycle: None,
            page_size: 100,
            after: None,
        })
        .await
        .expect("private execution fixture");
    assert!(visible.conversations.is_empty());
    let projection = PostgresConversationProjection::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufConversationProjectionCodec),
        content.clone(),
        Arc::new(TestSubscriptionErrorMapper),
    );
    assert!(
        projection
            .get_conversation_projection(GetConversationProjection {
                actor: owner,
                conversation_id: preparation.identities.conversation_id,
                preferred_snapshot_id: None
            })
            .await
            .is_err()
    );
    let raw = PostgresLedgerStore::new(pool.clone())
        .load(
            LedgerCursor {
                conversation_id: preparation.identities.conversation_id,
                home_epoch: HomeEpoch::new(1).expect("private execution fixture"),
                after_conversation_seq: LedgerOffset::ZERO,
            },
            100,
        )
        .await
        .expect("private execution fixture");
    assert!(
        !raw.records.is_empty(),
        "worker retains native private context access"
    );
    let retained:i64=sqlx::query_scalar("SELECT count(*) FROM capability.runtime_snapshot_content_references WHERE runtime_snapshot_id=$1").bind(ids.runtime_snapshot_id.into_uuid()).fetch_one(&pool).await.expect("private execution fixture");
    assert!(
        retained >= 3,
        "source commitment, instructions and host policy are retained"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&pool)
        .await
        .expect("private execution fixture");
    assert_eq!(count, 1);
}

pub(super) fn private_grant(
    actor: Actor,
    run_id: RunId,
    now: UtcTimestamp,
) -> SignedExecutionGrant {
    let mut signed = signed_test_grant(actor, run_id, now);
    signed.claims.permission_mode = Some(musteragent_policy::PermissionMode::ServiceManaged);
    signed
}
