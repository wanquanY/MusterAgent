//! Neutral source commitments and host source revocation use the real baseline.
use super::*;
use musteragent_goal::source_binding::*;
use musteragent_persistence_codec::ProtobufSnapshotCodec;
use musteragent_postgres::{
    PostgresAdmissionStore, PostgresDialogueExecutionStore, PostgresOwnerEventSourceStore,
};
use std::sync::Arc;

#[tokio::test]
async fn accepted_execution_source_hash_binds_original_input_and_configuration() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let ready = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let command = input(owner, ready.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    let accepted = PostgresAgentDialogueStore::new(pool.clone())
        .accept(command)
        .await
        .unwrap();
    let store = PostgresDialogueExecutionStore::new(PostgresAdmissionStore::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
    ));
    let source = store
        .prepare(owner, accepted.intent.intent_id)
        .await
        .unwrap();
    let hash = dialogue_execution_source_hash(&source).unwrap();
    assert_eq!(
        hash,
        dialogue_execution_source_hash(
            &store
                .prepare(owner, accepted.intent.intent_id)
                .await
                .unwrap()
        )
        .unwrap()
    );
    for change in [
        (|s: &mut DialogueExecutionPreparation| s.intent.command_id = CommandId::generate())
            as fn(&mut DialogueExecutionPreparation),
        |s| s.intent.observed_cursor.seq += 1,
        |s| {
            s.intent.accepted_at =
                UtcTimestamp::from_unix_millis(s.intent.accepted_at.unix_millis() + 1)
        },
        |s| s.intent.reply_to_intent_id = Some(UserIntentId::generate()),
        |s| s.agent.revision = NonZeroU64::new(s.agent.revision.get() + 1).unwrap(),
        |s| s.agent.space_id = OwnerSpaceId::generate(),
        |s| s.configuration.profile.display_name.push('x'),
        |s| s.intent.content_ref.sha256 = Sha256::digest(b"changed accepted content"),
    ] {
        let mut changed = source.clone();
        change(&mut changed);
        assert_ne!(hash, dialogue_execution_source_hash(&changed).unwrap());
    }
    let mut invalid = source.clone();
    invalid.intent.owner_id = PrincipalId::generate();
    assert!(dialogue_execution_source_hash(&invalid).is_err());
    assert!(
        store
            .prepare(actor(), accepted.intent.intent_id)
            .await
            .is_err()
    );
    pool.close().await;
}

#[tokio::test]
async fn host_source_binding_cas_requires_scope_and_monotonic_revocation() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let ready = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let store = PostgresOwnerEventSourceStore::new(pool.clone());
    let mut binding = GoalSourceBinding {
        binding_id: OwnerApplicationBindingId::generate(),
        subject: GoalSourceSubject {
            tenant_id: owner.tenant_id,
            owner_id: owner.principal_id,
            agent_id: ready.agent.agent_id,
        },
        guard: GoalSourceBindingGuard {
            revision: NonZeroU64::MIN,
            authority_epoch: NonZeroU64::MIN,
        },
        status: GoalSourceBindingStatus::Enabled,
    };
    store
        .compare_and_swap_source_binding(binding.clone(), None)
        .await
        .unwrap();
    assert!(
        store
            .compare_and_swap_source_binding(binding.clone(), None)
            .await
            .is_err()
    );
    let original = binding.guard;
    binding.guard.revision = NonZeroU64::new(2).unwrap();
    binding.status = GoalSourceBindingStatus::Disabled;
    assert!(
        store
            .compare_and_swap_source_binding(binding.clone(), Some(original))
            .await
            .is_err(),
        "revocation must advance the authority epoch"
    );
    binding.guard.authority_epoch = NonZeroU64::new(2).unwrap();
    let mut foreign = binding.clone();
    foreign.subject.owner_id = PrincipalId::generate();
    assert!(
        store
            .compare_and_swap_source_binding(foreign, Some(original))
            .await
            .is_err()
    );
    store
        .compare_and_swap_source_binding(binding.clone(), Some(original))
        .await
        .unwrap();
    assert!(
        store
            .compare_and_swap_source_binding(binding.clone(), Some(original))
            .await
            .is_err()
    );
    let disabled = binding.guard;
    binding.guard = GoalSourceBindingGuard {
        revision: NonZeroU64::new(3).unwrap(),
        authority_epoch: NonZeroU64::new(3).unwrap(),
    };
    binding.status = GoalSourceBindingStatus::Removed;
    store
        .compare_and_swap_source_binding(binding.clone(), Some(disabled))
        .await
        .unwrap();
    let persisted:(i64,i64,i16)=sqlx::query_as("SELECT revision,authority_epoch,status FROM agent.goal_source_bindings WHERE binding_id=$1").bind(binding.binding_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(persisted, (3, 3, 3));
    let removed = binding.guard;
    binding.guard = GoalSourceBindingGuard {
        revision: NonZeroU64::new(4).unwrap(),
        authority_epoch: NonZeroU64::new(4).unwrap(),
    };
    binding.status = GoalSourceBindingStatus::Enabled;
    assert!(
        store
            .compare_and_swap_source_binding(binding, Some(removed))
            .await
            .is_err(),
        "removed bindings cannot be revived"
    );
    pool.close().await;
}
