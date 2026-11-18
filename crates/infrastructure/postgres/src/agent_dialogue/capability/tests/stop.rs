//! Original terminal staging / root-proof owners plus real migrated PostgreSQL.
//! Covers both serializations of stop vs capability successor publication.
use super::*;
use crate::{PostgresDialogueActivityProjection, PostgresDialogueExecutionCancellationStore};
#[derive(Clone, Copy)]
pub(super) enum Timing {
    BeforeSuccessor,
    AfterSuccessor,
}
#[tokio::test]
async fn stop_root_cut_prevents_capability_successor_without_undoing_its_terminal() {
    scenario(Timing::BeforeSuccessor).await;
}
#[tokio::test]
async fn stale_observation_stop_captures_capability_successor_from_the_same_accepted_root() {
    scenario(Timing::AfterSuccessor).await;
}
async fn scenario(timing: Timing) {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap()
        .agent;
    let request = input(owner, agent.dialogue_id);
    register_content(&pool, owner, &request.content_ref).await;
    let original = PostgresAgentDialogueStore::new(pool.clone())
        .accept(request)
        .await
        .unwrap()
        .intent;
    let result = complete_original_context_control(
        pool.clone(),
        owner,
        super::PublishedAgent {
            agent_id: agent.agent_id,
            agent_version: agent.published_version_id,
        },
        original,
        "capability.request@2",
        1,
        Some(timing),
    )
    .await;
    assert_eq!(result.is_none(), matches!(timing, Timing::BeforeSuccessor));
    let mut later = input(owner, agent.dialogue_id);
    later.accepted_at = UtcTimestamp::from_unix_millis(10000);
    register_content(&pool, owner, &later.content_ref).await;
    let later = PostgresAgentDialogueStore::new(pool.clone())
        .accept(later)
        .await
        .unwrap()
        .intent;
    let mut tx = pool.begin().await.unwrap();
    crate::dialogue_handoff::progress::lock_account(&mut tx, owner)
        .await
        .unwrap();
    assert!(
        crate::dialogue_execution::lock_unresolved_intent(&mut tx, owner, later.intent_id)
            .await
            .is_ok(),
        "a new user intent is outside the stopped root cut"
    );
    tx.rollback().await.unwrap();
}
pub(super) async fn observe(
    pool: &sqlx::PgPool,
    actor: Actor,
    agent: AgentId,
    dialogue: AgentDialogueId,
    timing: Timing,
) -> DialogueExecutionCancellationRequest {
    PostgresDialogueActivityProjection::new(pool.clone())
        .refresh(agent)
        .await
        .unwrap();
    let home = PostgresOwnedAgentStore::new(pool.clone())
        .home_snapshot(actor)
        .await
        .unwrap()
        .unwrap();
    let activity = home
        .agents
        .into_iter()
        .find(|v| v.agent.agent_id == agent)
        .unwrap()
        .activity
        .unwrap();
    let request = DialogueExecutionCancellationRequest {
        actor,
        command_id: CommandId::generate(),
        agent_id: agent,
        dialogue_id: dialogue,
        observed_activity_revision: activity.revision,
        task_intent_id: None,
    };
    if matches!(timing, Timing::BeforeSuccessor) {
        assert_eq!(activity.foreground_execution_count, 1);
        assert_eq!(
            PostgresDialogueExecutionCancellationStore::new(pool.clone())
                .request(request.clone())
                .await
                .unwrap()
                .target_count,
            1
        );
    }
    request
}
pub(super) async fn assert_suppressed(pool: &sqlx::PgPool, interaction: InteractionId, run: RunId) {
    let children: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.dialogue_user_intents WHERE continuation_interaction_id=$1",
    )
    .bind(interaction.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        children, 0,
        "a retained stop suppresses new Capability input, not the real completed interaction"
    );
    let terminal: i16 = sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
        .bind(run.into_uuid())
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(terminal, 5);
    let response: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM approval.interaction_responses WHERE interaction_id=$1",
    )
    .bind(interaction.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(response, 1);
}
pub(super) async fn assert_late_successor(
    pool: &sqlx::PgPool,
    request: DialogueExecutionCancellationRequest,
    successor: &UserIntent,
) {
    let store = PostgresDialogueExecutionCancellationStore::new(pool.clone());
    // The observed parent Run has really completed before its successor exists.
    // Its stale observation remains immutable, and the new input is an exact
    // interaction continuation rather than another user-message entry.
    let observed: Vec<(uuid::Uuid, Option<uuid::Uuid>)> = sqlx::query_as(
        "SELECT intent_id,run_id FROM agent.dialogue_execution_control_targets WHERE agent_id=$1 AND activity_revision=$2 ORDER BY target_id",
    )
    .bind(request.agent_id.into_uuid())
    .bind(i64::try_from(request.observed_activity_revision).unwrap())
    .fetch_all(pool)
    .await
    .unwrap();
    let source: (String, Option<Vec<u8>>, uuid::Uuid, uuid::Uuid) = sqlx::query_as(
        "SELECT e.kind,e.content_ref,e.interaction_id,e.source_intent_id FROM agent.agent_dialogue_entries e WHERE e.entry_id=$1",
    )
    .bind(successor.source_entry_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(source.0, "interaction");
    assert!(source.1.is_none());
    assert_eq!(
        Some(source.3),
        successor.reply_to_intent_id.map(UserIntentId::into_uuid)
    );
    let source_interaction: uuid::Uuid = sqlx::query_scalar(
        "SELECT continuation_interaction_id FROM agent.dialogue_user_intents WHERE intent_id=$1",
    )
    .bind(successor.intent_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(source.2, source_interaction);
    let receipt = store.request(request.clone()).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    let proof = verified_root_id(
        &mut tx,
        request.actor,
        request.agent_id,
        successor.intent_id,
    )
    .await
    .unwrap();
    crate::dialogue_handoff::progress::lock_account(&mut tx, request.actor)
        .await
        .unwrap();
    if proof.is_direct_user_root {
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].0, proof.root_intent.intent_id.into_uuid());
        let original_run = observed[0].1.expect("the observed original Cloud Run");
        let terminal: i16 = sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
            .bind(original_run)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            terminal, 5,
            "a completed parent remains historical, not restarted"
        );
        let frozen: Vec<(uuid::Uuid, Option<uuid::Uuid>)> = sqlx::query_as(
            "SELECT intent_id,run_id FROM agent.dialogue_execution_cancellation_targets WHERE cancellation_id=$1 ORDER BY intent_id",
        )
        .bind(request.command_id.into_uuid())
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        let mut expected = vec![
            (proof.root_intent.intent_id.into_uuid(), Some(original_run)),
            (successor.intent_id.into_uuid(), None),
        ];
        expected.sort();
        assert_eq!(
            frozen, expected,
            "freeze the exact observed parent and new pending successor"
        );
        assert_eq!(
            receipt.target_count, 2,
            "old observed Run and same-root pending successor are both frozen"
        );
        assert_eq!(
            crate::dialogue_execution::lock_unresolved_intent(
                &mut tx,
                request.actor,
                successor.intent_id
            )
            .await,
            Err(DialogueStoreError::Inactive)
        );
        tx.rollback().await.unwrap();
        PostgresDialogueActivityProjection::new(pool.clone())
            .refresh(request.agent_id)
            .await
            .unwrap();
        let home = PostgresOwnedAgentStore::new(pool.clone())
            .home_snapshot(request.actor)
            .await
            .unwrap()
            .unwrap();
        let activity = home
            .agents
            .into_iter()
            .find(|value| value.agent.agent_id == request.agent_id)
            .unwrap()
            .activity
            .unwrap();
        assert_eq!(
            activity.foreground_execution_count, 1,
            "only the pending successor remains active"
        );
        assert_eq!(activity.pending_cancellation_count, 1);
        let cloud = store.delivery_page(&request, 64).await.unwrap();
        assert_eq!(cloud.len(), 1);
        let outcome:(String,String,i16)=sqlx::query_as("SELECT q.status,o.reason,p.stage FROM agent.dialogue_intent_admissions q JOIN agent.dialogue_admission_outcomes o USING(intent_id) JOIN agent.dialogue_admission_progress p USING(intent_id) WHERE q.intent_id=$1").bind(successor.intent_id.into_uuid()).fetch_one(pool).await.unwrap();
        assert_eq!(outcome, ("cancelled".into(), "user_requested".into(), 5));
        assert_eq!(store.request(request).await.unwrap().target_count, 2);
    } else {
        assert_eq!(
            receipt.target_count, 0,
            "Schedule-root capability work is not foreground control scope"
        );
        assert!(
            crate::dialogue_execution::lock_unresolved_intent(
                &mut tx,
                request.actor,
                successor.intent_id
            )
            .await
            .is_ok()
        );
        tx.rollback().await.unwrap();
        assert!(store.delivery_page(&request, 64).await.unwrap().is_empty());
        let state: String = sqlx::query_scalar(
            "SELECT status FROM agent.dialogue_intent_admissions WHERE intent_id=$1",
        )
        .bind(successor.intent_id.into_uuid())
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(state, "pending");
    }
}
