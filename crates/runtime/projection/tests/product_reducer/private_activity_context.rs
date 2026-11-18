use super::*;
use musteragent_identity::{DreamJobId, GoalId, GoalPursuitId};
use musteragent_projection::{
    CatalogAttentionAudience, CatalogEventContext, adapt_activity_event,
    ledger_record_reaches_product_content,
};

async fn assert_private_context(event: ActivityEvent, references: Vec<ContentRef>) {
    let tenant = TenantId::generate();
    let conversation = ConversationId::generate();
    let epoch = HomeEpoch::new(1).unwrap();
    let mut fact = record(
        tenant,
        conversation,
        epoch,
        1,
        Some(RunId::generate()),
        None,
        DomainEvent::Activity(event.clone()),
    );
    fact.metadata.activity_id = Some(ActivityId::generate());
    for reference in &references {
        assert!(!ledger_record_reaches_product_content(&fact, reference));
    }
    let drafts = adapt_activity_event(
        CatalogEventContext {
            control_home: RuntimeHome::Cloud,
            home_epoch: epoch,
            tenant_id: tenant,
            conversation_id: conversation,
            record_id: fact.metadata.record_id,
            audience: CatalogAttentionAudience::AllParticipants,
            occurred_at: fact.metadata.occurred_at,
            committed_at: fact.metadata.committed_at,
        },
        &event,
    )
    .unwrap();
    assert!(drafts.is_empty());

    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer =
        ProductProjectionReducer::new(tenant, conversation, epoch, None, BTreeMap::new()).unwrap();
    reducer.apply(fact, content.clone()).await.unwrap();
    reducer.advance_through(LedgerOffset::new(1)).unwrap();
    let snapshot = reducer
        .finish(
            content.clone(),
            LedgerOffset::new(1),
            UtcTimestamp::from_unix_millis(2),
        )
        .await
        .unwrap();
    // Checkpoint recovery must preserve the same private/public boundary.
    let recovered =
        ProductProjectionReducer::new(tenant, conversation, epoch, Some(snapshot), BTreeMap::new())
            .unwrap()
            .finish(
                content,
                LedgerOffset::new(1),
                UtcTimestamp::from_unix_millis(3),
            )
            .await
            .unwrap();
    assert!(recovered.messages.is_empty());
    assert!(recovered.activities.is_empty());
    assert!(recovered.runs.is_empty());
    assert!(recovered.content_manifest.is_empty());
}

#[tokio::test]
async fn maintenance_context_stays_private_across_replay_and_checkpoint() {
    let manifest = content_ref(b"private maintenance manifest");
    let model_input = content_ref(b"private maintenance input");
    assert_private_context(
        ActivityEvent::MaintenanceContextProjected {
            dream_job_id: DreamJobId::generate(),
            input_manifest_ref: manifest.clone(),
            model_input_ref: model_input.clone(),
        },
        vec![manifest, model_input],
    )
    .await;
}

#[tokio::test]
async fn standing_goal_context_stays_private_across_replay_and_checkpoint() {
    let objective = content_ref(b"private goal objective");
    let evaluation = content_ref(b"private evaluation summary");
    let input = content_ref(b"private goal input");
    assert_private_context(
        ActivityEvent::StandingGoalContextProjected {
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            objective_ref: objective.clone(),
            previous_evaluation_summary_ref: Some(evaluation.clone()),
            input_refs: vec![input.clone()],
        },
        vec![objective, evaluation, input],
    )
    .await;
}

#[tokio::test]
async fn standing_goal_judge_context_stays_private_across_replay_and_checkpoint() {
    let input = content_ref(b"private judge input");
    assert_private_context(
        ActivityEvent::StandingGoalJudgeContextProjected {
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            source_run_id: RunId::generate(),
            judge_input_ref: input.clone(),
        },
        vec![input],
    )
    .await;
}
