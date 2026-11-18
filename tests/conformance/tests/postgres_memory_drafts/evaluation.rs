use super::*;

fn scope(f: &Fixture) -> AgentMemoryDraftEvaluationScope {
    AgentMemoryDraftEvaluationScope {
        tenant_id: f.save.tenant_id,
        owner_principal_id: f.save.principal_id,
        agent_id: f.save.agent_id,
    }
}
fn clock() -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(400)
}

#[tokio::test]
async fn scoped_postgres_evaluation_cannot_consume_or_starve_on_another_owners_older_work() {
    let f = Fixture::new().await;
    let foreign_owner = PrincipalId::generate();
    let foreign = f
        .app()
        .save(SaveAgentMemoryDraftContent {
            principal_id: foreign_owner,
            ..f.save.clone()
        })
        .await
        .expect("foreign draft")
        .draft;
    f.app()
        .submit(SubmitAgentMemoryDrafts {
            principal_id: foreign_owner,
            submitted_at: UtcTimestamp::from_unix_millis(250),
            ..f.submit(foreign.draft_id)
        })
        .await
        .expect("older foreign submission");
    let saved = f
        .app()
        .save(f.save.clone())
        .await
        .expect("owned save")
        .draft;
    let submitted = f
        .app()
        .submit(f.submit(saved.draft_id))
        .await
        .expect("owned submission");
    let selected = f
        .store()
        .load_next_draft_candidate(scope(&f))
        .await
        .expect("original scoped query")
        .expect("owned work");
    assert_eq!(selected.draft, submitted.drafts[0]);
    selected.validate_for(scope(&f)).expect("shared invariant");
    for altered in [
        AgentMemoryDraftEvaluationScope {
            tenant_id: TenantId::generate(),
            ..scope(&f)
        },
        AgentMemoryDraftEvaluationScope {
            owner_principal_id: PrincipalId::generate(),
            ..scope(&f)
        },
        AgentMemoryDraftEvaluationScope {
            agent_id: AgentId::generate(),
            ..scope(&f)
        },
    ] {
        assert!(
            f.store()
                .load_next_draft_candidate(altered)
                .await
                .expect("bounded foreign query")
                .is_none()
        );
    }
    let evaluator = EvolutionCandidateEvaluationService::new(f.store(), f.content.clone());
    assert!(
        matches!(evaluator.process_next_draft(scope(&f), &EvidenceManifestCandidateEvaluator, f.save.principal_id, clock).await.expect("evaluate owned"), CandidateEvaluationWorkResult::Completed(value) if value.evaluation.passed)
    );
    assert!(
        f.store()
            .load_next_draft_candidate(scope(&f))
            .await
            .expect("owned drained")
            .is_none()
    );
    assert_eq!(
        f.store()
            .load_next_draft_candidate(AgentMemoryDraftEvaluationScope {
                owner_principal_id: foreign_owner,
                ..scope(&f)
            })
            .await
            .expect("foreign pending")
            .expect("untouched")
            .draft
            .draft_id,
        foreign.draft_id
    );
    let head = f
        .store()
        .load_active_head(f.save.tenant_id, f.save.memory_id)
        .await
        .expect("head")
        .expect("memory");
    assert_eq!(
        head.version.memory_version_id,
        f.save.expected_baseline_memory_version_id
    );
    let approvals: i64 = sqlx::query_scalar("SELECT count(*) FROM evolution.approvals")
        .fetch_one(&f.pool)
        .await
        .expect("approvals");
    assert_eq!(approvals, 0);
    f.pool.close().await;
}

#[tokio::test]
async fn scoped_postgres_work_reopens_and_concurrent_original_evaluations_are_idempotent() {
    let f = Fixture::new().await;
    let draft = f.app().save(f.save.clone()).await.expect("save").draft;
    let submitted = f
        .app()
        .submit(f.submit(draft.draft_id))
        .await
        .expect("submit");
    f.pool.close().await;
    let pool = f.postgres.pool().await.expect("reopen isolated pool");
    let store = Arc::new(PostgresMemoryEvolutionStore::new(pool.clone()));
    let content = Fixture::content(&pool, f.blobs.clone());
    let pending = store
        .load_next_draft_candidate(scope(&f))
        .await
        .expect("restart discover")
        .expect("pending");
    let first = EvolutionCandidateEvaluationService::new(store.clone(), content.clone());
    let second = EvolutionCandidateEvaluationService::new(store.clone(), content.clone());
    let (a, b) = tokio::join!(
        first.evaluate(
            pending.candidate.clone(),
            &EvidenceManifestCandidateEvaluator,
            f.save.principal_id,
            clock()
        ),
        second.evaluate(
            pending.candidate,
            &EvidenceManifestCandidateEvaluator,
            f.save.principal_id,
            clock()
        ),
    );
    assert!(a.expect("first").evaluation.passed);
    assert!(b.expect("idempotent second").evaluation.passed);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM evolution.evaluations")
        .fetch_one(&pool)
        .await
        .expect("reports");
    assert_eq!(count, 1);
    let app = AgentMemoryDraftApplicationService::new(store, content);
    let reviewed = app
        .review(f.save.tenant_id, f.save.principal_id, submitted.batch_id)
        .await
        .expect("review");
    assert_eq!(
        reviewed.items[0].draft.status,
        AgentMemoryDraftStatus::Submitted
    );
    assert!(
        reviewed.items[0]
            .proposal
            .evaluation
            .as_ref()
            .expect("report")
            .passed
    );
    pool.close().await;
}
