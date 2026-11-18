use super::*;

fn query(f: &Fixture) -> ListAgentMemoryDrafts {
    ListAgentMemoryDrafts {
        tenant_id: f.save.tenant_id,
        owner_principal_id: f.save.principal_id,
        agent_id: f.save.agent_id,
        status: AgentMemoryDraftStatus::Draft,
        limit: 2,
        cursor: None,
    }
}

fn read(f: &Fixture, draft_id: MemoryDraftId) -> GetAgentMemoryDraftContent {
    GetAgentMemoryDraftContent {
        tenant_id: f.save.tenant_id,
        principal_id: f.save.principal_id,
        agent_id: f.save.agent_id,
        draft_id,
    }
}

#[tokio::test]
async fn original_postgres_pages_are_complete_bounded_and_identity_scoped() {
    let f = Fixture::new().await;
    let mut expected = Vec::new();
    for (index, time) in [200, 210, 210, 210, 220, 220, 230].into_iter().enumerate() {
        expected.push(
            f.app()
                .save(SaveAgentMemoryDraftContent {
                    idempotency_key: IdempotencyKey::new(format!("draft:page:{index}"))
                        .expect("key"),
                    saved_at: UtcTimestamp::from_unix_millis(time),
                    ..f.save.clone()
                })
                .await
                .expect("save")
                .draft,
        );
    }
    expected.sort_by_key(|draft| (std::cmp::Reverse(draft.updated_at), draft.draft_id));
    let other_owner = PrincipalId::generate();
    let foreign = f
        .app()
        .save(SaveAgentMemoryDraftContent {
            principal_id: other_owner,
            ..f.save.clone()
        })
        .await
        .expect("other owner's work")
        .draft;
    let mut current = query(&f);
    let mut seen = Vec::new();
    for index in 0..4 {
        let page = f.app().list_drafts(current.clone()).await.expect("page");
        assert!(page.drafts.len() <= 2);
        seen.extend(page.drafts);
        assert_eq!(page.next_cursor.is_some(), index < 3);
        current.cursor = page.next_cursor;
    }
    assert_eq!(seen, expected);
    assert_eq!(
        f.app()
            .list_drafts(ListAgentMemoryDrafts {
                owner_principal_id: other_owner,
                ..query(&f)
            })
            .await
            .expect("other page")
            .drafts,
        vec![foreign]
    );
    current.cursor = f
        .app()
        .list_drafts(query(&f))
        .await
        .expect("first")
        .next_cursor;
    assert!(current.cursor.is_some());
    for altered in [
        ListAgentMemoryDrafts {
            tenant_id: TenantId::generate(),
            ..current.clone()
        },
        ListAgentMemoryDrafts {
            owner_principal_id: other_owner,
            ..current.clone()
        },
        ListAgentMemoryDrafts {
            agent_id: AgentId::generate(),
            ..current.clone()
        },
        ListAgentMemoryDrafts {
            status: AgentMemoryDraftStatus::Submitted,
            ..current.clone()
        },
        ListAgentMemoryDrafts {
            limit: 0,
            ..current.clone()
        },
        ListAgentMemoryDrafts {
            limit: 51,
            ..current.clone()
        },
        ListAgentMemoryDrafts {
            limit: u32::MAX,
            ..current.clone()
        },
    ] {
        assert_eq!(
            f.app().list_drafts(altered.clone()).await,
            Err(AgentMemoryDraftApplicationError::InvalidQuery)
        );
        assert!(f.store().list_drafts(altered).await.is_err());
    }
    for altered in [
        GetAgentMemoryDraftContent {
            tenant_id: TenantId::generate(),
            ..read(&f, expected[0].draft_id)
        },
        GetAgentMemoryDraftContent {
            principal_id: other_owner,
            ..read(&f, expected[0].draft_id)
        },
        GetAgentMemoryDraftContent {
            agent_id: AgentId::generate(),
            ..read(&f, expected[0].draft_id)
        },
    ] {
        assert_eq!(
            f.app().read_draft(altered).await,
            Err(AgentMemoryDraftApplicationError::NotFound)
        );
    }
    for altered in [
        ListAgentMemoryDrafts {
            tenant_id: TenantId::generate(),
            ..query(&f)
        },
        ListAgentMemoryDrafts {
            agent_id: AgentId::generate(),
            ..query(&f)
        },
    ] {
        assert!(
            f.app()
                .list_drafts(altered)
                .await
                .expect("scoped empty")
                .drafts
                .is_empty()
        );
    }
    f.pool.close().await;
}

#[tokio::test]
async fn unsubmitted_input_is_recovered_after_original_postgres_pool_reopens() {
    let f = Fixture::new().await;
    let saved = f.app().save(f.save.clone()).await.expect("save").draft;
    f.pool.close().await;
    let reopened = f
        .postgres
        .pool()
        .await
        .expect("new pool to this fixture only");
    let store = Arc::new(PostgresMemoryEvolutionStore::new(reopened.clone()));
    // Reopening the database client must retain the independent blob service.
    // This is an in-memory object-store fixture, not PostgreSQL inline storage
    // or proof of production object-service restart durability.
    let app = AgentMemoryDraftApplicationService::new(
        store.clone(),
        Fixture::content(&reopened, f.blobs.clone()),
    );
    let page = app
        .list_drafts(query(&f))
        .await
        .expect("discover with no Draft UUID");
    assert_eq!(page.drafts, vec![saved]);
    let restored = app
        .read_draft(read(&f, page.drafts[0].draft_id))
        .await
        .expect("read original content through the retained blob service");
    assert_eq!(restored.document.claim, f.save.edited_claim);
    assert!(
        app.list_reviews(f.save.tenant_id, f.save.principal_id, f.save.agent_id, 10)
            .await
            .expect("reviews")
            .is_empty()
    );
    let head = store
        .load_active_head(f.save.tenant_id, f.save.memory_id)
        .await
        .expect("head")
        .expect("published head");
    assert_eq!(
        head.version.memory_version_id,
        f.save.expected_baseline_memory_version_id
    );
    reopened.close().await;
}

#[tokio::test]
async fn lifecycle_pages_do_not_evaluate_publish_or_lose_stale_user_input() {
    let f = Fixture::new().await;
    for index in 0..3 {
        f.app()
            .save(SaveAgentMemoryDraftContent {
                idempotency_key: IdempotencyKey::new(format!("draft:state:{index}")).expect("key"),
                ..f.save.clone()
            })
            .await
            .expect("save");
    }
    let mut current = query(&f);
    current.limit = 1;
    let first = f
        .app()
        .list_drafts(current.clone())
        .await
        .expect("first page");
    let selected = first.drafts[0].draft_id;
    let submitted = f.app().submit(f.submit(selected)).await.expect("submit");
    current.cursor = first.next_cursor;
    let next = f
        .app()
        .list_drafts(current)
        .await
        .expect("cursor row has left Draft lifecycle");
    assert_ne!(next.drafts[0].draft_id, selected);
    assert_eq!(
        f.app()
            .list_drafts(ListAgentMemoryDrafts {
                status: AgentMemoryDraftStatus::Submitted,
                ..query(&f)
            })
            .await
            .expect("submitted")
            .drafts,
        submitted.drafts
    );
    assert_eq!(
        f.app().confirm(f.confirm(submitted.batch_id)).await,
        Err(AgentMemoryDraftApplicationError::EvaluationPending)
    );
    // Exercise the original deterministic evaluator, not a fabricated passing
    // report. Even after this transition, publication requires confirmation.
    let result = EvolutionCandidateEvaluationService::new(f.store(), f.content.clone())
        .process_next(
            &EvidenceManifestCandidateEvaluator,
            f.save.principal_id,
            UtcTimestamp::from_unix_millis(400),
        )
        .await
        .expect("original evaluation");
    assert!(
        matches!(result, CandidateEvaluationWorkResult::Completed(ref value) if value.evaluation.passed)
    );
    assert!(
        f.app()
            .list_drafts(ListAgentMemoryDrafts {
                status: AgentMemoryDraftStatus::Published,
                ..query(&f)
            })
            .await
            .expect("no implicit publication")
            .drafts
            .is_empty()
    );
    let published = f
        .app()
        .confirm(f.confirm(submitted.batch_id))
        .await
        .expect("explicit confirmation");
    assert_eq!(published.disposition, AgentMemoryDraftDisposition::Applied);
    let after = f
        .app()
        .read_draft(read(&f, selected))
        .await
        .expect("published input");
    assert_eq!(after.draft.status, AgentMemoryDraftStatus::Published);
    assert_eq!(
        f.app()
            .list_drafts(ListAgentMemoryDrafts {
                status: AgentMemoryDraftStatus::Published,
                ..query(&f)
            })
            .await
            .expect("published page")
            .drafts,
        vec![after.draft]
    );
    let stale = f
        .app()
        .read_draft(read(&f, next.drafts[0].draft_id))
        .await
        .expect("head drift cannot erase saved work");
    assert_eq!(
        stale.draft.baseline_memory_version_id,
        f.save.expected_baseline_memory_version_id
    );
    assert_eq!(stale.document.claim, f.save.edited_claim);
    f.app()
        .submit(SubmitAgentMemoryDrafts {
            idempotency_key: IdempotencyKey::new("draft:conflict").expect("key"),
            submitted_at: UtcTimestamp::from_unix_millis(600),
            ..f.submit(stale.draft.draft_id)
        })
        .await
        .expect_err("a stale batch is rejected, not submitted");
    let conflict = f
        .app()
        .read_draft(read(&f, stale.draft.draft_id))
        .await
        .expect("persisted conflict");
    assert_eq!(conflict.draft.status, AgentMemoryDraftStatus::Conflict);
    assert_eq!(conflict.document, stale.document);
    assert_eq!(
        f.app()
            .list_drafts(ListAgentMemoryDrafts {
                status: AgentMemoryDraftStatus::Conflict,
                ..query(&f)
            })
            .await
            .expect("conflict page")
            .drafts,
        vec![conflict.draft]
    );
    f.pool.close().await;
}

#[tokio::test]
async fn readback_rejects_oversize_wrong_media_corruption_and_invalid_claims() {
    let f = Fixture::new().await;
    let saved = f.app().save(f.save.clone()).await.expect("save").draft;
    sqlx::query(
        "UPDATE memory.agent_memory_drafts SET content_byte_length=66560 WHERE draft_id=$1",
    )
    .bind(saved.draft_id.into_uuid())
    .execute(&f.pool)
    .await
    .expect("isolated bounds fault");
    assert_eq!(
        f.app().read_draft(read(&f, saved.draft_id)).await,
        Err(AgentMemoryDraftApplicationError::InvalidContent)
    );
    sqlx::query("UPDATE memory.agent_memory_drafts SET content_byte_length=$1::numeric,content_media_type='text/plain' WHERE draft_id=$2")
        .bind(saved.content_ref.byte_length.to_string()).bind(saved.draft_id.into_uuid()).execute(&f.pool).await.expect("isolated media fault");
    assert_eq!(
        f.app().read_draft(read(&f, saved.draft_id)).await,
        Err(AgentMemoryDraftApplicationError::ContentIntegrity)
    );
    sqlx::query("UPDATE memory.agent_memory_drafts SET content_media_type=$1,content_sha256=$2 WHERE draft_id=$3")
        .bind(EXPERIENCE_CLAIM_MEDIA_TYPE).bind(vec![17_u8; 32]).bind(saved.draft_id.into_uuid()).execute(&f.pool).await.expect("isolated digest fault");
    assert!(f.app().read_draft(read(&f, saved.draft_id)).await.is_err());
    let invalid = f
        .content
        .put(PutContent {
            tenant_id: f.save.tenant_id,
            media_type: MediaType::new(EXPERIENCE_CLAIM_MEDIA_TYPE).expect("media"),
            bytes: b"{}".to_vec(),
        })
        .await
        .expect("invalid schema fixture");
    sqlx::query("UPDATE memory.agent_memory_drafts SET content_id=$1,content_byte_length=$2::numeric,content_sha256=$3 WHERE draft_id=$4")
        .bind(invalid.content_id.into_uuid()).bind(invalid.byte_length.to_string())
        .bind(invalid.sha256.into_bytes().to_vec()).bind(saved.draft_id.into_uuid()).execute(&f.pool).await.expect("isolated schema fault");
    assert_eq!(
        f.app().read_draft(read(&f, saved.draft_id)).await,
        Err(AgentMemoryDraftApplicationError::ContentIntegrity)
    );
    f.pool.close().await;
}
