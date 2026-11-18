//! New immutable provenance adds no second erasure engine. Use the original
//! ContentCiphertext job and verify actual bytes, not an optimistic UI status.
use super::*;

#[tokio::test]
async fn device_artifact_original_privacy_owner_covers_root_caption_and_receipt_without_fk_deadlock()
 {
    let f = Fixture::admitted().await;
    let e = f.export(None, true);
    let receipt = f.publish(&e).await;
    let privacy = PostgresPrivacyErasureStore::new(f.pool.clone());
    let roots = privacy
        .device_artifact_erasure_roots(f.actor, Some(f.custody.source.scope.agent_id))
        .await
        .unwrap();
    assert_eq!(
        roots.len(),
        3,
        "TXT, caption and persisted ACK are separate exact roots"
    );
    assert!(roots.contains(&e.manifest.entries[0].version.content_ref));
    assert!(roots.contains(&receipt.delivery_receipt.result_ref));
    assert!(
        !roots.contains(&e.manifest.entries[0].producer.result_ref),
        "private producer result was never copied into PG"
    );
    assert_eq!(
        roots,
        privacy
            .device_artifact_erasure_roots(f.actor, None)
            .await
            .unwrap()
    );
    assert!(
        privacy
            .device_artifact_erasure_roots(actor(), Some(f.custody.source.scope.agent_id))
            .await
            .is_err()
    );
    let foreign = Actor {
        principal_id: PrincipalId::generate(),
        ..f.actor
    };
    assert!(
        privacy
            .device_artifact_erasure_roots(foreign, None)
            .await
            .unwrap()
            .is_empty()
    );
    let base = now(&f.pool).await - 100;
    let request = ErasureRequest {
        schema_version: DottedKey::new("test.device-artifact-content-erasure.v1").unwrap(),
        erasure_job_id: ErasureJobId::generate(),
        tenant_id: f.actor.tenant_id,
        requested_by: f.actor,
        root_resource_refs: roots.iter().cloned().map(ResourceRef::Content).collect(),
        retention_policy_ref: DataRetentionPolicyRef::new("privacy.content.standard@1").unwrap(),
        idempotency_key: IdempotencyKey::new("erase-device-artifact-content").unwrap(),
        requested_at: UtcTimestamp::from_unix_millis(base),
        complete_by: UtcTimestamp::from_unix_millis(base + 60_000),
    };
    privacy.create(request.clone()).await.unwrap();
    let plan = ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1).unwrap(),
        request.root_resource_refs.clone(),
        vec![ErasureDataClass::ContentCiphertext],
        vec![],
        request.fingerprint().unwrap(),
        UtcTimestamp::from_unix_millis(base + 1),
    )
    .unwrap();
    privacy.append_plan(f.actor.tenant_id, plan).await.unwrap();
    for (sequence, status) in [
        (2, ErasureJobStatus::Planned),
        (3, ErasureJobStatus::Fenced),
    ] {
        privacy
            .append_transition(
                f.actor.tenant_id,
                request.erasure_job_id,
                ErasureTransition {
                    sequence: ErasureTransitionSequence::new(sequence).unwrap(),
                    status,
                    occurred_at: UtcTimestamp::from_unix_millis(base + sequence as i64),
                },
            )
            .await
            .unwrap();
    }
    let completed = ErasureExecutor::new(&privacy, f.content.as_ref())
        .execute(
            f.actor.tenant_id,
            request.erasure_job_id,
            UtcTimestamp::from_unix_millis(now(&f.pool).await),
        )
        .await
        .unwrap();
    assert_eq!(completed.job.status(), Some(ErasureJobStatus::Completed));
    for reference in &roots {
        assert!(f.content.read(f.actor.tenant_id, reference).await.is_err());
        let erased:bool=sqlx::query_scalar("SELECT state=3 AND storage_ref IS NULL AND storage_ref_protected IS NULL FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2")
            .bind(f.actor.tenant_id.into_uuid()).bind(reference.content_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
        assert!(
            erased,
            "physical Content owner must have completed, even though proof FKs remain"
        );
    }
    assert!(
        f.lookup(&e).await.is_err(),
        "a physically erased receipt cannot be reconstructed from metadata"
    );
    assert!(
        f.publisher
            .publish_device_artifacts(f.actor, &f.target, &e.delivery, &e.manifest, &e.contents)
            .await
            .is_err()
    );
    assert!(
        f.artifacts()
            .describe(
                f.actor,
                f.custody.source.scope.dialogue_id,
                *receipt.delivery_receipt.entry_ids.last().unwrap(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    let retained: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM artifact.device_versions),
        (SELECT count(*) FROM agent.dialogue_tool_publication_groups)",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        retained,
        (1, 1),
        "immutable evidence survives; ContentCiphertext does not claim projection/account deletion"
    );
    assert_eq!(
        privacy
            .device_artifact_erasure_roots(f.actor, None)
            .await
            .unwrap(),
        roots,
        "retry discovers the exact tombstoned closure without resurrecting bytes"
    );
    let replay = ErasureExecutor::new(&privacy, f.content.as_ref())
        .execute(
            f.actor.tenant_id,
            request.erasure_job_id,
            UtcTimestamp::from_unix_millis(now(&f.pool).await),
        )
        .await
        .unwrap();
    assert_eq!(replay.disposition, ErasureAppendDisposition::Duplicate);
}

#[tokio::test]
async fn device_artifact_deleted_agent_denies_read_and_receipt_but_retains_privacy_ownership() {
    let f = Fixture::admitted().await;
    let e = f.export(None, true);
    let receipt = f.publish(&e).await;
    let privacy = PostgresPrivacyErasureStore::new(f.pool.clone());
    let before = privacy
        .device_artifact_erasure_roots(f.actor, None)
        .await
        .unwrap();
    sqlx::query("UPDATE agent.owned_agents SET status='deleted' WHERE agent_id=$1")
        .bind(f.custody.source.scope.agent_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(f.lookup(&e).await.is_err());
    assert!(
        f.publisher
            .publish_device_artifacts(f.actor, &f.target, &e.delivery, &e.manifest, &e.contents)
            .await
            .is_err()
    );
    assert!(
        f.artifacts()
            .describe(
                f.actor,
                f.custody.source.scope.dialogue_id,
                *receipt.delivery_receipt.entry_ids.last().unwrap(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    assert_eq!(
        privacy
            .device_artifact_erasure_roots(f.actor, None)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        before
            .iter()
            .map(|r| r.content_id)
            .collect::<BTreeSet<_>>()
            .len(),
        before.len()
    );
}
