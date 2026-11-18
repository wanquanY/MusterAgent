use super::*;
use musteragent_testkit::IsolatedPostgres;

fn delivery() -> WorkItemDevicePublishedDelivery {
    let invocation_id = InvocationId::generate();
    WorkItemDevicePublishedDelivery {
        invocation_id,
        fingerprint: Sha256::digest(b"frozen original request"),
        entry_ids: vec![DialogueEntryId::generate()],
        result_ref: ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &invocation_id.into_uuid(),
                b"public-tool-receipt@1",
            ))
            .unwrap(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        },
    }
}
fn complete() -> WorkItemDevicePublicationCut {
    let mut deliveries = vec![delivery(), delivery()];
    deliveries.sort_by_key(|d| d.invocation_id);
    WorkItemDevicePublicationCut { deliveries }
}

#[test]
fn only_unsuccessful_observations_may_omit_unacknowledged_publications() {
    let complete = complete();
    let observed = WorkItemDevicePublicationCut {
        deliveries: vec![complete.deliveries[1].clone()],
    };
    assert!(
        verify_observed_cut(WorkItemDeviceRunOutcome::Succeeded, &observed, &complete).is_err()
    );
    assert!(verify_observed_cut(WorkItemDeviceRunOutcome::Succeeded, &complete, &complete).is_ok());
    for outcome in [
        WorkItemDeviceRunOutcome::Failed,
        WorkItemDeviceRunOutcome::Cancelled,
    ] {
        assert!(verify_observed_cut(outcome, &observed, &complete).is_ok());
        assert!(
            verify_observed_cut(outcome, &WorkItemDevicePublicationCut::default(), &complete)
                .is_ok()
        );
        let mut altered = observed.clone();
        altered.deliveries[0].fingerprint = Sha256::digest(b"wrong request");
        assert!(verify_observed_cut(outcome, &altered, &complete).is_err());
        altered = observed.clone();
        altered.deliveries[0].entry_ids[0] = DialogueEntryId::generate();
        assert!(verify_observed_cut(outcome, &altered, &complete).is_err());
        altered = observed.clone();
        altered.deliveries[0].result_ref.sha256 = Sha256::digest(b"wrong result");
        assert!(verify_observed_cut(outcome, &altered, &complete).is_err());
        altered.deliveries = vec![delivery()];
        assert!(verify_observed_cut(outcome, &altered, &complete).is_err());
    }
    assert_ne!(
        observed.commitment().unwrap(),
        complete.commitment().unwrap()
    );
}

/// Minimal custody-qualified tables exercise the real SQL selector. This is
/// boundary evidence, not an installed Native-host acceptance journey.
#[tokio::test]
async fn native_failed_cut_validates_every_cloud_fact_including_lost_ack_groups() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    sqlx::raw_sql("CREATE SCHEMA agent; CREATE SCHEMA artifact;
      CREATE TABLE agent.dialogue_tool_publication_groups(invocation_id uuid PRIMARY KEY,source_run_id uuid,tenant_id uuid,source_intent_id uuid,execution_home text,device_subject_kind text,device_request_fingerprint bytea,device_snapshot_hash bytea,device_grant_proof_hash bytea,device_receipt_content_id uuid,entry_count smallint);
      CREATE TABLE artifact.content_refs(content_id uuid,tenant_id uuid,media_type text,byte_length numeric,content_hash bytea);
      CREATE TABLE agent.dialogue_publications(source_tool_invocation_id uuid,source_run_id uuid,tenant_id uuid,owner_id uuid,source_intent_id uuid,execution_home text,device_subject_kind text,entry_id uuid);
      CREATE TABLE agent.agent_dialogue_entries(entry_id uuid,tenant_id uuid,owner_id uuid,epoch bigint,seq bigint);")
        .execute(&pool).await.unwrap();
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let run = RunId::generate();
    let intent = UserIntentId::generate();
    let snapshot = Sha256::digest(b"snapshot");
    let grant = Sha256::digest(b"grant");
    let complete = complete();
    for d in &complete.deliveries {
        sqlx::query("INSERT INTO agent.dialogue_tool_publication_groups VALUES($1,$2,$3,$4,'device','intent',$5,$6,$7,$8,1)")
            .bind(d.invocation_id.into_uuid()).bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(intent.into_uuid()).bind(d.fingerprint.into_bytes().to_vec()).bind(snapshot.into_bytes().to_vec()).bind(grant.into_bytes().to_vec()).bind(d.result_ref.content_id.into_uuid()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO artifact.content_refs VALUES($1,$2,'application/json',2,$3)")
            .bind(d.result_ref.content_id.into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(d.result_ref.sha256.into_bytes().to_vec())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO agent.dialogue_publications VALUES($1,$2,$3,$4,$5,'device','intent',$6)",
        )
        .bind(d.invocation_id.into_uuid())
        .bind(run.into_uuid())
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(intent.into_uuid())
        .bind(d.entry_ids[0].into_uuid())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent.agent_dialogue_entries VALUES($1,$2,$3,1,1)")
            .bind(d.entry_ids[0].into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
    }
    let observed = WorkItemDevicePublicationCut {
        deliveries: vec![complete.deliveries[1].clone()],
    };
    let before = observed.canonical_bytes().unwrap();
    let mut tx = pool.begin().await.unwrap();
    let recovered = verify_device_publication_cut(
        &mut tx,
        actor,
        run,
        snapshot,
        grant,
        WorkItemDeviceRunOutcome::Failed,
        &observed,
    )
    .await
    .unwrap();
    assert_eq!(recovered, complete);
    assert_eq!(before, observed.canonical_bytes().unwrap());
    assert_eq!(
        verify_device_publication_cut(
            &mut tx,
            actor,
            run,
            snapshot,
            grant,
            WorkItemDeviceRunOutcome::Cancelled,
            &observed,
        )
        .await
        .unwrap(),
        complete
    );
    // This same complete set passes the unchanged report/success proof.
    assert_eq!(
        verify_device_publication_cut(
            &mut tx,
            actor,
            run,
            snapshot,
            grant,
            WorkItemDeviceRunOutcome::Succeeded,
            &recovered
        )
        .await
        .unwrap(),
        complete
    );
    assert!(
        verify_device_publication_cut(
            &mut tx,
            actor,
            run,
            snapshot,
            grant,
            WorkItemDeviceRunOutcome::Succeeded,
            &observed
        )
        .await
        .is_err()
    );
    let unknown = complete.deliveries[0].invocation_id.into_uuid();
    for column in ["device_snapshot_hash", "device_grant_proof_hash"] {
        let original = if column == "device_snapshot_hash" {
            snapshot
        } else {
            grant
        };
        sqlx::query(&format!(
            "UPDATE agent.dialogue_tool_publication_groups SET {column}=$1 WHERE invocation_id=$2"
        ))
        .bind(Sha256::digest(b"wrong custody").into_bytes().to_vec())
        .bind(unknown)
        .execute(&mut *tx)
        .await
        .unwrap();
        assert!(
            verify_device_publication_cut(
                &mut tx,
                actor,
                run,
                snapshot,
                grant,
                WorkItemDeviceRunOutcome::Failed,
                &observed
            )
            .await
            .is_err()
        );
        sqlx::query(&format!(
            "UPDATE agent.dialogue_tool_publication_groups SET {column}=$1 WHERE invocation_id=$2"
        ))
        .bind(original.into_bytes().to_vec())
        .bind(unknown)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE agent.dialogue_tool_publication_groups SET tenant_id=$1 WHERE invocation_id=$2",
    )
    .bind(TenantId::generate().into_uuid())
    .bind(unknown)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(
        verify_device_publication_cut(
            &mut tx,
            actor,
            run,
            snapshot,
            grant,
            WorkItemDeviceRunOutcome::Cancelled,
            &observed
        )
        .await
        .is_err()
    );
    sqlx::query(
        "UPDATE agent.dialogue_tool_publication_groups SET tenant_id=$1 WHERE invocation_id=$2",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(unknown)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE agent.dialogue_publications SET owner_id=$1 WHERE source_tool_invocation_id=$2",
    )
    .bind(PrincipalId::generate().into_uuid())
    .bind(unknown)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(
        verify_device_publication_cut(
            &mut tx,
            actor,
            run,
            snapshot,
            grant,
            WorkItemDeviceRunOutcome::Failed,
            &observed
        )
        .await
        .is_err()
    );
    sqlx::query(
        "UPDATE agent.dialogue_publications SET owner_id=$1 WHERE source_tool_invocation_id=$2",
    )
    .bind(actor.principal_id.into_uuid())
    .bind(unknown)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE agent.dialogue_tool_publication_groups SET entry_count=2 WHERE invocation_id=$1",
    )
    .bind(unknown)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(
        verify_device_publication_cut(
            &mut tx,
            actor,
            run,
            snapshot,
            grant,
            WorkItemDeviceRunOutcome::Cancelled,
            &observed
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    pool.close().await;
}
