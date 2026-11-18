use super::*;

#[tokio::test]
async fn historical_workers_cannot_claim_a_run_with_a_new_frozen_policy() {
    let (_guard, _database, pool) = runtime_pool().await;
    for (failover, auxiliary_backup) in [(false, false), (true, false), (false, true)] {
        let fixture = seed_fixture(&pool, None).await;
        let mut batch = admission_batch(&pool, &fixture, "mixed-readers").await;
        let codec = ProtobufSnapshotCodec;
        let mut snapshot = codec
            .decode_content(&batch.runtime_snapshot.encoded_snapshot)
            .unwrap();
        snapshot.execution_policy.provider_failover_v1 = failover;
        if auxiliary_backup {
            let primary = &mut batch
                .runtime_snapshot
                .execution_grant
                .auxiliary_model_routes[0];
            primary
                .route
                .capabilities
                .insert(DottedKey::new("streaming").unwrap());
            primary.route.route_ref = VersionedKey::new(format!(
                "registry.aux.compression.id-{}@1",
                primary.route.model_entry_id
            ))
            .unwrap();
            snapshot.auxiliary_models[0].model.route = primary.route.clone();
            let mut alternative = primary.clone();
            alternative.route.model_entry_id =
                musteragent_model::ModelEntryId::from_uuid(uuid::Uuid::now_v7()).unwrap();
            alternative.route.route_ref = VersionedKey::new(format!(
                "registry.aux.compression.id-{}@1",
                alternative.route.model_entry_id
            ))
            .unwrap();
            alternative.credential_grant_ref =
                CredentialGrantRef::new(format!("grant_aux_{}", alternative.route.model_entry_id))
                    .unwrap();
            snapshot.auxiliary_models.push(
                musteragent_runtime_snapshot::PinnedAuxiliaryModelRoute {
                    role: alternative.role,
                    model: musteragent_runtime_snapshot::PinnedModelRoute {
                        route: alternative.route.clone(),
                        credential_grant_ref: alternative.credential_grant_ref.clone(),
                    },
                },
            );
            batch
                .runtime_snapshot
                .execution_grant
                .auxiliary_model_routes
                .push(alternative);
        }
        let bytes = codec.encode_content(&snapshot).unwrap();
        batch.runtime_snapshot.content_hash = Sha256::digest(&bytes);
        batch.runtime_snapshot.encoded_snapshot = bytes;
        let run = batch.run.context.run_id.into_uuid();
        seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
        admission_store(&pool).admit(batch).await.unwrap();
        for modern in [false, true] {
            let mut tx = pool.begin().await.unwrap();
            // Historical SQL omits the reader capability column completely.
            let (extra_column, extra_value) = if modern {
                (",supports_model_routing_v1", ",TRUE")
            } else {
                ("", "")
            };
            let insert = format!(
                "INSERT INTO runtime.attempts (attempt_id, tenant_id, run_id, attempt_number,
                 trigger, worker_id, lease_id, fencing_token, lease_expires_at_ms, status,
                 claimed_at_ms{extra_column}) VALUES (gen_random_uuid(),$1,$2,1,1,'mixed-reader-test',
                 gen_random_uuid(),1,60000,1,1{extra_value})"
            );
            let result = sqlx::query(&insert)
                .bind(fixture.tenant_id.into_uuid())
                .bind(run)
                .execute(&mut *tx)
                .await;
            if (failover || auxiliary_backup) && !modern {
                assert_eq!(
                    result
                        .unwrap_err()
                        .as_database_error()
                        .unwrap()
                        .code()
                        .as_deref(),
                    Some("55000")
                );
            } else {
                result.expect("compatible worker can claim");
            }
            tx.rollback().await.unwrap();
        }
        let attempts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime.attempts WHERE run_id=$1")
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(attempts, 0);
    }
}
