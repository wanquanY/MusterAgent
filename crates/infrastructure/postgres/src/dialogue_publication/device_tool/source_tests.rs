use super::*;
use musteragent_content_ref::Sha256;
use musteragent_testkit::IsolatedPostgres;

/// Exercise the actual custody row lock and new-effect fence. Retained receipt
/// lookup precedes this fence in each publication writer.
#[tokio::test]
async fn publication_waiting_on_terminal_commit_cannot_grow_the_frozen_cut() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    sqlx::raw_sql("CREATE SCHEMA agent; CREATE SCHEMA collaboration;
      CREATE TABLE agent.device_admissions(intent_id uuid,run_id uuid PRIMARY KEY);
      CREATE TABLE collaboration.work_item_device_custody(run_id uuid PRIMARY KEY,work_item_id uuid,goal_revision bigint,control_revision bigint,assignment_epoch bigint);
      CREATE TABLE agent.native_dialogue_terminals(run_id uuid PRIMARY KEY);
      CREATE TABLE collaboration.work_item_device_terminals(run_id uuid PRIMARY KEY);
      CREATE TABLE collaboration.work_item_execution_reservations(run_id uuid PRIMARY KEY,result_protocol text NOT NULL);")
        .execute(&pool).await.unwrap();
    for work_item in [
        None,
        Some(WorkItemDevicePublicationSubject {
            work_item_id: WorkItemId::generate(),
            goal_revision: 1,
            control_revision: 1,
            assignment_epoch: 1,
        }),
    ] {
        let binding = DeviceDeliveryBinding {
            intent_id: UserIntentId::generate(),
            work_item,
            run_id: RunId::generate(),
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            source_digest: Sha256::digest(b"source"),
            snapshot_hash: Sha256::digest(b"snapshot"),
            grant_proof_hash: Sha256::digest(b"grant"),
        };
        if let Some(task) = work_item {
            sqlx::query("INSERT INTO collaboration.work_item_device_custody VALUES($1,$2,1,1,1)")
                .bind(binding.run_id.into_uuid())
                .bind(task.work_item_id.into_uuid())
                .execute(&pool)
                .await
                .unwrap();
        } else {
            sqlx::query("INSERT INTO agent.device_admissions VALUES($1,$2)")
                .bind(binding.intent_id.into_uuid())
                .bind(binding.run_id.into_uuid())
                .execute(&pool)
                .await
                .unwrap();
        }
        let mut terminal = pool.begin().await.unwrap();
        lock(&mut terminal, &binding).await.unwrap();
        require_open_for_publication(&mut terminal, &binding)
            .await
            .unwrap();
        let other_pool = pool.clone();
        let waiting_binding = binding.clone();
        let mut waiter = tokio::spawn(async move {
            let mut publication = other_pool.begin().await.unwrap();
            lock(&mut publication, &waiting_binding).await.unwrap();
            let result = require_open_for_publication(&mut publication, &waiting_binding).await;
            publication.rollback().await.unwrap();
            result
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter)
                .await
                .is_err()
        );
        let table = if work_item.is_some() {
            "collaboration.work_item_device_terminals"
        } else {
            "agent.native_dialogue_terminals"
        };
        sqlx::query(&format!("INSERT INTO {table} VALUES($1)"))
            .bind(binding.run_id.into_uuid())
            .execute(&mut *terminal)
            .await
            .unwrap();
        terminal.commit().await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
                .await
                .unwrap()
                .unwrap(),
            Err(Failure::Forbidden)
        );
    }

    // Since 556eb6ebd7 a verified-result task seals its outputs through the
    // verified result, so its device publication path is closed from the start.
    let task = WorkItemDevicePublicationSubject {
        work_item_id: WorkItemId::generate(),
        goal_revision: 1,
        control_revision: 1,
        assignment_epoch: 1,
    };
    let verified = DeviceDeliveryBinding {
        intent_id: UserIntentId::generate(),
        work_item: Some(task),
        run_id: RunId::generate(),
        device_id: DeviceId::generate(),
        installation_id: DeviceInstallationId::generate(),
        source_digest: Sha256::digest(b"source"),
        snapshot_hash: Sha256::digest(b"snapshot"),
        grant_proof_hash: Sha256::digest(b"grant"),
    };
    sqlx::query("INSERT INTO collaboration.work_item_device_custody VALUES($1,$2,1,1,1)")
        .bind(verified.run_id.into_uuid())
        .bind(task.work_item_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO collaboration.work_item_execution_reservations VALUES($1,'verified_result_v1')")
        .bind(verified.run_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let mut publication = pool.begin().await.unwrap();
    lock(&mut publication, &verified).await.unwrap();
    assert_eq!(
        require_open_for_publication(&mut publication, &verified).await,
        Err(Failure::Forbidden)
    );
    publication.rollback().await.unwrap();
    pool.close().await;
}
