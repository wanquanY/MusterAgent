//! Account Goal effects and message cards from real native custody and receipts.
//! Native signing is a fixture; installed-device acceptance is separate.
use super::*;
fn args() -> Value {
    json!({"action_key":"watch-my-meetings","title":"My meeting actions","instructions":"Keep track of my explicit action items and report meaningful changes only.","constraints":["Do not send external messages"],"success_criteria":["Relevant action items are retained"],"evidence_requirements":["Original source"],"failure_conditions":[],"high_risk":false})
}
fn output(receipt: &DeviceResourceReceipt) -> Value {
    serde_json::from_slice(
        &receipt
            .success()
            .expect("successful Goal operation")
            .result_bytes,
    )
    .unwrap()
}
#[tokio::test]
async fn native_goal_creation_receipt_card_and_lost_response_share_one_original_transaction() {
    let f = Canonical::new().await;
    let create = f.request("goal.create@3", args()).await;
    sqlx::query("CREATE FUNCTION public.reject_native_goal_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tool_key='goal.create@3' AND NEW.outcome='succeeded' THEN RAISE EXCEPTION 'injected receipt failure'; END IF; RETURN NEW; END $$").execute(&f.f.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_native_goal_receipt BEFORE UPDATE ON resource.device_commands FOR EACH ROW EXECUTE FUNCTION public.reject_native_goal_receipt()").execute(&f.f.pool).await.unwrap();
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &create)
            .await
            .is_err()
    );
    let counts: (i64,i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM runtime.standing_goals),(SELECT count(*) FROM agent.agent_work_creation_publications),(SELECT count(*) FROM resource.device_commands),(SELECT count(*) FROM agent.owner_goal_setup_deliveries)").fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(
        counts,
        (0, 0, 0, 0),
        "failed original receipt leaves no task, card or acknowledged command"
    );
    sqlx::query("DROP TRIGGER reject_native_goal_receipt ON resource.device_commands")
        .execute(&f.f.pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION public.reject_native_goal_receipt()")
        .execute(&f.f.pool)
        .await
        .unwrap();
    let receipt = f.execute(&create).await;
    assert_eq!(f.execute(&create).await, receipt);
    let value = output(&receipt);
    let repeated = f.request("goal.create@3", args()).await;
    assert_eq!(output(&f.execute(&repeated).await), value);
    let pending: (i64, i64) =
        sqlx::query_as("SELECT count(*),count(ready_at_ms) FROM agent.owner_goal_setup_deliveries")
            .fetch_one(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(
        pending,
        (1, 0),
        "an unconfigured draft is one pending delivery, never setup success"
    );
    let work = musteragent_runtime_session::AgentWorkQuery::read(
        &musteragent_postgres::PostgresAgentWorkQuery::new(f.f.pool.clone()),
        musteragent_runtime_session::ReadAgentWork {
            actor: f.f.actor,
            agent_id: f.f.custody.source.scope.agent_id,
            kind: Some(AgentWorkKind::Goal),
            category: None,
            include_finished: true,
            limit: 10,
            cursor: None,
            reference: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        work.total_count, 0,
        "pending internal setup must not appear as a delivered user task"
    );
    assert!(work.records.is_empty());
    let delivery = f.f.delivery();
    let published =
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &delivery)
            .await
            .unwrap();
    assert_eq!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &delivery)
            .await
            .unwrap(),
        published
    );
    let card: (uuid::Uuid,String,Option<uuid::Uuid>,uuid::Uuid,uuid::Uuid) = sqlx::query_as("SELECT work_id,execution_home,source_invocation_id,device_source_invocation_id,entry_id FROM agent.agent_work_creation_publications").fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(card.0.to_string(), value["goal_id"].as_str().unwrap());
    assert_eq!(card.1, "device");
    assert_eq!(card.2, None);
    assert_eq!(
        card.3,
        create.command().invocation().invocation_id.into_uuid()
    );
    assert_eq!(card.4, published.entry_ids[0].into_uuid());
    let anchored_pending: (uuid::Uuid,Option<i64>) = sqlx::query_as("SELECT c.entry_id,d.ready_at_ms FROM agent.agent_work_creation_publications c JOIN agent.owner_goal_setup_deliveries d USING(creation_id)")
        .fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(
        anchored_pending,
        (published.entry_ids[0].into_uuid(), None),
        "publication anchors the original message without promoting the pending draft"
    );
    let cloud: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM runtime.runs),(SELECT count(*) FROM tool.tool_invocations),(SELECT count(*) FROM runtime.goal_pursuits)").fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(
        cloud,
        (0, 0, 0),
        "native creation must neither manufacture Cloud execution nor start the unconfigured Goal"
    );
}

#[tokio::test]
async fn native_goal_management_keeps_revision_fences_and_revokes_stale_read_grants() {
    let f = Canonical::new().await;
    let create = f.request("goal.create@3", args()).await;
    let value = output(&f.execute(&create).await);
    let id = value["goal_id"].as_str().unwrap();
    let list = f
        .request(
            "goal.list@2",
            json!({"limit":10,"include_finished":false,"cursor":null}),
        )
        .await;
    assert_eq!(output(&f.execute(&list).await)["goals"][0]["goal_id"], id);
    let read = f.request("goal.get@3", json!({"goal_id":id})).await;
    let before = f.execute(&read).await;
    assert_eq!(output(&before)["goal"]["revision"], 1);
    assert_eq!(before.success().unwrap().authorized_content.len(), 1);
    assert_eq!(f.execute(&read).await, before);
    let update = f.request("goal.update@3",json!({"goal_id":id,"expected_revision":1,"title":"Reviewed meeting actions","instructions":"Only retain my explicit action items; notify about actual changes."})).await;
    let updated = f.execute(&update).await;
    assert_eq!(output(&updated)["goal"]["revision"], 2);
    assert_eq!(f.execute(&update).await, updated);
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &read)
            .await
            .is_err(),
        "replaying the old read cannot restore superseded objective access"
    );
    let stale = f.request("goal.change_status@2",json!({"goal_id":id,"expected_revision":1,"expected_pursuit_revision":null,"action":"stop"})).await;
    assert!(matches!(
        f.execute(&stale).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let stop = f.request("goal.change_status@2",json!({"goal_id":id,"expected_revision":2,"expected_pursuit_revision":null,"action":"stop"})).await;
    assert_eq!(
        output(&f.execute(&stop).await)["goal"]["status"],
        "cancelled"
    );
    let rows: (String, String) =
        sqlx::query_as("SELECT title,revision::text FROM runtime.standing_goals")
            .fetch_one(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(rows, ("Reviewed meeting actions".into(), "3".into()));
}

async fn draft_directory(f: &Canonical) -> musteragent_runtime_session::AgentWorkPage {
    musteragent_runtime_session::AgentWorkQuery::read(
        &musteragent_postgres::PostgresAgentWorkQuery::new(f.f.pool.clone()),
        musteragent_runtime_session::ReadAgentWork {
            actor: f.f.actor,
            agent_id: f.f.custody.source.scope.agent_id,
            kind: Some(AgentWorkKind::Goal),
            category: None,
            include_finished: true,
            limit: 10,
            cursor: None,
            reference: None,
        },
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn native_explicit_draft_save_recovers_same_named_originals_without_activating_them() {
    let f = Canonical::new().await;
    let first = f.request("goal.create@3", args()).await;
    let one = output(&f.execute(&first).await);
    let mut second_args = args();
    second_args["action_key"] = json!("separate-deliberate-task");
    let second = f.request("goal.create@3", second_args).await;
    let two = output(&f.execute(&second).await);
    assert_ne!(one["goal_id"], two["goal_id"]);
    assert_eq!(one["title"], two["title"]);
    assert_eq!(draft_directory(&f).await.total_count, 0);
    let published =
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &f.f.delivery())
            .await
            .unwrap();
    let entry = published.entry_ids[0].into_uuid();
    let before: i64 = sqlx::query_scalar(
        "SELECT visible_revision FROM agent.agent_dialogue_entries WHERE entry_id=$1",
    )
    .bind(entry)
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    let save = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":one["goal_id"],"expected_revision":1}),
        )
        .await;
    sqlx::query("CREATE FUNCTION public.reject_native_draft_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tool_key='goal.draft.save@1' AND NEW.outcome='succeeded' THEN RAISE EXCEPTION 'injected receipt failure'; END IF; RETURN NEW; END $$").execute(&f.f.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_native_draft_receipt BEFORE UPDATE ON resource.device_commands FOR EACH ROW EXECUTE FUNCTION public.reject_native_draft_receipt()").execute(&f.f.pool).await.unwrap();
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &save)
            .await
            .is_err()
    );
    assert_eq!(draft_directory(&f).await.total_count, 0);
    let registered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM agent.owner_goal_draft_registrations")
            .fetch_one(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(registered, 0);
    let unchanged: i64 = sqlx::query_scalar(
        "SELECT visible_revision FROM agent.agent_dialogue_entries WHERE entry_id=$1",
    )
    .bind(entry)
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(unchanged, before);
    sqlx::query("DROP TRIGGER reject_native_draft_receipt ON resource.device_commands")
        .execute(&f.f.pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION public.reject_native_draft_receipt()")
        .execute(&f.f.pool)
        .await
        .unwrap();
    let receipt = f.execute(&save).await;
    assert_eq!(f.execute(&save).await, receipt);
    let page = draft_directory(&f).await;
    assert_eq!(page.total_count, 1);
    assert_eq!(page.records.len(), 1);
    let retry = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":one["goal_id"],"expected_revision":1}),
        )
        .await;
    assert_eq!(output(&f.execute(&retry).await), output(&receipt));
    let once: i64 = sqlx::query_scalar(
        "SELECT visible_revision FROM agent.agent_dialogue_entries WHERE entry_id=$1",
    )
    .bind(entry)
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(once, before + 1);
    let save2 = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":two["goal_id"],"expected_revision":1}),
        )
        .await;
    let racing = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":two["goal_id"],"expected_revision":1}),
        )
        .await;
    let (a, b) = tokio::join!(f.execute(&save2), f.execute(&racing));
    assert_eq!(output(&a), output(&b));
    assert_eq!(draft_directory(&f).await.total_count, 2);
    let twice: i64 = sqlx::query_scalar(
        "SELECT visible_revision FROM agent.agent_dialogue_entries WHERE entry_id=$1",
    )
    .bind(entry)
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(twice, before + 2);
    let counts:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runtime.standing_goals),(SELECT count(*) FROM agent.owner_goal_draft_registrations),(SELECT count(ready_at_ms) FROM agent.owner_goal_setup_deliveries),(SELECT count(*) FROM runtime.goal_pursuits),(SELECT count(*) FROM tool.tool_invocations)").fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(counts, (2, 2, 0, 0, 0));
    assert!(
        sqlx::query(
            "UPDATE agent.owner_goal_draft_registrations SET goal_revision=goal_revision+1"
        )
        .execute(&f.f.pool)
        .await
        .is_err(),
        "registration evidence is immutable"
    );
    let update=f.request("goal.update@3",json!({"goal_id":one["goal_id"],"expected_revision":1,"title":"Edited saved draft","instructions":"Keep the saved identity."})).await;
    assert_eq!(output(&f.execute(&update).await)["goal"]["revision"], 2);
    assert_eq!(
        f.execute(&save).await,
        receipt,
        "lost response replay returns the original receipt after later edits"
    );
    let stale = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":one["goal_id"],"expected_revision":1}),
        )
        .await;
    assert!(matches!(
        f.execute(&stale).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let fresh = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":one["goal_id"],"expected_revision":2}),
        )
        .await;
    let existing_registration = output(&f.execute(&fresh).await);
    assert_eq!(
        existing_registration,
        output(&receipt),
        "new command checks revision 2 but returns the original revision 1 registration"
    );
    let registered_revision:i64=sqlx::query_scalar("SELECT r.goal_revision FROM agent.owner_goal_draft_registrations r JOIN agent.agent_work_creation_publications c USING(creation_id) WHERE c.work_id=$1")
        .bind(uuid::Uuid::parse_str(one["goal_id"].as_str().unwrap()).unwrap()).fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(registered_revision, 1);
    let current = f
        .request("goal.get@3", json!({"goal_id":one["goal_id"]}))
        .await;
    assert_eq!(
        output(&f.execute(&current).await)["goal"]["revision"],
        2,
        "current Goal evidence remains independent of historical registration"
    );
    let after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM agent.owner_goal_draft_registrations")
            .fetch_one(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(after, 2);
}

#[tokio::test]
async fn draft_registration_rejects_a_creation_receipt_and_unauthorized_goal_identity() {
    let f = Canonical::new().await;
    let create = f.request("goal.create@3", args()).await;
    let created = output(&f.execute(&create).await);
    let mut tx = f.f.pool.begin().await.unwrap();
    assert!(sqlx::query("INSERT INTO agent.owner_goal_draft_registrations(creation_id,goal_revision,registered_at_ms,source_run_id,device_source_invocation_id,execution_home) SELECT creation_id,1,created_at_ms,source_run_id,device_source_invocation_id,'device' FROM agent.agent_work_creation_publications").execute(&mut *tx).await.is_err(),"a create receipt cannot authorize deliberate draft visibility");
    tx.rollback().await.unwrap();
    let unavailable = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":uuid::Uuid::now_v7(),"expected_revision":1}),
        )
        .await;
    assert!(matches!(
        f.execute(&unavailable).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    assert_eq!(draft_directory(&f).await.total_count, 0);
    let malformed = f
        .request(
            "goal.draft.save@1",
            json!({"goal_id":created["goal_id"],"expected_revision":1,"activate":true}),
        )
        .await;
    assert!(matches!(
        f.execute(&malformed).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
}
