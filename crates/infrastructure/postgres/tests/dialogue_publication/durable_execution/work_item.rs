use super::*;
use musteragent_runtime_capability::CapabilityOutcome;
use musteragent_tool_executor::{DefaultWorkItemTools, WorkItemCapabilityHandler};
fn tools(f: &Fixture, at: UtcTimestamp) -> Arc<DefaultWorkItemTools> {
    Arc::new(
        DefaultWorkItemTools::new(
            f.owner,
            f.content.clone().expect("valid durable execution fixture"),
            Arc::new(
                musteragent_postgres::PostgresWorkItemStartStore::new(
                    f.pool.clone(),
                    Arc::new(TestLedgerPayloadEncoder),
                    keys(),
                )
                .with_query_content(f.content.clone().expect("valid durable execution fixture")),
            ),
            Arc::new(Clock(at)),
        )
        .expect("valid durable execution fixture"),
    )
}
async fn invoke(f: &Fixture, c: &Call) -> DispatchedCapabilityOutcome {
    use musteragent_tool_executor::{CapabilityDispatcher, CompositeCapabilityDispatcher};
    let mut dispatcher = CompositeCapabilityDispatcher::new(Arc::new(
        musteragent_runtime_capability::CapabilityRegistry::new(),
    ));
    dispatcher
        .register_atomic(Arc::new(WorkItemCapabilityHandler::new(
            c.manifest.clone(),
            f.content.clone().expect("valid durable execution fixture"),
            tools(f, now(f).await),
            Arc::new(Clock(now(f).await)),
        )))
        .expect("valid durable execution fixture");
    dispatcher
        .invoke(&c.attempt, &c.material, &c.durable, c.invocation.clone())
        .await
        .expect("valid durable execution fixture")
}
fn args() -> Value {
    json!({"objective":"Review a result","success_criteria":["Verified evidence"],"evidence_boundary":"Supplied inputs"})
}
#[tokio::test]
async fn work_item_start_handoff_and_fence() {
    let f = setup().await;
    let c = call(&f, "tasks.start@3", args()).await;
    snapshot::install(&f, c.material.run_context).await;
    batch::install(&f, &c).await;
    let data = result(&f, invoke(&f, &c).await).await;
    assert_eq!(data["status"], "accepted");
    assert_eq!(result(&f, invoke(&f, &c).await).await, data);
    let handoff=call(&f,"tasks.handoff@1",json!({"tasks":[{"task_id":data["task_id"],"expected_goal_revision":1,"expected_control_revision":1,"expected_assignment_epoch":1}]})).await;
    batch::install(&f, &handoff).await;
    let handed = result(&f, invoke(&f, &handoff).await).await;
    assert_eq!(handed["status"], "handed_off");
    assert_eq!(result(&f, invoke(&f, &handoff).await).await, handed);
    let stale = call(&f, "tasks.start@3", args()).await;
    advance_fence(&f, &stale.attempt).await;
    assert!(
        matches!(invoke(&f,&stale).await,DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {error,..}) if error.code==musteragent_runtime_contract::ErrorCode::ResourceRevisionConflict)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM collaboration.work_items")
        .fetch_one(&f.pool)
        .await
        .expect("valid durable execution fixture");
    assert_eq!(count, 1);
}

async fn start(f: &Fixture) -> uuid::Uuid {
    let c = call(f, "tasks.start@3", args()).await;
    snapshot::install(f, c.material.run_context).await;
    batch::install(f, &c).await;
    let data = result(f, invoke(f, &c).await).await;
    data["task_id"]
        .as_str()
        .expect("valid durable execution fixture")
        .parse()
        .expect("valid durable execution fixture")
}
async fn submit(f: &Fixture, task: uuid::Uuid, epoch: i64) -> Value {
    let c=call(f,"tasks.submit_result@1",json!({"task_id":task,"goal_revision":1,"control_revision":1,"assignment_epoch":epoch,"delivery":{"kind":"message","text":"Verified evidence"}})).await;
    batch::install(f, &c).await;
    let first = result(f, invoke(f, &c).await).await;
    assert_eq!(result(f, invoke(f, &c).await).await, first);
    first
}
#[tokio::test]
async fn work_item_result_feedback_verification_and_followup() {
    let mut f = setup().await;
    let task = start(&f).await;
    let producer = session::execution(&mut f, task).await;
    submit(&f, task, 1).await;
    let public: i64 =
        sqlx::query_scalar("SELECT count(*) FROM collaboration.work_item_result_deliveries")
            .fetch_one(&f.pool)
            .await
            .expect("valid durable execution fixture");
    assert_eq!(
        public, 0,
        "submission is private until the requester verifies it"
    );
    session::report(&mut f, producer).await;
    let feedback = call(
        &f,
        "tasks.feedback@1",
        json!({"feedback":"Please include the verified evidence"}),
    )
    .await;
    batch::install(&f, &feedback).await;
    assert_eq!(
        result(&f, invoke(&f, &feedback).await).await["status"],
        "feedback_sent"
    );
    assert_eq!(
        result(&f, invoke(&f, &feedback).await).await["status"],
        "feedback_sent"
    );
    let producer = session::execution(&mut f, task).await;
    let receipt = submit(&f, task, 2).await;
    let report = session::report(&mut f, producer).await;
    let report_id:uuid::Uuid=sqlx::query_scalar("SELECT report_content_id FROM collaboration.work_item_execution_reports WHERE producing_run_id=$1").bind(producer.into_uuid()).fetch_one(&f.pool).await.expect("valid durable execution fixture");
    let verify=call(&f,"tasks.verify@3",json!({"task_id":task,"goal_revision":1,"control_revision":1,"assignment_epoch":2,"producing_run_id":producer.to_string(),"report_content_id":report_id,"result_id":receipt["result_id"],"manifest_sha256":receipt["manifest_sha256"],"decision":"accepted","criteria":[{"index":0,"decision":"accepted","evidence":[{"result_id":receipt["result_id"],"item_id":report["result"]["items"][0]["item_id"]}]}]})).await;
    batch::install(&f, &verify).await;
    let verified = result(&f, invoke(&f, &verify).await).await;
    assert_eq!(verified["status"], "completed");
    assert_eq!(result(&f, invoke(&f, &verify).await).await, verified);
    let public: i64 =
        sqlx::query_scalar("SELECT count(*) FROM collaboration.work_item_result_deliveries")
            .fetch_one(&f.pool)
            .await
            .expect("valid durable execution fixture");
    assert_eq!(public, 1);
    let store = PostgresAgentDialogueStore::new(f.pool.clone());
    let history = store
        .read_history(
            f.owner,
            f.intent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 100,
            },
        )
        .await
        .expect("valid durable execution fixture");
    let published:uuid::Uuid=sqlx::query_scalar("SELECT entry_id FROM agent.dialogue_publications WHERE source_run_id=$1 AND purpose='result'").bind(f.command.fence.run_id.into_uuid()).fetch_one(&f.pool).await.expect("valid durable execution fixture");
    let entry = history
        .entries
        .iter()
        .find(|e| e.entry.entry_id.into_uuid() == published)
        .expect("verified result is in history");
    let mut next = input(f.owner, f.intent.dialogue_id);
    next.observed_cursor = entry.entry.cursor;
    next.reply_target = entry.reply_target;
    next.accepted_at = now(&f).await;
    register_content(&f.pool, f.owner, &next.content_ref).await;
    f.intent = store
        .accept(next)
        .await
        .expect("valid durable execution fixture")
        .intent;
    assert!(
        f.intent
            .reply_binding
            .as_ref()
            .expect("valid durable execution fixture")
            .task_action()
            .is_some(),
        "followup binds exact task output: {:?}",
        f.intent.reply_binding
    );
    session::session(&mut f, ConversationId::generate(), RunId::generate(), None).await;
    let followup = call(&f, "tasks.followup@1", args()).await;
    batch::install(&f, &followup).await;
    let followed = result(&f, invoke(&f, &followup).await).await;
    assert_ne!(followed["task_id"], json!(task));
    assert_eq!(result(&f, invoke(&f, &followup).await).await, followed);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM collaboration.work_item_followups WHERE parent_work_item_id=$1",
    )
    .bind(task)
    .fetch_one(&f.pool)
    .await
    .expect("valid durable execution fixture");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn work_item_revision_dependencies_are_atomic_and_replayable() {
    let f = setup().await;
    let parent = start(&f).await;
    let mut dependent_args = args();
    dependent_args["dependencies"] = json!([parent]);
    let dependent = call(&f, "tasks.start@3", dependent_args).await;
    let created = result(&f, invoke(&f, &dependent).await).await;
    let task: uuid::Uuid = created["task_id"]
        .as_str()
        .expect("valid durable execution fixture")
        .parse()
        .expect("valid durable execution fixture");
    let mutation = |task: uuid::Uuid, goal: u64, dependencies: Value| json!({"task_id":task,"expected_goal_revision":goal,"expected_control_revision":1,"objective":"Revised objective","success_criteria":["Verified evidence"],"evidence_boundary":"Supplied inputs","dependencies":dependencies});
    let cycle = call(&f, "tasks.amend@2", mutation(parent, 1, json!([task]))).await;
    assert!(
        matches!(
            invoke(&f, &cycle).await,
            DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed { .. })
        ),
        "a cycle must not commit"
    );
    let amended = call(&f, "tasks.amend@2", mutation(task, 1, json!([]))).await;
    let receipt = result(&f, invoke(&f, &amended).await).await;
    assert_eq!(receipt["goal_revision"], 2);
    assert_eq!(result(&f, invoke(&f, &amended).await).await, receipt);
    let row=sqlx::query("SELECT r.goal_revision,r.dependencies FROM collaboration.work_items t JOIN collaboration.work_item_revisions r USING(work_item_id,state_revision) WHERE t.work_item_id=$1").bind(task).fetch_one(&f.pool).await.expect("valid durable execution fixture");
    assert_eq!(row.get::<i64, _>("goal_revision"), 2);
    assert!(row.get::<Vec<uuid::Uuid>, _>("dependencies").is_empty());
    let retained:i64=sqlx::query_scalar("SELECT count(*) FROM collaboration.work_item_revisions WHERE work_item_id=$1 AND goal_revision=1 AND dependencies=ARRAY[$2::uuid]").bind(task).bind(parent).fetch_one(&f.pool).await.expect("valid durable execution fixture");
    assert!(retained > 0, "the original dependency facts are retained");
    let stale = call(&f, "tasks.amend@2", mutation(task, 1, json!([]))).await;
    assert!(
        matches!(invoke(&f,&stale).await,DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed{error,..}) if error.code==musteragent_runtime_contract::ErrorCode::ResourceRevisionConflict)
    );
}
