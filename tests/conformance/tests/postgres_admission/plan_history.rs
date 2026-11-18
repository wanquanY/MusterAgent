use super::*;
use musteragent_runtime_execution_plan::ExecutionPlanStore;

#[tokio::test]
async fn predecessor_plan_requires_current_membership_and_root_scope() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let mut batch = admission_batch(&pool, &fixture, "plan-history:scope").await;
    let bytes = musteragent_wire::ProtobufLedgerPayloadCodec.encode(
        musteragent_ledger::LedgerPayload::DomainEvent(
            musteragent_ledger::DomainEvent::Conversation(
                musteragent_conversation::ConversationEvent::MessageAccepted {
                    project_task_target: None,
                    message_id: batch.message.message_id,
                    role: batch.message.role,
                    content_ref: batch.message.content.clone(),
                    attachment_refs: vec![],
                    requested_skill_refs: vec![],
                },
            ),
        ),
    );
    batch.append.records[0].payload_hash = Sha256::digest(&bytes);
    batch.append.records[0].payload = bytes;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    admission_store(&pool)
        .admit(batch.clone())
        .await
        .expect("admit root");
    let history = musteragent_postgres::PostgresDelegationHistorySource::new(
        pool.clone(),
        Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec),
    );
    use musteragent_runtime_data_access::delegation_history::*;
    let access = HistoryAccess {
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        current_run_id: batch.run.context.run_id,
        tool_fence: None,
    };
    let input: HistoryInput = serde_json::from_str("{}").expect("default history input");
    assert!(
        history
            .read(&access, &input)
            .await
            .expect("authorized empty history")
            .entries
            .is_empty()
    );
    let detail: HistoryInput = serde_json::from_value(
        serde_json::json!({"view":"detail", "record_id":batch.message_record_id.to_string()}),
    )
    .unwrap();
    let source_page = history
        .read(&access, &detail)
        .await
        .expect("root user input read");
    assert_eq!(source_page.entries.len(), 1);
    assert_eq!(
        source_page.entries[0].content,
        Some(batch.message.content.clone())
    );
    let forbidden: HistoryInput = serde_json::from_value(
        serde_json::json!({"view":"detail", "record_id":batch.run_record_id.to_string()}),
    )
    .unwrap();
    assert!(matches!(
        history.read(&access, &forbidden).await,
        Err(HistoryError::Denied | HistoryError::Corrupt)
    ));
    let plans = musteragent_postgres::PostgresExecutionPlanStore::new(
        pool.clone(),
        Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec),
        reference_keys(),
    );
    let context = batch.run.context;
    assert_eq!(
        plans
            .load_predecessor(context, None)
            .await
            .expect("no predecessor"),
        None
    );
    let mut wrong = context;
    wrong.tenant_id = TenantId::generate();
    assert!(plans.load_predecessor(wrong, None).await.is_err());
    let mut child = context;
    child.activity_kind = ActivityKind::Delegation;
    child.delegation_id = Some(musteragent_identity::DelegationId::generate());
    assert_eq!(
        plans
            .load_predecessor(child, None)
            .await
            .expect("child has no root candidate"),
        None
    );
    sqlx::query("UPDATE conversation.participants SET left_at_ms=100 WHERE tenant_id=$1 AND participant_id=$2")
        .bind(fixture.tenant_id.into_uuid()).bind(fixture.user_participant_id.into_uuid())
        .execute(&pool).await.expect("revoke member");
    assert!(plans.load_predecessor(context, None).await.is_err());
}
