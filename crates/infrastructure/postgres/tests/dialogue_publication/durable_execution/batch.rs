//! Seed an observed single-call model batch; generation itself is outside P7.
use super::*;
pub(super) async fn install(f: &Fixture, c: &Call) {
    let model = uuid::Uuid::now_v7();
    let i = &c.invocation;
    let r = &i.request_ref;
    let iteration: i32 =
        sqlx::query_scalar("SELECT count(*)::int+1 FROM runtime.context_plans WHERE run_id=$1")
            .bind(i.run_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .expect("valid durable execution fixture");
    sqlx::query("INSERT INTO runtime.model_invocations(invocation_id,tenant_id,run_id,attempt_id,purpose,status,model_route_ref,idempotency_key,request_content_id,request_hash,started_at_ms,finished_at_ms,conversation_id,credential_grant_ref,route_role) VALUES($1,$2,$3,$4,1,2,'test.route@1',$5,$6,$7,1,2,$8,'test-grant',0)")
        .bind(model).bind(i.tenant_id.into_uuid()).bind(i.run_id.into_uuid()).bind(c.attempt.attempt_id.into_uuid()).bind(model.to_string()).bind(r.content_id.into_uuid()).bind(r.sha256.into_bytes().to_vec()).bind(i.conversation_id.into_uuid()).execute(&f.pool).await.expect("valid durable execution fixture");
    let ledger:uuid::Uuid=sqlx::query_scalar("SELECT r.record_id FROM event_store.ledger_records r WHERE r.run_id=$1 AND NOT EXISTS(SELECT 1 FROM runtime.context_plans p WHERE p.ledger_record_id=r.record_id) ORDER BY r.conversation_seq DESC LIMIT 1").bind(i.run_id.into_uuid()).fetch_one(&f.pool).await.expect("valid durable execution fixture");
    sqlx::query("INSERT INTO runtime.context_plans(snapshot_id,tenant_id,conversation_id,run_id,attempt_id,iteration,source_sequence_from,source_sequence_to,retained_record_ids,summary_content_id,summary_content_hash,summary_content_bytes,request_content_id,request_content_hash,request_content_bytes,input_tokens_before,input_tokens_after,model_route_ref,checksum,invocation_id,assistant_message_id,assistant_output_id,reasoning_output_id,ledger_record_id,committed_sequence,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,1,$6::int+100,ARRAY[$7],$8,$9,$10::bigint,$8,$9,$10::bigint,1,1,'test.route@1',$9,$11,$12,$13,$14,$7,$6::int+100,1)")
        .bind(uuid::Uuid::now_v7()).bind(i.tenant_id.into_uuid()).bind(i.conversation_id.into_uuid()).bind(i.run_id.into_uuid()).bind(c.attempt.attempt_id.into_uuid()).bind(iteration).bind(ledger)
        .bind(r.content_id.into_uuid()).bind(r.sha256.into_bytes().to_vec()).bind(r.byte_length as i64).bind(model).bind(uuid::Uuid::now_v7()).bind(uuid::Uuid::now_v7()).bind(uuid::Uuid::now_v7()).execute(&f.pool).await.expect("valid durable execution fixture");
    sqlx::query("INSERT INTO runtime.pending_tool_calls(run_id,invocation_id,tool_call_id,wire_name,arguments_json,ordinal) VALUES($1,$2,$3,'test_call','{}'::jsonb,0)")
        .bind(i.run_id.into_uuid()).bind(model).bind(i.tool_call_id.as_str()).execute(&f.pool).await.expect("valid durable execution fixture");
}
