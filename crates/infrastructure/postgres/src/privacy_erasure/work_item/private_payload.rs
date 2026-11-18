//! Private Runtime copies are owned by the actual independent Task Runs. Input
//! references in their snapshots are not owned output and never enter this set.
use super::*;
use musteragent_content_ref::{MediaType, Sha256};

pub(super) async fn collect(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
) -> Result<Vec<ContentRef>, Error> {
    let live: bool = sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM runtime.model_invocations WHERE tenant_id=$1 AND run_id=ANY($2) AND status=1)
        OR EXISTS(SELECT 1 FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=ANY($2) AND status IN (1,2,3,6))")
        .bind(scope.tenant()).bind(scope.runs()).fetch_one(&mut *tx).await?;
    if live {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let rows=sqlx::query("WITH owned(content_id) AS (
        SELECT unnest(ARRAY[request_content_id,response_content_id,reasoning_replay_content_id]) FROM runtime.model_invocations WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT unnest(ARRAY[request_content_id,result_content_id]) FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT result_content_id FROM tool.tool_results WHERE invocation_id IN (SELECT invocation_id FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=ANY($2))
        UNION SELECT unnest(ARRAY[result_content_id,provider_replay_content_id]) FROM runtime.pending_tool_calls WHERE run_id=ANY($2)
        UNION SELECT prompt_content_id FROM approval.interactions WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT content_id FROM approval.interaction_responses WHERE interaction_id IN (SELECT interaction_id FROM approval.interactions WHERE tenant_id=$1 AND run_id=ANY($2))
        UNION SELECT unnest(ARRAY[summary_content_id,request_content_id]) FROM runtime.context_plans WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT plan_content_id FROM runtime.execution_plans WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT reason_content_id FROM runtime.tool_exposures WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT o.ready_content_id FROM artifact.invocation_content_outputs o JOIN tool.tool_invocations i ON i.invocation_id=o.invocation_id WHERE i.tenant_id=$1 AND i.run_id=ANY($2)
        UNION SELECT content_id FROM conversation.messages WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION SELECT unnest(ARRAY[manifest_content_id,receipt_content_id]) FROM collaboration.work_item_result_submissions WHERE tenant_id=$1 AND work_item_id=$3
        UNION SELECT item.content_id FROM collaboration.work_item_result_items item JOIN collaboration.work_item_result_submissions sub USING(result_id)
             WHERE sub.tenant_id=$1 AND sub.work_item_id=$3 AND item.artifact_id IS NULL
    ) SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
        FROM owned o LEFT JOIN artifact.content_refs c ON c.content_id=o.content_id AND c.tenant_id=$1
        WHERE o.content_id IS NOT NULL ORDER BY c.content_id LIMIT 65537")
        .bind(scope.tenant()).bind(scope.runs()).bind(scope.task()).fetch_all(&mut *tx).await?;
    if rows.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    rows.into_iter()
        .map(|row| {
            Ok(ContentRef {
                content_id: ContentId::from_uuid(row.try_get("content_id")?)
                    .map_err(|_| Error::CorruptState("private content id"))?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| Error::CorruptState("private media type"))?,
                byte_length: row
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| Error::CorruptState("private content length"))?,
                sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| Error::CorruptState("private content digest"))?,
            })
        })
        .collect()
}

pub(super) async fn erase(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    job: ErasureJobId,
) -> Result<(), Error> {
    // Each update is constrained again by the original owner's DB guard. All
    // stable identities/digests remain; NULL is a tombstone, never a fake body.
    for statement in [
        "UPDATE runtime.model_invocations SET provider_request_ref=NULL,continuation_input_ref=NULL,continuation_output_ref=NULL,completion_intent_protected=NULL,terminal_error=NULL,erasure_job_id=$3 WHERE tenant_id=$1 AND run_id=ANY($2) AND erasure_job_id IS NULL",
        "UPDATE tool.tool_invocations SET reconciliation_ref_protected=NULL,terminal_error=NULL,terminal_reason=NULL,erasure_job_id=$3 WHERE tenant_id=$1 AND run_id=ANY($2) AND erasure_job_id IS NULL",
        "UPDATE tool.tool_results SET provider_request_ref=NULL,provider_request_ref_protected=NULL,erasure_job_id=$3 WHERE invocation_id IN (SELECT invocation_id FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=ANY($2)) AND erasure_job_id IS NULL",
        "UPDATE approval.interaction_responses SET secure_response_ref=NULL,secure_response_ref_protected=NULL,erasure_job_id=$3 WHERE interaction_id IN (SELECT interaction_id FROM approval.interactions WHERE tenant_id=$1 AND run_id=ANY($2)) AND erasure_job_id IS NULL",
        "UPDATE runtime.pending_tool_calls SET arguments_json=NULL,argument_diagnostic_json=NULL,erasure_job_id=$3 WHERE run_id=ANY($2) AND EXISTS(SELECT 1 FROM runtime.runs r WHERE r.run_id=runtime.pending_tool_calls.run_id AND r.tenant_id=$1) AND erasure_job_id IS NULL",
        "UPDATE runtime.runs SET terminal_error=NULL,erasure_job_id=$3 WHERE tenant_id=$1 AND run_id=ANY($2) AND erasure_job_id IS NULL",
        "UPDATE capability.runtime_snapshots SET encoded_snapshot=NULL,execution_grant_proof_schema=NULL,execution_grant_proof=NULL,execution_grant_proof_hash=NULL,execution_grant_claims_hash=NULL,erasure_job_id=$3 WHERE tenant_id=$1 AND run_id=ANY($2) AND erasure_job_id IS NULL",
        "UPDATE event_store.domain_events SET encoded_event=NULL,erasure_job_id=$3 WHERE record_id IN (SELECT record_id FROM event_store.ledger_records WHERE tenant_id=$1 AND run_id=ANY($2)) AND erasure_job_id IS NULL",
        "UPDATE event_store.output_frames SET encoded_frame=NULL,erasure_job_id=$3 WHERE record_id IN (SELECT record_id FROM event_store.ledger_records WHERE tenant_id=$1 AND run_id=ANY($2)) AND erasure_job_id IS NULL",
        "UPDATE event_store.snapshots SET encoded_snapshot=NULL,erasure_job_id=$3 WHERE tenant_id=$1 AND conversation_id IN (SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=ANY($2)) AND erasure_job_id IS NULL",
        "UPDATE event_store.outbox SET payload=NULL,erasure_job_id=$3,lease_owner=NULL,lease_expires_at_ms=NULL,lease_generation=lease_generation+1 WHERE tenant_id=$1 AND conversation_id IN (SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=ANY($2)) AND erasure_job_id IS NULL",
    ] {
        sqlx::query(statement)
            .bind(scope.tenant())
            .bind(scope.runs())
            .bind(job.into_uuid())
            .execute(&mut *tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn verify(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    job: ErasureJobId,
) -> Result<(), Error> {
    // Guards make marked rows irrecoverable. Re-read every original relation,
    // not a cached count captured before a worker or projection may have run.
    for (table, predicate) in [
        ("runtime.runs", "tenant_id=$1 AND run_id=ANY($2)"),
        (
            "runtime.model_invocations",
            "tenant_id=$1 AND run_id=ANY($2)",
        ),
        ("tool.tool_invocations", "tenant_id=$1 AND run_id=ANY($2)"),
        (
            "tool.tool_results",
            "invocation_id IN (SELECT invocation_id FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=ANY($2))",
        ),
        (
            "approval.interaction_responses",
            "interaction_id IN (SELECT interaction_id FROM approval.interactions WHERE tenant_id=$1 AND run_id=ANY($2))",
        ),
        (
            "runtime.pending_tool_calls",
            "run_id=ANY($2) AND EXISTS(SELECT 1 FROM runtime.runs r WHERE r.run_id=x.run_id AND r.tenant_id=$1)",
        ),
        (
            "capability.runtime_snapshots",
            "tenant_id=$1 AND run_id=ANY($2)",
        ),
        (
            "event_store.domain_events",
            "record_id IN (SELECT record_id FROM event_store.ledger_records WHERE tenant_id=$1 AND run_id=ANY($2))",
        ),
        (
            "event_store.output_frames",
            "record_id IN (SELECT record_id FROM event_store.ledger_records WHERE tenant_id=$1 AND run_id=ANY($2))",
        ),
        (
            "event_store.snapshots",
            "tenant_id=$1 AND conversation_id IN (SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=ANY($2))",
        ),
        (
            "event_store.outbox",
            "tenant_id=$1 AND conversation_id IN (SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=ANY($2))",
        ),
    ] {
        let remaining:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {table} x WHERE ({predicate}) AND erasure_job_id IS DISTINCT FROM $3)"))
            .bind(scope.tenant()).bind(scope.runs()).bind(job.into_uuid()).fetch_one(&mut *tx).await?;
        if remaining {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    Ok(())
}
