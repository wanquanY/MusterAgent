//! Read the original successful Tool/publication receipts at the last model
//! boundary. A public entry on its own is never a Run-completion proof.
use super::*;

pub(crate) async fn native_tools_ready(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    iteration: i32,
    task_result: Option<musteragent_execution_context::WorkItemResultStep>,
) -> Result<bool, Error> {
    let plan = sqlx::query(
        "SELECT p.invocation_id,p.source_sequence_to::text AS source_sequence_to
         FROM runtime.context_plans p JOIN runtime.model_invocations m
           ON m.tenant_id=p.tenant_id AND m.run_id=p.run_id AND m.invocation_id=p.invocation_id
         WHERE p.tenant_id=$1 AND p.run_id=$2 AND p.iteration=$3 AND m.status=2
         ORDER BY p.committed_sequence DESC LIMIT 1",
    )
    .bind(tenant.into_uuid())
    .bind(run.into_uuid())
    .bind(iteration)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let Some(plan) = plan else { return Ok(false) };
    let calls = sqlx::query(
        "SELECT p.completed_at_ms,t.status,t.invocation_id,t.tool_ref AS tool_key,t.request_hash,
                t.result_content_id,result.result_hash,result.result_schema_key,result.result_schema_hash,
                g.protocol_ref,g.tool_ref,g.request_hash AS publication_request_hash,
                g.purpose,g.entry_count,g.source_intent_id,
                s.source_intent_id AS expected_intent,
                (SELECT COUNT(*) FROM agent.dialogue_publications a
                  WHERE a.tenant_id=$1 AND a.source_run_id=$2
                    AND a.source_kind='tool_owner' AND a.source_tool_invocation_id=t.invocation_id
                    AND a.source_intent_id=g.source_intent_id AND a.purpose=g.purpose) AS actual_entries
         FROM runtime.pending_tool_calls p
         LEFT JOIN tool.tool_invocations t ON t.tenant_id=$1 AND t.run_id=p.run_id AND t.tool_call_id=p.tool_call_id
         LEFT JOIN tool.tool_results result ON result.invocation_id=t.invocation_id AND result.result_content_id=t.result_content_id
         LEFT JOIN agent.dialogue_tool_publication_groups g
           ON g.tenant_id=t.tenant_id AND g.source_run_id=t.run_id AND g.invocation_id=t.invocation_id
         JOIN runtime.runs r ON r.tenant_id=$1 AND r.run_id=p.run_id
         JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
         WHERE p.run_id=$2 AND p.invocation_id=$3 ORDER BY p.ordinal",
    ).bind(tenant.into_uuid()).bind(run.into_uuid())
        .bind(plan.try_get::<uuid::Uuid,_>("invocation_id").map_err(db)?)
        .fetch_all(&mut *tx).await.map_err(db)?;
    if calls.is_empty() {
        return Ok(false);
    }
    use musteragent_execution_context::WorkItemResultStep;
    let expected_task_tool = task_result.map(|step| match step {
        WorkItemResultStep::SubmitV1 | WorkItemResultStep::ReturnToRequester => {
            WORK_ITEM_RESULT_SUBMIT_TOOL
        }
        WorkItemResultStep::VerifyV1 => WORK_ITEM_RESULT_VERIFY_TOOL,
    });
    if expected_task_tool.is_some() && calls.len() != 1 {
        return Ok(false);
    }
    let mut final_delivery = false;
    for call in calls {
        if call
            .try_get::<Option<i64>, _>("completed_at_ms")
            .map_err(db)?
            .is_none()
            || call.try_get::<Option<i16>, _>("status").map_err(db)? != Some(4)
        {
            // Includes rejected, cancelled, failed and indeterminate siblings.
            return Ok(false);
        }
        let key = call.try_get::<&str, _>("tool_key").map_err(db)?;
        if let Some(expected) = expected_task_tool {
            if key != expected {
                return Ok(false);
            }
            let invocation = InvocationId::from_uuid(call.try_get("invocation_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?;
            final_delivery = crate::work_item_execution::result_receipts::completion(
                tx, tenant, run, invocation, key,
            )
            .await?;
            continue;
        }
        if matches!(
            key,
            WORK_ITEM_RESULT_SUBMIT_TOOL | WORK_ITEM_RESULT_VERIFY_TOOL
        ) {
            return Ok(false);
        }
        if key == "tasks.feedback@1" {
            let invocation: uuid::Uuid = call.try_get("invocation_id").map_err(db)?;
            final_delivery |= sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_requester_actions WHERE report_run_id=$1 AND invocation_id=$2 AND next_run_id IS NOT NULL)").bind(run.into_uuid()).bind(invocation).fetch_one(&mut *tx).await.map_err(db)?;
            continue;
        }
        if key == "tasks.handoff@1" {
            // This tool's only success writer is the original Task Owner. It
            // atomically verifies the entire source-Run delegation set before
            // committing this result; task progress stays with the Task owner.
            if call
                .try_get::<Option<uuid::Uuid>, _>("result_content_id")
                .map_err(db)?
                .is_none()
                || call
                    .try_get::<Option<Vec<u8>>, _>("result_hash")
                    .map_err(db)?
                    .is_none()
                || call
                    .try_get::<Option<String>, _>("result_schema_key")
                    .map_err(db)?
                    .is_none()
                || call
                    .try_get::<Option<Vec<u8>>, _>("result_schema_hash")
                    .map_err(db)?
                    .is_none()
            {
                return Err(DialogueStoreError::Corrupt.into());
            }
            final_delivery = true;
            continue;
        }

        let Some(protocol) = call
            .try_get::<Option<String>, _>("protocol_ref")
            .map_err(db)?
        else {
            continue;
        };
        if protocol != DialogueOutputContract::NativeToolsV1.key()
            || call.try_get::<String, _>("tool_ref").map_err(db)?
                != call.try_get::<String, _>("tool_key").map_err(db)?
            || call
                .try_get::<Vec<u8>, _>("publication_request_hash")
                .map_err(db)?
                != call.try_get::<Vec<u8>, _>("request_hash").map_err(db)?
            || call
                .try_get::<uuid::Uuid, _>("source_intent_id")
                .map_err(db)?
                != call
                    .try_get::<uuid::Uuid, _>("expected_intent")
                    .map_err(db)?
            || i64::from(call.try_get::<i16, _>("entry_count").map_err(db)?)
                != call.try_get::<i64, _>("actual_entries").map_err(db)?
            || call
                .try_get::<Option<uuid::Uuid>, _>("result_content_id")
                .map_err(db)?
                .is_none()
            || call
                .try_get::<Option<Vec<u8>>, _>("result_hash")
                .map_err(db)?
                .is_none()
            || call
                .try_get::<Option<String>, _>("result_schema_key")
                .map_err(db)?
                .is_none()
            || call
                .try_get::<Option<Vec<u8>>, _>("result_schema_hash")
                .map_err(db)?
                .is_none()
        {
            return Err(DialogueStoreError::Corrupt.into());
        }
        final_delivery |= matches!(
            call.try_get::<&str, _>("purpose").map_err(db)?,
            "reply" | "result" | "blocked"
        );
    }
    if !final_delivery {
        return Ok(false);
    }
    // Only this request's unfinished work participates. Other dialogue Runs
    // and intentionally detached spawn children do not serialize the Dialogue.
    // A durable indeterminate observation stays unknown; it does not keep a
    // later, independently verified final delivery in an endless model loop.
    let unfinished: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id=$1 AND t.run_id=$2
             AND (t.status IN (1,2,3) OR (t.status=6 AND NOT EXISTS (
                 SELECT 1 FROM runtime.pending_tool_calls p
                 WHERE p.run_id=t.run_id AND p.tool_call_id=t.tool_call_id
                   AND p.completed_at_ms IS NOT NULL
                   AND p.result_content_id IS NOT NULL AND p.result_content_hash IS NOT NULL))))
             OR EXISTS(SELECT 1 FROM approval.interactions WHERE tenant_id=$1 AND run_id=$2 AND status=1)
             OR EXISTS(SELECT 1 FROM runtime.delegations WHERE tenant_id=$1 AND parent_run_id=$2 AND kind=1 AND status=1)
             OR EXISTS(SELECT 1 FROM runtime.loop_controls WHERE tenant_id=$1 AND run_id=$2
                 AND status IN (1,2) AND source_conversation_seq>$3::numeric)",
    ).bind(tenant.into_uuid()).bind(run.into_uuid())
        .bind(plan.try_get::<String,_>("source_sequence_to").map_err(db)?)
        .fetch_one(&mut *tx).await.map_err(db)?;
    Ok(!unfinished)
}
