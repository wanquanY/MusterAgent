//! Exact Task commands can originate in a retained User Run. Their private
//! copies are disposable; the Run, user input and unrelated calls are not.
use super::*;

pub(super) struct SourceCommandClosure {
    pub commands: Vec<uuid::Uuid>,
    pub models: Vec<uuid::Uuid>,
    pub context_contents: Vec<uuid::Uuid>,
    pub contents: Vec<ContentRef>,
}

pub(super) async fn collect(
    tx: &mut sqlx::PgConnection,
    s: &WorkItemProviderScope,
    frozen: Option<&[ContentRef]>,
) -> Result<SourceCommandClosure, Error> {
    // Snapshot references retain the original prerequisite report, not a copy.
    // Its consumer's private model caches must close before the bytes disappear;
    // authored downstream output and the consumer Run retain their own owner.
    let public_contents = crate::dialogue_publication::work_item_erasure::collect(tx, s)
        .await?
        .contents
        .into_iter()
        .map(|v| v.content_id.into_uuid())
        .collect::<Vec<_>>();
    let consumers: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT DISTINCT snap.run_id
        FROM capability.runtime_snapshot_content_references refs JOIN capability.runtime_snapshots snap USING(runtime_snapshot_id)
        WHERE snap.tenant_id=$1 AND NOT(snap.run_id=ANY($3)) AND (refs.content_id=ANY($4)
          OR EXISTS(SELECT 1 FROM collaboration.work_item_execution_reports report WHERE report.tenant_id=snap.tenant_id AND report.work_item_id=$2 AND report.report_content_id=refs.content_id))
        ORDER BY snap.run_id LIMIT 65537")
        .bind(s.tenant()).bind(s.task()).bind(s.runs()).bind(public_contents).fetch_all(&mut *tx).await?;
    if consumers.len() > 65536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let unsafe_consumer: bool = sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM runtime.runs r WHERE r.run_id=ANY($1) AND (r.status NOT IN (5,6,7) OR EXISTS(SELECT 1 FROM runtime.leases l WHERE l.run_id=r.run_id)))
        OR EXISTS(SELECT 1 FROM collaboration.work_item_revisions revision
          JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=revision.work_item_id AND x.goal_revision=revision.goal_revision
          JOIN collaboration.work_item_device_custody device ON device.run_id=x.run_id
          JOIN collaboration.work_items task ON task.work_item_id=x.work_item_id
          WHERE $2=ANY(revision.dependencies) AND task.tenant_id=$3 AND task.erasure_job_id IS NULL)")
        .bind(&consumers).bind(s.task()).bind(s.tenant()).fetch_one(&mut *tx).await?;
    if unsafe_consumer {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let commands:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT i.invocation_id FROM tool.tool_invocations i
        WHERE i.tenant_id=$1 AND NOT(i.run_id=ANY($3)) AND i.invocation_id IN (
          SELECT source_invocation_id FROM collaboration.work_item_execution_reservations WHERE tenant_id=$1 AND work_item_id=$2
          UNION SELECT source_invocation_id FROM collaboration.work_item_commands WHERE tenant_id=$1 AND work_item_id=$2 AND source_invocation_id IS NOT NULL)
        ORDER BY i.invocation_id LIMIT 65537 FOR SHARE OF i")
        .bind(s.tenant()).bind(s.task()).bind(s.runs()).fetch_all(&mut *tx).await?;
    if commands.len() > 65536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations i
        LEFT JOIN runtime.pending_tool_calls p ON p.run_id=i.run_id AND p.tool_call_id=i.tool_call_id
        WHERE i.invocation_id=ANY($1) AND (i.status<>4 OR p.completed_at_ms IS NULL))")
        .bind(&commands).fetch_one(&mut *tx).await?;
    if pending {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    // A frozen Content set is the exact cache cut. Subsequent safe Context
    // requests must never be swept into a previously completed erasure.
    let frozen_ids = frozen.map(|refs| {
        refs.iter()
            .map(|r| r.content_id.into_uuid())
            .collect::<Vec<_>>()
    });
    let plans=sqlx::query("SELECT p.invocation_id,p.request_content_id,
        CASE WHEN EXISTS(SELECT 1 FROM runtime.context_compaction_slots slot WHERE slot.tenant_id=p.tenant_id AND slot.run_id=p.run_id AND slot.loop_iteration=p.iteration)
          THEN p.summary_content_id ELSE NULL END AS summary_content_id
        FROM runtime.context_plans p WHERE p.tenant_id=$1 AND (p.run_id=ANY($4) OR EXISTS(
          SELECT 1 FROM tool.tool_invocations i JOIN runtime.pending_tool_calls call ON call.run_id=i.run_id AND call.tool_call_id=i.tool_call_id
          JOIN runtime.model_invocations m ON m.invocation_id=call.invocation_id
          WHERE i.invocation_id=ANY($2) AND p.run_id=i.run_id AND p.source_sequence_to>m.started_conversation_seq))
        AND ($3::uuid[] IS NULL OR p.request_content_id=ANY($3) OR p.summary_content_id=ANY($3))
        ORDER BY p.snapshot_id LIMIT 65537 FOR SHARE OF p")
        .bind(s.tenant()).bind(&commands).bind(frozen_ids).bind(&consumers).fetch_all(&mut *tx).await?;
    if plans.len() > 65536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let mut models:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT DISTINCT p.invocation_id FROM runtime.pending_tool_calls p
        JOIN tool.tool_invocations i ON i.run_id=p.run_id AND i.tool_call_id=p.tool_call_id WHERE i.invocation_id=ANY($1)")
        .bind(&commands).fetch_all(&mut *tx).await?;
    models.extend(sqlx::query_scalar::<_, uuid::Uuid>("SELECT invocation_id FROM runtime.model_invocations WHERE tenant_id=$1 AND run_id=ANY($2)")
        .bind(s.tenant()).bind(&consumers).fetch_all(&mut *tx).await?);
    let mut context_contents = Vec::new();
    for row in plans {
        models.push(row.try_get("invocation_id")?);
        context_contents.push(row.try_get("request_content_id")?);
        if let Some(id) = row.try_get::<Option<uuid::Uuid>, _>("summary_content_id")? {
            context_contents.push(id);
        }
    }
    // Only actual compaction may contain private summary prose. Empty structured
    // summaries carry a cut, not copied Tool arguments, and remain reusable.
    let compactors=sqlx::query("SELECT m.invocation_id,m.request_content_id,m.response_content_id
        FROM runtime.context_compaction_slots slot JOIN runtime.model_invocations m ON m.invocation_id=slot.invocation_id
        WHERE slot.tenant_id=$1 AND (slot.run_id=ANY($4) OR EXISTS(SELECT 1 FROM tool.tool_invocations i
          JOIN runtime.pending_tool_calls call ON call.run_id=i.run_id AND call.tool_call_id=i.tool_call_id
          JOIN runtime.model_invocations producer ON producer.invocation_id=call.invocation_id
          WHERE i.invocation_id=ANY($2) AND i.run_id=slot.run_id AND slot.source_sequence_to>producer.started_conversation_seq))
        AND ($3::uuid[] IS NULL OR m.request_content_id=ANY($3) OR m.response_content_id=ANY($3))
        ORDER BY m.invocation_id LIMIT 65537 FOR SHARE OF m")
        .bind(s.tenant()).bind(&commands).bind(frozen.map(|refs|refs.iter().map(|r|r.content_id.into_uuid()).collect::<Vec<_>>())).bind(&consumers)
        .fetch_all(&mut *tx).await?;
    if compactors.len() > 65536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    for row in compactors {
        models.push(row.try_get("invocation_id")?);
        context_contents.push(row.try_get("request_content_id")?);
        if let Some(id) = row.try_get::<Option<uuid::Uuid>, _>("response_content_id")? {
            context_contents.push(id);
        }
    }
    models.sort_unstable();
    models.dedup();
    context_contents.sort_unstable();
    context_contents.dedup();
    let live:bool=sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM runtime.model_invocations WHERE invocation_id=ANY($1) AND status=1)
        OR EXISTS(SELECT 1 FROM runtime.pending_tool_calls WHERE invocation_id=ANY($1) AND completed_at_ms IS NULL)
        OR EXISTS(SELECT 1 FROM runtime.model_completion_intents WHERE invocation_id=ANY($1) AND lifecycle<>5)")
        .bind(&models).fetch_one(&mut *tx).await?;
    if live {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let rows=sqlx::query("WITH ids(id) AS (
        SELECT unnest(ARRAY[request_content_id,result_content_id]) FROM tool.tool_invocations WHERE invocation_id=ANY($2)
        UNION SELECT result_content_id FROM tool.tool_results WHERE invocation_id=ANY($2)
        UNION SELECT unnest(ARRAY[p.result_content_id,p.provider_replay_content_id]) FROM runtime.pending_tool_calls p JOIN tool.tool_invocations i ON i.run_id=p.run_id AND i.tool_call_id=p.tool_call_id WHERE i.invocation_id=ANY($2)
        UNION SELECT prompt_content_id FROM approval.interactions WHERE requested_by_invocation_id=ANY($2) AND tenant_id=$1
        UNION SELECT r.content_id FROM approval.interaction_responses r JOIN approval.interactions i USING(interaction_id) WHERE i.requested_by_invocation_id=ANY($2) AND i.tenant_id=$1
        UNION SELECT unnest($3::uuid[])
        UNION SELECT reasoning_replay_content_id FROM runtime.model_invocations WHERE invocation_id=ANY($4)
    ) SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash FROM ids LEFT JOIN artifact.content_refs c ON c.content_id=ids.id AND c.tenant_id=$1 WHERE ids.id IS NOT NULL ORDER BY c.content_id LIMIT 65537")
        .bind(s.tenant()).bind(&commands).bind(&context_contents).bind(&models).fetch_all(&mut *tx).await?;
    if rows.len() > 65536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let contents = rows
        .into_iter()
        .map(|r| {
            Ok(ContentRef {
                content_id: ContentId::from_uuid(r.try_get("content_id")?)
                    .map_err(|_| Error::CorruptState("source command content"))?,
                media_type: musteragent_content_ref::MediaType::new(
                    r.try_get::<String, _>("media_type")?,
                )
                .map_err(|_| Error::CorruptState("source command media"))?,
                byte_length: r
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| Error::CorruptState("source command length"))?,
                sha256: musteragent_content_ref::Sha256::from_bytes(
                    r.try_get::<Vec<u8>, _>("content_hash")?,
                )
                .map_err(|_| Error::CorruptState("source command hash"))?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(SourceCommandClosure {
        commands,
        models,
        context_contents,
        contents,
    })
}

pub(super) async fn erase(
    tx: &mut sqlx::PgConnection,
    c: &SourceCommandClosure,
    job: ErasureJobId,
) -> Result<(), Error> {
    for sql in [
        "UPDATE tool.tool_invocations SET reconciliation_ref_protected=NULL,terminal_error=NULL,terminal_reason=NULL,erasure_job_id=$2 WHERE invocation_id=ANY($1) AND erasure_job_id IS NULL",
        "UPDATE tool.tool_results SET provider_request_ref=NULL,provider_request_ref_protected=NULL,erasure_job_id=$2 WHERE invocation_id=ANY($1) AND erasure_job_id IS NULL",
        "UPDATE runtime.pending_tool_calls p SET arguments_json=NULL,argument_diagnostic_json=NULL,erasure_job_id=$2 FROM tool.tool_invocations i WHERE i.invocation_id=ANY($1) AND i.run_id=p.run_id AND i.tool_call_id=p.tool_call_id AND p.erasure_job_id IS NULL",
        "UPDATE approval.interaction_responses r SET secure_response_ref=NULL,secure_response_ref_protected=NULL,erasure_job_id=$2 FROM approval.interactions i WHERE i.interaction_id=r.interaction_id AND i.requested_by_invocation_id=ANY($1) AND r.erasure_job_id IS NULL",
    ] {
        sqlx::query(sql)
            .bind(&c.commands)
            .bind(job.into_uuid())
            .execute(&mut *tx)
            .await?;
    }
    crate::dialogue_context::work_item_erasure::erase_model_caches(tx, &c.models, job).await?;
    Ok(())
}

pub(super) async fn verify(
    tx: &mut sqlx::PgConnection,
    c: &SourceCommandClosure,
    job: ErasureJobId,
) -> Result<(), Error> {
    for query in [
        "SELECT EXISTS(SELECT 1 FROM tool.tool_invocations WHERE invocation_id=ANY($1) AND erasure_job_id IS DISTINCT FROM $2)",
        "SELECT EXISTS(SELECT 1 FROM tool.tool_results WHERE invocation_id=ANY($1) AND erasure_job_id IS DISTINCT FROM $2)",
        "SELECT EXISTS(SELECT 1 FROM runtime.pending_tool_calls p JOIN tool.tool_invocations i ON i.run_id=p.run_id AND i.tool_call_id=p.tool_call_id WHERE i.invocation_id=ANY($1) AND p.erasure_job_id IS DISTINCT FROM $2)",
        "SELECT EXISTS(SELECT 1 FROM approval.interaction_responses r JOIN approval.interactions i USING(interaction_id) WHERE i.requested_by_invocation_id=ANY($1) AND r.erasure_job_id IS DISTINCT FROM $2)",
    ] {
        if sqlx::query_scalar::<_, bool>(query)
            .bind(&c.commands)
            .bind(job.into_uuid())
            .fetch_one(&mut *tx)
            .await?
        {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    crate::dialogue_context::work_item_erasure::verify_model_caches(tx, &c.models).await?;
    Ok(())
}
