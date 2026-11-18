//! A returned report is authored evidence, not the source conversation. Its
//! continuation has its own canonical Run, which must close independently.
use super::*;

pub(super) struct ReportClosure {
    pub runs: Vec<uuid::Uuid>,
    pub contents: Vec<ContentRef>,
}

pub(super) async fn collect_reports(
    tx: &mut sqlx::PgConnection,
    lineage: &WorkItemErasureLineage,
    executions: &[uuid::Uuid],
) -> Result<ReportClosure, Error> {
    let rows = sqlx::query(
        "SELECT p.report_run_id,p.report_content_ref,p.report_content_id,p.report_sha256,
           c.media_type,c.byte_length::text AS byte_length,c.content_hash,
           r.run_id IS NOT NULL AS admitted,r.status,
           EXISTS(SELECT 1 FROM runtime.runs any_run WHERE any_run.run_id=p.report_run_id) AS run_exists,
           EXISTS(SELECT 1 FROM runtime.leases l WHERE l.run_id=p.report_run_id) AS leased,
           s.conversation_id IS NOT NULL AS session_bound,
           p.producing_run_id=ANY($4) AND p.requesting_agent_id=$5
             AND p.source_dialogue_id=$6 AS source_bound
         FROM collaboration.work_item_execution_reports p
         JOIN artifact.content_refs c ON c.tenant_id=p.tenant_id AND c.content_id=p.report_content_id
         LEFT JOIN runtime.runs r ON r.run_id=p.report_run_id AND r.tenant_id=p.tenant_id
           AND r.conversation_id=p.report_conversation_id AND r.parent_run_id IS NULL AND r.delegation_id IS NULL
         LEFT JOIN agent.dialogue_sessions s ON s.conversation_id=p.report_conversation_id
           AND s.tenant_id=p.tenant_id AND s.owner_id=p.owner_id AND s.agent_id=p.requesting_agent_id
           AND s.dialogue_id=p.source_dialogue_id AND s.subject_kind='work_item_report'
           AND s.report_from_run_id=p.producing_run_id AND s.work_item_id=p.work_item_id
           AND s.work_item_goal_revision=p.goal_revision AND s.work_item_control_revision=p.control_revision
           AND s.work_item_assignment_epoch=p.assignment_epoch
         WHERE p.work_item_id=$1 AND p.tenant_id=$2 AND p.owner_id=$3
         ORDER BY p.producing_run_id LIMIT 4097 FOR SHARE OF p,c",
    ).bind(lineage.work_item_id.into_uuid()).bind(lineage.actor.tenant_id.into_uuid())
        .bind(lineage.actor.principal_id.into_uuid()).bind(executions)
        .bind(lineage.agent_id.into_uuid()).bind(lineage.source_dialogue_id.into_uuid())
        .fetch_all(&mut *tx).await?;
    if rows.len() > MAX_WORK_ITEM_ERASURE_EXECUTIONS {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let mut result = ReportClosure {
        runs: Vec::new(),
        contents: Vec::new(),
    };
    for row in rows {
        if !row.try_get::<bool, _>("source_bound")? {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
        if row.try_get::<bool, _>("admitted")? {
            if !matches!(row.try_get::<Option<i16>, _>("status")?, Some(5..=7))
                || row.try_get::<bool, _>("leased")?
                || !row.try_get::<bool, _>("session_bound")?
            {
                return Err(Error::WorkItemExecutionClosureRequired);
            }
        } else if row.try_get::<bool, _>("run_exists")?
            || row.try_get::<bool, _>("session_bound")?
            || row.try_get::<bool, _>("leased")?
        {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
        result.runs.push(row.try_get("report_run_id")?);
        let content = crate::dialogue_codec::content_from_bytes(row.try_get("report_content_ref")?)
            .map_err(|_| Error::CorruptState("report content reference"))?;
        if content.content_id.into_uuid() != row.try_get::<uuid::Uuid, _>("report_content_id")?
            || content.media_type.as_str() != row.try_get::<String, _>("media_type")?
            || content.byte_length
                != row
                    .try_get::<String, _>("byte_length")?
                    .parse::<u64>()
                    .map_err(|_| Error::CorruptState("report content length"))?
            || content.sha256.into_bytes().as_slice()
                != row.try_get::<Vec<u8>, _>("content_hash")?
            || content.sha256.into_bytes().as_slice()
                != row.try_get::<Vec<u8>, _>("report_sha256")?
        {
            return Err(Error::CorruptState("report content identity"));
        }
        result.contents.push(content);
    }
    Ok(result)
}
