//! Materialize only declared, accepted prerequisites in the same Task directory.
//! Original report references are both the signed source and snapshot lineage.
use super::*;

pub(super) async fn load(
    tx: &mut PgConnection,
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    dependencies: &[WorkItemId],
    lock: bool,
) -> Result<Vec<WorkItemDependencyResult>, DialogueStoreError> {
    if dependencies.is_empty() {
        return Ok(Vec::new());
    }
    let ids = dependencies
        .iter()
        .map(|id| id.into_uuid())
        .collect::<Vec<_>>();
    let sql = format!("SELECT t.work_item_id,r.goal_revision,r.control_revision,r.assignment_epoch,
          report.producing_run_id,report.report_content_ref
        FROM collaboration.work_items t
        JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
        JOIN collaboration.work_item_execution_reports report ON report.work_item_id=t.work_item_id
          AND report.tenant_id=t.tenant_id AND report.owner_id=t.owner_id
          AND report.requesting_agent_id=t.agent_id AND report.goal_revision=r.goal_revision
          AND report.control_revision=r.control_revision AND report.assignment_epoch=r.assignment_epoch
        JOIN artifact.content_refs content ON content.tenant_id=t.tenant_id AND content.content_id=report.report_content_id
        WHERE t.work_item_id=ANY($1) AND t.tenant_id=$2 AND t.owner_id=$3 AND t.space_id=$4 AND t.agent_id=$5
          AND t.erasure_job_id IS NULL AND r.status='completed' AND report.outcome='completed' AND content.state=2
        ORDER BY t.work_item_id {}", if lock { "FOR SHARE OF t,content" } else { "" });
    let rows = sqlx::query(&sql)
        .bind(&ids)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(scope.space_id.into_uuid())
        .bind(scope.agent_id.into_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
    let mut found = std::collections::BTreeMap::new();
    for row in rows {
        let id = WorkItemId::from_uuid(row.try_get("work_item_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let content_ref = content_from_bytes(row.try_get("report_content_ref").map_err(db)?)?;
        let canonical =
            super::verification::content(tx, actor.tenant_id, content_ref.content_id.into_uuid())
                .await?;
        if canonical != content_ref {
            return Err(DialogueStoreError::Corrupt);
        }
        let value = WorkItemDependencyResult {
            work_item_id: id,
            goal_revision: unsigned(row.try_get("goal_revision").map_err(db)?)?,
            control_revision: unsigned(row.try_get("control_revision").map_err(db)?)?,
            assignment_epoch: unsigned(row.try_get("assignment_epoch").map_err(db)?)?,
            producing_run_id: RunId::from_uuid(row.try_get("producing_run_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            content_ref,
        };
        value.validate()?;
        if found.insert(id, value).is_some() {
            return Err(DialogueStoreError::Corrupt);
        }
    }
    dependencies
        .iter()
        .map(|id| found.remove(id).ok_or(DialogueStoreError::Unavailable))
        .collect()
}
