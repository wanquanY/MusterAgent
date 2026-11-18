//! Provider scope is constructed only from the original fenced Task and its
//! canonical execution bindings. A caller cannot nominate arbitrary Run IDs.
use super::*;

pub(crate) struct WorkItemProviderScope {
    tenant: TenantId,
    task: WorkItemId,
    runs: Vec<uuid::Uuid>,
    report_contents: Vec<ContentRef>,
}
impl WorkItemProviderScope {
    pub(crate) fn report_contents(&self) -> &[ContentRef] {
        &self.report_contents
    }
    pub(crate) fn task(&self) -> uuid::Uuid {
        self.task.into_uuid()
    }
    pub(crate) fn tenant(&self) -> uuid::Uuid {
        self.tenant.into_uuid()
    }
    pub(crate) fn runs(&self) -> &[uuid::Uuid] {
        &self.runs
    }
}

pub(super) async fn provider_scope(
    tx: &mut sqlx::PgConnection,
    lineage: &WorkItemErasureLineage,
) -> Result<WorkItemProviderScope, Error> {
    let fenced: Option<bool> = sqlx::query_scalar(
        "SELECT erasure_job_id=$5 AND state_revision=$6 FROM collaboration.work_items
         WHERE work_item_id=$1 AND tenant_id=$2 AND owner_id=$3 AND agent_id=$4 FOR SHARE",
    )
    .bind(lineage.work_item_id.into_uuid())
    .bind(lineage.actor.tenant_id.into_uuid())
    .bind(lineage.actor.principal_id.into_uuid())
    .bind(lineage.agent_id.into_uuid())
    .bind(lineage.erasure_job_id.into_uuid())
    .bind(super::super::to_i64(lineage.state_revision + 1, "state")?)
    .fetch_optional(&mut *tx)
    .await?;
    if fenced != Some(true) {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let rows = sqlx::query(
        "SELECT x.run_id, r.status,
           EXISTS(SELECT 1 FROM runtime.runs actual WHERE actual.run_id=x.run_id OR actual.conversation_id=x.conversation_id) AS run_exists,
           EXISTS(SELECT 1 FROM runtime.leases l WHERE l.run_id=x.run_id) AS leased,
           s.conversation_id IS NOT NULL AS session_bound
         FROM collaboration.work_item_execution_reservations x
         LEFT JOIN runtime.runs r ON r.run_id=x.run_id AND r.tenant_id=x.tenant_id
           AND r.conversation_id=x.conversation_id AND r.parent_run_id IS NULL AND r.delegation_id IS NULL
         LEFT JOIN agent.dialogue_sessions s ON s.conversation_id=x.conversation_id
           AND s.tenant_id=x.tenant_id AND s.owner_id=x.owner_id AND s.work_item_id=x.work_item_id
           AND s.subject_kind='work_item' AND s.work_item_goal_revision=x.goal_revision
           AND s.work_item_control_revision=x.control_revision AND s.work_item_assignment_epoch=x.assignment_epoch
         WHERE x.work_item_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3 ORDER BY x.run_id LIMIT 4097",
    ).bind(lineage.work_item_id.into_uuid()).bind(lineage.actor.tenant_id.into_uuid())
        .bind(lineage.actor.principal_id.into_uuid()).fetch_all(&mut *tx).await?;
    if rows.is_empty() || rows.len() > MAX_WORK_ITEM_ERASURE_EXECUTIONS {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let mut runs = Vec::with_capacity(rows.len());
    for row in rows {
        let status = row.try_get::<Option<i16>, _>("status")?;
        let valid = if let Some(status) = status {
            matches!(status, 5..=7) && row.try_get::<bool, _>("session_bound")?
        } else {
            !row.try_get::<bool, _>("run_exists")? && !row.try_get::<bool, _>("session_bound")?
        };
        if !valid || row.try_get::<bool, _>("leased")? {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
        runs.push(row.try_get("run_id")?);
    }
    let reports = super::reports::collect_reports(tx, lineage, &runs).await?;
    runs.extend(reports.runs);
    runs.sort_unstable();
    let count = runs.len();
    runs.dedup();
    if runs.len() != count || count > MAX_WORK_ITEM_ERASURE_EXECUTIONS {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    // Every admitted typed session must belong to either its actual execution
    // reservation or exact report continuation, never the source User Run.
    let extra: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r
           ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
         WHERE s.work_item_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND NOT(r.run_id=ANY($4)))",
    )
    .bind(lineage.work_item_id.into_uuid())
    .bind(lineage.actor.tenant_id.into_uuid())
    .bind(lineage.actor.principal_id.into_uuid())
    .bind(&runs)
    .fetch_one(&mut *tx)
    .await?;
    if extra {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(WorkItemProviderScope {
        tenant: lineage.actor.tenant_id,
        task: lineage.work_item_id,
        runs,
        report_contents: reports.contents,
    })
}
