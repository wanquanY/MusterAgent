//! The original erasure aggregate freezes derived coverage separately from the
//! user's immutable roots. Every replay reconstructs and verifies that proof.
use super::*;
use musteragent_persistence_codec::encode_content_ref;
use prost::Message;

async fn identities(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
) -> Result<Vec<WorkItemErasureExecution>, Error> {
    let rows=sqlx::query("SELECT run_id,conversation_id,goal_revision AS work_item_goal_revision,
        control_revision AS work_item_control_revision,assignment_epoch AS work_item_assignment_epoch
        FROM collaboration.work_item_execution_reservations WHERE tenant_id=$1 AND run_id=ANY($2)
        UNION ALL SELECT report_run_id,report_conversation_id,goal_revision,control_revision,assignment_epoch
        FROM collaboration.work_item_execution_reports WHERE tenant_id=$1 AND report_run_id=ANY($2)
        ORDER BY run_id")
        .bind(scope.tenant()).bind(scope.runs()).fetch_all(&mut *tx).await?;
    if rows.len() != scope.runs().len() {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    rows.into_iter()
        .map(|r| {
            Ok(WorkItemErasureExecution {
                run_id: RunId::from_uuid(r.try_get("run_id")?)
                    .map_err(|_| Error::CorruptState("coverage run"))?,
                conversation_id: ConversationId::from_uuid(r.try_get("conversation_id")?)
                    .map_err(|_| Error::CorruptState("coverage conversation"))?,
                goal_revision: super::super::from_i64(
                    r.try_get("work_item_goal_revision")?,
                    "goal",
                )?,
                control_revision: super::super::from_i64(
                    r.try_get("work_item_control_revision")?,
                    "control",
                )?,
                assignment_epoch: super::super::from_i64(
                    r.try_get("work_item_assignment_epoch")?,
                    "assignment",
                )?,
            })
        })
        .collect()
}

pub(super) async fn load(
    tx: &mut sqlx::PgConnection,
    lineage: &WorkItemErasureLineage,
    scope: &WorkItemProviderScope,
) -> Result<Option<WorkItemExecutionErasureCoverage>, Error> {
    let row=sqlx::query("SELECT execution_coverage_version,execution_run_ids,execution_content_refs,execution_coverage_hash,execution_covered_at_ms,execution_source_invocation_ids FROM privacy_erasure.work_item_lineage WHERE tenant_id=$1 AND erasure_job_id=$2")
        .bind(scope.tenant()).bind(lineage.erasure_job_id.into_uuid()).fetch_one(&mut *tx).await?;
    let Some(hash) = row.try_get::<Option<Vec<u8>>, _>("execution_coverage_hash")? else {
        return Ok(None);
    };
    if !matches!(row.try_get::<i16, _>("execution_coverage_version")?, 1 | 2)
        || row.try_get::<Vec<uuid::Uuid>, _>("execution_run_ids")? != scope.runs()
    {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let contents = row
        .try_get::<Vec<Vec<u8>>, _>("execution_content_refs")?
        .into_iter()
        .map(|v| {
            crate::dialogue_codec::content_from_bytes(v)
                .map_err(|_| Error::CorruptState("frozen content ref"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let coverage = WorkItemExecutionErasureCoverage {
        scope_fingerprint: lineage.fingerprint(),
        executions: identities(tx, scope).await?,
        contents,
        source_evidence: super::source_evidence::collect(tx, scope).await?,
        frozen_at: UtcTimestamp::from_unix_millis(row.try_get("execution_covered_at_ms")?),
    };
    let ids = coverage
        .source_evidence
        .iter()
        .map(|p| p.invocation_id.into_uuid())
        .collect::<Vec<_>>();
    let frozen_ids = row
        .try_get::<Option<Vec<uuid::Uuid>>, _>("execution_source_invocation_ids")?
        .unwrap_or_default();
    if coverage.version() as i16 != row.try_get::<i16, _>("execution_coverage_version")?
        || ids != frozen_ids
        || coverage.fingerprint()?.into_bytes().as_slice() != hash
    {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(Some(coverage))
}

pub(super) async fn persist(
    tx: &mut sqlx::PgConnection,
    job: &ErasureJob,
    lineage: &WorkItemErasureLineage,
    scope: &WorkItemProviderScope,
    mut contents: Vec<ContentRef>,
    now: UtcTimestamp,
) -> Result<WorkItemExecutionErasureCoverage, Error> {
    contents.sort_by_key(|c| c.content_id);
    for adjacent in contents.windows(2) {
        if adjacent[0].content_id == adjacent[1].content_id && adjacent[0] != adjacent[1] {
            return Err(Error::CorruptState("conflicting closure content identity"));
        }
    }
    contents.dedup();
    let coverage = WorkItemExecutionErasureCoverage {
        scope_fingerprint: lineage.fingerprint(),
        executions: identities(tx, scope).await?,
        contents,
        frozen_at: now,
        source_evidence: super::source_evidence::collect(tx, scope).await?,
    };
    let plan = build_work_item_execution_plan(job, &coverage)?;
    let refs = coverage
        .contents
        .iter()
        .map(|r| encode_content_ref(r).encode_to_vec())
        .collect::<Vec<_>>();
    let changed=sqlx::query("UPDATE privacy_erasure.work_item_lineage SET execution_coverage_version=$7,
        execution_run_ids=$3,execution_content_refs=$4,execution_coverage_hash=$5,execution_covered_at_ms=$6,execution_source_invocation_ids=$8
        WHERE tenant_id=$1 AND erasure_job_id=$2 AND execution_coverage_hash IS NULL")
        .bind(scope.tenant()).bind(lineage.erasure_job_id.into_uuid()).bind(scope.runs()).bind(refs)
        .bind(plan.lineage_fingerprint.into_bytes().to_vec()).bind(now.unix_millis())
        .bind(coverage.version() as i16).bind(coverage.source_evidence.iter().map(|p|p.invocation_id.into_uuid()).collect::<Vec<_>>()).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    sqlx::query("INSERT INTO privacy_erasure.plans(tenant_id,erasure_job_id,plan_version,schema_version,data_class_count,retention_exception_count,lineage_fingerprint,plan_hash,frozen_at_ms) VALUES($1,$2,2,$3,$4,$5,$6,$7,$8)")
        .bind(scope.tenant()).bind(lineage.erasure_job_id.into_uuid()).bind(plan.schema_version.as_str())
        .bind(plan.data_classes.len() as i16).bind(plan.retention_exceptions.len() as i16)
        .bind(plan.lineage_fingerprint.into_bytes().to_vec()).bind(plan.plan_hash.into_bytes().to_vec())
        .bind(now.unix_millis()).execute(&mut *tx).await?;
    for class in &plan.data_classes {
        sqlx::query("INSERT INTO privacy_erasure.plan_data_classes(tenant_id,erasure_job_id,plan_version,data_class) VALUES($1,$2,2,$3)")
            .bind(scope.tenant()).bind(lineage.erasure_job_id.into_uuid()).bind(class.code()).execute(&mut *tx).await?;
    }
    for exception in &plan.retention_exceptions {
        insert_plan_exception(tx, lineage.actor.tenant_id, &plan, exception).await?;
    }
    super::source_evidence::freeze(tx, job, &plan, &coverage).await?;
    Ok(coverage)
}

impl PostgresPrivacyErasureStore {
    pub(crate) async fn work_item_execution_roots(
        &self,
        tenant: TenantId,
        plan: ErasurePlanSnapshot,
    ) -> Result<Vec<musteragent_runtime_contract::ResourceRef>, Error> {
        if plan.schema_version.as_str() != WORK_ITEM_EXECUTION_ERASURE_SCHEMA {
            return Ok(plan.root_resource_refs);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        super::super::recovery::require_execution_claim(
            &mut tx,
            tenant,
            plan.erasure_job_id,
            self.execution_claim,
        )
        .await?;
        let lineage = load_lineage(&mut tx, tenant, plan.erasure_job_id)
            .await?
            .ok_or(Error::WorkItemExecutionClosureRequired)?;
        let scope = super::coverage::provider_scope(&mut tx, &lineage).await?;
        let coverage = load(&mut tx, &lineage, &scope)
            .await?
            .ok_or(Error::WorkItemExecutionClosureRequired)?;
        let job =
            super::super::loading::load_jobs(&mut tx, tenant, &[plan.erasure_job_id.into_uuid()])
                .await?
                .pop()
                .ok_or(Error::JobNotFound)?;
        if job.latest_plan()? != &plan || build_work_item_execution_plan(&job, &coverage)? != plan {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
        tx.commit().await?;
        Ok(coverage
            .contents
            .into_iter()
            .map(musteragent_runtime_contract::ResourceRef::Content)
            .collect())
    }
}
