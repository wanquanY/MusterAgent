//! Typed Task cancellation authorizes only its actual independent Run, then
//! applies the original Runtime cancellation mutation and immutable receipt.
use super::*;
use musteragent_runtime_session::{
    WorkItemCancelRunCommit, WorkItemRunControlFence, WorkItemRunControlReason,
};
use sqlx::PgConnection;
impl PostgresRuntimeApplicationStore {
    pub(super) async fn authorize_work_item_cancel_inner(
        &self,
        fence: WorkItemRunControlFence,
        correlation: CorrelationId,
    ) -> Result<CancelRunAuthority, RuntimeError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(|_| dependency(correlation))?;
        validate(&mut tx, fence, false, correlation).await?;
        tx.commit().await.map_err(|_| dependency(correlation))?;
        cancellation_authority::load(
            &self.pool,
            fence.actor,
            fence.run_id,
            correlation,
            cancellation_authority::Scope::WorkItem,
        )
        .await
    }
    pub(super) async fn cancel_work_item_inner(
        &self,
        correlation: CorrelationId,
        commit: WorkItemCancelRunCommit,
    ) -> Result<CancelRunReceipt, RuntimeError> {
        let b = &commit.batch;
        b.validate().map_err(|_| corrupt(correlation))?;
        if b.run_id != commit.fence.run_id || b.append.tenant_id != commit.fence.actor.tenant_id {
            return Err(not_found(ErrorCode::RunNotFound, correlation));
        }
        // The original append locks the target stream before authority rows, matching
        // concurrent worker publications. Task erasure never holds this stream while
        // waiting on the cancellation worker.
        let mut prepared = match self
            .ledger
            .begin_append(&b.append)
            .await
            .map_err(|e| map_execution(e.into(), correlation))?
        {
            crate::ledger::BeginAppend::Ready(p) => p,
            crate::ledger::BeginAppend::Duplicate(receipt) => {
                let mut tx = self
                    .database
                    .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
                    .await
                    .map_err(|_| dependency(correlation))?;
                validate(&mut tx, commit.fence, true, correlation).await?;
                tx.commit().await.map_err(|_| dependency(correlation))?;
                return Ok(CancelRunReceipt {
                    ledger: receipt,
                    terminal: b.mode == CancelMode::CancelWithoutAttempt,
                });
            }
        };
        validate(prepared.transaction_mut(), commit.fence, true, correlation).await?;
        let now = crate::ledger::database_now_ms(prepared.transaction_mut())
            .await
            .map_err(|e| map_execution(e.into(), correlation))?;
        crate::execution::cascade::apply(prepared.transaction_mut(), b, now)
            .await
            .map_err(|e| map_execution(e, correlation))?;
        let receipt = prepared
            .commit(&b.append)
            .await
            .map_err(|e| map_execution(e.into(), correlation))?;
        Ok(CancelRunReceipt {
            ledger: receipt,
            terminal: b.mode == CancelMode::CancelWithoutAttempt,
        })
    }
}
async fn validate(
    tx: &mut PgConnection,
    f: WorkItemRunControlFence,
    lock: bool,
    c: CorrelationId,
) -> Result<(), RuntimeError> {
    f.validate()
        .map_err(|_| not_found(ErrorCode::RunNotFound, c))?;
    let suffix = if lock { " FOR SHARE OF t" } else { "" };
    // All identifiers originate in the persisted reservation and typed session;
    // the caller's source/coordinator Run can never satisfy this join.
    let query=format!("SELECT revision.control_revision,t.erasure_job_id,t.owner_id FROM collaboration.work_items t
 JOIN collaboration.work_item_revisions revision ON revision.work_item_id=t.work_item_id AND revision.state_revision=t.state_revision
 JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id AND x.tenant_id=t.tenant_id AND x.owner_id=t.owner_id
 JOIN agent.dialogue_sessions s ON s.tenant_id=x.tenant_id AND s.owner_id=x.owner_id AND s.work_item_id=x.work_item_id AND s.subject_kind IN ('work_item','work_item_report') AND s.work_item_goal_revision=x.goal_revision AND s.work_item_control_revision=x.control_revision AND s.work_item_assignment_epoch=x.assignment_epoch
 LEFT JOIN collaboration.work_item_execution_reports report ON report.producing_run_id=s.report_from_run_id
 JOIN runtime.runs r ON r.tenant_id=x.tenant_id AND r.conversation_id=s.conversation_id AND ((s.subject_kind='work_item' AND r.run_id=x.run_id AND s.conversation_id=x.conversation_id) OR (s.subject_kind='work_item_report' AND r.run_id=report.report_run_id AND s.conversation_id=report.report_conversation_id))
 WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND r.run_id=$4 AND r.parent_run_id IS NULL AND r.delegation_id IS NULL AND r.control_home_kind=1 AND r.control_device_id IS NULL AND r.execution_home_kind=1 AND r.execution_device_id IS NULL{suffix}");
    let row = sqlx::query(&query)
        .bind(f.task_id.into_uuid())
        .bind(f.actor.tenant_id.into_uuid())
        .bind(f.actor.principal_id.into_uuid())
        .bind(f.run_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| dependency(c))?
        .ok_or_else(|| not_found(ErrorCode::RunNotFound, c))?;
    let revision: i64 = row.try_get("control_revision").map_err(|_| corrupt(c))?;
    let erasure: Option<uuid::Uuid> = row.try_get("erasure_job_id").map_err(|_| corrupt(c))?;
    let expected = match f.reason {
        WorkItemRunControlReason::TaskCommand { .. } => None,
        WorkItemRunControlReason::PrivacyErasure { job_id } => Some(job_id.into_uuid()),
    };
    if let WorkItemRunControlReason::TaskCommand { command_id } = f.reason {
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_commands c JOIN collaboration.work_item_revisions accepted ON accepted.work_item_id=c.work_item_id AND accepted.state_revision=c.state_revision JOIN collaboration.work_items t ON t.work_item_id=c.work_item_id JOIN collaboration.work_item_revisions current ON current.work_item_id=t.work_item_id AND current.state_revision=t.state_revision WHERE c.tenant_id=$1 AND c.owner_id=$2 AND c.command_id=$3 AND c.work_item_id=$4 AND c.operation IN ('cancel','amend') AND current.goal_revision=accepted.goal_revision AND current.control_revision=accepted.control_revision AND current.status='blocked')")
   .bind(f.actor.tenant_id.into_uuid()).bind(f.actor.principal_id.into_uuid()).bind(command_id.into_uuid()).bind(f.task_id.into_uuid()).fetch_one(&mut *tx).await.map_err(|_|dependency(c))?;
        if !valid {
            return Err(not_found(ErrorCode::RunNotFound, c));
        }
    }
    if revision <= 0 || revision as u64 != f.control_revision.get() || erasure != expected {
        return Err(not_found(ErrorCode::RunNotFound, c));
    }
    Ok(())
}
