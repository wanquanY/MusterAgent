//! WorkItem reservation admission into the original Runtime ledger and Run owner.
use crate::dialogue_codec::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_work_item::*;
use sqlx::{PgConnection, Row};
pub(crate) mod coordination;
mod dependencies;
pub(crate) mod device;
mod device_target;
mod inputs;
mod preparation;
pub use device::{
    PostgresWorkItemDeviceExecutionStore, PostgresWorkItemDeviceLineageSweep,
    WorkItemDeviceLineageSweepReport,
};
pub(crate) use inputs::accepted_source as work_item_accepted_input_source;
mod commit;
mod reports;
pub use reports::PostgresWorkItemReportStore;
mod handoff;
pub use handoff::PostgresWorkItemExecutionHandoffStore;
mod control_queue;
mod followup;
mod input_actions;
mod mutations;
pub(crate) mod reply_action;
pub(crate) mod requester;
pub(crate) mod result_receipts;
pub(crate) mod result_submission;
pub(crate) mod result_verification;
mod start;
mod tool_commit;
mod verification;
mod verification_interaction;
pub use control_queue::PostgresWorkItemControlStore;
pub(crate) mod native;
mod native_source;
mod start_reservation;
mod start_source;
pub use start::PostgresWorkItemStartStore;

#[derive(Clone)]
pub struct PostgresWorkItemExecutionStore {
    admission: crate::PostgresAdmissionStore,
}
impl PostgresWorkItemExecutionStore {
    pub fn new(admission: crate::PostgresAdmissionStore) -> Self {
        Self { admission }
    }
}
impl WorkItemExecutionStore for PostgresWorkItemExecutionStore {
    fn prepare(
        &self,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'_, Result<WorkItemExecutionPreparation, DialogueStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .admission
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Admission,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let value =
                preparation::load(&mut tx, request, false, self.admission.snapshots.as_ref())
                    .await?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(value)
        })
    }
    fn prepare_grant_facts(
        &self,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'_, Result<WorkItemExecutionGrantFacts, DialogueStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .admission
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Admission,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let source =
                preparation::load(&mut tx, request, false, self.admission.snapshots.as_ref())
                    .await?;
            let mut facts = source.grant_fact_binding()?;
            facts.device_workspace_id = device::placement::read(&mut tx, &source).await?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(facts)
        })
    }
    fn lookup(
        &self,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemExecutionReceipt>, DialogueStoreError>> {
        Box::pin(async move {
            let mut connection = self.admission.ledger.pool().acquire().await.map_err(db)?;
            lookup(&mut connection, request).await
        })
    }
    fn admit(
        &self,
        commit: WorkItemExecutionAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<WorkItemExecutionReceipt, DialogueStoreError>> {
        Box::pin(async move { self.commit(commit).await })
    }
}
async fn lookup(
    tx: &mut PgConnection,
    r: WorkItemExecutionRequest,
) -> Result<Option<WorkItemExecutionReceipt>, DialogueStoreError> {
    require_user(r.actor)?;
    let row=sqlx::query("SELECT s.conversation_id,r.run_id FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=s.work_item_id AND x.goal_revision=s.work_item_goal_revision AND x.control_revision=s.work_item_control_revision AND x.assignment_epoch=s.work_item_assignment_epoch LEFT JOIN collaboration.work_item_execution_reports report ON report.producing_run_id=s.report_from_run_id WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.work_item_id=$3 AND s.work_item_goal_revision=$4 AND s.work_item_control_revision=$5 AND s.work_item_assignment_epoch=$6 AND (($7::uuid IS NULL AND s.subject_kind='work_item' AND x.run_id=r.run_id AND x.conversation_id=s.conversation_id) OR ($7::uuid IS NOT NULL AND s.subject_kind='work_item_report' AND s.report_from_run_id=$7 AND report.report_run_id=r.run_id AND report.report_conversation_id=s.conversation_id))")
 .bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.work_item_id.into_uuid()).bind(integer(r.goal_revision.get())?).bind(integer(r.control_revision.get())?).bind(integer(r.assignment_epoch.get())?).bind(r.report_from_run_id.map(RunId::into_uuid)).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    Ok(Some(WorkItemExecutionReceipt {
        request: r,
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        run_id: RunId::from_uuid(row.try_get("run_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    }))
}
fn authority_error(
    error: musteragent_runtime_session::WorkItemAuthorityError,
) -> DialogueStoreError {
    use musteragent_runtime_session::WorkItemAuthorityError::*;
    match error {
        Forbidden | NotFound => DialogueStoreError::Unavailable,
        ErasureFenced => DialogueStoreError::Inactive,
        Unavailable => DialogueStoreError::Infrastructure,
        IdempotencyConflict => DialogueStoreError::IdempotencyConflict,
        Contract(WorkItemContractError::Revision(
            WorkItemRevisionError::GoalConflict
            | WorkItemRevisionError::ControlConflict
            | WorkItemRevisionError::StateConflict,
        )) => DialogueStoreError::RevisionConflict,
        _ => DialogueStoreError::Corrupt,
    }
}

// Preserve safe storage diagnostics without logging SQL, payloads, or credentials.
fn db(error: sqlx::Error) -> DialogueStoreError {
    if let Some(database) = error.as_database_error() {
        tracing::warn!(sqlstate=?database.code(),table=database.table(),constraint=database.constraint(),"Task original-owner persistence rejected a database operation");
    }
    DialogueStoreError::Infrastructure
}
