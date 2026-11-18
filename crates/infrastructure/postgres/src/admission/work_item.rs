//! WorkItem admission uses its typed, frozen source when projecting authority.
//! A prompt marker or caller-selected policy cannot substitute for this owner.
use super::*;
use musteragent_runtime_session::WorkItemExecutionAdmissionCommit;

impl PostgresAdmissionStore {
    pub(crate) async fn validate_work_item_admission_prefix(
        &self,
        transaction: &mut sqlx::PgConnection,
        commit: &WorkItemExecutionAdmissionCommit,
    ) -> Result<RuntimeSnapshotContent, PostgresAdmissionError> {
        reject_consumed_execution_grant(transaction, &commit.admission.batch).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&commit.admission.batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection_ref(&decoded, RunAdmissionRef::from(commit))?;
        Ok(decoded)
    }

    pub(crate) async fn validate_duplicate_work_item_admission(
        &self,
        commit: &WorkItemExecutionAdmissionCommit,
    ) -> Result<(), PostgresAdmissionError> {
        validate_duplicate_run_snapshot(
            &self.snapshots,
            &self.ledger,
            RunAdmissionRef::from(commit),
        )
        .await?;
        self.content
            .as_ref()
            .ok_or(PostgresAdmissionError::ContentRefConflict)?
            .validate_published_admission_stage(&commit.admission.content_stage)
            .await
            .map_err(|_| PostgresAdmissionError::ContentRefConflict)
    }
}
