use super::*;
use musteragent_runtime_model::{
    SupervisedModelAdmission, SupervisedModelCompletion, SupervisedModelStart,
    SupervisedModelStoreError,
};
type Failure = SupervisedModelStoreError<PostgresModelInvocationError>;

impl PostgresModelInvocationStore {
    #[must_use]
    pub fn with_execution_liveness(
        mut self,
        journal: Arc<crate::PostgresFencedExecutionLivenessJournal>,
    ) -> Self {
        self.liveness = Some(journal);
        self
    }

    pub(super) async fn begin_supervised_inner(
        &self,
        request: SupervisedModelAdmission,
    ) -> Result<SupervisedModelStart, Failure> {
        request.validate().map_err(Failure::Invalid)?;
        let journal = self.liveness.as_ref().ok_or(Failure::Unsupported)?;
        let (mut tx, state) =
            crate::commit_lane::acquire(&self.database, &request.context.attempt_context)
                .await
                .map_err(|error| {
                    Failure::Liveness(crate::execution_liveness::StoreError::from(error).port())
                })?;
        let disposition = self
            .begin_in_connection(
                &mut tx,
                request.admission.invocation.clone(),
                request.admission.parent_invocation_id,
            )
            .await
            .map_err(Failure::Storage)?;
        let record = journal
            .start_auxiliary(tx, &state, &request)
            .await
            .map_err(Failure::Liveness)?;
        Ok(SupervisedModelStart {
            disposition,
            record,
        })
    }

    pub(super) async fn complete_supervised_inner(
        &self,
        request: SupervisedModelCompletion,
    ) -> Result<ModelPersistenceDisposition, Failure> {
        request.validate().map_err(Failure::Invalid)?;
        let journal = self.liveness.as_ref().ok_or(Failure::Unsupported)?;
        let (mut tx, state) =
            crate::commit_lane::acquire(&self.database, &request.context.attempt_context)
                .await
                .map_err(|error| {
                    Failure::Liveness(crate::execution_liveness::StoreError::from(error).port())
                })?;
        let record = journal
            .validate_auxiliary_completion(&mut tx, &state, &request)
            .await
            .map_err(Failure::Liveness)?;
        let parent: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT parent_invocation_id FROM runtime.model_invocations WHERE invocation_id=$1",
        )
        .bind(request.context.invocation_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| Failure::Storage(error.into()))?;
        if parent
            != request
                .context
                .parent_invocation_id
                .map(InvocationId::into_uuid)
        {
            return Err(Failure::Invalid(
                musteragent_runtime_contract::ErrorCode::RuntimeSnapshotInvalid,
            ));
        }
        let disposition = self
            .complete_in_transaction(&mut tx, request.completion, true)
            .await
            .map_err(Failure::Storage)?;
        journal
            .finish_auxiliary(tx, &state, &request.context, record)
            .await
            .map_err(Failure::Liveness)?;
        Ok(disposition)
    }
}
