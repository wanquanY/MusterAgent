//! Cloud uses the original atomic Run admission, with a private Coordination
//! context record instead of an invented user Message. Owner-scoped Goal receiver
//! qualification is a separate source contract, never inferred from a UUID.
use super::*;
use musteragent_runtime_session::CoordinationAdmissionCommit;

impl PostgresAdmissionStore {
    pub async fn admit_coordination_content(
        &self,
        commit: CoordinationAdmissionCommit,
    ) -> Result<AppendReceipt, PostgresAdmissionError> {
        commit
            .validate()
            .map_err(|_| PostgresAdmissionError::ContentRefConflict)?;
        let CoordinationAdmissionCommit {
            batch,
            content_stage,
            ..
        } = commit;
        let admission = RunAdmissionRef::from(&batch);
        let content = self
            .content
            .as_ref()
            .ok_or(PostgresAdmissionError::ContentRefConflict)?;
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                validate_duplicate_run_snapshot(&self.snapshots, &self.ledger, admission).await?;
                content
                    .validate_published_admission_stage(&content_stage)
                    .await
                    .map_err(|_| PostgresAdmissionError::ContentRefConflict)?;
                self.audit
                    .write_admission_allowed(batch.actor.tenant_id, batch.run.context.run_id)
                    .await?;
                return Ok(receipt);
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let admitted_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or(PostgresAdmissionError::AuditIdentityMissing)?;
        reject_consumed_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection_ref(&snapshot, admission)?;
        // Dialogue contexts require their own typed source/snapshot authority;
        // the generic private-Conversation path must never opt them in.
        let standard: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2 AND purpose='standard' FOR SHARE)")
            .bind(batch.append.tenant_id.into_uuid()).bind(batch.append.conversation_id.into_uuid())
            .fetch_one(prepared.transaction_mut()).await?;
        if !standard {
            return Err(PostgresAdmissionError::ConversationUnavailable);
        }
        validate_private_user_admission_authority(prepared.transaction_mut(), admission).await?;
        content
            .publish_admission_stage(prepared.transaction_mut(), &content_stage)
            .await
            .map_err(|_| PostgresAdmissionError::ContentRefConflict)?;
        authorize_published_content_ref(
            prepared.transaction_mut(),
            batch.append.tenant_id,
            &batch.payload_ref,
        )
        .await?;
        insert_activity_ref(prepared.transaction_mut(), admission).await?;
        consume_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        insert_execution_grant_routes_ref(prepared.transaction_mut(), admission).await?;
        persist_interactive_run_projection(
            prepared.transaction_mut(),
            admission,
            admitted_sequence.get(),
            &snapshot,
        )
        .await?;
        let mut staged = prepared.stage(&batch.append).await?;
        self.audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                batch.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await?;
        staged.commit().await.map_err(Into::into)
    }
}
