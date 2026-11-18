//! The same admission owner as foreground/coordination, inside the parent Tool UoW.
use super::*;
impl PostgresAdmissionStore {
    pub(crate) async fn stage_cloud_delegation(
        &self,
        tx: crate::GovernedPostgresTransaction<'static>,
        batch: &musteragent_ledger::DelegationAdmissionBatch,
        source: &RuntimeSnapshotContent,
    ) -> Result<crate::ledger::StagedLedgerAppend, PostgresAdmissionError> {
        batch.validate().map_err(PostgresAdmissionError::Contract)?;
        let admission = RunAdmissionRef {
            derived_goal: source.owner_goal_authority.is_some(),
            ..RunAdmissionRef::from(batch)
        };
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(tx, &batch.append)
            .await?
        {
            BeginAppend::Ready(p) => p,
            BeginAppend::Duplicate(_) => {
                return Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch);
            }
        };
        let sequence = prepared.sequences()[1].get();
        reject_consumed_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        if source.owner_goal_authority != snapshot.owner_goal_authority {
            return Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch);
        }
        validate_snapshot_grant_projection_ref(&snapshot, admission)?;
        authorize_published_content_ref(
            prepared.transaction_mut(),
            batch.actor.tenant_id,
            &batch.objective_ref,
        )
        .await?;
        sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,agent_ref,display_name,joined_at_ms) VALUES($1,$2,$3,2,$4,$5,$6)")
            .bind(batch.executor_participant.participant_id.into_uuid()).bind(batch.actor.tenant_id.into_uuid())
            .bind(batch.append.conversation_id.into_uuid()).bind(batch.executor_participant.agent_ref.map(|a| a.into_uuid()))
            .bind(batch.executor_participant.display_snapshot.display_name.as_str()).bind(batch.run.admitted_at.unix_millis())
            .execute(prepared.transaction_mut()).await?;
        insert_activity_ref(prepared.transaction_mut(), admission).await?;
        consume_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        insert_execution_grant_routes_ref(prepared.transaction_mut(), admission).await?;
        persist_interactive_run_projection(
            prepared.transaction_mut(),
            admission,
            sequence,
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
        Ok(staged)
    }
}

impl PostgresAdmissionStore {
    pub(crate) async fn validate_derived_admission_prefix(
        &self,
        transaction: &mut sqlx::PgConnection,
        batch: &AdmissionBatch,
        source: &RuntimeSnapshotContent,
    ) -> Result<RuntimeSnapshotContent, PostgresAdmissionError> {
        reject_consumed_execution_grant(transaction, batch).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        if decoded.owner_goal_authority != source.owner_goal_authority
            || decoded.owner_goal_authority.as_ref().is_some_and(|a| {
                a.context_cut.tenant_id != batch.actor.tenant_id
                    || a.context_cut.owner_id != batch.actor.principal_id
                    || a.context_cut.agent_id != decoded.agent.agent_id
            })
        {
            return Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch);
        }
        let admission = RunAdmissionRef {
            derived_goal: source.owner_goal_authority.is_some(),
            ..RunAdmissionRef::from(batch)
        };
        validate_snapshot_grant_projection_ref(&decoded, admission)?;
        Ok(decoded)
    }
}
