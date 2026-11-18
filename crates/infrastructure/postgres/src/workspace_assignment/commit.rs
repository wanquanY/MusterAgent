use super::{PostgresWorkspaceAssignmentPreparationStore, corrupt, unavailable};
use crate::PostgresLedgerStore;
use crate::ledger::BeginAppend;
use musteragent_identity::PrincipalKind;
use musteragent_ledger::AppendReceipt;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::workspace_assignment::WorkspaceAssignmentError as Error;
use musteragent_runtime_session::workspace_assignment_commit::{
    WorkspaceAssignmentCommit, WorkspaceAssignmentCommitStore,
};
use sqlx::Row as _;

impl WorkspaceAssignmentCommitStore for PostgresWorkspaceAssignmentPreparationStore {
    fn commit_assignment(
        &self,
        command: WorkspaceAssignmentCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Error>> {
        Box::pin(self.commit_assignment_inner(command))
    }
}
impl PostgresWorkspaceAssignmentPreparationStore {
    async fn commit_assignment_inner(
        &self,
        command: WorkspaceAssignmentCommit,
    ) -> Result<AppendReceipt, Error> {
        let request = command.preparation();
        if request.actor.principal_kind != PrincipalKind::User {
            return Err(Error::AuthorityChanged);
        }
        let ledger = PostgresLedgerStore::new(self.database.clone());
        let mut append = match ledger
            .begin_append(command.append())
            .await
            .map_err(ledger_error)?
        {
            BeginAppend::Duplicate(receipt) => return Ok(receipt),
            BeginAppend::Ready(append) => append,
        };
        let tx = append.transaction_mut();
        // Match admission lock order: Ledger stream, Conversation, resources.
        let row = sqlx::query(
            "SELECT created_by, workspace_id, kind, control_home_kind, title,
                home_epoch::text AS home_epoch FROM conversation.conversations
                WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE",
        )
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.conversation_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(Error::AuthorityChanged)?;
        if row
            .try_get::<uuid::Uuid, _>("created_by")
            .map_err(corrupt)?
            != request.actor.principal_id.into_uuid()
            || row
                .try_get::<Option<uuid::Uuid>, _>("workspace_id")
                .map_err(corrupt)?
                .is_some()
            || row.try_get::<i16, _>("kind").map_err(corrupt)? != 1
            || row
                .try_get::<i16, _>("control_home_kind")
                .map_err(corrupt)?
                != 1
            || row.try_get::<String, _>("home_epoch").map_err(corrupt)?
                != request.expected_home_epoch.get().to_string()
        {
            return Err(Error::AuthorityChanged);
        }
        let valid: bool = sqlx::query_scalar("SELECT EXISTS (
            SELECT 1 FROM conversation.workspace_assignment_preparations p
            JOIN resource.workspace_roots r ON r.workspace_id=p.target_workspace_id AND r.tenant_id=p.tenant_id
            WHERE p.command_id=$1 AND p.tenant_id=$2 AND p.conversation_id=$3
            AND p.owner_principal_id=$4 AND p.target_workspace_id=$5
            AND p.expected_home_epoch=$6::text::numeric AND p.phase=1
            AND r.owner_principal_id=p.owner_principal_id)")
            .bind(request.command_id.into_uuid()).bind(request.actor.tenant_id.into_uuid())
            .bind(request.conversation_id.into_uuid()).bind(request.actor.principal_id.into_uuid())
            .bind(request.target_workspace_id.into_uuid()).bind(request.expected_home_epoch.get().to_string())
            .fetch_one(&mut *tx).await.map_err(unavailable)?;
        if !valid {
            return Err(Error::IdentityConflict);
        }
        super::safety::require_quiescent(tx, &request).await?;
        let title = row.try_get::<Option<String>, _>("title").map_err(corrupt)?;
        super::tree::transfer(
            tx,
            &request,
            title.as_deref(),
            command.append().expected_offset.get() + 1,
        )
        .await?;
        sqlx::query("UPDATE conversation.workspace_assignment_preparations SET phase=3 WHERE command_id=$1 AND phase=1")
            .bind(request.command_id.into_uuid()).execute(&mut *tx).await.map_err(unavailable)?;
        let mut staged = append.stage(command.append()).await.map_err(ledger_error)?;
        let record = &command.append().records[0];
        let committed_at: i64 = sqlx::query_scalar(
            "SELECT committed_at_ms FROM event_store.ledger_records WHERE record_id=$1",
        )
        .bind(record.record_id.into_uuid())
        .fetch_one(staged.transaction_mut())
        .await
        .map_err(unavailable)?;
        let facts = musteragent_projection::adapt_conversation_event(
            musteragent_projection::CatalogEventContext {
                control_home: musteragent_event_envelope::RuntimeHome::Cloud,
                home_epoch: request.expected_home_epoch,
                tenant_id: request.actor.tenant_id,
                conversation_id: request.conversation_id,
                record_id: record.record_id,
                audience: musteragent_projection::CatalogAttentionAudience::AllParticipants,
                occurred_at: record.occurred_at,
                committed_at: musteragent_identity::UtcTimestamp::from_unix_millis(committed_at),
            },
            &musteragent_conversation::ConversationEvent::WorkspaceAssigned {
                workspace_id: request.target_workspace_id,
                assignment_command_id: request.command_id,
            },
        )
        .map_err(|_| Error::Corrupt)?;
        for mut fact in facts {
            fact.source_record_id = Some(record.record_id);
            crate::conversation_catalog::fact::apply_fact_on(staged.transaction_mut(), fact)
                .await
                .map_err(|_| Error::DependencyUnavailable)?;
        }
        // Catalog revision and its invalidation notification commit with the
        // tree, assignment fact and receipt, without advancing user activity.
        staged.commit().await.map_err(ledger_error)
    }
}
fn ledger_error(error: crate::PostgresLedgerError) -> Error {
    match error {
        crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::SequenceConflict { .. },
        ) => Error::CursorConflict,
        crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::IdempotencyConflict,
        ) => Error::IdentityConflict,
        crate::PostgresLedgerError::Contract(_) => Error::AuthorityChanged,
        _ => Error::DependencyUnavailable,
    }
}
