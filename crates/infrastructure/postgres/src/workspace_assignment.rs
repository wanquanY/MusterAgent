mod commit;
mod safety;
mod tree;

use musteragent_identity::PrincipalKind;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::workspace_assignment::{
    PrepareWorkspaceAssignment, WorkspaceAssignmentError, WorkspaceAssignmentPhase,
    WorkspaceAssignmentPreparation, WorkspaceAssignmentPreparationStore,
};
use musteragent_telemetry::DbCommitKind;
use sqlx::Row as _;

use crate::PostgresDatabaseRuntime;

#[derive(Clone)]
pub struct PostgresWorkspaceAssignmentPreparationStore {
    database: PostgresDatabaseRuntime,
}

impl PostgresWorkspaceAssignmentPreparationStore {
    #[must_use]
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }

    async fn change(
        &self,
        request: PrepareWorkspaceAssignment,
        cancel: bool,
    ) -> Result<WorkspaceAssignmentPreparation, WorkspaceAssignmentError> {
        if request.actor.principal_kind != PrincipalKind::User {
            return Err(WorkspaceAssignmentError::AuthorityChanged);
        }
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Migration)
            .await
            .map_err(|_| WorkspaceAssignmentError::DependencyUnavailable)?;
        // Serialize command reuse across different Conversations before locking
        // a Conversation, matching admission's idempotency-before-row order.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "workspace-assignment:{}",
                request.command_id.into_uuid()
            ))
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let conversation = sqlx::query(
            "SELECT created_by, workspace_id, kind, control_home_kind, control_device_id,
                    home_epoch::text AS home_epoch
               FROM conversation.conversations
              WHERE tenant_id = $1 AND conversation_id = $2 FOR UPDATE",
        )
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.conversation_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(WorkspaceAssignmentError::AuthorityChanged)?;
        if conversation
            .try_get::<uuid::Uuid, _>("created_by")
            .map_err(corrupt)?
            != request.actor.principal_id.into_uuid()
            || conversation.try_get::<i16, _>("kind").map_err(corrupt)? != 1
            || conversation
                .try_get::<i16, _>("control_home_kind")
                .map_err(corrupt)?
                != 1
            || conversation
                .try_get::<Option<uuid::Uuid>, _>("control_device_id")
                .map_err(corrupt)?
                .is_some()
            || conversation
                .try_get::<String, _>("home_epoch")
                .map_err(corrupt)?
                != request.expected_home_epoch.get().to_string()
        {
            return Err(WorkspaceAssignmentError::AuthorityChanged);
        }
        let current_workspace = conversation
            .try_get::<Option<uuid::Uuid>, _>("workspace_id")
            .map_err(corrupt)?;
        if current_workspace.is_some_and(|id| id != request.target_workspace_id.into_uuid()) {
            return Err(WorkspaceAssignmentError::AuthorityChanged);
        }
        let owns_root: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM resource.workspace_roots
             WHERE tenant_id = $1 AND workspace_id = $2 AND owner_principal_id = $3)",
        )
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.target_workspace_id.into_uuid())
        .bind(request.actor.principal_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if !owns_root {
            return Err(WorkspaceAssignmentError::AuthorityChanged);
        }
        let existing = sqlx::query(
            "SELECT tenant_id, conversation_id, owner_principal_id, target_workspace_id,
                    expected_home_epoch::text AS expected_home_epoch, phase, prepared_at_ms
               FROM conversation.workspace_assignment_preparations WHERE command_id = $1",
        )
        .bind(request.command_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let (mut phase, prepared_at_ms) = if let Some(row) = existing {
            if row.try_get::<uuid::Uuid, _>("tenant_id").map_err(corrupt)?
                != request.actor.tenant_id.into_uuid()
                || row
                    .try_get::<uuid::Uuid, _>("conversation_id")
                    .map_err(corrupt)?
                    != request.conversation_id.into_uuid()
                || row
                    .try_get::<uuid::Uuid, _>("owner_principal_id")
                    .map_err(corrupt)?
                    != request.actor.principal_id.into_uuid()
                || row
                    .try_get::<uuid::Uuid, _>("target_workspace_id")
                    .map_err(corrupt)?
                    != request.target_workspace_id.into_uuid()
                || row
                    .try_get::<String, _>("expected_home_epoch")
                    .map_err(corrupt)?
                    != request.expected_home_epoch.get().to_string()
            {
                return Err(WorkspaceAssignmentError::IdentityConflict);
            }
            (
                row.try_get::<i16, _>("phase").map_err(corrupt)?,
                row.try_get::<i64, _>("prepared_at_ms").map_err(corrupt)?,
            )
        } else {
            if cancel || current_workspace.is_some() {
                return Err(WorkspaceAssignmentError::IdentityConflict);
            }
            // A standing Goal may need further Runs to finish. Do not arm a
            // barrier that would starve its continuation while waiting for it.
            let active_goal: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM runtime.standing_goals WHERE tenant_id=$1 AND conversation_id=$2 AND status IN (1,2,3))"
            ).bind(request.actor.tenant_id.into_uuid()).bind(request.conversation_id.into_uuid())
                .fetch_one(&mut *tx).await.map_err(unavailable)?;
            if active_goal {
                return Err(WorkspaceAssignmentError::WorkPending);
            }
            let pending: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM conversation.workspace_assignment_preparations
                 WHERE tenant_id = $1 AND conversation_id = $2 AND phase = 1)",
            )
            .bind(request.actor.tenant_id.into_uuid())
            .bind(request.conversation_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            if pending {
                return Err(WorkspaceAssignmentError::IdentityConflict);
            }
            let prepared_at_ms: i64 = sqlx::query_scalar(
                "INSERT INTO conversation.workspace_assignment_preparations
                 (command_id, tenant_id, conversation_id, owner_principal_id, target_workspace_id,
                  expected_home_epoch, prepared_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6::text::numeric,
                         floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint)
                 RETURNING prepared_at_ms",
            )
            .bind(request.command_id.into_uuid())
            .bind(request.actor.tenant_id.into_uuid())
            .bind(request.conversation_id.into_uuid())
            .bind(request.actor.principal_id.into_uuid())
            .bind(request.target_workspace_id.into_uuid())
            .bind(request.expected_home_epoch.get().to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            (1, prepared_at_ms)
        };
        if (phase == 3) != current_workspace.is_some() || (cancel && phase == 3) {
            return Err(WorkspaceAssignmentError::IdentityConflict);
        }
        if cancel && phase == 1 {
            sqlx::query("UPDATE conversation.workspace_assignment_preparations SET phase = 2,
                        cancelled_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
                        WHERE command_id = $1 AND phase = 1")
                .bind(request.command_id.into_uuid()).execute(&mut *tx).await.map_err(unavailable)?;
            phase = 2;
        }
        let phase = match phase {
            1 => WorkspaceAssignmentPhase::Preparing,
            2 => WorkspaceAssignmentPhase::Cancelled,
            3 => WorkspaceAssignmentPhase::Committed,
            _ => return Err(WorkspaceAssignmentError::Corrupt),
        };
        tx.commit()
            .await
            .map_err(|_| WorkspaceAssignmentError::DependencyUnavailable)?;
        Ok(WorkspaceAssignmentPreparation {
            request,
            phase,
            prepared_at: musteragent_identity::UtcTimestamp::from_unix_millis(prepared_at_ms),
        })
    }
}

impl WorkspaceAssignmentPreparationStore for PostgresWorkspaceAssignmentPreparationStore {
    fn prepare(
        &self,
        request: PrepareWorkspaceAssignment,
    ) -> ApplicationFuture<'_, Result<WorkspaceAssignmentPreparation, WorkspaceAssignmentError>>
    {
        Box::pin(self.change(request, false))
    }
    fn cancel(
        &self,
        request: PrepareWorkspaceAssignment,
    ) -> ApplicationFuture<'_, Result<WorkspaceAssignmentPreparation, WorkspaceAssignmentError>>
    {
        Box::pin(self.change(request, true))
    }
}

/// The caller must already hold the Conversation row through commit. Keep this
/// as a separate statement: a SELECT waiting for that row's lock may have an
/// older READ COMMITTED snapshot than the newly committed preparation.
pub(crate) async fn admission_is_fenced(
    connection: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: musteragent_identity::ConversationId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM conversation.workspace_assignment_preparations
         WHERE tenant_id = $1 AND conversation_id = $2 AND phase = 1)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(connection)
    .await
}

fn unavailable(_: sqlx::Error) -> WorkspaceAssignmentError {
    WorkspaceAssignmentError::DependencyUnavailable
}
fn corrupt(_: sqlx::Error) -> WorkspaceAssignmentError {
    WorkspaceAssignmentError::Corrupt
}
