//! PostgreSQL ownership adapter for the common collaboration WorkItem. No Run
//! is scheduled here; a commit records a durable internal scheduling request.

use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_work_item::*;
use sqlx::Row;

mod codec;
pub(crate) mod commands;
pub(crate) mod queries;

use codec::{db, decode, signed};
type Result<T> = std::result::Result<T, WorkItemAuthorityError>;

#[derive(Clone)]
pub struct PostgresWorkItemAuthority {
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresWorkItemAuthority {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self { database }
    }
}

impl AgentWorkItemAuthority for PostgresWorkItemAuthority {
    fn create(
        &self,
        request: CreateAgentWorkItem,
    ) -> ApplicationFuture<'_, Result<WorkItemAuthorityReceipt>> {
        Box::pin(self.create_inner(request))
    }
    fn amend(
        &self,
        request: AmendAgentWorkItem,
    ) -> ApplicationFuture<'_, Result<WorkItemAuthorityReceipt>> {
        Box::pin(self.amend_inner(request))
    }
    fn inspect(
        &self,
        request: InspectAgentWorkItem,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemAuthorityRecord>>> {
        Box::pin(self.inspect_inner(request))
    }
    fn list(
        &self,
        request: ListAgentWorkItems,
    ) -> ApplicationFuture<'_, Result<WorkItemDirectoryPage>> {
        Box::pin(self.list_inner(request))
    }
}

/// The same Agent row serializes graph mutations, including references read
/// during cycle checks. Read access never acquires this mutation lock.
pub(crate) async fn authorize(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    write: bool,
) -> Result<()> {
    validate_work_item_actor(actor, scope)?;
    // Match the original Dialogue owner's global lock order. Taking Agent
    // first can deadlock with configuration/admission holding space while a
    // new task's FK waits for that same space or Dialogue.
    if write {
        let space = sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(scope.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?;
        if space.is_none() {
            return Err(WorkItemAuthorityError::Forbidden);
        }
        let dialogue = sqlx::query("SELECT dialogue_id FROM agent.agent_dialogues WHERE agent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(scope.agent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?;
        if dialogue.is_none() {
            return Err(WorkItemAuthorityError::Forbidden);
        }
    }
    let lock = if write { " FOR UPDATE OF a" } else { "" };
    let row = sqlx::query(&format!(
        "SELECT a.status FROM agent.owned_agents a JOIN agent.owner_spaces s ON s.space_id=a.space_id \
         WHERE a.agent_id=$1 AND s.space_id=$2 AND s.tenant_id=$3 AND s.owner_id=$4{lock}"))
        .bind(scope.agent_id.into_uuid()).bind(scope.space_id.into_uuid())
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?
        .ok_or(WorkItemAuthorityError::Forbidden)?;
    let status: String = row.try_get("status").map_err(db)?;
    match status.as_str() {
        "active" => Ok(()),
        "archived" if !write => Ok(()),
        _ => Err(WorkItemAuthorityError::Forbidden),
    }
}

pub(crate) const SELECT: &str = "SELECT t.*, r.goal_revision,r.control_revision,r.status,r.assignment_epoch,\
    r.goal_content_id,r.goal_content_ref,r.dependencies,r.priority,r.follow_up_of,\
    r.updated_at_ms,r.state_revision AS selected_revision \
    FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id";

/// Reuse the original immutable metadata/ContentRef validation for bounded
/// internal readers. Callers retain their own exact account/scope predicates.
pub(crate) fn decode_record(row: &sqlx::postgres::PgRow) -> Result<WorkItemAuthorityRecord> {
    decode(row)
}

pub(crate) async fn load(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    id: WorkItemId,
    revision: Option<u64>,
) -> Result<Option<WorkItemAuthorityRecord>> {
    let row = sqlx::query(&format!(
        "{SELECT} WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.space_id=$3 \
        AND t.agent_id=$4 AND t.work_item_id=$5 AND r.state_revision=COALESCE($6,t.state_revision)"
    ))
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(scope.space_id.into_uuid())
    .bind(scope.agent_id.into_uuid())
    .bind(id.into_uuid())
    .bind(revision.map(signed).transpose()?)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    row.as_ref().map(decode).transpose()
}
