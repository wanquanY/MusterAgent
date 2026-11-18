//! Source facts and Goal input commit in the existing account/Goal transaction.
mod authority;
mod control;
mod delivery;
mod goal_control;
mod record;
mod resolution;
pub(crate) mod setup_delivery;
pub(crate) mod tool_control;
pub(crate) mod tool_read;
pub(crate) use goal_control::{
    goal_changed, goal_deleting, lock_account_before_goal, required_sources_ready,
};

use musteragent_content_ref::Sha256;
use musteragent_goal::{StandingGoal, StandingGoalMode, StandingGoalStatus};
use musteragent_goal::{event_subscription::*, source_binding::*};
use musteragent_identity::*;
use musteragent_persistence_codec::{owner_event_sources_codec as codec, wire};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{GoalSourceCommandIdentity, owner_event_sources::*};
use prost::Message;
use sqlx::{PgConnection, Row};

type Error = EventSourceStoreError;
const CONTROL: &str = "owner.event_source.control.v1";
const OBSERVE: &str = "owner.event_source.observe.v1";
const DELIVER: &str = "owner.event_source.deliver.v1";
pub const SOURCE_DESIRED_TOPIC: &str = "owner.event_source.desired.v1";

#[derive(Clone)]
pub struct PostgresOwnerEventSourceStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresOwnerEventSourceStore {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
    async fn write(&self) -> Result<crate::GovernedPostgresTransaction<'static>, Error> {
        self.database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(|_| Error::Infrastructure)
    }
}

impl OwnerEventSourceStore for PostgresOwnerEventSourceStore {
    fn read_delivery_receipt(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<OwnerSourceInputReceipt>, Error>> {
        Box::pin(async move {
            self.read_receipt_inner(actor, command, DELIVER)
                .await?
                .map(|receipt| {
                    let Some(wire::owner_command_receipt::Result::SourceInput(value)) =
                        receipt.result
                    else {
                        return Err(Error::Corrupt);
                    };
                    codec::decode_input_receipt(value).map_err(|_| Error::Corrupt)
                })
                .transpose()
        })
    }
    fn read_control_receipt(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<OwnerEventSubscription>, Error>> {
        Box::pin(self.read_control_receipt_inner(actor, command))
    }
    fn resolve(
        &self,
        actor: Actor,
        agent: AgentId,
        goal: GoalId,
        subscription: OwnerEventSubscriptionId,
    ) -> ApplicationFuture<'_, Result<OwnerEventSourceResolution, Error>> {
        Box::pin(self.resolve_inner(actor, agent, goal, subscription))
    }
    fn control(
        &self,
        request: ControlOwnerEventSubscription,
    ) -> ApplicationFuture<'_, Result<OwnerEventSubscription, Error>> {
        Box::pin(self.control_inner(request))
    }
    fn observe(
        &self,
        request: ObserveOwnerEventSource,
    ) -> ApplicationFuture<'_, Result<OwnerEventSubscription, Error>> {
        Box::pin(self.observe_inner(request))
    }
    fn deliver(
        &self,
        request: DeliverDialogueSourceEvent,
    ) -> ApplicationFuture<'_, Result<OwnerSourceInputReceipt, Error>> {
        Box::pin(self.deliver_inner(request))
    }
    fn list(
        &self,
        actor: Actor,
        agent: AgentId,
        goal: GoalId,
    ) -> ApplicationFuture<'_, Result<Vec<OwnerEventSubscription>, Error>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Projection,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| Error::Infrastructure)?;
            crate::dialogue_codec::require_user(actor).map_err(dialogue_store_error)?;
            authority::require_account(&mut tx, actor.principal_id.into_uuid()).await?;
            let goal = record::goal(&mut tx, actor, agent, goal).await?;
            let result = record::list(&mut tx, &goal).await?;
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
            Ok(result)
        })
    }
}

fn db(error: sqlx::Error) -> Error {
    // Provider selection and event content may be present in database DETAIL;
    // report only the bounded structural failure, never those private values.
    let details = error.as_database_error();
    tracing::warn!(sqlstate = ?details.and_then(|e| e.code()), constraint = ?details.and_then(|e| e.constraint()), "event source persistence failed");
    Error::Infrastructure
}
fn dialogue_store_error(error: musteragent_runtime_session::DialogueStoreError) -> Error {
    use musteragent_runtime_session::DialogueStoreError as E;
    match error {
        E::IdempotencyConflict => Error::IdempotencyConflict,
        E::RevisionConflict => Error::Conflict,
        E::Inactive => Error::Forbidden,
        E::PermissionReduced | E::Unavailable => Error::NotFound,
        E::Corrupt => Error::Corrupt,
        E::Infrastructure => Error::Infrastructure,
    }
}

fn fingerprint(
    identity: &GoalSourceCommandIdentity,
    goal: GoalId,
    source: OwnerEventSubscriptionId,
    fields: &[Vec<u8>],
) -> Sha256 {
    let mut data = b"musteragent.owner-event-command.v1\0".to_vec();
    for part in [
        identity.actor.tenant_id.to_string(),
        identity.actor.principal_id.to_string(),
        identity.agent_id.to_string(),
        goal.to_string(),
        source.to_string(),
    ]
    .iter()
    .map(|p| p.as_bytes())
    .chain(fields.iter().map(Vec::as_slice))
    {
        data.extend_from_slice(&(part.len() as u64).to_be_bytes());
        data.extend_from_slice(part);
    }
    Sha256::digest(data)
}

async fn replay(
    tx: &mut PgConnection,
    identity: &GoalSourceCommandIdentity,
    kind: &str,
    hash: Sha256,
) -> Result<Option<wire::owner_command_receipt::Result>, Error> {
    identity.validate()?;
    authority::require_account(tx, identity.actor.principal_id.into_uuid()).await?;
    crate::dialogue_codec::command_receipt(tx, identity.actor, identity.command_id, kind, hash)
        .await
        .map_err(dialogue_store_error)?
        .map(|r| r.result.ok_or(Error::Corrupt))
        .transpose()
}

async fn save_receipt(
    tx: &mut PgConnection,
    identity: &GoalSourceCommandIdentity,
    kind: &str,
    hash: Sha256,
    result: wire::owner_command_receipt::Result,
) -> Result<(), Error> {
    crate::dialogue_codec::save_receipt(
        tx,
        identity.actor,
        identity.command_id,
        kind,
        hash,
        wire::OwnerCommandReceipt {
            result: Some(result),
        },
        identity.changed_at,
    )
    .await
    .map_err(dialogue_store_error)
}

fn subscription_receipt(
    value: wire::owner_command_receipt::Result,
    identity: &GoalSourceCommandIdentity,
    goal: GoalId,
    id: OwnerEventSubscriptionId,
) -> Result<OwnerEventSubscription, Error> {
    let wire::owner_command_receipt::Result::EventSubscription(value) = value else {
        return Err(Error::Corrupt);
    };
    let value = codec::decode_subscription(Some(value)).map_err(|_| Error::Corrupt)?;
    if value.subject != identity.subject() || value.goal_id != goal || value.subscription_id != id {
        return Err(Error::Corrupt);
    }
    Ok(value)
}
