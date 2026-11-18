//! Capture authorization is read from existing owners in one snapshot. This
//! neither advances commands nor creates an alternate editable grant store.
use super::*;

impl PostgresOwnerEventSourceStore {
    pub(super) async fn read_control_receipt_inner(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> Result<Option<OwnerEventSubscription>, Error> {
        self.read_receipt_inner(actor, command, CONTROL)
            .await?
            .map(|receipt| {
                let Some(wire::owner_command_receipt::Result::EventSubscription(source)) =
                    receipt.result
                else {
                    return Err(Error::Corrupt);
                };
                let source =
                    codec::decode_subscription(Some(source)).map_err(|_| Error::Corrupt)?;
                if source.subject.tenant_id != actor.tenant_id
                    || source.subject.owner_id != actor.principal_id
                {
                    return Err(Error::Corrupt);
                }
                Ok(source)
            })
            .transpose()
    }

    pub(super) async fn read_receipt_inner(
        &self,
        actor: Actor,
        command: CommandId,
        kind: &str,
    ) -> Result<Option<wire::OwnerCommandReceipt>, Error> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        crate::dialogue_codec::require_user(actor).map_err(dialogue_store_error)?;
        super::authority::require_account(&mut tx, actor.principal_id.into_uuid()).await?;
        let row = sqlx::query("SELECT command_kind,receipt FROM agent.owner_command_receipts WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
            .bind(command.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let result = if let Some(row) = row {
            if row.try_get::<&str, _>("command_kind").map_err(db)? != kind {
                return Err(Error::IdempotencyConflict);
            }
            let receipt = wire::OwnerCommandReceipt::decode(
                row.try_get::<Vec<u8>, _>("receipt").map_err(db)?.as_slice(),
            )
            .map_err(|_| Error::Corrupt)?;
            Some(receipt)
        } else {
            None
        };
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(result)
    }

    pub(super) async fn resolve_inner(
        &self,
        actor: Actor,
        agent: AgentId,
        goal: GoalId,
        subscription: OwnerEventSubscriptionId,
    ) -> Result<OwnerEventSourceResolution, Error> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        super::authority::scope(&mut tx, actor, agent, false, false).await?;
        let goal = record::goal(&mut tx, actor, agent, goal).await?;
        let subscription = record::list(&mut tx, &goal)
            .await?
            .into_iter()
            .find(|source| source.subscription_id == subscription)
            .ok_or(Error::NotFound)?;
        let capture_requested = record::capture_requested(&subscription, &goal);
        let application = if capture_requested {
            let binding = record::application(
                &mut tx,
                subscription.subject,
                subscription.configuration.application_binding_id,
                true,
            )
            .await?;
            subscription.authorize(&binding)?;
            Some(binding)
        } else {
            None
        };
        let cursor = record::account_cursor(&mut tx, actor).await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(OwnerEventSourceResolution {
            subscription,
            goal_revision: goal.revision,
            capture_requested,
            cursor,
            application,
        })
    }
}
