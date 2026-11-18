//! Acknowledgement and private Goal identity share the original creation transaction.
use super::*;
use musteragent_persistence_codec::{owner_goal_creation_codec as codec, wire};
use prost::Message;

const KIND: &str = "owner.goal.create.v1";

fn decode(
    retained: wire::OwnerCommandReceipt,
    actor: Actor,
    command: CommandId,
) -> Result<OwnerGoalDraftReceipt, DialogueStoreError> {
    let Some(wire::owner_command_receipt::Result::GoalDraft(value)) = retained.result else {
        return Err(DialogueStoreError::Corrupt);
    };
    let receipt = codec::decode(value)?;
    if receipt.binding.scope.actor() != actor || receipt.command_id != command {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(receipt)
}

pub(super) async fn read(
    store: &PostgresStandingGoalProductStore,
    actor: Actor,
    command: CommandId,
) -> Result<Option<OwnerGoalDraftReceipt>, DialogueStoreError> {
    crate::dialogue_codec::require_user(actor)?;
    let mut tx = store
        .database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Projection,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    if !account_available(&mut tx, actor.principal_id)
        .await
        .map_err(storage)?
    {
        return Err(DialogueStoreError::Unavailable);
    }
    let row: Option<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT command_kind,receipt FROM agent.owner_command_receipts WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3",
    ).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(command.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let result = row
        .map(|(kind, bytes)| {
            if kind != KIND {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            decode(
                wire::OwnerCommandReceipt::decode(bytes.as_slice())
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                actor,
                command,
            )
        })
        .transpose()?;
    tx.commit()
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    Ok(result)
}

pub(super) async fn create(
    store: &PostgresStandingGoalProductStore,
    request: CreateOwnerGoalDraft,
) -> Result<OwnerGoalDraftReceipt, DialogueStoreError> {
    let mut tx = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    let receipt = create_in_transaction(&mut tx, request).await?;
    tx.commit()
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    Ok(receipt)
}

/// Shared by the account command and the original fenced Tool transaction.
/// The caller owns commit; this function never acknowledges a partial write.
pub(crate) async fn create_in_transaction(
    tx: &mut sqlx::PgConnection,
    request: CreateOwnerGoalDraft,
) -> Result<OwnerGoalDraftReceipt, DialogueStoreError> {
    let fingerprint = codec::fingerprint(&request)?;
    // Original order: command -> account -> Goal/Agent. The same command may
    // be replayed after Goal edits or deletion, but never after account erasure.
    if !account_available(tx, request.actor.principal_id)
        .await
        .map_err(storage)?
    {
        return Err(DialogueStoreError::Unavailable);
    }
    let replay = crate::dialogue_codec::command_receipt(
        tx,
        request.actor,
        request.command_id,
        KIND,
        fingerprint,
    )
    .await?;
    crate::dialogue_handoff::progress::lock_account(tx, request.actor).await?;
    if !account_available(tx, request.actor.principal_id)
        .await
        .map_err(storage)?
    {
        return Err(DialogueStoreError::Unavailable);
    }
    if let Some(value) = replay {
        let receipt = decode(value, request.actor, request.command_id)?;
        if receipt.binding != request.binding {
            return Err(DialogueStoreError::Corrupt);
        }
        return Ok(receipt);
    }
    let binding = &request.binding;
    let scope = binding.scope;
    if load_goal_tx(tx, scope.tenant_id, scope.goal_id)
        .await
        .map_err(storage)?
        .is_some()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let deleted: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM runtime.goal_deletions WHERE tenant_id=$1 AND goal_id=$2)",
    )
    .bind(scope.tenant_id.into_uuid())
    .bind(scope.goal_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    if deleted {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let current =
        crate::dialogue_execution::agent::load(tx, request.actor, scope.dialogue_id, true).await?;
    if current.agent.status != OwnedAgentStatus::Active
        || current.agent.agent_id != scope.agent_id
        || current.agent.space_id != scope.space_id
    {
        return Err(DialogueStoreError::Unavailable);
    }
    let requester =
        crate::dialogue_execution::agent::load(tx, request.actor, binding.source_dialogue_id, true)
            .await?;
    if requester.agent.status != OwnedAgentStatus::Active
        || requester.agent.agent_id != binding.requesting_agent_id
        || requester.agent.space_id != scope.space_id
    {
        return Err(DialogueStoreError::Unavailable);
    }
    insert_context(
        tx,
        &request,
        &current.configuration.profile.display_name,
        &requester.configuration.profile.display_name,
    )
    .await?;
    if insert_goal(tx, &request.goal).await.map_err(storage)? != 1 {
        return Err(DialogueStoreError::RevisionConflict);
    }
    // The content owner must confirm the exact staged reference, not just its ID.
    if load_goal_tx(tx, scope.tenant_id, scope.goal_id)
        .await
        .map_err(storage)?
        .as_ref()
        != Some(&request.goal)
    {
        return Err(DialogueStoreError::Corrupt);
    }
    insert_session(tx, &request).await?;
    let receipt = OwnerGoalDraftReceipt {
        command_id: request.command_id,
        binding: request.binding.clone(),
        created_at: request.goal.created_at,
    };
    crate::dialogue_codec::save_receipt(
        tx,
        request.actor,
        request.command_id,
        KIND,
        fingerprint,
        wire::OwnerCommandReceipt {
            result: Some(wire::owner_command_receipt::Result::GoalDraft(
                codec::receipt(&receipt),
            )),
        },
        receipt.created_at,
    )
    .await?;
    Ok(receipt)
}
