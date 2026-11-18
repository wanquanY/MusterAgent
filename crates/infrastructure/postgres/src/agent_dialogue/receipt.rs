//! Exact input receipt observations on the existing primary writer pool.
use crate::dialogue_codec::*;
use musteragent_identity::*;
use musteragent_persistence_codec::wire;
use musteragent_runtime_session::{
    AcceptDialogueIntent, DialogueIntentReceiptObservation as Observation,
    DialogueStoreError as Error,
};
use prost::Message;
use sqlx::Row;

pub(super) async fn read(
    database: &crate::PostgresDatabaseRuntime,
    actor: Actor,
    dialogue_id: AgentDialogueId,
    command_id: CommandId,
) -> Result<Observation, Error> {
    require_user(actor)?;
    let mut tx = database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Projection)
        .await
        .map_err(crate::dialogue_codec::transaction)?;
    // Same lock and order as accept_inner; never a replica/history-window read.
    lock_dialogue_command(&mut tx, actor, command_id).await?;
    let row = sqlx::query(
        "SELECT d.* FROM agent.agent_dialogues d
         JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
         JOIN agent.owner_spaces s ON s.space_id=a.space_id
         WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3
           AND s.tenant_id=d.tenant_id AND s.owner_id=d.owner_id
           AND s.product='musteragent' AND a.status<>'deleted'",
    )
    .bind(dialogue_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    let dialogue = dialogue_row(&row)?;
    let receipt = sqlx::query(
        "SELECT command_kind,payload_sha256,receipt FROM agent.owner_command_receipts
         WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(command_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let original = sqlx::query(
        "SELECT * FROM agent.dialogue_user_intents
         WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(command_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let Some(receipt) = receipt else {
        if original.is_some() {
            return Err(Error::Corrupt);
        }
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        // Releasing this lock does not reserve or close the command. A waiting
        // original submission may still commit immediately after this read.
        return Ok(Observation::NotObservedAtRead);
    };
    if receipt.try_get::<&str, _>("command_kind").map_err(db)? != "accept_intent" {
        return Err(Error::IdempotencyConflict);
    }
    let bytes: Vec<u8> = receipt.try_get("receipt").map_err(db)?;
    let value = wire::OwnerCommandReceipt::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?;
    let Some(wire::owner_command_receipt::Result::AcceptedIntent(accepted)) = value.result else {
        return Err(Error::Corrupt);
    };
    let wire_intent = required(accepted.intent)?;
    let intent = decode_intent(wire_intent.clone())?;
    // Original storage timestamps are milliseconds. Reject a noncanonical
    // snapshot instead of silently rounding its nanoseconds during decoding.
    if crate::dialogue_codec::intent(&intent) != wire_intent {
        return Err(Error::Corrupt);
    }
    if intent.tenant_id != actor.tenant_id
        || intent.owner_id != actor.principal_id
        || intent.command_id != command_id
    {
        return Err(Error::Corrupt);
    }
    if intent.dialogue_id != dialogue_id {
        return Err(Error::Unavailable);
    }
    if accepted.replayed || intent_row(&original.ok_or(Error::Corrupt)?)? != intent {
        return Err(Error::Corrupt);
    }
    intent
        .accepted_cursor
        .require_at_or_before(dialogue.head)
        .map_err(|_| Error::Corrupt)?;
    let command = AcceptDialogueIntent {
        actor,
        command_id,
        dialogue_id,
        content_ref: intent.content_ref.clone(),
        input_attachments: intent.input_attachments.clone(),
        origin: intent.origin.clone(),
        observed_cursor: intent.observed_cursor,
        reply_to_intent_id: intent.reply_to_intent_id,
        task_target: intent.task_target,
        reply_target: intent.reply_binding.as_ref().map(|b| b.target),
        accepted_at: intent.accepted_at,
    };
    command.validate().map_err(|_| Error::Corrupt)?;
    let stored_hash: Vec<u8> = receipt.try_get("payload_sha256").map_err(db)?;
    let target = super::execution_target::retained(&mut tx, &command)
        .await?
        .ok_or(Error::Corrupt)?;
    target.validate_for(&command).map_err(|_| Error::Corrupt)?;
    if stored_hash
        != super::execution_target::accepted_fingerprint(
            &command,
            &target,
            intent.reply_binding.as_ref(),
        )?
        .into_bytes()
    {
        return Err(Error::Corrupt);
    }
    super::attachments::verify(&mut tx, &intent).await?;
    tx.commit()
        .await
        .map_err(crate::dialogue_codec::transaction)?;
    Ok(Observation::Accepted(intent))
}
