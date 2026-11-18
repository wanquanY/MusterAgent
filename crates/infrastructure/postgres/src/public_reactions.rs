//! Current-account feedback uses original command locks and account outbox.
use crate::dialogue_codec::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_persistence_codec::{public_reactions_codec as codec, wire};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_telemetry::DbCommitKind;
use sqlx::{PgConnection, Row};
type Error = DialogueStoreError;

/// Exact public bubble relation, including pending Task privacy fences. Metadata
/// is not authority to recover an erased entry or an internal execution message.
pub(crate) const RELATION: &str = "FROM agent.agent_dialogue_entries e
 JOIN agent.agent_dialogues d USING(dialogue_id,tenant_id,owner_id)
 JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id";
pub(crate) const VISIBLE: &str = "a.status<>'deleted' AND e.kind IN ('user_message','agent_message') AND e.erasure_job_id IS NULL
 AND NOT EXISTS(SELECT 1 FROM collaboration.work_items w WHERE w.tenant_id=e.tenant_id
 AND w.owner_id=e.owner_id AND w.erasure_job_id IS NOT NULL AND (
 EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations x
 LEFT JOIN agent.dialogue_publications p ON p.source_run_id=x.run_id AND p.source_conversation_id=x.conversation_id AND p.tenant_id=x.tenant_id
 WHERE x.work_item_id=w.work_item_id AND (p.entry_id=e.entry_id OR x.partner_input_entry_id=e.entry_id))
 OR EXISTS(SELECT 1 FROM collaboration.work_item_execution_reports x JOIN agent.dialogue_publications p
 ON p.source_run_id=x.report_run_id AND p.source_conversation_id=x.report_conversation_id AND p.tenant_id=x.tenant_id
 WHERE x.work_item_id=w.work_item_id AND p.entry_id=e.entry_id)))";

async fn snapshot(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    lock: bool,
) -> Result<PublicEntryReactions, Error> {
    require_user(actor)?;
    let sql = format!(
        "SELECT d.agent_id {RELATION} WHERE e.entry_id=$1 AND e.dialogue_id=$2 AND e.tenant_id=$3 AND e.owner_id=$4 AND {VISIBLE} {}",
        if lock {
            "FOR UPDATE OF e FOR SHARE OF a"
        } else {
            ""
        }
    );
    let row = sqlx::query(&sql)
        .bind(entry.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::Unavailable)?;
    let state = sqlx::query(
        "SELECT revision,selected_mask FROM agent.public_entry_reactions WHERE entry_id=$1",
    )
    .bind(entry.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let (revision, selected) = if let Some(state) = state {
        let mask: i32 = state.try_get("selected_mask").map_err(db)?;
        (
            unsigned(state.try_get("revision").map_err(db)?)?,
            PublicReactionSet::from_mask(u16::try_from(mask).map_err(|_| Error::Corrupt)?)
                .map_err(|_| Error::Corrupt)?,
        )
    } else {
        (0, PublicReactionSet::EMPTY)
    };
    let result = PublicEntryReactions {
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        dialogue_id: dialogue,
        entry_id: entry,
        revision,
        selected,
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    Ok(result)
}
impl PublicEntryReactionsStore for crate::PostgresAgentDialogueStore {
    fn read_reactions(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
    ) -> ApplicationFuture<'_, Result<PublicEntryReactions, Error>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    DbCommitKind::Projection,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(transaction)?;
            let value = snapshot(&mut tx, actor, dialogue, entry, false).await?;
            tx.commit().await.map_err(transaction)?;
            Ok(value)
        })
    }
    fn set_reaction(
        &self,
        command: SetPublicEntryReaction,
    ) -> ApplicationFuture<'_, Result<PublicEntryReactions, Error>> {
        Box::pin(async move {
            command.validate()?;
            let actor = command.actor;
            let hash = command.fingerprint();
            let mut tx = self
                .database
                .write_unit_of_work(DbCommitKind::Projection)
                .await
                .map_err(transaction)?;
            // Fixed original command -> account -> entry order serializes all devices.
            let receipt = command_receipt(
                &mut tx,
                actor,
                command.command_id,
                "set_entry_reaction",
                hash,
            )
            .await?;
            crate::dialogue_handoff::progress::lock_account(&mut tx, actor).await?;
            let mut current =
                snapshot(&mut tx, actor, command.dialogue_id, command.entry_id, true).await?;
            if let Some(receipt) = receipt {
                let Some(wire::owner_command_receipt::Result::Reactions(receipt)) = receipt.result
                else {
                    return Err(Error::Corrupt);
                };
                if receipt.dialogue_id.as_ref().map(|v| v.value.as_str())
                    != Some(command.dialogue_id.to_string().as_str())
                    || receipt.entry_id.as_ref().map(|v| v.value.as_str())
                        != Some(command.entry_id.to_string().as_str())
                    || receipt.applied_revision > current.revision
                {
                    return Err(Error::Corrupt);
                }
                tx.commit().await.map_err(transaction)?;
                return Ok(current);
            }
            if current.revision != command.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let selected = current
                .selected
                .set(&command.emoji_key, command.present)
                .map_err(|_| Error::RevisionConflict)?;
            let at = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| Error::Infrastructure)?;
            if selected != current.selected {
                current.revision = current
                    .revision
                    .checked_add(1)
                    .filter(|v| *v <= i64::MAX as u64)
                    .ok_or(Error::Corrupt)?;
                current.selected = selected;
                sqlx::query("INSERT INTO agent.public_entry_reactions(entry_id,revision,selected_mask,updated_at_ms) VALUES($1,$2,$3,$4) ON CONFLICT(entry_id) DO UPDATE SET revision=EXCLUDED.revision,selected_mask=EXCLUDED.selected_mask,updated_at_ms=EXCLUDED.updated_at_ms")
     .bind(command.entry_id.into_uuid()).bind(integer(current.revision)?).bind(i32::from(selected.mask())).bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
                outbox(
                    &mut tx,
                    actor,
                    crate::dialogue_codec::AccountOutboxEvent {
                        topic: "public_reactions",
                        aggregate: command.entry_id.into_uuid(),
                        cursor: DialogueCursor {
                            epoch: 1,
                            seq: current.revision,
                        },
                        event: "reactions_changed",
                        payload: wire::AccountChange {
                            event: Some(wire::account_change::Event::ReactionsChanged(
                                codec::encode_changed(&current.changed()),
                            )),
                        },
                        at,
                    },
                )
                .await?;
            }
            save_receipt(
                &mut tx,
                actor,
                command.command_id,
                "set_entry_reaction",
                hash,
                wire::OwnerCommandReceipt {
                    result: Some(wire::owner_command_receipt::Result::Reactions(
                        wire::PublicEntryReactionReceipt {
                            dialogue_id: Some(wire::AgentDialogueId {
                                value: command.dialogue_id.to_string(),
                            }),
                            entry_id: Some(wire::DialogueEntryId {
                                value: command.entry_id.to_string(),
                            }),
                            applied_revision: current.revision,
                        },
                    )),
                },
                at,
            )
            .await?;
            tx.commit().await.map_err(transaction)?;
            Ok(current)
        })
    }
}
