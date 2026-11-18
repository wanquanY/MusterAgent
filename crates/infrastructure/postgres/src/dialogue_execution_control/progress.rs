//! A terminal accepted-input receipt projects its original queue fact. Both
//! Cloud pre-admission and signed Native NotAdmitted use this one publisher.
use super::*;
use musteragent_execution_context::DialogueCursor;
use musteragent_persistence_codec::{dialogue_admission_progress_codec as codec, wire};
pub(crate) async fn cancelled(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    dialogue: AgentDialogueId,
    intent: UserIntentId,
    at: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    // The original terminal trigger requires its exact owner receipt. No caller
    // can publish this merely because a stop request was durably enqueued.
    let entry:uuid::Uuid=sqlx::query_scalar("SELECT i.source_entry_id FROM agent.dialogue_user_intents i JOIN agent.dialogue_intent_admissions q USING(intent_id) JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND d.agent_id=$4 AND d.dialogue_id=$5 AND q.status='cancelled' AND q.terminal_at_ms=$6")
        .bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).bind(dialogue.into_uuid()).bind(at.unix_millis()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Corrupt)?;
    let old=sqlx::query("SELECT stage,blocker,execution_finished FROM agent.dialogue_admission_progress WHERE intent_id=$1 FOR UPDATE").bind(intent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    if let Some(old) = old {
        match old.try_get::<i16, _>("stage").map_err(db)? {
            5 if old.try_get::<i16, _>("blocker").map_err(db)? == 0
                && !old.try_get::<bool, _>("execution_finished").map_err(db)? =>
            {
                return Ok(());
            }
            1 | 2 => (),
            _ => return Err(DialogueStoreError::Corrupt),
        }
    }
    let revision:i64=sqlx::query_scalar("INSERT INTO agent.dialogue_admission_progress(intent_id,revision,stage,blocker,observed_at_ms,execution_finished) VALUES($1,1,5,0,$2,FALSE) ON CONFLICT(intent_id) DO UPDATE SET revision=agent.dialogue_admission_progress.revision+1,stage=5,blocker=0,observed_at_ms=EXCLUDED.observed_at_ms,execution_finished=FALSE RETURNING revision")
        .bind(intent.into_uuid()).bind(at.unix_millis()).fetch_one(&mut *tx).await.map_err(db)?;
    let progress = wire::DialogueAdmissionProgress {
        intent_id: Some(wire::UserIntentId {
            value: intent.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: agent.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: dialogue.to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: entry.to_string(),
        }),
        revision: unsigned(revision)?,
        stage: 5,
        blocker: 0,
        observed_at_ms: at.unix_millis(),
        execution_finished: false,
    };
    codec::decode(progress.clone()).map_err(|_| DialogueStoreError::Corrupt)?;
    crate::dialogue_codec::outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "dialogue_admission",
            aggregate: intent.into_uuid(),
            cursor: DialogueCursor {
                epoch: 1,
                seq: progress.revision,
            },
            event: "admission_progress_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::AdmissionProgressChanged(
                    progress,
                )),
            },
            at,
        },
    )
    .await
}
