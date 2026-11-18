//! Invalidate the root view through the original durable account tail. Callers
//! use the existing activity projection and account-first lock. No copied task state.
use musteragent_identity::*;
use musteragent_persistence_codec::wire;
use musteragent_runtime_session::DialogueStoreError;
use sqlx::Row;

pub(crate) async fn publish(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    now: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    let account = sqlx::query(
        "SELECT s.sync_epoch,s.head_event_seq FROM agent.owner_spaces s
        JOIN agent.owned_agents a USING(space_id)
        WHERE s.tenant_id=$1 AND s.owner_id=$2 AND a.agent_id=$3 FOR UPDATE OF s",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(agent.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let epoch = u64::try_from(account.try_get::<i64, _>("sync_epoch").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let seq = u64::try_from(account.try_get::<i64, _>("head_event_seq").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?
        .checked_add(1)
        .ok_or(DialogueStoreError::Corrupt)?;
    crate::dialogue_codec::outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "agent_work",
            aggregate: agent.into_uuid(),
            cursor: musteragent_execution_context::DialogueCursor { epoch, seq },
            event: "work_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::WorkChanged(
                    wire::AgentWorkChanged {
                        reference: None,
                        agent_id: Some(wire::AgentId {
                            value: agent.to_string(),
                        }),
                    },
                )),
            },
            at: now,
        },
    )
    .await
}

fn db(_: sqlx::Error) -> DialogueStoreError {
    DialogueStoreError::Infrastructure
}
