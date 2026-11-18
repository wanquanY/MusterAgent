//! Observations share original custody locks. They never settle execution.
use super::*;
use musteragent_execution_context::{DialogueAdmissionBlocker as Blocker, DialogueCursor};
use musteragent_persistence_codec::{dialogue_admission_progress_codec as codec, wire};
use musteragent_runtime_session::{DeviceIntentClaim, OriginDeviceExecutionTarget};

// Serialize the account outbox before taking source/custody locks. In particular,
// never upgrade two concurrent source readers' shared space locks to writers.
pub(crate) async fn lock_account(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
) -> Result<(), DialogueStoreError> {
    let found: Option<uuid::Uuid> = sqlx::query_scalar("SELECT space_id FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent' FOR UPDATE")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(tx).await.map_err(db)?;
    found.ok_or(DialogueStoreError::Unavailable).map(|_| ())
}
/// Project original terminal evidence for at most one bounded page. A different
/// active task on this Agent cannot keep an older message's receipt alive.
pub(crate) async fn finish_receipts(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
) -> Result<bool, DialogueStoreError> {
    let intents = sqlx::query(
        "SELECT p.intent_id,i.source_entry_id,i.dialogue_id FROM agent.dialogue_admission_progress p
         JOIN agent.dialogue_user_intents i USING(intent_id)
         JOIN agent.agent_dialogues d USING(dialogue_id)
         LEFT JOIN agent.device_admissions c USING(intent_id)
         LEFT JOIN agent.native_dialogue_terminals t ON t.run_id=c.run_id
         LEFT JOIN agent.native_execution_activity o ON o.run_id=c.run_id
         WHERE d.agent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3
           AND p.stage=3 AND NOT p.execution_finished
           AND (t.run_id IS NOT NULL OR o.state=4 OR EXISTS(
             SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r
             ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
             WHERE s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id
               AND s.source_intent_id=i.intent_id AND s.purpose='intent_coordination'
               AND r.status BETWEEN 5 AND 8))
         ORDER BY p.intent_id LIMIT 64",
    ).bind(agent.into_uuid()).bind(actor.tenant_id.into_uuid())
     .bind(actor.principal_id.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
    if !intents.is_empty() {
        // Receipt repair writes the public tail. Acquire its original account
        // boundary only when there is actual repair work, before source locks.
        lock_account(tx, actor).await?;
    }
    for row in &intents {
        let id: uuid::Uuid = row.try_get("intent_id").map_err(db)?;
        let at = crate::ledger::database_now_ms(&mut *tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let revision: Option<i64> = sqlx::query_scalar(
            "UPDATE agent.dialogue_admission_progress SET execution_finished=TRUE,revision=revision+1,observed_at_ms=$2
             WHERE intent_id=$1 AND stage=3 AND NOT execution_finished RETURNING revision"
        ).bind(id).bind(at.unix_millis()).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(revision) = revision else {
            continue;
        };
        let progress = wire::DialogueAdmissionProgress {
            intent_id: Some(wire::UserIntentId {
                value: id.to_string(),
            }),
            agent_id: Some(wire::AgentId {
                value: agent.to_string(),
            }),
            dialogue_id: Some(wire::AgentDialogueId {
                value: row
                    .try_get::<uuid::Uuid, _>("dialogue_id")
                    .map_err(db)?
                    .to_string(),
            }),
            entry_id: Some(wire::DialogueEntryId {
                value: row
                    .try_get::<uuid::Uuid, _>("source_entry_id")
                    .map_err(db)?
                    .to_string(),
            }),
            revision: u64::try_from(revision).map_err(|_| DialogueStoreError::Corrupt)?,
            stage: 3,
            blocker: 0,
            observed_at_ms: at.unix_millis(),
            execution_finished: true,
        };
        codec::decode(progress.clone()).map_err(|_| DialogueStoreError::Corrupt)?;
        crate::dialogue_codec::outbox(
            tx,
            actor,
            crate::dialogue_codec::AccountOutboxEvent {
                topic: "dialogue_admission",
                aggregate: id,
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
        .await?;
    }
    Ok(intents.len() == 64)
}
/// Refresh from original custody. Clearing is only called by successful owner
/// transitions; a backend observation can never clear a confirmed source change.
pub(crate) async fn refresh(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    intent: UserIntentId,
    blocker: Option<Blocker>,
    clear: bool,
) -> Result<(), DialogueStoreError> {
    let row=sqlx::query("SELECT i.source_entry_id,i.dialogue_id,d.agent_id,CASE WHEN c.admitted_at_ms IS NOT NULL OR admitted.run_id IS NOT NULL THEN 3 WHEN c.intent_id IS NOT NULL THEN 2 ELSE 1 END AS stage FROM agent.dialogue_user_intents i JOIN agent.dialogue_intent_admissions q USING(intent_id) JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.status<>'deleted' JOIN agent.agent_dialogue_entries e ON e.entry_id=i.source_entry_id AND e.kind='user_message' LEFT JOIN agent.device_admissions c USING(intent_id) LEFT JOIN LATERAL (SELECT r.run_id FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id WHERE s.source_intent_id=i.intent_id AND s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id AND s.purpose='intent_coordination') admitted ON TRUE WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND q.status NOT IN('rejected','failed','cancelled') AND (q.execution_target='origin_device' OR (q.execution_target='cloud' AND admitted.run_id IS NOT NULL))")
        .bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(());
    };
    let stage: i32 = row.try_get("stage").map_err(db)?;
    let old = sqlx::query(
        "SELECT * FROM agent.dialogue_admission_progress WHERE intent_id=$1 FOR UPDATE",
    )
    .bind(intent.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if let Some(old) = &old
        && old.try_get::<i16, _>("stage").map_err(db)? == 4
    {
        // An observation/reconnect cannot revive a committed pre-Run failure.
        return if stage == 3 {
            Err(DialogueStoreError::Corrupt)
        } else {
            Ok(())
        };
    }
    let before = old
        .as_ref()
        .map(|v| v.try_get::<i16, _>("blocker"))
        .transpose()
        .map_err(db)?
        .unwrap_or(0);
    let new = match blocker {
        None => before,
        Some(Blocker::AuthorizationCheckFailed) => 1,
        Some(Blocker::DependencyUnavailable) => 2,
        Some(Blocker::SourceChanged) => 3,
        Some(Blocker::RuntimeContractRejected) => 4,
    };
    let reason = if stage == 3 || clear {
        0
    } else if before == 3 {
        3
    } else {
        new
    };
    if let Some(old) = &old
        && i32::from(old.try_get::<i16, _>("stage").map_err(db)?) == stage
        && before == reason
    {
        return Ok(());
    }
    let revision = old
        .as_ref()
        .map(|v| v.try_get::<i64, _>("revision"))
        .transpose()
        .map_err(db)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(DialogueStoreError::Corrupt)?;
    let at = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    sqlx::query("INSERT INTO agent.dialogue_admission_progress(intent_id,revision,stage,blocker,observed_at_ms) VALUES($1,$2,$3,$4,$5) ON CONFLICT(intent_id) DO UPDATE SET revision=EXCLUDED.revision,stage=EXCLUDED.stage,blocker=EXCLUDED.blocker,observed_at_ms=EXCLUDED.observed_at_ms")
        .bind(intent.into_uuid()).bind(revision).bind(stage as i16).bind(reason).bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    let progress = wire::DialogueAdmissionProgress {
        intent_id: Some(wire::UserIntentId {
            value: intent.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: row
                .try_get::<uuid::Uuid, _>("agent_id")
                .map_err(db)?
                .to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: row
                .try_get::<uuid::Uuid, _>("dialogue_id")
                .map_err(db)?
                .to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: row
                .try_get::<uuid::Uuid, _>("source_entry_id")
                .map_err(db)?
                .to_string(),
        }),
        revision: revision as u64,
        stage,
        blocker: i32::from(reason),
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
                seq: revision as u64,
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
/// The queue remains the sole retry owner. Receipt ACK never needs this lease.
pub(super) async fn defer(
    tx: &mut sqlx::PgConnection,
    requested: DeviceIntentClaim,
    blocker: Blocker,
) -> Result<(), DialogueStoreError> {
    let claim = requested.claim;
    let delay = 5i64
        .saturating_mul(1i64 << claim.attempt_count.min(6))
        .min(300)
        * 1000;
    let reason = match blocker {
        Blocker::SourceChanged => "preparation_changed",
        Blocker::AuthorizationCheckFailed => "policy_denied",
        Blocker::DependencyUnavailable => "dependency_unavailable",
        Blocker::RuntimeContractRejected => "invalid_contract",
    };
    refresh(tx, claim.actor, claim.intent_id, Some(blocker), false).await?;
    sqlx::query("UPDATE agent.dialogue_intent_admissions SET status='pending',claim_owner=NULL,lease_expires_at_ms=NULL,last_error_code=$4,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+$5 WHERE intent_id=$1 AND status='claimed' AND claim_owner=$2 AND claim_generation=$3")
        .bind(claim.intent_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?).bind(reason).bind(delay).execute(tx).await.map_err(db)?;
    Ok(())
}
#[path = "preparation_observation.rs"]
mod preparation_observation;

/// Only the original local failure transaction calls this after committing its
/// canonical failed outcome and terminal queue row, under their custody locks.
/// No execution happened, so execution_finished remains false and stage is not
/// Admitted. The existing failure message owns the explanation.
pub(super) async fn fail_before_admission(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    let row = sqlx::query(
        "SELECT i.source_entry_id,i.dialogue_id,d.agent_id,o.created_at_ms
         FROM agent.dialogue_user_intents i
         JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id
           AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
         JOIN agent.dialogue_intent_admissions q USING(intent_id)
         JOIN agent.dialogue_admission_outcomes o ON o.intent_id=i.intent_id
           AND o.tenant_id=i.tenant_id AND o.owner_id=i.owner_id
           AND o.dialogue_id=i.dialogue_id AND o.agent_id=d.agent_id
         WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3
           AND q.status='failed' AND o.outcome='failed'
           AND q.terminal_at_ms=o.created_at_ms
           AND ((o.reason='runtime_contract_failure' AND o.internal_failure_code='invalid_preparation'
             AND NOT EXISTS(SELECT 1 FROM agent.device_admissions c WHERE c.intent_id=i.intent_id))
             OR (o.reason='source_authority_changed' AND agent.dialogue_source_failure_qualified(i.intent_id,o.facts_sha256)))
           AND NOT EXISTS(SELECT 1 FROM agent.dialogue_sessions s WHERE s.source_intent_id=i.intent_id)",
    ).bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid())
     .bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
     .ok_or(DialogueStoreError::Corrupt)?;
    let old = sqlx::query("SELECT stage,blocker,execution_finished FROM agent.dialogue_admission_progress WHERE intent_id=$1 FOR UPDATE")
        .bind(intent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    if let Some(old) = &old {
        match old.try_get::<i16, _>("stage").map_err(db)? {
            4 if old.try_get::<i16, _>("blocker").map_err(db)? == 0
                && !old.try_get::<bool, _>("execution_finished").map_err(db)? =>
            {
                return Ok(());
            }
            1 | 2 => (),
            _ => return Err(DialogueStoreError::Corrupt),
        }
    }
    let at: i64 = row.try_get("created_at_ms").map_err(db)?;
    let revision: i64 = sqlx::query_scalar(
        "INSERT INTO agent.dialogue_admission_progress(intent_id,revision,stage,blocker,observed_at_ms,execution_finished)
         VALUES($1,1,4,0,$2,FALSE) ON CONFLICT(intent_id) DO UPDATE
         SET revision=agent.dialogue_admission_progress.revision+1,stage=4,blocker=0,
             observed_at_ms=EXCLUDED.observed_at_ms,execution_finished=FALSE RETURNING revision",
    ).bind(intent.into_uuid()).bind(at).fetch_one(&mut *tx).await.map_err(db)?;
    let progress = wire::DialogueAdmissionProgress {
        intent_id: Some(wire::UserIntentId {
            value: intent.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: row
                .try_get::<uuid::Uuid, _>("agent_id")
                .map_err(db)?
                .to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: row
                .try_get::<uuid::Uuid, _>("dialogue_id")
                .map_err(db)?
                .to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: row
                .try_get::<uuid::Uuid, _>("source_entry_id")
                .map_err(db)?
                .to_string(),
        }),
        revision: unsigned(revision)?,
        stage: 4,
        blocker: 0,
        observed_at_ms: at,
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
            at: UtcTimestamp::from_unix_millis(at),
        },
    )
    .await
}

#[cfg(test)]
#[path = "progress/tests.rs"]
mod tests;
