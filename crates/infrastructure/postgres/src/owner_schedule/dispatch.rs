use super::*;
use musteragent_content_ref::MediaType;
use musteragent_execution_context::{DialogueCursor, DialogueInputOrigin, DialogueInputSurface};
use musteragent_identity::{
    AgentDialogueId, CommandId, ContentId, PrincipalId, TenantId, UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

// Project the original admission/Run outcome. No schedule-owned execution status.
const OUTCOME: &str = "CASE
 WHEN o.status<>'accepted' THEN o.status
 WHEN a.outcome IS NOT NULL THEN a.outcome
 WHEN r.status=5 THEN 'completed' WHEN r.status=6 THEN 'failed' WHEN r.status=7 THEN 'cancelled'
 WHEN r.status=8 THEN 'failed' WHEN r.status=3 THEN 'waiting' WHEN r.status IS NOT NULL THEN 'running'
 ELSE 'queued' END";
pub(super) async fn history(
    tx: &mut sqlx::PgConnection,
    id: uuid::Uuid,
) -> Result<Vec<OwnerScheduleOccurrence>, Error> {
    Ok(histories(tx, &[id]).await?.remove(&id).unwrap_or_default())
}
pub(crate) async fn histories(
    tx: &mut sqlx::PgConnection,
    ids: &[uuid::Uuid],
) -> Result<std::collections::HashMap<uuid::Uuid, Vec<OwnerScheduleOccurrence>>, Error> {
    let query=format!("SELECT o.schedule_id,o.occurrence_id,o.scheduled_at_ms,{OUTCOME} AS outcome,
        COALESCE(public.entry_id,i.source_entry_id) AS entry_id,COALESCE(o.error_code,a.reason) AS error_code
        FROM unnest($1::uuid[]) AS selected(id)
        JOIN LATERAL (SELECT * FROM agent.owner_schedule_occurrences WHERE schedule_id=selected.id ORDER BY created_at_ms DESC,occurrence_id DESC LIMIT 10) o ON TRUE
        LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=o.intent_id
        LEFT JOIN agent.dialogue_admission_outcomes a ON a.intent_id=o.intent_id
        LEFT JOIN agent.dialogue_sessions s ON s.source_intent_id=o.intent_id AND s.purpose='intent_coordination'
        LEFT JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id
        LEFT JOIN LATERAL (SELECT entry_id FROM agent.agent_dialogue_entries WHERE source_intent_id=o.intent_id AND kind='agent_message' ORDER BY created_at_ms DESC LIMIT 1) public ON TRUE
        ORDER BY o.created_at_ms DESC,o.occurrence_id DESC");
    let mut result = std::collections::HashMap::<uuid::Uuid, Vec<OwnerScheduleOccurrence>>::new();
    for r in sqlx::query(&query)
        .bind(ids)
        .fetch_all(tx)
        .await
        .map_err(db)?
    {
        result
            .entry(r.try_get("schedule_id").map_err(db)?)
            .or_default()
            .push(OwnerScheduleOccurrence {
                occurrence_id: r.try_get("occurrence_id").map_err(db)?,
                scheduled_at: UtcTimestamp::from_unix_millis(
                    r.try_get("scheduled_at_ms").map_err(db)?,
                ),
                status: r.try_get("outcome").map_err(db)?,
                entry_id: r.try_get("entry_id").map_err(db)?,
                error_code: r.try_get("error_code").map_err(db)?,
            });
    }
    Ok(result)
}
impl PostgresOwnerScheduleStore {
    /// Same process/shutdown boundary as Dialogue admission. DB locks and an
    /// atomic cursor + accepted intent protect against multiple replicas/restarts.
    pub async fn run(&self, content: Arc<dyn ContentStore>, cancel: CancellationToken) {
        loop {
            tokio::select! { biased; ()=cancel.cancelled()=>break, ()=tokio::time::sleep(Duration::from_secs(1))=>{} }
            let now = UtcTimestamp::from_unix_millis(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            );
            let candidates=sqlx::query_scalar::<_,uuid::Uuid>("SELECT s.schedule_id FROM agent.owner_schedules s JOIN agent.owned_agents a USING(agent_id) WHERE s.source_run_id IS NULL AND a.status='active' AND s.retry_after_ms<=$1 AND ((s.status='active' AND s.next_run_at_ms<=$1) OR EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o WHERE o.schedule_id=s.schedule_id AND o.status='pending')) ORDER BY s.updated_at_ms LIMIT 32")
                .bind(now.unix_millis()).fetch_all(self.database.pool()).await;
            match candidates {
                Ok(ids) => {
                    for id in ids {
                        let result = tokio::select! {biased;()=cancel.cancelled()=>return,result=tokio::time::timeout(Duration::from_secs(15),self.dispatch_one(id,now,content.as_ref()))=>result};
                        if !matches!(result, Ok(Ok(_))) {
                            tracing::warn!(schedule_id=%id,"Scheduled input deferred; original cursor retained");
                            let _=sqlx::query("UPDATE agent.owner_schedules SET retry_after_ms=$2 WHERE schedule_id=$1").bind(id).bind(now.unix_millis()+15000).execute(self.database.pool()).await;
                        }
                    }
                }
                Err(error) => {
                    let _ = db(error);
                }
            }
        }
    }
    pub async fn dispatch_one(
        &self,
        id: uuid::Uuid,
        now: UtcTimestamp,
        content: &dyn ContentStore,
    ) -> Result<bool, Error> {
        let mut tx = self.write_unit_of_work().await?;
        // Space precedes schedule/dialogue locks, matching all Dialogue writes.
        let space=sqlx::query("SELECT p.tenant_id,p.owner_id,s.agent_id,a.dialogue_id FROM agent.owner_schedules s JOIN agent.owned_agents a USING(agent_id) JOIN agent.owner_spaces p USING(space_id) WHERE s.source_run_id IS NULL AND s.schedule_id=$1 AND a.status='active' FOR UPDATE OF p SKIP LOCKED")
            .bind(id).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(space) = space else { return Ok(false) };
        let actor = Actor {
            tenant_id: TenantId::from_uuid(space.try_get("tenant_id").map_err(db)?)
                .map_err(|_| Error::Unavailable)?,
            principal_id: PrincipalId::from_uuid(space.try_get("owner_id").map_err(db)?)
                .map_err(|_| Error::Unavailable)?,
            principal_kind: PrincipalKind::User,
        };
        let raw = sqlx::query(
            "SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 FOR UPDATE SKIP LOCKED",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(raw) = raw else { return Ok(false) };
        let item = row(&raw)?;
        if item.status == "deleted" {
            return Ok(false);
        }
        let manual=sqlx::query("SELECT occurrence_id,scheduled_at_ms FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='pending' ORDER BY created_at_ms LIMIT 1 FOR UPDATE")
            .bind(id).fetch_optional(&mut *tx).await.map_err(db)?;
        let is_manual = manual.is_some();
        let (occurrence_id, _due) = if let Some(r) = manual {
            (
                r.try_get::<uuid::Uuid, _>("occurrence_id").map_err(db)?,
                UtcTimestamp::from_unix_millis(r.try_get("scheduled_at_ms").map_err(db)?),
            )
        } else {
            let Some(due) = item.next_run_at.filter(|v| *v <= now) else {
                return Ok(false);
            };
            let occurrence = uuid::Uuid::now_v7();
            let publication = super::release::encode(&super::release::original_or_legacy(
                raw.try_get("product_release").map_err(db)?,
            )?)?;
            sqlx::query("INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,status,created_at_ms,product_release) VALUES($1,$2,$3,$4,'pending',$5,$6)")
                .bind(occurrence).bind(id).bind(item.revision as i64).bind(due.unix_millis()).bind(now.unix_millis()).bind(publication).execute(&mut *tx).await.map_err(db)?;
            (occurrence, due)
        };
        let overlaps: bool = sqlx::query_scalar("SELECT EXISTS(
            SELECT 1 FROM agent.owner_schedule_occurrences o
            LEFT JOIN agent.dialogue_admission_outcomes a ON a.intent_id=o.intent_id
            LEFT JOIN agent.dialogue_sessions s ON s.source_intent_id=o.intent_id AND s.purpose='intent_coordination'
            LEFT JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id
            WHERE o.schedule_id=$1 AND o.status='accepted' AND a.outcome IS NULL AND (r.status IS NULL OR r.status NOT IN (5,6,7,8)))")
            .bind(id).fetch_one(&mut *tx).await.map_err(db)?;
        if overlaps {
            sqlx::query("UPDATE agent.owner_schedule_occurrences SET status='skipped',error_code='previous_execution_active' WHERE occurrence_id=$1").bind(occurrence_id).execute(&mut *tx).await.map_err(db)?;
        } else {
            let text = format!(
                "定时任务：{}\n\n{}",
                item.definition.title, item.definition.instructions
            );
            let content_id =
                ContentId::from_uuid(uuid::Uuid::new_v5(&occurrence_id, b"owner.schedule.input"))
                    .map_err(|_| Error::Unavailable)?;
            let reference = content
                .put_reclaimable_at(
                    content_id,
                    PutContent {
                        tenant_id: actor.tenant_id,
                        media_type: MediaType::new("text/plain;charset=utf-8")
                            .map_err(|_| Error::Invalid)?,
                        bytes: text.into_bytes(),
                    },
                )
                .await
                .map_err(|_| Error::Unavailable)?;
            let dialogue = sqlx::query(
                "SELECT dialogue_id,epoch,head_seq FROM agent.agent_dialogues WHERE agent_id=$1",
            )
            .bind(item.agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            let accepted = crate::agent_dialogue::accept_in_transaction(
                &mut tx,
                AcceptDialogueIntent {
                    reply_target: None,

                    actor,
                    command_id: CommandId::from_uuid(occurrence_id)
                        .map_err(|_| Error::Unavailable)?,
                    dialogue_id: AgentDialogueId::from_uuid(
                        dialogue.try_get("dialogue_id").map_err(db)?,
                    )
                    .map_err(|_| Error::Unavailable)?,
                    content_ref: reference,
                    input_attachments: vec![],
                    origin: DialogueInputOrigin {
                        surface: DialogueInputSurface::Schedule,
                        installation_id: None,
                    },
                    observed_cursor: DialogueCursor {
                        epoch: dialogue.try_get::<i64, _>("epoch").map_err(db)? as u64,
                        seq: dialogue.try_get::<i64, _>("head_seq").map_err(db)? as u64,
                    },
                    reply_to_intent_id: None,
                    task_target: None,
                    accepted_at: now,
                },
            )
            .await
            .map_err(|_| Error::Unavailable)?;
            sqlx::query("UPDATE agent.owner_schedule_occurrences SET status='accepted',intent_id=$2 WHERE occurrence_id=$1").bind(occurrence_id).bind(accepted.intent.intent_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        }
        if !is_manual {
            // Coalesce downtime into at most one execution; never flood the conversation.
            let next = musteragent_goal::next_run_after(
                &item.definition.timing,
                &item.definition.timezone,
                now,
            )
            .map_err(|_| Error::Invalid)?;
            sqlx::query("UPDATE agent.owner_schedules SET next_run_at_ms=$2,status=$3,updated_at_ms=$4 WHERE schedule_id=$1")
                .bind(id).bind(next.map(|v|v.unix_millis())).bind(if next.is_some(){"active"}else{"completed"}).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(transaction)?;
        Ok(true)
    }
}
