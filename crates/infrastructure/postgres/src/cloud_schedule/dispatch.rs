//! Occurrence ownership is a database transaction, not a process lease. Once
//! admitted, the original P9 Run scheduler owns Attempt recovery.
use super::*;
use musteragent_content_ref::MediaType;
use musteragent_runtime_content::{ContentStore, PutContent};
use std::sync::Arc;

#[derive(Clone)]
pub struct PostgresScheduleDispatcher {
    database: PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    codec: Arc<dyn musteragent_ledger::LedgerPayloadEncoder>,
}
impl PostgresScheduleDispatcher {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        codec: Arc<dyn musteragent_ledger::LedgerPayloadEncoder>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
            codec,
        }
    }

    /// Advance bounded work, then publish complete terminal projections. Hosts
    /// subscribe to the durable outbox; a worker crash between ticks is harmless.
    pub async fn tick(&self, now: UtcTimestamp) -> Result<(), Error> {
        self.deliver(now).await?;
        let mut first_error = None;
        for (id, epoch) in self.due(now, 32).await? {
            match self.fire(id, ScheduleTriggerOwner::Cloud, epoch, now).await {
                Ok(_) | Err(Error::StaleOwner) => {}
                Err(error) => {
                    self.defer(id, epoch, now, error).await?;
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// A broken source/dependency must not starve the next schedule. The due
    /// cursor remains intact and the owner receives the exact typed reason.
    /// Admission retries use a bounded scan delay; admitted Runs use RetrySpec.
    async fn defer(
        &self,
        schedule: uuid::Uuid,
        epoch: u64,
        now: UtcTimestamp,
        error: Error,
    ) -> Result<(), Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let retry_at = now
            .unix_millis()
            .checked_add(30_000)
            .ok_or(Error::Invalid)?;
        let row = sqlx::query("UPDATE agent.owner_schedules s SET next_dispatch_after_ms=$3 WHERE schedule_id=$1 AND trigger_epoch=$2 AND status IN ('active','completed') AND trigger_owner=$4 AND (next_dispatch_after_ms IS NULL OR next_dispatch_after_ms<=$5) AND (next_run_at_ms<=$5 OR EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o WHERE o.schedule_id=s.schedule_id AND o.status='pending')) RETURNING tenant_id,owner_conversation_id")
            .bind(schedule).bind(integer(epoch)?).bind(retry_at).bind(serde_json::to_value(ScheduleTriggerOwner::Cloud).map_err(|_|Error::Corrupt)?).bind(now.unix_millis()).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(row) = row {
            let payload = serde_json::to_vec(&serde_json::json!({"schedule_id":schedule,"owner_epoch":epoch,"reason":format!("{error:?}"),"retry_at_ms":retry_at})).map_err(|_|Error::Corrupt)?;
            event(
                &mut tx,
                TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                ConversationId::from_uuid(row.try_get("owner_conversation_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                schedule,
                uuid::Uuid::new_v5(
                    &schedule,
                    format!("defer:{epoch}:{}", now.unix_millis()).as_bytes(),
                ),
                "schedule.dispatch.deferred",
                &payload,
                now,
            )
            .await?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// A terminal occurrence retains every assistant result reference and its
    /// exact session/Run identity. Delivery is atomic with the terminal cursor.
    pub async fn deliver(&self, now: UtcTimestamp) -> Result<u64, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let rows=sqlx::query("SELECT o.occurrence_id,o.schedule_id,o.run_id,o.conversation_id,s.tenant_id,s.owner_conversation_id,r.status,r.terminal_error FROM agent.owner_schedule_occurrences o JOIN agent.owner_schedules s USING(schedule_id) JOIN runtime.runs r ON r.run_id=o.run_id WHERE o.status='running' AND r.status IN (5,6,7,8) ORDER BY r.finished_at_ms,o.occurrence_id LIMIT 32 FOR UPDATE OF o SKIP LOCKED").fetch_all(&mut *tx).await.map_err(db)?;
        let count = rows.len() as u64;
        for row in rows {
            let occurrence: uuid::Uuid = row.try_get("occurrence_id").map_err(db)?;
            let run: uuid::Uuid = row.try_get("run_id").map_err(db)?;
            let status = match row.try_get::<i16, _>("status").map_err(db)? {
                5 => "completed",
                6 => "failed",
                7 | 8 => "interrupted",
                _ => return Err(Error::Corrupt),
            };
            let results:serde_json::Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(jsonb_build_object('message_id',m.message_id,'content_id',c.content_id,'sha256',encode(c.content_hash,'hex'),'byte_length',c.byte_length,'media_type',c.media_type) ORDER BY m.committed_sequence),'[]'::jsonb) FROM conversation.messages m JOIN artifact.content_refs c ON c.content_id=m.content_id WHERE m.run_id=$1 AND m.role=2").bind(run).fetch_one(&mut *tx).await.map_err(db)?;
            let payload=serde_json::to_vec(&serde_json::json!({"occurrence_id":occurrence,"schedule_id":row.try_get::<uuid::Uuid,_>("schedule_id").map_err(db)?,"run_id":run,"session_id":row.try_get::<uuid::Uuid,_>("conversation_id").map_err(db)?,"owner_conversation_id":row.try_get::<uuid::Uuid,_>("owner_conversation_id").map_err(db)?,"status":status,"results":results,"terminal_error":row.try_get::<Option<Vec<u8>>,_>("terminal_error").map_err(db)?})).map_err(|_|Error::Corrupt)?;
            event(
                &mut tx,
                TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                ConversationId::from_uuid(row.try_get("owner_conversation_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                row.try_get("schedule_id").map_err(db)?,
                uuid::Uuid::new_v5(&occurrence, b"end"),
                "schedule.occurrence.ended",
                &payload,
                now,
            )
            .await?;
            sqlx::query("UPDATE agent.owner_schedule_occurrences SET status=$2,completed_at_ms=$3 WHERE occurrence_id=$1").bind(occurrence).bind(status).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(count)
    }

    /// Scan bounded candidates. A later dispatch rechecks placement, revision,
    /// and due time while holding the schedule lock; this is only a hint.
    pub async fn due(
        &self,
        now: UtcTimestamp,
        limit: u16,
    ) -> Result<Vec<(uuid::Uuid, u64)>, Error> {
        if limit == 0 || limit > 100 {
            return Err(Error::Invalid);
        }
        let rows=sqlx::query("SELECT schedule_id,trigger_epoch FROM agent.owner_schedules s WHERE source_run_id IS NOT NULL AND status IN ('active','completed') AND trigger_owner=$1 AND (next_dispatch_after_ms IS NULL OR next_dispatch_after_ms<=$2) AND (next_run_at_ms<=$2 OR EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o WHERE o.schedule_id=s.schedule_id AND o.status='pending')) ORDER BY next_run_at_ms,schedule_id LIMIT $3")
            .bind(serde_json::to_value(ScheduleTriggerOwner::Cloud).map_err(|_|Error::Corrupt)?).bind(now.unix_millis()).bind(i64::from(limit)).fetch_all(self.database.pool()).await.map_err(db)?;
        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get("schedule_id").map_err(db)?,
                    positive(r.try_get("trigger_epoch").map_err(db)?)?,
                ))
            })
            .collect()
    }

    /// Fire at most one occurrence under an exact placement epoch. Repeated
    /// or concurrent calls cannot admit a second Run for the same occurrence.
    pub async fn fire(
        &self,
        schedule: uuid::Uuid,
        owner: ScheduleTriggerOwner,
        epoch: u64,
        now: UtcTimestamp,
    ) -> Result<Option<RunId>, Error> {
        if owner != ScheduleTriggerOwner::Cloud {
            return Err(Error::StaleOwner);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row=sqlx::query("SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND source_run_id IS NOT NULL FOR UPDATE").bind(schedule).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
        let schedule_view = view(&row)?;
        if schedule_view.owner != owner
            || schedule_view.owner_epoch != epoch
            || !matches!(schedule_view.status.as_str(), "active" | "completed")
            || schedule_view.pending_owner.is_some()
        {
            return Err(Error::StaleOwner);
        }
        let pending=sqlx::query("SELECT occurrence_id,scheduled_at_ms,manual FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='pending' AND trigger_epoch=$2 ORDER BY scheduled_at_ms,occurrence_id LIMIT 1 FOR UPDATE").bind(schedule).bind(integer(epoch)?).fetch_optional(&mut *tx).await.map_err(db)?;
        let (occurrence, due, manual) = if let Some(pending) = pending {
            (
                pending
                    .try_get::<uuid::Uuid, _>("occurrence_id")
                    .map_err(db)?,
                pending.try_get::<i64, _>("scheduled_at_ms").map_err(db)?,
                pending.try_get::<bool, _>("manual").map_err(db)?,
            )
        } else {
            let Some(due) = schedule_view.next_run_at_unix_ms else {
                return Ok(None);
            };
            if due > now.unix_millis() {
                return Ok(None);
            }
            let occurrence = uuid::Uuid::new_v5(&schedule, &due.to_be_bytes());
            sqlx::query("INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,created_at_ms,trigger_epoch) VALUES($1,$2,$3,$4,FALSE,'pending',$5,$6)")
                .bind(occurrence).bind(schedule).bind(integer(schedule_view.revision)?).bind(due).bind(now.unix_millis()).bind(integer(epoch)?).execute(&mut *tx).await.map_err(db)?;
            let next = musteragent_goal::next_run_after(
                &schedule_view.definition.timing,
                &schedule_view.definition.timezone,
                UtcTimestamp::from_unix_millis(due),
            )
            .map_err(|_| Error::Corrupt)?;
            sqlx::query("UPDATE agent.owner_schedules SET next_run_at_ms=$2,status=CASE WHEN $2::bigint IS NULL THEN 'completed' ELSE 'active' END WHERE schedule_id=$1")
                .bind(schedule).bind(next.map(UtcTimestamp::unix_millis)).execute(&mut *tx).await.map_err(db)?;
            (occurrence, due, false)
        };
        let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o JOIN runtime.runs r ON r.run_id=o.run_id WHERE o.schedule_id=$1 AND r.status IN (1,2,3,4))").bind(schedule).fetch_one(&mut *tx).await.map_err(db)?;
        let decision = musteragent_runtime_automation::schedule_policy::occurrence_decision(
            schedule_view.definition.concurrency,
            schedule_view.definition.lateness,
            schedule_view.definition.lateness_window_millis,
            due,
            now.unix_millis(),
            active,
            manual,
        );
        use musteragent_runtime_automation::schedule_policy::OccurrenceDecision;
        if matches!(
            decision,
            OccurrenceDecision::SkipLate
                | OccurrenceDecision::SkipOverlap
                | OccurrenceDecision::FailLate
        ) {
            let status = if decision == OccurrenceDecision::FailLate {
                "dead_lettered"
            } else {
                "skipped"
            };
            sqlx::query("UPDATE agent.owner_schedule_occurrences SET status=$2,error_code=$3,completed_at_ms=$4 WHERE occurrence_id=$1").bind(occurrence).bind(status).bind(format!("{decision:?}")).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
            let tenant = TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?;
            let conversation =
                ConversationId::from_uuid(row.try_get("owner_conversation_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?;
            event(&mut tx,tenant,conversation,schedule,occurrence,"schedule.occurrence.ended",&serde_json::to_vec(&serde_json::json!({"occurrence_id":occurrence,"status":status,"reason":format!("{decision:?}")})).map_err(|_|Error::Corrupt)?,now).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(None);
        }
        if decision == OccurrenceDecision::ReplaceActive {
            let runs:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT r.run_id FROM agent.owner_schedule_occurrences o JOIN runtime.runs r ON r.run_id=o.run_id WHERE o.schedule_id=$1 AND r.status IN (1,2,3,4) ORDER BY r.run_id").bind(schedule).fetch_all(&mut *tx).await.map_err(db)?;
            let actor = Actor {
                tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                principal_kind: PrincipalKind::User,
            };
            // The pending occurrence is durable before requesting cancellation.
            // A replacement cannot enter admission until all prior Runs stop.
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            for run in runs {
                crate::cloud_cancellation::cancel(
                    &self.database,
                    self.codec.as_ref(),
                    actor,
                    RunId::from_uuid(run).map_err(|_| Error::Corrupt)?,
                    uuid::Uuid::new_v5(&occurrence, run.as_bytes()),
                    musteragent_runtime_contract::CancelReason::PolicyRevoked,
                    now,
                )
                .await?;
            }
            return Ok(None);
        }
        let source = crate::cloud_admission::source(
            &mut tx,
            RunId::from_uuid(row.try_get("source_run_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        )
        .await?;
        let input = self
            .content
            .put(PutContent {
                tenant_id: source.actor.tenant_id,
                media_type: MediaType::new("text/plain").map_err(|_| Error::Corrupt)?,
                bytes: schedule_view.definition.instructions.as_bytes().to_vec(),
            })
            .await
            .map_err(|_| Error::Unavailable)?;
        let batch = crate::cloud_admission::schedule_batch(
            &source,
            occurrence,
            input,
            now,
            self.codec.as_ref(),
            schedule_view.definition.retry,
        )
        .inspect_err(|error| {
            tracing::warn!(?error, "schedule batch construction failed");
        })?;
        crate::cloud_admission::session(&mut tx, &source, batch.run.context, now).await?;
        let run = batch.run.context.run_id;
        event(&mut tx,source.actor.tenant_id,source.conversation_id,schedule,uuid::Uuid::new_v5(&occurrence,b"start"),"schedule.occurrence.started",&serde_json::to_vec(&serde_json::json!({"occurrence_id":occurrence,"schedule_id":schedule,"run_id":run.to_string(),"session_id":batch.append.conversation_id.to_string(),"owner_conversation_id":source.conversation_id.to_string()})).map_err(|_|Error::Corrupt)?,now).await?;
        crate::cloud_admission::admit_schedule(&self.database, tx, batch, occurrence, &source)
            .await?;
        Ok(Some(run))
    }
}
