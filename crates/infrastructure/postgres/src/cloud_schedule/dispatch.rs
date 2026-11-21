//! Occurrence ownership is a database transaction, not a process lease. Once
//! admitted, the original P9 Run scheduler owns Attempt recovery.
use super::*;
use musteragent_content_ref::MediaType;
use musteragent_runtime_admission::AdmissionIdentities;
use musteragent_runtime_automation::schedule_policy::{OccurrenceDecision, occurrence_decision};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::SafeDisplayText;
use std::sync::Arc;

#[derive(Clone)]
pub struct PostgresScheduleDispatcher {
    database: PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    codec: Arc<dyn musteragent_ledger::LedgerPayloadEncoder>,
    hook: Option<Arc<dyn ScheduleAdmissionHook>>,
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
            hook: None,
        }
    }

    /// Install an optional host gate consulted before each scheduled Run is admitted.
    ///
    /// Delegated child sub-agent Runs do not pass through this hook.
    #[must_use]
    pub fn with_admission_hook(mut self, hook: Arc<dyn ScheduleAdmissionHook>) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Advance bounded work, then publish complete terminal projections. Hosts
    /// subscribe to the durable outbox; a worker crash between ticks is harmless.
    pub async fn tick(&self, now: UtcTimestamp) -> Result<(), Error> {
        self.deliver(now).await?;
        let mut first_error = None;
        for (id, epoch) in self.due(now, 32).await? {
            match self.fire(id, ScheduleTriggerOwner::Cloud, epoch, now).await {
                Ok(_) | Err(Error::StaleOwner) => {}
                Err(Error::AdmissionHookFailed(hook_error)) if !hook_error.retryable => {
                    first_error.get_or_insert(Error::AdmissionHookFailed(hook_error));
                }
                Err(error) => {
                    self.defer(id, epoch, now, &error).await?;
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
        error: &Error,
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
            let payload = match error {
                Error::AdmissionHookFailed(hook_error) => serde_json::to_vec(&serde_json::json!({
                    "schedule_id": schedule,
                    "owner_epoch": epoch,
                    "reason": hook_error.message.as_str(),
                    "error": format!("{error:?}"),
                    "retryable": hook_error.retryable,
                    "runtime_code": hook_error.runtime_code.map(|code| code.as_str()),
                    "retry_at_ms": retry_at,
                }))
                .map_err(|_| Error::Corrupt)?,
                _ => serde_json::to_vec(&serde_json::json!({
                    "schedule_id": schedule,
                    "owner_epoch": epoch,
                    "reason": format!("{error:?}"),
                    "retry_at_ms": retry_at,
                }))
                .map_err(|_| Error::Corrupt)?,
            };
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
        if let Some(hook) = &self.hook {
            self.fire_with_hook(hook.as_ref(), schedule, owner, epoch, now)
                .await
        } else {
            self.fire_unhooked(schedule, owner, epoch, now).await
        }
    }

    async fn fire_unhooked(
        &self,
        schedule: uuid::Uuid,
        owner: ScheduleTriggerOwner,
        epoch: u64,
        now: UtcTimestamp,
    ) -> Result<Option<RunId>, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row=sqlx::query("SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND source_run_id IS NOT NULL FOR UPDATE").bind(schedule).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
        let schedule_view = view(&row)?;
        validate_fire_fence(&schedule_view, &owner, epoch)?;
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
            materialize_timer_occurrence(
                &mut tx,
                schedule,
                &schedule_view,
                occurrence,
                due,
                epoch,
                now,
            )
            .await?;
            (occurrence, due, false)
        };
        let active = has_active_occurrence_run(&mut tx, schedule).await?;
        let decision = occurrence_decision(
            schedule_view.definition.concurrency,
            schedule_view.definition.lateness,
            schedule_view.definition.lateness_window_millis,
            due,
            now.unix_millis(),
            active,
            manual,
        );
        if matches!(
            decision,
            OccurrenceDecision::SkipLate
                | OccurrenceDecision::SkipOverlap
                | OccurrenceDecision::FailLate
        ) {
            settle_policy_non_admission(&mut tx, &row, schedule, occurrence, decision, now).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(None);
        }
        if decision == OccurrenceDecision::ReplaceActive {
            return self
                .replace_active_runs(tx, &row, schedule, occurrence, now)
                .await;
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
            None,
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

    async fn fire_with_hook(
        &self,
        hook: &dyn ScheduleAdmissionHook,
        schedule: uuid::Uuid,
        owner: ScheduleTriggerOwner,
        epoch: u64,
        now: UtcTimestamp,
    ) -> Result<Option<RunId>, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = sqlx::query(
            "SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND source_run_id IS NOT NULL FOR UPDATE",
        )
        .bind(schedule)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::NotFound)?;
        let schedule_view = view(&row)?;
        validate_fire_fence(&schedule_view, &owner, epoch)?;
        let pending = sqlx::query(
            "SELECT occurrence_id,scheduled_at_ms,manual FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='pending' AND trigger_epoch=$2 ORDER BY scheduled_at_ms,occurrence_id LIMIT 1 FOR UPDATE",
        )
        .bind(schedule)
        .bind(integer(epoch)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let (occurrence, due, manual, was_pending) = if let Some(pending) = pending {
            (
                pending
                    .try_get::<uuid::Uuid, _>("occurrence_id")
                    .map_err(db)?,
                pending.try_get::<i64, _>("scheduled_at_ms").map_err(db)?,
                pending.try_get::<bool, _>("manual").map_err(db)?,
                true,
            )
        } else {
            let Some(due) = schedule_view.next_run_at_unix_ms else {
                return Ok(None);
            };
            if due > now.unix_millis() {
                return Ok(None);
            }
            let occurrence = uuid::Uuid::new_v5(&schedule, &due.to_be_bytes());
            (occurrence, due, false, false)
        };
        let active = has_active_occurrence_run(&mut tx, schedule).await?;
        let decision = occurrence_decision(
            schedule_view.definition.concurrency,
            schedule_view.definition.lateness,
            schedule_view.definition.lateness_window_millis,
            due,
            now.unix_millis(),
            active,
            manual,
        );
        if matches!(
            decision,
            OccurrenceDecision::SkipLate
                | OccurrenceDecision::SkipOverlap
                | OccurrenceDecision::FailLate
        ) {
            if !was_pending {
                materialize_timer_occurrence(
                    &mut tx,
                    schedule,
                    &schedule_view,
                    occurrence,
                    due,
                    epoch,
                    now,
                )
                .await?;
            }
            settle_policy_non_admission(&mut tx, &row, schedule, occurrence, decision, now).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(None);
        }
        if decision == OccurrenceDecision::ReplaceActive {
            if !was_pending {
                materialize_timer_occurrence(
                    &mut tx,
                    schedule,
                    &schedule_view,
                    occurrence,
                    due,
                    epoch,
                    now,
                )
                .await?;
            }
            return self
                .replace_active_runs(tx, &row, schedule, occurrence, now)
                .await;
        }
        let source = crate::cloud_admission::source(
            &mut tx,
            RunId::from_uuid(row.try_get("source_run_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        )
        .await?;
        if !source.snapshot.application_catalogs.is_empty() {
            return Err(Error::ApplicationCatalogUnsupported);
        }
        let owner_conversation_id =
            ConversationId::from_uuid(row.try_get("owner_conversation_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?;
        let target_run_id = AdmissionIdentities::derive(
            CommandId::from_uuid(occurrence).map_err(|_| Error::Invalid)?,
        )
        .ok_or(Error::Invalid)?
        .run_id;
        // Release row locks before invoking the external host admission hook.
        tx.rollback().await.map_err(|_| Error::Unavailable)?;

        let hook_decision = hook
            .before_trigger(ScheduleTriggerContext {
                actor: source.actor,
                owner_conversation_id,
                workspace_id: source.workspace,
                schedule: schedule_view.clone(),
                occurrence_id: occurrence,
                scheduled_at: UtcTimestamp::from_unix_millis(due),
                manual,
                target_run_id,
                source_run_id: source.run_id,
                source_snapshot: source.snapshot.clone(),
            })
            .await;

        let partition = match hook_decision {
            Err(hook_error) if hook_error.retryable => {
                tracing::warn!(
                    schedule_id = %schedule,
                    occurrence_id = %occurrence,
                    retryable = true,
                    runtime_code = ?hook_error.runtime_code,
                    reason = %hook_error.message.as_str(),
                    "schedule admission hook failed transiently"
                );
                return Err(Error::AdmissionHookFailed(hook_error));
            }
            Err(hook_error) => {
                tracing::warn!(
                    schedule_id = %schedule,
                    occurrence_id = %occurrence,
                    retryable = false,
                    runtime_code = ?hook_error.runtime_code,
                    reason = %hook_error.message.as_str(),
                    "schedule admission hook failed permanently"
                );
                self.dead_letter_hook_failure(
                    schedule,
                    &owner,
                    epoch,
                    schedule_view.revision,
                    &schedule_view.status,
                    occurrence,
                    due,
                    was_pending,
                    source.actor.tenant_id,
                    owner_conversation_id,
                    &hook_error,
                    now,
                )
                .await?;
                return Err(Error::AdmissionHookFailed(hook_error));
            }
            Ok(PreTriggerDecision::Pause { reason }) => {
                return self
                    .pause_from_hook(
                        schedule,
                        &owner,
                        epoch,
                        schedule_view.revision,
                        &schedule_view.status,
                        occurrence,
                        due,
                        was_pending,
                        source.actor.tenant_id,
                        owner_conversation_id,
                        reason,
                        now,
                    )
                    .await;
            }
            Ok(PreTriggerDecision::UseFrozenSource) => None,
            Ok(PreTriggerDecision::Refresh(partition)) => Some(*partition),
        };

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
            partition,
        )
        .inspect_err(|error| {
            tracing::warn!(?error, "schedule batch construction failed");
        })?;

        let mut tx2 = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row2 = sqlx::query(
            "SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND source_run_id IS NOT NULL FOR UPDATE",
        )
        .bind(schedule)
        .fetch_optional(&mut *tx2)
        .await
        .map_err(db)?
        .ok_or(Error::NotFound)?;
        let current = view(&row2)?;
        validate_fire_fence(&current, &owner, epoch)?;
        if current.revision != schedule_view.revision {
            return Err(Error::StaleOwner);
        }
        if was_pending {
            let locked: Option<uuid::Uuid> = sqlx::query_scalar(
                "SELECT occurrence_id FROM agent.owner_schedule_occurrences WHERE occurrence_id=$1 AND schedule_id=$2 AND status='pending' AND trigger_epoch=$3 FOR UPDATE",
            )
            .bind(occurrence)
            .bind(schedule)
            .bind(integer(epoch)?)
            .fetch_optional(&mut *tx2)
            .await
            .map_err(db)?;
            if locked.is_none() {
                return Ok(None);
            }
        } else {
            if current.next_run_at_unix_ms != Some(due) {
                return Ok(None);
            }
            let inserted = materialize_timer_occurrence_if_absent(
                &mut tx2, schedule, &current, occurrence, due, epoch, now,
            )
            .await?;
            if !inserted {
                return Ok(None);
            }
        }
        let active = has_active_occurrence_run(&mut tx2, schedule).await?;
        let rechecked_decision = occurrence_decision(
            current.definition.concurrency,
            current.definition.lateness,
            current.definition.lateness_window_millis,
            due,
            now.unix_millis(),
            active,
            manual,
        );
        if matches!(
            rechecked_decision,
            OccurrenceDecision::SkipLate
                | OccurrenceDecision::SkipOverlap
                | OccurrenceDecision::FailLate
        ) {
            settle_policy_non_admission(
                &mut tx2,
                &row2,
                schedule,
                occurrence,
                rechecked_decision,
                now,
            )
            .await?;
            tx2.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(None);
        }
        if rechecked_decision == OccurrenceDecision::ReplaceActive {
            return self
                .replace_active_runs(tx2, &row2, schedule, occurrence, now)
                .await;
        }
        let fresh_source = crate::cloud_admission::source(&mut tx2, source.run_id).await?;
        if fresh_source.snapshot_hash != source.snapshot_hash {
            return Err(Error::Conflict);
        }
        crate::cloud_admission::session(&mut tx2, &source, batch.run.context, now).await?;
        let run = batch.run.context.run_id;
        event(
            &mut tx2,
            source.actor.tenant_id,
            source.conversation_id,
            schedule,
            uuid::Uuid::new_v5(&occurrence, b"start"),
            "schedule.occurrence.started",
            &serde_json::to_vec(&serde_json::json!({
                "occurrence_id": occurrence,
                "schedule_id": schedule,
                "run_id": run.to_string(),
                "session_id": batch.append.conversation_id.to_string(),
                "owner_conversation_id": source.conversation_id.to_string()
            }))
            .map_err(|_| Error::Corrupt)?,
            now,
        )
        .await?;
        crate::cloud_admission::admit_schedule(&self.database, tx2, batch, occurrence, &source)
            .await?;
        Ok(Some(run))
    }

    #[allow(clippy::too_many_arguments)]
    async fn pause_from_hook(
        &self,
        schedule: uuid::Uuid,
        owner: &ScheduleTriggerOwner,
        epoch: u64,
        expected_revision: u64,
        expected_status: &str,
        occurrence: uuid::Uuid,
        due: i64,
        was_pending: bool,
        tenant: TenantId,
        owner_conversation_id: ConversationId,
        reason: SafeDisplayText,
        now: UtcTimestamp,
    ) -> Result<Option<RunId>, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = sqlx::query(
            "SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND source_run_id IS NOT NULL FOR UPDATE",
        )
        .bind(schedule)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::NotFound)?;
        let current = view(&row)?;
        validate_fire_fence(&current, owner, epoch)?;
        if current.revision != expected_revision || current.status != expected_status {
            return Err(Error::StaleOwner);
        }
        let settled = if was_pending {
            sqlx::query(
                "UPDATE agent.owner_schedule_occurrences SET status='skipped',error_code=$2,completed_at_ms=$3 WHERE occurrence_id=$1 AND schedule_id=$4 AND status='pending' AND trigger_epoch=$5",
            )
            .bind(occurrence)
            .bind(reason.as_str())
            .bind(now.unix_millis())
            .bind(schedule)
            .bind(integer(epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
                == 1
        } else {
            if current.next_run_at_unix_ms != Some(due) {
                return Ok(None);
            }
            sqlx::query(
                "INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,error_code,created_at_ms,trigger_epoch,completed_at_ms) VALUES($1,$2,$3,$4,FALSE,'skipped',$5,$6,$7,$6) ON CONFLICT DO NOTHING",
            )
            .bind(occurrence)
            .bind(schedule)
            .bind(integer(current.revision)?)
            .bind(due)
            .bind(reason.as_str())
            .bind(now.unix_millis())
            .bind(integer(epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
                == 1
        };
        if !settled {
            return Ok(None);
        }
        event(
            &mut tx,
            tenant,
            owner_conversation_id,
            schedule,
            uuid::Uuid::new_v5(&occurrence, b"end"),
            "schedule.occurrence.ended",
            &serde_json::to_vec(&serde_json::json!({
                "occurrence_id": occurrence,
                "schedule_id": schedule,
                "owner_conversation_id": owner_conversation_id.to_string(),
                "status": "skipped",
                "reason": reason.as_str(),
            }))
            .map_err(|_| Error::Corrupt)?,
            now,
        )
        .await?;
        let cancelled: Vec<uuid::Uuid> = sqlx::query_scalar(
            "UPDATE agent.owner_schedule_occurrences SET status='cancelled',error_code='schedule_paused',completed_at_ms=$2 WHERE schedule_id=$1 AND status='pending' RETURNING occurrence_id",
        )
        .bind(schedule)
        .bind(now.unix_millis())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        for other in cancelled {
            event(
                &mut tx,
                tenant,
                owner_conversation_id,
                schedule,
                uuid::Uuid::new_v5(&other, b"end"),
                "schedule.occurrence.ended",
                &serde_json::to_vec(&serde_json::json!({
                    "occurrence_id": other,
                    "schedule_id": schedule,
                    "owner_conversation_id": owner_conversation_id.to_string(),
                    "status": "cancelled",
                    "reason": "schedule_paused",
                }))
                .map_err(|_| Error::Corrupt)?,
                now,
            )
            .await?;
        }
        let new_revision = current.revision.checked_add(1).ok_or(Error::Corrupt)?;
        sqlx::query(
            "UPDATE agent.owner_schedules SET next_dispatch_after_ms=NULL,revision=$2,status='paused',next_run_at_ms=NULL,pause_reason=$3,updated_at_ms=$4 WHERE schedule_id=$1",
        )
        .bind(schedule)
        .bind(integer(new_revision)?)
        .bind(reason.as_str())
        .bind(now.unix_millis())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let mut paused_view = current;
        paused_view.revision = new_revision;
        paused_view.status = "paused".into();
        paused_view.next_run_at_unix_ms = None;
        paused_view.pause_reason = Some(reason);
        let changed_payload = serde_json::to_vec(&paused_view).map_err(|_| Error::Corrupt)?;
        event(
            &mut tx,
            tenant,
            owner_conversation_id,
            schedule,
            uuid::Uuid::new_v5(&occurrence, b"schedule.paused"),
            "schedule.changed",
            &changed_payload,
            now,
        )
        .await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn dead_letter_hook_failure(
        &self,
        schedule: uuid::Uuid,
        owner: &ScheduleTriggerOwner,
        epoch: u64,
        expected_revision: u64,
        expected_status: &str,
        occurrence: uuid::Uuid,
        due: i64,
        was_pending: bool,
        tenant: TenantId,
        owner_conversation_id: ConversationId,
        hook_error: &ScheduleAdmissionHookError,
        now: UtcTimestamp,
    ) -> Result<(), Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = sqlx::query(
            "SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND source_run_id IS NOT NULL FOR UPDATE",
        )
        .bind(schedule)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::NotFound)?;
        let current = view(&row)?;
        validate_fire_fence(&current, owner, epoch)?;
        if current.revision != expected_revision || current.status != expected_status {
            return Err(Error::StaleOwner);
        }
        let settled = if was_pending {
            sqlx::query(
                "UPDATE agent.owner_schedule_occurrences SET status='dead_lettered',error_code=$2,completed_at_ms=$3 WHERE occurrence_id=$1 AND schedule_id=$4 AND status='pending' AND trigger_epoch=$5",
            )
            .bind(occurrence)
            .bind(hook_error.message.as_str())
            .bind(now.unix_millis())
            .bind(schedule)
            .bind(integer(epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
                == 1
        } else {
            if current.next_run_at_unix_ms != Some(due) {
                return Err(Error::StaleOwner);
            }
            let inserted = sqlx::query(
                "INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,error_code,created_at_ms,trigger_epoch,completed_at_ms) VALUES($1,$2,$3,$4,FALSE,'dead_lettered',$5,$6,$7,$6) ON CONFLICT DO NOTHING",
            )
            .bind(occurrence)
            .bind(schedule)
            .bind(integer(current.revision)?)
            .bind(due)
            .bind(hook_error.message.as_str())
            .bind(now.unix_millis())
            .bind(integer(epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
                == 1;
            if inserted {
                let next = musteragent_goal::next_run_after(
                    &current.definition.timing,
                    &current.definition.timezone,
                    UtcTimestamp::from_unix_millis(due),
                )
                .map_err(|_| Error::Corrupt)?;
                sqlx::query(
                    "UPDATE agent.owner_schedules SET next_dispatch_after_ms=NULL,next_run_at_ms=$2,status=CASE WHEN $2::bigint IS NULL THEN 'completed' ELSE 'active' END WHERE schedule_id=$1",
                )
                .bind(schedule)
                .bind(next.map(UtcTimestamp::unix_millis))
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            inserted
        };
        if !settled {
            return Err(Error::StaleOwner);
        }
        event(
            &mut tx,
            tenant,
            owner_conversation_id,
            schedule,
            uuid::Uuid::new_v5(&occurrence, b"end"),
            "schedule.occurrence.ended",
            &serde_json::to_vec(&serde_json::json!({
                "occurrence_id": occurrence,
                "schedule_id": schedule,
                "owner_conversation_id": owner_conversation_id.to_string(),
                "status": "dead_lettered",
                "error_code": "admission_hook_failed",
                "retryable": false,
                "runtime_code": hook_error.runtime_code.map(|code| code.as_str()),
                "reason": hook_error.message.as_str(),
            }))
            .map_err(|_| Error::Corrupt)?,
            now,
        )
        .await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    async fn replace_active_runs(
        &self,
        mut tx: GovernedPostgresTransaction<'static>,
        row: &PgRow,
        schedule: uuid::Uuid,
        occurrence: uuid::Uuid,
        now: UtcTimestamp,
    ) -> Result<Option<RunId>, Error> {
        let runs: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT r.run_id FROM agent.owner_schedule_occurrences o JOIN runtime.runs r ON r.run_id=o.run_id WHERE o.schedule_id=$1 AND r.status IN (1,2,3,4) ORDER BY r.run_id",
        )
        .bind(schedule)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
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
        Ok(None)
    }
}

fn validate_fire_fence(
    schedule_view: &ScheduleView,
    owner: &ScheduleTriggerOwner,
    epoch: u64,
) -> Result<(), Error> {
    if &schedule_view.owner != owner
        || schedule_view.owner_epoch != epoch
        || !matches!(schedule_view.status.as_str(), "active" | "completed")
        || schedule_view.pending_owner.is_some()
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}

async fn has_active_occurrence_run(
    tx: &mut sqlx::PgConnection,
    schedule: uuid::Uuid,
) -> Result<bool, Error> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o JOIN runtime.runs r ON r.run_id=o.run_id WHERE o.schedule_id=$1 AND r.status IN (1,2,3,4))",
    )
    .bind(schedule)
    .fetch_one(&mut *tx)
    .await
    .map_err(db)
}

async fn materialize_timer_occurrence(
    tx: &mut sqlx::PgConnection,
    schedule: uuid::Uuid,
    schedule_view: &ScheduleView,
    occurrence: uuid::Uuid,
    due: i64,
    epoch: u64,
    now: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,created_at_ms,trigger_epoch) VALUES($1,$2,$3,$4,FALSE,'pending',$5,$6)",
    )
    .bind(occurrence)
    .bind(schedule)
    .bind(integer(schedule_view.revision)?)
    .bind(due)
    .bind(now.unix_millis())
    .bind(integer(epoch)?)
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    advance_schedule_cursor(tx, schedule, schedule_view, due).await
}

async fn materialize_timer_occurrence_if_absent(
    tx: &mut sqlx::PgConnection,
    schedule: uuid::Uuid,
    schedule_view: &ScheduleView,
    occurrence: uuid::Uuid,
    due: i64,
    epoch: u64,
    now: UtcTimestamp,
) -> Result<bool, Error> {
    let inserted = sqlx::query(
        "INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,created_at_ms,trigger_epoch) VALUES($1,$2,$3,$4,FALSE,'pending',$5,$6) ON CONFLICT DO NOTHING",
    )
    .bind(occurrence)
    .bind(schedule)
    .bind(integer(schedule_view.revision)?)
    .bind(due)
    .bind(now.unix_millis())
    .bind(integer(epoch)?)
    .execute(&mut *tx)
    .await
    .map_err(db)?
    .rows_affected()
        == 1;
    if inserted {
        advance_schedule_cursor(tx, schedule, schedule_view, due).await?;
    }
    Ok(inserted)
}

async fn advance_schedule_cursor(
    tx: &mut sqlx::PgConnection,
    schedule: uuid::Uuid,
    schedule_view: &ScheduleView,
    due: i64,
) -> Result<(), Error> {
    let next = musteragent_goal::next_run_after(
        &schedule_view.definition.timing,
        &schedule_view.definition.timezone,
        UtcTimestamp::from_unix_millis(due),
    )
    .map_err(|_| Error::Corrupt)?;
    sqlx::query(
        "UPDATE agent.owner_schedules SET next_run_at_ms=$2,status=CASE WHEN $2::bigint IS NULL THEN 'completed' ELSE 'active' END WHERE schedule_id=$1",
    )
    .bind(schedule)
    .bind(next.map(UtcTimestamp::unix_millis))
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    Ok(())
}

async fn settle_policy_non_admission(
    tx: &mut sqlx::PgConnection,
    row: &PgRow,
    schedule: uuid::Uuid,
    occurrence: uuid::Uuid,
    decision: OccurrenceDecision,
    now: UtcTimestamp,
) -> Result<(), Error> {
    let status = if decision == OccurrenceDecision::FailLate {
        "dead_lettered"
    } else {
        "skipped"
    };
    sqlx::query(
        "UPDATE agent.owner_schedule_occurrences SET status=$2,error_code=$3,completed_at_ms=$4 WHERE occurrence_id=$1",
    )
    .bind(occurrence)
    .bind(status)
    .bind(format!("{decision:?}"))
    .bind(now.unix_millis())
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    let tenant =
        TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?).map_err(|_| Error::Corrupt)?;
    let conversation = ConversationId::from_uuid(row.try_get("owner_conversation_id").map_err(db)?)
        .map_err(|_| Error::Corrupt)?;
    event(
        tx,
        tenant,
        conversation,
        schedule,
        occurrence,
        "schedule.occurrence.ended",
        &serde_json::to_vec(&serde_json::json!({
            "occurrence_id": occurrence,
            "status": status,
            "reason": format!("{decision:?}")
        }))
        .map_err(|_| Error::Corrupt)?,
        now,
    )
    .await
}
