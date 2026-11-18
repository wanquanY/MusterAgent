//! Neutral scheduled execution over the P4 schedule inventory and original Runs.
use crate::{GovernedPostgresTransaction, PostgresDatabaseRuntime};
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_runtime_automation::*;
use sqlx::{Row, postgres::PgRow};

type Error = ScheduleServiceError;
mod dispatch;
mod tools;
pub use dispatch::PostgresScheduleDispatcher;
pub use tools::PostgresCloudScheduleTools;

/// Host and Tool management share this service. Actor membership, execution
/// origin, attempt fences, revision and command identity are rechecked in SQL.
#[derive(Clone)]
pub struct PostgresScheduleService {
    database: PostgresDatabaseRuntime,
}
impl PostgresScheduleService {
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }

    async fn scope(
        &self,
        scope: &ScheduleScope,
    ) -> Result<GovernedPostgresTransaction<'static>, Error> {
        if scope.actor.principal_kind != PrincipalKind::User {
            return Err(Error::NotFound);
        }
        let mut tx = if let Some(attempt) = &scope.attempt {
            if attempt.run_id != scope.source_run_id {
                return Err(Error::StaleAttempt);
            }
            let (tx, state) = crate::commit_lane::acquire(&self.database, attempt)
                .await
                .map_err(|_| Error::StaleAttempt)?;
            if state.tenant_id != scope.actor.tenant_id
                || state.conversation_id != scope.owner_conversation_id
            {
                return Err(Error::NotFound);
            }
            tx
        } else {
            crate::commit_lane::acquire_for_run(&self.database, scope.source_run_id)
                .await
                .map_err(|_| Error::Unavailable)?
                .ok_or(Error::NotFound)?
        };
        Self::authorize_in(&mut tx, scope).await?;
        Ok(tx)
    }

    async fn authorize_in(tx: &mut sqlx::PgConnection, scope: &ScheduleScope) -> Result<(), Error> {
        if scope.actor.principal_kind != PrincipalKind::User {
            return Err(Error::NotFound);
        }
        let authorized: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.runs r JOIN conversation.conversations c ON c.conversation_id=r.conversation_id AND c.tenant_id=r.tenant_id WHERE r.run_id=$1 AND r.tenant_id=$2 AND c.conversation_id=$3 AND c.created_by=$4 AND c.lifecycle=1 AND r.execution_home_kind=1)")
            .bind(scope.source_run_id.into_uuid()).bind(scope.actor.tenant_id.into_uuid()).bind(scope.owner_conversation_id.into_uuid()).bind(scope.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !authorized {
            return Err(Error::NotFound);
        }
        let scheduled: bool = sqlx::query_scalar("WITH RECURSIVE ancestry AS (SELECT run_id,parent_run_id FROM runtime.runs WHERE run_id=$1 UNION SELECT r.run_id,r.parent_run_id FROM runtime.runs r JOIN ancestry a ON r.run_id=a.parent_run_id) SELECT EXISTS(SELECT 1 FROM ancestry a JOIN agent.owner_schedule_occurrences o ON o.run_id=a.run_id)")
            .bind(scope.source_run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if scheduled {
            return Err(Error::ScheduledContextDenied);
        }
        recheck_attempt(tx, scope).await
    }

    /// List only schedules owned by this authenticated conversation. Deleted
    /// records remain inspectable through history but are excluded from this list.
    pub async fn list(&self, scope: &ScheduleScope) -> Result<Vec<ScheduleView>, Error> {
        let mut tx = self.scope(scope).await?;
        let rows = sqlx::query("SELECT * FROM agent.owner_schedules WHERE tenant_id=$1 AND owner_id=$2 AND owner_conversation_id=$3 AND status<>'deleted' ORDER BY created_at_ms,schedule_id LIMIT 100")
            .bind(scope.actor.tenant_id.into_uuid()).bind(scope.actor.principal_id.into_uuid()).bind(scope.owner_conversation_id.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
        let result = rows.iter().map(view).collect::<Result<_, _>>()?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(result)
    }

    /// Immutable history, ordered by event time and identity. The caller may
    /// inspect deleted schedules; actor and owning conversation still apply.
    pub async fn history(
        &self,
        scope: &ScheduleScope,
        schedule: uuid::Uuid,
    ) -> Result<Vec<serde_json::Value>, Error> {
        let mut events = Vec::new();
        let mut after = None;
        loop {
            let page = self.history_page(scope, schedule, after, 100).await?;
            events.extend(page.events);
            match page.next {
                Some(cursor) => after = Some(cursor),
                None => return Ok(events),
            }
        }
    }

    /// Host and tool consumers can traverse all history without a silent cap.
    pub async fn history_page(
        &self,
        scope: &ScheduleScope,
        schedule: uuid::Uuid,
        after: Option<ScheduleHistoryCursor>,
        limit: u16,
    ) -> Result<ScheduleHistoryPage, Error> {
        if limit == 0 || limit > 100 {
            return Err(Error::Invalid);
        }
        let mut tx = self.scope(scope).await?;
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owner_schedules WHERE schedule_id=$1 AND tenant_id=$2 AND owner_id=$3 AND owner_conversation_id=$4)")
            .bind(schedule).bind(scope.actor.tenant_id.into_uuid()).bind(scope.actor.principal_id.into_uuid()).bind(scope.owner_conversation_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !exists {
            return Err(Error::NotFound);
        }
        let mut events: Vec<serde_json::Value> = sqlx::query_scalar("SELECT jsonb_build_object('event_id',event_id,'kind',kind,'occurred_at_ms',occurred_at_ms,'payload',payload) FROM runtime.schedule_events WHERE schedule_id=$1 AND ($2::bigint IS NULL OR (occurred_at_ms,event_id)>($2,$3)) ORDER BY occurred_at_ms,event_id LIMIT $4")
            .bind(schedule).bind(after.map(|c|c.occurred_at_ms)).bind(after.map(|c|c.event_id)).bind(i64::from(limit)+1).fetch_all(&mut *tx).await.map_err(db)?;
        let next = if events.len() > usize::from(limit) {
            events.pop();
            let last = events.last().ok_or(Error::Corrupt)?;
            Some(ScheduleHistoryCursor {
                occurred_at_ms: last["occurred_at_ms"].as_i64().ok_or(Error::Corrupt)?,
                event_id: last["event_id"]
                    .as_str()
                    .ok_or(Error::Corrupt)?
                    .parse()
                    .map_err(|_| Error::Corrupt)?,
            })
        } else {
            None
        };
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ScheduleHistoryPage { events, next })
    }

    /// A single transaction owns command receipt, projection and outbox event.
    pub async fn change(&self, command: ScheduleCommand) -> Result<ScheduleView, Error> {
        let mut tx = self.scope(&command.scope).await?;
        let result = self.change_in(&mut tx, &command).await?;
        recheck_attempt(&mut tx, &command.scope).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(result)
    }

    pub(crate) async fn change_in(
        &self,
        tx: &mut sqlx::PgConnection,
        command: &ScheduleCommand,
    ) -> Result<ScheduleView, Error> {
        if command.schedule_id.is_nil() {
            return Err(Error::Invalid);
        }
        let fingerprint = Sha256::digest(serde_json::to_vec(&serde_json::json!({"schedule": command.schedule_id,"revision":command.expected_revision,"action":command.action,"source_run":command.scope.source_run_id.to_string(),"conversation":command.scope.owner_conversation_id.to_string()})).map_err(|_|Error::Invalid)?);
        let actor = command.scope.actor;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "schedule-owner:{}:{}",
                actor.tenant_id, actor.principal_id
            ))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "schedule-command:{}:{}:{}",
                actor.tenant_id, actor.principal_id, command.command_id
            ))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        if let Some(row)=sqlx::query("SELECT fingerprint,response FROM agent.owner_schedule_commands WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3").bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.command_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? {
            if row.try_get::<Vec<u8>,_>("fingerprint").map_err(db)? != fingerprint.into_bytes() { return Err(Error::Conflict); }
            return serde_json::from_slice(&row.try_get::<Vec<u8>,_>("response").map_err(db)?).map_err(|_|Error::Corrupt);
        }
        let item = if let ScheduleAction::Create { definition, owner } = &command.action {
            if command.expected_revision != 0 {
                return Err(Error::Invalid);
            }
            definition.validate()?;
            let source = crate::cloud_admission::source(tx, command.scope.source_run_id).await?;
            if !source.snapshot.application_catalogs.is_empty() {
                return Err(Error::ApplicationCatalogUnsupported);
            }
            owner.validate()?;
            let next = musteragent_goal::initial_next_run(
                &definition.timing,
                &definition.timezone,
                command.now,
            )
            .map_err(|_| Error::Invalid)?
            .ok_or(Error::Invalid)?;
            let count: i64=sqlx::query_scalar("SELECT count(*) FROM agent.owner_schedules WHERE tenant_id=$1 AND owner_id=$2 AND status<>'deleted'").bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            if count >= 100 {
                return Err(Error::Invalid);
            }
            let agent: uuid::Uuid=sqlx::query_scalar("SELECT p.agent_ref FROM runtime.runs r JOIN conversation.participants p ON p.participant_id=r.executor_participant_id WHERE r.run_id=$1").bind(command.scope.source_run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            let encoded = serde_json::to_vec(definition).map_err(|_| Error::Invalid)?;
            let inserted=sqlx::query("INSERT INTO agent.owner_schedules(schedule_id,tenant_id,owner_id,agent_id,revision,definition,status,next_run_at_ms,created_at_ms,updated_at_ms,source_run_id,owner_conversation_id,trigger_owner) VALUES($1,$2,$3,$4,1,$5,'active',$6,$7,$7,$8,$9,$10) ON CONFLICT DO NOTHING")
                .bind(command.schedule_id).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent).bind(encoded).bind(next.unix_millis()).bind(command.now.unix_millis()).bind(command.scope.source_run_id.into_uuid()).bind(command.scope.owner_conversation_id.into_uuid()).bind(serde_json::to_value(owner).map_err(|_|Error::Invalid)?).execute(&mut *tx).await.map_err(db)?;
            if inserted.rows_affected() != 1 {
                return Err(Error::Conflict);
            }
            ScheduleView {
                schedule_id: command.schedule_id,
                revision: 1,
                definition: definition.clone(),
                status: "active".into(),
                next_run_at_unix_ms: Some(next.unix_millis()),
                owner: owner.clone(),
                owner_epoch: 1,
                pending_owner: None,
                handover_due_at_unix_ms: None,
            }
        } else {
            let raw=sqlx::query("SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND tenant_id=$2 AND owner_id=$3 AND owner_conversation_id=$4 AND status<>'deleted' FOR UPDATE")
                .bind(command.schedule_id).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.scope.owner_conversation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
            let mut item = view(&raw)?;
            let mut handover_status: Option<String> = raw.try_get("handover_status").map_err(db)?;
            if item.revision != command.expected_revision {
                return Err(Error::Conflict);
            }
            if item.pending_owner.is_some()
                && !matches!(
                    command.action,
                    ScheduleAction::CompleteHandover { .. } | ScheduleAction::Delete
                )
            {
                return Err(Error::StaleOwner);
            }
            match &command.action {
                ScheduleAction::Update { definition } => {
                    definition.validate()?;
                    item.definition = definition.clone();
                    if item.status != "paused" {
                        item.status = "active".into();
                        item.next_run_at_unix_ms = Some(next(&item, command.now)?);
                    }
                }
                ScheduleAction::Pause => {
                    item.status = "paused".into();
                    item.next_run_at_unix_ms = None;
                }
                ScheduleAction::Resume => {
                    item.status = "active".into();
                    item.next_run_at_unix_ms = Some(next(&item, command.now)?);
                }
                ScheduleAction::Delete => {
                    item.status = "deleted".into();
                    item.next_run_at_unix_ms = None;
                    item.pending_owner = None;
                    item.handover_due_at_unix_ms = None;
                    handover_status = None;
                }
                ScheduleAction::RunNow => {
                    if !matches!(item.status.as_str(), "active" | "completed") {
                        return Err(Error::StaleOwner);
                    }
                    sqlx::query("INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,created_at_ms,trigger_epoch) VALUES($1,$2,$3,$4,TRUE,'pending',$4,$5)")
                        .bind(command.command_id.into_uuid()).bind(item.schedule_id).bind(integer(item.revision)?).bind(command.now.unix_millis()).bind(integer(item.owner_epoch)?).execute(&mut *tx).await.map_err(db)?;
                }
                ScheduleAction::BeginHandover { owner } => {
                    owner.validate()?;
                    if *owner == item.owner {
                        return Err(Error::Invalid);
                    }
                    item.handover_due_at_unix_ms = item.next_run_at_unix_ms;
                    handover_status = Some(item.status.clone());
                    item.status = "paused".into();
                    item.next_run_at_unix_ms = None;
                    item.owner_epoch = item.owner_epoch.checked_add(1).ok_or(Error::Corrupt)?;
                    item.pending_owner = Some(owner.clone());
                }
                ScheduleAction::CompleteHandover { epoch } => {
                    if *epoch != item.owner_epoch {
                        return Err(Error::StaleOwner);
                    }
                    item.owner = item.pending_owner.take().ok_or(Error::StaleOwner)?;
                    item.status = handover_status.take().ok_or(Error::Corrupt)?;
                    item.next_run_at_unix_ms = item.handover_due_at_unix_ms.take();
                }
                ScheduleAction::Create { .. } => return Err(Error::Invalid),
            }
            item.revision = item.revision.checked_add(1).ok_or(Error::Corrupt)?;
            sqlx::query("UPDATE agent.owner_schedules SET next_dispatch_after_ms=NULL,revision=$2,definition=$3,status=$4,next_run_at_ms=$5,updated_at_ms=$6,trigger_owner=$7,trigger_epoch=$8,pending_trigger_owner=$9,handover_due_at_ms=$10,handover_status=$11 WHERE schedule_id=$1")
                .bind(item.schedule_id).bind(integer(item.revision)?).bind(serde_json::to_vec(&item.definition).map_err(|_|Error::Corrupt)?).bind(&item.status).bind(item.next_run_at_unix_ms).bind(command.now.unix_millis()).bind(serde_json::to_value(&item.owner).map_err(|_|Error::Corrupt)?).bind(integer(item.owner_epoch)?).bind(item.pending_owner.as_ref().map(serde_json::to_value).transpose().map_err(|_|Error::Corrupt)?).bind(item.handover_due_at_unix_ms).bind(handover_status).execute(&mut *tx).await.map_err(db)?;
            if !matches!(command.action, ScheduleAction::RunNow) {
                let cancelled:Vec<uuid::Uuid> = sqlx::query_scalar("UPDATE agent.owner_schedule_occurrences SET status='cancelled',error_code='schedule_changed',completed_at_ms=$2 WHERE schedule_id=$1 AND status='pending' RETURNING occurrence_id").bind(item.schedule_id).bind(command.now.unix_millis()).fetch_all(&mut *tx).await.map_err(db)?;
                for occurrence in cancelled {
                    event(tx,actor.tenant_id,command.scope.owner_conversation_id,item.schedule_id,uuid::Uuid::new_v5(&occurrence,b"end"),"schedule.occurrence.ended",&serde_json::to_vec(&serde_json::json!({"occurrence_id":occurrence,"schedule_id":item.schedule_id,"owner_conversation_id":command.scope.owner_conversation_id.to_string(),"status":"cancelled","reason":"schedule_changed"})).map_err(|_|Error::Corrupt)?,command.now).await?;
                }
            }
            item
        };
        // Normalize the persisted response once; replays return this exact view.
        item.definition.validate()?;
        let response = serde_json::to_vec(&item).map_err(|_| Error::Corrupt)?;
        sqlx::query("INSERT INTO agent.owner_schedule_commands(tenant_id,owner_id,command_id,schedule_id,fingerprint,response) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.command_id.into_uuid()).bind(item.schedule_id).bind(fingerprint.into_bytes().as_slice()).bind(&response).execute(&mut *tx).await.map_err(db)?;
        event(
            tx,
            actor.tenant_id,
            command.scope.owner_conversation_id,
            item.schedule_id,
            command.command_id.into_uuid(),
            "schedule.changed",
            &response,
            command.now,
        )
        .await?;
        Ok(item)
    }
}

fn next(item: &ScheduleView, now: UtcTimestamp) -> Result<i64, Error> {
    musteragent_goal::initial_next_run(&item.definition.timing, &item.definition.timezone, now)
        .map_err(|_| Error::Invalid)?
        .map(|t| t.unix_millis())
        .ok_or(Error::Invalid)
}
pub(super) fn view(row: &PgRow) -> Result<ScheduleView, Error> {
    Ok(ScheduleView {
        schedule_id: row.try_get("schedule_id").map_err(db)?,
        revision: positive(row.try_get("revision").map_err(db)?)?,
        definition: serde_json::from_slice(&row.try_get::<Vec<u8>, _>("definition").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        status: row.try_get("status").map_err(db)?,
        next_run_at_unix_ms: row.try_get("next_run_at_ms").map_err(db)?,
        owner: serde_json::from_value(row.try_get("trigger_owner").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        owner_epoch: positive(row.try_get("trigger_epoch").map_err(db)?)?,
        handover_due_at_unix_ms: row.try_get("handover_due_at_ms").map_err(db)?,
        pending_owner: row
            .try_get::<Option<serde_json::Value>, _>("pending_trigger_owner")
            .map_err(db)?
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
    })
}
fn integer(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::Invalid)
}
fn positive(value: i64) -> Result<u64, Error> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n > 0)
        .ok_or(Error::Corrupt)
}
fn db(error: sqlx::Error) -> Error {
    tracing::warn!(code=?error.as_database_error().and_then(|e|e.code()),"schedule database operation failed");
    Error::Unavailable
}
#[allow(clippy::too_many_arguments)]
async fn event(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    owner: ConversationId,
    schedule: uuid::Uuid,
    event: uuid::Uuid,
    kind: &str,
    payload: &[u8],
    now: UtcTimestamp,
) -> Result<(), Error> {
    let value: serde_json::Value = serde_json::from_slice(payload).map_err(|_| Error::Corrupt)?;
    sqlx::query("INSERT INTO runtime.schedule_events(event_id,tenant_id,schedule_id,command_id,kind,payload,occurred_at_ms) VALUES($1,$2,$3,$1,$4,$5,$6)").bind(event).bind(tenant.into_uuid()).bind(schedule).bind(kind).bind(value).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$7)")
        .bind(uuid::Uuid::new_v5(&event,b"schedule.outbox")).bind(tenant.into_uuid()).bind(owner.into_uuid()).bind(kind).bind(payload).bind(Sha256::digest(payload).into_bytes().as_slice()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    Ok(())
}

async fn recheck_attempt(tx: &mut sqlx::PgConnection, scope: &ScheduleScope) -> Result<(), Error> {
    let Some(a) = &scope.attempt else {
        return Ok(());
    };
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.runs r JOIN runtime.attempts a ON a.run_id=r.run_id JOIN runtime.leases l ON l.run_id=r.run_id AND l.attempt_id=a.attempt_id WHERE r.run_id=$1 AND r.status=2 AND r.cancel_requested_at_ms IS NULL AND a.attempt_id=$2 AND a.attempt_number=$3 AND a.status=1 AND l.worker_id=$4 AND l.lease_id=$5 AND l.fencing_token=$6::bigint AND l.expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint)")
        .bind(a.run_id.into_uuid()).bind(a.attempt_id.into_uuid()).bind(i32::try_from(a.attempt_number.get()).map_err(|_|Error::Invalid)?).bind(a.worker_id.as_str()).bind(a.lease_id.into_uuid()).bind(a.fencing_token.get().to_string()).fetch_one(tx).await.map_err(db)?;
    if valid {
        Ok(())
    } else {
        Err(Error::StaleAttempt)
    }
}
