//! Durable schedule management and occurrence acceptance in the original DB.
pub mod codec;
pub(crate) mod dispatch;
pub(crate) mod release;
use codec::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::{Actor, AgentId, PrincipalKind};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use prost::Message;
use sqlx::Row;
type Error = OwnerScheduleError;

#[derive(Clone)]
pub struct PostgresOwnerScheduleStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresOwnerScheduleStore {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
    async fn write_unit_of_work(
        &self,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, Error> {
        self.database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(transaction)
    }
    async fn change_inner(&self, command: ChangeOwnerSchedule) -> Result<OwnerSchedule, Error> {
        let mut tx = self.write_unit_of_work().await?;
        let result = change_in_transaction(&mut tx, command, None, None).await?;
        tx.commit().await.map_err(transaction)?;
        Ok(result)
    }
}
pub(crate) async fn change_in_transaction(
    tx: &mut sqlx::PgConnection,
    command: ChangeOwnerSchedule,
    creation_source: Option<(
        musteragent_identity::RunId,
        musteragent_identity::InvocationId,
    )>,
    source_release: Option<musteragent_identity::ProductReleaseRef>,
) -> Result<OwnerSchedule, Error> {
    use OwnerScheduleAction as Action;
    if command.actor.principal_kind != PrincipalKind::User || command.schedule_id.is_nil() {
        return Err(Error::Invalid);
    }
    let has_definition = matches!(command.action, Action::Create | Action::Update);
    if has_definition != command.definition.is_some() {
        return Err(Error::Invalid);
    }
    if let Some(v) = &command.definition {
        v.validate()?;
    }
    let encoded = command
        .definition
        .as_ref()
        .map(|v| definition_wire(v).encode_to_vec())
        .unwrap_or_default();
    let fingerprint = Sha256::digest(
        [
            format!(
                "{}:{}:{:?}:{}:",
                command.agent_id, command.schedule_id, command.action, command.expected_revision
            )
            .as_bytes(),
            &encoded,
        ]
        .concat(),
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!(
            "owner-schedule:{}:{}:{}",
            command.actor.tenant_id, command.actor.principal_id, command.command_id
        ))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    authorize(tx, command.actor, command.agent_id).await?;
    if let Some(receipt) = sqlx::query("SELECT fingerprint,response FROM agent.owner_schedule_commands WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3")
            .bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).bind(command.command_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)? {
            if receipt.try_get::<Vec<u8>,_>("fingerprint").map_err(db)? != fingerprint.into_bytes() { return Err(Error::Conflict); }
            return decode_receipt(&receipt.try_get::<Vec<u8>,_>("response").map_err(db)?);
        }
    let mut item = if command.action == Action::Create {
        if command.expected_revision != 0 {
            return Err(Error::Invalid);
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.owner_schedules WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND source_run_id IS NULL AND status<>'deleted'")
                .bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).bind(command.agent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if count >= 100 {
            return Err(Error::Limit);
        }
        let definition = command.definition.clone().ok_or(Error::Invalid)?;
        let next = definition.next_after(command.now)?.ok_or(Error::Invalid)?;
        let reference = source_release.unwrap_or_else(|| {
            musteragent_skill::ProductSkillRelease::for_product(
                musteragent_identity::ProductId::KERNEL,
            )
            .reference()
        });
        let publication = release::encode(&reference)?;
        let inserted = sqlx::query("INSERT INTO agent.owner_schedules(schedule_id,tenant_id,owner_id,agent_id,revision,definition,status,next_run_at_ms,created_at_ms,updated_at_ms,product_release) VALUES($1,$2,$3,$4,1,$5,'active',$6,$7,$7,$8) ON CONFLICT DO NOTHING")
                .bind(command.schedule_id).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).bind(command.agent_id.into_uuid())
                .bind(&encoded).bind(next.unix_millis()).bind(command.now.unix_millis()).bind(publication).execute(&mut *tx).await.map_err(db)?;
        if inserted.rows_affected() != 1 {
            return Err(Error::Conflict);
        }
        OwnerSchedule {
            schedule_id: command.schedule_id,
            agent_id: command.agent_id,
            revision: 1,
            definition,
            status: "active".into(),
            next_run_at: Some(next),
            recent_runs: vec![],
        }
    } else {
        let found = sqlx::query("SELECT * FROM agent.owner_schedules WHERE schedule_id=$1 AND tenant_id=$2 AND owner_id=$3 AND agent_id=$4 AND source_run_id IS NULL AND status<>'deleted' FOR UPDATE")
                .bind(command.schedule_id).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).bind(command.agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
        let mut item = row(&found)?;
        if item.revision != command.expected_revision {
            return Err(Error::Conflict);
        }
        match command.action {
            Action::Update => {
                item.definition = command.definition.clone().ok_or(Error::Invalid)?;
                if item.status != "paused" {
                    item.status = "active".into();
                    item.next_run_at = Some(
                        item.definition
                            .next_after(command.now)?
                            .ok_or(Error::Invalid)?,
                    );
                }
            }
            Action::Pause => {
                item.status = "paused".into();
                item.next_run_at = None;
            }
            Action::Resume => {
                item.next_run_at = Some(
                    item.definition
                        .next_after(command.now)?
                        .ok_or(Error::Invalid)?,
                );
                item.status = "active".into();
            }
            Action::Delete => {
                item.status = "deleted".into();
                item.next_run_at = None;
            }
            Action::RunNow => {
                // Explicit executions have their own command identity; they never move the regular cursor.
                let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='pending')").bind(item.schedule_id).fetch_one(&mut *tx).await.map_err(db)?;
                if busy {
                    return Err(Error::Conflict);
                }
                let publication = release::encode(&release::original_or_legacy(
                    found.try_get("product_release").map_err(db)?,
                )?)?;
                sqlx::query("INSERT INTO agent.owner_schedule_occurrences(occurrence_id,schedule_id,schedule_revision,scheduled_at_ms,manual,status,created_at_ms,product_release) VALUES($1,$2,$3,$4,TRUE,'pending',$4,$5)")
                        .bind(command.command_id.into_uuid()).bind(item.schedule_id).bind(item.revision as i64).bind(command.now.unix_millis()).bind(publication).execute(&mut *tx).await.map_err(db)?;
            }
            Action::Create => unreachable!(),
        }
        item.revision += 1;
        sqlx::query("UPDATE agent.owner_schedules SET retry_after_ms=0,revision=$2,definition=$3,status=$4,next_run_at_ms=$5,updated_at_ms=$6 WHERE schedule_id=$1")
                .bind(item.schedule_id).bind(item.revision as i64).bind(definition_wire(&item.definition).encode_to_vec()).bind(&item.status).bind(item.next_run_at.map(|v|v.unix_millis())).bind(command.now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        if command.action != Action::RunNow {
            sqlx::query("UPDATE agent.owner_schedule_occurrences SET status='cancelled' WHERE schedule_id=$1 AND status='pending'").bind(item.schedule_id).execute(&mut *tx).await.map_err(db)?;
        }
        item
    };
    item.recent_runs = dispatch::history(tx, item.schedule_id).await?;
    sqlx::query("INSERT INTO agent.owner_schedule_commands(tenant_id,owner_id,command_id,schedule_id,fingerprint,response) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).bind(command.command_id.into_uuid()).bind(item.schedule_id)
            .bind(fingerprint.into_bytes().as_slice()).bind(owner_schedule_wire(&item).encode_to_vec()).execute(&mut *tx).await.map_err(db)?;
    if command.action == Action::Create
        && let Some((run_id, invocation_id)) = creation_source
    {
        crate::agent_work_creations::record(
            tx,
            crate::agent_work_creations::Creation {
                actor: command.actor,
                reference: AgentWorkReference {
                    kind: AgentWorkKind::Schedule,
                    id: item.schedule_id,
                    agent_id: item.agent_id,
                },
                run_id,
                invocation_id,
                revision: item.revision,
                title: &item.definition.title,
                created_at: command.now,
            },
        )
        .await
        .map_err(|_| Error::Unavailable)?;
    }
    Ok(item)
}

async fn authorize(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
) -> Result<uuid::Uuid, Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::NotFound);
    }
    sqlx::query_scalar("SELECT a.dialogue_id FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND a.status='active' FOR UPDATE OF s")
        .bind(agent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(tx).await.map_err(db)?.ok_or(Error::NotFound)
}
impl OwnerScheduleStore for PostgresOwnerScheduleStore {
    fn list(
        &self,
        actor: Actor,
        agent: AgentId,
    ) -> ApplicationFuture<'_, Result<Vec<OwnerSchedule>, Error>> {
        Box::pin(async move {
            // Read-write: authorization takes the Space row lock (FOR UPDATE).
            let mut tx = self.write_unit_of_work().await?;
            authorize(&mut tx, actor, agent).await?;
            let rows=sqlx::query("SELECT * FROM agent.owner_schedules WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND source_run_id IS NULL AND status<>'deleted' ORDER BY created_at_ms DESC,schedule_id LIMIT 100")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
            let mut items = rows.iter().map(row).collect::<Result<Vec<_>, _>>()?;
            let mut histories = dispatch::histories(
                &mut tx,
                &items.iter().map(|v| v.schedule_id).collect::<Vec<_>>(),
            )
            .await?;
            for item in &mut items {
                item.recent_runs = histories.remove(&item.schedule_id).unwrap_or_default();
            }
            tx.commit().await.map_err(transaction)?;
            Ok(items)
        })
    }
    fn change(
        &self,
        command: ChangeOwnerSchedule,
    ) -> ApplicationFuture<'_, Result<OwnerSchedule, Error>> {
        Box::pin(self.change_inner(command))
    }
}
