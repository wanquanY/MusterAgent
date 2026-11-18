//! Bounded account projection over original owners, with one repeatable read.
//! Root selection never exposes internal children or schedule-generated tasks.
use crate::{PostgresDatabaseRuntime, PostgresTransactionIsolation};
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use sqlx::{PgConnection, Row};
use std::collections::HashMap;
type Error = DialogueStoreError;

const ROOTS: &str = "WITH task_members AS (
 SELECT t.work_item_id,t.created_at_ms,t.state_revision,coalesce(f.chain_root_id,t.work_item_id) chain_id,coalesce(f.chain_sequence,1) chain_sequence
 FROM collaboration.work_items t
 LEFT JOIN collaboration.work_item_followups f ON f.work_item_id=t.work_item_id
 JOIN agent.dialogue_user_intents i ON i.intent_id=t.source_intent_id
 WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.agent_id=$3 AND t.parent_work_item_id IS NULL
   AND t.erasure_job_id IS NULL AND i.origin_surface<>'schedule'
), task_display AS (
 SELECT DISTINCT ON(chain_id) chain_id,work_item_id,created_at_ms FROM task_members ORDER BY chain_id,chain_sequence
), task_current AS (
 SELECT DISTINCT ON(chain_id) chain_id,work_item_id,state_revision FROM task_members ORDER BY chain_id,chain_sequence DESC
), roots AS (
 SELECT 1::int kind,1::int category,t.work_item_id id,t.created_at_ms,r.status IN ('completed','superseded','cancelled') finished
 FROM task_display t JOIN task_current h USING(chain_id)
 JOIN collaboration.work_item_revisions r ON r.work_item_id=h.work_item_id AND r.state_revision=h.state_revision
 UNION ALL
 SELECT 2::int,2::int,s.schedule_id,s.created_at_ms,s.status='completed'
 FROM agent.owner_schedules s
 WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.source_run_id IS NULL AND s.status<>'deleted'
 UNION ALL
 SELECT 3::int,CASE WHEN g.mode=2 THEN 3 ELSE 1 END,g.goal_id,g.created_at_ms,g.status IN (4,5,6)
 FROM runtime.standing_goals g
 WHERE g.tenant_id=$1 AND g.owner_principal_id=$2 AND g.target_agent_id=$3
 AND EXISTS (SELECT 1 FROM agent.dialogue_sessions p
   WHERE p.tenant_id=g.tenant_id AND p.owner_id=g.owner_principal_id
     AND p.agent_id=g.target_agent_id AND p.source_goal_id=g.goal_id
     AND p.conversation_id=g.conversation_id AND p.subject_kind='goal')
 AND NOT EXISTS (SELECT 1 FROM runtime.goal_deletions d WHERE d.tenant_id=g.tenant_id AND d.goal_id=g.goal_id)
 AND ($10::bool OR NOT EXISTS (SELECT 1 FROM agent.agent_work_creation_publications c
   JOIN agent.owner_goal_setup_deliveries d USING(creation_id)
   WHERE c.tenant_id=g.tenant_id AND c.owner_id=g.owner_principal_id AND c.agent_id=g.target_agent_id
     AND c.work_kind=3 AND c.work_id=g.goal_id AND d.ready_at_ms IS NULL
     AND NOT EXISTS(SELECT 1 FROM agent.owner_goal_draft_registrations r WHERE r.creation_id=c.creation_id)))
), selected AS (SELECT * FROM roots WHERE created_at_ms<=$4
 AND ($5::int IS NULL OR kind=$5) AND ($6 OR NOT finished)
 AND ($7::uuid IS NULL OR (id=$7 AND kind=$8))
 AND ($9::int IS NULL OR category=$9))";

#[derive(Clone)]
pub struct PostgresAgentWorkQuery {
    database: PostgresDatabaseRuntime,
}
impl PostgresAgentWorkQuery {
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
    /// Internal Goal Tools must inspect and finish their own setup draft. This
    /// is not exposed through the user directory trait or its HTTP/RPC schema.
    pub(crate) async fn read_for_goal_tools(
        &self,
        r: ReadAgentWork,
    ) -> Result<AgentWorkPage, Error> {
        if r.kind != Some(AgentWorkKind::Goal) {
            return Err(Error::Unavailable);
        }
        self.read_inner(r, true).await
    }
    async fn read_inner(
        &self,
        r: ReadAgentWork,
        include_setup: bool,
    ) -> Result<AgentWorkPage, Error> {
        if r.actor.principal_kind != PrincipalKind::User
            || !(1..=50).contains(&r.limit)
            || r.reference
                .is_some_and(|v| v.id.is_nil() || v.agent_id != r.agent_id)
            || (r.reference.is_some() && r.cursor.is_some())
        {
            return Err(Error::Unavailable);
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        let available: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent.owned_agents a
            JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2
            AND s.owner_id=$3 AND a.status IN ('active','archived'))",
        )
        .bind(r.agent_id.into_uuid())
        .bind(r.actor.tenant_id.into_uuid())
        .bind(r.actor.principal_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !available {
            return Err(Error::Unavailable);
        }
        let now: i64 =
            sqlx::query_scalar("SELECT (extract(epoch FROM transaction_timestamp())*1000)::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        let through = r.cursor.map_or(now, |v| v.through.unix_millis());
        if through < 0
            || through > now
            || r.cursor.is_some_and(|c| {
                c.before_id.is_nil()
                    || c.before_created_at.unix_millis() < 0
                    || c.before_created_at.unix_millis() > through
            })
        {
            return Err(Error::RevisionConflict);
        }
        let count_sql = format!("{ROOTS} SELECT count(*) FROM selected");
        let count: i64 = bind_filter(sqlx::query_scalar(&count_sql), &r, through, include_setup)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let sql = format!(
            "{ROOTS} SELECT kind,id,created_at_ms FROM selected
            WHERE ($11::bigint IS NULL OR (created_at_ms,kind,id)<($11,$12,$13))
            ORDER BY created_at_ms DESC,kind DESC,id DESC LIMIT $14"
        );
        let rows = sqlx::query(&sql)
            .bind(r.actor.tenant_id.into_uuid())
            .bind(r.actor.principal_id.into_uuid())
            .bind(r.agent_id.into_uuid())
            .bind(through)
            .bind(r.kind.map(|v| v as i32))
            .bind(r.include_finished || r.reference.is_some())
            .bind(r.reference.map(|v| v.id))
            .bind(r.reference.map(|v| v.kind as i32))
            .bind(r.category.map(|v| v as i32))
            .bind(include_setup)
            .bind(r.cursor.map(|v| v.before_created_at.unix_millis()))
            .bind(r.cursor.map(|v| v.before_kind as i32))
            .bind(r.cursor.map(|v| v.before_id))
            .bind(i64::from(r.limit) + 1)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        let more = rows.len() > usize::from(r.limit);
        let roots = rows
            .iter()
            .take(usize::from(r.limit))
            .map(|row| {
                Ok((
                    kind(row.try_get("kind").map_err(db)?)?,
                    row.try_get::<uuid::Uuid, _>("id").map_err(db)?,
                    row.try_get::<i64, _>("created_at_ms").map_err(db)?,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut records = load_records(&mut tx, &r, &roots).await?;
        attach_task_chains(&mut tx, &r, &mut records).await?;
        let items = roots
            .iter()
            .map(|(kind, id, _)| records.remove(&(*kind, *id)).ok_or(Error::Corrupt))
            .collect::<Result<Vec<_>, _>>()?;
        let next = if more {
            roots.last().map(|(kind, id, at)| AgentWorkCursor {
                through: UtcTimestamp::from_unix_millis(through),
                before_created_at: UtcTimestamp::from_unix_millis(*at),
                before_kind: *kind,
                before_id: *id,
            })
        } else {
            None
        };
        let task_ids = items
            .iter()
            .filter_map(|record| match record {
                AgentWorkRecord::WorkItem(task) => Some(task.core.work_item_id.into_uuid()),
                AgentWorkRecord::WorkItemChain { current, .. } => {
                    Some(current.core.work_item_id.into_uuid())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let workspace_unavailable=sqlx::query_scalar::<_,uuid::Uuid>("SELECT DISTINCT w.work_item_id FROM collaboration.work_items w
            JOIN collaboration.work_item_revisions r ON r.work_item_id=w.work_item_id AND r.state_revision=w.state_revision
            JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=w.work_item_id AND x.goal_revision=r.goal_revision AND x.control_revision=r.control_revision AND x.assignment_epoch=r.assignment_epoch
            JOIN collaboration.work_item_outbox q ON q.work_item_id=x.work_item_id AND q.state_revision=x.accepted_state_revision
            WHERE w.work_item_id=ANY($1) AND w.tenant_id=$2 AND w.owner_id=$3 AND w.agent_id=$4 AND w.erasure_job_id IS NULL AND r.status='ready' AND q.event_kind='execution_requested' AND q.delivered_at_ms IS NULL AND q.failure_code='workspace_unavailable'")
            .bind(task_ids).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.agent_id.into_uuid())
            .fetch_all(&mut *tx).await.map_err(db)?.into_iter().map(|id|WorkItemId::from_uuid(id).map_err(|_|Error::Corrupt)).collect::<Result<Vec<_>,_>>()?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(AgentWorkPage {
            workspace_unavailable,
            agent_id: r.agent_id,
            records: items,
            total_count: u64::try_from(count).map_err(|_| Error::Corrupt)?,
            next,
            observed_at: UtcTimestamp::from_unix_millis(now),
        })
    }
}

fn bind_filter<'a>(
    q: sqlx::query::QueryScalar<'a, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    r: &ReadAgentWork,
    through: i64,
    include_setup: bool,
) -> sqlx::query::QueryScalar<'a, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
    q.bind(r.actor.tenant_id.into_uuid())
        .bind(r.actor.principal_id.into_uuid())
        .bind(r.agent_id.into_uuid())
        .bind(through)
        .bind(r.kind.map(|v| v as i32))
        .bind(r.include_finished || r.reference.is_some())
        .bind(r.reference.map(|v| v.id))
        .bind(r.reference.map(|v| v.kind as i32))
        .bind(r.category.map(|v| v as i32))
        .bind(include_setup)
}
fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
fn kind(v: i32) -> Result<AgentWorkKind, Error> {
    match v {
        1 => Ok(AgentWorkKind::WorkItem),
        2 => Ok(AgentWorkKind::Schedule),
        3 => Ok(AgentWorkKind::Goal),
        _ => Err(Error::Corrupt),
    }
}

async fn load_records(
    tx: &mut PgConnection,
    r: &ReadAgentWork,
    roots: &[(AgentWorkKind, uuid::Uuid, i64)],
) -> Result<HashMap<(AgentWorkKind, uuid::Uuid), AgentWorkRecord>, Error> {
    let mut output = HashMap::new();
    let ids = |kind| {
        roots
            .iter()
            .filter(|v| v.0 == kind)
            .map(|v| v.1)
            .collect::<Vec<_>>()
    };
    let work_ids = ids(AgentWorkKind::WorkItem);
    if !work_ids.is_empty() {
        let sql=format!("{} WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.agent_id=$3
            AND t.work_item_id=ANY($4) AND t.state_revision=r.state_revision AND t.erasure_job_id IS NULL",crate::work_item_authority::SELECT);
        for row in sqlx::query(&sql)
            .bind(r.actor.tenant_id.into_uuid())
            .bind(r.actor.principal_id.into_uuid())
            .bind(r.agent_id.into_uuid())
            .bind(&work_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?
        {
            let value =
                crate::work_item_authority::decode_record(&row).map_err(|_| Error::Corrupt)?;
            output.insert(
                (AgentWorkKind::WorkItem, value.core.work_item_id.into_uuid()),
                AgentWorkRecord::WorkItem(value),
            );
        }
    }
    let schedule_ids = ids(AgentWorkKind::Schedule);
    if !schedule_ids.is_empty() {
        let mut histories = crate::owner_schedule::dispatch::histories(tx, &schedule_ids)
            .await
            .map_err(|_| Error::Infrastructure)?;
        for row in sqlx::query("SELECT * FROM agent.owner_schedules WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND source_run_id IS NULL AND schedule_id=ANY($4)")
            .bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.agent_id.into_uuid())
            .bind(&schedule_ids).fetch_all(&mut *tx).await.map_err(db)? {
            let mut value=crate::owner_schedule::codec::row(&row).map_err(|_|Error::Corrupt)?;
            value.recent_runs=histories.remove(&value.schedule_id).unwrap_or_default();
            output.insert((AgentWorkKind::Schedule,value.schedule_id),AgentWorkRecord::Schedule {
                value,created_at:UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
                updated_at:UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms").map_err(db)?),
            });
        }
    }
    let goal_ids = ids(AgentWorkKind::Goal);
    if !goal_ids.is_empty() {
        let sql = format!(
            "{} WHERE goal.tenant_id=$1 AND goal.owner_principal_id=$2 AND goal.target_agent_id=$3 AND goal.goal_id=ANY($4)",
            crate::standing_goal::GOAL_SELECT
        );
        let goals = sqlx::query(&sql)
            .bind(r.actor.tenant_id.into_uuid())
            .bind(r.actor.principal_id.into_uuid())
            .bind(r.agent_id.into_uuid())
            .bind(&goal_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?
            .iter()
            .map(crate::standing_goal::row_decode::decode_goal)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Error::Corrupt)?;
        // Pausing clears active_pursuit_id in the original Goal owner. Keep the
        // latest Pursuit visible so a paused definition does not lose its work
        // state or the revision required by the original resume command.
        let sql = format!(
            "{} WHERE pursuit.tenant_id=$1 AND pursuit.goal_id=ANY($2)
            AND pursuit.pursuit_id=(SELECT latest.pursuit_id FROM runtime.goal_pursuits latest
                WHERE latest.tenant_id=pursuit.tenant_id AND latest.goal_id=pursuit.goal_id
                ORDER BY latest.started_at_ms DESC,latest.pursuit_id DESC LIMIT 1)",
            crate::standing_goal::PURSUIT_SELECT
        );
        let mut pursuits = HashMap::new();
        for row in sqlx::query(&sql)
            .bind(r.actor.tenant_id.into_uuid())
            .bind(&goal_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?
        {
            let value = crate::standing_goal::row_decode::decode_pursuit(&row)
                .map_err(|_| Error::Corrupt)?;
            pursuits.insert(value.goal_id, value);
        }
        for value in goals {
            let pursuit = pursuits.remove(&value.goal_id);
            if value
                .active_pursuit_id
                .is_some_and(|id| pursuit.as_ref().is_none_or(|p| p.pursuit_id != id))
            {
                return Err(Error::Corrupt);
            }
            output.insert(
                (AgentWorkKind::Goal, value.goal_id.into_uuid()),
                AgentWorkRecord::Goal { value, pursuit },
            );
        }
    }
    Ok(output)
}

impl AgentWorkQuery for PostgresAgentWorkQuery {
    fn read(&self, request: ReadAgentWork) -> ApplicationFuture<'_, Result<AgentWorkPage, Error>> {
        Box::pin(self.read_inner(request, false))
    }
}

/// One batch, independent of chain depth. The original link's indexed sequence
/// selects the current action; old result references keep their original owner.
async fn attach_task_chains(
    tx: &mut PgConnection,
    r: &ReadAgentWork,
    records: &mut HashMap<(AgentWorkKind, uuid::Uuid), AgentWorkRecord>,
) -> Result<(), Error> {
    let roots = records
        .keys()
        .filter(|(kind, _)| *kind == AgentWorkKind::WorkItem)
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    if roots.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "{} JOIN (SELECT display.work_item_id AS display_id,head.work_item_id,head.chain_sequence FROM collaboration.work_items display LEFT JOIN collaboration.work_item_followups df ON df.work_item_id=display.work_item_id JOIN LATERAL (SELECT f.work_item_id,f.chain_sequence FROM collaboration.work_item_followups f JOIN collaboration.work_items alive ON alive.work_item_id=f.work_item_id AND alive.erasure_job_id IS NULL WHERE f.chain_root_id=coalesce(df.chain_root_id,display.work_item_id) ORDER BY f.chain_sequence DESC LIMIT 1) head ON true WHERE display.work_item_id=ANY($4)) f ON f.work_item_id=t.work_item_id WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.agent_id=$3 AND t.erasure_job_id IS NULL AND t.state_revision=r.state_revision",
        crate::work_item_authority::SELECT.replacen(
            "SELECT t.*",
            "SELECT f.display_id,f.chain_sequence,t.*",
            1
        )
    );
    let rows = sqlx::query(&sql)
        .bind(r.actor.tenant_id.into_uuid())
        .bind(r.actor.principal_id.into_uuid())
        .bind(r.agent_id.into_uuid())
        .bind(roots)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
    for row in rows {
        let current =
            crate::work_item_authority::decode_record(&row).map_err(|_| Error::Corrupt)?;
        let root_id = row.try_get::<uuid::Uuid, _>("display_id").map_err(db)?;
        let sequence = u64::try_from(row.try_get::<i64, _>("chain_sequence").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
        let Some(AgentWorkRecord::WorkItem(root)) =
            records.remove(&(AgentWorkKind::WorkItem, root_id))
        else {
            return Err(Error::Corrupt);
        };
        records.insert(
            (AgentWorkKind::WorkItem, root_id),
            AgentWorkRecord::WorkItemChain {
                root,
                current,
                sequence,
            },
        );
    }
    Ok(())
}
