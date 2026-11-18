//! Original owner receipts are attached once by the original Publication owner.
//! No matching of model prose, titles, timestamps or adjacent messages is used.
pub(crate) mod readiness;
pub(crate) mod visibility;
use musteragent_identity::*;
use musteragent_runtime_session::*;
use sqlx::Row;
type Error = DialogueStoreError;

pub(crate) struct Creation<'a> {
    pub actor: Actor,
    pub reference: AgentWorkReference,
    pub run_id: RunId,
    pub invocation_id: InvocationId,
    pub revision: u64,
    pub title: &'a str,
    pub created_at: UtcTimestamp,
}

pub(crate) async fn record(tx: &mut sqlx::PgConnection, c: Creation<'_>) -> Result<(), Error> {
    record_at_home(tx, c, false).await
}
pub(crate) async fn record_device(
    tx: &mut sqlx::PgConnection,
    c: Creation<'_>,
) -> Result<(), Error> {
    record_at_home(tx, c, true).await
}
async fn record_at_home(
    tx: &mut sqlx::PgConnection,
    c: Creation<'_>,
    device: bool,
) -> Result<(), Error> {
    let retained: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.agent_work_creation_publications WHERE tenant_id=$1 AND source_run_id=$2 AND entry_id IS NULL")
        .bind(c.actor.tenant_id.into_uuid()).bind(c.run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if retained >= 32 {
        return Err(Error::RevisionConflict);
    }
    let title: String = c
        .title
        .lines()
        .next()
        .unwrap_or(c.title)
        .chars()
        .take(120)
        .collect();
    let category = match c.reference.kind {
        AgentWorkKind::WorkItem => AgentWorkCategory::Once,
        AgentWorkKind::Schedule => AgentWorkCategory::Scheduled,
        AgentWorkKind::Goal => AgentWorkCategory::Standing,
    };
    sqlx::query("INSERT INTO agent.agent_work_creation_publications
        (creation_id,tenant_id,owner_id,agent_id,source_run_id,source_invocation_id,work_kind,work_id,definition_revision,category,title,created_at_ms,execution_home,device_source_invocation_id)
        VALUES($1,$2,$3,$4,$5,CASE WHEN $12 THEN NULL ELSE $1 END,$6,$7,$8,$9,$10,$11,CASE WHEN $12 THEN 'device' ELSE 'cloud' END,CASE WHEN $12 THEN $1 ELSE NULL END)")
        .bind(c.invocation_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid())
        .bind(c.reference.agent_id.into_uuid()).bind(c.run_id.into_uuid()).bind(c.reference.kind as i16)
        .bind(c.reference.id).bind(i64::try_from(c.revision).map_err(|_|Error::Corrupt)?)
        .bind(category as i16).bind(title).bind(c.created_at.unix_millis()).bind(device)
        .execute(&mut *tx).await.map_err(db)?;
    if c.reference.kind == AgentWorkKind::Goal {
        sqlx::query(
            "INSERT INTO agent.owner_goal_setup_deliveries(creation_id)
            SELECT $1 FROM runtime.standing_goals g WHERE g.goal_id=$2 AND g.tenant_id=$3
              AND g.owner_principal_id=$4 AND g.target_agent_id=$5 AND g.mode=2",
        )
        .bind(c.invocation_id.into_uuid())
        .bind(c.reference.id)
        .bind(c.actor.tenant_id.into_uuid())
        .bind(c.actor.principal_id.into_uuid())
        .bind(c.reference.agent_id.into_uuid())
        .execute(tx)
        .await
        .map_err(db)?;
    }
    Ok(())
}

pub(crate) async fn publish(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    run: RunId,
    entry: DialogueEntryId,
) -> Result<(), Error> {
    // The Run commit lane serializes creation and publication. Recovery reads
    // the existing delivery, so a later reply cannot claim the receipt twice.
    sqlx::query("UPDATE agent.agent_work_creation_publications SET entry_id=$5
        WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND source_run_id=$4 AND entry_id IS NULL")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid())
        .bind(run.into_uuid()).bind(entry.into_uuid()).execute(tx).await.map_err(db)?;
    Ok(())
}

pub(crate) async fn attach<'a>(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    entries: impl Iterator<Item = &'a mut DialogueHistoryEntry>,
) -> Result<(), Error> {
    let mut entries: Vec<_> = entries
        .filter(|v| {
            matches!(
                v.entry.content,
                musteragent_execution_context::AgentDialogueContent::AgentMessage { .. }
            )
        })
        .collect();
    if entries.is_empty() {
        return Ok(());
    }
    let ids: Vec<_> = entries
        .iter()
        .map(|v| v.entry.entry_id.into_uuid())
        .collect();
    let rows = sqlx::query("SELECT c.*, CASE c.work_kind
        WHEN 1 THEN EXISTS(SELECT 1 FROM collaboration.work_items w WHERE w.work_item_id=c.work_id
          AND w.tenant_id=c.tenant_id AND w.owner_id=c.owner_id AND w.agent_id=c.agent_id AND w.erasure_job_id IS NULL)
        WHEN 2 THEN EXISTS(SELECT 1 FROM agent.owner_schedules s WHERE s.schedule_id=c.work_id
          AND s.tenant_id=c.tenant_id AND s.owner_id=c.owner_id AND s.agent_id=c.agent_id AND s.status<>'deleted')
        WHEN 3 THEN EXISTS(SELECT 1 FROM runtime.standing_goals g
          JOIN agent.dialogue_sessions s ON s.source_goal_id=g.goal_id AND s.tenant_id=g.tenant_id
            AND s.owner_id=g.owner_principal_id AND s.conversation_id=g.conversation_id AND s.subject_kind='goal'
          WHERE g.goal_id=c.work_id AND g.tenant_id=c.tenant_id AND g.owner_principal_id=c.owner_id
            AND g.target_agent_id=c.agent_id AND s.agent_id=c.agent_id AND g.mode=2
            AND NOT EXISTS(SELECT 1 FROM runtime.goal_deletions d WHERE d.tenant_id=g.tenant_id AND d.goal_id=g.goal_id))
        ELSE false END AS available FROM agent.agent_work_creation_publications c
        WHERE c.entry_id=ANY($1) AND c.tenant_id=$2 AND c.owner_id=$3
          AND NOT EXISTS(SELECT 1 FROM agent.owner_goal_setup_deliveries d WHERE d.creation_id=c.creation_id AND d.ready_at_ms IS NULL
            AND NOT EXISTS(SELECT 1 FROM agent.owner_goal_draft_registrations r WHERE r.creation_id=c.creation_id))
        ORDER BY c.created_at_ms,c.creation_id")
        .bind(ids).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_all(tx).await.map_err(db)?;
    for row in rows {
        let entry_id: uuid::Uuid = row.try_get("entry_id").map_err(db)?;
        let Some(entry) = entries
            .iter_mut()
            .find(|v| v.entry.entry_id.into_uuid() == entry_id)
        else {
            return Err(Error::Corrupt);
        };
        let available: bool = row.try_get("available").map_err(db)?;
        let kind = match row.try_get::<i16, _>("work_kind").map_err(db)? {
            1 => AgentWorkKind::WorkItem,
            2 => AgentWorkKind::Schedule,
            3 => AgentWorkKind::Goal,
            _ => return Err(Error::Corrupt),
        };
        let category = match row.try_get::<i16, _>("category").map_err(db)? {
            1 => AgentWorkCategory::Once,
            2 => AgentWorkCategory::Scheduled,
            3 => AgentWorkCategory::Standing,
            _ => return Err(Error::Corrupt),
        };
        entry.work_creations.push(AgentWorkCreation {
            creation_id: row.try_get("creation_id").map_err(db)?,
            reference: AgentWorkReference {
                kind,
                id: row.try_get("work_id").map_err(db)?,
                agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
            },
            revision: u64::try_from(row.try_get::<i64, _>("definition_revision").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            title: if available {
                row.try_get("title").map_err(db)?
            } else {
                String::new()
            },
            category,
            available,
        });
    }
    Ok(())
}
fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
