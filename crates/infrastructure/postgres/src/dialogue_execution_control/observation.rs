//! SQL owns complete control sets; Rust only receives counts and bounded pages.
use super::*;
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ObservedTarget {
    pub id: uuid::Uuid,
    pub intent: uuid::Uuid,
    pub run: Option<uuid::Uuid>,
    pub conversation: Option<uuid::Uuid>,
    pub native_target: Option<Vec<u8>>,
}
pub(crate) struct ControlObservation {
    pub count: u32,
    pub pending: u32,
    pub locally_completed_unconfirmed: u32,
    pub unconfirmed_cancellations: u32,
    pub observed_cancellation_sequence: u64,
    pub changed: bool,
}
/// $1/$2/$3 are the authenticated tenant/owner/Agent. Native custody uses its
/// original owner's relation, including custody without a grant or admission ACK.
pub(super) fn candidates() -> String {
    let cloud = include_str!("foreground_targets.sql")
        .trim()
        .trim_end_matches("ORDER BY r.run_id");
    let accepted = include_str!("accepted_targets.sql")
        .trim()
        .trim_end_matches("ORDER BY i.intent_id");
    format!(
        "WITH native_open AS MATERIALIZED (SELECT * FROM agent.native_execution_control_candidates WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3), native_locally_completed AS MATERIALIZED (SELECT n.* FROM native_open n JOIN agent.native_execution_activity a ON a.run_id=n.run_id AND a.tenant_id=n.tenant_id AND a.owner_id=n.owner_id AND a.agent_id=n.agent_id AND a.dialogue_id=n.dialogue_id AND a.state=4), candidates AS MATERIALIZED (SELECT target_id,intent_id,run_id,conversation_id,native_target FROM ({cloud}) cloud UNION ALL SELECT n.target_id,n.intent_id,n.run_id,n.conversation_id,n.native_target FROM native_open n WHERE NOT EXISTS(SELECT 1 FROM native_locally_completed f WHERE f.run_id=n.run_id) UNION ALL SELECT intent_id AS target_id,intent_id,NULL::uuid AS run_id,NULL::uuid AS conversation_id,NULL::bytea AS native_target FROM ({accepted}) accepted)"
    )
}
pub(crate) async fn current(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    revision: Option<u64>,
) -> Result<ControlObservation, DialogueStoreError> {
    source::retain_missing(tx, actor, agent).await?;
    let query = format!(
        "{}, previous AS (SELECT target_id,intent_id,run_id,conversation_id,native_target FROM agent.dialogue_execution_control_targets WHERE agent_id=$3 AND activity_revision=$4) SELECT count(*) AS count,count(DISTINCT target_id) AS identities,count(*) FILTER(WHERE EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_targets t JOIN agent.dialogue_execution_cancellations c ON c.command_id=t.cancellation_id WHERE c.agent_id=$3 AND t.intent_id=candidates.intent_id)) AS pending, (SELECT count(*) FROM native_locally_completed) AS locally_completed_unconfirmed, (SELECT count(*) FROM native_locally_completed f WHERE EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_targets t JOIN agent.dialogue_execution_cancellations c ON c.command_id=t.cancellation_id WHERE c.tenant_id=$1 AND c.owner_id=$2 AND c.agent_id=$3 AND t.intent_id=f.intent_id AND t.run_id=f.run_id)) AS unconfirmed_cancellations, (SELECT coalesce(max(c.cancellation_sequence),0) FROM agent.dialogue_execution_cancellations c WHERE c.agent_id=$3 AND c.tenant_id=$1 AND c.owner_id=$2) AS observed_cancellation_sequence, NOT EXISTS(SELECT 1 FROM agent.dialogue_execution_control_snapshots WHERE agent_id=$3 AND activity_revision=$4) OR EXISTS((SELECT * FROM candidates EXCEPT SELECT * FROM previous) UNION ALL (SELECT * FROM previous EXCEPT SELECT * FROM candidates)) AS changed FROM candidates",
        candidates()
    );
    let row = sqlx::query(&query)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid())
        .bind(revision.map(integer).transpose()?)
        .fetch_one(tx)
        .await
        .map_err(db)?;
    let count: i64 = row.try_get("count").map_err(db)?;
    if count != row.try_get::<i64, _>("identities").map_err(db)? {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(ControlObservation {
        count: u32::try_from(count).map_err(|_| DialogueStoreError::Corrupt)?,
        pending: u32::try_from(row.try_get::<i64, _>("pending").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        locally_completed_unconfirmed: u32::try_from(
            row.try_get::<i64, _>("locally_completed_unconfirmed")
                .map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
        unconfirmed_cancellations: u32::try_from(
            row.try_get::<i64, _>("unconfirmed_cancellations")
                .map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
        observed_cancellation_sequence: unsigned(
            row.try_get("observed_cancellation_sequence").map_err(db)?,
        )?,
        changed: row.try_get("changed").map_err(db)?,
    })
}
pub(crate) fn decode(row: &sqlx::postgres::PgRow) -> Result<ObservedTarget, DialogueStoreError> {
    let value = ObservedTarget {
        id: row.try_get("target_id").map_err(db)?,
        intent: row.try_get("intent_id").map_err(db)?,
        run: row.try_get("run_id").map_err(db)?,
        conversation: row.try_get("conversation_id").map_err(db)?,
        native_target: row.try_get("native_target").map_err(db)?,
    };
    if value.run.is_none() != value.conversation.is_none()
        || (value.native_target.is_some() && value.run.is_none())
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(value)
}
pub(crate) async fn retain(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    dialogue: AgentDialogueId,
    revision: u64,
    at: i64,
    expected_count: u32,
) -> Result<(), DialogueStoreError> {
    // One statement materializes a coherent set and retains its exact count.
    let query = format!(
        "{}, snapshot AS (INSERT INTO agent.dialogue_execution_control_snapshots(agent_id,activity_revision,tenant_id,owner_id,dialogue_id,target_count,observed_at_ms) SELECT $3,$4,$1,$2,$5,count(*),$6 FROM candidates RETURNING agent_id,activity_revision), inserted AS (INSERT INTO agent.dialogue_execution_control_targets(agent_id,activity_revision,target_id,intent_id,run_id,conversation_id,native_target) SELECT snapshot.agent_id,snapshot.activity_revision,c.target_id,c.intent_id,c.run_id,c.conversation_id,c.native_target FROM candidates c CROSS JOIN snapshot RETURNING target_id) SELECT count(*) FROM inserted",
        candidates()
    );
    let count: i64 = sqlx::query_scalar(&query)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid())
        .bind(integer(revision)?)
        .bind(dialogue.into_uuid())
        .bind(at)
        .fetch_one(tx)
        .await
        .map_err(db)?;
    if count != i64::from(expected_count) {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
