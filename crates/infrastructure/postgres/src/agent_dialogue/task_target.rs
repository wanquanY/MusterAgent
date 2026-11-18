use super::*;
use musteragent_persistence_codec::dialogue_codec;
use prost::Message;

pub(super) async fn authorize(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    dialogue: &AgentDialogue,
    target: Option<AgentTaskTarget>,
) -> Result<(), Error> {
    let Some(target) = target else { return Ok(()) };
    target.validate().map_err(|_| Error::Corrupt)?;
    let revisions = sqlx::query(
        "SELECT r.goal_revision, r.control_revision
         FROM collaboration.work_items w
         JOIN collaboration.work_item_revisions r
           ON r.work_item_id=w.work_item_id AND r.state_revision=w.state_revision
         WHERE w.work_item_id=$1 AND w.tenant_id=$2 AND w.owner_id=$3
           AND w.agent_id=$4 AND w.source_dialogue_id=$5 AND w.erasure_job_id IS NULL
         FOR SHARE OF w",
    )
    .bind(target.work_item_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(dialogue.agent_id.into_uuid())
    .bind(dialogue.dialogue_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    if unsigned(revisions.try_get("goal_revision").map_err(db)?)? != target.goal_revision
        || unsigned(revisions.try_get("control_revision").map_err(db)?)? != target.control_revision
    {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}

pub(super) fn encode(target: Option<AgentTaskTarget>) -> Option<Vec<u8>> {
    target.map(|value| dialogue_codec::task_target(value).encode_to_vec())
}
