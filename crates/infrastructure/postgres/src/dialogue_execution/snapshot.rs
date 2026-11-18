//! Per-Run Dialogue authority comes from the original immutable Snapshot. Goal
//! sessions retain routing identity only, so they cannot carry mutable Run pins.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_snapshot::{OwnerGoalSnapshotAuthority, SnapshotCodec};

pub(crate) async fn goal_authority(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    run: RunId,
) -> Result<OwnerGoalSnapshotAuthority, DialogueStoreError> {
    let row = sqlx::query("SELECT snapshot.encoded_snapshot,snapshot.content_hash,s.source_goal_id,s.goal_binding_revision,s.dialogue_id
        FROM runtime.runs r JOIN capability.runtime_snapshots snapshot
          ON snapshot.runtime_snapshot_id=r.runtime_snapshot_id AND snapshot.tenant_id=r.tenant_id
          AND snapshot.conversation_id=r.conversation_id AND snapshot.run_id=r.run_id
        JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
        WHERE r.tenant_id=$1 AND r.run_id=$2 AND s.owner_id=$3 AND s.agent_id=$4
          AND s.subject_kind='goal' AND s.purpose='goal_execution' AND r.erasure_job_id IS NULL")
        .bind(actor.tenant_id.into_uuid()).bind(run.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
    decode_goal_authority(&row, actor, agent)
}

/// Shared proof verification. Callers select visibility separately: active
/// execution excludes erased runs; historical usage retains financial identity.
pub(crate) fn decode_goal_authority(
    row: &sqlx::postgres::PgRow,
    actor: Actor,
    agent: AgentId,
) -> Result<OwnerGoalSnapshotAuthority, DialogueStoreError> {
    let bytes: Vec<u8> = row.try_get("encoded_snapshot").map_err(db)?;
    if Sha256::digest(&bytes).into_bytes().as_slice()
        != row
            .try_get::<Vec<u8>, _>("content_hash")
            .map_err(db)?
            .as_slice()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_content(&bytes)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let authority = snapshot
        .owner_goal_authority
        .ok_or(DialogueStoreError::Corrupt)?;
    if actor.principal_kind != PrincipalKind::User
        || authority.context_cut.tenant_id != actor.tenant_id
        || authority.context_cut.owner_id != actor.principal_id
        || authority.context_cut.agent_id != agent
        || snapshot.agent.agent_id != agent
        || authority.context_cut.dialogue_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("dialogue_id").map_err(db)?
        || authority.goal_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("source_goal_id").map_err(db)?
        || authority.binding_revision
            != unsigned(row.try_get("goal_binding_revision").map_err(db)?)?
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(authority)
}
