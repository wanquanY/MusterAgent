use super::*;
use crate::dialogue_codec::content_from_bytes;
use musteragent_identity::{DialogueContextCheckpointId, InvocationId, RunId, VersionedKey};

pub(crate) async fn select(
    tx: &mut PgConnection,
    cut: &DialogueContextCut,
    run: Option<RunId>,
    bound_through: u64,
    policy: &VersionedKey,
) -> Result<Option<DialogueContextCheckpoint>, Error> {
    let row=sqlx::query("SELECT c.* FROM agent.dialogue_context_checkpoints c JOIN runtime.model_invocations m ON m.invocation_id=c.generator_invocation_id AND m.status=2 WHERE (to_jsonb(c)->>'erasure_job_id') IS NULL AND c.tenant_id=$1 AND c.owner_id=$2 AND c.agent_id=$3 AND c.dialogue_id=$4 AND c.epoch=$5 AND c.through_seq<=$6 AND ($7::uuid IS NULL OR m.run_id=$7) AND c.policy_ref=$8 ORDER BY c.through_seq DESC,c.created_at_ms DESC,c.checkpoint_id LIMIT 1")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(bound_through.min(cut.through.seq))?).bind(run.map(|id|id.into_uuid())).bind(policy.as_str()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let value = decode(&row)?;
    if value.coverage.source_chain_sha256 != chain_at(tx, cut, value.coverage.through.seq).await? {
        return Err(Error::Corrupt);
    }
    Ok(Some(value))
}

pub(crate) fn decode(row: &sqlx::postgres::PgRow) -> Result<DialogueContextCheckpoint, Error> {
    use musteragent_identity::{PrincipalId, TenantId};
    if row
        .try_get::<Option<uuid::Uuid>, _>("erasure_job_id")
        .ok()
        .flatten()
        .is_some()
    {
        return Err(Error::Unavailable);
    }
    macro_rules! id {
        ($t:ty,$key:literal) => {
            <$t>::from_uuid(row.try_get($key).map_err(db)?).map_err(|_| Error::Corrupt)?
        };
    }
    let value = DialogueContextCheckpoint {
        checkpoint_id: id!(DialogueContextCheckpointId, "checkpoint_id"),
        coverage: DialogueContextCut {
            tenant_id: id!(TenantId, "tenant_id"),
            owner_id: id!(PrincipalId, "owner_id"),
            agent_id: id!(AgentId, "agent_id"),
            dialogue_id: id!(AgentDialogueId, "dialogue_id"),
            through: DialogueCursor {
                epoch: unsigned(row.try_get("epoch").map_err(db)?)?,
                seq: unsigned(row.try_get("through_seq").map_err(db)?)?,
            },
            entry_count: unsigned(row.try_get("through_seq").map_err(db)?)?,
            source_chain_sha256: Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("source_chain_sha256")
                    .map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
        },
        parent_checkpoint_id: row
            .try_get::<Option<uuid::Uuid>, _>("parent_checkpoint_id")
            .map_err(db)?
            .map(DialogueContextCheckpointId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        summary_ref: content_from_bytes(row.try_get("summary_ref").map_err(db)?)?,
        source_manifest_ref: content_from_bytes(row.try_get("source_manifest_ref").map_err(db)?)?,
        generator_invocation_id: id!(InvocationId, "generator_invocation_id"),
        policy_ref: VersionedKey::new(row.try_get::<String, _>("policy_ref").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
    };
    value.validate().map_err(|_| Error::Corrupt)?;
    Ok(value)
}

pub(crate) async fn by_id(
    tx: &mut PgConnection,
    cut: &DialogueContextCut,
    id: DialogueContextCheckpointId,
) -> Result<DialogueContextCheckpoint, Error> {
    let row=sqlx::query("SELECT c.* FROM agent.dialogue_context_checkpoints c JOIN runtime.model_invocations m ON m.invocation_id=c.generator_invocation_id AND m.status=2 WHERE (to_jsonb(c)->>'erasure_job_id') IS NULL AND c.checkpoint_id=$1 AND c.tenant_id=$2 AND c.owner_id=$3 AND c.agent_id=$4 AND c.dialogue_id=$5 AND c.epoch=$6 AND c.through_seq<=$7")
        .bind(id.into_uuid()).bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(cut.through.seq)?).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let value = decode(&row)?;
    if value.coverage.source_chain_sha256 != chain_at(tx, cut, value.coverage.through.seq).await? {
        return Err(Error::Corrupt);
    }
    Ok(value)
}
