//! Bounded current observations of already-visible references in the frozen
//! public cut. Source hashes/checkpoints remain immutable and contain no state.
use super::*;
use crate::dialogue_publication::PostgresNativeDialogueInteractionStore;
use musteragent_execution_context::{DIALOGUE_APPROVAL_CONTEXT_LIMIT, DialogueApprovalContext};

pub(crate) async fn load(
    tx: &mut PgConnection,
    actor: Actor,
    cut: &DialogueContextCut,
) -> Result<DialogueApprovalContext, Error> {
    if load_cut(tx, actor, cut.agent_id, cut.dialogue_id, cut.through).await? != *cut {
        return Err(Error::RevisionConflict);
    }
    // Original immutable source membership and current erasure are both checked.
    // Return at most 65 candidates from this cut to detect truncation; actual
    // rows examined depend on the plan. Original owners decide visibility and
    // meaning through at most one batch per execution home, never per entry.
    let rows=sqlx::query("SELECT e.entry_id,e.seq,p.execution_home FROM agent.dialogue_context_source_entries s JOIN agent.agent_dialogue_entries e ON e.entry_id=s.entry_id AND e.tenant_id=s.tenant_id AND e.owner_id=s.owner_id AND e.dialogue_id=s.dialogue_id AND e.epoch=s.epoch AND e.seq=s.seq JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4 AND s.epoch=$5 AND s.seq<=$6 AND e.kind='interaction' AND e.erasure_job_id IS NULL ORDER BY s.seq DESC LIMIT $7")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid())
        .bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(cut.through.seq)?)
        .bind((DIALOGUE_APPROVAL_CONTEXT_LIMIT+1) as i64).fetch_all(&mut *tx).await.map_err(db)?;
    let selected = &rows[..rows.len().min(DIALOGUE_APPROVAL_CONTEXT_LIMIT)];
    let from_seq = if rows.len() > DIALOGUE_APPROVAL_CONTEXT_LIMIT {
        unsigned(
            selected
                .last()
                .ok_or(Error::Corrupt)?
                .try_get("seq")
                .map_err(db)?,
        )?
    } else {
        1
    };
    let mut cloud = Vec::new();
    let mut native = Vec::new();
    for row in selected {
        let entry: uuid::Uuid = row.try_get("entry_id").map_err(db)?;
        match row.try_get::<&str, _>("execution_home").map_err(db)? {
            "device" => native.push(entry),
            "cloud" => cloud.push(entry),
            _ => return Err(Error::Corrupt),
        }
    }
    let mut observations = crate::interaction_application::dialogue_context::observations(
        tx,
        actor,
        cut.dialogue_id,
        &cloud,
    )
    .await
    .map_err(|e| match e {
        crate::interaction::PostgresInteractionError::Sql(_) => Error::Infrastructure,
        _ => Error::Unavailable,
    })?;
    observations.extend(
        PostgresNativeDialogueInteractionStore::published_context_observations(
            tx,
            actor,
            cut.dialogue_id,
            &native,
        )
        .await
        .map_err(|e| match e.code {
            musteragent_runtime_contract::ErrorCode::DependencyUnavailable => Error::Infrastructure,
            _ => Error::Unavailable,
        })?,
    );
    observations.sort_by_key(|o| o.sequence);
    let result = DialogueApprovalContext {
        frozen_cut: cut.clone(),
        from_seq,
        observations,
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    Ok(result)
}
