use musteragent_content_ref::Sha256;
use musteragent_evolution::ExperienceCandidateOrigin;
use musteragent_runtime_memory::PromoteExperience;
use uuid::Uuid;

use crate::memory::PostgresMemoryEvolutionError;

pub(crate) async fn insert_evolution_publication_outbox(
    transaction: &mut sqlx::PgConnection,
    command: &PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    insert_evolution_publication_outbox_for_home(transaction, command, false).await
}

pub(crate) async fn insert_evolution_publication_outbox_for_home(
    transaction: &mut sqlx::PgConnection,
    command: &PromoteExperience,
    native: bool,
) -> Result<(), PostgresMemoryEvolutionError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema":"musteragent.legacy.evolution.published.v1",
        "proposal_id":command.candidate.proposal_id.to_string(),
        "evaluation_id":command.evaluation.evaluation_id.to_string(),
        "approval_interaction_id":command.approval.approval_interaction_id.to_string(),
        "memory_id":command.memory_record.memory_id.to_string(),
        "memory_version_id":command.memory_version.memory_version_id.to_string(),
        "replaced_memory_version_id":command.expected_current_version_id.map(|value| value.to_string()),
    }))?;
    let message_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("evolution-published:{}", command.promotion.promotion_id).as_bytes(),
    );
    if native || command.candidate.origin == ExperienceCandidateOrigin::UserDraft {
        sqlx::query(
            "INSERT INTO memory.governance_publication_outbox (
                 message_id,tenant_id,proposal_id,memory_id,memory_version_id,
                 topic,payload,payload_sha256,committed_at_ms
             ) VALUES ($1,$2,$3,$4,$5,'evolution.published.v1',$6,$7,$8)
             ON CONFLICT (message_id) DO NOTHING",
        )
        .bind(message_id)
        .bind(command.candidate.tenant_id.into_uuid())
        .bind(command.candidate.proposal_id.into_uuid())
        .bind(command.memory_record.memory_id.into_uuid())
        .bind(command.memory_version.memory_version_id.into_uuid())
        .bind(&payload)
        .bind(Sha256::digest(&payload).into_bytes().as_slice())
        .bind(command.promotion.promoted_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        return Ok(());
    }
    let source_run_id = command
        .candidate
        .source_run_ids
        .iter()
        .next()
        .ok_or(PostgresMemoryEvolutionError::EvolutionConflict)?;
    let conversation_id: Uuid = sqlx::query_scalar(
        "SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(command.candidate.tenant_id.into_uuid())
    .bind(source_run_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO event_store.outbox
         (message_id,tenant_id,conversation_id,topic,payload,payload_hash,
          committed_at_ms,available_at_ms)
         VALUES ($1,$2,$3,'evolution.published.v1',$4,$5,$6,$6)
         ON CONFLICT (message_id) DO NOTHING",
    )
    .bind(message_id)
    .bind(command.candidate.tenant_id.into_uuid())
    .bind(conversation_id)
    .bind(&payload)
    .bind(Sha256::digest(&payload).into_bytes().as_slice())
    .bind(command.promotion.promoted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn mark_draft_published(
    transaction: &mut sqlx::PgConnection,
    command: &PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    let Some(draft_id) = command.candidate.source_draft_id else {
        return Ok(());
    };
    let updated = sqlx::query(
        "UPDATE memory.agent_memory_drafts
         SET status=4,updated_at_ms=$1
         WHERE tenant_id=$2 AND draft_id=$3 AND proposal_id=$4 AND status=2",
    )
    .bind(command.promotion.promoted_at.unix_millis())
    .bind(command.candidate.tenant_id.into_uuid())
    .bind(draft_id.into_uuid())
    .bind(command.candidate.proposal_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    Ok(())
}
