use super::*;
use crate::memory::decode_candidate;
use musteragent_identity::{LedgerRecordId, RunId};

pub(super) async fn load_next_draft_candidate(
    store: &PostgresMemoryEvolutionStore,
    scope: AgentMemoryDraftEvaluationScope,
) -> Result<Option<PendingAgentMemoryDraftEvaluation>, PostgresMemoryEvolutionError> {
    let mut transaction = store
        .database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Memory,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await?;
    let row = sqlx::query(
        "SELECT draft.draft_id AS pending_draft_id,
                proposal.proposal_id,proposal.tenant_id,proposal.proposed_by,
                proposal.scope_kind,proposal.scope_id,proposal.target_memory_id,
                proposal.supersedes_proposal_id,proposal.proposal_origin,
                proposal.source_draft_id,proposal.proposal_kind,proposal.artifact_kind,
                proposal.status,proposal.confidence_bps,proposal.proposed_at_ms,
                content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash
         FROM memory.agent_memory_drafts AS draft
         JOIN evolution.proposals AS proposal
           ON proposal.proposal_id=draft.proposal_id
          AND proposal.source_draft_id=draft.draft_id
          AND proposal.tenant_id=draft.tenant_id
          AND proposal.proposed_by=draft.owner_principal_id
         LEFT JOIN artifact.content_refs AS content
           ON content.content_id=proposal.candidate_content_id
          AND content.tenant_id=proposal.tenant_id
          AND content.content_hash=proposal.candidate_content_hash
         WHERE draft.tenant_id=$1 AND draft.owner_principal_id=$2 AND draft.agent_id=$3
           AND draft.status=2 AND proposal.proposal_origin=3 AND proposal.status IN (1,2)
           AND ((proposal.scope_kind=1 AND proposal.scope_id=draft.owner_principal_id)
             OR (proposal.scope_kind=2 AND proposal.scope_id=draft.agent_id))
           AND NOT EXISTS (
             SELECT 1 FROM evolution.evaluations AS evaluation
             WHERE evaluation.proposal_id=proposal.proposal_id
           )
         ORDER BY draft.updated_at_ms,draft.draft_id LIMIT 1",
    )
    .bind(scope.tenant_id.into_uuid())
    .bind(scope.owner_principal_id.into_uuid())
    .bind(scope.agent_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        transaction.rollback().await?;
        return Ok(None);
    };
    let draft_id = parse_id(row.try_get("pending_draft_id")?)?;
    let proposal_id: EvolutionProposalId = parse_id(row.try_get("proposal_id")?)?;
    let source_run_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT run_id FROM evolution.proposal_source_runs WHERE proposal_id=$1 ORDER BY run_id",
    )
    .bind(proposal_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(parse_id::<RunId>)
    .collect::<Result<_, _>>()?;
    let evidence_record_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT ledger_record_id FROM evolution.proposal_evidence_records
         WHERE proposal_id=$1 ORDER BY ledger_record_id",
    )
    .bind(proposal_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(parse_id::<LedgerRecordId>)
    .collect::<Result<_, _>>()?;
    let candidate = decode_candidate(row, proposal_id, source_run_ids, evidence_record_ids)?;
    let draft = load_draft_in_transaction(
        &mut transaction,
        scope.tenant_id,
        scope.owner_principal_id,
        draft_id,
    )
    .await?
    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
    let pending = PendingAgentMemoryDraftEvaluation { draft, candidate };
    pending
        .validate_for(scope)
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    transaction.rollback().await?;
    Ok(Some(pending))
}
