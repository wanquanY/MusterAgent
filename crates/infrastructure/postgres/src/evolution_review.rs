use std::collections::BTreeSet;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::EvolutionEvaluation;
use musteragent_identity::{
    EvolutionEvaluationId, EvolutionProposalId, InteractionId, LedgerRecordId, MemoryId,
    PrincipalId, RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_memory::{
    ApprovedEvolutionInteraction, EvolutionReview, MemorySearch, MemorySearchHit,
};
use sqlx::Row as _;
use uuid::Uuid;

use crate::memory::{
    PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore, decode_candidate,
    decode_memory_hit, memory_kind_code,
};

pub(crate) async fn load_published_memory_head(
    store: &PostgresMemoryEvolutionStore,
    visibility: MemorySearch,
    memory_id: MemoryId,
) -> Result<Option<MemorySearchHit>, PostgresMemoryEvolutionError> {
    visibility.validate()?;
    let workspace_ids = visibility
        .workspace_ids
        .iter()
        .map(|value| value.into_uuid())
        .collect::<Vec<_>>();
    let agent_ids = visibility
        .agent_ids
        .iter()
        .map(|value| value.into_uuid())
        .collect::<Vec<_>>();
    let kind_codes = visibility
        .kinds
        .iter()
        .copied()
        .map(memory_kind_code)
        .collect::<Vec<_>>();
    let row = sqlx::query(
        "SELECT memory.memory_id,memory.tenant_id,memory.scope_kind,memory.scope_id,
                memory.memory_key,memory.kind,memory.lifecycle,memory.current_version_id,
                memory.created_at_ms,memory.updated_at_ms,memory.revoked_at_ms,
                version.memory_version_id,version.version::text AS version_number,
                version.source,version.confidence_bps,version.sensitivity,
                version.valid_from_ms,version.valid_to_ms,
                content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version
           ON version.memory_version_id=memory.current_version_id
         JOIN artifact.content_refs AS content
           ON content.content_id=version.content_id AND content.tenant_id=memory.tenant_id
          AND content.content_hash=version.content_hash
         WHERE memory.tenant_id=$1 AND memory.memory_id=$2 AND memory.lifecycle=1
           AND version.valid_from_ms <= $3
           AND (version.valid_to_ms IS NULL OR version.valid_to_ms > $3)
           AND (version.source <> 1 OR EXISTS (
               SELECT 1 FROM memory.explicit_publications AS publication
               WHERE publication.memory_version_id=version.memory_version_id
                 AND publication.memory_id=memory.memory_id
                 AND publication.tenant_id=memory.tenant_id
           ) OR EXISTS (
               SELECT 1 FROM memory.governance_publication_outbox AS publication
               WHERE publication.memory_version_id=version.memory_version_id
                 AND publication.memory_id=memory.memory_id
                 AND publication.tenant_id=memory.tenant_id
           ))
           AND ((memory.scope_kind=1 AND memory.scope_id=$4)
             OR (memory.scope_kind=2 AND memory.scope_id=$1)
             OR (memory.scope_kind=3 AND memory.scope_id=ANY($5))
             OR (memory.scope_kind=4 AND memory.scope_id=ANY($6)))
           AND (cardinality($7::smallint[])=0 OR memory.kind=ANY($7))",
    )
    .bind(visibility.tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .bind(visibility.now.unix_millis())
    .bind(visibility.principal_id.into_uuid())
    .bind(workspace_ids)
    .bind(agent_ids)
    .bind(kind_codes)
    .fetch_optional(&store.pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut hit = decode_memory_hit(row)?;
    hit.version.source_run_ids = load_memory_uuid_set::<RunId>(
        store,
        "SELECT run_id FROM memory.memory_version_source_runs
         WHERE memory_version_id=$1 ORDER BY run_id",
        hit.version.memory_version_id,
    )
    .await?;
    hit.version.evidence_record_ids = load_memory_uuid_set::<LedgerRecordId>(
        store,
        "SELECT ledger_record_id FROM memory.memory_version_evidence_records
         WHERE memory_version_id=$1 ORDER BY ledger_record_id",
        hit.version.memory_version_id,
    )
    .await?;
    hit.record.validate()?;
    hit.version.validate()?;
    Ok(Some(hit))
}

pub(crate) async fn load_active_memory_head(
    store: &PostgresMemoryEvolutionStore,
    tenant_id: TenantId,
    memory_id: MemoryId,
) -> Result<Option<MemorySearchHit>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT memory.memory_id,memory.tenant_id,memory.scope_kind,memory.scope_id,
                memory.memory_key,memory.kind,memory.lifecycle,memory.current_version_id,
                memory.created_at_ms,memory.updated_at_ms,memory.revoked_at_ms,
                version.memory_version_id,version.version::text AS version_number,
                version.source,version.confidence_bps,version.sensitivity,
                version.valid_from_ms,version.valid_to_ms,
                content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version
           ON version.memory_version_id=memory.current_version_id
         JOIN artifact.content_refs AS content
           ON content.content_id=version.content_id AND content.tenant_id=memory.tenant_id
          AND content.content_hash=version.content_hash
         WHERE memory.tenant_id=$1 AND memory.memory_id=$2 AND memory.lifecycle=1",
    )
    .bind(tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .fetch_optional(&store.pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut hit = decode_memory_hit(row)?;
    hit.version.source_run_ids = load_memory_uuid_set::<RunId>(
        store,
        "SELECT run_id FROM memory.memory_version_source_runs
         WHERE memory_version_id=$1 ORDER BY run_id",
        hit.version.memory_version_id,
    )
    .await?;
    hit.version.evidence_record_ids = load_memory_uuid_set::<LedgerRecordId>(
        store,
        "SELECT ledger_record_id FROM memory.memory_version_evidence_records
         WHERE memory_version_id=$1 ORDER BY ledger_record_id",
        hit.version.memory_version_id,
    )
    .await?;
    hit.record.validate()?;
    hit.version.validate()?;
    Ok(Some(hit))
}

pub(crate) async fn load_verified_review(
    store: &PostgresMemoryEvolutionStore,
    proposal_id: EvolutionProposalId,
) -> Result<Option<EvolutionReview>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT proposal.proposal_id,proposal.tenant_id,proposal.proposed_by,
                proposal.scope_kind,proposal.scope_id,proposal.target_memory_id,
                proposal.supersedes_proposal_id,proposal.proposal_origin,
                proposal.source_draft_id,proposal.proposal_kind,proposal.artifact_kind,
                proposal.status,
                proposal.confidence_bps,proposal.proposed_at_ms,
                claim.content_id,claim.media_type,claim.byte_length::text AS byte_length,
                claim.content_hash,
                evaluation.evaluation_id,evaluation.evaluated_by,evaluation.passed,
                evaluation.evaluated_at_ms,
                report.content_id AS report_content_id,
                report.media_type AS report_media_type,
                report.byte_length::text AS report_byte_length,
                report.content_hash AS report_content_hash
         FROM evolution.proposals AS proposal
         JOIN artifact.content_refs AS claim
           ON claim.content_id=proposal.candidate_content_id
          AND claim.tenant_id=proposal.tenant_id
         JOIN evolution.evaluations AS evaluation
           ON evaluation.proposal_id=proposal.proposal_id
          AND evaluation.passed=TRUE
         JOIN artifact.content_refs AS report
           ON report.content_id=evaluation.report_content_id
          AND report.tenant_id=proposal.tenant_id
          AND report.content_hash=evaluation.report_content_hash
         WHERE proposal.proposal_id=$1 AND proposal.status=3
         ORDER BY evaluation.evaluated_at_ms DESC,evaluation.evaluation_id
         LIMIT 1",
    )
    .bind(proposal_id.into_uuid())
    .fetch_optional(&store.pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let source_run_ids = load_uuid_set::<RunId>(
        store,
        "SELECT run_id FROM evolution.proposal_source_runs
         WHERE proposal_id=$1 ORDER BY run_id",
        proposal_id,
    )
    .await?;
    let evidence_record_ids = load_uuid_set::<LedgerRecordId>(
        store,
        "SELECT ledger_record_id FROM evolution.proposal_evidence_records
         WHERE proposal_id=$1 ORDER BY ledger_record_id",
        proposal_id,
    )
    .await?;
    let evaluation = EvolutionEvaluation {
        evaluation_id: parse_uuid(row.try_get("evaluation_id")?)?,
        proposal_id,
        evaluated_by: parse_uuid(
            row.try_get::<Option<Uuid>, _>("evaluated_by")?
                .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
        )?,
        passed: row
            .try_get::<Option<bool>, _>("passed")?
            .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
        report_ref: content_ref(
            row.try_get("report_content_id")?,
            row.try_get("report_media_type")?,
            row.try_get("report_byte_length")?,
            row.try_get("report_content_hash")?,
        )?,
        evaluated_at: UtcTimestamp::from_unix_millis(row.try_get("evaluated_at_ms")?),
    };
    let candidate = decode_candidate(row, proposal_id, source_run_ids, evidence_record_ids)?;
    candidate.validate()?;
    if !evaluation.passed || evaluation.proposal_id != candidate.proposal_id {
        return Err(PostgresMemoryEvolutionError::CorruptDatabase);
    }
    Ok(Some(EvolutionReview {
        candidate,
        evaluation,
    }))
}

pub(crate) async fn load_approved_interaction(
    store: &PostgresMemoryEvolutionStore,
    interaction_id: InteractionId,
) -> Result<Option<ApprovedEvolutionInteraction>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT interaction.interaction_id,interaction.responded_by,
                interaction.responded_at_ms,content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash
         FROM approval.interactions AS interaction
         JOIN approval.interaction_responses AS response
           ON response.interaction_id=interaction.interaction_id
          AND response.response_kind=1 AND response.approval_decision=1
         JOIN artifact.content_refs AS content
           ON content.content_id=interaction.prompt_content_id
          AND content.tenant_id=interaction.tenant_id
         WHERE interaction.interaction_id=$1
           AND interaction.kind=1 AND interaction.status=2",
    )
    .bind(interaction_id.into_uuid())
    .fetch_optional(&store.pool)
    .await?;
    if row.is_none() {
        let mut conn = store.pool.acquire().await?;
        if let Some((actor, _, projection)) =
            crate::PostgresNativeDialogueInteractionStore::approved_memory_interaction(
                &mut conn,
                interaction_id,
            )
            .await
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?
        {
            return Ok(Some(ApprovedEvolutionInteraction {
                interaction_id,
                prompt_ref: projection.interaction.prompt_ref,
                approved_by: actor.principal_id,
                approved_at: projection
                    .interaction
                    .responded_at
                    .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?,
            }));
        }
    }
    row.map(|row| {
        Ok(ApprovedEvolutionInteraction {
            interaction_id: parse_uuid(row.try_get("interaction_id")?)?,
            prompt_ref: content_ref(
                row.try_get("content_id")?,
                row.try_get("media_type")?,
                row.try_get("byte_length")?,
                row.try_get("content_hash")?,
            )?,
            approved_by: parse_uuid(
                row.try_get::<Option<Uuid>, _>("responded_by")?
                    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            )?,
            approved_at: UtcTimestamp::from_unix_millis(
                row.try_get::<Option<i64>, _>("responded_at_ms")?
                    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            ),
        })
    })
    .transpose()
}

async fn load_uuid_set<T>(
    store: &PostgresMemoryEvolutionStore,
    sql: &'static str,
    proposal_id: EvolutionProposalId,
) -> Result<BTreeSet<T>, PostgresMemoryEvolutionError>
where
    T: Ord + FromUuid,
{
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(proposal_id.into_uuid())
        .fetch_all(&store.pool)
        .await?
        .into_iter()
        .map(parse_uuid)
        .collect()
}

async fn load_memory_uuid_set<T>(
    store: &PostgresMemoryEvolutionStore,
    sql: &'static str,
    memory_version_id: musteragent_identity::MemoryVersionId,
) -> Result<BTreeSet<T>, PostgresMemoryEvolutionError>
where
    T: Ord + FromUuid,
{
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(memory_version_id.into_uuid())
        .fetch_all(&store.pool)
        .await?
        .into_iter()
        .map(parse_uuid)
        .collect()
}

fn content_ref(
    content_id: Uuid,
    media_type: String,
    byte_length: String,
    hash: Vec<u8>,
) -> Result<ContentRef, PostgresMemoryEvolutionError> {
    Ok(ContentRef {
        content_id: parse_uuid(content_id)?,
        media_type: MediaType::new(media_type)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        byte_length: byte_length
            .parse()
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        sha256: Sha256::from_bytes(hash)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
    })
}

trait FromUuid: Sized {
    fn parse(value: Uuid) -> Result<Self, ()>;
}

macro_rules! uuid_type {
    ($($type:ty),+ $(,)?) => {$(
        impl FromUuid for $type {
            fn parse(value: Uuid) -> Result<Self, ()> {
                <$type>::from_uuid(value).map_err(|_| ())
            }
        }
    )+};
}

uuid_type!(
    EvolutionEvaluationId,
    InteractionId,
    PrincipalId,
    RunId,
    LedgerRecordId,
    musteragent_identity::ContentId,
);

fn parse_uuid<T: FromUuid>(value: Uuid) -> Result<T, PostgresMemoryEvolutionError> {
    T::parse(value).map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}
