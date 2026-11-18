//! One original governance projection for Agent inspection and Draft review.
//! Callers own the read snapshot; nested queries never open another connection.
use super::parse_uuid;
use super::{PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore};
use crate::memory::decode_candidate;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::EvolutionEvaluation;
use musteragent_identity::{EvolutionProposalId, TenantId};
use musteragent_identity::{LedgerRecordId, RunId};
use musteragent_runtime_memory::{AgentEvolutionInspectionQuery, EvolutionProposalInspection};
use sqlx::Row as _;
use std::{collections::BTreeSet, str::FromStr};
use uuid::Uuid;

const SELECT_PROPOSAL: &str = "SELECT proposal.proposal_id,proposal.tenant_id,proposal.proposed_by,
                proposal.scope_kind,proposal.scope_id,proposal.target_memory_id,
                proposal.supersedes_proposal_id,proposal.proposal_origin,
                proposal.source_draft_id,proposal.proposal_kind,proposal.artifact_kind,
                proposal.status,
                proposal.confidence_bps,proposal.proposed_at_ms,
                claim.content_id,claim.media_type,
                claim.byte_length::text AS byte_length,claim.content_hash,
                evaluation.evaluation_id,evaluation.evaluated_by,evaluation.passed,
                evaluation.evaluated_at_ms,
                report.content_id AS report_content_id,
                report.media_type AS report_media_type,
                report.byte_length::text AS report_byte_length,
                report.content_hash AS report_content_hash,
                approval.approval_interaction_id,
                promotion.published_memory_version_id,
                instinct.instinct_id AS published_instinct_id,
                studio.studio_revision_draft_id,
                studio.artifact_kind AS studio_artifact_kind
         FROM evolution.proposals AS proposal
         JOIN artifact.content_refs AS claim
           ON claim.content_id=proposal.candidate_content_id
          AND claim.tenant_id=proposal.tenant_id
          AND claim.content_hash=proposal.candidate_content_hash
         LEFT JOIN LATERAL (
             SELECT value.evaluation_id,value.evaluated_by,value.passed,
                    value.report_content_id,value.report_content_hash,value.evaluated_at_ms
             FROM evolution.evaluations AS value
             WHERE value.proposal_id=proposal.proposal_id
             ORDER BY value.evaluated_at_ms DESC,value.evaluation_id
             LIMIT 1
         ) AS evaluation ON TRUE
         LEFT JOIN artifact.content_refs AS report
           ON report.content_id=evaluation.report_content_id
          AND report.tenant_id=proposal.tenant_id
          AND report.content_hash=evaluation.report_content_hash
         LEFT JOIN evolution.approvals AS approval
           ON approval.proposal_id=proposal.proposal_id
          AND approval.evaluation_id=evaluation.evaluation_id
         LEFT JOIN evolution.promotions AS promotion
           ON promotion.proposal_id=proposal.proposal_id
          AND promotion.evaluation_id=evaluation.evaluation_id
          AND promotion.approval_interaction_id=approval.approval_interaction_id
         LEFT JOIN evolution.artifact_promotions AS artifact_promotion
           ON artifact_promotion.proposal_id=proposal.proposal_id
          AND artifact_promotion.evaluation_id=evaluation.evaluation_id
          AND artifact_promotion.approval_interaction_id=approval.approval_interaction_id
         LEFT JOIN evolution.instincts AS instinct
           ON instinct.promotion_id=artifact_promotion.promotion_id
          AND artifact_promotion.target_kind=1
          AND instinct.instinct_id=artifact_promotion.target_id
         LEFT JOIN evolution.studio_revision_drafts AS studio
           ON studio.promotion_id=artifact_promotion.promotion_id
          AND artifact_promotion.target_kind=2
          AND studio.studio_revision_draft_id=artifact_promotion.target_id";

pub(super) async fn load_proposals(
    store: &PostgresMemoryEvolutionStore,
    query: &AgentEvolutionInspectionQuery,
) -> Result<Vec<EvolutionProposalInspection>, PostgresMemoryEvolutionError> {
    let mut transaction = store
        .database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Memory,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await?;
    let sql = format!(
        "{SELECT_PROPOSAL} WHERE proposal.tenant_id=$1 AND proposal.scope_kind=2 AND proposal.scope_id=$2 ORDER BY proposal.proposed_at_ms DESC,proposal.proposal_id LIMIT $3"
    );
    let rows = sqlx::query(&sql)
        .bind(query.tenant_id.into_uuid())
        .bind(query.agent_id.into_uuid())
        .bind(i64::from(query.proposal_limit.get()))
        .fetch_all(&mut *transaction)
        .await?;
    let mut inspections = Vec::with_capacity(rows.len());
    for row in rows {
        inspections.push(decode_proposal(&mut transaction, row).await?);
    }
    transaction.rollback().await?;
    Ok(inspections)
}

pub(crate) async fn load_proposal(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    proposal_id: EvolutionProposalId,
) -> Result<Option<EvolutionProposalInspection>, PostgresMemoryEvolutionError> {
    let sql = format!("{SELECT_PROPOSAL} WHERE proposal.tenant_id=$1 AND proposal.proposal_id=$2");
    let row = sqlx::query(&sql)
        .bind(tenant_id.into_uuid())
        .bind(proposal_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
    match row {
        Some(row) => Ok(Some(decode_proposal(transaction, row).await?)),
        None => Ok(None),
    }
}

async fn decode_proposal(
    transaction: &mut sqlx::PgConnection,
    row: sqlx::postgres::PgRow,
) -> Result<EvolutionProposalInspection, PostgresMemoryEvolutionError> {
    let proposal_id: EvolutionProposalId = parse_uuid(row.try_get("proposal_id")?)?;
    let evaluation = decode_evaluation(&row, proposal_id)?;
    let approval_interaction_id = row
        .try_get::<Option<Uuid>, _>("approval_interaction_id")?
        .map(parse_uuid)
        .transpose()?;
    let published_memory_version_id = row
        .try_get::<Option<Uuid>, _>("published_memory_version_id")?
        .map(parse_uuid)
        .transpose()?;
    let published_instinct_id = row
        .try_get::<Option<Uuid>, _>("published_instinct_id")?
        .map(parse_uuid)
        .transpose()?;
    let studio_revision_draft_id = row
        .try_get::<Option<Uuid>, _>("studio_revision_draft_id")?
        .map(parse_uuid)
        .transpose()?;
    let studio_artifact_kind = row
        .try_get::<Option<i16>, _>("studio_artifact_kind")?
        .map(studio_artifact_kind)
        .transpose()?;
    let source_run_ids = load_uuid_set::<RunId>(
        &mut *transaction,
        "SELECT run_id FROM evolution.proposal_source_runs
         WHERE proposal_id=$1 ORDER BY run_id",
        proposal_id,
    )
    .await?;
    let evidence_record_ids = load_uuid_set::<LedgerRecordId>(
        &mut *transaction,
        "SELECT ledger_record_id FROM evolution.proposal_evidence_records
         WHERE proposal_id=$1 ORDER BY ledger_record_id",
        proposal_id,
    )
    .await?;
    let candidate = decode_candidate(row, proposal_id, source_run_ids, evidence_record_ids)?;
    Ok(EvolutionProposalInspection {
        candidate,
        claim_detail: musteragent_runtime_memory::ExperienceClaimInspection::content_unavailable(),
        evaluation,
        approval_interaction_id,
        published_memory_version_id,
        published_instinct_id,
        studio_revision_draft_id,
        studio_artifact_kind,
    })
}

fn decode_evaluation(
    row: &sqlx::postgres::PgRow,
    proposal_id: EvolutionProposalId,
) -> Result<Option<EvolutionEvaluation>, PostgresMemoryEvolutionError> {
    let Some(evaluation_id) = row.try_get::<Option<Uuid>, _>("evaluation_id")? else {
        if row
            .try_get::<Option<Uuid>, _>("approval_interaction_id")?
            .is_some()
            || row
                .try_get::<Option<Uuid>, _>("published_memory_version_id")?
                .is_some()
            || row
                .try_get::<Option<Uuid>, _>("published_instinct_id")?
                .is_some()
            || row
                .try_get::<Option<Uuid>, _>("studio_revision_draft_id")?
                .is_some()
        {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        }
        return Ok(None);
    };
    Ok(Some(EvolutionEvaluation {
        evaluation_id: parse_uuid(evaluation_id)?,
        proposal_id,
        evaluated_by: parse_uuid(
            row.try_get::<Option<Uuid>, _>("evaluated_by")?
                .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
        )?,
        passed: row
            .try_get::<Option<bool>, _>("passed")?
            .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
        report_ref: ContentRef {
            content_id: parse_uuid(
                row.try_get::<Option<Uuid>, _>("report_content_id")?
                    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            )?,
            media_type: MediaType::new(
                row.try_get::<Option<String>, _>("report_media_type")?
                    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            )
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            byte_length: row
                .try_get::<Option<String>, _>("report_byte_length")?
                .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?
                .parse()
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            sha256: Sha256::from_bytes(
                row.try_get::<Option<Vec<u8>>, _>("report_content_hash")?
                    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            )
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        },
        evaluated_at: musteragent_identity::UtcTimestamp::from_unix_millis(
            row.try_get::<Option<i64>, _>("evaluated_at_ms")?
                .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
        ),
    }))
}

fn studio_artifact_kind(
    value: i16,
) -> Result<musteragent_runtime_memory::StudioRevisionArtifactKind, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(musteragent_runtime_memory::StudioRevisionArtifactKind::Skill),
        2 => Ok(musteragent_runtime_memory::StudioRevisionArtifactKind::PersonaPatch),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

async fn load_uuid_set<T>(
    transaction: &mut sqlx::PgConnection,
    sql: &'static str,
    proposal_id: EvolutionProposalId,
) -> Result<BTreeSet<T>, PostgresMemoryEvolutionError>
where
    T: FromStr + Ord,
{
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(proposal_id.into_uuid())
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .map(parse_uuid)
        .collect()
}

/// Exact scope-qualified Tool query on the original governance snapshot.
pub(crate) async fn inspect_tool_proposal(
    store: &PostgresMemoryEvolutionStore,
    query: musteragent_runtime_memory::ExperienceProposalInspectionQuery,
) -> Result<
    Option<musteragent_runtime_memory::ExperienceProposalInspection>,
    PostgresMemoryEvolutionError,
> {
    let mut transaction = store
        .database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Memory,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await?;
    let sql = format!("{SELECT_PROPOSAL} WHERE proposal.tenant_id=$1 AND proposal.proposal_id=$2
        AND ((proposal.scope_kind=1 AND proposal.scope_id=$3) OR (proposal.scope_kind=2 AND proposal.scope_id=$4))
        AND (NOT EXISTS (SELECT 1 FROM agent.owned_agents a WHERE a.agent_id=$4)
          OR EXISTS (SELECT 1 FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id)
            WHERE a.agent_id=$4 AND s.tenant_id=$1 AND s.owner_id=$3 AND s.product='musteragent' AND a.status='active'))");
    let row = sqlx::query(&sql)
        .bind(query.tenant_id.into_uuid())
        .bind(query.proposal_id.into_uuid())
        .bind(query.principal_id.into_uuid())
        .bind(query.agent_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
    let Some(row) = row else {
        transaction.rollback().await?;
        return Ok(None);
    };
    let proposal = decode_proposal(&mut transaction, row).await?;
    let publication = if let Some(version_id) = proposal.published_memory_version_id {
        let saved = sqlx::query("SELECT saved.memory_id,saved.memory_version_id,saved.version::text AS revision
            FROM evolution.promotions promotion
            JOIN evolution.proposals p ON p.proposal_id=promotion.proposal_id AND p.tenant_id=$1 AND p.status=5
            JOIN evolution.evaluations e ON e.evaluation_id=promotion.evaluation_id AND e.proposal_id=p.proposal_id AND e.passed=TRUE
            JOIN evolution.approvals a ON a.approval_interaction_id=promotion.approval_interaction_id AND a.proposal_id=p.proposal_id AND a.evaluation_id=e.evaluation_id
            JOIN memory.memory_versions saved ON saved.memory_version_id=promotion.published_memory_version_id AND saved.content_id=p.candidate_content_id AND saved.content_hash=p.candidate_content_hash
            JOIN memory.memories m ON m.memory_id=saved.memory_id AND m.tenant_id=p.tenant_id
            WHERE p.proposal_id=$2 AND saved.memory_version_id=$3
              AND ((p.scope_kind=1 AND p.scope_id=$4 AND m.scope_kind=1 AND m.scope_id=$4)
                OR (p.scope_kind=2 AND p.scope_id=$5 AND m.scope_kind=4 AND m.scope_id=$5))")
            .bind(query.tenant_id.into_uuid()).bind(query.proposal_id.into_uuid()).bind(version_id.into_uuid())
            .bind(query.principal_id.into_uuid()).bind(query.agent_id.into_uuid()).fetch_optional(&mut *transaction).await?;
        saved
            .map(|row| -> Result<_, PostgresMemoryEvolutionError> {
                Ok(musteragent_runtime_memory::ExperiencePublicationReceipt {
                    memory_id: parse_uuid(row.try_get("memory_id")?)?,
                    memory_version_id: parse_uuid(row.try_get("memory_version_id")?)?,
                    revision: musteragent_identity::VersionNumber::new(
                        row.try_get::<String, _>("revision")?
                            .parse()
                            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    )
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                })
            })
            .transpose()?
    } else {
        None
    };
    let inspection = musteragent_runtime_memory::ExperienceProposalInspection {
        proposal,
        publication,
    };
    inspection
        .validate_for(&query)
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    transaction.rollback().await?;
    Ok(Some(inspection))
}
