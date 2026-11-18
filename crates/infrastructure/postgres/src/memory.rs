mod native_reconciliation;
pub use native_reconciliation::NativeMemoryReconciliation;
mod explicit_publication;
pub(crate) mod native;
mod read_provenance;
pub(crate) use read_provenance::load_memory_provenance;
mod write_precondition;
pub(crate) use explicit_publication::publish_explicit_user_memory_transaction;
use std::collections::BTreeSet;
mod work_item_erasure;
pub(crate) use work_item_erasure::{collect_work_item_memories, erase_work_item_memories};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, ConfidenceBps, EvolutionApproval, EvolutionEvaluation, EvolutionPromotion,
    EvolutionScope, ExperienceCandidate, ExperienceCandidateOrigin, ExperienceKind,
};
use musteragent_identity::{
    AgentId, EvolutionEvaluationId, EvolutionPromotionId, EvolutionProposalId, IdempotencyKey,
    LedgerRecordId, MemoryDraftId, MemoryId, MemoryVersionId, PrincipalId, RunId, TenantId,
    UtcTimestamp, VersionNumber, WorkspaceId,
};
use musteragent_memory::{
    MemoryConfidenceBps, MemoryKind, MemoryLifecycle, MemoryRecord, MemoryScope, MemorySource,
    MemoryVersion,
};
use musteragent_policy::DataClassification;
use musteragent_runtime_memory::{
    EvolutionStore, ExperienceSearch, MemoryPersistenceDisposition, MemorySearch,
    MemorySearchDocument, MemorySearchHit, MemoryStore, PromoteExperience, RecordEvolutionApproval,
    RecordEvolutionEvaluation, RememberUserMemory, RevokeUserMemory,
};
use sqlx::{PgPool, Row as _};
use uuid::Uuid;

use crate::PostgresDatabaseRuntime;
pub(crate) use crate::memory_error::PostgresMemoryEvolutionError;
use crate::memory_publication::{insert_evolution_publication_outbox, mark_draft_published};

const NORMALIZED_PROVENANCE_MARKER: &[u8] = b"normalized-provenance:v1";

#[derive(Clone)]
pub struct PostgresMemoryEvolutionStore {
    pub(crate) pool: PgPool,
    pub(crate) database: PostgresDatabaseRuntime,
    /// Only Cloud management composition enables account-authorized Draft access.
    pub(crate) managed_drafts: bool,
}

impl PostgresMemoryEvolutionStore {
    #[must_use]
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            managed_drafts: false,
        }
    }

    async fn remember_user_inner(
        &self,
        command: RememberUserMemory,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        let disposition = remember_user_transaction(&mut transaction, &command).await?;
        transaction.commit().await?;
        Ok(disposition)
    }

    async fn search_inner(
        &self,
        query: MemorySearch,
    ) -> Result<Vec<MemorySearchHit>, PostgresMemoryEvolutionError> {
        query.validate()?;
        let workspace_ids = query
            .workspace_ids
            .iter()
            .map(|value| value.into_uuid())
            .collect::<Vec<_>>();
        let agent_ids = query
            .agent_ids
            .iter()
            .map(|value| value.into_uuid())
            .collect::<Vec<_>>();
        let kind_codes = query
            .kinds
            .iter()
            .copied()
            .map(memory_kind_code)
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT memory.memory_id, memory.tenant_id, memory.scope_kind,
                    memory.scope_id, memory.memory_key, memory.kind,
                    memory.lifecycle, memory.current_version_id,
                    memory.created_at_ms, memory.updated_at_ms, memory.revoked_at_ms,
                    version.memory_version_id, version.version::text AS version_number,
                    version.source, version.confidence_bps, version.sensitivity,
                    version.valid_from_ms, version.valid_to_ms,
                    content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, content.content_hash
             FROM memory.memories AS memory
             JOIN memory.memory_versions AS version
               ON version.memory_version_id = memory.current_version_id
             JOIN artifact.content_refs AS content
               ON content.content_id = version.content_id
              AND content.tenant_id = memory.tenant_id
             WHERE memory.tenant_id = $1
               AND memory.lifecycle = 1
               AND content.state = 2 AND version.work_item_erasure_job_id IS NULL
               AND (
                    version.source <> 1
                    OR EXISTS (
                        SELECT 1 FROM memory.explicit_publications AS publication
                        WHERE publication.memory_version_id = version.memory_version_id
                          AND publication.memory_id = memory.memory_id
                          AND publication.tenant_id = memory.tenant_id
                    )
                    OR EXISTS (
                        SELECT 1 FROM memory.governance_publication_outbox AS publication
                        WHERE publication.memory_version_id = version.memory_version_id
                          AND publication.memory_id = memory.memory_id
                          AND publication.tenant_id = memory.tenant_id
                    )
               )
               AND version.valid_from_ms <= $2
               AND (version.valid_to_ms IS NULL OR version.valid_to_ms > $2)
               AND (
                    (memory.scope_kind = 1 AND memory.scope_id = $3)
                    OR (memory.scope_kind = 2 AND memory.scope_id = $1)
                    OR (memory.scope_kind = 3 AND memory.scope_id = ANY($4))
                    OR (memory.scope_kind = 4 AND memory.scope_id = ANY($5))
               )
               AND (cardinality($6::smallint[]) = 0 OR memory.kind = ANY($6))
             ORDER BY version.confidence_bps DESC, memory.updated_at_ms DESC,
                      memory.memory_id
             LIMIT $7",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(query.now.unix_millis())
        .bind(query.principal_id.into_uuid())
        .bind(workspace_ids)
        .bind(agent_ids)
        .bind(kind_codes)
        .bind(i64::from(query.limit.get()))
        .fetch_all(&self.pool)
        .await?;
        let mut hits = rows
            .into_iter()
            .map(decode_memory_hit)
            .collect::<Result<Vec<_>, _>>()?;
        load_memory_provenance(&self.pool, &mut hits).await?;
        Ok(hits)
    }

    async fn find_user_by_key_inner(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        memory_key: IdempotencyKey,
    ) -> Result<Option<MemorySearchHit>, PostgresMemoryEvolutionError> {
        let row = sqlx::query(
            "SELECT memory.memory_id, memory.tenant_id, memory.scope_kind,
                    memory.scope_id, memory.memory_key, memory.kind,
                    memory.lifecycle, memory.current_version_id,
                    memory.created_at_ms, memory.updated_at_ms, memory.revoked_at_ms,
                    version.memory_version_id, version.version::text AS version_number,
                    version.source, version.confidence_bps, version.sensitivity,
                    version.valid_from_ms, version.valid_to_ms,
                    content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, content.content_hash
             FROM memory.memories AS memory
             JOIN memory.memory_versions AS version
               ON version.memory_version_id = memory.current_version_id
             JOIN artifact.content_refs AS content
               ON content.content_id = version.content_id
              AND content.tenant_id = memory.tenant_id
             WHERE memory.tenant_id = $1
               AND memory.scope_kind = 1
               AND memory.scope_id = $2
               AND memory.memory_key = $3",
        )
        .bind(tenant_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(memory_key.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut hit = decode_memory_hit(row)?;
        load_memory_provenance(&self.pool, std::slice::from_mut(&mut hit)).await?;
        Ok(Some(hit))
    }

    async fn revoke_user_inner(
        &self,
        command: RevokeUserMemory,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        let result = sqlx::query(
            "UPDATE memory.memories
             SET lifecycle = 3, revoked_at_ms = $4, updated_at_ms = $4
             WHERE tenant_id = $1 AND memory_id = $2
               AND scope_kind = 1 AND scope_id = $3
               AND lifecycle = 1",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.actor_principal_id.into_uuid())
        .bind(command.revoked_at.unix_millis())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(MemoryPersistenceDisposition::Applied);
        }
        let existing = sqlx::query(
            "SELECT lifecycle, revoked_at_ms
             FROM memory.memories
             WHERE tenant_id = $1 AND memory_id = $2
               AND scope_kind = 1 AND scope_id = $3",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.actor_principal_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        match existing {
            Some(row)
                if row.try_get::<i16, _>("lifecycle")? == 3
                    && row.try_get::<Option<i64>, _>("revoked_at_ms")?
                        == Some(command.revoked_at.unix_millis()) =>
            {
                Ok(MemoryPersistenceDisposition::Duplicate)
            }
            Some(_) => Err(PostgresMemoryEvolutionError::MemoryConflict),
            None => Err(PostgresMemoryEvolutionError::MemoryNotFound),
        }
    }

    async fn propose_inner(
        &self,
        candidate: ExperienceCandidate,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        let disposition = propose_transaction(&mut transaction, &candidate).await?;
        transaction.commit().await?;
        Ok(disposition)
    }

    async fn load_next_candidate_for_evaluation_inner(
        &self,
    ) -> Result<Option<ExperienceCandidate>, PostgresMemoryEvolutionError> {
        let row = sqlx::query(
            "SELECT proposal.proposal_id, proposal.tenant_id,
                    proposal.proposed_by, proposal.scope_kind, proposal.scope_id,
                    proposal.target_memory_id, proposal.supersedes_proposal_id,
                    proposal.proposal_origin, proposal.source_draft_id,
                    proposal.proposal_kind, proposal.artifact_kind,
                    proposal.status, proposal.confidence_bps,
                    proposal.proposed_at_ms, content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, content.content_hash
             FROM evolution.proposals AS proposal
             JOIN artifact.content_refs AS content
               ON content.content_id=proposal.candidate_content_id
              AND content.tenant_id=proposal.tenant_id
              AND content.content_hash=proposal.candidate_content_hash
             WHERE proposal.status IN (1,2)
               AND NOT EXISTS (
                   SELECT 1 FROM evolution.evaluations AS evaluation
                   WHERE evaluation.proposal_id=proposal.proposal_id
               )
             ORDER BY proposal.proposed_at_ms,proposal.proposal_id
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let proposal_id =
            id::<EvolutionProposalId>(row.try_get("proposal_id")?, "evolution.proposal_id")?;
        let source_run_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT run_id FROM evolution.proposal_source_runs
             WHERE proposal_id=$1 ORDER BY run_id",
        )
        .bind(proposal_id.into_uuid())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|value| id::<RunId>(value, "evolution.source_run_id"))
        .collect::<Result<BTreeSet<_>, _>>()?;
        let evidence_record_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT ledger_record_id FROM evolution.proposal_evidence_records
             WHERE proposal_id=$1 ORDER BY ledger_record_id",
        )
        .bind(proposal_id.into_uuid())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|value| id::<LedgerRecordId>(value, "evolution.evidence_record_id"))
        .collect::<Result<BTreeSet<_>, _>>()?;
        let candidate = decode_candidate(row, proposal_id, source_run_ids, evidence_record_ids)?;
        candidate.validate()?;
        Ok(Some(candidate))
    }

    async fn record_evaluation_inner(
        &self,
        command: RecordEvolutionEvaluation,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        command.validate()?;
        let evaluation = &command.evaluation;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        let proposal = sqlx::query(
            "SELECT tenant_id, status
             FROM evolution.proposals
             WHERE proposal_id = $1
             FOR UPDATE",
        )
        .bind(evaluation.proposal_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresMemoryEvolutionError::EvolutionNotFound)?;
        let tenant_id: Uuid = proposal.try_get("tenant_id")?;
        let current_status: i16 = proposal.try_get("status")?;
        if !matches!(current_status, 1 | 2) {
            let mut persisted_candidate = command.candidate.clone();
            persisted_candidate.status = if evaluation.passed {
                CandidateStatus::Verified
            } else {
                CandidateStatus::Rejected
            };
            let duplicate = proposal_matches(&mut transaction, &persisted_candidate).await?
                && evaluation_matches(&mut transaction, evaluation).await?;
            transaction.rollback().await?;
            return if duplicate {
                Ok(MemoryPersistenceDisposition::Duplicate)
            } else {
                Err(PostgresMemoryEvolutionError::EvolutionConflict)
            };
        }
        if !proposal_matches(&mut transaction, &command.candidate).await? {
            return Err(PostgresMemoryEvolutionError::EvolutionConflict);
        }
        ensure_content_ref(
            &mut transaction,
            TenantId::from_uuid(tenant_id)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            &evaluation.report_ref,
        )
        .await?;
        let result = sqlx::query(
            "INSERT INTO evolution.evaluations (
                 evaluation_id, proposal_id, evaluator_ref, result, evidence,
                 evaluated_at_ms, evaluated_by, passed,
                 report_content_id, report_content_hash
             ) VALUES ($1, $2, $3, $4, $5, $6, $3, $7, $8, $9)
             ON CONFLICT DO NOTHING",
        )
        .bind(evaluation.evaluation_id.into_uuid())
        .bind(evaluation.proposal_id.into_uuid())
        .bind(evaluation.evaluated_by.into_uuid())
        .bind(if evaluation.passed { 1_i16 } else { 2_i16 })
        .bind(NORMALIZED_PROVENANCE_MARKER)
        .bind(evaluation.evaluated_at.unix_millis())
        .bind(evaluation.passed)
        .bind(evaluation.report_ref.content_id.into_uuid())
        .bind(evaluation.report_ref.sha256.into_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 {
            sqlx::query(
                "UPDATE evolution.proposals
                 SET status = $2,
                     confidence_bps = CASE
                         WHEN $3 THEN COALESCE(confidence_bps, 7500)
                         ELSE confidence_bps
                     END
                 WHERE proposal_id = $1",
            )
            .bind(evaluation.proposal_id.into_uuid())
            .bind(if evaluation.passed { 3_i16 } else { 4_i16 })
            .bind(evaluation.passed)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(MemoryPersistenceDisposition::Applied);
        }
        let row = sqlx::query(
            "SELECT proposal_id, evaluated_by, passed, report_content_id,
                    report_content_hash, evaluated_at_ms
             FROM evolution.evaluations WHERE evaluation_id = $1",
        )
        .bind(evaluation.evaluation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        let duplicate = match row {
            Some(row)
                if row.try_get::<uuid::Uuid, _>("proposal_id")?
                    == evaluation.proposal_id.into_uuid()
                    && row.try_get::<Option<uuid::Uuid>, _>("evaluated_by")?
                        == Some(evaluation.evaluated_by.into_uuid())
                    && row.try_get::<Option<bool>, _>("passed")? == Some(evaluation.passed)
                    && row.try_get::<Option<uuid::Uuid>, _>("report_content_id")?
                        == Some(evaluation.report_ref.content_id.into_uuid())
                    && row.try_get::<Option<Vec<u8>>, _>("report_content_hash")?
                        == Some(evaluation.report_ref.sha256.into_bytes().to_vec())
                    && row.try_get::<i64, _>("evaluated_at_ms")?
                        == evaluation.evaluated_at.unix_millis() =>
            {
                true
            }
            Some(_) | None => false,
        };
        transaction.rollback().await?;
        if duplicate {
            Ok(MemoryPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresMemoryEvolutionError::EvolutionConflict)
        }
    }

    async fn record_approval_inner(
        &self,
        command: RecordEvolutionApproval,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        command.validate()?;
        let approval = &command.approval;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        ensure_content_ref(
            &mut transaction,
            command.candidate.tenant_id,
            &command.candidate.claim_ref,
        )
        .await?;
        ensure_content_ref(
            &mut transaction,
            command.candidate.tenant_id,
            &command.evaluation.report_ref,
        )
        .await?;
        ensure_content_ref(
            &mut transaction,
            command.candidate.tenant_id,
            &command.approval.prompt_ref,
        )
        .await?;
        let row = sqlx::query(
            "SELECT proposal.tenant_id, proposal.status,
                    evaluation.passed, evaluation.evaluated_at_ms,
                    interaction.kind AS interaction_kind,
                    interaction.status AS interaction_status,
                    interaction.prompt_content_id,
                    interaction.responded_by, interaction.responded_at_ms,
                    response.approval_decision,
                    content.media_type, content.byte_length::text AS byte_length,
                    content.content_hash
             FROM evolution.proposals AS proposal
             JOIN evolution.evaluations AS evaluation
               ON evaluation.evaluation_id = $2
              AND evaluation.proposal_id = proposal.proposal_id
             JOIN approval.interactions AS interaction
               ON interaction.interaction_id = $3
              AND interaction.tenant_id = proposal.tenant_id
             JOIN approval.interaction_responses AS response
               ON response.interaction_id = interaction.interaction_id
             JOIN artifact.content_refs AS content
               ON content.content_id = interaction.prompt_content_id
              AND content.tenant_id = proposal.tenant_id
             WHERE proposal.proposal_id = $1
             FOR UPDATE OF proposal",
        )
        .bind(approval.proposal_id.into_uuid())
        .bind(approval.evaluation_id.into_uuid())
        .bind(approval.approval_interaction_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
        let frozen_facts_match = proposal_matches(&mut transaction, &command.candidate).await?
            && evaluation_matches(&mut transaction, &command.evaluation).await?;
        let canonical = frozen_facts_match
            && row.try_get::<i16, _>("status")? == candidate_status_code(CandidateStatus::Verified)
            && row.try_get::<Option<bool>, _>("passed")? == Some(true)
            && row.try_get::<i16, _>("interaction_kind")? == 1
            && row.try_get::<i16, _>("interaction_status")? == 2
            && row.try_get::<Option<i16>, _>("approval_decision")? == Some(1)
            && row.try_get::<Uuid, _>("prompt_content_id")?
                == approval.prompt_ref.content_id.into_uuid()
            && row.try_get::<String, _>("media_type")? == approval.prompt_ref.media_type.as_str()
            && parse_u64(row.try_get("byte_length")?, "approval.prompt_byte_length")?
                == approval.prompt_ref.byte_length
            && row.try_get::<Vec<u8>, _>("content_hash")?
                == approval.prompt_ref.sha256.into_bytes()
            && row.try_get::<Option<Uuid>, _>("responded_by")?
                == Some(approval.approved_by.into_uuid())
            && row.try_get::<Option<i64>, _>("responded_at_ms")?
                == Some(approval.approved_at.unix_millis())
            && approval.approved_at.unix_millis() >= row.try_get::<i64, _>("evaluated_at_ms")?;
        if !canonical {
            return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
        }
        let inserted = sqlx::query(
            "INSERT INTO evolution.approvals (
                 approval_interaction_id, proposal_id, evaluation_id,
                 prompt_content_id, prompt_content_hash, approved_by, approved_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT DO NOTHING",
        )
        .bind(approval.approval_interaction_id.into_uuid())
        .bind(approval.proposal_id.into_uuid())
        .bind(approval.evaluation_id.into_uuid())
        .bind(approval.prompt_ref.content_id.into_uuid())
        .bind(approval.prompt_ref.sha256.into_bytes().as_slice())
        .bind(approval.approved_by.into_uuid())
        .bind(approval.approved_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 1 {
            transaction.commit().await?;
            return Ok(MemoryPersistenceDisposition::Applied);
        }
        let duplicate = approval_matches(&mut transaction, approval).await?;
        transaction.rollback().await?;
        if duplicate {
            Ok(MemoryPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresMemoryEvolutionError::EvolutionConflict)
        }
    }

    async fn promote_inner(
        &self,
        command: PromoteExperience,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        command.validate()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        advisory_lock(
            &mut transaction,
            command.candidate.tenant_id,
            command.memory_record.memory_key.as_str(),
        )
        .await?;
        let proposal_status: Option<i16> = sqlx::query_scalar(
            "SELECT status FROM evolution.proposals
             WHERE proposal_id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(command.candidate.proposal_id.into_uuid())
        .bind(command.candidate.tenant_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        if proposal_status == Some(candidate_status_code(CandidateStatus::Promoted)) {
            ensure_content_ref(
                &mut transaction,
                command.memory_record.tenant_id,
                &command.memory_version.content_ref,
            )
            .await?;
            let duplicate = proposal_matches(&mut transaction, &command.candidate).await?
                && evaluation_matches(&mut transaction, &command.evaluation).await?
                && approval_matches(&mut transaction, &command.approval).await?
                && promotion_matches(&mut transaction, &command.promotion).await?
                && memory_record_matches(&mut transaction, &command.memory_record).await?
                && version_matches(&mut transaction, &command.memory_version).await?
                && crate::memory_search_index::search_document_matches(
                    &mut transaction,
                    &command.search_document,
                    command.memory_version.created_at.unix_millis(),
                )
                .await?;
            transaction.rollback().await?;
            return if duplicate {
                Ok(MemoryPersistenceDisposition::Duplicate)
            } else {
                Err(PostgresMemoryEvolutionError::EvolutionConflict)
            };
        }
        if proposal_status != Some(candidate_status_code(CandidateStatus::Verified)) {
            return Err(PostgresMemoryEvolutionError::ProposalNotVerified);
        }
        let mut verified_candidate = command.candidate.clone();
        verified_candidate.status = CandidateStatus::Verified;
        if !proposal_matches(&mut transaction, &verified_candidate).await?
            || !evaluation_matches(&mut transaction, &command.evaluation).await?
            || !approval_matches(&mut transaction, &command.approval).await?
        {
            return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
        }
        ensure_content_ref(
            &mut transaction,
            command.memory_record.tenant_id,
            &command.memory_version.content_ref,
        )
        .await?;
        assert_expected_memory_head(&mut transaction, &command).await?;
        let disposition = persist_memory(
            &mut transaction,
            &command.memory_record,
            &command.memory_version,
            &command.search_document,
        )
        .await?;
        if disposition == MemoryPersistenceDisposition::Duplicate {
            return Err(PostgresMemoryEvolutionError::EvolutionConflict);
        }
        sqlx::query(
            "INSERT INTO evolution.promotions (
                 promotion_id, candidate_id, promoted_version_ref,
                 promoted_by, promoted_at_ms, proposal_id,
                 published_memory_version_id, evaluation_id,
                 approval_interaction_id
             ) VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(command.promotion.promotion_id.into_uuid())
        .bind(command.promotion.published_memory_version_id.to_string())
        .bind(command.promotion.promoted_by.into_uuid())
        .bind(command.promotion.promoted_at.unix_millis())
        .bind(command.promotion.proposal_id.into_uuid())
        .bind(command.promotion.published_memory_version_id.into_uuid())
        .bind(command.promotion.evaluation_id.into_uuid())
        .bind(command.promotion.approval_interaction_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE evolution.proposals
             SET status = 5
             WHERE proposal_id = $1 AND status = 3",
        )
        .bind(command.candidate.proposal_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        mark_draft_published(&mut transaction, &command).await?;
        insert_evolution_publication_outbox(&mut transaction, &command).await?;
        transaction.commit().await?;
        Ok(MemoryPersistenceDisposition::Applied)
    }

    async fn search_published_inner(
        &self,
        query: ExperienceSearch,
    ) -> Result<Vec<ExperienceCandidate>, PostgresMemoryEvolutionError> {
        query.validate()?;
        let agent_ids = query
            .agent_ids
            .iter()
            .map(|value| value.into_uuid())
            .collect::<Vec<_>>();
        let kind_codes = query
            .kinds
            .iter()
            .copied()
            .map(experience_kind_code)
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT proposal.proposal_id, proposal.tenant_id,
                    proposal.proposed_by, proposal.scope_kind, proposal.scope_id,
                    proposal.target_memory_id, proposal.supersedes_proposal_id,
                    proposal.proposal_origin, proposal.source_draft_id,
                    proposal.proposal_kind, proposal.artifact_kind,
                    proposal.status, proposal.confidence_bps,
                    proposal.proposed_at_ms, content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, content.content_hash
             FROM evolution.proposals AS proposal
             JOIN artifact.content_refs AS content
               ON content.content_id = proposal.candidate_content_id
              AND content.tenant_id = proposal.tenant_id
             JOIN evolution.promotions AS promotion
               ON promotion.proposal_id = proposal.proposal_id
             JOIN evolution.approvals AS approval
               ON approval.approval_interaction_id = promotion.approval_interaction_id
              AND approval.proposal_id = proposal.proposal_id
              AND approval.evaluation_id = promotion.evaluation_id
             WHERE proposal.tenant_id = $1
               AND proposal.status = 5
               AND proposal.artifact_kind IS NULL
               AND (
                    (proposal.scope_kind = 1 AND proposal.scope_id = $2)
                    OR (proposal.scope_kind = 2 AND proposal.scope_id = ANY($3))
               )
               AND (cardinality($4::smallint[]) = 0 OR proposal.proposal_kind = ANY($4))
             ORDER BY proposal.confidence_bps DESC, proposal.proposed_at_ms DESC,
                      proposal.proposal_id
             LIMIT $5",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(query.principal_id.into_uuid())
        .bind(agent_ids)
        .bind(kind_codes)
        .bind(i64::from(query.limit.get()))
        .fetch_all(&self.pool)
        .await?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let proposal_id =
                id::<EvolutionProposalId>(row.try_get("proposal_id")?, "evolution.proposal_id")?;
            let source_run_ids = sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT run_id FROM evolution.proposal_source_runs
                 WHERE proposal_id = $1 ORDER BY run_id",
            )
            .bind(proposal_id.into_uuid())
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|value| id::<RunId>(value, "evolution.source_run_id"))
            .collect::<Result<_, _>>()?;
            let evidence_record_ids = sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT ledger_record_id FROM evolution.proposal_evidence_records
                 WHERE proposal_id = $1 ORDER BY ledger_record_id",
            )
            .bind(proposal_id.into_uuid())
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|value| id::<LedgerRecordId>(value, "evolution.evidence_record_id"))
            .collect::<Result<_, _>>()?;
            candidates.push(decode_candidate(
                row,
                proposal_id,
                source_run_ids,
                evidence_record_ids,
            )?);
        }
        Ok(candidates)
    }
}

impl MemoryStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn remember_user(
        &self,
        command: RememberUserMemory,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.remember_user_inner(command).await })
    }

    fn search(
        &self,
        query: MemorySearch,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Vec<MemorySearchHit>, Self::Error>,
    > {
        Box::pin(async move { self.search_inner(query).await })
    }

    fn find_user_by_key(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        memory_key: IdempotencyKey,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<MemorySearchHit>, Self::Error>,
    > {
        Box::pin(async move {
            self.find_user_by_key_inner(tenant_id, principal_id, memory_key)
                .await
        })
    }

    fn load_published_head(
        &self,
        visibility: MemorySearch,
        memory_id: MemoryId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<MemorySearchHit>, Self::Error>,
    > {
        Box::pin(crate::evolution_review::load_published_memory_head(
            self, visibility, memory_id,
        ))
    }

    fn load_active_head(
        &self,
        tenant_id: TenantId,
        memory_id: MemoryId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<MemorySearchHit>, Self::Error>,
    > {
        Box::pin(crate::evolution_review::load_active_memory_head(
            self, tenant_id, memory_id,
        ))
    }

    fn revoke_user(
        &self,
        command: RevokeUserMemory,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.revoke_user_inner(command).await })
    }
}

impl EvolutionStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn inspect_proposal(
        &self,
        query: musteragent_runtime_memory::ExperienceProposalInspectionQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_memory::ExperienceProposalInspection>, Self::Error>,
    > {
        Box::pin(crate::evolution_inspection::proposals::inspect_tool_proposal(self, query))
    }

    fn propose(
        &self,
        candidate: ExperienceCandidate,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.propose_inner(candidate).await })
    }

    fn record_evaluation(
        &self,
        command: RecordEvolutionEvaluation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.record_evaluation_inner(command).await })
    }

    fn load_next_candidate_for_evaluation(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<ExperienceCandidate>, Self::Error>,
    > {
        Box::pin(async move { self.load_next_candidate_for_evaluation_inner().await })
    }

    fn load_verified_review(
        &self,
        proposal_id: EvolutionProposalId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_memory::EvolutionReview>, Self::Error>,
    > {
        Box::pin(crate::evolution_review::load_verified_review(
            self,
            proposal_id,
        ))
    }

    fn load_approved_interaction(
        &self,
        interaction_id: musteragent_identity::InteractionId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_memory::ApprovedEvolutionInteraction>, Self::Error>,
    > {
        Box::pin(crate::evolution_review::load_approved_interaction(
            self,
            interaction_id,
        ))
    }

    fn record_approval(
        &self,
        command: RecordEvolutionApproval,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.record_approval_inner(command).await })
    }

    fn promote(
        &self,
        command: PromoteExperience,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move { self.promote_inner(command).await })
    }

    fn search_published(
        &self,
        query: ExperienceSearch,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Vec<ExperienceCandidate>, Self::Error>,
    > {
        Box::pin(async move { self.search_published_inner(query).await })
    }
}

pub(crate) async fn assert_expected_memory_head(
    transaction: &mut sqlx::PgConnection,
    command: &PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    write_precondition::assert_publication(transaction, command).await
}

pub(crate) async fn remember_user_transaction(
    transaction: &mut sqlx::PgConnection,
    command: &RememberUserMemory,
) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
    command.validate()?;
    advisory_lock(
        transaction,
        command.record.tenant_id,
        command.record.memory_key.as_str(),
    )
    .await?;
    write_precondition::assert_current(transaction, command).await?;
    ensure_content_ref(
        transaction,
        command.record.tenant_id,
        &command.version.content_ref,
    )
    .await?;
    persist_memory(
        transaction,
        &command.record,
        &command.version,
        &command.search_document,
    )
    .await
}

pub(crate) async fn propose_transaction(
    transaction: &mut sqlx::PgConnection,
    candidate: &ExperienceCandidate,
) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
    candidate.validate()?;
    ensure_content_ref(transaction, candidate.tenant_id, &candidate.claim_ref).await?;
    let (scope_kind, scope_id) = evolution_scope(&candidate.target_scope);
    let result = sqlx::query(
        "INSERT INTO evolution.proposals (
             proposal_id, tenant_id, scope_kind, scope_id, proposal_kind,
             candidate_content_id, evidence, status, proposed_at_ms,
             proposed_by, target_memory_id, supersedes_proposal_id,
             candidate_content_hash, confidence_bps, proposal_origin, source_draft_id,
             artifact_kind
         ) VALUES (
             $1, $2, $3, $4, $5,
             $6, $7, $8, $9,
             $10, $11, $12,
             $13, $14, $15, $16, $17
         )
         ON CONFLICT DO NOTHING",
    )
    .bind(candidate.proposal_id.into_uuid())
    .bind(candidate.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(experience_kind_code(candidate.kind))
    .bind(candidate.claim_ref.content_id.into_uuid())
    .bind(NORMALIZED_PROVENANCE_MARKER)
    .bind(candidate_status_code(candidate.status))
    .bind(candidate.created_at.unix_millis())
    .bind(candidate.proposed_by.into_uuid())
    .bind(candidate.target_memory_id.map(MemoryId::into_uuid))
    .bind(
        candidate
            .supersedes_proposal_id
            .map(EvolutionProposalId::into_uuid),
    )
    .bind(candidate.claim_ref.sha256.into_bytes().as_slice())
    .bind(candidate.confidence.map(|value| i32::from(value.get())))
    .bind(candidate_origin_code(candidate.origin))
    .bind(candidate.source_draft_id.map(MemoryDraftId::into_uuid))
    .bind(experience_artifact_kind_code(candidate.kind))
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return if proposal_matches(transaction, candidate).await? {
            Ok(MemoryPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresMemoryEvolutionError::EvolutionConflict)
        };
    }
    insert_proposal_provenance(transaction, candidate).await?;
    Ok(MemoryPersistenceDisposition::Applied)
}

pub(crate) async fn advisory_lock(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    key: &str,
) -> Result<(), PostgresMemoryEvolutionError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("{tenant_id}:{key}"))
        .execute(&mut *transaction)
        .await?;
    Ok(())
}

pub(crate) async fn ensure_content_ref(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content: &ContentRef,
) -> Result<(), PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT media_type, byte_length::text AS byte_length, content_hash
         FROM artifact.content_refs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresMemoryEvolutionError::ContentNotFound)?;
    if row.try_get::<String, _>("media_type")? != content.media_type.as_str()
        || parse_u64(row.try_get("byte_length")?, "content.byte_length")? != content.byte_length
        || row.try_get::<Vec<u8>, _>("content_hash")? != content.sha256.into_bytes()
    {
        return Err(PostgresMemoryEvolutionError::ContentIntegrityMismatch);
    }
    Ok(())
}

pub(crate) async fn persist_memory(
    transaction: &mut sqlx::PgConnection,
    record: &MemoryRecord,
    version: &MemoryVersion,
    search_document: &MemorySearchDocument,
) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
    record.validate()?;
    version.validate()?;
    search_document.validate_for(record, version)?;
    let (scope_kind, scope_id) = memory_scope(&record.scope);
    let inserted = sqlx::query(
        "INSERT INTO memory.memories (
             memory_id, tenant_id, scope_kind, scope_id, memory_key,
             lifecycle, created_at_ms, updated_at_ms, kind,
             current_version_id, revoked_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT DO NOTHING",
    )
    .bind(record.memory_id.into_uuid())
    .bind(record.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(record.memory_key.as_str())
    .bind(memory_lifecycle_code(record.lifecycle))
    .bind(record.created_at.unix_millis())
    .bind(record.updated_at.unix_millis())
    .bind(memory_kind_code(record.kind))
    .bind(record.current_version_id.into_uuid())
    .bind(record.revoked_at.map(UtcTimestamp::unix_millis))
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 0 {
        let existing = sqlx::query(
            "SELECT memory_id, current_version_id
             FROM memory.memories
             WHERE tenant_id = $1 AND scope_kind = $2
               AND scope_id IS NOT DISTINCT FROM $3 AND memory_key = $4
             FOR UPDATE",
        )
        .bind(record.tenant_id.into_uuid())
        .bind(scope_kind)
        .bind(scope_id)
        .bind(record.memory_key.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresMemoryEvolutionError::MemoryConflict)?;
        let existing_id: Uuid = existing.try_get("memory_id")?;
        let current_version_id: Option<Uuid> = existing.try_get("current_version_id")?;
        if existing_id != record.memory_id.into_uuid() {
            return Err(PostgresMemoryEvolutionError::MemoryConflict);
        }
        if current_version_id == Some(version.memory_version_id.into_uuid()) {
            return if version_matches(transaction, version).await?
                && crate::memory_search_index::search_document_matches(
                    transaction,
                    search_document,
                    version.created_at.unix_millis(),
                )
                .await?
            {
                Ok(MemoryPersistenceDisposition::Duplicate)
            } else {
                Err(PostgresMemoryEvolutionError::MemoryConflict)
            };
        }
        let expected_previous = version
            .version
            .get()
            .checked_sub(1)
            .ok_or(PostgresMemoryEvolutionError::MemoryVersionConflict)?;
        let current_version: String = sqlx::query_scalar(
            "SELECT version::text FROM memory.memory_versions
             WHERE memory_version_id = $1",
        )
        .bind(current_version_id)
        .fetch_one(&mut *transaction)
        .await?;
        if parse_u64(current_version, "memory.current_version")? != expected_previous {
            return Err(PostgresMemoryEvolutionError::MemoryVersionConflict);
        }
        insert_memory_version(transaction, record, version).await?;
        crate::memory_search_index::persist_search_document(
            transaction,
            search_document,
            version.created_at.unix_millis(),
        )
        .await?;
        sqlx::query(
            "UPDATE memory.memories
             SET current_version_id = $2, lifecycle = $3,
                 updated_at_ms = $4, revoked_at_ms = $5, kind = $6
             WHERE memory_id = $1",
        )
        .bind(record.memory_id.into_uuid())
        .bind(version.memory_version_id.into_uuid())
        .bind(memory_lifecycle_code(record.lifecycle))
        .bind(record.updated_at.unix_millis())
        .bind(record.revoked_at.map(UtcTimestamp::unix_millis))
        .bind(memory_kind_code(record.kind))
        .execute(&mut *transaction)
        .await?;
        return Ok(MemoryPersistenceDisposition::Applied);
    }
    insert_memory_version(transaction, record, version).await?;
    crate::memory_search_index::persist_search_document(
        transaction,
        search_document,
        version.created_at.unix_millis(),
    )
    .await?;
    Ok(MemoryPersistenceDisposition::Applied)
}

async fn insert_memory_version(
    transaction: &mut sqlx::PgConnection,
    record: &MemoryRecord,
    version: &MemoryVersion,
) -> Result<(), PostgresMemoryEvolutionError> {
    sqlx::query(
        "INSERT INTO memory.memory_versions (
             memory_version_id, memory_id, version, content_id, content_hash,
             sensitivity, confidence, provenance, valid_from_ms, valid_to_ms,
             source, confidence_bps
         ) VALUES (
             $1, $2, $3::bigint, $4, $5,
             $6, $7, $8, $9, $10,
             $11, $12
         )",
    )
    .bind(version.memory_version_id.into_uuid())
    .bind(record.memory_id.into_uuid())
    .bind(version.version.get().to_string())
    .bind(version.content_ref.content_id.into_uuid())
    .bind(version.content_ref.sha256.into_bytes().as_slice())
    .bind(classification_code(version.data_classification))
    .bind(f64::from(version.confidence.get()) / 10_000.0)
    .bind(NORMALIZED_PROVENANCE_MARKER)
    .bind(version.created_at.unix_millis())
    .bind(version.valid_until.map(UtcTimestamp::unix_millis))
    .bind(memory_source_code(version.source))
    .bind(i32::from(version.confidence.get()))
    .execute(&mut *transaction)
    .await?;
    for run_id in &version.source_run_ids {
        sqlx::query(
            "INSERT INTO memory.memory_version_source_runs (memory_version_id, run_id)
             VALUES ($1, $2)",
        )
        .bind(version.memory_version_id.into_uuid())
        .bind(run_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    for record_id in &version.evidence_record_ids {
        sqlx::query(
            "INSERT INTO memory.memory_version_evidence_records (
                 memory_version_id, ledger_record_id
             ) VALUES ($1, $2)",
        )
        .bind(version.memory_version_id.into_uuid())
        .bind(record_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

async fn version_matches(
    transaction: &mut sqlx::PgConnection,
    version: &MemoryVersion,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT memory_id, version::text AS version_number, content_id,
                content_hash, sensitivity, source, confidence_bps,
                valid_from_ms, valid_to_ms
         FROM memory.memory_versions WHERE memory_version_id = $1",
    )
    .bind(version.memory_version_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let base = row.try_get::<Uuid, _>("memory_id")? == version.memory_id.into_uuid()
        && parse_u64(row.try_get("version_number")?, "memory.version")? == version.version.get()
        && row.try_get::<Uuid, _>("content_id")? == version.content_ref.content_id.into_uuid()
        && row.try_get::<Vec<u8>, _>("content_hash")? == version.content_ref.sha256.into_bytes()
        && row.try_get::<i16, _>("sensitivity")?
            == classification_code(version.data_classification)
        && row.try_get::<Option<i16>, _>("source")? == Some(memory_source_code(version.source))
        && row.try_get::<Option<i32>, _>("confidence_bps")?
            == Some(i32::from(version.confidence.get()))
        && row.try_get::<i64, _>("valid_from_ms")? == version.created_at.unix_millis()
        && row.try_get::<Option<i64>, _>("valid_to_ms")?
            == version.valid_until.map(UtcTimestamp::unix_millis);
    if !base {
        return Ok(false);
    }
    let source_run_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT run_id FROM memory.memory_version_source_runs
         WHERE memory_version_id = $1 ORDER BY run_id",
    )
    .bind(version.memory_version_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|value| id::<RunId>(value, "memory.source_run_id"))
    .collect::<Result<BTreeSet<_>, _>>()?;
    let evidence_record_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT ledger_record_id FROM memory.memory_version_evidence_records
         WHERE memory_version_id = $1 ORDER BY ledger_record_id",
    )
    .bind(version.memory_version_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|value| id::<LedgerRecordId>(value, "memory.evidence_record_id"))
    .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(source_run_ids == version.source_run_ids
        && evidence_record_ids == version.evidence_record_ids)
}

async fn memory_record_matches(
    transaction: &mut sqlx::PgConnection,
    record: &MemoryRecord,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT tenant_id, scope_kind, scope_id, memory_key, kind, lifecycle,
                current_version_id, created_at_ms, updated_at_ms, revoked_at_ms
         FROM memory.memories WHERE memory_id = $1",
    )
    .bind(record.memory_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let (scope_kind, scope_id) = memory_scope(&record.scope);
    Ok(
        row.try_get::<Uuid, _>("tenant_id")? == record.tenant_id.into_uuid()
            && row.try_get::<i16, _>("scope_kind")? == scope_kind
            && row.try_get::<Option<Uuid>, _>("scope_id")? == scope_id
            && row.try_get::<String, _>("memory_key")? == record.memory_key.as_str()
            && row.try_get::<i16, _>("kind")? == memory_kind_code(record.kind)
            && row.try_get::<i16, _>("lifecycle")? == memory_lifecycle_code(record.lifecycle)
            && row.try_get::<Option<Uuid>, _>("current_version_id")?
                == Some(record.current_version_id.into_uuid())
            && row.try_get::<i64, _>("created_at_ms")? == record.created_at.unix_millis()
            && row.try_get::<i64, _>("updated_at_ms")? == record.updated_at.unix_millis()
            && row.try_get::<Option<i64>, _>("revoked_at_ms")?
                == record.revoked_at.map(UtcTimestamp::unix_millis),
    )
}

async fn insert_proposal_provenance(
    transaction: &mut sqlx::PgConnection,
    candidate: &ExperienceCandidate,
) -> Result<(), PostgresMemoryEvolutionError> {
    for run_id in &candidate.source_run_ids {
        sqlx::query(
            "INSERT INTO evolution.proposal_source_runs (proposal_id, run_id)
             VALUES ($1, $2)",
        )
        .bind(candidate.proposal_id.into_uuid())
        .bind(run_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    for record_id in &candidate.evidence_record_ids {
        sqlx::query(
            "INSERT INTO evolution.proposal_evidence_records (
                 proposal_id, ledger_record_id
             ) VALUES ($1, $2)",
        )
        .bind(candidate.proposal_id.into_uuid())
        .bind(record_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

pub(crate) async fn proposal_matches(
    transaction: &mut sqlx::PgConnection,
    candidate: &ExperienceCandidate,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT tenant_id, scope_kind, scope_id, proposal_kind, artifact_kind,
                candidate_content_id, candidate_content_hash, status,
                confidence_bps, proposed_at_ms, proposed_by,
                target_memory_id, supersedes_proposal_id, proposal_origin, source_draft_id
         FROM evolution.proposals WHERE proposal_id = $1",
    )
    .bind(candidate.proposal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let (scope_kind, scope_id) = evolution_scope(&candidate.target_scope);
    let base = row.try_get::<Uuid, _>("tenant_id")? == candidate.tenant_id.into_uuid()
        && row.try_get::<i16, _>("scope_kind")? == scope_kind
        && row.try_get::<Option<Uuid>, _>("scope_id")? == scope_id
        && row.try_get::<i16, _>("proposal_kind")? == experience_kind_code(candidate.kind)
        && row.try_get::<Option<i16>, _>("artifact_kind")?
            == experience_artifact_kind_code(candidate.kind)
        && row.try_get::<Uuid, _>("candidate_content_id")?
            == candidate.claim_ref.content_id.into_uuid()
        && row.try_get::<Option<Vec<u8>>, _>("candidate_content_hash")?
            == Some(candidate.claim_ref.sha256.into_bytes().to_vec())
        && row.try_get::<i16, _>("status")? == candidate_status_code(candidate.status)
        && row.try_get::<Option<i32>, _>("confidence_bps")?
            == candidate.confidence.map(|value| i32::from(value.get()))
        && row.try_get::<i64, _>("proposed_at_ms")? == candidate.created_at.unix_millis()
        && row.try_get::<Option<Uuid>, _>("proposed_by")?
            == Some(candidate.proposed_by.into_uuid())
        && row.try_get::<Option<Uuid>, _>("target_memory_id")?
            == candidate.target_memory_id.map(MemoryId::into_uuid)
        && row.try_get::<Option<Uuid>, _>("supersedes_proposal_id")?
            == candidate
                .supersedes_proposal_id
                .map(EvolutionProposalId::into_uuid)
        && row.try_get::<i16, _>("proposal_origin")? == candidate_origin_code(candidate.origin)
        && row.try_get::<Option<Uuid>, _>("source_draft_id")?
            == candidate.source_draft_id.map(MemoryDraftId::into_uuid);
    if !base {
        return Ok(false);
    }
    let source_run_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT run_id FROM evolution.proposal_source_runs
         WHERE proposal_id = $1 ORDER BY run_id",
    )
    .bind(candidate.proposal_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|value| id::<RunId>(value, "evolution.source_run_id"))
    .collect::<Result<BTreeSet<_>, _>>()?;
    let evidence_record_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT ledger_record_id FROM evolution.proposal_evidence_records
         WHERE proposal_id = $1 ORDER BY ledger_record_id",
    )
    .bind(candidate.proposal_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|value| id::<LedgerRecordId>(value, "evolution.evidence_record_id"))
    .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(source_run_ids == candidate.source_run_ids
        && evidence_record_ids == candidate.evidence_record_ids)
}

pub(crate) async fn evaluation_matches(
    transaction: &mut sqlx::PgConnection,
    evaluation: &EvolutionEvaluation,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT proposal_id, evaluated_by, passed, report_content_id,
                report_content_hash, evaluated_at_ms
         FROM evolution.evaluations WHERE evaluation_id = $1",
    )
    .bind(evaluation.evaluation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    Ok(
        row.try_get::<Uuid, _>("proposal_id")? == evaluation.proposal_id.into_uuid()
            && row.try_get::<Option<Uuid>, _>("evaluated_by")?
                == Some(evaluation.evaluated_by.into_uuid())
            && row.try_get::<Option<bool>, _>("passed")? == Some(evaluation.passed)
            && row.try_get::<Option<Uuid>, _>("report_content_id")?
                == Some(evaluation.report_ref.content_id.into_uuid())
            && row.try_get::<Option<Vec<u8>>, _>("report_content_hash")?
                == Some(evaluation.report_ref.sha256.into_bytes().to_vec())
            && row.try_get::<i64, _>("evaluated_at_ms")? == evaluation.evaluated_at.unix_millis(),
    )
}

pub(crate) async fn approval_matches(
    transaction: &mut sqlx::PgConnection,
    approval: &EvolutionApproval,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT proposal_id, evaluation_id, prompt_content_id,
                prompt_content_hash, approved_by, approved_at_ms
         FROM evolution.approvals WHERE approval_interaction_id = $1",
    )
    .bind(approval.approval_interaction_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    Ok(row.is_some_and(|row| {
        row.try_get::<Uuid, _>("proposal_id").ok() == Some(approval.proposal_id.into_uuid())
            && row.try_get::<Uuid, _>("evaluation_id").ok()
                == Some(approval.evaluation_id.into_uuid())
            && row.try_get::<Uuid, _>("prompt_content_id").ok()
                == Some(approval.prompt_ref.content_id.into_uuid())
            && row.try_get::<Vec<u8>, _>("prompt_content_hash").ok()
                == Some(approval.prompt_ref.sha256.into_bytes().to_vec())
            && row.try_get::<Uuid, _>("approved_by").ok() == Some(approval.approved_by.into_uuid())
            && row.try_get::<i64, _>("approved_at_ms").ok()
                == Some(approval.approved_at.unix_millis())
    }))
}

async fn promotion_matches(
    transaction: &mut sqlx::PgConnection,
    promotion: &EvolutionPromotion,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT promotion_id, published_memory_version_id,
                evaluation_id, approval_interaction_id,
                promoted_by, promoted_at_ms
         FROM evolution.promotions WHERE proposal_id = $1",
    )
    .bind(promotion.proposal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    Ok(row.is_some_and(|row| {
        row.try_get::<Uuid, _>("promotion_id").ok() == Some(promotion.promotion_id.into_uuid())
            && row
                .try_get::<Option<Uuid>, _>("published_memory_version_id")
                .ok()
                == Some(Some(promotion.published_memory_version_id.into_uuid()))
            && row.try_get::<Uuid, _>("promoted_by").ok() == Some(promotion.promoted_by.into_uuid())
            && row.try_get::<Option<Uuid>, _>("evaluation_id").ok()
                == Some(Some(promotion.evaluation_id.into_uuid()))
            && row
                .try_get::<Option<Uuid>, _>("approval_interaction_id")
                .ok()
                == Some(Some(promotion.approval_interaction_id.into_uuid()))
            && row.try_get::<i64, _>("promoted_at_ms").ok()
                == Some(promotion.promoted_at.unix_millis())
    }))
}

pub(crate) fn decode_memory_hit(
    row: sqlx::postgres::PgRow,
) -> Result<MemorySearchHit, PostgresMemoryEvolutionError> {
    let memory_id = id::<MemoryId>(row.try_get("memory_id")?, "memory.memory_id")?;
    let tenant_id = id::<TenantId>(row.try_get("tenant_id")?, "memory.tenant_id")?;
    let scope_id: Option<Uuid> = row.try_get("scope_id")?;
    let scope = decode_memory_scope(row.try_get("scope_kind")?, scope_id)?;
    let memory_version_id =
        id::<MemoryVersionId>(row.try_get("memory_version_id")?, "memory.version_id")?;
    let content_ref = ContentRef {
        content_id: id(row.try_get("content_id")?, "memory.content_id")?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        byte_length: parse_u64(row.try_get("byte_length")?, "memory.byte_length")?,
        sha256: digest(row.try_get("content_hash")?)?,
    };
    let source_run_ids = BTreeSet::new();
    let evidence_record_ids = BTreeSet::new();
    Ok(MemorySearchHit {
        record: MemoryRecord {
            memory_id,
            tenant_id,
            scope,
            memory_key: IdempotencyKey::new(row.try_get::<String, _>("memory_key")?)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            kind: decode_memory_kind(row.try_get("kind")?)?,
            lifecycle: decode_memory_lifecycle(row.try_get("lifecycle")?)?,
            current_version_id: id(
                row.try_get("current_version_id")?,
                "memory.current_version_id",
            )?,
            created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
            updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
            revoked_at: row
                .try_get::<Option<i64>, _>("revoked_at_ms")?
                .map(UtcTimestamp::from_unix_millis),
        },
        version: MemoryVersion {
            memory_version_id,
            memory_id,
            version: VersionNumber::new(parse_u64(
                row.try_get("version_number")?,
                "memory.version",
            )?)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            source: decode_memory_source(
                row.try_get::<Option<i16>, _>("source")?
                    .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            )?,
            content_ref,
            confidence: MemoryConfidenceBps::new(
                u16::try_from(
                    row.try_get::<Option<i32>, _>("confidence_bps")?
                        .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
                )
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            )?,
            data_classification: decode_classification(row.try_get("sensitivity")?)?,
            source_run_ids,
            evidence_record_ids,
            created_at: UtcTimestamp::from_unix_millis(row.try_get("valid_from_ms")?),
            valid_until: row
                .try_get::<Option<i64>, _>("valid_to_ms")?
                .map(UtcTimestamp::from_unix_millis),
        },
    })
}

pub(crate) fn decode_candidate(
    row: sqlx::postgres::PgRow,
    proposal_id: EvolutionProposalId,
    source_run_ids: BTreeSet<RunId>,
    evidence_record_ids: BTreeSet<LedgerRecordId>,
) -> Result<ExperienceCandidate, PostgresMemoryEvolutionError> {
    let scope_id: Uuid = row
        .try_get::<Option<Uuid>, _>("scope_id")?
        .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
    let target_scope = match row.try_get::<i16, _>("scope_kind")? {
        1 => EvolutionScope::User {
            principal_id: id(scope_id, "evolution.scope_id")?,
        },
        2 => EvolutionScope::Agent {
            agent_id: id(scope_id, "evolution.scope_id")?,
        },
        _ => return Err(PostgresMemoryEvolutionError::CorruptDatabase),
    };
    Ok(ExperienceCandidate {
        proposal_id,
        tenant_id: id(row.try_get("tenant_id")?, "evolution.tenant_id")?,
        proposed_by: id(
            row.try_get::<Option<Uuid>, _>("proposed_by")?
                .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
            "evolution.proposed_by",
        )?,
        origin: decode_candidate_origin(row.try_get("proposal_origin")?)?,
        source_draft_id: row
            .try_get::<Option<Uuid>, _>("source_draft_id")?
            .map(|value| id(value, "evolution.source_draft_id"))
            .transpose()?,
        target_scope,
        target_memory_id: row
            .try_get::<Option<Uuid>, _>("target_memory_id")?
            .map(|value| id(value, "evolution.target_memory_id"))
            .transpose()?,
        supersedes_proposal_id: row
            .try_get::<Option<Uuid>, _>("supersedes_proposal_id")?
            .map(|value| id(value, "evolution.supersedes_proposal_id"))
            .transpose()?,
        kind: decode_experience_kind(row.try_get("proposal_kind")?, row.try_get("artifact_kind")?)?,
        claim_ref: ContentRef {
            content_id: id(row.try_get("content_id")?, "evolution.content_id")?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            byte_length: parse_u64(row.try_get("byte_length")?, "evolution.byte_length")?,
            sha256: digest(row.try_get("content_hash")?)?,
        },
        source_run_ids,
        evidence_record_ids,
        status: decode_candidate_status(row.try_get("status")?)?,
        confidence: row
            .try_get::<Option<i32>, _>("confidence_bps")?
            .map(|value| {
                u16::try_from(value)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
                    .and_then(|value| ConfidenceBps::new(value).map_err(Into::into))
            })
            .transpose()?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("proposed_at_ms")?),
    })
}

fn memory_scope(value: &MemoryScope) -> (i16, Option<Uuid>) {
    match value {
        MemoryScope::User { principal_id } => (1, Some(principal_id.into_uuid())),
        MemoryScope::Tenant { tenant_id } => (2, Some(tenant_id.into_uuid())),
        MemoryScope::Workspace { workspace_id } => (3, Some(workspace_id.into_uuid())),
        MemoryScope::Agent { agent_id } => (4, Some(agent_id.into_uuid())),
    }
}

fn evolution_scope(value: &EvolutionScope) -> (i16, Option<Uuid>) {
    match value {
        EvolutionScope::User { principal_id } => (1, Some(principal_id.into_uuid())),
        EvolutionScope::Agent { agent_id } => (2, Some(agent_id.into_uuid())),
    }
}

pub(crate) const fn memory_kind_code(value: MemoryKind) -> i16 {
    match value {
        MemoryKind::Preference => 1,
        MemoryKind::Semantic => 2,
        MemoryKind::Procedure => 3,
        MemoryKind::FailureLesson => 4,
    }
}

const fn memory_lifecycle_code(value: MemoryLifecycle) -> i16 {
    match value {
        MemoryLifecycle::Active => 1,
        MemoryLifecycle::Superseded => 2,
        MemoryLifecycle::Revoked => 3,
        MemoryLifecycle::Expired => 4,
    }
}

const fn memory_source_code(value: MemorySource) -> i16 {
    match value {
        MemorySource::UserAuthored => 1,
        MemorySource::VerifiedExperience => 2,
        MemorySource::GovernedImport => 3,
    }
}

const fn classification_code(value: DataClassification) -> i16 {
    match value {
        DataClassification::Public => 1,
        DataClassification::Internal => 2,
        DataClassification::Confidential => 3,
        DataClassification::Restricted => 4,
    }
}

const fn experience_kind_code(value: ExperienceKind) -> i16 {
    match value {
        ExperienceKind::Semantic => 1,
        ExperienceKind::Procedure => 2,
        ExperienceKind::Failure => 3,
        ExperienceKind::Preference => 4,
        ExperienceKind::PolicyProposal
        | ExperienceKind::Skill
        | ExperienceKind::Instinct
        | ExperienceKind::PersonaPatch => 5,
    }
}

const fn experience_artifact_kind_code(value: ExperienceKind) -> Option<i16> {
    match value {
        ExperienceKind::Skill => Some(1),
        ExperienceKind::Instinct => Some(2),
        ExperienceKind::PersonaPatch => Some(3),
        ExperienceKind::Semantic
        | ExperienceKind::Procedure
        | ExperienceKind::Failure
        | ExperienceKind::Preference
        | ExperienceKind::PolicyProposal => None,
    }
}

pub(crate) const fn candidate_origin_code(value: ExperienceCandidateOrigin) -> i16 {
    match value {
        ExperienceCandidateOrigin::RuntimeTool => 1,
        ExperienceCandidateOrigin::DreamInduction => 2,
        ExperienceCandidateOrigin::UserDraft => 3,
    }
}

pub(crate) const fn candidate_status_code(value: CandidateStatus) -> i16 {
    match value {
        CandidateStatus::Candidate => 1,
        CandidateStatus::Quarantined => 2,
        CandidateStatus::Verified => 3,
        CandidateStatus::Rejected => 4,
        CandidateStatus::Promoted => 5,
    }
}

mod row_codec;
pub(crate) use row_codec::*;

pub(super) fn dialogue_source_error(
    error: musteragent_runtime_session::DialogueStoreError,
) -> PostgresMemoryEvolutionError {
    use musteragent_runtime_session::DialogueStoreError as E;
    match error {
        E::Infrastructure => PostgresMemoryEvolutionError::SourceUnavailable,
        E::Corrupt => PostgresMemoryEvolutionError::CorruptDatabase,
        _ => PostgresMemoryEvolutionError::ApprovalNotSatisfied,
    }
}
