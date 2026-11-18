mod authority;
mod evaluation;
mod inspection;
mod receipts;
mod review;
use receipts::{load_batch, load_confirmation};
use review::{list_batch_reviews, load_batch_review};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{CandidateStatus, EvolutionScope, ExperienceCandidateOrigin};
use musteragent_identity::{
    AgentId, EvolutionProposalId, MemoryDraftBatchId, MemoryDraftId, MemoryId, MemoryVersionId,
    PrincipalId, TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    AgentMemoryDraft, AgentMemoryDraftBatchMutation, AgentMemoryDraftDisposition,
    AgentMemoryDraftEvaluationScope, AgentMemoryDraftMutation, AgentMemoryDraftPage,
    AgentMemoryDraftPublicationBatchMutation, AgentMemoryDraftPublicationItem,
    AgentMemoryDraftReviewBatch, AgentMemoryDraftSaveBatchMutation, AgentMemoryDraftStatus,
    AgentMemoryDraftStore, ConfirmAgentMemoryDraftBatch, GetAgentMemoryDraftConfirmation,
    GetAgentMemoryDraftSubmission, ListAgentMemoryDrafts, MemoryPersistenceDisposition,
    MemorySearchHit, PendingAgentMemoryDraftEvaluation, SaveAgentMemoryDraft,
    SaveAgentMemoryDraftBatch, SubmitAgentMemoryDraftBatch,
};
use sqlx::Row as _;
use uuid::Uuid;

use crate::PostgresMemoryEvolutionError;
use crate::memory::{
    PostgresMemoryEvolutionStore, advisory_lock, assert_expected_memory_head,
    candidate_status_code, decode_memory_hit, ensure_content_ref, evaluation_matches,
    persist_memory, proposal_matches, propose_transaction,
};
use crate::memory_publication::{insert_evolution_publication_outbox, mark_draft_published};

impl AgentMemoryDraftStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn save_draft(
        &self,
        command: SaveAgentMemoryDraft,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftMutation, Self::Error>> {
        Box::pin(async move { save_draft(self, command).await })
    }

    fn save_draft_batch(
        &self,
        command: SaveAgentMemoryDraftBatch,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftSaveBatchMutation, Self::Error>> {
        Box::pin(async move { save_draft_batch(self, command).await })
    }

    fn list_editable_heads(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        agent_id: AgentId,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Vec<MemorySearchHit>, Self::Error>> {
        Box::pin(async move {
            list_editable_heads(self, tenant_id, owner_principal_id, agent_id, now).await
        })
    }

    fn load_draft(
        &self,
        tenant_id: TenantId,
        owner_principal_id: musteragent_identity::PrincipalId,
        draft_id: MemoryDraftId,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraft>, Self::Error>> {
        Box::pin(async move {
            let mut tx = self.draft_read_transaction().await?;
            let draft =
                load_draft_in_transaction(&mut tx, tenant_id, owner_principal_id, draft_id).await?;
            if let Some(draft) = &draft {
                self.authorize_draft_transaction(
                    &mut tx,
                    tenant_id,
                    owner_principal_id,
                    draft.agent_id,
                )
                .await?;
            }
            tx.rollback().await?;
            Ok(draft)
        })
    }

    fn submit_batch(
        &self,
        command: SubmitAgentMemoryDraftBatch,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftBatchMutation, Self::Error>> {
        Box::pin(async move { submit_batch(self, command).await })
    }

    fn list_drafts(
        &self,
        query: ListAgentMemoryDrafts,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftPage, Self::Error>> {
        Box::pin(async move { inspection::list_drafts(self, query).await })
    }

    fn load_submission(
        &self,
        query: GetAgentMemoryDraftSubmission,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraftBatchMutation>, Self::Error>> {
        Box::pin(async move {
            let mut transaction = self.draft_read_transaction().await?;
            self.authorize_draft_transaction(
                &mut transaction,
                query.tenant_id,
                query.owner_principal_id,
                query.agent_id,
            )
            .await?;
            let receipt = load_batch(&mut transaction, &query).await?;
            transaction.rollback().await?;
            Ok(receipt)
        })
    }

    fn load_next_draft_candidate(
        &self,
        scope: AgentMemoryDraftEvaluationScope,
    ) -> ApplicationFuture<'_, Result<Option<PendingAgentMemoryDraftEvaluation>, Self::Error>> {
        Box::pin(async move { evaluation::load_next_draft_candidate(self, scope).await })
    }

    fn load_batch_review(
        &self,
        tenant_id: TenantId,
        owner_principal_id: musteragent_identity::PrincipalId,
        batch_id: musteragent_identity::MemoryDraftBatchId,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraftReviewBatch>, Self::Error>> {
        Box::pin(
            async move { load_batch_review(self, tenant_id, owner_principal_id, batch_id).await },
        )
    }

    fn list_batch_reviews(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        agent_id: AgentId,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<Vec<AgentMemoryDraftReviewBatch>, Self::Error>> {
        Box::pin(async move {
            list_batch_reviews(self, tenant_id, owner_principal_id, agent_id, limit).await
        })
    }

    fn load_confirmation(
        &self,
        query: GetAgentMemoryDraftConfirmation,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraftPublicationBatchMutation>, Self::Error>>
    {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await?;
            self.authorize_draft_transaction(
                &mut transaction,
                query.tenant_id,
                query.owner_principal_id,
                query.agent_id,
            )
            .await?;
            let confirmation = load_confirmation(&mut transaction, &query).await?;
            transaction.rollback().await?;
            Ok(confirmation)
        })
    }

    fn confirm_batch(
        &self,
        command: ConfirmAgentMemoryDraftBatch,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftPublicationBatchMutation, Self::Error>> {
        Box::pin(async move { confirm_batch(self, command).await })
    }
}

async fn save_draft(
    store: &PostgresMemoryEvolutionStore,
    command: SaveAgentMemoryDraft,
) -> Result<AgentMemoryDraftMutation, PostgresMemoryEvolutionError> {
    let mut mutation = save_draft_batch(
        store,
        SaveAgentMemoryDraftBatch {
            drafts: vec![command.draft],
        },
    )
    .await?;
    let draft = mutation
        .drafts
        .pop()
        .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
    Ok(AgentMemoryDraftMutation {
        disposition: mutation.disposition,
        draft,
    })
}

async fn save_draft_batch(
    store: &PostgresMemoryEvolutionStore,
    command: SaveAgentMemoryDraftBatch,
) -> Result<AgentMemoryDraftSaveBatchMutation, PostgresMemoryEvolutionError> {
    command
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::DraftConflict)?;
    let mut drafts = command.drafts;
    drafts.sort_unstable_by_key(|draft| (draft.memory_id, draft.draft_id));
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
        .await?;
    let mut applied = false;
    for draft in &mut drafts {
        store
            .authorize_draft_transaction(
                &mut transaction,
                draft.tenant_id,
                draft.owner_principal_id,
                draft.agent_id,
            )
            .await?;
        applied |= save_one_draft(&mut transaction, draft).await?;
    }
    transaction.commit().await?;
    Ok(AgentMemoryDraftSaveBatchMutation {
        disposition: if applied {
            AgentMemoryDraftDisposition::Applied
        } else {
            AgentMemoryDraftDisposition::Duplicate
        },
        drafts,
    })
}

async fn save_one_draft(
    transaction: &mut sqlx::PgConnection,
    draft: &mut AgentMemoryDraft,
) -> Result<bool, PostgresMemoryEvolutionError> {
    advisory_lock(
        transaction,
        draft.tenant_id,
        &format!("memory-draft:{}", draft.memory_id),
    )
    .await?;
    if let Some(existing) = load_draft_in_transaction(
        transaction,
        draft.tenant_id,
        draft.owner_principal_id,
        draft.draft_id,
    )
    .await?
    {
        if !existing.same_save_intent(draft) {
            return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
        }
        *draft = existing;
        return Ok(false);
    }
    ensure_content_ref(transaction, draft.tenant_id, &draft.content_ref).await?;
    let head = active_checkout_head(
        transaction,
        draft.tenant_id,
        draft.owner_principal_id,
        draft.agent_id,
        draft.memory_id,
    )
    .await?
    .ok_or(PostgresMemoryEvolutionError::MemoryNotFound)?;
    if head != draft.baseline_memory_version_id {
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    if let Some(superseded) = draft.supersedes_draft_id {
        let eligible: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM memory.agent_memory_drafts
                WHERE tenant_id=$1 AND owner_principal_id=$2 AND draft_id=$3
                  AND agent_id=$4 AND memory_id=$5 AND status=3
             )",
        )
        .bind(draft.tenant_id.into_uuid())
        .bind(draft.owner_principal_id.into_uuid())
        .bind(superseded.into_uuid())
        .bind(draft.agent_id.into_uuid())
        .bind(draft.memory_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        if !eligible {
            return Err(PostgresMemoryEvolutionError::DraftConflict);
        }
    }
    let inserted = sqlx::query(
        "INSERT INTO memory.agent_memory_drafts (
             draft_id,tenant_id,owner_principal_id,agent_id,memory_id,
             baseline_memory_version_id,content_id,content_media_type,
             content_byte_length,content_sha256,status,proposal_id,
             conflicting_memory_version_id,supersedes_draft_id,idempotency_key,
             created_at_ms,updated_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10,1,NULL,NULL,$11,$12,$13,$13)
         ON CONFLICT DO NOTHING",
    )
    .bind(draft.draft_id.into_uuid())
    .bind(draft.tenant_id.into_uuid())
    .bind(draft.owner_principal_id.into_uuid())
    .bind(draft.agent_id.into_uuid())
    .bind(draft.memory_id.into_uuid())
    .bind(draft.baseline_memory_version_id.into_uuid())
    .bind(draft.content_ref.content_id.into_uuid())
    .bind(draft.content_ref.media_type.as_str())
    .bind(draft.content_ref.byte_length.to_string())
    .bind(draft.content_ref.sha256.into_bytes().as_slice())
    .bind(draft.supersedes_draft_id.map(MemoryDraftId::into_uuid))
    .bind(draft.idempotency_key.as_str())
    .bind(draft.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 1 {
        Ok(true)
    } else {
        let existing = load_draft_in_transaction(
            transaction,
            draft.tenant_id,
            draft.owner_principal_id,
            draft.draft_id,
        )
        .await?;
        let existing = existing.ok_or(PostgresMemoryEvolutionError::DraftIdempotencyConflict)?;
        if !existing.same_save_intent(draft) {
            return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
        }
        *draft = existing;
        Ok(false)
    }
}

async fn list_editable_heads(
    store: &PostgresMemoryEvolutionStore,
    tenant_id: TenantId,
    owner_principal_id: PrincipalId,
    agent_id: AgentId,
    now: UtcTimestamp,
) -> Result<Vec<MemorySearchHit>, PostgresMemoryEvolutionError> {
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
         WHERE memory.tenant_id=$1 AND memory.lifecycle=1
           AND (version.valid_to_ms IS NULL OR version.valid_to_ms > $2)
           AND ((memory.scope_kind=1 AND memory.scope_id=$3)
                OR (memory.scope_kind=4 AND memory.scope_id=$4))
           AND (
                version.source <> 1
                OR EXISTS (
                    SELECT 1 FROM memory.explicit_publications AS publication
                    WHERE publication.memory_version_id=version.memory_version_id
                      AND publication.memory_id=memory.memory_id
                      AND publication.tenant_id=memory.tenant_id
                )
                OR EXISTS (
                    SELECT 1 FROM memory.governance_publication_outbox AS publication
                    WHERE publication.memory_version_id=version.memory_version_id
                      AND publication.memory_id=memory.memory_id
                      AND publication.tenant_id=memory.tenant_id
                )
           )
         ORDER BY memory.scope_kind, memory.memory_key, memory.memory_id",
    )
    .bind(tenant_id.into_uuid())
    .bind(now.unix_millis())
    .bind(owner_principal_id.into_uuid())
    .bind(agent_id.into_uuid())
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(decode_memory_hit).collect()
}

async fn submit_batch(
    store: &PostgresMemoryEvolutionStore,
    command: SubmitAgentMemoryDraftBatch,
) -> Result<AgentMemoryDraftBatchMutation, PostgresMemoryEvolutionError> {
    command
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::DraftConflict)?;
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
        .await?;
    store
        .authorize_draft_transaction(
            &mut transaction,
            command.tenant_id,
            command.owner_principal_id,
            command.agent_id,
        )
        .await?;
    advisory_lock(
        &mut transaction,
        command.tenant_id,
        &format!(
            "memory-draft-submit:{}:{}",
            command.owner_principal_id,
            command.idempotency_key.as_str()
        ),
    )
    .await?;
    if let Some(existing) = load_batch(
        &mut transaction,
        &GetAgentMemoryDraftSubmission::from(&command),
    )
    .await?
    {
        if !existing.matches_submission(&command) {
            return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
        }
        transaction.rollback().await?;
        return Ok(existing);
    }
    // Every transaction acquires Memory/Draft locks in one global order. This
    // keeps concurrent overlapping batches deadlock-free without weakening
    // the all-or-nothing baseline check.
    let mut ordered_items = command.drafts.iter().collect::<Vec<_>>();
    ordered_items.sort_unstable_by_key(|item| (item.candidate.target_memory_id, item.draft_id));
    let mut locked = std::collections::BTreeMap::new();
    for item in ordered_items {
        let mut draft = load_draft_for_update(
            &mut transaction,
            command.tenant_id,
            command.owner_principal_id,
            item.draft_id,
        )
        .await?
        .ok_or(PostgresMemoryEvolutionError::DraftNotFound)?;
        let candidate = &item.candidate;
        if draft.status != AgentMemoryDraftStatus::Draft
            || draft.agent_id != command.agent_id
            || draft.baseline_memory_version_id != item.expected_baseline_memory_version_id
            || candidate.origin != ExperienceCandidateOrigin::UserDraft
            || candidate.source_draft_id != Some(draft.draft_id)
            || candidate.target_memory_id != Some(draft.memory_id)
            || candidate.claim_ref != draft.content_ref
            || !draft_scope_matches(candidate.target_scope, &draft)
        {
            return Err(PostgresMemoryEvolutionError::DraftConflict);
        }
        advisory_lock(
            &mut transaction,
            command.tenant_id,
            &format!("memory-draft:{}", draft.memory_id),
        )
        .await?;
        let head = active_checkout_head(
            &mut transaction,
            command.tenant_id,
            command.owner_principal_id,
            command.agent_id,
            draft.memory_id,
        )
        .await?
        .ok_or(PostgresMemoryEvolutionError::MemoryNotFound)?;
        if head != draft.baseline_memory_version_id {
            sqlx::query(
                "UPDATE memory.agent_memory_drafts
                 SET status=3,conflicting_memory_version_id=$1,updated_at_ms=$2
                 WHERE tenant_id=$3 AND draft_id=$4 AND status=1",
            )
            .bind(head.into_uuid())
            .bind(command.submitted_at.unix_millis())
            .bind(command.tenant_id.into_uuid())
            .bind(draft.draft_id.into_uuid())
            .execute(&mut *transaction)
            .await?;
            draft.status = AgentMemoryDraftStatus::Conflict;
            draft.conflicting_memory_version_id = Some(head);
            draft.updated_at = command.submitted_at;
            locked.insert(draft.draft_id, (draft, None));
        } else {
            locked.insert(draft.draft_id, (draft, Some(candidate.clone())));
        }
    }
    if locked.values().any(|(_, candidate)| candidate.is_none()) {
        transaction.commit().await?;
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    sqlx::query(
        "INSERT INTO memory.agent_memory_draft_batches (
             batch_id,tenant_id,owner_principal_id,agent_id,idempotency_key,submitted_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(command.batch_id.into_uuid())
    .bind(command.tenant_id.into_uuid())
    .bind(command.owner_principal_id.into_uuid())
    .bind(command.agent_id.into_uuid())
    .bind(command.idempotency_key.as_str())
    .bind(command.submitted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    let mut submitted = Vec::with_capacity(locked.len());
    for (ordinal, item) in command.drafts.iter().enumerate() {
        let (mut draft, candidate) = locked
            .remove(&item.draft_id)
            .ok_or(PostgresMemoryEvolutionError::DraftConflict)?;
        let candidate = candidate.expect("conflicts returned before Candidate insertion");
        propose_transaction(&mut transaction, &candidate).await?;
        sqlx::query(
            "UPDATE memory.agent_memory_drafts
             SET status=2,proposal_id=$1,updated_at_ms=$2
             WHERE tenant_id=$3 AND draft_id=$4 AND status=1",
        )
        .bind(candidate.proposal_id.into_uuid())
        .bind(command.submitted_at.unix_millis())
        .bind(command.tenant_id.into_uuid())
        .bind(draft.draft_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO memory.agent_memory_draft_batch_items
             (tenant_id,batch_id,ordinal,draft_id,proposal_id)
             VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.batch_id.into_uuid())
        .bind(i32::try_from(ordinal).map_err(|_| PostgresMemoryEvolutionError::NumericRange)?)
        .bind(draft.draft_id.into_uuid())
        .bind(candidate.proposal_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        draft.status = AgentMemoryDraftStatus::Submitted;
        draft.proposal_id = Some(candidate.proposal_id);
        draft.updated_at = command.submitted_at;
        submitted.push(draft);
    }
    transaction.commit().await?;
    Ok(AgentMemoryDraftBatchMutation {
        disposition: AgentMemoryDraftDisposition::Applied,
        batch_id: command.batch_id,
        drafts: submitted,
    })
}

async fn confirm_batch(
    store: &PostgresMemoryEvolutionStore,
    command: ConfirmAgentMemoryDraftBatch,
) -> Result<AgentMemoryDraftPublicationBatchMutation, PostgresMemoryEvolutionError> {
    command
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::DraftConflict)?;
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
        .await?;
    store
        .authorize_draft_transaction(
            &mut transaction,
            command.tenant_id,
            command.owner_principal_id,
            command.agent_id,
        )
        .await?;
    advisory_lock(
        &mut transaction,
        command.tenant_id,
        &format!("memory-draft-confirm:{}", command.batch_id),
    )
    .await?;
    let confirmation_query = GetAgentMemoryDraftConfirmation {
        batch_id: command.batch_id,
        tenant_id: command.tenant_id,
        owner_principal_id: command.owner_principal_id,
        agent_id: command.agent_id,
        idempotency_key: command.idempotency_key.clone(),
    };
    if let Some(existing) = load_confirmation(&mut transaction, &confirmation_query).await? {
        transaction.rollback().await?;
        return Ok(existing);
    }
    let batch = sqlx::query(
        "SELECT agent_id,submitted_at_ms
         FROM memory.agent_memory_draft_batches
         WHERE tenant_id=$1 AND owner_principal_id=$2 AND batch_id=$3
         FOR UPDATE",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.owner_principal_id.into_uuid())
    .bind(command.batch_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresMemoryEvolutionError::DraftNotFound)?;
    if parse_id::<musteragent_identity::AgentId>(batch.try_get("agent_id")?)? != command.agent_id {
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    let requested_at_ms: i64 = batch.try_get("submitted_at_ms")?;
    let expected_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.agent_memory_draft_batch_items
         WHERE tenant_id=$1 AND batch_id=$2",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.batch_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if usize::try_from(expected_count).ok() != Some(command.publications.len()) {
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    sqlx::query(
        "INSERT INTO memory.agent_memory_draft_publication_batches (
             tenant_id,batch_id,owner_principal_id,agent_id,idempotency_key,confirmed_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.batch_id.into_uuid())
    .bind(command.owner_principal_id.into_uuid())
    .bind(command.agent_id.into_uuid())
    .bind(command.idempotency_key.as_str())
    .bind(command.confirmed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    let mut ordered = command.publications.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|publication| publication.memory_record.memory_id);
    for publication in ordered {
        publish_management_revision(&mut transaction, &command, publication, requested_at_ms)
            .await?;
    }
    transaction.commit().await?;
    Ok(AgentMemoryDraftPublicationBatchMutation {
        disposition: AgentMemoryDraftDisposition::Applied,
        batch_id: command.batch_id,
        items: command
            .publications
            .iter()
            .map(AgentMemoryDraftPublicationItem::from_publication)
            .collect::<Result<_, _>>()
            .map_err(|_| PostgresMemoryEvolutionError::DraftConflict)?,
    })
}

async fn publish_management_revision(
    transaction: &mut sqlx::PgConnection,
    batch: &ConfirmAgentMemoryDraftBatch,
    publication: &musteragent_runtime_memory::PromoteExperience,
    requested_at_ms: i64,
) -> Result<(), PostgresMemoryEvolutionError> {
    publication.validate()?;
    let draft_id = publication
        .candidate
        .source_draft_id
        .ok_or(PostgresMemoryEvolutionError::DraftConflict)?;
    let row = sqlx::query(
        "SELECT item.ordinal,draft.status,draft.baseline_memory_version_id,
                draft.memory_id,draft.proposal_id,proposal.status AS proposal_status
         FROM memory.agent_memory_draft_batch_items AS item
         JOIN memory.agent_memory_drafts AS draft
           ON draft.tenant_id=item.tenant_id AND draft.draft_id=item.draft_id
         JOIN evolution.proposals AS proposal ON proposal.proposal_id=item.proposal_id
         WHERE item.tenant_id=$1 AND item.batch_id=$2 AND item.draft_id=$3
           AND item.proposal_id=$4
         FOR UPDATE OF draft,proposal",
    )
    .bind(batch.tenant_id.into_uuid())
    .bind(batch.batch_id.into_uuid())
    .bind(draft_id.into_uuid())
    .bind(publication.candidate.proposal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresMemoryEvolutionError::DraftNotFound)?;
    if row.try_get::<i16, _>("status")? != 2
        || row.try_get::<i16, _>("proposal_status")?
            != candidate_status_code(CandidateStatus::Verified)
        || parse_id::<MemoryVersionId>(row.try_get("baseline_memory_version_id")?)?
            != publication
                .expected_current_version_id
                .ok_or(PostgresMemoryEvolutionError::DraftConflict)?
        || parse_id::<MemoryId>(row.try_get("memory_id")?)? != publication.memory_record.memory_id
        || parse_id::<EvolutionProposalId>(row.try_get("proposal_id")?)?
            != publication.candidate.proposal_id
    {
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    ensure_content_ref(
        transaction,
        batch.tenant_id,
        &publication.candidate.claim_ref,
    )
    .await?;
    ensure_content_ref(
        transaction,
        batch.tenant_id,
        &publication.evaluation.report_ref,
    )
    .await?;
    ensure_content_ref(
        transaction,
        batch.tenant_id,
        &publication.approval.prompt_ref,
    )
    .await?;
    assert_expected_memory_head(transaction, publication).await?;
    let mut verified = publication.candidate.clone();
    verified.status = CandidateStatus::Verified;
    if !proposal_matches(transaction, &verified).await?
        || !evaluation_matches(transaction, &publication.evaluation).await?
    {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    sqlx::query(
        "INSERT INTO approval.management_interactions (
             interaction_id,tenant_id,batch_id,proposal_id,evaluation_id,
             prompt_content_id,prompt_content_hash,requested_by,requested_at_ms,
             approved_by,approved_at_ms,approval_decision
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$8,$10,1)",
    )
    .bind(publication.approval.approval_interaction_id.into_uuid())
    .bind(batch.tenant_id.into_uuid())
    .bind(batch.batch_id.into_uuid())
    .bind(publication.candidate.proposal_id.into_uuid())
    .bind(publication.evaluation.evaluation_id.into_uuid())
    .bind(publication.approval.prompt_ref.content_id.into_uuid())
    .bind(
        publication
            .approval
            .prompt_ref
            .sha256
            .into_bytes()
            .as_slice(),
    )
    .bind(batch.owner_principal_id.into_uuid())
    .bind(requested_at_ms)
    .bind(batch.confirmed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO evolution.approvals (
             approval_interaction_id,proposal_id,evaluation_id,prompt_content_id,
             prompt_content_hash,approved_by,approved_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(publication.approval.approval_interaction_id.into_uuid())
    .bind(publication.candidate.proposal_id.into_uuid())
    .bind(publication.evaluation.evaluation_id.into_uuid())
    .bind(publication.approval.prompt_ref.content_id.into_uuid())
    .bind(
        publication
            .approval
            .prompt_ref
            .sha256
            .into_bytes()
            .as_slice(),
    )
    .bind(batch.owner_principal_id.into_uuid())
    .bind(batch.confirmed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if persist_memory(
        transaction,
        &publication.memory_record,
        &publication.memory_version,
        &publication.search_document,
    )
    .await?
        != MemoryPersistenceDisposition::Applied
    {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    sqlx::query(
        "INSERT INTO evolution.promotions (
             promotion_id,candidate_id,promoted_version_ref,promoted_by,promoted_at_ms,
             proposal_id,published_memory_version_id,evaluation_id,approval_interaction_id
         ) VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(publication.promotion.promotion_id.into_uuid())
    .bind(
        publication
            .promotion
            .published_memory_version_id
            .to_string(),
    )
    .bind(batch.owner_principal_id.into_uuid())
    .bind(batch.confirmed_at.unix_millis())
    .bind(publication.candidate.proposal_id.into_uuid())
    .bind(publication.memory_version.memory_version_id.into_uuid())
    .bind(publication.evaluation.evaluation_id.into_uuid())
    .bind(publication.approval.approval_interaction_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    let updated =
        sqlx::query("UPDATE evolution.proposals SET status=5 WHERE proposal_id=$1 AND status=3")
            .bind(publication.candidate.proposal_id.into_uuid())
            .execute(&mut *transaction)
            .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    mark_draft_published(transaction, publication).await?;
    insert_evolution_publication_outbox(transaction, publication).await?;
    sqlx::query(
        "INSERT INTO memory.agent_memory_draft_publication_items (
             tenant_id,batch_id,ordinal,draft_id,proposal_id,approval_interaction_id,
             promotion_id,memory_version_id
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(batch.tenant_id.into_uuid())
    .bind(batch.batch_id.into_uuid())
    .bind(row.try_get::<i32, _>("ordinal")?)
    .bind(draft_id.into_uuid())
    .bind(publication.candidate.proposal_id.into_uuid())
    .bind(publication.approval.approval_interaction_id.into_uuid())
    .bind(publication.promotion.promotion_id.into_uuid())
    .bind(publication.memory_version.memory_version_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn active_checkout_head(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    owner_principal_id: PrincipalId,
    agent_id: musteragent_identity::AgentId,
    memory_id: MemoryId,
) -> Result<Option<MemoryVersionId>, PostgresMemoryEvolutionError> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT current_version_id FROM memory.memories
         WHERE tenant_id=$1 AND memory_id=$2 AND lifecycle=1
           AND ((scope_kind=1 AND scope_id=$3) OR (scope_kind=4 AND scope_id=$4))
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .bind(owner_principal_id.into_uuid())
    .bind(agent_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .map(parse_id)
    .transpose()
}

fn draft_scope_matches(scope: EvolutionScope, draft: &AgentMemoryDraft) -> bool {
    matches!(
        scope,
        EvolutionScope::User { principal_id } if principal_id == draft.owner_principal_id
    ) || matches!(
        scope,
        EvolutionScope::Agent { agent_id } if agent_id == draft.agent_id
    )
}

async fn load_draft_for_update(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    owner_principal_id: musteragent_identity::PrincipalId,
    draft_id: MemoryDraftId,
) -> Result<Option<AgentMemoryDraft>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(&format!("{} FOR UPDATE", draft_select_sql()))
        .bind(tenant_id.into_uuid())
        .bind(owner_principal_id.into_uuid())
        .bind(draft_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
    row.map(decode_draft).transpose()
}

async fn load_draft_in_transaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    owner_principal_id: musteragent_identity::PrincipalId,
    draft_id: MemoryDraftId,
) -> Result<Option<AgentMemoryDraft>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(draft_select_sql())
        .bind(tenant_id.into_uuid())
        .bind(owner_principal_id.into_uuid())
        .bind(draft_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
    row.map(decode_draft).transpose()
}

fn draft_select_sql() -> &'static str {
    "SELECT draft_id,tenant_id,owner_principal_id,agent_id,memory_id,
            baseline_memory_version_id,content_id,content_media_type,
            content_byte_length::text AS content_byte_length,content_sha256,status,
            proposal_id,conflicting_memory_version_id,supersedes_draft_id,
            idempotency_key,created_at_ms,updated_at_ms
     FROM memory.agent_memory_drafts
     WHERE tenant_id=$1 AND owner_principal_id=$2 AND draft_id=$3"
}

fn decode_draft(
    row: sqlx::postgres::PgRow,
) -> Result<AgentMemoryDraft, PostgresMemoryEvolutionError> {
    let draft = AgentMemoryDraft {
        draft_id: parse_id(row.try_get("draft_id")?)?,
        tenant_id: parse_id(row.try_get("tenant_id")?)?,
        owner_principal_id: parse_id(row.try_get("owner_principal_id")?)?,
        agent_id: parse_id(row.try_get("agent_id")?)?,
        memory_id: parse_id(row.try_get("memory_id")?)?,
        baseline_memory_version_id: parse_id(row.try_get("baseline_memory_version_id")?)?,
        content_ref: ContentRef {
            content_id: parse_id(row.try_get("content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("content_media_type")?)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            byte_length: row
                .try_get::<String, _>("content_byte_length")?
                .parse()
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_sha256")?)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        },
        status: decode_status(row.try_get("status")?)?,
        proposal_id: optional_id(row.try_get("proposal_id")?)?,
        conflicting_memory_version_id: optional_id(row.try_get("conflicting_memory_version_id")?)?,
        supersedes_draft_id: optional_id(row.try_get("supersedes_draft_id")?)?,
        idempotency_key: musteragent_identity::IdempotencyKey::new(
            row.try_get::<String, _>("idempotency_key")?,
        )
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
    };
    draft
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    Ok(draft)
}

fn decode_status(value: i16) -> Result<AgentMemoryDraftStatus, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(AgentMemoryDraftStatus::Draft),
        2 => Ok(AgentMemoryDraftStatus::Submitted),
        3 => Ok(AgentMemoryDraftStatus::Conflict),
        4 => Ok(AgentMemoryDraftStatus::Published),
        5 => Ok(AgentMemoryDraftStatus::Abandoned),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

fn optional_id<T>(value: Option<Uuid>) -> Result<Option<T>, PostgresMemoryEvolutionError>
where
    T: std::str::FromStr,
{
    value.map(parse_id).transpose()
}

fn parse_id<T>(value: Uuid) -> Result<T, PostgresMemoryEvolutionError>
where
    T: std::str::FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}
