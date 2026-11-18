use musteragent_evolution::CandidateStatus;
use musteragent_identity::{IdempotencyKey, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    GetManagementEvolutionPublication, ManagementEvolutionPublicationDisposition,
    ManagementEvolutionPublicationMutation, ManagementEvolutionPublicationReceipt,
    ManagementEvolutionPublicationStore, ManagementEvolutionPublishedTarget,
    ManagementEvolutionTarget, MemoryPersistenceDisposition, PublishManagementEvolution,
    StudioRevisionArtifactKind,
};
use sqlx::Row as _;
use uuid::Uuid;

use crate::instinct::publish_instinct_transaction;
use crate::memory::{
    PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore, advisory_lock, approval_matches,
    assert_expected_memory_head, ensure_content_ref, evaluation_matches, id, persist_memory,
    proposal_matches,
};
use crate::memory_publication::{insert_evolution_publication_outbox, mark_draft_published};

const STUDIO_DRAFT_TARGET_KIND: i16 = 2;

impl ManagementEvolutionPublicationStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn get_management_publication(
        &self,
        query: GetManagementEvolutionPublication,
    ) -> ApplicationFuture<'_, Result<Option<ManagementEvolutionPublicationReceipt>, Self::Error>>
    {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
                .await?;
            let value = load_receipt(&mut transaction, &query).await?;
            transaction.rollback().await?;
            Ok(value)
        })
    }

    fn publish_management_evolution(
        &self,
        command: PublishManagementEvolution,
    ) -> ApplicationFuture<'_, Result<ManagementEvolutionPublicationMutation, Self::Error>> {
        Box::pin(async move { publish_management(self, command).await })
    }
}

async fn publish_management(
    store: &PostgresMemoryEvolutionStore,
    command: PublishManagementEvolution,
) -> Result<ManagementEvolutionPublicationMutation, PostgresMemoryEvolutionError> {
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
        .await?;
    advisory_lock(
        &mut transaction,
        command.tenant_id,
        &format!(
            "evolution-management:{}:{}",
            command.actor_principal_id,
            command.idempotency_key.as_str()
        ),
    )
    .await?;
    let lookup = GetManagementEvolutionPublication {
        tenant_id: command.tenant_id,
        actor_principal_id: command.actor_principal_id,
        idempotency_key: command.idempotency_key.clone(),
    };
    if let Some(receipt) = load_receipt(&mut transaction, &lookup).await? {
        transaction.rollback().await?;
        if receipt.agent_id == command.agent_id && receipt.proposal_id == command.proposal_id {
            return Ok(ManagementEvolutionPublicationMutation {
                disposition: ManagementEvolutionPublicationDisposition::Duplicate,
                receipt,
            });
        }
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    let (candidate, evaluation, approval) = target_governance(&command.target)?;
    if candidate.tenant_id != command.tenant_id
        || candidate.proposal_id != command.proposal_id
        || approval.approved_by != command.actor_principal_id
        || approval.approved_at != command.approved_at
        || !matches!(
            candidate.target_scope,
            musteragent_evolution::EvolutionScope::Agent { agent_id }
                if agent_id == command.agent_id
        )
    {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    ensure_content_ref(&mut transaction, command.tenant_id, &candidate.claim_ref).await?;
    ensure_content_ref(&mut transaction, command.tenant_id, &evaluation.report_ref).await?;
    ensure_content_ref(&mut transaction, command.tenant_id, &approval.prompt_ref).await?;
    let mut verified = candidate.clone();
    verified.status = CandidateStatus::Verified;
    if !proposal_matches(&mut transaction, &verified).await?
        || !evaluation_matches(&mut transaction, evaluation).await?
    {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    sqlx::query(
        "INSERT INTO approval.evolution_management_interactions (
             interaction_id,tenant_id,target_agent_id,proposal_id,evaluation_id,
             prompt_content_id,prompt_content_hash,requested_by,requested_at_ms,
             approved_by,approved_at_ms,approval_decision,idempotency_key
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$8,$9,1,$10)",
    )
    .bind(approval.approval_interaction_id.into_uuid())
    .bind(command.tenant_id.into_uuid())
    .bind(command.agent_id.into_uuid())
    .bind(command.proposal_id.into_uuid())
    .bind(evaluation.evaluation_id.into_uuid())
    .bind(approval.prompt_ref.content_id.into_uuid())
    .bind(approval.prompt_ref.sha256.into_bytes().as_slice())
    .bind(command.actor_principal_id.into_uuid())
    .bind(command.approved_at.unix_millis())
    .bind(command.idempotency_key.as_str())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO evolution.approvals (
             approval_interaction_id,proposal_id,evaluation_id,prompt_content_id,
             prompt_content_hash,approved_by,approved_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(approval.approval_interaction_id.into_uuid())
    .bind(command.proposal_id.into_uuid())
    .bind(evaluation.evaluation_id.into_uuid())
    .bind(approval.prompt_ref.content_id.into_uuid())
    .bind(approval.prompt_ref.sha256.into_bytes().as_slice())
    .bind(command.actor_principal_id.into_uuid())
    .bind(command.approved_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    let target = match &command.target {
        ManagementEvolutionTarget::Memory(publication) => {
            publish_memory(&mut transaction, publication).await?;
            ManagementEvolutionPublishedTarget::Memory {
                memory_id: publication.memory_record.memory_id,
                memory_version_id: publication.memory_version.memory_version_id,
                promotion_id: publication.promotion.promotion_id,
            }
        }
        ManagementEvolutionTarget::Instinct(publication) => {
            if publish_instinct_transaction(&mut transaction, publication).await?
                != MemoryPersistenceDisposition::Applied
            {
                return Err(PostgresMemoryEvolutionError::EvolutionConflict);
            }
            ManagementEvolutionPublishedTarget::Instinct {
                instinct_id: publication.instinct.instinct_id,
                promotion_id: publication.instinct.promotion_id,
            }
        }
        ManagementEvolutionTarget::StudioRevisionDraft(publication) => {
            publish_studio_draft(&mut transaction, publication).await?;
            ManagementEvolutionPublishedTarget::StudioRevisionDraft {
                studio_revision_draft_id: publication.draft.studio_revision_draft_id,
                promotion_id: publication.draft.promotion_id,
                artifact_kind: publication.draft.artifact_kind,
            }
        }
    };
    let receipt = ManagementEvolutionPublicationReceipt {
        tenant_id: command.tenant_id,
        actor_principal_id: command.actor_principal_id,
        agent_id: command.agent_id,
        proposal_id: command.proposal_id,
        evaluation_id: evaluation.evaluation_id,
        approval_interaction_id: approval.approval_interaction_id,
        idempotency_key: command.idempotency_key,
        approved_at: command.approved_at,
        target,
    };
    transaction.commit().await?;
    Ok(ManagementEvolutionPublicationMutation {
        disposition: ManagementEvolutionPublicationDisposition::Applied,
        receipt,
    })
}

fn target_governance(
    target: &ManagementEvolutionTarget,
) -> Result<
    (
        &musteragent_evolution::ExperienceCandidate,
        &musteragent_evolution::EvolutionEvaluation,
        &musteragent_evolution::EvolutionApproval,
    ),
    PostgresMemoryEvolutionError,
> {
    match target {
        ManagementEvolutionTarget::Memory(value) => {
            value.validate()?;
            Ok((&value.candidate, &value.evaluation, &value.approval))
        }
        ManagementEvolutionTarget::Instinct(value) => {
            value.validate()?;
            Ok((&value.candidate, &value.evaluation, &value.approval))
        }
        ManagementEvolutionTarget::StudioRevisionDraft(value) => {
            value
                .validate()
                .map_err(|_| PostgresMemoryEvolutionError::EvolutionConflict)?;
            Ok((&value.candidate, &value.evaluation, &value.approval))
        }
    }
}

async fn publish_memory(
    transaction: &mut sqlx::PgConnection,
    publication: &musteragent_runtime_memory::PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    advisory_lock(
        transaction,
        publication.candidate.tenant_id,
        publication.memory_record.memory_key.as_str(),
    )
    .await?;
    assert_expected_memory_head(transaction, publication).await?;
    if !approval_matches(transaction, &publication.approval).await?
        || persist_memory(
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
    .bind(publication.memory_version.memory_version_id.to_string())
    .bind(publication.promotion.promoted_by.into_uuid())
    .bind(publication.promotion.promoted_at.unix_millis())
    .bind(publication.candidate.proposal_id.into_uuid())
    .bind(publication.memory_version.memory_version_id.into_uuid())
    .bind(publication.evaluation.evaluation_id.into_uuid())
    .bind(publication.approval.approval_interaction_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    require_proposal_transition(transaction, publication.candidate.proposal_id).await?;
    mark_draft_published(transaction, publication).await?;
    insert_evolution_publication_outbox(transaction, publication).await?;
    Ok(())
}

async fn publish_studio_draft(
    transaction: &mut sqlx::PgConnection,
    publication: &musteragent_runtime_memory::PublishStudioRevisionDraft,
) -> Result<(), PostgresMemoryEvolutionError> {
    publication
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::EvolutionConflict)?;
    if !approval_matches(transaction, &publication.approval).await? {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    let draft = &publication.draft;
    sqlx::query(
        "INSERT INTO evolution.artifact_promotions (
             promotion_id,tenant_id,proposal_id,evaluation_id,approval_interaction_id,
             target_kind,target_id,promoted_by,promoted_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(draft.promotion_id.into_uuid())
    .bind(draft.tenant_id.into_uuid())
    .bind(draft.proposal_id.into_uuid())
    .bind(draft.evaluation_id.into_uuid())
    .bind(draft.approval_interaction_id.into_uuid())
    .bind(STUDIO_DRAFT_TARGET_KIND)
    .bind(draft.studio_revision_draft_id.into_uuid())
    .bind(draft.created_by.into_uuid())
    .bind(draft.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO evolution.studio_revision_drafts (
             studio_revision_draft_id,tenant_id,target_agent_id,proposal_id,evaluation_id,
             approval_interaction_id,promotion_id,artifact_kind,payload_content_id,
             payload_content_hash,created_by,created_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
    )
    .bind(draft.studio_revision_draft_id.into_uuid())
    .bind(draft.tenant_id.into_uuid())
    .bind(draft.target_agent_id.into_uuid())
    .bind(draft.proposal_id.into_uuid())
    .bind(draft.evaluation_id.into_uuid())
    .bind(draft.approval_interaction_id.into_uuid())
    .bind(draft.promotion_id.into_uuid())
    .bind(studio_kind_code(draft.artifact_kind))
    .bind(draft.payload_ref.content_id.into_uuid())
    .bind(draft.payload_ref.sha256.into_bytes().as_slice())
    .bind(draft.created_by.into_uuid())
    .bind(draft.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_proposal_transition(transaction, draft.proposal_id).await?;
    let source_run_id = publication
        .candidate
        .source_run_ids
        .iter()
        .next()
        .ok_or(PostgresMemoryEvolutionError::EvolutionConflict)?;
    let conversation_id: Uuid = sqlx::query_scalar(
        "SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(draft.tenant_id.into_uuid())
    .bind(source_run_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema":"musteragent.legacy.evolution.studio-revision-draft-created.v1",
        "proposal_id":draft.proposal_id.to_string(),
        "studio_revision_draft_id":draft.studio_revision_draft_id.to_string(),
        "target_agent_id":draft.target_agent_id.to_string(),
        "artifact_kind":studio_kind_code(draft.artifact_kind),
    }))?;
    let message_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("studio-revision-draft:{}", draft.promotion_id).as_bytes(),
    );
    sqlx::query(
        "INSERT INTO event_store.outbox (
             message_id,tenant_id,conversation_id,topic,payload,payload_hash,
             committed_at_ms,available_at_ms
         ) VALUES ($1,$2,$3,'evolution.studio-revision-draft-created.v1',$4,$5,$6,$6)",
    )
    .bind(message_id)
    .bind(draft.tenant_id.into_uuid())
    .bind(conversation_id)
    .bind(&payload)
    .bind(
        musteragent_content_ref::Sha256::digest(&payload)
            .into_bytes()
            .as_slice(),
    )
    .bind(draft.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn require_proposal_transition(
    transaction: &mut sqlx::PgConnection,
    proposal_id: musteragent_identity::EvolutionProposalId,
) -> Result<(), PostgresMemoryEvolutionError> {
    let updated =
        sqlx::query("UPDATE evolution.proposals SET status=5 WHERE proposal_id=$1 AND status=3")
            .bind(proposal_id.into_uuid())
            .execute(&mut *transaction)
            .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresMemoryEvolutionError::EvolutionConflict)
    }
}

async fn load_receipt(
    transaction: &mut sqlx::PgConnection,
    query: &GetManagementEvolutionPublication,
) -> Result<Option<ManagementEvolutionPublicationReceipt>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT interaction.tenant_id,interaction.target_agent_id,
                interaction.proposal_id,interaction.evaluation_id,interaction.interaction_id,
                interaction.approved_by,interaction.approved_at_ms,interaction.idempotency_key,
                memory.memory_id,memory_version.memory_version_id,
                memory_promotion.promotion_id AS memory_promotion_id,
                instinct.instinct_id,instinct.promotion_id AS instinct_promotion_id,
                studio.studio_revision_draft_id,
                studio.promotion_id AS studio_promotion_id,studio.artifact_kind
         FROM approval.evolution_management_interactions AS interaction
         JOIN evolution.approvals AS approval
           ON approval.approval_interaction_id=interaction.interaction_id
          AND approval.proposal_id=interaction.proposal_id
          AND approval.evaluation_id=interaction.evaluation_id
         LEFT JOIN evolution.promotions AS memory_promotion
           ON memory_promotion.approval_interaction_id=interaction.interaction_id
         LEFT JOIN memory.memory_versions AS memory_version
           ON memory_version.memory_version_id=memory_promotion.published_memory_version_id
         LEFT JOIN memory.memories AS memory ON memory.memory_id=memory_version.memory_id
         LEFT JOIN evolution.instincts AS instinct
           ON instinct.approval_interaction_id=interaction.interaction_id
         LEFT JOIN evolution.studio_revision_drafts AS studio
           ON studio.approval_interaction_id=interaction.interaction_id
         WHERE interaction.tenant_id=$1 AND interaction.approved_by=$2
           AND interaction.idempotency_key=$3",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.actor_principal_id.into_uuid())
    .bind(query.idempotency_key.as_str())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let target_count = usize::from(
        row.try_get::<Option<Uuid>, _>("memory_version_id")?
            .is_some(),
    ) + usize::from(row.try_get::<Option<Uuid>, _>("instinct_id")?.is_some())
        + usize::from(
            row.try_get::<Option<Uuid>, _>("studio_revision_draft_id")?
                .is_some(),
        );
    if target_count != 1 {
        return Err(PostgresMemoryEvolutionError::CorruptDatabase);
    }
    let target = if let Some(memory_version_id) = row.try_get("memory_version_id")? {
        ManagementEvolutionPublishedTarget::Memory {
            memory_id: id(row.try_get::<Uuid, _>("memory_id")?, "management.memory_id")?,
            memory_version_id: id(memory_version_id, "management.memory_version_id")?,
            promotion_id: id(
                row.try_get::<Uuid, _>("memory_promotion_id")?,
                "management.promotion_id",
            )?,
        }
    } else if let Some(instinct_id) = row.try_get("instinct_id")? {
        ManagementEvolutionPublishedTarget::Instinct {
            instinct_id: id(instinct_id, "management.instinct_id")?,
            promotion_id: id(
                row.try_get::<Uuid, _>("instinct_promotion_id")?,
                "management.promotion_id",
            )?,
        }
    } else {
        ManagementEvolutionPublishedTarget::StudioRevisionDraft {
            studio_revision_draft_id: id(
                row.try_get::<Uuid, _>("studio_revision_draft_id")?,
                "management.studio_revision_draft_id",
            )?,
            promotion_id: id(
                row.try_get::<Uuid, _>("studio_promotion_id")?,
                "management.promotion_id",
            )?,
            artifact_kind: decode_studio_kind(row.try_get("artifact_kind")?)?,
        }
    };
    Ok(Some(ManagementEvolutionPublicationReceipt {
        tenant_id: id(row.try_get("tenant_id")?, "management.tenant_id")?,
        actor_principal_id: id(row.try_get("approved_by")?, "management.approved_by")?,
        agent_id: id(row.try_get("target_agent_id")?, "management.agent_id")?,
        proposal_id: id(row.try_get("proposal_id")?, "management.proposal_id")?,
        evaluation_id: id(row.try_get("evaluation_id")?, "management.evaluation_id")?,
        approval_interaction_id: id(row.try_get("interaction_id")?, "management.interaction_id")?,
        idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        approved_at: UtcTimestamp::from_unix_millis(row.try_get("approved_at_ms")?),
        target,
    }))
}

const fn studio_kind_code(value: StudioRevisionArtifactKind) -> i16 {
    match value {
        StudioRevisionArtifactKind::Skill => 1,
        StudioRevisionArtifactKind::PersonaPatch => 2,
    }
}

fn decode_studio_kind(
    value: i16,
) -> Result<StudioRevisionArtifactKind, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(StudioRevisionArtifactKind::Skill),
        2 => Ok(StudioRevisionArtifactKind::PersonaPatch),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}
