use musteragent_evolution::CandidateStatus;
use musteragent_identity::InvocationId;
use musteragent_runtime_memory::{MemoryPersistenceDisposition, PromoteExperience};
use sqlx::Row as _;
use uuid::Uuid;

use crate::memory::{
    PostgresMemoryEvolutionError, advisory_lock, approval_matches, assert_expected_memory_head,
    candidate_status_code, ensure_content_ref, evaluation_matches, persist_memory,
    proposal_matches,
};
use crate::memory_publication::mark_draft_published;

/// Publishes a newly approved Experience inside the caller's Attempt-fenced
/// Tool transaction. No approval or production-visible Memory fact can commit
/// without the Tool receipt and Ledger transition committing with it.
pub(crate) async fn publish_evolution_transaction(
    transaction: &mut sqlx::PgConnection,
    invocation_id: InvocationId,
    command: &PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    command.validate()?;
    let binding = command
        .tool_approval
        .as_ref()
        .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
    if binding.invocation_id() != invocation_id {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    crate::dialogue_input_provenance::require_user_invocation(
        transaction,
        musteragent_identity::Actor {
            tenant_id: command.candidate.tenant_id,
            principal_id: command.approval.approved_by,
            principal_kind: musteragent_identity::PrincipalKind::User,
        },
        invocation_id,
    )
    .await
    .map_err(crate::memory::dialogue_source_error)?;
    let approved_envelope = binding.prompt_ref();
    ensure_content_ref(
        transaction,
        command.candidate.tenant_id,
        &command.candidate.claim_ref,
    )
    .await?;
    ensure_content_ref(
        transaction,
        command.candidate.tenant_id,
        &command.evaluation.report_ref,
    )
    .await?;
    ensure_content_ref(
        transaction,
        command.candidate.tenant_id,
        &command.approval.prompt_ref,
    )
    .await?;
    ensure_content_ref(transaction, command.candidate.tenant_id, approved_envelope).await?;
    ensure_content_ref(
        transaction,
        command.candidate.tenant_id,
        binding.request_ref(),
    )
    .await?;
    let row = sqlx::query(
        "SELECT proposal.status,evaluation.passed,evaluation.evaluated_at_ms,
                interaction.kind AS interaction_kind,
                interaction.status AS interaction_status,
                interaction.prompt_content_id,interaction.responded_by,
                interaction.responded_at_ms,response.approval_decision,
                content.media_type,content.byte_length::text AS byte_length,
                content.content_hash
         FROM evolution.proposals AS proposal
         JOIN evolution.evaluations AS evaluation
           ON evaluation.evaluation_id=$2
          AND evaluation.proposal_id=proposal.proposal_id
         JOIN approval.interactions AS interaction
           ON interaction.interaction_id=$3
          AND interaction.tenant_id=proposal.tenant_id
         JOIN tool.tool_invocations AS invocation
           ON invocation.invocation_id=interaction.requested_by_invocation_id
          AND invocation.tenant_id=proposal.tenant_id
          AND invocation.interaction_id=interaction.interaction_id
         JOIN approval.interaction_responses AS response
           ON response.interaction_id=interaction.interaction_id
         JOIN artifact.content_refs AS content
           ON content.content_id=interaction.prompt_content_id
          AND content.tenant_id=proposal.tenant_id
         WHERE proposal.proposal_id=$1
           AND invocation.invocation_id=$4
           AND invocation.tool_ref='experience.publish@1'
           AND invocation.approval_policy=3
           AND invocation.status=3
           AND invocation.request_content_id=$5
         FOR UPDATE OF proposal",
    )
    .bind(command.approval.proposal_id.into_uuid())
    .bind(command.approval.evaluation_id.into_uuid())
    .bind(command.approval.approval_interaction_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(binding.request_ref().content_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
    let verified_candidate = verified_candidate(command);
    let canonical = row.try_get::<i16, _>("status")?
        == candidate_status_code(CandidateStatus::Verified)
        && row.try_get::<Option<bool>, _>("passed")? == Some(true)
        && row.try_get::<i16, _>("interaction_kind")? == 1
        && row.try_get::<i16, _>("interaction_status")? == 2
        && row.try_get::<Option<i16>, _>("approval_decision")? == Some(1)
        && row.try_get::<Uuid, _>("prompt_content_id")? == approved_envelope.content_id.into_uuid()
        && row.try_get::<String, _>("media_type")? == approved_envelope.media_type.as_str()
        && row.try_get::<String, _>("byte_length")?.parse::<u64>().ok()
            == Some(approved_envelope.byte_length)
        && row.try_get::<Vec<u8>, _>("content_hash")? == approved_envelope.sha256.into_bytes()
        && row.try_get::<Option<Uuid>, _>("responded_by")?
            == Some(command.approval.approved_by.into_uuid())
        && row.try_get::<Option<i64>, _>("responded_at_ms")?
            == Some(command.approval.approved_at.unix_millis())
        && command.approval.approved_at.unix_millis()
            >= row.try_get::<i64, _>("evaluated_at_ms")?
        && proposal_matches(transaction, &verified_candidate).await?
        && evaluation_matches(transaction, &command.evaluation).await?;
    if !canonical {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    persist_authorized_evolution(transaction, command, false).await
}

/// Both execution homes use the same CAS, immutable version and search owner.
/// Native callers must first retain the exact account-approved source proof.
pub(crate) async fn persist_authorized_evolution(
    transaction: &mut sqlx::PgConnection,
    command: &PromoteExperience,
    native: bool,
) -> Result<(), PostgresMemoryEvolutionError> {
    let binding = command
        .tool_approval
        .as_ref()
        .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
    let invocation_id = binding.invocation_id();
    let approved_envelope = binding.prompt_ref();
    let prompt = &command.approval.prompt_ref;
    let inserted = sqlx::query(
        "INSERT INTO evolution.approvals (
             approval_interaction_id,proposal_id,evaluation_id,prompt_content_id,
             prompt_content_hash,approved_by,approved_at_ms,
             tool_invocation_id,tool_prompt_content_id,tool_prompt_content_hash,execution_home
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         ON CONFLICT DO NOTHING",
    )
    .bind(command.approval.approval_interaction_id.into_uuid())
    .bind(command.approval.proposal_id.into_uuid())
    .bind(command.approval.evaluation_id.into_uuid())
    .bind(prompt.content_id.into_uuid())
    .bind(prompt.sha256.into_bytes().as_slice())
    .bind(command.approval.approved_by.into_uuid())
    .bind(command.approval.approved_at.unix_millis())
    .bind(invocation_id.into_uuid())
    .bind(approved_envelope.content_id.into_uuid())
    .bind(approved_envelope.sha256.into_bytes().as_slice())
    .bind(if native { "device" } else { "cloud" })
    .execute(&mut *transaction)
    .await?;
    let exact_existing_binding = if inserted.rows_affected() == 0 {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM evolution.approvals
             WHERE approval_interaction_id=$1 AND tool_invocation_id=$2
               AND tool_prompt_content_id=$3 AND tool_prompt_content_hash=$4)",
        )
        .bind(command.approval.approval_interaction_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(approved_envelope.content_id.into_uuid())
        .bind(approved_envelope.sha256.into_bytes().as_slice())
        .fetch_one(&mut *transaction)
        .await?
    } else {
        true
    };
    if !exact_existing_binding
        || (inserted.rows_affected() == 0
            && !approval_matches(transaction, &command.approval).await?)
    {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }

    advisory_lock(
        transaction,
        command.candidate.tenant_id,
        command.memory_record.memory_key.as_str(),
    )
    .await?;
    assert_expected_memory_head(transaction, command).await?;
    if persist_memory(
        transaction,
        &command.memory_record,
        &command.memory_version,
        &command.search_document,
    )
    .await?
        != MemoryPersistenceDisposition::Applied
    {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    sqlx::query(
        "INSERT INTO evolution.promotions (
             promotion_id,candidate_id,promoted_version_ref,promoted_by,
             promoted_at_ms,proposal_id,published_memory_version_id,
             evaluation_id,approval_interaction_id
         ) VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8)",
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
    let updated = sqlx::query(
        "UPDATE evolution.proposals SET status=5
         WHERE proposal_id=$1 AND status=3",
    )
    .bind(command.candidate.proposal_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    mark_draft_published(transaction, command).await?;
    crate::memory_publication::insert_evolution_publication_outbox_for_home(
        transaction,
        command,
        native,
    )
    .await?;
    Ok(())
}

fn verified_candidate(command: &PromoteExperience) -> musteragent_evolution::ExperienceCandidate {
    let mut candidate = command.candidate.clone();
    candidate.status = CandidateStatus::Verified;
    candidate
}

/// Recheck live Dialogue ownership under the same transaction, including on
/// receipt replay. A snapshot cannot preserve an archived Agent's authority.
pub(crate) async fn authorize_tool_publication(
    tx: &mut sqlx::PgConnection,
    invocation_id: InvocationId,
    command: &PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    let binding = command
        .tool_approval
        .as_ref()
        .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
    let row = sqlx::query(
        "SELECT conversation.purpose, dialogue_session.agent_id, dialogue_session.owner_id
         FROM tool.tool_invocations invocation
         JOIN runtime.runs run ON run.run_id=invocation.run_id AND run.tenant_id=invocation.tenant_id
         JOIN conversation.conversations conversation ON conversation.conversation_id=run.conversation_id AND conversation.tenant_id=run.tenant_id
         LEFT JOIN agent.dialogue_sessions dialogue_session ON dialogue_session.conversation_id=run.conversation_id AND dialogue_session.tenant_id=run.tenant_id
         WHERE invocation.invocation_id=$1 AND invocation.tenant_id=$2"
    ).bind(invocation_id.into_uuid()).bind(command.candidate.tenant_id.into_uuid()).fetch_optional(&mut *tx).await?
        .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
    if row.try_get::<String, _>("purpose")? != "standard"
        && (row.try_get::<Option<Uuid>, _>("agent_id")? != Some(binding.agent_id().into_uuid())
            || row.try_get::<Option<Uuid>, _>("owner_id")?
                != Some(command.approval.approved_by.into_uuid())
            || !crate::memory_management_authority::authorize(
                tx,
                musteragent_identity::Actor {
                    tenant_id: command.candidate.tenant_id,
                    principal_id: command.approval.approved_by,
                    principal_kind: musteragent_identity::PrincipalKind::User,
                },
                binding.agent_id(),
            )
            .await?)
    {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    Ok(())
}
