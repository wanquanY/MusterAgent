//! Read-only Memory Owner facts through an authorized original approval entry.
//! Never expose arbitrary memory lookup through client-selected IDs.
use super::*;
use musteragent_approval::{
    ApprovalDecision, Interaction, InteractionKind, InteractionResponseRef, InteractionStatus,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AgentDialogueId, ContentId, DialogueEntryId, EvolutionProposalId, MemoryId, MemoryVersionId,
    UtcTimestamp,
};
use musteragent_runtime_session::{
    OwnerMemoryCurrentAuthority, OwnerMemoryCurrentStatus as Current,
    OwnerMemoryExecutionAuthority, OwnerMemoryOperation, OwnerMemoryPublicationReceipt,
    OwnerMemoryReceipt,
};
use musteragent_tool::ToolInvocationStatus as Tool;

fn reference(row: &sqlx::postgres::PgRow) -> Result<Option<ContentRef>, PostgresInteractionError> {
    let Some(id) = row.try_get::<Option<uuid::Uuid>, _>("content_id")? else {
        return Ok(None);
    };
    Ok(Some(ContentRef {
        content_id: ContentId::from_uuid(id).map_err(|_| PostgresInteractionError::Corrupt)?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("byte_length")?
            .parse()
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
    }))
}

pub(super) async fn execution(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    interaction: &Interaction,
) -> Result<Option<OwnerMemoryExecutionAuthority>, PostgresInteractionError> {
    if interaction.kind != InteractionKind::Approval
        || interaction.status == InteractionStatus::Pending
    {
        return Ok(None);
    }
    let row = sqlx::query("SELECT t.tool_ref,t.status,c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
        FROM tool.tool_invocations t
        LEFT JOIN tool.tool_results result ON result.invocation_id=t.invocation_id AND result.result_content_id=t.result_content_id
        LEFT JOIN artifact.content_refs c ON c.content_id=result.result_content_id AND c.tenant_id=t.tenant_id
          AND c.content_hash=result.result_hash AND c.state=2
        WHERE t.tenant_id=$1 AND t.run_id=$2 AND t.conversation_id=$3 AND t.invocation_id=$4
          AND t.interaction_id=$5 AND t.tool_ref IN ('user_memory.remember@2','experience.publish@1') AND t.approval_policy=3")
        .bind(actor.tenant_id.into_uuid()).bind(interaction.run_id.into_uuid())
        .bind(interaction.conversation_id.into_uuid()).bind(interaction.requested_by_invocation_id.into_uuid())
        .bind(interaction.interaction_id.into_uuid()).fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(None) };
    let status = match row.try_get::<i16, _>("status")? {
        1 => Tool::Requested,
        2 => Tool::WaitingInteraction,
        3 => Tool::Running,
        4 => Tool::Succeeded,
        5 => Tool::Failed,
        6 => Tool::Indeterminate,
        7 => Tool::Cancelled,
        _ => return Err(PostgresInteractionError::Corrupt),
    };
    if status == Tool::Succeeded
        && interaction.response_ref
            != Some(InteractionResponseRef::Approval(ApprovalDecision::Approve))
    {
        return Err(PostgresInteractionError::Corrupt);
    }
    let operation = match row.try_get::<String, _>("tool_ref")?.as_str() {
        "user_memory.remember@2" => OwnerMemoryOperation::Remember,
        "experience.publish@1" => OwnerMemoryOperation::Publish,
        _ => return Err(PostgresInteractionError::Corrupt),
    };
    let publication_receipt =
        if operation == OwnerMemoryOperation::Publish && status == Tool::Succeeded {
            publication_receipt(tx, actor, interaction).await?
        } else {
            None
        };
    Ok(Some(OwnerMemoryExecutionAuthority {
        status,
        result_ref: reference(&row)?,
        operation,
        publication_receipt,
    }))
}

/// A publication result is corroborated by the original, uniquely bound promotion.
/// No outbox delivery or model message is interpreted as successful persistence.
async fn publication_receipt(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    interaction: &Interaction,
) -> Result<Option<OwnerMemoryPublicationReceipt>, PostgresInteractionError> {
    let row = sqlx::query("SELECT proposal.proposal_id,saved.memory_id,saved.memory_version_id,saved.version::text AS revision
        FROM evolution.promotions promotion
        JOIN evolution.proposals proposal ON proposal.proposal_id=promotion.proposal_id AND proposal.tenant_id=$1
        JOIN evolution.approvals approved ON approved.approval_interaction_id=promotion.approval_interaction_id
          AND approved.proposal_id=proposal.proposal_id AND approved.evaluation_id=promotion.evaluation_id
          AND approved.approved_by=$2
        JOIN evolution.evaluations evaluation ON evaluation.evaluation_id=promotion.evaluation_id
          AND evaluation.proposal_id=proposal.proposal_id AND evaluation.passed=TRUE
        JOIN approval.interactions i ON i.interaction_id=approved.approval_interaction_id AND i.tenant_id=proposal.tenant_id
          AND i.kind=1 AND i.status=2 AND i.run_id=$3 AND i.conversation_id=$4
          AND i.responded_by=approved.approved_by AND i.responded_at_ms=approved.approved_at_ms
        JOIN approval.interaction_responses response ON response.interaction_id=i.interaction_id AND response.approval_decision=1
        JOIN tool.tool_invocations invocation ON invocation.invocation_id=i.requested_by_invocation_id
          AND invocation.tenant_id=i.tenant_id AND invocation.run_id=i.run_id AND invocation.conversation_id=i.conversation_id
          AND invocation.interaction_id=i.interaction_id AND invocation.tool_ref='experience.publish@1'
          AND invocation.approval_policy=3 AND invocation.status=4
        JOIN agent.dialogue_sessions session ON session.tenant_id=i.tenant_id AND session.conversation_id=i.conversation_id
          AND session.owner_id=$2
        JOIN memory.memory_versions saved ON saved.memory_version_id=promotion.published_memory_version_id
          AND saved.content_id=proposal.candidate_content_id AND saved.content_hash=proposal.candidate_content_hash
        JOIN memory.memories memory ON memory.memory_id=saved.memory_id AND memory.tenant_id=proposal.tenant_id
        WHERE invocation.invocation_id=$5 AND i.interaction_id=$6 AND proposal.status=5
          AND ((proposal.scope_kind=1 AND proposal.scope_id=$2 AND memory.scope_kind=1 AND memory.scope_id=$2)
            OR (proposal.scope_kind=2 AND proposal.scope_id=session.agent_id AND memory.scope_kind=4 AND memory.scope_id=session.agent_id))")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(interaction.run_id.into_uuid()).bind(interaction.conversation_id.into_uuid())
        .bind(interaction.requested_by_invocation_id.into_uuid()).bind(interaction.interaction_id.into_uuid())
        .fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(None) };
    let revision = row
        .try_get::<String, _>("revision")?
        .parse::<u64>()
        .map_err(|_| PostgresInteractionError::Corrupt)?;
    if revision == 0 {
        return Err(PostgresInteractionError::Corrupt);
    }
    Ok(Some(OwnerMemoryPublicationReceipt {
        proposal_id: EvolutionProposalId::from_uuid(row.try_get("proposal_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        receipt: OwnerMemoryReceipt {
            memory_id: MemoryId::from_uuid(row.try_get("memory_id")?)
                .map_err(|_| PostgresInteractionError::Corrupt)?,
            version_id: MemoryVersionId::from_uuid(row.try_get("memory_version_id")?)
                .map_err(|_| PostgresInteractionError::Corrupt)?,
            revision,
        },
    }))
}

impl PostgresInteractionApplicationStore {
    pub(super) async fn owner_memory_current(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        result_ref: ContentRef,
        receipt: OwnerMemoryReceipt,
        correlation: CorrelationId,
    ) -> Result<OwnerMemoryCurrentAuthority, RuntimeError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Interaction,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| dependency(correlation))?;
        let bound = dialogue::binding(&mut tx, actor, dialogue, entry)
            .await
            .map_err(|e| map_interaction(e, correlation))?;
        let authority = self
            .load_authority_scoped_on(&mut tx, actor, bound.interaction_id, correlation, true)
            .await?;
        let original = execution(&mut tx, actor, &authority.interaction)
            .await
            .map_err(|e| map_interaction(e, correlation))?
            .ok_or_else(|| corrupt(correlation))?;
        if original.status != Tool::Succeeded || original.result_ref.as_ref() != Some(&result_ref) {
            return Err(corrupt(correlation));
        }
        // The application verified the Tool receipt. Publication additionally has
        // to match the exact promotion/version tuple read through this original entry.
        if original.operation == OwnerMemoryOperation::Publish
            && original
                .publication_receipt
                .as_ref()
                .map(|value| &value.receipt)
                != Some(&receipt)
        {
            return Err(corrupt(correlation));
        }
        let query = match original.operation {
            OwnerMemoryOperation::Remember => sqlx::query("SELECT m.current_version_id,m.lifecycle,m.updated_at_ms,
             v.valid_to_ms,c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
          FROM memory.memories m
          JOIN memory.memory_versions saved ON saved.memory_id=m.memory_id AND saved.memory_version_id=$4 AND saved.version=$5::bigint
          JOIN memory.explicit_publications p ON p.tenant_id=m.tenant_id AND p.memory_id=m.memory_id AND p.memory_version_id=saved.memory_version_id
          JOIN memory.memory_versions v ON v.memory_id=m.memory_id AND v.memory_version_id=m.current_version_id
          LEFT JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=m.tenant_id AND c.content_hash=v.content_hash AND c.state=2
          WHERE m.tenant_id=$1 AND m.scope_kind=1 AND m.scope_id=$2 AND m.memory_id=$3
            AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_lineage e WHERE e.tenant_id=m.tenant_id AND e.memory_id=m.memory_id)
            AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_content_roots e WHERE e.tenant_id=m.tenant_id AND e.content_id=v.content_id)")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(receipt.memory_id.into_uuid())
                .bind(receipt.version_id.into_uuid()).bind(i64::try_from(receipt.revision).map_err(|_| corrupt(correlation))?),
            OwnerMemoryOperation::Publish => sqlx::query("SELECT m.current_version_id,m.lifecycle,m.updated_at_ms,
                v.valid_to_ms,c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
              FROM memory.memories m
              JOIN memory.memory_versions saved ON saved.memory_id=m.memory_id AND saved.memory_version_id=$4 AND saved.version=$5::bigint
              JOIN memory.memory_versions v ON v.memory_id=m.memory_id AND v.memory_version_id=m.current_version_id
              JOIN agent.dialogue_sessions session ON session.tenant_id=m.tenant_id AND session.owner_id=$2 AND session.conversation_id=$6
              LEFT JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=m.tenant_id AND c.content_hash=v.content_hash AND c.state=2
              WHERE m.tenant_id=$1 AND m.memory_id=$3
                AND ((m.scope_kind=1 AND m.scope_id=$2) OR (m.scope_kind=4 AND m.scope_id=session.agent_id))
                AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_lineage e WHERE e.tenant_id=m.tenant_id AND e.memory_id=m.memory_id)
                AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_content_roots e WHERE e.tenant_id=m.tenant_id AND e.content_id=v.content_id)")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(receipt.memory_id.into_uuid())
                .bind(receipt.version_id.into_uuid()).bind(i64::try_from(receipt.revision).map_err(|_| corrupt(correlation))?)
                .bind(authority.interaction.conversation_id.into_uuid()),
        };
        let row = query
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| dependency(correlation))?;
        let Some(row) = row else {
            return Ok(OwnerMemoryCurrentAuthority {
                status: Current::Unavailable,
                version_id: None,
                updated_at: None,
                content_ref: None,
            });
        };
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| dependency(correlation))?
            .unix_millis();
        let lifecycle: i16 = row.try_get("lifecycle").map_err(|_| corrupt(correlation))?;
        let expired = row
            .try_get::<Option<i64>, _>("valid_to_ms")
            .map_err(|_| corrupt(correlation))?
            .is_some_and(|until| until <= now);
        let content_ref = reference(&row).map_err(|e| map_interaction(e, correlation))?;
        let status = match lifecycle {
            1 if expired => Current::Expired,
            1 => Current::Active,
            2 => Current::Superseded,
            3 => Current::Revoked,
            4 => Current::Expired,
            _ => return Err(corrupt(correlation)),
        };
        Ok(OwnerMemoryCurrentAuthority {
            status: if content_ref.is_none() {
                Current::Unavailable
            } else {
                status
            },
            version_id: Some(
                MemoryVersionId::from_uuid(
                    row.try_get("current_version_id")
                        .map_err(|_| corrupt(correlation))?,
                )
                .map_err(|_| corrupt(correlation))?,
            ),
            updated_at: Some(UtcTimestamp::from_unix_millis(
                row.try_get("updated_at_ms")
                    .map_err(|_| corrupt(correlation))?,
            )),
            content_ref,
        })
    }
}
