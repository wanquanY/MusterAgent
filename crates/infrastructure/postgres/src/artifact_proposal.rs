use musteragent_artifact::{ArtifactProposal, ArtifactProposalStatus};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactProposalId, ContentId, InvocationId, ParticipantId, TenantId, UtcTimestamp,
    VersionNumber,
};
use musteragent_runtime_artifact::{
    ApplyArtifactProposal, ArtifactProposalDisposition, ArtifactProposalReceipt,
    ArtifactProposalStore, ArtifactRuntimeError, CloseArtifactProposal, ProposeArtifactEdit,
};
use sqlx::Row as _;
use thiserror::Error;

use crate::artifact::{PostgresArtifactError, authorize_content, insert_version, update_artifact};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore};

const CONTENT_READY: i16 = 2;

#[derive(Clone)]
pub struct PostgresArtifactProposalStore {
    ledger: PostgresLedgerStore,
}

impl PostgresArtifactProposalStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            ledger: PostgresLedgerStore::new(database),
        }
    }

    async fn propose_inner(
        &self,
        command: ProposeArtifactEdit,
    ) -> Result<ArtifactProposalReceipt, PostgresArtifactProposalError> {
        command.validate()?;
        let proposal_id = command.proposal.proposal_id;
        let mut prepared = match self.ledger.begin_append(&command.append).await? {
            BeginAppend::Duplicate(ledger) => {
                let proposal = self
                    .load_inner(command.proposal.tenant_id, proposal_id)
                    .await?;
                return Ok(receipt(
                    ArtifactProposalDisposition::Duplicate,
                    &proposal,
                    ledger,
                ));
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let transaction = prepared.transaction_mut();
        authorize_proposal_target(
            transaction,
            &command.proposal,
            command.append.conversation_id,
        )
        .await?;
        authorize_proposal_content(transaction, &command.proposal).await?;
        authorize_source_invocation(transaction, &command.proposal).await?;
        sqlx::query(
            "INSERT INTO artifact.artifact_proposals (
                 proposal_id, tenant_id, artifact_id, base_version,
                 proposal_content_id, proposal_hash, source_invocation_id,
                 proposed_by, proposed_at_ms, status, resolved_by, resolved_at_ms
             ) VALUES (
                 $1, $2, $3, $4::bigint, $5, $6, $7, $8, $9, $10, NULL, NULL
             )",
        )
        .bind(proposal_id.into_uuid())
        .bind(command.proposal.tenant_id.into_uuid())
        .bind(command.proposal.artifact_id.into_uuid())
        .bind(command.proposal.base_version.get().to_string())
        .bind(command.proposal.proposal_ref.content_id.into_uuid())
        .bind(command.proposal.proposal_hash.into_bytes().as_slice())
        .bind(
            command
                .proposal
                .source_invocation_id
                .map(InvocationId::into_uuid),
        )
        .bind(command.proposal.proposed_by.into_uuid())
        .bind(command.proposal.proposed_at.unix_millis())
        .bind(status_code(ArtifactProposalStatus::Pending))
        .execute(&mut *transaction)
        .await
        .map_err(map_proposal_insert)?;
        let ledger = prepared.commit(&command.append).await?;
        Ok(receipt(
            ArtifactProposalDisposition::Committed,
            &command.proposal,
            ledger,
        ))
    }

    async fn apply_inner(
        &self,
        command: ApplyArtifactProposal,
    ) -> Result<ArtifactProposalReceipt, PostgresArtifactProposalError> {
        command.validate()?;
        let mut prepared = match self.ledger.begin_append(&command.append).await? {
            BeginAppend::Duplicate(ledger) => {
                let proposal = self
                    .load_inner(command.tenant_id, command.proposal_id)
                    .await?;
                if proposal.status != ArtifactProposalStatus::Applied
                    || proposal.resolved_by != Some(command.applied_by)
                    || proposal.resolved_at != Some(command.applied_at)
                {
                    return Err(PostgresArtifactProposalError::IdempotencyConflict);
                }
                return Ok(receipt(
                    ArtifactProposalDisposition::Duplicate,
                    &proposal,
                    ledger,
                ));
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let transaction = prepared.transaction_mut();
        let proposal = lock_proposal(transaction, command.tenant_id, command.proposal_id).await?;
        if proposal.status != ArtifactProposalStatus::Pending
            || proposal.artifact_id != command.artifact_id
            || proposal.base_version != command.expected_base_version
            || proposal.proposal_ref != command.version.content_ref
            || proposal.proposal_hash != command.version.content_hash
        {
            return Err(PostgresArtifactProposalError::StateConflict);
        }
        authorize_content(transaction, command.tenant_id, &command.version).await?;
        update_artifact(
            transaction,
            command.tenant_id,
            command.append.conversation_id,
            command.artifact_id,
            command.expected_base_version,
            &command.version,
            command.title.as_deref(),
        )
        .await?;
        insert_version(transaction, &command.version).await?;
        resolve_proposal(
            transaction,
            command.tenant_id,
            command.proposal_id,
            ArtifactProposalStatus::Applied,
            command.applied_by,
            command.applied_at,
        )
        .await?;
        let ledger = prepared.commit(&command.append).await?;
        let applied = ArtifactProposal {
            status: ArtifactProposalStatus::Applied,
            resolved_by: Some(command.applied_by),
            resolved_at: Some(command.applied_at),
            ..proposal
        };
        Ok(receipt(
            ArtifactProposalDisposition::Committed,
            &applied,
            ledger,
        ))
    }

    async fn close_inner(
        &self,
        command: CloseArtifactProposal,
    ) -> Result<ArtifactProposalReceipt, PostgresArtifactProposalError> {
        command.validate()?;
        let mut prepared = match self.ledger.begin_append(&command.append).await? {
            BeginAppend::Duplicate(ledger) => {
                let proposal = self
                    .load_inner(command.tenant_id, command.proposal_id)
                    .await?;
                if proposal.status != command.resolution
                    || proposal.resolved_by != Some(command.resolved_by)
                    || proposal.resolved_at != Some(command.resolved_at)
                {
                    return Err(PostgresArtifactProposalError::IdempotencyConflict);
                }
                return Ok(receipt(
                    ArtifactProposalDisposition::Duplicate,
                    &proposal,
                    ledger,
                ));
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let transaction = prepared.transaction_mut();
        let proposal = lock_proposal(transaction, command.tenant_id, command.proposal_id).await?;
        if proposal.status != ArtifactProposalStatus::Pending
            || proposal.artifact_id != command.artifact_id
        {
            return Err(PostgresArtifactProposalError::StateConflict);
        }
        authorize_conversation(
            transaction,
            command.tenant_id,
            command.artifact_id,
            command.append.conversation_id,
        )
        .await?;
        resolve_proposal(
            transaction,
            command.tenant_id,
            command.proposal_id,
            command.resolution,
            command.resolved_by,
            command.resolved_at,
        )
        .await?;
        let ledger = prepared.commit(&command.append).await?;
        let closed = ArtifactProposal {
            status: command.resolution,
            resolved_by: Some(command.resolved_by),
            resolved_at: Some(command.resolved_at),
            ..proposal
        };
        Ok(receipt(
            ArtifactProposalDisposition::Committed,
            &closed,
            ledger,
        ))
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        proposal_id: ArtifactProposalId,
    ) -> Result<ArtifactProposal, PostgresArtifactProposalError> {
        let row = sqlx::query(
            "SELECT proposal.proposal_id, proposal.tenant_id,
                    proposal.artifact_id,
                    proposal.base_version::text AS base_version,
                    proposal.proposal_content_id, proposal.proposal_hash,
                    proposal.source_invocation_id, proposal.proposed_by,
                    proposal.proposed_at_ms, proposal.status,
                    proposal.resolved_by, proposal.resolved_at_ms,
                    content.media_type,
                    content.byte_length::text AS byte_length,
                    content.content_hash
             FROM artifact.artifact_proposals AS proposal
             JOIN artifact.content_refs AS content
               ON content.content_id = proposal.proposal_content_id
              AND content.tenant_id = proposal.tenant_id
              AND content.state = $3
             WHERE proposal.tenant_id = $1 AND proposal.proposal_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(proposal_id.into_uuid())
        .bind(CONTENT_READY)
        .fetch_optional(self.ledger.pool())
        .await?
        .ok_or(PostgresArtifactProposalError::NotFound)?;
        decode_proposal(&row)
    }
}

impl ArtifactProposalStore for PostgresArtifactProposalStore {
    type Error = PostgresArtifactProposalError;

    fn propose(
        &self,
        command: ProposeArtifactEdit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ArtifactProposalReceipt, Self::Error>,
    > {
        Box::pin(async move { self.propose_inner(command).await })
    }

    fn apply(
        &self,
        command: ApplyArtifactProposal,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ArtifactProposalReceipt, Self::Error>,
    > {
        Box::pin(async move { self.apply_inner(command).await })
    }

    fn close(
        &self,
        command: CloseArtifactProposal,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ArtifactProposalReceipt, Self::Error>,
    > {
        Box::pin(async move { self.close_inner(command).await })
    }

    fn load_proposal(
        &self,
        tenant_id: TenantId,
        proposal_id: ArtifactProposalId,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ArtifactProposal, Self::Error>>
    {
        Box::pin(async move { self.load_inner(tenant_id, proposal_id).await })
    }
}

async fn authorize_proposal_target(
    transaction: &mut sqlx::PgConnection,
    proposal: &ArtifactProposal,
    conversation_id: musteragent_identity::ConversationId,
) -> Result<(), PostgresArtifactProposalError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.artifacts
             WHERE tenant_id = $1 AND artifact_id = $2
               AND conversation_id = $3
               AND current_version = $4::bigint
               AND lifecycle IN (1, 2)
         )",
    )
    .bind(proposal.tenant_id.into_uuid())
    .bind(proposal.artifact_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(proposal.base_version.get().to_string())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresArtifactProposalError::StateConflict)
    }
}

async fn authorize_conversation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    artifact_id: ArtifactId,
    conversation_id: musteragent_identity::ConversationId,
) -> Result<(), PostgresArtifactProposalError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.artifacts
             WHERE tenant_id = $1 AND artifact_id = $2 AND conversation_id = $3
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresArtifactProposalError::StateConflict)
    }
}

async fn authorize_proposal_content(
    transaction: &mut sqlx::PgConnection,
    proposal: &ArtifactProposal,
) -> Result<(), PostgresArtifactProposalError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND state = $3
               AND content_hash = $4 AND byte_length = $5::bigint
               AND media_type = $6
         )",
    )
    .bind(proposal.tenant_id.into_uuid())
    .bind(proposal.proposal_ref.content_id.into_uuid())
    .bind(CONTENT_READY)
    .bind(proposal.proposal_hash.into_bytes().as_slice())
    .bind(proposal.proposal_ref.byte_length.to_string())
    .bind(proposal.proposal_ref.media_type.as_str())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresArtifactProposalError::ContentUnavailable)
    }
}

async fn authorize_source_invocation(
    transaction: &mut sqlx::PgConnection,
    proposal: &ArtifactProposal,
) -> Result<(), PostgresArtifactProposalError> {
    let Some(invocation_id) = proposal.source_invocation_id else {
        return Ok(());
    };
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM tool.tool_invocations
             WHERE tenant_id = $1 AND invocation_id = $2
         )",
    )
    .bind(proposal.tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresArtifactProposalError::SourceInvocationUnavailable)
    }
}

async fn lock_proposal(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    proposal_id: ArtifactProposalId,
) -> Result<ArtifactProposal, PostgresArtifactProposalError> {
    let row = sqlx::query(
        "SELECT proposal.proposal_id, proposal.tenant_id,
                proposal.artifact_id,
                proposal.base_version::text AS base_version,
                proposal.proposal_content_id, proposal.proposal_hash,
                proposal.source_invocation_id, proposal.proposed_by,
                proposal.proposed_at_ms, proposal.status,
                proposal.resolved_by, proposal.resolved_at_ms,
                content.media_type,
                content.byte_length::text AS byte_length,
                content.content_hash
         FROM artifact.artifact_proposals AS proposal
         JOIN artifact.content_refs AS content
           ON content.content_id = proposal.proposal_content_id
          AND content.tenant_id = proposal.tenant_id
          AND content.state = $3
         WHERE proposal.tenant_id = $1 AND proposal.proposal_id = $2
         FOR UPDATE OF proposal",
    )
    .bind(tenant_id.into_uuid())
    .bind(proposal_id.into_uuid())
    .bind(CONTENT_READY)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresArtifactProposalError::NotFound)?;
    decode_proposal(&row)
}

async fn resolve_proposal(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    proposal_id: ArtifactProposalId,
    status: ArtifactProposalStatus,
    resolved_by: ParticipantId,
    resolved_at: UtcTimestamp,
) -> Result<(), PostgresArtifactProposalError> {
    let updated = sqlx::query(
        "UPDATE artifact.artifact_proposals
         SET status = $3, resolved_by = $4, resolved_at_ms = $5
         WHERE tenant_id = $1 AND proposal_id = $2 AND status = $6",
    )
    .bind(tenant_id.into_uuid())
    .bind(proposal_id.into_uuid())
    .bind(status_code(status))
    .bind(resolved_by.into_uuid())
    .bind(resolved_at.unix_millis())
    .bind(status_code(ArtifactProposalStatus::Pending))
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresArtifactProposalError::StateConflict)
    }
}

fn decode_proposal(
    row: &sqlx::postgres::PgRow,
) -> Result<ArtifactProposal, PostgresArtifactProposalError> {
    let proposal_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("proposal_hash")?)
        .map_err(|_| PostgresArtifactProposalError::Corrupt)?;
    let content_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
        .map_err(|_| PostgresArtifactProposalError::Corrupt)?;
    if proposal_hash != content_hash {
        return Err(PostgresArtifactProposalError::Corrupt);
    }
    let proposal = ArtifactProposal {
        proposal_id: ArtifactProposalId::from_uuid(row.try_get("proposal_id")?)
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        artifact_id: ArtifactId::from_uuid(row.try_get("artifact_id")?)
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        base_version: VersionNumber::new(parse_u64(row.try_get("base_version")?)?)
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        proposal_ref: ContentRef {
            content_id: ContentId::from_uuid(row.try_get("proposal_content_id")?)
                .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
            byte_length: parse_u64(row.try_get("byte_length")?)?,
            sha256: content_hash,
        },
        proposal_hash,
        source_invocation_id: row
            .try_get::<Option<uuid::Uuid>, _>("source_invocation_id")?
            .map(InvocationId::from_uuid)
            .transpose()
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        proposed_by: ParticipantId::from_uuid(row.try_get("proposed_by")?)
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        proposed_at: UtcTimestamp::from_unix_millis(row.try_get("proposed_at_ms")?),
        status: decode_status(row.try_get("status")?)?,
        resolved_by: row
            .try_get::<Option<uuid::Uuid>, _>("resolved_by")?
            .map(ParticipantId::from_uuid)
            .transpose()
            .map_err(|_| PostgresArtifactProposalError::Corrupt)?,
        resolved_at: row
            .try_get::<Option<i64>, _>("resolved_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
    };
    proposal
        .validate()
        .map_err(|_| PostgresArtifactProposalError::Corrupt)?;
    Ok(proposal)
}

fn receipt(
    disposition: ArtifactProposalDisposition,
    proposal: &ArtifactProposal,
    ledger: musteragent_ledger::AppendReceipt,
) -> ArtifactProposalReceipt {
    ArtifactProposalReceipt {
        disposition,
        proposal_id: proposal.proposal_id,
        status: proposal.status,
        ledger,
    }
}

const fn status_code(status: ArtifactProposalStatus) -> i16 {
    match status {
        ArtifactProposalStatus::Pending => 1,
        ArtifactProposalStatus::Applied => 2,
        ArtifactProposalStatus::Rejected => 3,
        ArtifactProposalStatus::Expired => 4,
    }
}

fn decode_status(status: i16) -> Result<ArtifactProposalStatus, PostgresArtifactProposalError> {
    match status {
        1 => Ok(ArtifactProposalStatus::Pending),
        2 => Ok(ArtifactProposalStatus::Applied),
        3 => Ok(ArtifactProposalStatus::Rejected),
        4 => Ok(ArtifactProposalStatus::Expired),
        _ => Err(PostgresArtifactProposalError::Corrupt),
    }
}

fn parse_u64(value: String) -> Result<u64, PostgresArtifactProposalError> {
    value
        .parse()
        .map_err(|_| PostgresArtifactProposalError::Corrupt)
}

fn map_proposal_insert(error: sqlx::Error) -> PostgresArtifactProposalError {
    if error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::constraint)
        == Some("artifact_proposals_pkey")
    {
        PostgresArtifactProposalError::IdempotencyConflict
    } else {
        PostgresArtifactProposalError::Sql(error)
    }
}

#[derive(Debug, Error)]
pub enum PostgresArtifactProposalError {
    #[error(transparent)]
    Contract(#[from] ArtifactRuntimeError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Artifact(#[from] PostgresArtifactError),
    #[error("Artifact Proposal or target Artifact was not found")]
    NotFound,
    #[error("Artifact Proposal state or base version conflicts")]
    StateConflict,
    #[error("Artifact Proposal candidate content is unavailable for the tenant")]
    ContentUnavailable,
    #[error("Artifact Proposal source Tool Invocation is unavailable for the tenant")]
    SourceInvocationUnavailable,
    #[error("Artifact Proposal idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("persisted Artifact Proposal data is corrupt")]
    Corrupt,
    #[error("PostgreSQL Artifact Proposal operation failed")]
    Sql(#[from] sqlx::Error),
}
