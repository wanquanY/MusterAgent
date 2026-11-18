//! Canonical assignment event construction; persistence owns the atomic transfer.
use crate::workspace_assignment::{PrepareWorkspaceAssignment, WorkspaceAssignmentError};
use musteragent_content_ref::Sha256;
use musteragent_conversation::ConversationEvent;
use musteragent_identity::{
    CorrelationId, IdempotencyKey, LedgerOffset, LedgerRecordId, UtcTimestamp,
};
use musteragent_ledger::{AppendReceipt, AppendRequest, DomainEvent, LedgerPayloadEncoder};
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Debug, Clone)]
pub struct WorkspaceAssignmentCommit {
    preparation: PrepareWorkspaceAssignment,
    append: AppendRequest,
}

impl WorkspaceAssignmentCommit {
    pub fn new(
        preparation: PrepareWorkspaceAssignment,
        expected_offset: LedgerOffset,
        now: UtcTimestamp,
        correlation_id: CorrelationId,
        encoder: &dyn LedgerPayloadEncoder,
    ) -> Result<Self, WorkspaceAssignmentError> {
        let record = crate::record::domain_record(
            encoder,
            crate::record::RecordIdentity {
                record_id: LedgerRecordId::from_uuid(preparation.command_id.into_uuid())
                    .map_err(|_| WorkspaceAssignmentError::Corrupt)?,
                activity_id: None,
                run_id: None,
                run_seq: None,
                participant_id: None,
            },
            DomainEvent::Conversation(ConversationEvent::WorkspaceAssigned {
                workspace_id: preparation.target_workspace_id,
                assignment_command_id: preparation.command_id,
            }),
            now,
            correlation_id,
        )
        .map_err(|_| WorkspaceAssignmentError::Corrupt)?;
        // Cursor and transport timestamps are retry observations, not a new
        // migration identity. A committed retry returns its original receipt.
        let identity = format!(
            "workspace-assignment:v1:{}:{}:{}:{}:{}:{}",
            preparation.command_id,
            preparation.actor.tenant_id,
            preparation.actor.principal_id,
            preparation.conversation_id,
            preparation.target_workspace_id,
            preparation.expected_home_epoch.get()
        );
        let append = AppendRequest {
            tenant_id: preparation.actor.tenant_id,
            conversation_id: preparation.conversation_id,
            expected_home_epoch: preparation.expected_home_epoch,
            expected_offset,
            idempotency_key: IdempotencyKey::new(format!(
                "workspace-assignment:{}",
                preparation.command_id
            ))
            .map_err(|_| WorkspaceAssignmentError::Corrupt)?,
            command_fingerprint: Sha256::digest(identity.as_bytes()),
            records: vec![record],
            outbox: vec![],
        };
        Ok(Self {
            preparation,
            append,
        })
    }
    pub const fn preparation(&self) -> PrepareWorkspaceAssignment {
        self.preparation
    }
    pub const fn append(&self) -> &AppendRequest {
        &self.append
    }
}

pub trait WorkspaceAssignmentCommitStore: Send + Sync {
    fn commit_assignment(
        &self,
        command: WorkspaceAssignmentCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, WorkspaceAssignmentError>>;
}
