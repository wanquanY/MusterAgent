//! Memory publication outcome is independent of the original approval decision.
use musteragent_content_ref::ContentRef;
use musteragent_identity::{EvolutionProposalId, MemoryId, MemoryVersionId, UtcTimestamp};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OwnerMemoryExecutionStatus {
    Waiting,
    Saved,
    Unchanged,
    Failed,
    Unknown,
    Cancelled,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OwnerMemoryCurrentStatus {
    Active,
    Revoked,
    Expired,
    Superseded,
    Unavailable,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OwnerMemoryOperation {
    Remember,
    Publish,
}
/// Historical publication identity read from the original promotion and version.
/// This is not sufficient alone: the application also verifies the Tool receipt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerMemoryPublicationReceipt {
    pub proposal_id: EvolutionProposalId,
    pub receipt: OwnerMemoryReceipt,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerMemoryExecutionAuthority {
    pub status: musteragent_tool::ToolInvocationStatus,
    pub result_ref: Option<ContentRef>,
    pub operation: OwnerMemoryOperation,
    pub publication_receipt: Option<OwnerMemoryPublicationReceipt>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerMemoryReceipt {
    pub memory_id: MemoryId,
    pub version_id: MemoryVersionId,
    pub revision: u64,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerMemoryCurrentAuthority {
    pub status: OwnerMemoryCurrentStatus,
    pub version_id: Option<MemoryVersionId>,
    pub updated_at: Option<UtcTimestamp>,
    pub content_ref: Option<ContentRef>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerMemoryCurrent {
    pub status: OwnerMemoryCurrentStatus,
    pub version_id: Option<MemoryVersionId>,
    pub updated_at: Option<UtcTimestamp>,
    pub content: Option<String>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerMemoryExecution {
    pub status: OwnerMemoryExecutionStatus,
    pub receipt: Option<OwnerMemoryReceipt>,
    pub current: Option<OwnerMemoryCurrent>,
}
