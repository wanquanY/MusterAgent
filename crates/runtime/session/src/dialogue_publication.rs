//! Trusted execution delivery admission. This is not a client message API.
use crate::DialogueStoreError;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::{
    AgentDialogueEntry, DialoguePublicationContractError, DialoguePublicationPurpose,
    PublicDeliveryContent,
};
use musteragent_identity::{CommandId, RunSeq};
use musteragent_ledger::AttemptFence;
use musteragent_runtime_contract::ApplicationFuture;
use std::num::NonZeroU64;

/// Created only for an explicit delivery candidate in the shared Loop. It is
/// never inferred from arbitrary AssistantText or the Run terminal status.
/// No caller-supplied tenant, owner, Agent, or Dialogue can select the target:
/// the store derives those from the authoritative internal execution binding.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicEntryAppend {
    /// Stable logical delivery identity allocated by Runtime, across Attempts.
    pub delivery_id: CommandId,
    pub fence: AttemptFence,
    pub expected_last_run_sequence: RunSeq,
    pub expected_permission_revision: NonZeroU64,
    pub purpose: DialoguePublicationPurpose,
    pub content: PublicDeliveryContent,
}

impl PublicEntryAppend {
    pub fn validate(&self) -> Result<(), DialoguePublicationContractError> {
        // Only the original activation transaction may create this card. A
        // model can subsequently acknowledge its existing receipt, not mint it.
        if matches!(self.content, PublicDeliveryContent::PartnerCreated(_)) {
            return Err(DialoguePublicationContractError::InvalidPartner);
        }
        self.content.validate(self.purpose)
    }

    /// Physical lease, Attempt, sequence, and server time are deliberately
    /// excluded. A retry cannot rewrite the original logical delivery.
    pub fn fingerprint(&self) -> Sha256 {
        public_delivery_fingerprint(
            self.delivery_id,
            self.fence.run_id,
            self.expected_permission_revision,
            self.purpose,
            &self.content,
        )
    }
}

/// The immutable logical payload fingerprint, independent of a live Attempt.
/// Terminal recovery reuses it only after proving the original completion.
pub fn public_delivery_fingerprint(
    delivery_id: CommandId,
    run_id: musteragent_identity::RunId,
    permission_revision: NonZeroU64,
    purpose: DialoguePublicationPurpose,
    content: &PublicDeliveryContent,
) -> Sha256 {
    let mut bytes = b"musteragent.publication.v1".to_vec();
    bytes.extend_from_slice(delivery_id.into_uuid().as_bytes());
    bytes.extend_from_slice(run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&permission_revision.get().to_be_bytes());
    field(&mut bytes, purpose.as_str().as_bytes());
    match content {
        PublicDeliveryContent::PartnerCreated(partner) => {
            bytes.push(4);
            bytes.extend(partner.context_bytes(|reference| {
                let mut value = reference.content_id.into_uuid().as_bytes().to_vec();
                field(&mut value, reference.media_type.as_str().as_bytes());
                value.extend(reference.byte_length.to_be_bytes());
                value.extend(reference.sha256.into_bytes());
                value
            }));
        }
        PublicDeliveryContent::Message(content) => {
            bytes.push(1);
            bytes.extend_from_slice(content.content_id.into_uuid().as_bytes());
            field(&mut bytes, content.media_type.as_str().as_bytes());
            bytes.extend_from_slice(&content.byte_length.to_be_bytes());
            bytes.extend_from_slice(&content.sha256.into_bytes());
        }
        PublicDeliveryContent::Interaction(id) => {
            bytes.push(2);
            bytes.extend_from_slice(id.into_uuid().as_bytes());
        }
        PublicDeliveryContent::Artifact {
            artifact_id,
            artifact_version_id,
        } => {
            bytes.push(3);
            bytes.extend_from_slice(artifact_id.into_uuid().as_bytes());
            bytes.extend_from_slice(artifact_version_id.into_uuid().as_bytes());
        }
    }
    Sha256::digest(bytes)
}

fn field(bytes: &mut Vec<u8>, field: &[u8]) {
    bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
    bytes.extend_from_slice(field);
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialoguePublicationReceipt {
    pub entry: AgentDialogueEntry,
    pub replayed: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum DialoguePublicationError {
    #[error(transparent)]
    Contract(#[from] DialoguePublicationContractError),
    #[error(transparent)]
    Store(#[from] DialogueStoreError),
    #[error("publication execution lease is no longer current")]
    StaleFence,
    #[error("cancelled execution cannot publish a new result")]
    Cancelled,
}

/// Validation and append are one Authority transaction. Implementations must
/// derive agent scope from the canonical internal execution binding,
/// validate the original Run Owner's fence, serialize current permission /
/// lifecycle checks, lock and validate real content / Interaction / Artifact
/// references, then commit the entry, retention pins, publication identity,
/// Dialogue cursor and account Outbox together. An empty reference lookup is
/// denial, never permission to generate a stand-in object.
pub trait DialoguePublicationStore: Send + Sync {
    fn append(
        &self,
        command: PublicEntryAppend,
    ) -> ApplicationFuture<'_, Result<DialoguePublicationReceipt, DialoguePublicationError>>;
}
