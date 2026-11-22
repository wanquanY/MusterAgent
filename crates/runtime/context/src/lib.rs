//! Rebuildable, provenance-preserving model context snapshots.

#![forbid(unsafe_code)]
mod model_field_views;

mod approval_observation;
mod attachment;
pub use approval_observation::ApprovalContextCollector;
mod compaction_persistence;
mod compaction_slot;
mod compaction_source;
pub use compaction_source::CompactionSource;
mod content;
mod iteration_identity;
mod material;
mod message_text;
pub use message_text::materialize_input_body_from_batch;
pub mod compaction_history;
mod dialogue_material;
mod model_compaction;
mod model_tool_views;
mod public_delivery_references;
pub use dialogue_material::{
    BoundConversationHistory, ConversationHistoryItem, DialogueHistoryBoundary,
    DialogueHistoryMaterial, DialogueHistoryMaterialError, append_conversation_history,
    conversation_history_segment, dialogue_history_text_refs,
    materialize_dialogue_history_with_transcripts,
};
mod plan_checksum;
pub use plan_checksum::validate_context_plan_request;
mod plan_lifecycle;
mod planner;
mod port;
mod profile;
pub use profile::ContextPlanProfile;
mod model_skill_views;
#[cfg(test)]
mod model_skill_views_tests;
mod provider_context;
mod skill_history_view;
pub mod token_estimate;
mod tool_message;
mod tool_receipt_metadata;
mod tool_result_visibility;
pub use tool_result_visibility::{project_original_json, project_original_message};
mod progress_note;
mod tool_schema_view;
mod tool_turn;
pub use public_delivery_references::{
    project_dialogue_publication_references, project_public_delivery_references,
};
mod visibility;

use std::collections::BTreeSet;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_conversation::ContextBranchOrigin;
use musteragent_identity::{
    ContextBranchId, ContextSnapshotId, ConversationId, ConversationSeq, CorrelationId,
    InvocationId, LedgerRecordId, MessageId, PrincipalId, RunId,
};
use musteragent_runtime_contract::ErrorInfo;
use musteragent_runtime_model::ModelRouteRef;
use thiserror::Error;

pub use attachment::{
    AttachmentMaterializationError, materialize_dialogue_attachment,
    materialize_dialogue_attachment_from_batch, materialize_message_attachment,
    materialize_message_attachment_from_batch, message_attachment_text_ref,
};
pub use compaction_persistence::{
    ContextCompactionDispatchState, ContextCompactionInvocationStore, ContextCompactionRecovery,
    ContextCompactionRecoveryState, ContextCompactionStoreError, PersistedContextCompactionSlot,
};
pub use content::{ContextPlanContentKind, ContextPlanContentWrite, ContextPlanContentWriter};
pub use material::{
    AgentInstructionMaterial, AssistantOrigin, CONTEXT_SUMMARY_SCHEMA, ConsumedStimulusCut,
    ContextEntry, ContextFileOperations, ContextMaterial, ContextPresentationOrder,
    ContextSourceFence, ContextSummaryDocument, ContextSummaryItem, ContextSummaryRole,
    ContextToolPolicy, ContextUsageAnchor, DesktopSourceHost, ISOLATED_CONTEXT_SUMMARY_SCHEMA,
    LEGACY_CONTEXT_SUMMARY_SCHEMA, MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS, MaterializedContent,
    MaterializedContextBaseline, MaterializedDelegationCompletion, MaterializedExecutionPlan,
    MaterializedInstinct, MaterializedMemory, MaterializedModelActionRejection, MaterializedSkill,
    MeasuredContextUsage, NATIVE_CONTEXT_SUMMARY_SCHEMA,
};
pub use message_text::{
    materialize_message_text_from_batch, materialize_tool_assistant_text_from_batch,
    partial_assistant_message_text,
};
pub use model_compaction::{
    CompactionExecutionObservation, ContextCompactionModelFailure, ContextCompactionModelInvoker,
    ContextCompactionModelOutput,
};
pub use plan_lifecycle::{
    ContextPlanControlCut, ContextPlanHead, ContextPlanHeadRef, ContextPlanLineage,
    ContextPlanSelection, select_context_plan_for_sealed_input, select_context_plan_generation,
};
pub use planner::{DurableContextPlanner, present_file_read_parts_for_route};
pub use port::{
    COMPACTION_HISTORY_CODEC_SCHEMA, COMPACTION_PROMPT_SCHEMA, CONTEXT_PLAN_GENERATION,
    ContextCodec, ContextCompactionFailureCommit, ContextCompactionRequest,
    ContextCompactionResolution, ContextCompactionResolutionCommit, ContextCompactionResult,
    ContextCompactionSlotV1, ContextCompactionStartCommit, ContextCompactor, ContextMaterialSource,
    ContextPlanCommit, ContextPlanDisposition, ContextPlanError, ContextPlanInput, ContextPlanMode,
    ContextPlanResolution, ContextPlanStore, ContextPlanner, ModelIteration,
    NoopRunProgressCoordinator, RunProgressCoordinator, RunProgressDirective,
    RuntimeSnapshotDecoder, StoredContextPlanIdentity,
};
pub use provider_context::{HandoffProjection, ProviderContextCompilation};
pub use tool_message::materialize_tool_message_from_batch;
pub use tool_turn::{contiguous_model_turns, materialize_tool_turn_assistant_from_batch};
pub use visibility::{ContextSourceOwner, ContextSourceSelector, ConversationVisibilityBoundary};

/// Lossy model-input projection. It never replaces the canonical Ledger.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextSnapshot {
    /// Explicit immutable plan revision; absence preserves legacy plan bytes.
    pub plan_lineage: Option<ContextPlanLineage>,
    /// Forward Dialogue-only stage; absent for all legacy plans and Work Runs.
    pub delivery_repair: Option<musteragent_execution_context::PublicDeliveryRepairBinding>,
    pub snapshot_id: ContextSnapshotId,
    pub run_id: RunId,
    pub iteration: u32,
    pub parent_snapshot_id: Option<ContextSnapshotId>,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
    pub retained_record_ids: Vec<LedgerRecordId>,
    pub structured_summary_ref: ContentRef,
    pub model_request_ref: ContentRef,
    pub input_tokens_before: u64,
    pub input_tokens_after: u64,
    pub model_route_ref: ModelRouteRef,
    pub checksum: Sha256,
}

impl ContextSnapshot {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if let Some(lineage) = self.plan_lineage {
            lineage
                .validate()
                .map_err(|_| ContextContractError::InvalidPlanLineage)?;
            if lineage
                .predecessor
                .is_some_and(|head| head.snapshot_id == self.snapshot_id)
                || lineage
                    .control
                    .is_some_and(|control| control.sequence > self.source_seq_to)
            {
                return Err(ContextContractError::InvalidPlanLineage);
            }
        }
        if let Some(binding) = &self.delivery_repair {
            binding
                .validate()
                .map_err(|_| ContextContractError::InvalidDeliveryRepair)?;
            if binding.candidate.ledger_cutoff > self.source_seq_to {
                return Err(ContextContractError::InvalidDeliveryRepair);
            }
        }
        if self.iteration == 0 {
            return Err(ContextContractError::InvalidIteration);
        }
        if self.source_seq_from > self.source_seq_to {
            return Err(ContextContractError::InvalidSourceRange);
        }
        if self.input_tokens_after > self.input_tokens_before {
            return Err(ContextContractError::TokenCountIncreased);
        }
        let unique: BTreeSet<_> = self.retained_record_ids.iter().copied().collect();
        if unique.len() != self.retained_record_ids.len() {
            return Err(ContextContractError::DuplicateRetainedRecord);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextCompactionOutcome {
    Succeeded,
    FailedContinuedUncompressed,
    FailedStopped,
    IndeterminateStopped,
    /// This physical invocation is sealed; logical compression waits for a new generation.
    RetryScheduled,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ContextEvent {
    /// Authorized public history bound privately to this execution Context.
    /// It retains its public provenance and is never a participant message.
    DialogueBound {
        binding: Box<musteragent_execution_context::DialogueContextBound>,
    },
    BranchCreated {
        branch_id: ContextBranchId,
        parent_branch_id: Option<ContextBranchId>,
        fork_from_record_id: LedgerRecordId,
        branch_summary_ref: Option<ContentRef>,
        origin: ContextBranchOrigin,
        edit_group_id: Option<CorrelationId>,
        edit_target_message_id: Option<MessageId>,
        created_by: PrincipalId,
    },
    BranchActivated {
        branch_id: ContextBranchId,
        activated_by: PrincipalId,
    },
    Derived {
        branch_id: ContextBranchId,
        source_conversation_id: ConversationId,
        source_branch_id: ContextBranchId,
        source_seq_to: ConversationSeq,
        source_context_snapshot_id: ContextSnapshotId,
        target_context_snapshot_id: ContextSnapshotId,
    },
    CompactionStarted {
        invocation_id: InvocationId,
        loop_iteration: u32,
        source_seq_from: ConversationSeq,
        source_seq_to: ConversationSeq,
        model_route_ref: ModelRouteRef,
        input_tokens_before: u64,
    },
    SnapshotCreated {
        snapshot: Box<ContextSnapshot>,
    },
    CompactionFailed {
        invocation_id: InvocationId,
        error: ErrorInfo,
    },
    CompactionResolved {
        resolution: Box<ContextCompactionResolution>,
    },
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ContextContractError {
    #[error("context plan lineage is inconsistent with its immutable predecessor")]
    InvalidPlanLineage,
    #[error("private delivery repair is inconsistent with its sealed source boundary")]
    InvalidDeliveryRepair,
    #[error("context iteration must be greater than zero")]
    InvalidIteration,
    #[error("context source sequence range is reversed")]
    InvalidSourceRange,
    #[error("context compaction cannot increase input token count")]
    TokenCountIncreased,
    #[error("context retained record list contains duplicates")]
    DuplicateRetainedRecord,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        ContentId, ContextSnapshotId, ConversationSeq, LedgerRecordId, VersionedKey,
    };
    use musteragent_runtime_model::ModelRouteRef;

    use super::{ContextContractError, ContextSnapshot};

    #[test]
    fn snapshot_preserves_a_valid_source_range_and_unique_provenance() {
        let record_id = LedgerRecordId::generate();
        let summary_ref = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        let snapshot = ContextSnapshot {
            plan_lineage: None,
            delivery_repair: None,
            snapshot_id: ContextSnapshotId::generate(),
            run_id: musteragent_identity::RunId::generate(),
            iteration: 1,
            parent_snapshot_id: None,
            source_seq_from: ConversationSeq::new(1).expect("source start"),
            source_seq_to: ConversationSeq::new(2).expect("source end"),
            retained_record_ids: vec![record_id, record_id],
            structured_summary_ref: summary_ref,
            model_request_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/json").expect("media type"),
                byte_length: 2,
                sha256: Sha256::digest(b"{}"),
            },
            input_tokens_before: 100,
            input_tokens_after: 40,
            model_route_ref: ModelRouteRef::from_key(
                VersionedKey::new("default.compaction@1").expect("route key"),
            ),
            checksum: Sha256::digest(b"snapshot"),
        };
        assert_eq!(
            snapshot.validate(),
            Err(ContextContractError::DuplicateRetainedRecord)
        );
    }
}

mod compaction_continuation;
mod compaction_recovery;
pub use compaction_continuation::{
    CompactionCheckpoint, CompactionContinuation, ContextCompactionStep,
};
pub use compaction_recovery::{
    CompactionCandidatePredecessor, CompactionGenerationObservation, CompactionGenerationState,
};

mod public_delivery;
pub use public_delivery::{public_delivery_schema, public_delivery_schema_for};

mod dialogue;
pub use dialogue::*;

pub use planner::validate_public_delivery_repair_request;

mod resolved_tools;
pub use resolved_tools::{ResolvedContextTool, resolve_context_tools};

mod standing_goal;
pub use standing_goal::standing_goal_input_references;
mod coordination;
pub use coordination::coordination_input_text;

mod extension_view;
pub use extension_view::{ContextViewProjection, ModelContextView};
