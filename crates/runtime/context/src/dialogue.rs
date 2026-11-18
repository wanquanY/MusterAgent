//! Private source preparation within the original Context/compactor lifecycle.
use crate::{ContextCompactionResolutionCommit, ContextPlanError, ContextSourceFence};
use musteragent_execution_context::{
    DialogueContextBound, DialogueContextCheckpoint, DialogueContextCut, DialogueContextSourcePage,
};
use musteragent_identity::LedgerRecordId;
use musteragent_run::AttemptContext;

/// Advisory sizing state for one catch-up invocation. It never authorizes a
/// request: compaction and primary planning both reload full current material.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnerCapacityBasis {
    pub frozen_cut: DialogueContextCut,
    pub through_seq: u64,
    pub next_page_ordinal: std::num::NonZeroU32,
    pub ledger_cutoff: musteragent_identity::ConversationSeq,
    pub instructions: Option<crate::AgentInstructionMaterial>,
    pub approvals: Option<musteragent_execution_context::DialogueApprovalContext>,
    pub executions: Option<musteragent_execution_context::DialogueExecutionContext>,
    pub checkpoint_id: Option<musteragent_identity::DialogueContextCheckpointId>,
}

/// Newly bound history only. Sources return None when the non-history basis
/// changed, so the planner resizes from a fresh complete material observation.
pub struct DialogueCapacityDelta {
    pub ledger_cutoff: musteragent_identity::ConversationSeq,
    pub entries: Vec<crate::ContextEntry>,
}

/// Bounded read-ahead for sizing only. These observations have no Ledger
/// provenance until the original fenced binding transaction commits them.
pub struct DialogueSourcePreview {
    pub page: DialogueContextSourcePage,
    pub material: crate::DialogueHistoryMaterial,
}

pub const DIALOGUE_SOURCE_BATCH_PAGES: usize = 8;
pub const DIALOGUE_SOURCE_BATCH_TEXT_BYTES: u64 = 8 * 1024 * 1024;

/// One bounded owner read. Before the primary model may run, every item through
/// frozen_cut must be covered by an immutable checkpoint plus a contiguous tail.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextWindow {
    pub frozen_cut: DialogueContextCut,
    pub bound_through_seq: u64,
    pub bound_source_chain_sha256: musteragent_content_ref::Sha256,
    pub next_page_ordinal: std::num::NonZeroU32,
    pub checkpoint: Option<DialogueContextCheckpoint>,
    pub next_page: Option<DialogueContextSourcePage>,
    /// A durable compaction graph already owns the currently bound prefix.
    /// Recovery must finish it before advancing source coverage.
    pub started_coverage: Option<DialogueContextCut>,
}
impl DialogueContextWindow {
    pub fn has_uncompacted_prefix(&self) -> bool {
        self.bound_through_seq
            > self
                .checkpoint
                .as_ref()
                .map_or(0, |c| c.coverage.through.seq)
    }

    pub fn compaction_source(&self) -> Result<crate::CompactionSource, ContextPlanError> {
        self.validate_bound_source()?;
        if (self.complete() && self.started_coverage.is_none()) || !self.has_uncompacted_prefix() {
            return Ok(crate::CompactionSource::RuntimeLedger);
        }
        self.history_source()
    }

    /// Identity of the exact public prefix, including when the last raw page
    /// was already bound before a primary invocation supplied measured usage.
    pub fn history_source(&self) -> Result<crate::CompactionSource, ContextPlanError> {
        self.validate_bound_source()?;
        if !self.has_uncompacted_prefix() {
            return Ok(crate::CompactionSource::RuntimeLedger);
        }
        match &self.started_coverage {
            Some(coverage) => {
                self.history_source_at(coverage.through.seq, coverage.source_chain_sha256)
            }
            None => self.history_source_at(self.bound_through_seq, self.bound_source_chain_sha256),
        }
    }

    pub fn match_started_cut(
        &self,
        key: &str,
        seq: u64,
        chain: musteragent_content_ref::Sha256,
    ) -> Option<DialogueContextCut> {
        let source = self.history_source_at(seq, chain).ok()?;
        if source.key() != key {
            return None;
        }
        match source {
            crate::CompactionSource::DialogueHistory { coverage, .. } => Some(coverage),
            _ => None,
        }
    }

    pub fn history_source_at(
        &self,
        through: u64,
        chain: musteragent_content_ref::Sha256,
    ) -> Result<crate::CompactionSource, ContextPlanError> {
        if through == 0
            || through > self.bound_through_seq
            || self
                .checkpoint
                .as_ref()
                .is_some_and(|c| c.coverage.through.seq >= through)
        {
            return Err(ContextPlanError::invariant());
        }
        let mut coverage = self.frozen_cut.clone();
        coverage.through.seq = through;
        coverage.entry_count = through;
        coverage.source_chain_sha256 = chain;
        Ok(crate::CompactionSource::DialogueHistory {
            coverage,
            parent_checkpoint_id: self.checkpoint.as_ref().map(|c| c.checkpoint_id),
        })
    }

    // Material loading observes the durable prefix without fetching a next page.
    // Only a planner window needs to promise that iteration can advance.
    fn validate_bound_source(&self) -> Result<(), ContextPlanError> {
        self.frozen_cut
            .validate()
            .map_err(|_| ContextPlanError::invariant())?;
        if let Some(coverage) = &self.started_coverage {
            coverage
                .validate()
                .map_err(|_| ContextPlanError::invariant())?;
            if !coverage.same_dialogue(&self.frozen_cut)
                || (coverage.through.seq == self.bound_through_seq
                    && coverage.source_chain_sha256 != self.bound_source_chain_sha256)
            {
                return Err(ContextPlanError::invariant());
            }
            self.history_source_at(coverage.through.seq, coverage.source_chain_sha256)?;
        }
        if self.bound_through_seq > self.frozen_cut.through.seq
            || (self.bound_through_seq == 0
                && self.bound_source_chain_sha256 != self.frozen_cut.empty_chain_hash())
            || (self.complete()
                && self.bound_source_chain_sha256 != self.frozen_cut.source_chain_sha256)
        {
            return Err(ContextPlanError::invariant());
        }
        if let Some(c) = &self.checkpoint {
            c.validate().map_err(|_| ContextPlanError::invariant())?;
            if !c.coverage.same_dialogue(&self.frozen_cut)
                || c.coverage.through.seq > self.bound_through_seq
            {
                return Err(ContextPlanError::invariant());
            }
        }
        if let Some(page) = &self.next_page {
            page.validate().map_err(|_| ContextPlanError::invariant())?;
            if page.frozen_cut != self.frozen_cut || page.from_seq != self.bound_through_seq + 1 {
                return Err(ContextPlanError::invariant());
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ContextPlanError> {
        self.validate_bound_source()?;
        if self.bound_through_seq < self.frozen_cut.through.seq
            && self.next_page.is_none()
            && self
                .checkpoint
                .as_ref()
                .map_or(0, |c| c.coverage.through.seq)
                >= self.bound_through_seq
        {
            return Err(ContextPlanError::invariant());
        }
        Ok(())
    }
    pub fn complete(&self) -> bool {
        self.bound_through_seq == self.frozen_cut.through.seq
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextBindCommit {
    pub attempt: AttemptContext,
    pub binding: DialogueContextBound,
}

/// A successful paid summary is committed to this native Context target, never
/// to a fabricated primary-model request or a second Run lifecycle.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextCheckpointCommit {
    pub attempt: AttemptContext,
    pub checkpoint: DialogueContextCheckpoint,
    pub source_fence: ContextSourceFence,
    pub summarized_record_ids: Vec<LedgerRecordId>,
    pub resolution: ContextCompactionResolutionCommit,
}
impl DialogueContextCheckpointCommit {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        self.checkpoint
            .validate()
            .map_err(|_| ContextPlanError::invariant())?;
        self.resolution.resolution.validate()?;
        if self.attempt != self.resolution.attempt
            || self.summarized_record_ids.is_empty()
            || self.resolution.resolution.dialogue_checkpoint_id
                != Some(self.checkpoint.checkpoint_id)
            || self.resolution.resolution.invocation_id != self.checkpoint.generator_invocation_id
        {
            return Err(ContextPlanError::invariant());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{ContextCompactionOutcome, ContextCompactionResolution};
    use musteragent_identity::{ContextSnapshotId, DialogueContextCheckpointId, InvocationId};
    #[test]
    fn compaction_success_commits_exactly_one_real_target() {
        let mut receipt = ContextCompactionResolution {
            invocation_id: InvocationId::generate(),
            outcome: ContextCompactionOutcome::Succeeded,
            loop_iteration: 1,
            context_snapshot_id: None,
            dialogue_checkpoint_id: Some(DialogueContextCheckpointId::generate()),
            input_tokens_before: 100,
            input_tokens_after: Some(30),
            summarized_record_count: 1,
            retained_record_count: 1,
            error: None,
        };
        assert!(receipt.validate().is_ok());
        receipt.context_snapshot_id = Some(ContextSnapshotId::generate());
        assert!(receipt.validate().is_err());
        receipt.dialogue_checkpoint_id = None;
        assert!(receipt.validate().is_ok());
        receipt.context_snapshot_id = None;
        assert!(receipt.validate().is_err());
        receipt.dialogue_checkpoint_id = Some(DialogueContextCheckpointId::generate());
        receipt.outcome = ContextCompactionOutcome::FailedContinuedUncompressed;
        assert!(receipt.validate().is_err());
        receipt.outcome = ContextCompactionOutcome::Succeeded;
        receipt.input_tokens_after = None;
        assert!(receipt.validate().is_err());
    }
}
