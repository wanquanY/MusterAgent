use musteragent_content_ref::ContentRef;
use musteragent_identity::{ContextSnapshotId, InvocationId};
use musteragent_runtime_contract::{ErrorInfo, SafeDisplayText};
use musteragent_runtime_model::ModelText;

use crate::{
    ContextCompactionOutcome, ContextCompactionResolution, ContextEntry, ContextPlanError,
};

pub(super) struct GeneratedSummary {
    pub(super) summary: ModelText,
    pub(super) generator_invocation_id: Option<InvocationId>,
    pub(super) performed_invocation_id: Option<InvocationId>,
    pub(super) previous_summary_ref: Option<ContentRef>,
}

pub(super) enum SummaryGeneration {
    Ready(GeneratedSummary),
    FailedAfterStart {
        invocation_id: InvocationId,
        error: ContextPlanError,
    },
}

pub(super) struct PendingCompactionResolution {
    pub(super) invocation_id: InvocationId,
    pub(super) outcome: ContextCompactionOutcome,
    pub(super) loop_iteration: u32,
    pub(super) input_tokens_before: u64,
    pub(super) summarized_record_count: u64,
    pub(super) retained_record_count: u64,
    pub(super) error: Option<ErrorInfo>,
}

impl PendingCompactionResolution {
    pub(super) fn committed(
        self,
        context_snapshot_id: ContextSnapshotId,
        input_tokens_after: u64,
    ) -> ContextCompactionResolution {
        ContextCompactionResolution {
            invocation_id: self.invocation_id,
            outcome: self.outcome,
            loop_iteration: self.loop_iteration,
            context_snapshot_id: Some(context_snapshot_id),
            dialogue_checkpoint_id: None,
            input_tokens_before: self.input_tokens_before,
            input_tokens_after: Some(input_tokens_after),
            summarized_record_count: self.summarized_record_count,
            retained_record_count: self.retained_record_count,
            error: self.error,
        }
    }
}

pub(super) fn record_count(entries: &[&ContextEntry]) -> Result<u64, ContextPlanError> {
    entries
        .iter()
        .try_fold(0_u64, |total, entry| {
            u64::try_from(entry.source_record_ids.len())
                .ok()
                .and_then(|count| total.checked_add(count))
        })
        .ok_or_else(ContextPlanError::quota)
}

pub(super) fn compaction_error_info(
    error: &ContextPlanError,
) -> Result<ErrorInfo, ContextPlanError> {
    Ok(ErrorInfo {
        code: error.code,
        category: error.code.category(),
        safe_message: SafeDisplayText::new(
            "Context compaction failed; source history was preserved",
        )
        .map_err(|_| ContextPlanError::invariant())?,
        retryable: error.retryable,
        dependency_ref: None,
        diagnostic_ref: None,
    })
}

/// Resolving one physical generation does not necessarily terminalize its Run.
/// Indeterminate calls remain immutable; only a replacement Attempt can mint
/// the next physical identity through the persisted compactor.
pub(super) fn failure_resolution(
    mut error: ContextPlanError,
    policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
) -> (ContextCompactionOutcome, ContextPlanError) {
    let persistent = matches!(
        policy,
        musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
    );
    let outcome = if persistent && (error.retryable || error.is_indeterminate()) {
        error.retryable = true;
        ContextCompactionOutcome::RetryScheduled
    } else if error.is_indeterminate() {
        ContextCompactionOutcome::IndeterminateStopped
    } else {
        ContextCompactionOutcome::FailedStopped
    };
    (outcome, error)
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use musteragent_runtime_contract::ErrorCode;
    use musteragent_runtime_snapshot::RecoveryPolicySnapshot;
    #[test]
    fn persistent_run_waits_after_ambiguous_compression_but_preserves_terminal_denials() {
        let (outcome, error) = failure_resolution(
            ContextPlanError::indeterminate(),
            RecoveryPolicySnapshot::production_default(),
        );
        assert_eq!(outcome, ContextCompactionOutcome::RetryScheduled);
        assert!(error.retryable);
        let (outcome, error) = failure_resolution(
            ContextPlanError::indeterminate(),
            RecoveryPolicySnapshot::legacy_default(),
        );
        assert_eq!(outcome, ContextCompactionOutcome::IndeterminateStopped);
        assert!(!error.retryable);
        let (outcome, error) = failure_resolution(
            ContextPlanError {
                early_provider_cause: None,
                retry_after_ms: None,
                code: ErrorCode::Unauthenticated,
                retryable: false,
            },
            RecoveryPolicySnapshot::production_default(),
        );
        assert_eq!(outcome, ContextCompactionOutcome::FailedStopped);
        assert!(!error.retryable);
    }
}
