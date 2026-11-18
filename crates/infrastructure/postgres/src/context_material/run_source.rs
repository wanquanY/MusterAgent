//! Fenced Run identity and immutable context-load boundary.

use musteragent_context::ConversationVisibilityBoundary;
use musteragent_run::RunContext;

pub(super) struct RunSource {
    pub(super) run_context: RunContext,
    pub(super) snapshot: musteragent_runtime_snapshot::RuntimeSnapshotContent,
    pub(super) visibility: ConversationVisibilityBoundary,
    /// Stream watermark read with this execution fence. A plan can be committed
    /// after its compaction input was sealed at the older ledger cutoff.
    pub(super) observed_ledger_head: u64,
    pub(super) superseding_control_id: Option<musteragent_identity::LedgerRecordId>,
    pub(super) ledger_cutoff: u64,
}
