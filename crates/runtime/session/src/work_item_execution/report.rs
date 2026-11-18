//! An immutable result from an actual Task Run. It is an Agent input, never a
//! new UserIntent and never authority for a report-to-report execution loop.
use super::*;
use musteragent_content_ref::ContentRef;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemExecutionReportOutcome {
    Completed,
    Failed,
    Cancelled,
}
impl WorkItemExecutionReportOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemExecutionReport {
    pub producing_run_id: RunId,
    pub producing_snapshot_id: RuntimeSnapshotId,
    pub producing_agent_id: AgentId,
    pub content_ref: ContentRef,
    pub outcome: WorkItemExecutionReportOutcome,
}
