//! Account view of the executions currently owned by one Account-owned Agent.
//! A dialogue may run several accepted requests at once, so every row is one
//! execution with its own name, progress and control revision. This is a
//! projection of the original Run, plan and directory owners: it admits
//! nothing and controls nothing.
use crate::{DialogueExecutionActivityFact, DialogueStoreError};
use musteragent_identity::{Actor, AgentDialogueId, AgentId, RunId, UserIntentId};
use musteragent_runtime_contract::ApplicationFuture;

/// Ended executions stay listed this long so an outcome is not missed.
pub const AGENT_RUNNING_TASK_RECENT_WINDOW_MS: i64 = 10 * 60 * 1000;
pub const MAX_AGENT_RUNNING_TASK_TITLE_CHARS: usize = 60;
pub const MAX_AGENT_RUNNING_TASKS: u16 = 50;
pub const MAX_AGENT_RECENT_TASKS: u16 = 10;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ReadAgentRunningTasks {
    pub actor: Actor,
    pub agent_id: AgentId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentRunningTaskTitleSource {
    /// Provisional: an excerpt of the user's own request.
    Request,
    /// Written by the executing Agent.
    Agent,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentRunningTaskStepStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentRunningTaskStep {
    pub content: String,
    pub status: AgentRunningTaskStepStatus,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentRunningTask {
    pub run_id: RunId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    /// The accepted request this execution answers; None for a Goal pursuit.
    pub intent_id: Option<UserIntentId>,
    /// Version of this Run only. A stop is fenced against it.
    pub revision: u64,
    /// None when the execution has no request of its own in this dialogue.
    pub title: Option<String>,
    pub title_source: AgentRunningTaskTitleSource,
    pub activity: DialogueExecutionActivityFact,
    /// A durable stop exists but the original owner has not reported a terminal.
    pub stop_requested: bool,
    /// The Agent's current plan in order; empty when it has written none.
    pub plan: Vec<AgentRunningTaskStep>,
    /// Public terminal notice already delivered for this execution.
    pub notice: Option<String>,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentRunningTasksPage {
    pub tasks: Vec<AgentRunningTask>,
    pub observed_at_ms: i64,
    /// Latest execution-control observation of this Agent; a stop of one
    /// task is fenced against it. Zero when none was ever recorded.
    pub control_revision: u64,
}

pub trait AgentRunningTasksQuery: Send + Sync {
    fn read(
        &self,
        request: ReadAgentRunningTasks,
    ) -> ApplicationFuture<'_, Result<AgentRunningTasksPage, DialogueStoreError>>;
}

/// One display line from the user's request: its first non-empty line, bounded
/// by characters so a multi-byte script is never cut inside a code point.
#[must_use]
pub fn agent_running_task_title(request: &str) -> Option<String> {
    let line = request
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .find(|line| !line.is_empty())?;
    let mut chars = line.chars();
    let mut title: String = chars
        .by_ref()
        .take(MAX_AGENT_RUNNING_TASK_TITLE_CHARS)
        .collect();
    if chars.next().is_some() {
        title.push('…');
    }
    Some(title)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn title_uses_the_first_meaningful_line_and_collapses_whitespace() {
        assert_eq!(
            agent_running_task_title("\n\n  Recreate   the\tfilm \nwith the skill").as_deref(),
            Some("Recreate the film")
        );
        assert_eq!(agent_running_task_title(" \n\t "), None);
    }

    #[test]
    fn title_is_bounded_by_characters_not_bytes() {
        let long = "视".repeat(MAX_AGENT_RUNNING_TASK_TITLE_CHARS + 5);
        let title = agent_running_task_title(&long).expect("valid test fixture");
        assert_eq!(
            title.chars().count(),
            MAX_AGENT_RUNNING_TASK_TITLE_CHARS + 1
        );
        assert!(title.ends_with('…'));
        let exact = "a".repeat(MAX_AGENT_RUNNING_TASK_TITLE_CHARS);
        assert_eq!(
            agent_running_task_title(&exact).as_deref(),
            Some(exact.as_str())
        );
    }
}
