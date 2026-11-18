//! A current, authorized query of original executions, separate from frozen
//! prompt history and the long-lived WorkItem directory. Text is untrusted
//! source material; neither a cursor nor an excerpt confers authority.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_execution_context::DialogueExecutionSource;
use musteragent_identity::RunId;
use serde::{Deserialize, Serialize};

/// Chosen by the sealed Tool version, never by model arguments. Historical
/// directory contracts retain their original source population and wire shape.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueExecutionDirectoryContract {
    IntentAndTaskV1,
    GoalV2,
}
impl DialogueExecutionDirectoryContract {
    pub fn from_tool_key(key: &str) -> Option<Self> {
        match key {
            "tasks.executions.list@1" => Some(Self::IntentAndTaskV1),
            "tasks.executions.list@2" => Some(Self::GoalV2),
            _ => None,
        }
    }
    pub const fn tool_key(self) -> &'static str {
        match self {
            Self::IntentAndTaskV1 => "tasks.executions.list@1",
            Self::GoalV2 => "tasks.executions.list@2",
        }
    }
    pub const fn includes_goals(self) -> bool {
        matches!(self, Self::GoalV2)
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DialogueExecutionDirectoryFilter {
    #[default]
    All,
    Active,
    Terminal,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueExecutionDirectoryCursor {
    pub agent_id: uuid::Uuid,
    pub dialogue_id: uuid::Uuid,
    pub epoch: u64,
    pub through_seq: u64,
    pub through_admitted_at_ms: i64,
    pub before_admitted_at_ms: i64,
    pub before_run_id: uuid::Uuid,
}
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueExecutionDirectoryQuery {
    pub cursor: Option<DialogueExecutionDirectoryCursor>,
    #[serde(default = "default_limit")]
    pub limit: u16,
    #[serde(default)]
    pub state: DialogueExecutionDirectoryFilter,
}
fn default_limit() -> u16 {
    25
}
impl DialogueExecutionDirectoryQuery {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > 4096 {
            return Err(DialogueStoreError::Corrupt);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if !(1..=100).contains(&self.limit)
            || self.cursor.is_some_and(|c| {
                c.agent_id.is_nil()
                    || c.dialogue_id.is_nil()
                    || c.before_run_id.is_nil()
                    || c.epoch == 0
                    || c.epoch > i64::MAX as u64
                    || c.through_seq > i64::MAX as u64
                    || c.before_admitted_at_ms < 0
                    || c.before_admitted_at_ms > c.through_admitted_at_ms
            })
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DialogueExecutionSubject {
    Intent,
    Task,
    TaskReport,
    Goal,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DialogueExecutionHome {
    Cloud,
    Native,
}
#[derive(Debug, Clone, Serialize)]
pub struct DialogueExecutionExcerpt {
    pub text: String,
    pub truncated: bool,
}
impl DialogueExecutionExcerpt {
    pub fn from_text(text: &str) -> Self {
        let mut chars = text.chars();
        let text = chars.by_ref().take(512).collect();
        Self {
            text,
            truncated: chars.next().is_some(),
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct DialogueExecutionDelivery {
    /// Counts only currently visible original publications in this dialogue.
    pub committed_publications: u64,
    pub committed_results: u64,
    pub committed_artifact_cards: u64,
    /// None is unknown/not recorded, never inferred from Run success.
    pub terminal_notice: Option<String>,
}
#[derive(Debug, Clone)]
pub struct DialogueExecutionDirectoryEntry {
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub subject: DialogueExecutionSubject,
    pub execution_home: DialogueExecutionHome,
    pub source: DialogueExecutionSource,
    pub run_id: RunId,
    /// Cursor of the accepted input in this executing dialogue. For delegated
    /// Tasks this is the reservation's local context, not a foreign user entry.
    pub accepted_epoch: u64,
    pub accepted_seq: u64,
    pub admitted_at_ms: i64,
    pub activity: DialogueExecutionActivityFact,
    /// Only the original user message in this same public dialogue. A delegated
    /// Task from another Agent does not expose that Agent's private input/Goal.
    pub request: Option<DialogueExecutionExcerpt>,
    pub goal_ref: Option<ContentRef>,
    pub delivery: DialogueExecutionDelivery,
}
#[derive(Debug, Clone)]
pub struct DialogueExecutionDirectoryPage {
    pub entries: Vec<DialogueExecutionDirectoryEntry>,
    /// Source reads enter the existing terminal content-grant/retention graph.
    /// Goal references are structural metadata, never included as read grants.
    pub authorized_content: Vec<ContentRef>,
    pub observed_at_ms: i64,
    pub through_admitted_at_ms: i64,
    pub next_cursor: Option<DialogueExecutionDirectoryCursor>,
}
pub fn dialogue_execution_directory_value(
    page: &DialogueExecutionDirectoryPage,
) -> serde_json::Value {
    use serde_json::json;
    let entries = page.entries.iter().map(|e| {
        use DialogueActivityState as S;
        use DialogueActivityStep as P;
        use DialogueActivityOutcome as O;
        let state = match e.activity.state {
            S::Working => "running", S::Waiting => "waiting", S::Unconfirmed => "unconfirmed",
            S::Terminal => "terminal", S::Idle => "idle",
        };
        let step = e.activity.step.map(|s| match s {
            P::Preparing => "preparing", P::Thinking => "thinking", P::UsingTool => "using_tool",
            P::WaitingUser => "waiting_user", P::WaitingApproval => "waiting_approval",
            P::WaitingDependency => "waiting_dependency", P::Cancelling => "cancelling",
        });
        let outcome = e.activity.outcome.map(|o| match o {
            O::Succeeded => "succeeded", O::Failed => "failed", O::Cancelled => "cancelled",
            O::Interrupted => "interrupted",
        });
        let mut value = json!({"agent_id":e.agent_id.to_string(),"dialogue_id":e.dialogue_id.to_string(),
            "subject":e.subject,"execution_home":e.execution_home,"intent_id":e.source.intent_id().map(|id|id.to_string()),
            "task_id":e.source.task_id().map(|id|id.to_string()),"run_id":e.run_id.to_string(),
            "accepted_epoch":e.accepted_epoch,"accepted_seq":e.accepted_seq,"admitted_at_ms":e.admitted_at_ms,
            "state":state,"step":step,"outcome":outcome,"request":e.request,
            "goal_ref":e.goal_ref.as_ref().map(|r|json!({"content_id":r.content_id.to_string(),"media_type":r.media_type.as_str(),"byte_length":r.byte_length,"sha256":r.sha256.to_string()})),
            "delivery":e.delivery,
            "production_selector":{"kind":"run","run_id":e.run_id.to_string()}});
        if let DialogueExecutionSource::Goal { goal_id, pursuit_id } = e.source {
            value["goal"] = json!({"goal_id":goal_id.to_string(),"pursuit_id":pursuit_id.to_string()});
        }
        value
    }).collect::<Vec<_>>();
    json!({"executions":entries,"observed_at_ms":page.observed_at_ms,
        "through_admitted_at_ms":page.through_admitted_at_ms,"next_cursor":page.next_cursor,
        "pagination":"fixed_admission_bound_current_observation"})
}

#[cfg(test)]
mod tests;
