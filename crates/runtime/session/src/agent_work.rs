//! Account-readable roots over the existing WorkItem, Schedule and Goal owners.
//! No task lifecycle, execution queue or writable aggregate lives in this view.
use crate::WorkItemAuthorityRecord;
use crate::{DialogueStoreError, OwnerSchedule};
use musteragent_goal::{GoalPursuit, StandingGoal};
use musteragent_identity::{Actor, AgentId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AgentWorkKind {
    WorkItem = 1,
    Schedule = 2,
    Goal = 3,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentWorkCategory {
    Once = 1,
    Scheduled = 2,
    Standing = 3,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentWorkReference {
    pub kind: AgentWorkKind,
    pub id: uuid::Uuid,
    pub agent_id: AgentId,
}

/// Frozen creation acknowledgement; current state stays with the root owner.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentWorkCreation {
    pub creation_id: uuid::Uuid,
    pub reference: AgentWorkReference,
    pub revision: u64,
    pub category: AgentWorkCategory,
    pub title: String,
    pub available: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentWorkCursor {
    pub through: UtcTimestamp,
    pub before_created_at: UtcTimestamp,
    pub before_kind: AgentWorkKind,
    pub before_id: uuid::Uuid,
}

#[derive(Debug, Clone)]
pub struct ReadAgentWork {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub kind: Option<AgentWorkKind>,
    pub category: Option<AgentWorkCategory>,
    pub include_finished: bool,
    pub limit: u16,
    pub cursor: Option<AgentWorkCursor>,
    pub reference: Option<AgentWorkReference>,
}

#[derive(Debug, Clone)]
pub enum AgentWorkRecord {
    WorkItem(WorkItemAuthorityRecord),
    /// One root in the public directory; each action retains its own Task state.
    WorkItemChain {
        root: WorkItemAuthorityRecord,
        current: WorkItemAuthorityRecord,
        sequence: u64,
    },
    Schedule {
        value: OwnerSchedule,
        created_at: UtcTimestamp,
        updated_at: UtcTimestamp,
    },
    Goal {
        value: StandingGoal,
        pursuit: Option<GoalPursuit>,
    },
}

#[derive(Debug, Clone)]
pub struct AgentWorkPage {
    /// Read only observation from the current original Task reservation queue.
    pub workspace_unavailable: Vec<musteragent_identity::WorkItemId>,
    pub agent_id: AgentId,
    pub records: Vec<AgentWorkRecord>,
    pub total_count: u64,
    pub next: Option<AgentWorkCursor>,
    pub observed_at: UtcTimestamp,
}

pub trait AgentWorkQuery: Send + Sync {
    fn read(
        &self,
        request: ReadAgentWork,
    ) -> ApplicationFuture<'_, Result<AgentWorkPage, DialogueStoreError>>;
}
