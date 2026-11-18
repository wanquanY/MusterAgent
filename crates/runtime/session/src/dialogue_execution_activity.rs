//! Safe public execution facts. Admission and messages cannot manufacture these
//! observations; only the original Run/checkpoint and retained terminal owners do.
use crate::{DeviceDeliveryBinding, DialogueStoreError, OriginDeviceExecutionTarget};
use musteragent_content_ref::Sha256;
use musteragent_identity::{Actor, AgentDialogueId, AgentId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
mod directory;
pub use directory::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(i16)]
pub enum DialogueActivityState {
    Idle = 1,
    Working = 2,
    Waiting = 3,
    Terminal = 4,
    Unconfirmed = 5,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i16)]
pub enum DialogueActivityStep {
    Preparing = 1,
    Thinking = 2,
    UsingTool = 3,
    WaitingUser = 4,
    WaitingApproval = 5,
    WaitingDependency = 6,
    Cancelling = 7,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(i16)]
pub enum DialogueActivityOutcome {
    Succeeded = 1,
    Failed = 2,
    Cancelled = 3,
    Interrupted = 4,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueExecutionActivityFact {
    pub state: DialogueActivityState,
    pub step: Option<DialogueActivityStep>,
    pub outcome: Option<DialogueActivityOutcome>,
}
impl DialogueExecutionActivityFact {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        use DialogueActivityState as S;
        use DialogueActivityStep as P;
        let valid = match self.state {
            S::Working => {
                self.outcome.is_none()
                    && matches!(
                        self.step,
                        None | Some(P::Preparing | P::Thinking | P::UsingTool | P::Cancelling)
                    )
            }
            S::Waiting => {
                self.outcome.is_none()
                    && matches!(
                        self.step,
                        None | Some(P::WaitingUser | P::WaitingApproval | P::WaitingDependency)
                    )
            }
            S::Terminal => self.step.is_none() && self.outcome.is_some(),
            S::Unconfirmed => self.step.is_none() && self.outcome.is_none(),
            S::Idle => false,
        };
        if valid {
            Ok(())
        } else {
            Err(DialogueStoreError::Corrupt)
        }
    }

    /// Closed storage codes of the original Run/Loop owners, shared by PG and
    /// SQLite. This is a projection only: no transitions, timers or message data.
    pub fn from_owner_projection(
        status: i64,
        wait: Option<i64>,
        phase: Option<i64>,
    ) -> Result<Self, DialogueStoreError> {
        use DialogueActivityOutcome as O;
        use DialogueActivityState as S;
        use DialogueActivityStep as P;
        if phase.is_some_and(|v| !(1..=8).contains(&v))
            || wait.is_some_and(|v| !(1..=11).contains(&v))
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let (state, step, outcome) = match status {
            1 => (S::Working, Some(P::Preparing), None),
            2 => (
                S::Working,
                match phase {
                    Some(1) => Some(P::Preparing),
                    Some(2) => Some(P::Thinking),
                    Some(3) => Some(P::UsingTool),
                    _ => None,
                },
                None,
            ),
            3 => (
                S::Waiting,
                Some(match wait {
                    Some(1) => P::WaitingApproval,
                    Some(2) => P::WaitingUser,
                    Some(3..=11) => P::WaitingDependency,
                    _ => return Err(DialogueStoreError::Corrupt),
                }),
                None,
            ),
            4 => (S::Working, Some(P::Cancelling), None),
            5 => (S::Terminal, None, Some(O::Succeeded)),
            6 => (S::Terminal, None, Some(O::Failed)),
            7 => (S::Terminal, None, Some(O::Cancelled)),
            8 => (S::Terminal, None, Some(O::Interrupted)),
            _ => return Err(DialogueStoreError::Corrupt),
        };
        Ok(Self {
            state,
            step,
            outcome,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgentActivity {
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub revision: u64,
    pub state: DialogueActivityState,
    pub step: Option<DialogueActivityStep>,
    pub active_execution_count: u32,
    pub waiting_execution_count: u32,
    pub waiting_approval_count: u32,
    pub scheduled_retry_count: u32,
    pub waiting_permission_count: u32,
    pub earliest_retry_at: Option<UtcTimestamp>,
    pub unconfirmed_execution_count: u32,
    pub foreground_execution_count: u32,
    pub pending_cancellation_count: u32,
    /// Maximum committed cancellation seen with these original target facts.
    /// None preserves historical observations without control-snapshot proof.
    pub observed_cancellation_sequence: Option<u64>,
    /// Original Native Run ended locally; stronger settlement is still pending.
    /// Both detail fields are absent on historical observations.
    pub locally_completed_unconfirmed_count: Option<u32>,
    /// Subset with an original stop whose physical outcome is still unresolved.
    pub unconfirmed_cancellation_count: Option<u32>,
    pub last_outcome: Option<DialogueActivityOutcome>,
    pub observed_at: UtcTimestamp,
}
impl OwnedAgentActivity {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        use DialogueActivityState as S;
        match (
            self.locally_completed_unconfirmed_count,
            self.unconfirmed_cancellation_count,
        ) {
            (None, None) => {}
            (Some(local), Some(stops))
                if self.observed_cancellation_sequence.is_some()
                    && stops <= local
                    && stops <= self.pending_cancellation_count => {}
            _ => return Err(DialogueStoreError::Corrupt),
        }
        let expected = if self.active_execution_count > 0 {
            S::Working
        } else if self.waiting_execution_count > 0 {
            S::Waiting
        } else if self.unconfirmed_execution_count > 0 {
            S::Unconfirmed
        } else if self.last_outcome.is_some() {
            S::Terminal
        } else {
            S::Idle
        };
        if self.revision == 0
            || self.revision > i64::MAX as u64
            || self
                .observed_cancellation_sequence
                .is_some_and(|v| v > i64::MAX as u64)
            || self.observed_at.unix_millis() < 0
            || u64::from(self.waiting_approval_count)
                + u64::from(self.scheduled_retry_count)
                + u64::from(self.waiting_permission_count)
                > u64::from(self.waiting_execution_count)
            || (self.scheduled_retry_count == 0) != self.earliest_retry_at.is_none()
            || self.earliest_retry_at.is_some_and(|t| t.unix_millis() <= 0)
            || self.pending_cancellation_count > self.foreground_execution_count
            || self.state != expected
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if self.state == S::Idle || self.state == S::Unconfirmed {
            if self.step.is_some() {
                return Err(DialogueStoreError::Corrupt);
            }
        } else {
            DialogueExecutionActivityFact {
                state: self.state,
                step: self.step,
                outcome: if self.state == S::Terminal {
                    self.last_outcome
                } else {
                    None
                },
            }
            .validate()?;
        }
        Ok(())
    }
    /// Input order is original observation order, oldest first. The final
    /// terminal outcome never removes another execution's working/waiting fact.
    pub fn aggregate(
        agent_id: AgentId,
        dialogue_id: AgentDialogueId,
        revision: u64,
        observed_at: UtcTimestamp,
        facts: impl IntoIterator<Item = DialogueExecutionActivityFact>,
    ) -> Result<Self, DialogueStoreError> {
        Self::aggregate_counts(
            agent_id,
            dialogue_id,
            revision,
            observed_at,
            facts.into_iter().map(|fact| (fact, 1)),
        )
    }
    /// Store-side grouping keeps history reads bounded by the closed state set.
    pub fn aggregate_counts(
        agent_id: AgentId,
        dialogue_id: AgentDialogueId,
        revision: u64,
        observed_at: UtcTimestamp,
        facts: impl IntoIterator<Item = (DialogueExecutionActivityFact, u32)>,
    ) -> Result<Self, DialogueStoreError> {
        use DialogueActivityState as S;
        let mut value = Self {
            agent_id,
            dialogue_id,
            revision,
            state: S::Idle,
            step: None,
            active_execution_count: 0,
            waiting_execution_count: 0,
            waiting_approval_count: 0,
            scheduled_retry_count: 0,
            waiting_permission_count: 0,
            earliest_retry_at: None,
            unconfirmed_execution_count: 0,
            foreground_execution_count: 0,
            pending_cancellation_count: 0,
            observed_cancellation_sequence: None,
            locally_completed_unconfirmed_count: None,
            unconfirmed_cancellation_count: None,
            last_outcome: None,
            observed_at,
        };
        let mut active_step = None;
        let mut waiting_step = None;
        for (fact, amount) in facts {
            if amount == 0 {
                return Err(DialogueStoreError::Corrupt);
            }
            fact.validate()?;
            let count = match fact.state {
                S::Working => {
                    active_step = fact.step;
                    Some(&mut value.active_execution_count)
                }
                S::Waiting => {
                    if fact.step == Some(DialogueActivityStep::WaitingApproval) {
                        value.waiting_approval_count = value
                            .waiting_approval_count
                            .checked_add(amount)
                            .ok_or(DialogueStoreError::Corrupt)?;
                    }
                    if waiting_step != Some(DialogueActivityStep::WaitingApproval) {
                        waiting_step = fact.step;
                    }
                    Some(&mut value.waiting_execution_count)
                }
                S::Unconfirmed => Some(&mut value.unconfirmed_execution_count),
                S::Terminal => {
                    value.last_outcome = fact.outcome;
                    None
                }
                S::Idle => return Err(DialogueStoreError::Corrupt),
            };
            if let Some(count) = count {
                *count = count
                    .checked_add(amount)
                    .ok_or(DialogueStoreError::Corrupt)?;
            }
        }
        (value.state, value.step) = if value.active_execution_count > 0 {
            (S::Working, active_step)
        } else if value.waiting_execution_count > 0 {
            (S::Waiting, waiting_step)
        } else if value.unconfirmed_execution_count > 0 {
            (S::Unconfirmed, None)
        } else if value.last_outcome.is_some() {
            (S::Terminal, None)
        } else {
            (S::Idle, None)
        };
        value.validate()?;
        Ok(value)
    }
}

pub const NATIVE_DIALOGUE_ACTIVITY_LIMIT: usize = 4096;
pub const NATIVE_DIALOGUE_ACTIVITY_RPC_LIMIT: usize = 32 * 1024;
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueActivityObservation {
    pub scheduled_retry_at: Option<UtcTimestamp>,
    pub permission_wait: bool,
    pub binding: DeviceDeliveryBinding,
    pub run_sequence: u64,
    pub checkpoint_version: u64,
    pub activity: DialogueExecutionActivityFact,
}
impl NativeDialogueActivityObservation {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.binding
            .validate_subject()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if self.permission_wait
            && (self.scheduled_retry_at.is_some()
                || self.activity.state != DialogueActivityState::Waiting
                || self.activity.step != Some(DialogueActivityStep::WaitingDependency))
            || self.scheduled_retry_at.is_some_and(|t| {
                t.unix_millis() <= 0
                    || !matches!(
                        (self.activity.state, self.activity.step),
                        (
                            DialogueActivityState::Working,
                            Some(DialogueActivityStep::Preparing)
                        ) | (
                            DialogueActivityState::Waiting,
                            Some(DialogueActivityStep::WaitingDependency)
                        )
                    )
            })
            || self.run_sequence == 0
            || [
                self.binding.source_digest,
                self.binding.snapshot_hash,
                self.binding.grant_proof_hash,
            ]
            .iter()
            .any(|v| v.into_bytes() == [0; 32])
            || matches!(
                self.activity.state,
                DialogueActivityState::Idle | DialogueActivityState::Unconfirmed
            )
        {
            return Err(DialogueStoreError::Corrupt);
        }
        self.activity.validate()
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueActivityAcknowledgement {
    pub binding: DeviceDeliveryBinding,
    pub run_sequence: u64,
    pub checkpoint_version: u64,
    pub observation_sha256: Sha256,
}
impl NativeDialogueActivityAcknowledgement {
    pub fn validate_for(
        &self,
        observation: &NativeDialogueActivityObservation,
        hash: Sha256,
    ) -> Result<(), DialogueStoreError> {
        if self.binding != observation.binding
            || self.run_sequence != observation.run_sequence
            || self.checkpoint_version != observation.checkpoint_version
            || self.observation_sha256 != hash
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
pub trait NativeDialogueActivityStore: Send + Sync {
    fn observe<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        observation: &'a NativeDialogueActivityObservation,
    ) -> ApplicationFuture<'a, Result<NativeDialogueActivityAcknowledgement, DialogueStoreError>>;
}

#[cfg(test)]
#[path = "dialogue_execution_activity/tests.rs"]
mod tests;
