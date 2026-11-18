use musteragent_activity::Activity;
use musteragent_identity::Actor;
pub(super) use musteragent_ledger::AdmissionSnapshotRef as RunSnapshotRef;
use musteragent_ledger::{
    AdmissionBatch, AppendRequest, CoordinationAdmissionBatch, MaintenanceAdmissionBatch,
    RetryAdmissionBatch, StandingGoalAdmissionBatch,
};
use musteragent_run::Run;
use musteragent_runtime_contract::TraceContext;

/// Message-free projection of the fields shared by every durable Run
/// admission. Keeping this internal view explicit prevents Maintenance from
/// fabricating an [`AdmissionBatch`] merely to reuse persistence helpers.
#[derive(Copy, Clone)]
pub(super) struct RunAdmissionRef<'a> {
    /// Set only by derived admission after checking the persisted source.
    pub(super) derived_goal: bool,
    pub(super) actor: Actor,
    pub(super) trace_context: &'a TraceContext,
    pub(super) append: &'a AppendRequest,
    pub(super) activity: &'a Activity,
    pub(super) run: &'a Run,
    pub(super) snapshot: RunSnapshotRef<'a>,
    pub(super) owner_goal: Option<&'a StandingGoalAdmissionBatch>,
    pub(super) agent_work_item:
        Option<&'a musteragent_runtime_session::WorkItemExecutionPreparation>,
}

impl<'a> From<&'a AdmissionBatch> for RunAdmissionRef<'a> {
    fn from(batch: &'a AdmissionBatch) -> Self {
        Self {
            derived_goal: false,
            agent_work_item: None,
            owner_goal: None,
            actor: batch.actor,
            trace_context: &batch.trace_context,
            append: &batch.append,
            activity: &batch.activity,
            run: &batch.run,
            snapshot: RunSnapshotRef::Interactive(&batch.runtime_snapshot),
        }
    }
}

impl<'a> From<&'a MaintenanceAdmissionBatch> for RunAdmissionRef<'a> {
    fn from(batch: &'a MaintenanceAdmissionBatch) -> Self {
        Self {
            derived_goal: false,
            agent_work_item: None,
            owner_goal: None,
            actor: batch.actor,
            trace_context: &batch.trace_context,
            append: &batch.append,
            activity: &batch.activity,
            run: &batch.run,
            snapshot: RunSnapshotRef::Maintenance(&batch.runtime_snapshot),
        }
    }
}

impl<'a> From<&'a CoordinationAdmissionBatch> for RunAdmissionRef<'a> {
    fn from(batch: &'a CoordinationAdmissionBatch) -> Self {
        Self {
            derived_goal: false,
            agent_work_item: None,
            owner_goal: None,
            actor: batch.actor,
            trace_context: &batch.trace_context,
            append: &batch.append,
            activity: &batch.activity,
            run: &batch.run,
            snapshot: RunSnapshotRef::Interactive(&batch.runtime_snapshot),
        }
    }
}

impl<'a> From<&'a StandingGoalAdmissionBatch> for RunAdmissionRef<'a> {
    fn from(batch: &'a StandingGoalAdmissionBatch) -> Self {
        Self {
            derived_goal: false,
            agent_work_item: None,
            owner_goal: matches!(
                batch.source,
                musteragent_ledger::StandingGoalAdmissionSource::OwnerGoal { .. }
            )
            .then_some(batch),
            actor: batch.actor,
            trace_context: &batch.trace_context,
            append: &batch.append,
            activity: &batch.activity,
            run: &batch.run,
            snapshot: RunSnapshotRef::Interactive(&batch.runtime_snapshot),
        }
    }
}

impl<'a> From<&'a RetryAdmissionBatch> for RunAdmissionRef<'a> {
    fn from(batch: &'a RetryAdmissionBatch) -> Self {
        Self {
            derived_goal: false,
            agent_work_item: None,
            owner_goal: None,
            actor: batch.actor,
            trace_context: &batch.trace_context,
            append: &batch.append,
            activity: &batch.activity,
            run: &batch.run,
            snapshot: RunSnapshotRef::Interactive(&batch.runtime_snapshot),
        }
    }
}

impl<'a> From<&'a musteragent_runtime_session::WorkItemExecutionAdmissionCommit>
    for RunAdmissionRef<'a>
{
    fn from(commit: &'a musteragent_runtime_session::WorkItemExecutionAdmissionCommit) -> Self {
        Self {
            agent_work_item: Some(&commit.preparation),
            ..Self::from(&commit.admission.batch)
        }
    }
}

impl<'a> From<&'a musteragent_ledger::DelegationAdmissionBatch> for RunAdmissionRef<'a> {
    fn from(batch: &'a musteragent_ledger::DelegationAdmissionBatch) -> Self {
        Self {
            derived_goal: false,
            agent_work_item: None,
            owner_goal: None,
            actor: batch.actor,
            trace_context: &batch.trace_context,
            append: &batch.append,
            activity: &batch.activity,
            run: &batch.run,
            snapshot: RunSnapshotRef::Interactive(&batch.runtime_snapshot),
        }
    }
}
