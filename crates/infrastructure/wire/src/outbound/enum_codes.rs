use crate::wire;
use musteragent_run::AttemptTrigger;
use musteragent_runtime_contract::{InterruptionReason, ResumeReason};

pub(super) fn interruption_reason(value: InterruptionReason) -> i32 {
    (match value {
        InterruptionReason::LeaseLost => wire::InterruptionReason::LeaseLost,
        InterruptionReason::WorkerLost => wire::InterruptionReason::WorkerLost,
        InterruptionReason::ControlHomeMoved => wire::InterruptionReason::ControlHomeMoved,
        InterruptionReason::SystemShutdown => wire::InterruptionReason::SystemShutdown,
        InterruptionReason::ExternalOutcomeUnknown => {
            wire::InterruptionReason::ExternalOutcomeUnknown
        }
        InterruptionReason::CoordinationRequested => {
            wire::InterruptionReason::CoordinationRequested
        }
        InterruptionReason::CoordinationForcedAbort => {
            wire::InterruptionReason::CoordinationForcedAbort
        }
    }) as i32
}

pub(super) fn resume_reason(value: ResumeReason) -> i32 {
    (match value {
        ResumeReason::InteractionResponded => wire::ResumeReason::InteractionResponded,
        ResumeReason::RetryBackoffElapsed => wire::ResumeReason::RetryBackoffElapsed,
        ResumeReason::DeviceAvailable => wire::ResumeReason::DeviceAvailable,
        ResumeReason::Recovery => wire::ResumeReason::Recovery,
        ResumeReason::BudgetAmended => wire::ResumeReason::BudgetAmended,
        ResumeReason::DelegationCompleted => wire::ResumeReason::DelegationCompleted,
        ResumeReason::ReconciliationDue => wire::ResumeReason::ReconciliationDue,
    }) as i32
}

pub(super) fn attempt_trigger(value: AttemptTrigger) -> i32 {
    (match value {
        AttemptTrigger::Initial => wire::AttemptTrigger::Initial,
        AttemptTrigger::Retry => wire::AttemptTrigger::Retry,
        AttemptTrigger::Recovery => wire::AttemptTrigger::Recovery,
        AttemptTrigger::InteractionResumed => wire::AttemptTrigger::InteractionResumed,
        AttemptTrigger::BudgetAmended => wire::AttemptTrigger::BudgetAmended,
        AttemptTrigger::DelegationCompleted => wire::AttemptTrigger::DelegationCompleted,
        AttemptTrigger::ReconciliationDue => wire::AttemptTrigger::ReconciliationDue,
    }) as i32
}
