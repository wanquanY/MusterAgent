use thiserror::Error;

const MAX_EXTERNAL_REF_BYTES: usize = 512;

/// Recovery choice derived from the pinned idempotency contract.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RetryDisposition {
    DoNotRetry,
    RetryNewAttempt,
    ReconcileBeforeRetry,
    RetryFallbackRoute,
}

/// Why a waiting Run returned to the admission queue.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ResumeReason {
    InteractionResponded,
    RetryBackoffElapsed,
    DeviceAvailable,
    Recovery,
    BudgetAmended,
    DelegationCompleted,
    ReconciliationDue,
}

/// Durable reason for terminating a Run without claiming normal completion or failure.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InterruptionReason {
    LeaseLost,
    WorkerLost,
    ControlHomeMoved,
    SystemShutdown,
    ExternalOutcomeUnknown,
    CoordinationRequested,
    CoordinationForcedAbort,
}

/// Bounded opaque reference to provider or reconciliation state.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpaqueExternalRef(String);

impl OpaqueExternalRef {
    pub fn new(value: impl Into<String>) -> Result<Self, OpaqueExternalRefError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_EXTERNAL_REF_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(OpaqueExternalRefError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("external reference must be bounded visible ASCII")]
pub struct OpaqueExternalRefError;
