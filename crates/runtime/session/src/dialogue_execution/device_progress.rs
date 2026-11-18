//! Authenticated service observations, scoped to the original live custody.
use super::*;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeviceAttemptObservation {
    AuthorizationCheckFailed,
    DependencyUnavailable,
    /// A qualified Native validator rejected this source before admission.
    /// This blocks identical preparation; it is never an execution outcome.
    RuntimeContractRejected,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeviceAdmissionObservation {
    pub claim: DeviceIntentClaim,
    pub source_digest: Sha256,
    pub expected_grant_hash: Option<Sha256>,
    pub observation: DeviceAttemptObservation,
}
