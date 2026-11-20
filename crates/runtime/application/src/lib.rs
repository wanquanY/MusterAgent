//! Host-facing durable Runtime application facade.
#![forbid(unsafe_code)]
mod grant_binding;
mod home_policy;
mod identity;
mod record;
mod service;
mod snapshot;
mod standing_goal_admission;
pub use home_policy::{RuntimeHomePolicy, RuntimeHomePolicyError};
pub use identity::derive_admission_run_id;
pub use musteragent_runtime_automation::StandingGoalStart;
pub use musteragent_runtime_contract::{runtime_error, runtime_error_with_diagnostic};
pub use musteragent_runtime_session::*;
pub use service::{
    CoordinationAdmissionReceipt, CoordinationPreparation, coordination_target_grant_nonce,
};
pub use service::{RuntimeApplication, RuntimeApplicationPorts, StandingGoalPreparation};
pub use snapshot::*;
pub use standing_goal_admission::{OwnerGoalAdmissionAdapter, RuntimeStandingGoalAdmission};
