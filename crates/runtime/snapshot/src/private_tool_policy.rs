//! Versioned private protocol tools are admitted by their sealed Agent role,
//! not by public discovery. They use the ordinary invocation journal/reactor.
use musteragent_identity::{CapabilityProviderId, VersionedKey};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, InvocationExecutionTiming,
};
use musteragent_tool::{ToolExecutionPolicy, ToolRetryContract};
use std::{sync::Arc, time::Duration};

pub struct PrivateToolPolicy {
    pub provider_id: CapabilityProviderId,
    pub retry: ToolRetryContract,
    pub liveness: Arc<musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy>,
}
impl PrivateToolPolicy {
    fn new(key: &str, provider: &str, retry: ToolRetryContract, idle: u64) -> Self {
        let execution = ToolExecutionPolicy::activity(
            VersionedKey::new(format!(
                "{}.liveness@1",
                key.split('@').next().expect("key")
            ))
            .expect("private policy key"),
            Duration::from_secs(idle),
        );
        Self {
            provider_id: CapabilityProviderId::from_uuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                provider.as_bytes(),
            ))
            .expect("private provider"),
            retry,
            liveness: execution.liveness().expect("activity").clone(),
        }
    }
    /// Apply only the floor sealed with this Run. Missing historical wire
    /// fields decode to zero, preserving prior policy references on recovery.
    #[must_use]
    pub fn with_minimum_window(mut self, minimum: Duration) -> Self {
        let idle = self.liveness.windows().idle.max(minimum);
        self.liveness = ToolExecutionPolicy::activity(self.liveness.reference().key.clone(), idle)
            .liveness()
            .expect("activity")
            .clone();
        self
    }
    pub fn evaluation(key: &str) -> Option<Self> {
        matches!(
            key,
            "runtime.evaluation.decide@1" | "runtime.evaluation.read-evidence@1"
        )
        .then(|| {
            Self::new(
                key,
                "musteragent:runtime:evaluation",
                ToolRetryContract::ReadOnly,
                60,
            )
        })
    }
    pub fn dream(key: &str) -> Option<Self> {
        (key == "runtime.dream.submit_experience_candidates@2").then(|| {
            Self::new(
                key,
                "musteragent:runtime:dream",
                ToolRetryContract::ReadOnly,
                60,
            )
        })
    }
    pub fn timing(&self) -> InvocationExecutionTiming {
        InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: self.liveness.reference().clone(),
            business_deadline: None,
        })
    }
}
