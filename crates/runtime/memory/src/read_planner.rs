//! Pure policy over the original invocation and Host-verified facts. Storage,
//! account scope, approval, publication and final Tool results stay in Runtime.
use std::sync::Arc;

pub use musteragent_memory_business::{
    experience_ranking::ExperienceCandidate as ExperienceRankingCandidate,
    inspection::InspectionFacts,
    read_plan::{QueryStrategy, ReadPlan, ReadPreparation},
};
use musteragent_runtime_capability::{CapabilityInvocation, ToolExecutionMaterial};
use musteragent_runtime_contract::ApplicationFuture;

use crate::MemoryToolError;

pub trait MemoryReadPlanner: Send + Sync {
    fn prepare<'a>(
        &'a self,
        request: &'a [u8],
    ) -> ApplicationFuture<'a, Result<ReadPreparation, MemoryToolError>>;

    fn rank_experiences<'a>(
        &'a self,
        query: &'a str,
        candidates: &'a [ExperienceRankingCandidate],
        limit: u16,
    ) -> ApplicationFuture<'a, Result<Vec<u16>, MemoryToolError>>;

    fn project_inspection<'a>(
        &'a self,
        facts: &'a InspectionFacts,
    ) -> ApplicationFuture<'a, Result<serde_json::Value, MemoryToolError>>;
}

pub trait MemoryReadPlannerFactory: Send + Sync {
    fn bind(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Arc<dyn MemoryReadPlanner>, MemoryToolError>;
}

/// Await physical process ownership before observing cancellation. Dropping
/// this RPC future would otherwise let the Tool stop before the Host reaps its
/// process. Data-store reads still use the ordinary cancellable read stage.
pub(crate) async fn complete_computation<T>(
    execution: Option<&musteragent_runtime_capability::OperationContext>,
    future: impl std::future::Future<Output = Result<T, MemoryToolError>>,
) -> Result<T, MemoryToolError> {
    if execution.is_some_and(|context| context.stopping()) {
        return Err(MemoryToolError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ));
    }
    let result = future.await?;
    if execution.is_some_and(|context| context.stopping()) {
        return Err(MemoryToolError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ));
    }
    Ok(result)
}
