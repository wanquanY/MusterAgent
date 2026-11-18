use musteragent_runtime_model::{CanonicalRuntimeEvent, ModelText};

use crate::{ContextPlanError, MaterializedDelegationCompletion};

pub(super) fn delegation_completion_events(
    completions: &[MaterializedDelegationCompletion],
) -> Result<Vec<CanonicalRuntimeEvent>, ContextPlanError> {
    completions
        .iter()
        .map(|completion| {
            let report = std::str::from_utf8(&completion.report.bytes)
                .map_err(|_| ContextPlanError::invariant())?;
            Ok(CanonicalRuntimeEvent::DelegationCompletion {
                delivery_id: completion.delivery_id,
                delegation_id: completion.delegation_id,
                child_activity_id: completion.child_activity_id,
                child_run_id: completion.child_run_id,
                report: ModelText::new(report.to_owned()).map_err(|_| ContextPlanError::quota())?,
            })
        })
        .collect()
}
