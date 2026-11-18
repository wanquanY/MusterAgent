//! Feedback for postprocessing. Execution observations
//! come exclusively from the Tool owner and its durable result boundary.
use crate::{LoopPortError, ToolExecutionOutcome};
use musteragent_runtime_model::{CanonicalContentPart, ModelText};
/// Preserve observed effects and an already committed wait. A postprocessing
/// dependency failure cannot erase the Tool result or terminate its Agent.
pub(super) fn postprocess_error(
    mut outcome: ToolExecutionOutcome,
    error: LoopPortError,
) -> ToolExecutionOutcome {
    if let ToolExecutionOutcome::Completed { message, .. } = &mut outcome {
        message.content.push(CanonicalContentPart::Text(ModelText::new(serde_json::json!({
            "postprocessing_error": {"code":error.code.as_str(), "retryable":error.retryable,
                "message":"Tool postprocessing failed; the preceding observed result remains valid."}
        }).to_string()).expect("bounded postprocessing error")));
    }
    outcome
}

pub(super) async fn control_request<T>(
    request: impl std::future::Future<Output = Result<T, LoopPortError>>,
) -> Result<T, LoopPortError> {
    tokio::time::timeout(
        musteragent_runtime_contract::execution_liveness::DISCOVERY_REQUEST_WINDOW,
        request,
    )
    .await
    .map_err(|_| LoopPortError {
        code: musteragent_runtime_contract::ErrorCode::DependencyTimeout,
        retryable: true,
    })?
}
