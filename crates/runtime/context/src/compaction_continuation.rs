//! Output continuation stays inside the original durable compaction graph.
//! A physical invocation may finish while its logical checkpoint is incomplete.
use crate::{
    ContextCompactionRequest, ContextCompactionResult, ContextPlanContentKind, ContextPlanError,
};
use musteragent_identity::InvocationId;
use musteragent_runtime_model::{ModelRouteRef, ModelText};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionContinuation {
    pub parent_invocation_id: InvocationId,
    /// Exact accumulated provider text, including whitespace at the join.
    pub prefix: ModelText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionCheckpoint {
    Complete(ModelText),
    Incomplete(ModelText),
}
impl CompactionCheckpoint {
    pub fn text(&self) -> &ModelText {
        match self {
            Self::Complete(text) | Self::Incomplete(text) => text,
        }
    }
    pub fn content_kind(&self) -> ContextPlanContentKind {
        match self {
            Self::Complete(_) => ContextPlanContentKind::CompactionSummary,
            Self::Incomplete(_) => ContextPlanContentKind::CompactionPartial,
        }
    }
    pub fn recover(media_type: &str, text: String) -> Result<Self, ContextPlanError> {
        if media_type
            == ContextPlanContentKind::CompactionSummary
                .media_type()
                .as_str()
        {
            return crate::ContextCompactionModelInvoker::validate_summary(text)
                .map(Self::Complete);
        }
        if media_type
            != ContextPlanContentKind::CompactionPartial
                .media_type()
                .as_str()
        {
            return Err(ContextPlanError::invariant());
        }
        validate_partial(text).map(Self::Incomplete)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCompactionStep {
    pub invocation_id: InvocationId,
    pub model_route_ref: ModelRouteRef,
    pub checkpoint: CompactionCheckpoint,
}
impl ContextCompactionStep {
    pub fn into_complete(self) -> Result<ContextCompactionResult, ContextPlanError> {
        let CompactionCheckpoint::Complete(summary) = self.checkpoint else {
            return Err(incomplete());
        };
        Ok(ContextCompactionResult {
            invocation_id: self.invocation_id,
            model_route_ref: self.model_route_ref,
            summary,
        })
    }
}
impl From<ContextCompactionResult> for ContextCompactionStep {
    fn from(result: ContextCompactionResult) -> Self {
        Self {
            invocation_id: result.invocation_id,
            model_route_ref: result.model_route_ref,
            checkpoint: CompactionCheckpoint::Complete(result.summary),
        }
    }
}

pub(crate) fn validate_partial(text: String) -> Result<ModelText, ContextPlanError> {
    if text.trim().is_empty() {
        return Err(incomplete());
    }
    if text.len() > crate::model_compaction::MAX_SUMMARY_BYTES {
        return Err(ContextPlanError::quota());
    }
    ModelText::new(text).map_err(|_| ContextPlanError::quota())
}

pub(crate) fn next_request(
    mut request: ContextCompactionRequest,
    step: ContextCompactionStep,
) -> Result<ContextCompactionRequest, ContextPlanError> {
    let CompactionCheckpoint::Incomplete(prefix) = step.checkpoint else {
        return Err(ContextPlanError::invariant());
    };
    if !musteragent_runtime_snapshot::uses_compaction_output_continuation(
        request.slot.compaction_policy_ref.as_str(),
    ) {
        return Err(incomplete());
    }
    if let Some(previous) = &request.continuation
        && (!prefix.as_str().starts_with(previous.prefix.as_str())
            || prefix.as_str()[previous.prefix.as_str().len()..]
                .trim()
                .is_empty())
    {
        return Err(incomplete());
    }
    request.continuation = Some(CompactionContinuation {
        parent_invocation_id: step.invocation_id,
        prefix,
    });
    request.slot.plan_fingerprint = request.expected_plan_fingerprint()?;
    request.validate()?;
    Ok(request)
}

fn incomplete() -> ContextPlanError {
    ContextPlanError {
        early_provider_cause: None,
        code: musteragent_runtime_contract::ErrorCode::ModelResponseInvalid,
        retryable: false,
        retry_after_ms: None,
    }
}
