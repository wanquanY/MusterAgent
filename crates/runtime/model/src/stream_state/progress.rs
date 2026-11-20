//! Canonical progress is a validated state advance, not a received SSE packet
//! or an incremented wrapper sequence. The caller still verifies Run/operation
//! provenance, stream budgets and callable Tool names before stamping at Home.
use super::{ModelStreamState, ModelStreamStateError};
use crate::{ModelStreamEvent, ModelUsage};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ModelStreamProgress {
    Accepted,
    Advanced,
    Unchanged,
    Terminal,
}
impl ModelStreamState {
    pub fn accept_with_progress(
        &mut self,
        event: &ModelStreamEvent,
    ) -> Result<ModelStreamProgress, ModelStreamStateError> {
        let previous_usage = self.latest_usage;
        self.accept(event)?;
        Ok(match event {
            ModelStreamEvent::ToolCallStarted { .. }
            | ModelStreamEvent::ToolCallCompleted { .. }
            | ModelStreamEvent::ToolCallRejected { .. }
            | ModelStreamEvent::ToolCallArgumentsDelta { .. }
            | ModelStreamEvent::TextDelta { .. }
            | ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ReasoningSummaryDelta { .. }
                if self.rejected_tool_actions =>
            {
                ModelStreamProgress::Unchanged
            }
            ModelStreamEvent::ResponseStarted { .. } => ModelStreamProgress::Accepted,
            ModelStreamEvent::TextDelta { .. }
            | ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ReasoningSummaryDelta { .. }
            | ModelStreamEvent::ToolCallStarted { .. }
            | ModelStreamEvent::ToolCallCompleted { .. }
            | ModelStreamEvent::ToolCallRejected { .. } => ModelStreamProgress::Advanced,
            ModelStreamEvent::ToolCallArgumentsDelta { delta, .. } => {
                if delta.is_empty() {
                    ModelStreamProgress::Unchanged
                } else {
                    ModelStreamProgress::Advanced
                }
            }
            ModelStreamEvent::Usage(usage) => {
                if usage_advances(*usage, previous_usage) {
                    ModelStreamProgress::Advanced
                } else {
                    ModelStreamProgress::Unchanged
                }
            }
            ModelStreamEvent::ResponseCompleted { .. } | ModelStreamEvent::ResponseError(_) => {
                ModelStreamProgress::Terminal
            }
            // The adapter can reject a name as soon as a tool starts. This is
            // a local policy terminal, not evidence that the Provider ended.
            ModelStreamEvent::ToolCallUnexposed { .. } => ModelStreamProgress::Unchanged,
            // Opaque replay/material references do not prove live work. Their
            // original committed checkpoint can supply its own admitted proof.
            ModelStreamEvent::ProviderReasoningReplay { .. }
            | ModelStreamEvent::NativeCompactionItem { .. } => ModelStreamProgress::Unchanged,
            ModelStreamEvent::ArtifactContentFrame { .. } => {
                return Err(ModelStreamStateError::RuntimeDerivedEventFromProvider);
            }
        })
    }
}
fn usage_advances(current: ModelUsage, previous: Option<ModelUsage>) -> bool {
    previous.map_or_else(
        || {
            [
                current.input_tokens_uncached,
                current.output_tokens,
                current.reasoning_tokens,
                current.provider_cache_read_tokens,
                current.provider_cache_write_tokens,
                current.provider_cache_write_1h_tokens,
            ]
            .into_iter()
            .any(|value| value > 0)
        },
        // Liveness only: some counter grew. A Provider correction downwards
        // is accepted as the latest report but is not new work.
        |previous| {
            current.input_tokens_uncached > previous.input_tokens_uncached
                || current.output_tokens > previous.output_tokens
                || current.reasoning_tokens > previous.reasoning_tokens
                || current.provider_cache_read_tokens > previous.provider_cache_read_tokens
                || current.provider_cache_write_tokens > previous.provider_cache_write_tokens
                || current.provider_cache_write_1h_tokens > previous.provider_cache_write_1h_tokens
        },
    )
}
