//! Local monotonic response stages, independent of model text and wall clocks.
//! A Provider heartbeat/header event is not counted as visible answer text.
use musteragent_runtime_model::{CanonicalModelRequest, ModelStreamEvent};
use musteragent_telemetry::{ModelResponseStage, ModelResponseTiming};

pub(crate) struct ResponseTiming {
    timing: ModelResponseTiming,
}

impl ResponseTiming {
    pub(crate) fn new(request: &CanonicalModelRequest) -> Self {
        Self {
            timing: ModelResponseTiming::new(
                request.invocation.run_context.run_id,
                request.invocation.invocation_id,
            ),
        }
    }
    pub(crate) fn prepared(&self) {
        self.timing.observe(ModelResponseStage::RequestPrepared);
    }
    pub(crate) fn sending(&self) {
        self.timing.observe(ModelResponseStage::HttpSendStarted);
    }
    pub(crate) fn headers(&self) {
        self.timing.observe(ModelResponseStage::ResponseHeaders);
    }
    pub(crate) fn transport_bytes(&self, nonempty: bool) {
        if nonempty {
            self.timing.observe(ModelResponseStage::FirstTransportByte);
        }
    }
    pub(crate) fn provider_event(&self) {
        self.timing.observe(ModelResponseStage::FirstProviderEvent);
    }
    pub(crate) fn canonical(&self, event: &ModelStreamEvent) {
        if !self.timing.has_observed(ModelResponseStage::FirstText)
            && matches!(event, ModelStreamEvent::TextDelta { delta, .. } if delta.as_str().chars().any(|c| !c.is_whitespace()))
        {
            self.timing.observe(ModelResponseStage::FirstText);
            self.timing.observe(ModelResponseStage::FirstAction);
        }
        if matches!(event, ModelStreamEvent::ToolCallStarted { .. }) {
            self.timing.observe(ModelResponseStage::FirstAction);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::{InvocationId, RunId};
    use musteragent_runtime_model::ModelText;
    #[test]
    fn heartbeats_reasoning_and_whitespace_are_not_answer_text_or_action() {
        let timing = ResponseTiming {
            timing: ModelResponseTiming::new(RunId::generate(), InvocationId::generate()),
        };
        timing.transport_bytes(false);
        assert!(
            !timing
                .timing
                .has_observed(ModelResponseStage::FirstTransportByte)
        );
        timing.headers();
        timing.transport_bytes(true);
        timing.provider_event();
        timing.canonical(&ModelStreamEvent::ReasoningSummaryDelta {
            output_index: 0,
            delta: ModelText::new("thinking").expect("reasoning text"),
        });
        timing.canonical(&ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: ModelText::new(" \n").expect("whitespace text"),
        });
        assert!(!timing.timing.has_observed(ModelResponseStage::FirstText));
        assert!(!timing.timing.has_observed(ModelResponseStage::FirstAction));
        timing.canonical(&ModelStreamEvent::ToolCallStarted {
            output_index: 1,
            call_id: musteragent_identity::ToolCallId::new("call").expect("tool call ID"),
            name: musteragent_tool::WireName::new("read_file").expect("tool name"),
            provider_item_ref: None,
            provider_replay: None,
        });
        assert!(!timing.timing.has_observed(ModelResponseStage::FirstText));
        assert!(timing.timing.has_observed(ModelResponseStage::FirstAction));
        timing.canonical(&ModelStreamEvent::TextDelta {
            output_index: 2,
            delta: ModelText::new("正文").expect("answer text"),
        });
        assert!(timing.timing.has_observed(ModelResponseStage::FirstText));
        assert!(timing.timing.has_observed(ModelResponseStage::FirstAction));
    }
}
