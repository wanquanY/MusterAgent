mod progress;
pub use progress::ModelStreamProgress;

use std::collections::BTreeMap;

use musteragent_identity::ToolCallId;
use thiserror::Error;

use crate::{ModelStreamEvent, ModelUsage};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ResponseState {
    NotStarted,
    Streaming,
    Terminal,
}

/// Validates normalized Provider events before they can enter the Ledger.
#[derive(Debug, Clone)]
pub struct ModelStreamState {
    response: ResponseState,
    tools: BTreeMap<ToolCallId, bool>,
    latest_usage: Option<ModelUsage>,
    rejected_tool_actions: bool,
}

impl Default for ModelStreamState {
    fn default() -> Self {
        Self {
            response: ResponseState::NotStarted,
            tools: BTreeMap::new(),
            latest_usage: None,
            rejected_tool_actions: false,
        }
    }
}

impl ModelStreamState {
    pub fn accept(&mut self, event: &ModelStreamEvent) -> Result<(), ModelStreamStateError> {
        if self.response == ResponseState::Terminal {
            return Err(ModelStreamStateError::EventAfterTerminal);
        }
        match event {
            ModelStreamEvent::ResponseStarted { .. } => {
                if self.response != ResponseState::NotStarted {
                    return Err(ModelStreamStateError::DuplicateStart);
                }
                self.response = ResponseState::Streaming;
            }
            ModelStreamEvent::ResponseError(_) if self.response == ResponseState::NotStarted => {
                self.response = ResponseState::Terminal;
            }
            _ if self.response == ResponseState::NotStarted => {
                return Err(ModelStreamStateError::OutputBeforeStart);
            }
            ModelStreamEvent::ToolCallUnexposed { .. } => {
                // A local action rejection is not a Provider terminal. No
                // Tool in this response can be admitted; retain only strict
                // response-envelope/usage validation while its body drains.
                self.rejected_tool_actions = true;
                self.tools.clear();
            }
            ModelStreamEvent::ToolCallStarted { .. }
            | ModelStreamEvent::ToolCallArgumentsDelta { .. }
            | ModelStreamEvent::ToolCallCompleted { .. }
            | ModelStreamEvent::ToolCallRejected { .. }
                if self.rejected_tool_actions => {}
            ModelStreamEvent::ToolCallStarted { call_id, .. } => {
                match self.tools.entry(call_id.clone()) {
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(ModelStreamStateError::DuplicateToolCall);
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(false);
                    }
                }
            }
            ModelStreamEvent::ToolCallArgumentsDelta { call_id, .. } => {
                if !matches!(self.tools.get(call_id), Some(false)) {
                    return Err(ModelStreamStateError::UnknownOrCompletedToolCall);
                }
            }
            ModelStreamEvent::ToolCallCompleted { call_id, .. }
            | ModelStreamEvent::ToolCallRejected { call_id, .. } => {
                if let ModelStreamEvent::ToolCallCompleted { arguments, .. } = event {
                    arguments
                        .as_object()
                        .map_err(|_| ModelStreamStateError::ToolArgumentsNotObject)?;
                }
                let completed = self
                    .tools
                    .get_mut(call_id)
                    .ok_or(ModelStreamStateError::UnknownOrCompletedToolCall)?;
                if *completed {
                    return Err(ModelStreamStateError::UnknownOrCompletedToolCall);
                }
                *completed = true;
            }
            ModelStreamEvent::Usage(usage) => {
                // The latest Provider report wins; usage never fails a response.
                usage
                    .validate()
                    .map_err(|_| ModelStreamStateError::InvalidUsage)?;
                self.latest_usage = Some(*usage);
            }
            ModelStreamEvent::ResponseCompleted { .. } => {
                if self.tools.values().any(|completed| !completed) {
                    return Err(ModelStreamStateError::IncompleteToolCall);
                }
                self.response = ResponseState::Terminal;
            }
            ModelStreamEvent::ResponseError(_) => {
                self.response = ResponseState::Terminal;
            }
            ModelStreamEvent::ArtifactContentFrame { .. } => {
                return Err(ModelStreamStateError::RuntimeDerivedEventFromProvider);
            }
            ModelStreamEvent::TextDelta { .. }
            | ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ReasoningSummaryDelta { .. }
            | ModelStreamEvent::NativeCompactionItem { .. } => {}
            ModelStreamEvent::ProviderReasoningReplay { item, .. } => {
                item.validate()
                    .map_err(|_| ModelStreamStateError::InvalidProviderReplay)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self.response, ResponseState::Terminal)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ModelStreamStateError {
    #[error("Provider emitted a second response start")]
    DuplicateStart,
    #[error("Provider emitted output before response start")]
    OutputBeforeStart,
    #[error("Provider emitted an event after terminal")]
    EventAfterTerminal,
    #[error("Provider duplicated a Tool call identity")]
    DuplicateToolCall,
    #[error("Provider referenced an unknown or completed Tool call")]
    UnknownOrCompletedToolCall,
    #[error("Provider completed a Tool call with non-object arguments")]
    ToolArgumentsNotObject,
    #[error("Provider emitted a terminal response with an incomplete Tool call")]
    IncompleteToolCall,
    #[error("Provider usage violated the canonical usage contract")]
    InvalidUsage,
    #[error("Provider reasoning replay item is invalid")]
    InvalidProviderReplay,
    #[error("Provider emitted a Runtime-derived Artifact preview event")]
    RuntimeDerivedEventFromProvider,
}
