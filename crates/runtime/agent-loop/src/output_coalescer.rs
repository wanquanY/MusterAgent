use std::collections::{BTreeMap, btree_map::Entry};

use musteragent_identity::{InvocationId, OutputId};
use musteragent_runtime_model::{ModelStreamEvent, ModelText};

use crate::ToolDocumentPreview;

/// Result of accepting one canonical provider event.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct CoalescerUpdate {
    /// A byte threshold or semantic boundary made records ready to commit.
    pub(crate) should_commit: bool,
    /// An uncommitted public output buffer is waiting for its fixed deadline.
    pub(crate) has_pending_output: bool,
}

/// Application-owned policy boundary between provider events and durable
/// public output.
///
/// Raw reasoning and incomplete Tool-argument deltas are deliberately not
/// forwarded to the public Journal. The canonical stream validator and Tool
/// accumulator consume them before this boundary.
#[derive(Clone)]
pub(crate) struct ModelEventCoalescer {
    max_output_bytes: usize,
    pending: Option<PendingOutput>,
    /// A Tool call becomes public only after the provider has completed the
    /// whole response. Argument failures become durable Tool outcomes;
    /// until then start/completion events remain an
    /// invocation-private protocol buffer.
    pending_tool_events: Vec<ModelStreamEvent>,
    artifact_previews: BTreeMap<OutputId, PendingArtifactPreview>,
    ready: Vec<ModelStreamEvent>,
}

#[derive(Clone)]
struct PendingOutput {
    channel: PublicChannel,
    output_index: u32,
    text: String,
}

#[derive(Clone)]
struct PendingArtifactPreview {
    artifact_id: musteragent_identity::ArtifactId,
    tool_invocation_id: InvocationId,
    model_invocation_id: InvocationId,
    operation: crate::ToolDocumentPreviewOperation,
    title: Option<String>,
    observed_content: String,
    pending: String,
    next_frame_index: u64,
    next_byte_offset: u64,
    finalized: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum PublicChannel {
    AssistantText,
    ReasoningSummary,
}

impl ModelEventCoalescer {
    pub(crate) fn new(max_output_bytes: usize, event_capacity: usize) -> Self {
        Self {
            max_output_bytes,
            pending: None,
            pending_tool_events: Vec::new(),
            artifact_previews: BTreeMap::new(),
            ready: Vec::with_capacity(event_capacity),
        }
    }

    /// A semantically rejected response cannot release any of its private
    /// Tool lifecycle, even when its physical terminal is subsequently read.
    pub(crate) fn reject_tool_batch(&mut self) {
        self.pending_tool_events.clear();
    }

    pub(crate) fn accept(&mut self, event: ModelStreamEvent) -> Result<CoalescerUpdate, ()> {
        let mut should_commit = false;
        match event {
            ModelStreamEvent::ToolCallUnexposed { .. } => return Err(()),
            ModelStreamEvent::ToolCallRejected {
                output_index,
                call_id,
                ..
            } => {
                should_commit = self.flush_pending()?;
                // Never publish malformed argument bytes. The associated
                // planned call carries a durable, non-executable rejection.
                self.pending_tool_events
                    .push(ModelStreamEvent::ToolCallCompleted {
                        output_index,
                        call_id,
                        arguments: musteragent_runtime_model::JsonValue::Object(BTreeMap::new()),
                    });
            }
            ModelStreamEvent::TextDelta {
                output_index,
                delta,
            } => {
                should_commit =
                    self.append_public(PublicChannel::AssistantText, output_index, delta.as_str())?;
            }
            ModelStreamEvent::ReasoningSummaryDelta {
                output_index,
                delta,
            } => {
                // A whitespace-only summary must not create a public output.
                let summary_is_open = self.pending.as_ref().is_some_and(|pending| {
                    pending.channel == PublicChannel::ReasoningSummary
                        && pending.output_index == output_index
                });
                if summary_is_open || !delta.as_str().trim().is_empty() {
                    should_commit = self.append_public(
                        PublicChannel::ReasoningSummary,
                        output_index,
                        delta.as_str(),
                    )?;
                } else {
                    // The channel switch is still a semantic boundary for a
                    // previously buffered Assistant output.
                    should_commit = self.flush_pending()?;
                }
            }
            ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ProviderReasoningReplay { .. }
            | ModelStreamEvent::ToolCallArgumentsDelta { .. }
            | ModelStreamEvent::ArtifactContentFrame { .. } => {
                // Raw reasoning belongs to protected provider diagnostics.
                // Partial Tool arguments are superseded by ToolCallCompleted.
                // ArtifactContentFrame is Runtime-derived and enters through
                // `accept_artifact_preview`, never through a Provider stream.
            }
            event @ (ModelStreamEvent::ToolCallStarted { .. }
            | ModelStreamEvent::ToolCallCompleted { .. }) => {
                // Flush preceding Assistant text without admitting an
                // incomplete Tool lifecycle. Tool validation happens after
                // the response has closed and its usage has been committed.
                should_commit = self.flush_pending()?;
                self.pending_tool_events.push(event);
            }
            event @ ModelStreamEvent::ResponseCompleted { .. } => {
                self.flush_pending()?;
                self.ready.append(&mut self.pending_tool_events);
                self.ready.push(event);
                should_commit = true;
            }
            event @ ModelStreamEvent::ResponseError(_) => {
                self.flush_pending()?;
                // An errored response has not admitted a complete executable
                // Tool batch. Preserve the Provider failure but discard the
                // invocation-private partial Tool protocol.
                self.pending_tool_events.clear();
                self.ready.push(event);
                should_commit = true;
            }
            event => {
                self.flush_pending()?;
                should_commit = is_semantic_commit_boundary(&event);
                self.ready.push(event);
            }
        }
        Ok(CoalescerUpdate {
            should_commit,
            has_pending_output: self.has_pending(),
        })
    }

    pub(crate) fn accept_artifact_preview(
        &mut self,
        model_invocation_id: InvocationId,
        preview: ToolDocumentPreview,
    ) -> Result<CoalescerUpdate, ()> {
        let output_id =
            OutputId::from_uuid(preview.tool_invocation_id.into_uuid()).map_err(|_| ())?;
        if let Entry::Vacant(entry) = self.artifact_previews.entry(output_id) {
            let header = preview_header(model_invocation_id, &preview)?;
            entry.insert(PendingArtifactPreview {
                artifact_id: preview.artifact_id,
                tool_invocation_id: preview.tool_invocation_id,
                model_invocation_id,
                operation: preview.operation,
                title: preview.title.clone(),
                observed_content: String::new(),
                pending: header,
                next_frame_index: 0,
                next_byte_offset: 0,
                finalized: false,
            });
        }
        let state = self.artifact_previews.get(&output_id).ok_or(())?;
        if state.finalized
            || state.artifact_id != preview.artifact_id
            || state.tool_invocation_id != preview.tool_invocation_id
            || state.model_invocation_id != model_invocation_id
            || state.operation != preview.operation
            || (state.title.is_some() && preview.title.is_some() && state.title != preview.title)
            || !preview.content.starts_with(&state.observed_content)
        {
            return Err(());
        }
        let suffix = preview
            .content
            .get(state.observed_content.len()..)
            .ok_or(())?
            .to_owned();
        if let Some(state) = self.artifact_previews.get_mut(&output_id) {
            if state.title.is_none() {
                state.title = preview.title;
            }
            state.observed_content = preview.content;
        }
        let should_commit = self.append_artifact_preview(output_id, &suffix)?;
        Ok(CoalescerUpdate {
            should_commit,
            has_pending_output: self.has_pending(),
        })
    }

    pub(crate) fn finish_artifact_preview(
        &mut self,
        output_id: OutputId,
    ) -> Result<CoalescerUpdate, ()> {
        if !self.artifact_previews.contains_key(&output_id) {
            return Ok(CoalescerUpdate {
                should_commit: false,
                has_pending_output: self.has_pending(),
            });
        }
        self.flush_artifact_preview(output_id, true)?;
        Ok(CoalescerUpdate {
            should_commit: true,
            has_pending_output: self.has_pending(),
        })
    }

    pub(crate) fn flush_pending(&mut self) -> Result<bool, ()> {
        let mut flushed = false;
        let pending = self.pending.take();
        // Provider-compatible APIs occasionally emit whitespace-only content
        // before a terminal event. It is not an Assistant response and must
        // not create a public frame that later conflicts with EmptyResponse
        // recovery. Keeping it buffered until this boundary still preserves
        // legitimate leading whitespace when non-whitespace text follows.
        if let Some(pending) = pending
            && !pending.text.trim().is_empty()
        {
            self.push_public_event(pending.channel, pending.output_index, pending.text)?;
            flushed = true;
        }
        let output_ids = self
            .artifact_previews
            .iter()
            .filter_map(|(output_id, state)| (!state.pending.is_empty()).then_some(*output_id))
            .collect::<Vec<_>>();
        for output_id in output_ids {
            self.flush_artifact_preview(output_id, false)?;
            flushed = true;
        }
        Ok(flushed)
    }

    #[must_use]
    pub(crate) fn ready_len(&self) -> usize {
        self.ready.len()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn has_pending_output(&self) -> bool {
        self.has_pending()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn pending_tool_event_count(&self) -> usize {
        self.pending_tool_events.len()
    }

    pub(crate) fn ready_batch(&self, limit: usize) -> Vec<ModelStreamEvent> {
        let count = self.ready.len().min(limit);
        self.ready[..count].to_vec()
    }

    pub(crate) fn acknowledge_ready(&mut self, count: usize) -> Result<(), ()> {
        if count > self.ready.len() {
            return Err(());
        }
        self.ready.drain(..count);
        Ok(())
    }

    fn append_public(
        &mut self,
        channel: PublicChannel,
        output_index: u32,
        mut delta: &str,
    ) -> Result<bool, ()> {
        let same_output = self.pending.as_ref().is_none_or(|pending| {
            pending.channel == channel && pending.output_index == output_index
        });
        if !same_output {
            self.flush_pending()?;
        }

        let mut threshold_reached = false;
        while !delta.is_empty() {
            let pending = self.pending.get_or_insert_with(|| PendingOutput {
                channel,
                output_index,
                text: String::with_capacity(self.max_output_bytes),
            });
            let remaining = self.max_output_bytes.saturating_sub(pending.text.len());
            let split_at = utf8_prefix_len(delta, remaining);
            if split_at == 0 {
                self.flush_pending()?;
                threshold_reached = true;
                continue;
            }
            pending.text.push_str(&delta[..split_at]);
            delta = &delta[split_at..];
            if pending.text.len() == self.max_output_bytes {
                self.flush_pending()?;
                threshold_reached = true;
            }
        }
        Ok(threshold_reached)
    }

    fn append_artifact_preview(
        &mut self,
        output_id: OutputId,
        mut delta: &str,
    ) -> Result<bool, ()> {
        let mut threshold_reached = false;
        while !delta.is_empty() {
            let pending_len = self
                .artifact_previews
                .get(&output_id)
                .ok_or(())?
                .pending
                .len();
            let remaining = self.max_output_bytes.saturating_sub(pending_len);
            let split_at = utf8_prefix_len(delta, remaining);
            if split_at == 0 {
                self.flush_artifact_preview(output_id, false)?;
                threshold_reached = true;
                continue;
            }
            self.artifact_previews
                .get_mut(&output_id)
                .ok_or(())?
                .pending
                .push_str(&delta[..split_at]);
            delta = &delta[split_at..];
            if self
                .artifact_previews
                .get(&output_id)
                .is_some_and(|state| state.pending.len() == self.max_output_bytes)
            {
                self.flush_artifact_preview(output_id, false)?;
                threshold_reached = true;
            }
        }
        Ok(threshold_reached)
    }

    fn flush_artifact_preview(
        &mut self,
        output_id: OutputId,
        final_for_output: bool,
    ) -> Result<(), ()> {
        let state = self.artifact_previews.get_mut(&output_id).ok_or(())?;
        if state.finalized || (state.pending.is_empty() && !final_for_output) {
            return Ok(());
        }
        let content = std::mem::take(&mut state.pending);
        self.ready.push(ModelStreamEvent::ArtifactContentFrame {
            output_id,
            frame_index: state.next_frame_index,
            byte_offset_start: state.next_byte_offset,
            content: content.clone(),
            final_for_output,
        });
        state.next_frame_index = state.next_frame_index.checked_add(1).ok_or(())?;
        state.next_byte_offset = state
            .next_byte_offset
            .checked_add(u64::try_from(content.len()).map_err(|_| ())?)
            .ok_or(())?;
        state.finalized = final_for_output;
        Ok(())
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending.is_some()
            || self
                .artifact_previews
                .values()
                .any(|state| !state.pending.is_empty())
    }

    fn push_public_event(
        &mut self,
        channel: PublicChannel,
        output_index: u32,
        text: String,
    ) -> Result<(), ()> {
        let delta = ModelText::new(text).map_err(|_| ())?;
        self.ready.push(match channel {
            PublicChannel::AssistantText => ModelStreamEvent::TextDelta {
                output_index,
                delta,
            },
            PublicChannel::ReasoningSummary => ModelStreamEvent::ReasoningSummaryDelta {
                output_index,
                delta,
            },
        });
        Ok(())
    }
}

fn preview_header(
    model_invocation_id: InvocationId,
    preview: &ToolDocumentPreview,
) -> Result<String, ()> {
    let value = serde_json::json!({
        "schema": "musteragent.note-preview.v3",
        "artifact_id": preview.artifact_id.to_string(),
        "tool_invocation_id": preview.tool_invocation_id.to_string(),
        "model_invocation_id": model_invocation_id.to_string(),
        "operation": preview.operation.as_str(),
        "title": preview.title,
    });
    let mut encoded = serde_json::to_string(&value).map_err(|_| ())?;
    encoded.push('\n');
    Ok(encoded)
}

fn utf8_prefix_len(value: &str, max_bytes: usize) -> usize {
    if value.len() <= max_bytes {
        return value.len();
    }
    let mut split_at = max_bytes;
    while split_at > 0 && !value.is_char_boundary(split_at) {
        split_at -= 1;
    }
    split_at
}

fn is_semantic_commit_boundary(event: &ModelStreamEvent) -> bool {
    matches!(event, ModelStreamEvent::NativeCompactionItem { .. })
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{ArtifactId, InvocationId, ToolCallId};
    use musteragent_runtime_model::{
        JsonValue, ModelStreamEvent, ModelText, ProviderError, ProviderErrorCode,
        ProviderErrorKind, StopReason,
    };
    use musteragent_tool::WireName;

    use super::ModelEventCoalescer;
    use crate::{ToolDocumentPreview, ToolDocumentPreviewOperation};

    #[test]
    fn coalesces_many_text_deltas_and_preserves_utf8() {
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        for delta in ["你", "好", "，", "Musteragent"] {
            coalescer
                .accept(ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new(delta).expect("valid text"),
                })
                .expect("coalesce");
        }
        coalescer.flush_pending().expect("flush");

        assert_eq!(
            coalescer.ready_batch(32),
            vec![ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new("你好，Musteragent").expect("valid text"),
            }]
        );
    }

    #[test]
    fn publishes_only_safe_growing_note_content_with_replay_identity() {
        let model_invocation_id = InvocationId::generate();
        let tool_invocation_id = InvocationId::generate();
        let artifact_id =
            ArtifactId::from_uuid(tool_invocation_id.into_uuid()).expect("artifact identity");
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        for content in ["# 标题", "# 标题\n\n正文"] {
            coalescer
                .accept_artifact_preview(
                    model_invocation_id,
                    ToolDocumentPreview {
                        artifact_id,
                        tool_invocation_id,
                        operation: ToolDocumentPreviewOperation::Create,
                        title: Some("文档".to_owned()),
                        content: content.to_owned(),
                    },
                )
                .expect("preview");
        }
        coalescer.flush_pending().expect("flush preview");
        let output_id = tool_invocation_id
            .to_string()
            .parse()
            .expect("output identity");
        coalescer
            .finish_artifact_preview(output_id)
            .expect("finalize preview");

        let events = coalescer.ready_batch(32);
        assert_eq!(events.len(), 2);
        let ModelStreamEvent::ArtifactContentFrame {
            content,
            frame_index,
            byte_offset_start,
            final_for_output,
            ..
        } = &events[0]
        else {
            panic!("expected content frame");
        };
        let (header, markdown) = content.split_once('\n').expect("preview header");
        assert!(header.contains("musteragent.note-preview.v3"));
        assert!(header.contains(&artifact_id.to_string()));
        assert_eq!(markdown, "# 标题\n\n正文");
        assert_eq!((*frame_index, *byte_offset_start), (0, 0));
        assert!(!final_for_output);
        assert!(matches!(
            &events[1],
            ModelStreamEvent::ArtifactContentFrame {
                frame_index: 1,
                content,
                final_for_output: true,
                ..
            } if content.is_empty()
        ));
    }

    #[test]
    fn coalesces_the_observed_349_delta_reply_into_one_public_event() {
        let source = "x".repeat(1_377);
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        let mut offset = 0;
        for index in 0..349 {
            let remaining_chunks = 349 - index;
            let remaining_bytes = source.len() - offset;
            let chunk_bytes = remaining_bytes.div_ceil(remaining_chunks);
            let end = offset + chunk_bytes;
            coalescer
                .accept(ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new(&source[offset..end]).expect("valid text"),
                })
                .expect("coalesce");
            offset = end;
        }
        coalescer.flush_pending().expect("flush");
        let events = coalescer.ready_batch(32);
        coalescer
            .acknowledge_ready(events.len())
            .expect("ack ready");

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events.as_slice(),
            [ModelStreamEvent::TextDelta { delta, .. }] if delta.as_str() == source
        ));
    }

    #[test]
    fn raw_reasoning_and_whitespace_summary_are_not_public() {
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        coalescer
            .accept(ModelStreamEvent::ReasoningDelta {
                output_index: 0,
                delta: ModelText::new("private chain of thought").expect("valid text"),
            })
            .expect("ignore raw reasoning");
        coalescer
            .accept(ModelStreamEvent::ReasoningSummaryDelta {
                output_index: 0,
                delta: ModelText::new(" \n ").expect("valid text"),
            })
            .expect("ignore empty summary");

        assert_eq!(coalescer.ready_len(), 0);
        assert!(!coalescer.has_pending_output());
    }

    #[test]
    fn whitespace_only_assistant_text_is_not_a_public_output() {
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        coalescer
            .accept(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new(" \n ").expect("valid text"),
            })
            .expect("buffer whitespace");
        let update = coalescer
            .accept(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            })
            .expect("complete");

        assert!(update.should_commit);
        assert!(!update.has_pending_output);
        let events = coalescer.ready_batch(1);
        assert!(matches!(
            events.as_slice(),
            [ModelStreamEvent::ResponseCompleted { .. }]
        ));
        coalescer
            .acknowledge_ready(events.len())
            .expect("ack ready");
        assert_eq!(coalescer.ready_len(), 0);
    }

    #[test]
    fn completion_flushes_pending_output_before_the_boundary() {
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        coalescer
            .accept(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new("answer").expect("valid text"),
            })
            .expect("buffer");
        let update = coalescer
            .accept(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            })
            .expect("complete");

        assert!(update.should_commit);
        assert!(!update.has_pending_output);
        let text = coalescer.ready_batch(1);
        assert!(matches!(
            text.as_slice(),
            [ModelStreamEvent::TextDelta { .. }]
        ));
        coalescer.acknowledge_ready(text.len()).expect("ack text");
        assert!(matches!(
            coalescer.ready_batch(1).as_slice(),
            [ModelStreamEvent::ResponseCompleted { .. }]
        ));
    }

    #[test]
    fn tool_lifecycle_is_private_until_the_terminal_response_boundary() {
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        let call_id = ToolCallId::new("call-1").expect("call id");
        coalescer
            .accept(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: WireName::new("interaction_request").expect("wire name"),
                provider_item_ref: None,
                provider_replay: None,
            })
            .expect("buffer Tool start");
        coalescer
            .accept(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(Default::default()),
            })
            .expect("buffer Tool completion");

        assert_eq!(coalescer.ready_len(), 0);
        assert_eq!(coalescer.pending_tool_event_count(), 2);

        let update = coalescer
            .accept(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            })
            .expect("complete response");
        assert!(update.should_commit);
        assert_eq!(coalescer.pending_tool_event_count(), 0);
        assert!(matches!(
            coalescer.ready_batch(3).as_slice(),
            [
                ModelStreamEvent::ToolCallStarted { .. },
                ModelStreamEvent::ToolCallCompleted { .. },
                ModelStreamEvent::ResponseCompleted { .. }
            ]
        ));
    }

    #[test]
    fn provider_failure_discards_an_incomplete_tool_lifecycle() {
        let mut coalescer = ModelEventCoalescer::new(4 * 1_024, 32);
        coalescer
            .accept(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: ToolCallId::new("call-1").expect("call id"),
                name: WireName::new("interaction_request").expect("wire name"),
                provider_item_ref: None,
                provider_replay: None,
            })
            .expect("buffer Tool start");
        let failure = ProviderError {
            kind: ProviderErrorKind::Protocol,
            safe_code: ProviderErrorCode::ProtocolViolation,
            retryable: false,
            retry_after_ms: None,
            diagnostic_ref: None,
        };
        coalescer
            .accept(ModelStreamEvent::ResponseError(failure))
            .expect("record provider failure");

        assert_eq!(coalescer.pending_tool_event_count(), 0);
        assert!(matches!(
            coalescer.ready_batch(2).as_slice(),
            [ModelStreamEvent::ResponseError(_)]
        ));
    }
}
