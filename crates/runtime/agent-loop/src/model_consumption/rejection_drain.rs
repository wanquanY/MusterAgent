//! Once an action is rejected, only the physical response envelope can still
//! contribute evidence. No remaining body or Tool batch crosses admission.
use super::*;
use musteragent_runtime_model::ModelStreamProgress;

impl CanonicalConsumptionState {
    pub(crate) fn reject_tool_batch(&mut self) {
        self.rejected_tool_call_count = u32::try_from(self.tools.len()).unwrap_or(u32::MAX);
        self.coalescer.reject_tool_batch();
        self.tools.clear();
        self.tool_outputs.clear();
        self.tool_argument_fragments.clear();
        self.private_reasoning.clear();
        self.provider_reasoning_replay.clear();
    }

    pub(crate) fn stage_rejected_event(
        &self,
        event: ModelStreamEvent,
        budget: ModelStreamBudgetSnapshot,
    ) -> Result<StagedCanonicalEvent, LoopPortError> {
        let mut staged = self.clone();
        staged.account_event(&event, budget)?;
        if matches!(
            event,
            ModelStreamEvent::ToolCallStarted { .. } | ModelStreamEvent::ToolCallUnexposed { .. }
        ) {
            staged.rejected_tool_call_count = staged
                .rejected_tool_call_count
                .checked_add(1)
                .filter(|count| *count <= budget.max_tool_calls.get())
                .ok_or_else(quota)?;
        }
        // A rejected Tool body need not be executable or complete to observe
        // the Provider's real terminal. Its envelope, identity and cumulative
        // usage still must be valid. EOF and a semantic error are never proof.
        if staged.provider_response_ref.is_none() || staged.stop_reason.is_some() {
            return Err(invariant());
        }
        let mut progress = ModelStreamProgress::Unchanged;
        let mut provider_failure = None;
        let mut update = crate::output_coalescer::CoalescerUpdate {
            should_commit: false,
            has_pending_output: staged.coalescer.has_pending(),
        };
        match &event {
            ModelStreamEvent::ResponseStarted { .. }
            | ModelStreamEvent::ArtifactContentFrame { .. } => return Err(invariant()),
            ModelStreamEvent::Usage(usage) => {
                usage.validate().map_err(|_| invariant())?;
                if staged
                    .usage
                    .is_some_and(|previous| !usage.dominates(previous))
                {
                    return Err(invariant());
                }
                staged.usage = Some(*usage);
                update = staged.coalescer.accept(event).map_err(|_| invariant())?;
            }
            ModelStreamEvent::ResponseCompleted { stop_reason } => {
                staged.stop_reason = Some(*stop_reason);
                progress = ModelStreamProgress::Terminal;
                update = staged.coalescer.accept(event).map_err(|_| invariant())?;
            }
            ModelStreamEvent::ResponseError(error) => provider_failure = Some(error.clone()),
            // The first rejection remains authoritative. Remaining private
            // body bytes are counted against the same sealed stream limits,
            // discarded, and never counted as liveness progress.
            _ => {}
        }
        Ok(StagedCanonicalEvent {
            state: staged,
            update,
            provider_failure,
            progress,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU32, NonZeroUsize};
    fn started() -> (CanonicalConsumptionState, ModelStreamBudgetSnapshot) {
        let policy =
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults();
        let state = CanonicalConsumptionState::new(
            policy.max_output_frame_bytes.get(),
            policy.max_model_events_per_commit.get(),
        );
        let event = ModelStreamEvent::ResponseStarted {
            response_ref: ProviderRequestRef::from_opaque(
                OpaqueExternalRef::new("rejection-budget").expect("response"),
            ),
            response_model_key: musteragent_model::ModelKey::new("model").expect("model"),
        };
        let mut state = state
            .stage_event(
                InvocationId::generate(),
                event,
                policy.model_stream,
                |_, _, _| None,
            )
            .expect("start")
            .state;
        state.reject_tool_batch();
        (state, policy.model_stream)
    }
    fn rejected() -> ModelStreamEvent {
        ModelStreamEvent::ToolCallUnexposed {
            name: musteragent_tool::WireName::new("unexposed").expect("name"),
        }
    }
    #[test]
    fn rejection_drain_shares_original_event_and_byte_limits() {
        let (state, mut budget) = started();
        budget.max_events = NonZeroU32::new(2).expect("bound");
        let state = state
            .stage_rejected_event(rejected(), budget)
            .expect("second event")
            .state;
        assert!(matches!(
            state.stage_rejected_event(
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop
                },
                budget
            ),
            Err(LoopPortError {
                code: ErrorCode::QuotaExceeded,
                ..
            })
        ));
        let (state, mut budget) = started();
        budget.max_event_bytes = NonZeroUsize::new(1).expect("bound");
        assert!(matches!(
            state.stage_rejected_event(rejected(), budget),
            Err(LoopPortError {
                code: ErrorCode::QuotaExceeded,
                ..
            })
        ));
    }
    #[test]
    fn rejection_drain_never_accepts_another_response_or_runtime_derived_output() {
        let (state, budget) = started();
        assert!(
            state
                .stage_rejected_event(
                    ModelStreamEvent::ResponseStarted {
                        response_ref: ProviderRequestRef::from_opaque(
                            OpaqueExternalRef::new("foreign-response").expect("response")
                        ),
                        response_model_key: musteragent_model::ModelKey::new("model")
                            .expect("model"),
                    },
                    budget
                )
                .is_err()
        );
        assert!(
            state
                .stage_rejected_event(
                    ModelStreamEvent::ArtifactContentFrame {
                        output_id: OutputId::generate(),
                        frame_index: 0,
                        byte_offset_start: 0,
                        content: "untrusted".to_owned(),
                        final_for_output: false,
                    },
                    budget
                )
                .is_err()
        );
    }
}
