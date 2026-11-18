//! Owned Compression model operation. The original compaction slot owns
//! dispatch and durable completion; this adapter only reads canonical progress.
use super::*;
use musteragent_execution_liveness::{ExecutionActivitySink, LivenessError};
use musteragent_runtime_contract::execution_liveness::{ActivitySource, ExecutionBinding};
use musteragent_runtime_model::{ModelExecution, ModelStreamProgress};
use std::num::NonZeroU64;

#[derive(Default)]
pub struct CompactionExecutionObservation {
    usage: Option<ModelUsage>,
    terminal: bool,
    cursor: u64,
}
impl CompactionExecutionObservation {
    pub fn usage(&self) -> Option<ModelUsage> {
        self.usage
    }

    pub fn terminal_observed(&self) -> bool {
        self.terminal
    }

    pub(super) fn record(
        &mut self,
        event: &ModelStreamEvent,
        progress: ModelStreamProgress,
        sink: Option<&dyn ExecutionActivitySink>,
    ) -> Result<(), LivenessError> {
        // Validated supplier usage remains a fact even if Home rejects late
        // progress. It must not disappear just because the call is stopping.
        if let ModelStreamEvent::Usage(usage) = event {
            self.usage = Some(*usage);
        }
        if progress == ModelStreamProgress::Terminal {
            self.terminal = true;
        }
        let Some(sink) = sink else {
            return Ok(());
        };
        match progress {
            ModelStreamProgress::Accepted => sink.accepted().map(|_| ()),
            ModelStreamProgress::Advanced => {
                self.cursor = self
                    .cursor
                    .checked_add(1)
                    .ok_or(LivenessError::ScopeClosed)?;
                sink.advanced(
                    ActivitySource::CanonicalModelEvent,
                    NonZeroU64::new(self.cursor).ok_or(LivenessError::ScopeClosed)?,
                )
            }
            ModelStreamProgress::Terminal => sink.ended(),
            ModelStreamProgress::Unchanged => Ok(()),
        }
    }
}

impl ContextCompactionModelInvoker {
    /// Pure construction before dispatch; the caller polls preflight inside
    /// the shared reactor and then applies the original slot's dispatch fence.
    pub fn execution(
        &self,
        request: &ContextCompactionRequest,
        invocation: InvocationContext,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'_> + '_>, ContextCompactionModelFailure> {
        let canonical = self
            .canonical_request(request, invocation)
            .map_err(ContextCompactionModelFailure::without_usage)?;
        self.provider
            .execution(canonical, binding)
            .map_err(|error| {
                ContextCompactionModelFailure::without_usage(provider_failure(&error, false))
            })
    }

    pub async fn consume_execution(
        &self,
        request: &ContextCompactionRequest,
        invocation: &InvocationContext,
        operation: Box<dyn ModelExecution<'_> + '_>,
        activity: &dyn ExecutionActivitySink,
        observed: &mut CompactionExecutionObservation,
    ) -> Result<ContextCompactionModelOutput, ContextCompactionModelFailure> {
        consume_events(
            invocation.invocation_id,
            ModelRouteRef::from_key(request.model.route.route_ref.clone()),
            operation.into_stream(),
            None,
            Some(activity),
            observed,
            output_policy(request),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn canonical_end_returns_compaction_without_waiting_for_transport_eof() {
        let events = [
            ModelStreamEvent::ResponseStarted {
                response_ref: ProviderRequestRef::from_opaque(
                    musteragent_runtime_contract::OpaqueExternalRef::new(
                        "compaction-native-response",
                    )
                    .expect("reference"),
                ),
                response_model_key: ModelKey::new("compression-model").expect("model"),
            },
            ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new(super::super::tests::SUMMARY).expect("summary"),
            },
            ModelStreamEvent::Usage(ModelUsage::default()),
            ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            },
        ];
        let stream = futures_util::stream::iter(events.into_iter().map(Ok))
            .chain(futures_util::stream::pending());
        let mut observed = CompactionExecutionObservation::default();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            consume_events(
                InvocationId::generate(),
                ModelRouteRef::from_key(
                    musteragent_identity::VersionedKey::new("compression.test@1").expect("route"),
                ),
                Box::pin(stream),
                None,
                None,
                &mut observed,
                CompactionOutputPolicy::default(),
            ),
        )
        .await
        .expect("native end must not wait for EOF")
        .expect("valid checkpoint");
        assert!(observed.terminal_observed());
        assert_eq!(observed.usage(), Some(ModelUsage::default()));
        assert_eq!(
            result.checkpoint.text().as_str(),
            super::super::tests::SUMMARY
        );
    }

    struct Rejected;
    impl ExecutionActivitySink for Rejected {
        fn binding(&self) -> ExecutionBinding {
            panic!("pure observation does not construct a binding")
        }
        fn accepted(
            &self,
        ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
            Err(LivenessError::ScopeClosed)
        }
        fn advanced(&self, _: ActivitySource, _: NonZeroU64) -> Result<(), LivenessError> {
            Err(LivenessError::ScopeClosed)
        }
        fn wait_for_acceptance(
            &self,
        ) -> musteragent_runtime_contract::ApplicationFuture<
            '_,
            Result<(), musteragent_execution_liveness::LivenessError>,
        > {
            Box::pin(async { Ok(()) })
        }
        fn ended(&self) -> Result<(), LivenessError> {
            Err(LivenessError::ScopeClosed)
        }
    }
    #[test]
    fn valid_paid_usage_survives_home_rejection_without_becoming_stop_proof() {
        let usage = ModelUsage {
            output_tokens: 7,
            ..ModelUsage::default()
        };
        let mut observed = CompactionExecutionObservation::default();
        assert_eq!(
            observed.record(
                &ModelStreamEvent::Usage(usage),
                ModelStreamProgress::Advanced,
                Some(&Rejected)
            ),
            Err(LivenessError::ScopeClosed)
        );
        assert_eq!(observed.usage(), Some(usage));
        assert!(!observed.terminal_observed());
    }
}
