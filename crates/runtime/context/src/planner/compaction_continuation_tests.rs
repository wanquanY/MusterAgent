use super::*;
use crate::{
    CompactionCheckpoint, CompactionGenerationObservation, CompactionGenerationState,
    ContextCompactionStep,
};
use musteragent_identity::{UtcTimestamp, VersionedKey};
use musteragent_runtime_model::ModelRouteRef;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

type Saved = (
    ContextCompactionRequest,
    Result<ContextCompactionStep, ContextPlanError>,
);
struct Fixture {
    paid: Mutex<Vec<Saved>>,
    interrupt_once: AtomicBool,
    unknown_second: bool,
    repeat_second: bool,
}
impl Fixture {
    fn new() -> Self {
        Self {
            paid: Mutex::new(Vec::new()),
            interrupt_once: AtomicBool::new(false),
            unknown_second: false,
            repeat_second: false,
        }
    }
}
impl ContextCompactor for Fixture {
    fn prepare_invocation(
        &self,
        request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionRequest, ContextPlanError>> {
        Box::pin(async move {
            let saved = self.paid.lock().unwrap();
            let high = saved.iter().map(|(r, _)| r.slot.generation).max();
            let observations = saved
                .iter()
                .rev()
                .map(|(r, result)| CompactionGenerationObservation {
                    generation: r.slot.generation,
                    generation_high_watermark: high.unwrap(),
                    invocation_id: r.invocation.invocation_id,
                    correlation_id: r.invocation.correlation_id,
                    plan_fingerprint: r.slot.plan_fingerprint,
                    logical_plan_fingerprint: Some(r.logical_plan_fingerprint().unwrap()),
                    sealed_ledger_cutoff: Some(r.slot.ledger_cutoff),
                    sealed_input_tokens_before: Some(r.slot.input_tokens_before),
                    owner_attempt_id: r.invocation.attempt_context.attempt_id,
                    owner_fencing_token: r.invocation.attempt_context.fencing_token,
                    timing: r.invocation.timing.clone(),
                    state: match result {
                        Ok(_) => CompactionGenerationState::Succeeded,
                        Err(e) if e.is_indeterminate() => {
                            CompactionGenerationState::DispatchedUnknown
                        }
                        Err(e) => CompactionGenerationState::Failed(e.clone()),
                    },
                })
                .collect::<Vec<_>>();
            let mut selected = observations.first().cloned();
            for prior in observations {
                if request.matches_sealed_plan(&prior)? {
                    selected = Some(prior);
                    break;
                }
            }
            request.select_recovery_generation(selected, UtcTimestamp::from_unix_millis(1000))
        })
    }
    fn compact(
        &self,
        _: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        Box::pin(async {
            panic!("the planner must use physical steps, never publish partial results")
        })
    }
    fn compact_step(
        &self,
        request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionStep, ContextPlanError>> {
        Box::pin(async move {
            let mut saved = self.paid.lock().unwrap();
            if let Some((_, result)) = saved
                .iter()
                .find(|(r, _)| r.invocation.invocation_id == request.invocation.invocation_id)
            {
                return result.clone();
            }
            if request.continuation.is_some() && self.interrupt_once.swap(false, Ordering::SeqCst) {
                return Err(ContextPlanError::timeout()); // process exits before next dispatch
            }
            let result = if request.continuation.is_some() && self.unknown_second {
                Err(ContextPlanError::indeterminate())
            } else {
                let previous = request
                    .continuation
                    .as_ref()
                    .map(|c| c.prefix.as_str())
                    .unwrap_or("");
                let checkpoint = if previous.is_empty() {
                    CompactionCheckpoint::Incomplete(ModelText::new("## Goal\nPreserve ").unwrap())
                } else if self.repeat_second {
                    CompactionCheckpoint::Incomplete(ModelText::new(previous).unwrap())
                } else if !previous.contains("文件") {
                    CompactionCheckpoint::Incomplete(
                        ModelText::new(format!("{previous}文件🦀 and finish ")).unwrap(),
                    )
                } else {
                    CompactionCheckpoint::Complete(
                        ModelText::new(format!("{previous}the task.")).unwrap(),
                    )
                };
                Ok(ContextCompactionStep {
                    invocation_id: request.invocation.invocation_id,
                    model_route_ref: ModelRouteRef::from_key(request.model.route.route_ref.clone()),
                    checkpoint,
                })
            };
            saved.push((request, result.clone()));
            result
        })
    }
}
fn request() -> ContextCompactionRequest {
    use musteragent_runtime_contract::execution_liveness::{
        ActivityExecutionTiming, InvocationExecutionTiming, LivenessPolicyRef,
    };
    let mut request = crate::model_compaction::tests::request();
    request.slot.compaction_policy_ref =
        VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V10).unwrap();
    request.recovery_policy =
        musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default();
    request.invocation.timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new("test.execution@1").unwrap(),
            digest: Sha256::digest(b"activity"),
        },
        business_deadline: None,
    });
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
    request
}

#[tokio::test]
async fn interrupted_output_continuation_resumes_paid_prefix_and_replays_the_whole_graph() {
    let fixture = Fixture::new();
    fixture.interrupt_once.store(true, Ordering::SeqCst);
    let mut request = request();
    assert!(
        compact(&fixture, request.clone())
            .await
            .unwrap_err()
            .retryable
    );
    assert_eq!(fixture.paid.lock().unwrap().len(), 1);
    // Admission refresh changes the estimate, not the original source truth.
    request.slot.input_tokens_before += 17;
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
    let final_result = compact(&fixture, request.clone()).await.unwrap();
    assert_eq!(
        final_result.summary.as_str(),
        "## Goal\nPreserve 文件🦀 and finish the task."
    );
    assert_eq!(
        compact(&fixture, request.clone()).await.unwrap(),
        final_result
    );
    let paid = fixture.paid.lock().unwrap();
    assert_eq!(
        paid.len(),
        3,
        "one physical dispatch per distinct continuation, including recovery"
    );
    for (index, (step, _)) in paid.iter().enumerate() {
        assert_eq!(step.slot.generation.get(), index as u32 + 1);
        assert_eq!(step.serialized_history, request.serialized_history);
        if index > 0 {
            assert_eq!(
                step.continuation.as_ref().unwrap().parent_invocation_id,
                paid[index - 1].0.invocation.invocation_id
            );
        }
    }
}

#[tokio::test]
async fn unknown_or_nonprogressing_continuation_cannot_spin_or_redispatch() {
    for unknown in [false, true] {
        let mut fixture = Fixture::new();
        fixture.unknown_second = unknown;
        fixture.repeat_second = !unknown;
        let request = request();
        let first = compact(&fixture, request.clone()).await.unwrap_err();
        assert_eq!(first.is_indeterminate(), unknown);
        assert_eq!(compact(&fixture, request).await.unwrap_err(), first);
        assert_eq!(fixture.paid.lock().unwrap().len(), 2);
    }
}

#[test]
fn input_overflow_during_fragment_continuation_retains_partial_checkpoint() {
    let mut request = request();
    request.serialized_history = serde_json::to_vec(&serde_json::json!({
        "schema_version": "musteragent.context.compaction-fragment.v1", "source_sha256": "original", "source_byte_length": 100, "offset": 2, "fragment": "source facts"
    })).unwrap();
    request.continuation = Some(crate::CompactionContinuation {
        parent_invocation_id: request.invocation.invocation_id,
        prefix: ModelText::new("## Goal\nAlready generated ").unwrap(),
    });
    let source = fragment_source(&request).unwrap();
    let material: serde_json::Value = serde_json::from_str(&source.text).unwrap();
    assert_eq!(material["history"], "source facts");
    assert!(!source.text.contains("source_sha256"));
    assert_eq!(
        material["incomplete_checkpoint"],
        "## Goal\nAlready generated "
    );
    assert_eq!(source.offset, 0);
}
