//! Provider-driven auxiliary capacity recovery. Each node goes through the same
//! durable invocation store; replay reuses paid results by logical fingerprint.
use super::*;
use crate::ContextCompactionResult;
use musteragent_content_ref::Sha256;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

pub(super) fn compact<'a>(
    compactor: &'a dyn ContextCompactor,
    request: ContextCompactionRequest,
) -> ApplicationFuture<'a, Result<ContextCompactionResult, ContextPlanError>> {
    Box::pin(async move {
        let prepared = compactor.prepare_invocation(request.clone()).await;
        compact_node(compactor, request, prepared).await
    })
}

/// The root node has already been recovered before its Started event. Child
/// nodes still enter through `compact` and their original durable lookup.
pub(super) fn compact_prepared<'a>(
    compactor: &'a dyn ContextCompactor,
    request: ContextCompactionRequest,
) -> ApplicationFuture<'a, Result<ContextCompactionResult, ContextPlanError>> {
    let prepared = request.clone();
    compact_node(compactor, request, Ok(prepared))
}

fn compact_node<'a>(
    compactor: &'a dyn ContextCompactor,
    mut request: ContextCompactionRequest,
    mut prepared: Result<ContextCompactionRequest, ContextPlanError>,
) -> ApplicationFuture<'a, Result<ContextCompactionResult, ContextPlanError>> {
    Box::pin(async move {
        loop {
            let attempted = match prepared {
                Ok(prepared) => {
                    let expected = prepared.invocation.invocation_id;
                    compactor.compact_step(prepared).await.and_then(|result| {
                        if result.invocation_id != expected
                            || result.model_route_ref.as_str()
                                != request.model.route.route_ref.as_str()
                        {
                            Err(ContextPlanError::invariant())
                        } else {
                            Ok(result)
                        }
                    })
                }
                Err(error) => Err(error),
            };
            match attempted {
                Ok(step) => match step.checkpoint {
                    crate::CompactionCheckpoint::Complete(_) => return step.into_complete(),
                    crate::CompactionCheckpoint::Incomplete(_) => {
                        request = crate::compaction_continuation::next_request(request, step)?;
                        prepared = compactor.prepare_invocation(request.clone()).await;
                        continue;
                    }
                },
                Err(error) if error.code == ErrorCode::ModelContextExceeded => break,
                Err(error) if request.continuation.is_some() => {
                    return Err(error.after_partial_output());
                }
                Err(error) => return Err(error),
            }
        }
        let source = fragment_source(&request)?;
        // A smaller body cannot repair a model that cannot accept the fixed
        // instructions plus this minimum fragment. Never spin on that request.
        if source.text.len() <= 1024 {
            return Err(capacity());
        }
        let mut middle = source.text.len() / 2;
        while !source.text.is_char_boundary(middle) {
            middle -= 1;
        }
        if middle == 0 {
            return Err(capacity());
        }
        let mut checkpoints = Vec::with_capacity(2);
        for (start, end) in [(0, middle), (middle, source.text.len())] {
            let body = serde_json::to_vec(&serde_json::json!({
                "schema_version": "musteragent.context.compaction-fragment.v1",
                "source_sha256": source.hash,
                "source_byte_length": source.total,
                "offset": source.offset + start,
                "end_offset": source.offset + end,
                "partial": true,
                "instruction": "Summarize only this source fragment. It may begin or end inside a record. Preserve constraints and unfinished work; do not infer omitted content or completion.",
                "fragment": &source.text[start..end],
            })).map_err(|_|ContextPlanError::invariant())?;
            let child = derived(&request, body)?;
            // Deterministic fragment identity includes byte range and source
            // digest, so equal-looking fragments cannot alias one another.
            let result = compact(compactor, child)
                .await
                .map_err(ContextPlanError::after_partial_output)?;
            checkpoints.push(serde_json::json!({
                "source_sha256": source.hash, "offset": source.offset + start,
                "end_offset": source.offset + end,
                "checkpoint": result.summary.as_str(),
            }));
        }
        let merged = serde_json::to_vec(&serde_json::json!({
            "schema_version": "musteragent.context.compaction-merge.v1",
            "instruction": "Merge these ordered partial checkpoints into one continuation checkpoint. Preserve all constraints, exact identifiers and outstanding work. A partial checkpoint is not proof of overall completion.",
            "checkpoints": checkpoints,
        })).map_err(|_|ContextPlanError::invariant())?;
        // Enforce actual byte progress, independently of token estimates.
        // A non-shrinking summary must not create an unbounded merge tree.
        if merged.len() >= source.text.len() {
            return Err(capacity());
        }
        compact(compactor, derived(&request, merged)?)
            .await
            .map_err(ContextPlanError::after_partial_output)
    })
}

struct FragmentSource {
    text: String,
    hash: String,
    total: usize,
    offset: usize,
}
fn fragment_source(request: &ContextCompactionRequest) -> Result<FragmentSource, ContextPlanError> {
    let value: serde_json::Value = serde_json::from_slice(&request.serialized_history)
        .map_err(|_| ContextPlanError::invariant())?;
    if request.continuation.is_none()
        && value["schema_version"] == "musteragent.context.compaction-fragment.v1"
    {
        let text = value["fragment"]
            .as_str()
            .ok_or_else(ContextPlanError::invariant)?
            .to_owned();
        let offset = value["offset"]
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(ContextPlanError::invariant)?;
        let total = value["source_byte_length"]
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(ContextPlanError::invariant)?;
        let hash = value["source_sha256"]
            .as_str()
            .ok_or_else(ContextPlanError::invariant)?
            .to_owned();
        if offset.checked_add(text.len()).is_none_or(|end| end > total) {
            return Err(ContextPlanError::invariant());
        }
        return Ok(FragmentSource {
            text,
            hash,
            total,
            offset,
        });
    }
    let semantic = match value["schema_version"].as_str() {
        Some("musteragent.context.compaction-fragment.v1") => value
            .get("fragment")
            .cloned()
            .ok_or_else(ContextPlanError::invariant)?,
        Some("musteragent.context.compaction-merge.v1") => {
            let checkpoints = value["checkpoints"]
                .as_array()
                .ok_or_else(ContextPlanError::invariant)?;
            serde_json::Value::Array(
                checkpoints
                    .iter()
                    .map(|item| {
                        item.get("checkpoint")
                            .cloned()
                            .ok_or_else(ContextPlanError::invariant)
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
        _ => value,
    };
    let mut document = serde_json::json!({
        "history": semantic,
        "previous_checkpoint": request.previous_summary.as_ref().map(|summary|summary.as_str()),
    });
    if let Some(continuation) = &request.continuation {
        document["incomplete_checkpoint"] = serde_json::json!(continuation.prefix.as_str());
    }
    let bytes = serde_json::to_vec(&document).map_err(|_| ContextPlanError::invariant())?;
    let hash = Sha256::digest(&bytes).to_string();
    let total = bytes.len();
    let text = String::from_utf8(bytes).map_err(|_| ContextPlanError::invariant())?;
    Ok(FragmentSource {
        text,
        hash,
        total,
        offset: 0,
    })
}
fn derived(
    parent: &ContextCompactionRequest,
    history: Vec<u8>,
) -> Result<ContextCompactionRequest, ContextPlanError> {
    let mut child = parent.clone();
    child.serialized_history = history;
    child.previous_summary = None;
    child.continuation = None;
    child.slot.previous_summary_ref = None;
    child.slot.plan_fingerprint = child.expected_plan_fingerprint()?;
    child.validate()?;
    Ok(child)
}
fn capacity() -> ContextPlanError {
    ContextPlanError {
        early_provider_cause: None,
        code: ErrorCode::ModelContextExceeded,
        retryable: false,
        retry_after_ms: None,
    }
}

#[cfg(test)]
#[path = "compaction_continuation_tests.rs"]
mod continuation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompactionGenerationObservation, CompactionGenerationState};
    use musteragent_identity::{UtcTimestamp, VersionedKey};
    use musteragent_runtime_model::ModelRouteRef;
    use std::sync::Mutex;
    struct DurableFixture {
        state: Mutex<
            Vec<(
                ContextCompactionRequest,
                Result<ContextCompactionResult, ContextPlanError>,
            )>,
        >,
        fail_once: Mutex<bool>,
        unknown_once: Mutex<bool>,
        capacity: usize,
    }
    impl ContextCompactor for DurableFixture {
        fn prepare_invocation(
            &self,
            request: ContextCompactionRequest,
        ) -> ApplicationFuture<'_, Result<ContextCompactionRequest, ContextPlanError>> {
            Box::pin(async move {
                let saved = self.state.lock().expect("valid durable compaction fixture");
                let observations = saved
                    .iter()
                    .rev()
                    .map(|(r, result)| CompactionGenerationObservation {
                        generation: r.slot.generation,
                        generation_high_watermark: saved
                            .iter()
                            .map(|(r, _)| r.slot.generation)
                            .max()
                            .unwrap(),
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
                            Err(error) if error.is_indeterminate() => {
                                CompactionGenerationState::DispatchedUnknown
                            }
                            Err(error) => CompactionGenerationState::Failed(error.clone()),
                        },
                    })
                    .collect::<Vec<_>>();
                let mut observed = observations.first().cloned();
                for original in &observations {
                    if request.matches_sealed_plan(original)? {
                        observed = Some(original.clone());
                        break;
                    }
                }
                request.select_recovery_generation(observed, UtcTimestamp::from_unix_millis(1000))
            })
        }
        fn compact(
            &self,
            request: ContextCompactionRequest,
        ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
            Box::pin(async move {
                let mut saved = self.state.lock().expect("valid durable compaction fixture");
                if let Some((prior, result)) = saved
                    .iter()
                    .find(|(r, _)| r.invocation.invocation_id == request.invocation.invocation_id)
                {
                    assert_eq!(
                        prior.logical_plan_fingerprint()?,
                        request.logical_plan_fingerprint()?
                    );
                    return result.clone();
                }
                let value: serde_json::Value = serde_json::from_slice(&request.serialized_history)
                    .expect("valid durable compaction fixture");
                let mut fail = self
                    .fail_once
                    .lock()
                    .expect("valid durable compaction fixture");
                if *fail && value["offset"].as_u64().is_some_and(|v| v > 0) {
                    *fail = false;
                    return Err(ContextPlanError::timeout()); // Interrupted before dispatch/seal.
                }
                let mut unknown = self.unknown_once.lock().unwrap();
                if *unknown && value["offset"].as_u64().is_some_and(|v| v > 0) {
                    *unknown = false;
                    let result = Err(ContextPlanError::indeterminate());
                    saved.push((request, result.clone()));
                    return result;
                }
                let result = if request.serialized_history.len() > self.capacity {
                    Err(capacity())
                } else {
                    Ok(ContextCompactionResult {
                        invocation_id: request.invocation.invocation_id,
                        model_route_ref: ModelRouteRef::from_key(
                            request.model.route.route_ref.clone(),
                        ),
                        summary: ModelText::new(
                            "Checkpoint: preserve the supplied user constraints and pending work.",
                        )
                        .expect("valid durable compaction fixture"),
                    })
                };
                saved.push((request, result.clone()));
                result
            })
        }
    }
    fn request() -> ContextCompactionRequest {
        let mut r = crate::model_compaction::tests::request();
        r.recovery_policy =
            musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default();
        r.slot.compaction_policy_ref =
            VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V7)
                .expect("valid durable compaction fixture");
        r.serialized_history = serde_json::to_vec(
            &serde_json::json!({"entries":["文件🦀 constraints ".repeat(2048)]}),
        )
        .expect("valid durable compaction fixture");
        r.previous_summary = Some(
            ModelText::new("Prior constraint: never overwrite existing files.")
                .expect("valid durable compaction fixture"),
        );
        r.slot.plan_fingerprint = r
            .expected_plan_fingerprint()
            .expect("valid durable compaction fixture");
        r
    }
    #[tokio::test]
    async fn capacity_chunks_resume_without_repaying_completed_nodes() {
        let fixture = DurableFixture {
            state: Mutex::new(Vec::new()),
            fail_once: Mutex::new(true),
            unknown_once: Mutex::new(false),
            capacity: 4096,
        };
        let r = request();
        assert!(
            compact(&fixture, r.clone())
                .await
                .expect_err("expected fixture rejection")
                .retryable
        );
        let before = fixture
            .state
            .lock()
            .expect("valid durable compaction fixture")
            .len();
        assert!(before > 1);
        let mut resumed = r.clone();
        resumed.slot.input_tokens_before += 29;
        resumed.slot.ledger_cutoff =
            musteragent_identity::ConversationSeq::new(resumed.slot.ledger_cutoff.get() + 1)
                .unwrap();
        resumed.slot.plan_fingerprint = resumed.expected_plan_fingerprint().unwrap();
        let result = compact(&fixture, resumed.clone())
            .await
            .expect("valid durable compaction fixture");
        assert!(result.summary.as_str().contains("constraints"));
        let paid = fixture
            .state
            .lock()
            .expect("valid durable compaction fixture")
            .len();
        assert!(paid > before);
        assert_eq!(
            compact(&fixture, resumed)
                .await
                .expect("valid durable compaction fixture"),
            result
        );
        let saved = fixture
            .state
            .lock()
            .expect("valid durable compaction fixture");
        assert_eq!(saved.len(), paid);
        let ids = saved
            .iter()
            .map(|(r, _)| r.invocation.invocation_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), paid);
        for (r, _) in saved.iter() {
            r.validate().expect("valid durable compaction fixture");
        }
    }
    #[tokio::test]
    async fn changed_estimate_replays_paid_prefix_but_cannot_pass_unknown_chunk() {
        use musteragent_runtime_contract::execution_liveness::{
            ActivityExecutionTiming, InvocationExecutionTiming, LivenessPolicyRef,
        };
        let fixture = DurableFixture {
            state: Mutex::new(Vec::new()),
            fail_once: Mutex::new(false),
            unknown_once: Mutex::new(true),
            capacity: 4096,
        };
        let mut r = request();
        r.invocation.timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: VersionedKey::new("test.execution@1").unwrap(),
                digest: Sha256::digest(b"sealed activity policy"),
            },
            business_deadline: None,
        });
        assert!(
            compact(&fixture, r.clone())
                .await
                .unwrap_err()
                .is_indeterminate()
        );
        let original = fixture.state.lock().unwrap().clone();
        assert!(original.iter().any(|(_, result)| result.is_ok()));
        r.slot.input_tokens_before += 73;
        r.slot.plan_fingerprint = r.expected_plan_fingerprint().unwrap();
        assert!(compact(&fixture, r).await.unwrap_err().is_indeterminate());
        assert_eq!(
            *fixture.state.lock().unwrap(),
            original,
            "paid ancestors are reusable, but the original unknown node blocks all new dispatch"
        );
    }
    #[tokio::test]
    async fn fixed_overhead_capacity_stops_with_precise_reason() {
        let fixture = DurableFixture {
            state: Mutex::new(Vec::new()),
            fail_once: Mutex::new(false),
            unknown_once: Mutex::new(false),
            capacity: 1,
        };
        let error = compact(&fixture, request())
            .await
            .expect_err("expected fixture rejection");
        assert_eq!(error.code, ErrorCode::ModelContextExceeded);
        assert!(!error.retryable);
        assert!(
            fixture
                .state
                .lock()
                .expect("valid durable compaction fixture")
                .len()
                < 20
        );
    }
}
