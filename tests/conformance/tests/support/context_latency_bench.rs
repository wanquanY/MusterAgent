//! Real-store, real-planner performance fixture. No Provider or paid model call is made. The synthetic 144-message history is identified
//! separately from browser end-to-end and exported production-trace evidence.
use musteragent_context::{ContextPlanInput, ContextPlanner};
use musteragent_model_providers::{NativeWireBody, ResolvedContents, map_request};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_snapshot::RuntimeSnapshotContent;
use std::{num::NonZeroU64, time::Instant};

pub const MESSAGES: usize = 144;
const SAMPLES: usize = 200;
const WARMUP: usize = 8;

pub fn history_text(index: usize) -> String {
    let mut text = format!("Message {index:03}: retain the entire original context.\n");
    while text.len() < 4096 {
        text.push_str("Source evidence, parameters and complete tool results remain available across turns.\n");
    }
    text.truncate(4096);
    text
}

pub fn sized_snapshot(mut snapshot: RuntimeSnapshotContent) -> RuntimeSnapshotContent {
    // Large-context fixture admission, not a change to production defaults.
    snapshot.model.route.context_window =
        NonZeroU64::new(1_048_576).expect("sized snapshot fixture");
    snapshot.context_policy.max_input_tokens =
        NonZeroU64::new(1_000_000).expect("sized snapshot fixture");
    snapshot.context_policy.compaction_trigger_tokens =
        NonZeroU64::new(900_000).expect("sized snapshot fixture");
    if let Some(budgets) = &mut snapshot.context_policy.resolved_v2 {
        budgets.hard_input_budget = snapshot.context_policy.max_input_tokens;
        budgets.trigger_budget = snapshot.context_policy.compaction_trigger_tokens;
    }
    snapshot
}

pub async fn measure(
    runtime: &str,
    planner: &dyn ContextPlanner,
    mut input: ContextPlanInput,
    content: &dyn ContentStore,
) {
    let mut plan_times = Vec::new();
    let mut map_times = Vec::new();
    let mut golden = None;
    for ordinal in 0..WARMUP + SAMPLES {
        input.next_iteration = (ordinal + 1) as u32;
        let started = Instant::now();
        let iteration = planner
            .prepare_model_iteration(input.clone())
            .await
            .expect("actual complete context plan");
        let planned = started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            iteration.request.input.len(),
            MESSAGES,
            "every message must survive"
        );
        let started = Instant::now();
        let mapped =
            map_request(&iteration.request, &ResolvedContents::default()).expect("measure fixture");
        let wire = NativeWireBody::serialize(&mapped.body).expect("measure fixture");
        let mapped_ms = started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            wire.as_bytes(),
            serde_json::to_vec(&mapped.body).expect("measure fixture"),
            "native serializer equivalence"
        );
        if let Some(expected) = &golden {
            assert_eq!(
                wire.as_bytes(),
                expected,
                "warm planning must preserve every native byte"
            );
        } else {
            golden = Some(wire.as_bytes().to_vec());
        }
        if ordinal >= WARMUP {
            plan_times.push(planned);
            map_times.push(mapped_ms);
        }
    }
    let native_bytes = golden.expect("measure fixture").len();
    assert!(native_bytes >= 580_000);
    let stats = content
        .material_cache()
        .expect("measure fixture")
        .stats()
        .expect("measure fixture");
    assert!(
        stats.material_hits >= (SAMPLES * MESSAGES) as u64,
        "measure real warm material reuse"
    );
    println!(
        "CONTEXT_BENCH {}",
        serde_json::json!({"runtime":runtime, "samples":SAMPLES,
        "native_bytes":native_bytes,"planner_ms":percentiles(plan_times),"mapping_ms":percentiles(map_times),
        "material_hits":stats.material_hits,"cache_retained_bytes":stats.retained_bytes})
    );
}

fn percentiles(mut values: Vec<f64>) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    serde_json::json!({"p50":values[values.len()/2-1],"p95":values[values.len()*95/100-1],"max":values[values.len()-1]})
}

pub struct NeverCompact;
impl musteragent_context::ContextCompactor for NeverCompact {
    fn compact(
        &self,
        _: musteragent_context::ContextCompactionRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<musteragent_context::ContextCompactionResult, musteragent_context::ContextPlanError>,
    > {
        Box::pin(async { panic!("large-window acceptance fixture must retain every message") })
    }
}
