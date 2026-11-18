use super::*;
use musteragent_context::{ContextCodec, ModelIteration};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, InvocationExecutionTiming, LivenessPolicyRef,
};

#[test]
fn dialogue_model_begin_requires_original_activity_mode_and_exact_request_content() {
    let mut request = CanonicalJsonContextCodec
        .decode_model_request(&serde_json::to_vec(&fixture()).unwrap(), bound())
        .unwrap();
    let legacy_timing = request.invocation.timing.clone();
    request.invocation.timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: musteragent_identity::VersionedKey::new("musteragent.model-liveness@1").unwrap(),
            digest: Sha256::digest(b"original sealed activity policy"),
        },
        business_deadline: None,
    });
    let bytes = CanonicalJsonContextCodec
        .encode_model_request(&request)
        .unwrap();
    let mut iteration = ModelIteration {
        delivery_repair: None,
        iteration: 1,
        invocation_id: request.invocation.invocation_id,
        assistant_message_id: ID.parse().unwrap(),
        assistant_output_id: ID.parse().unwrap(),
        reasoning_output_id: SECOND.parse().unwrap(),
        request_ref: ContentRef {
            content_id: ID.parse().unwrap(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        },
        request,
    };
    let supported = [
        musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION,
        musteragent_runtime_memory::MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
    ];
    let mut changed_bytes = bytes.clone();
    changed_bytes.push(b' ');
    for schema in supported {
        assert!(crate::validate_model_begin_timing(&iteration, schema, &bytes).is_ok());
        assert!(crate::validate_model_begin_timing(&iteration, schema, &changed_bytes).is_err());
    }
    for schema in [
        "musteragent.runtime.snapshot.v13",
        "musteragent.runtime.snapshot.v18",
    ] {
        assert!(crate::validate_model_begin_timing(&iteration, schema, &bytes).is_err());
    }
    iteration.request.invocation.timing = legacy_timing;
    for schema in supported {
        assert!(crate::validate_model_begin_timing(&iteration, schema, &bytes).is_err());
    }
    // Retired snapshot owners cannot admit new model intents in either timing mode.
    assert!(
        crate::validate_model_begin_timing(&iteration, "musteragent.runtime.snapshot.v13", &bytes)
            .is_err()
    );
}
