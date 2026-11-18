use super::*;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, BusinessDeadline, InvocationExecutionTiming, LivenessPolicyRef,
};

#[test]
fn legacy_context_timing_preserves_golden_zero_and_nonzero_deadlines() {
    // Original field-4 proto3 wire encoding, including its historical zero
    // default. Neither vector acquires a new timing field or schema version.
    for (deadline, digest) in [
        (
            0,
            "b4b137596016533a4bbb4a18ce8af6bf54ca9c83cfbf04a1f2c584cfbc790a68",
        ),
        (
            2_000_000,
            "c51c427760c27af5fad328e392a79a5b87edf3fd87342fb14b1a70d4afa35ae5",
        ),
    ] {
        let mut input = input();
        input.invocation_timing = InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(deadline),
        };
        let command = AgentContextCommand::ReadMaterial(Box::new(input));
        let encoded = encode_agent_context_command(&command, maximum()).expect("legacy command");
        assert_eq!(Sha256::digest(&encoded).to_string(), digest);
        assert_eq!(
            decode_agent_context_command(&encoded, maximum()).expect("legacy decode"),
            command
        );
        let mut wire = wire::AgentContextCommand::decode(encoded.as_slice()).expect("wire");
        assert_eq!(wire.schema_version, 1);
        wire.schema_version = 2;
        assert!(
            decode_agent_context_command(&wire.encode_to_vec(), maximum()).is_err(),
            "a version cannot reinterpret a legacy zero as no deadline"
        );
    }
}

#[test]
fn activity_context_timing_uses_explicit_schema_and_rejects_mixed_modes() {
    let mut input = input();
    input.invocation_timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new("musteragent.model-liveness@1").expect("key"),
            digest: Sha256::digest(b"original sealed model policy"),
        },
        business_deadline: Some(BusinessDeadline {
            at: UtcTimestamp::from_unix_millis(5_000_000),
            source: VersionedKey::new("fixture.job-policy@1").expect("source"),
        }),
    });
    let command = AgentContextCommand::ReadMaterial(Box::new(input));
    let encoded = encode_agent_context_command(&command, maximum()).expect("activity command");
    assert_eq!(
        decode_agent_context_command(&encoded, maximum()).expect("activity decode"),
        command
    );
    let original = wire::AgentContextCommand::decode(encoded.as_slice()).expect("wire");
    assert_eq!(original.schema_version, 2);
    let mut old = original.clone();
    old.schema_version = 1;
    assert!(decode_agent_context_command(&old.encode_to_vec(), maximum()).is_err());
    let mut mixed = original.clone();
    let Some(wire::agent_context_command::Operation::ReadMaterial(ref mut read)) = mixed.operation
    else {
        panic!("read material")
    };
    read.invocation_deadline_ms = 1;
    assert!(decode_agent_context_command(&mixed.encode_to_vec(), maximum()).is_err());

    let Some(wire::agent_context_command::Operation::ReadMaterial(read)) = original.operation
    else {
        panic!("read material")
    };
    let mut bytes = read.encode_to_vec();
    bytes.extend_from_slice(&[32, 0]); // Explicit field 4 = 0 alongside activity.
    #[derive(prost::Message)]
    struct RawCommand {
        #[prost(bytes = "vec", tag = "3")]
        read_material: Vec<u8>,
        #[prost(uint32, tag = "15")]
        schema_version: u32,
    }
    let noncanonical = RawCommand {
        read_material: bytes,
        schema_version: 2,
    }
    .encode_to_vec();
    assert!(decode_agent_context_command(&noncanonical, maximum()).is_err());
}
