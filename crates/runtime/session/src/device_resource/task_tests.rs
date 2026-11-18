//! New Task transports retain the existing canonical original Tool identity.
use super::*;

const TASKS: &[(&str, bool)] = &[
    ("tasks.start@3", false),
    ("tasks.start@4", false),
    ("tasks.list@1", true),
    ("tasks.inspect@1", true),
    ("tasks.control@1", false),
    ("tasks.amend@2", false),
    ("tasks.verify@2", false),
    ("tasks.handoff@1", false),
    ("tasks.followup@1", false),
];

fn task_command(key: &str) -> DeviceResourceCommand {
    let original = command();
    let mut invocation = original.invocation().clone();
    invocation.tool_key =
        ToolKey::from_versioned_key(VersionedKey::new(key).expect("valid test fixture"));
    invocation.capability_key =
        CapabilityKey::from_versioned_key(VersionedKey::new(key).expect("valid test fixture"));
    DeviceResourceCommand::new(
        original.admission().clone(),
        invocation,
        original.request_bytes().to_vec(),
    )
    .expect("valid test fixture")
}

#[test]
fn task_commands_and_receipts_keep_exact_identity_across_current_attempts() {
    for (key, read) in TASKS {
        let command = task_command(key);
        assert!(command.operation().is_task());
        assert_eq!(command.invocation().tool_key.as_str(), *key);
        let discriminator = if *key == "tasks.start@4" {
            "tasks.start@3"
        } else {
            key
        };
        assert_eq!(command.operation().tool_key(), discriminator);
        assert_eq!(command.operation().is_read_only(), *read);
        assert!(!command.operation().can_produce_versions());
        assert!(!command.operation().has_memory_approval());
        let request = DeviceResourceRequest::new(command.clone(), attempt(&command))
            .expect("valid test fixture");
        assert_eq!(
            DeviceResourceRequest::decode(&request.encode().expect("valid test fixture"))
                .expect("valid test fixture"),
            request
        );
        let mut retained = receipt(&command);
        let DeviceResourceOutcome::Succeeded(result) = &mut retained.outcome else {
            unreachable!()
        };
        result.produced_versions.clear();
        assert_eq!(
            DeviceResourceReceipt::decode(
                &retained.encode(&command).expect("valid test fixture"),
                &command
            )
            .expect("valid test fixture"),
            retained
        );
        let next = DeviceResourceRequest::new(command.clone(), attempt(&command))
            .expect("valid test fixture");
        assert_ne!(
            request.proof_digest().expect("valid test fixture"),
            next.proof_digest().expect("valid test fixture")
        );
        assert_eq!(request.command().commitment(), next.command().commitment());
        assert!(retained.validate_for(next.command()).is_ok());
    }
}

#[test]
fn task_receipt_cannot_follow_changed_input_target_or_home_proof() {
    let original = task_command("tasks.start@3");
    let mut retained = receipt(&original);
    let DeviceResourceOutcome::Succeeded(value) = &mut retained.outcome else {
        unreachable!()
    };
    value.produced_versions.clear();
    for kind in 0..5 {
        let mut binding = original.admission().clone();
        let mut invocation = original.invocation().clone();
        let mut bytes = original.request_bytes().to_vec();
        match kind {
            0 => {
                bytes = br#"{"objective":"another task"}"#.to_vec();
                invocation.request_ref = reference(&bytes);
            }
            1 => binding.intent_id = UserIntentId::generate(),
            2 => binding.source_digest = Sha256::digest(b"different Native source"),
            3 => binding.grant_proof_hash = Sha256::digest(b"different Native grant"),
            _ => {
                invocation.tool_key = ToolKey::from_versioned_key(
                    VersionedKey::new("tasks.followup@1").expect("valid test fixture"),
                );
                invocation.capability_key = CapabilityKey::from_versioned_key(
                    VersionedKey::new("tasks.followup@1").expect("valid test fixture"),
                );
            }
        }
        let changed =
            DeviceResourceCommand::new(binding, invocation, bytes).expect("valid test fixture");
        assert_ne!(changed.commitment(), original.commitment());
        assert!(retained.validate_for(&changed).is_err());
    }
    assert!(
        receipt(&original).validate_for(&original).is_err(),
        "a Task acceptance cannot claim to have created canonical file versions"
    );
}

#[test]
fn task_transport_does_not_invent_support_for_other_contracts() {
    for key in [
        "tasks.start@1",
        "tasks.start@2",
        "tasks.start@5",
        "tasks.verify@1",
        "tasks.followup@2",
        "tasks.amend@1",
        "sample_queue.my_tasks.list@1",
    ] {
        assert!(
            DeviceResourceOperation::from_tool_key(key).is_err(),
            "{key}"
        );
    }
    for key in [
        "goal.create@3",
        "goal.draft.save@1",
        "tasks.executions.list@2",
    ] {
        assert!(
            !DeviceResourceOperation::from_tool_key(key)
                .expect("valid test fixture")
                .is_task()
        );
    }
}
