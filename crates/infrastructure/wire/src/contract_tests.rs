use super::*;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::RunEvent;
use musteragent_sandbox::*;
use musteragent_scheduler::SchedulerEventEncoder;
use musteragent_tool::ToolEvent;
use prost::Message;

#[test]
fn rpc_and_storage_share_type_identity_and_package() {
    fn shared(value: wire::RunStarted) -> musteragent_persistence_codec::wire::RunStarted {
        value
    }
    assert_eq!(shared(wire::RunStarted::default()), Default::default());
    let descriptors =
        prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).expect("descriptors");
    let mut names = std::collections::BTreeSet::new();
    let mut services = std::collections::BTreeSet::new();
    for file in descriptors.file {
        if file
            .package
            .as_deref()
            .is_some_and(|p| p.starts_with("google."))
        {
            continue;
        }
        assert_eq!(file.package.as_deref(), Some("musteragent.v1"));
        assert!(!file.name.as_deref().expect("file").contains("hosted_"));
        let rpc_file = file
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with("rpc_"));
        for message in file.message_type {
            if rpc_file {
                let name = message.name.as_deref().expect("message name");
                assert!(
                    ![
                        "Hosted",
                        "Presentation",
                        "ProductNote",
                        "ListCoordinationProjects",
                        "ListProjectTasks",
                        "UpdateNoteArtifact"
                    ]
                    .iter()
                    .any(|part| name.contains(part)),
                    "excluded product message: {name}"
                );
            }
            assert!(
                names.insert(message.name.expect("message")),
                "duplicate message owner"
            );
        }
        for service in file.service {
            let name = service.name.expect("service");
            assert!(
                ![
                    "Hosted",
                    "Billing",
                    "Presentation",
                    "Channel",
                    "Collaboration"
                ]
                .iter()
                .any(|v| name.contains(v))
            );
            for method in service.method {
                let name = method.name.expect("method");
                assert!(
                    ![
                        "Hosted",
                        "Presentation",
                        "ListCoordinationProjects",
                        "ListProjectTasks",
                        "UpdateNoteArtifact"
                    ]
                    .iter()
                    .any(|part| name.contains(part)),
                    "excluded product RPC: {name}"
                );
            }
            services.insert(name);
        }
    }
    for name in [
        "RuntimeService",
        "RuntimeContentService",
        "StandingGoalService",
        "EvolutionManagementService",
        "WorkItemExecutionControlService",
        "DelegationAdmissionService",
        "MeteringProjectionService",
    ] {
        assert!(services.contains(name), "missing service: {name}");
    }
    for name in [
        "ExecutionJournalCommandV1",
        "AgentJournalCommand",
        "BrowserJobRequest",
        "ContextPlanSelectionV1",
    ] {
        assert!(names.contains(name), "missing contract: {name}");
    }
}

#[test]
fn scheduler_uses_the_exact_canonical_ledger_bytes() {
    // Original field 18, length-delimited empty RunCompleted. Namespace changes
    // do not change protobuf tags or empty-message encoding.
    let event = DomainEvent::Run(RunEvent::Completed {
        assistant_message_id: None,
        result_refs: Vec::new(),
    });
    let expected = [0x92, 0x01, 0x00];
    let bytes = ProtobufSchedulerEventEncoder
        .encode_domain_event(event.clone())
        .expect("scheduler encode");
    assert_eq!(bytes, expected);
    assert_eq!(
        ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(event.clone())),
        expected
    );
    assert_eq!(
        ProtobufLedgerPayloadCodec
            .decode(LedgerRecordKind::DomainEvent, &bytes)
            .expect("decode"),
        LedgerPayload::DomainEvent(event)
    );
}

#[test]
fn sandbox_decisions_round_trip_and_reject_inconsistent_routes() {
    for (executor, reason, compatibility) in [
        (
            SandboxExecutor::Browser,
            SandboxRouteReason::BrowserClientOnline,
            SandboxCompatibility::BrowserCompatible,
        ),
        (
            SandboxExecutor::Server,
            SandboxRouteReason::ApiSurface,
            SandboxCompatibility::BrowserCompatible,
        ),
        (
            SandboxExecutor::Server,
            SandboxRouteReason::CompatibilityRequiresServer,
            SandboxCompatibility::ServerRequired,
        ),
    ] {
        let event = LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::SandboxRouted {
            invocation_id: InvocationId::generate(),
            route: SandboxRouteRecord {
                decision: SandboxRouteDecision {
                    executor,
                    reason,
                    compatibility,
                },
                decided_at: UtcTimestamp::from_unix_millis(1234),
            },
        }));
        let bytes = ProtobufLedgerPayloadCodec.encode(event.clone());
        assert_eq!(
            ProtobufLedgerPayloadCodec
                .decode(LedgerRecordKind::DomainEvent, &bytes)
                .expect("decode"),
            event
        );
        assert_eq!(
            ledger_content_references(LedgerRecordKind::DomainEvent, &bytes)
                .expect("retention coverage"),
            Vec::new()
        );
        let mut encoded = wire::DomainEvent::decode(bytes.as_slice()).expect("wire");
        let Some(wire::domain_event::Event::ToolSandboxRouted(ref mut routed)) = encoded.event
        else {
            panic!("route event")
        };
        routed.route.as_mut().expect("route").executor = "unknown".into();
        assert!(
            ProtobufLedgerPayloadCodec
                .decode(LedgerRecordKind::DomainEvent, &encoded.encode_to_vec())
                .is_err()
        );
        let Some(wire::domain_event::Event::ToolSandboxRouted(ref mut routed)) = encoded.event
        else {
            panic!("route event")
        };
        routed.route.as_mut().expect("route").executor = "browser".into();
        routed.route.as_mut().expect("route").compatibility = "server-required".into();
        assert!(
            ProtobufLedgerPayloadCodec
                .decode(LedgerRecordKind::DomainEvent, &encoded.encode_to_vec())
                .is_err()
        );
    }
}
