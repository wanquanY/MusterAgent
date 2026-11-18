use super::*;
use musteragent_content_ref::MediaType;
use musteragent_identity::{AttemptId, ContentId, IdempotencyKey, RunId, ToolCallId};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_tool::{ToolInvocationStatus, ToolKey};

fn invocation(resolution: InteractionResolution) -> DurableToolInvocation {
    DurableToolInvocation {
        sandbox_route: None,
        request_record_id: LedgerRecordId::generate(),
        invocation: ToolInvocation {
            invocation_id: InvocationId::generate(),
            run_id: RunId::generate(),
            requested_by_attempt_id: AttemptId::generate(),
            execution_attempt_id: Some(AttemptId::generate()),
            tool_call_id: ToolCallId::new("question").expect("call"),
            tool_key: ToolKey::from_versioned_key(
                VersionedKey::new("interaction.request@2").expect("key"),
            ),
            capability_provider_id: CapabilityProviderId::generate(),
            status: ToolInvocationStatus::WaitingInteraction,
            effect: Effect::WriteReversible,
            approval: ApprovalRequirement::Never,
            idempotency_key: IdempotencyKey::new("question").expect("key"),
            request_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/json").expect("media"),
                byte_length: 2,
                sha256: Sha256::digest(b"{}"),
            },
            result_ref: None,
            interaction_id: Some(InteractionId::generate()),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(1000) },
        },
        retry_contract: ToolRetryContract::IdempotentByKey,
        prebound_reconciliation_ref: None,
        recorded_outcome: None,
        interaction_resolution: Some(resolution),
    }
}

#[test]
fn released_questions_preserve_negative_resolutions_through_running_and_success() {
    for key in [
        "interaction.request@1",
        "interaction.request@2",
        "interaction.question@1",
        "interaction.question@2",
    ] {
        for resolution in [
            InteractionResolution::Approval(ApprovalDecision::Deny),
            InteractionResolution::Cancelled,
            InteractionResolution::Expired,
        ] {
            let mut durable = invocation(resolution);
            durable.invocation.tool_key =
                ToolKey::from_versioned_key(VersionedKey::new(key).expect("key"));
            assert!(durable.permits_interaction_resume());
            assert!(durable.validate().is_ok());
            durable.invocation.status = ToolInvocationStatus::Running;
            assert!(durable.validate().is_ok());
            durable.invocation.status = ToolInvocationStatus::Succeeded;
            durable.invocation.result_ref = Some(durable.invocation.request_ref.clone());
            durable.recorded_outcome = Some(crate::ToolTerminalOutcome::Succeeded(
                crate::CapabilityResult {
                    result_ref: durable.invocation.request_ref.clone(),
                    result_hash: durable.invocation.request_ref.sha256,
                    result_schema: SchemaRef {
                        schema_key: VersionedKey::new("interaction.output@2").expect("key"),
                        schema_hash: Sha256::digest(b"schema"),
                    },
                    provider_request_ref: None,
                },
            ));
            assert!(durable.validate().is_ok());
        }
    }
}

#[test]
fn negative_resolution_never_authorizes_an_effect_or_unreleased_question_contract() {
    for resolution in [
        InteractionResolution::Approval(ApprovalDecision::Deny),
        InteractionResolution::Cancelled,
        InteractionResolution::Expired,
    ] {
        for (key, mutation) in [
            "interaction.request@2",
            "interaction.question@1",
            "interaction.question@2",
        ]
        .into_iter()
        .flat_map(|key| (0..7).map(move |mutation| (key, mutation)))
        {
            let mut durable = invocation(resolution.clone());
            durable.invocation.tool_key =
                ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
            match mutation {
                0 => {
                    durable.invocation.tool_key = ToolKey::from_versioned_key(
                        VersionedKey::new("device.execute@1").expect("key"),
                    )
                }
                1 => {
                    durable.invocation.tool_key = ToolKey::from_versioned_key(
                        VersionedKey::new("interaction.request@99").expect("key"),
                    )
                }
                6 => {
                    durable.invocation.tool_key = ToolKey::from_versioned_key(
                        VersionedKey::new("interaction.question@3").expect("key"),
                    )
                }
                2 => durable.invocation.effect = Effect::Read,
                3 => durable.invocation.approval = ApprovalRequirement::Always,
                4 => durable.retry_contract = ToolRetryContract::NotReplayable,
                _ => durable.invocation.interaction_id = None,
            }
            assert!(!durable.permits_interaction_resume());
            durable.invocation.status = ToolInvocationStatus::Running;
            assert!(durable.validate().is_err());
        }
    }
    let pending = invocation(InteractionResolution::Pending);
    assert!(!pending.permits_interaction_resume());
    assert!(pending.validate().is_ok());
    let mut approved = invocation(InteractionResolution::Approval(ApprovalDecision::Approve));
    approved.invocation.tool_key =
        ToolKey::from_versioned_key(VersionedKey::new("device.execute@1").expect("key"));
    approved.invocation.approval = ApprovalRequirement::Always;
    assert!(approved.permits_interaction_resume());
}
