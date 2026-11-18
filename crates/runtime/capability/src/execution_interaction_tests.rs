use super::*;
use musteragent_content_ref::MediaType;
use musteragent_identity::{AttemptId, ContentId, IdempotencyKey, ProductId, RunId, ToolCallId};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_tool::{BuiltinToolCatalog, ProductExecutionTarget, ToolInvocationStatus, ToolKey};

fn waiting(resolution: InteractionResolution) -> DurableToolInvocation {
    let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
    let catalog =
        BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud).unwrap();
    let definition = catalog
        .definitions()
        .iter()
        .find(|tool| tool.key.as_str() == "interaction.request@2")
        .unwrap();
    let manifest = definition.pin(CapabilityProviderId::generate());
    DurableToolInvocation {
        sandbox_route: None,
        request_record_id: LedgerRecordId::generate(),
        invocation: ToolInvocation {
            invocation_id: InvocationId::generate(),
            run_id: RunId::generate(),
            requested_by_attempt_id: AttemptId::generate(),
            execution_attempt_id: None,
            tool_call_id: ToolCallId::new("question").unwrap(),
            tool_key: manifest.key,
            capability_provider_id: manifest.capability_provider_id,
            status: ToolInvocationStatus::WaitingInteraction,
            effect: manifest.effect,
            approval: manifest.approval,
            idempotency_key: IdempotencyKey::new("question").unwrap(),
            request_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/json").unwrap(),
                byte_length: 2,
                sha256: Sha256::digest(b"{}"),
            },
            result_ref: None,
            interaction_id: Some(InteractionId::generate()),
            timing: InvocationExecutionTiming::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(1000),
            },
        },
        retry_contract: manifest.retry_contract,
        prebound_reconciliation_ref: None,
        recorded_outcome: None,
        interaction_resolution: Some(resolution),
    }
}

#[test]
fn question_negative_resolutions_are_valid_through_start_and_success() {
    for resolution in [
        InteractionResolution::Cancelled,
        InteractionResolution::Expired,
        InteractionResolution::Approval(ApprovalDecision::Deny),
    ] {
        let mut question = waiting(resolution);
        question.validate().unwrap();
        assert!(question.permits_initial_start());
        question.invocation.status = ToolInvocationStatus::Running;
        question.validate().unwrap();
        question.invocation.status = ToolInvocationStatus::Succeeded;
        question.recorded_outcome = Some(crate::ToolTerminalOutcome::Succeeded(
            crate::CapabilityResult {
                result_ref: question.invocation.request_ref.clone(),
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("interaction.result@1").unwrap(),
                    schema_hash: Sha256::digest(b"schema"),
                },
                result_hash: question.invocation.request_ref.sha256,
                provider_request_ref: None,
            },
        ));
        question.validate().unwrap();
    }
}

#[test]
fn cancelling_a_question_cannot_authorize_another_tool_or_a_denied_approval() {
    for resolution in [
        InteractionResolution::Cancelled,
        InteractionResolution::Expired,
        InteractionResolution::Approval(ApprovalDecision::Deny),
        InteractionResolution::Pending,
    ] {
        let mut effect = waiting(resolution);
        effect.invocation.tool_key =
            ToolKey::from_versioned_key(VersionedKey::new("files.write@1").unwrap());
        effect.validate().unwrap();
        assert!(!effect.permits_initial_start());
        effect.invocation.status = ToolInvocationStatus::Running;
        assert!(effect.validate().is_err());
    }
    let mut denied_question = waiting(InteractionResolution::Approval(ApprovalDecision::Deny));
    denied_question.invocation.approval = ApprovalRequirement::Always;
    assert!(!denied_question.permits_initial_start());
}

#[test]
fn a_question_key_cannot_hide_an_unapproved_effect_or_replay_contract() {
    for resolution in [
        InteractionResolution::Cancelled,
        InteractionResolution::Expired,
    ] {
        let mut read = waiting(resolution.clone());
        read.invocation.effect = Effect::Read;
        assert!(!read.permits_initial_start());
        let mut replay = waiting(resolution);
        replay.retry_contract = ToolRetryContract::ReadOnly;
        assert!(!replay.permits_initial_start());
    }
}

#[test]
fn a_successor_can_recover_a_cancelled_question_without_granting_external_replay() {
    let mut question = waiting(InteractionResolution::Cancelled);
    let original = AttemptId::generate();
    let successor = AttemptId::generate();
    question.invocation.status = ToolInvocationStatus::Running;
    question.invocation.execution_attempt_id = Some(original);
    assert!(question.permits_start(ToolStartMode::ReplaySafeRecovery, successor));
    assert!(!question.permits_start(ToolStartMode::ReplaySafeRecovery, original));
    question.retry_contract = ToolRetryContract::NotReplayable;
    assert!(!question.permits_start(ToolStartMode::ReplaySafeRecovery, successor));
    question.retry_contract = ToolRetryContract::IdempotentByKey;
    question.invocation.tool_key =
        ToolKey::from_versioned_key(VersionedKey::new("files.write@1").unwrap());
    assert!(!question.permits_start(ToolStartMode::ReplaySafeRecovery, successor));
}
