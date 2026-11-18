use std::collections::BTreeSet;

use super::*;
use musteragent_identity::VersionedKey;
use musteragent_tool::ToolKey;

#[test]
fn mandatory_or_denied_boundaries_never_issue_session_authorization() {
    assert!(session_authorization_is_reusable(
        Decision::Allow,
        ApprovalRequirement::ByPolicy,
        InvocationPolicyDisposition::RequirePolicyApproval("ordinary boundary"),
    ));
    assert!(!session_authorization_is_reusable(
        Decision::RequireApproval,
        ApprovalRequirement::ByPolicy,
        InvocationPolicyDisposition::Inherit,
    ));
    assert!(!session_authorization_is_reusable(
        Decision::Allow,
        ApprovalRequirement::Always,
        InvocationPolicyDisposition::Inherit,
    ));
    assert!(!session_authorization_is_reusable(
        Decision::Allow,
        ApprovalRequirement::ByPolicy,
        InvocationPolicyDisposition::RequireApproval("mandatory argument boundary"),
    ));
    assert!(!session_authorization_is_reusable(
        Decision::Allow,
        ApprovalRequirement::ByPolicy,
        InvocationPolicyDisposition::Deny("policy denial"),
    ));
}

#[test]
fn missing_workspace_has_a_stable_capability_unavailable_reason() {
    let required = BTreeSet::from([RequiredContext::Workspace]);
    assert_eq!(
        unavailable_context_reason(&required, &BTreeSet::new()),
        Some("workspace_context_required: Tool requires an active authorized workspace")
    );
    assert_eq!(unavailable_context_reason(&required, &required), None);
}

#[test]
fn other_missing_contexts_do_not_claim_a_workspace_failure() {
    let required = BTreeSet::from([RequiredContext::ConversationAttachments]);
    assert_eq!(
        unavailable_context_reason(&required, &BTreeSet::new()),
        Some("required_context_unavailable: Tool requires unavailable conversation context")
    );

    let required = BTreeSet::from([
        RequiredContext::Workspace,
        RequiredContext::ConversationAttachments,
    ]);
    let available = BTreeSet::from([RequiredContext::Workspace]);
    assert_eq!(
        unavailable_context_reason(&required, &available),
        Some("required_context_unavailable: Tool requires unavailable conversation context")
    );
}

#[test]
fn tool_observation_text_escapes_controls_without_reclassifying_content_integrity() {
    let content = text_content("{\"location\":\"Heartâ\u{80}\u{99}s Content,\"}".to_owned())
        .expect("valid escaped tool observation");

    let [CanonicalContentPart::Text(text)] = content.as_slice() else {
        panic!("tool observation must remain one text part");
    };
    assert_eq!(
        text.as_str(),
        "{\"location\":\"Heartâ\\u0080\\u0099s Content,\"}"
    );
}

#[test]
fn invalid_generated_tool_text_is_a_runtime_invariant_not_content_corruption() {
    assert_eq!(
        text_content(String::new()),
        Err(port(ErrorCode::InternalInvariantViolated, false))
    );
}

#[test]
fn content_write_errors_retry_only_a_genuinely_unavailable_dependency() {
    assert_eq!(
        port_from_content_write(ContentStoreError::Unavailable),
        port(ErrorCode::DependencyUnavailable, true)
    );
    for error in [
        ContentStoreError::NotFound,
        ContentStoreError::Unauthorized,
        ContentStoreError::Invariant,
    ] {
        assert_eq!(
            port_from_content_write(error),
            port(ErrorCode::InternalInvariantViolated, false)
        );
    }
    assert_eq!(
        port_from_content_write(ContentStoreError::TooLarge),
        port(ErrorCode::InternalInvariantViolated, false)
    );
    for error in [
        ContentStoreError::LengthMismatch,
        ContentStoreError::HashMismatch,
    ] {
        assert_eq!(
            port_from_content_write(error),
            port(ErrorCode::ContentIntegrityFailed, false)
        );
    }
    assert_eq!(
        port_from_content_write(ContentStoreError::IdentityConflict),
        port(ErrorCode::IdempotencyConflict, false)
    );
}

#[test]
fn atomic_commit_adopts_success_with_authorized_content_without_reconstruction() {
    let result = committed_result(b"canonical media Tool result");
    let asset = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("image media type"),
        byte_length: 3,
        sha256: Sha256::digest(b"png"),
    };
    let outcome = ToolTerminalOutcome::SucceededWithAuthorizedContent {
        result: result.clone(),
        authorized_content: vec![asset],
    };

    assert_eq!(validate_committed_success(&outcome, &result), Ok(()));
}

#[test]
fn ordinary_and_reconciled_success_use_the_durable_empty_authority_identity() {
    let result = committed_result(b"presentation describe contract");
    assert_eq!(
        ToolTerminalOutcome::succeeded(result.clone(), vec![]),
        ToolTerminalOutcome::Succeeded(result.clone()),
    );
    let asset = result.result_ref.clone();
    assert_eq!(
        ToolTerminalOutcome::succeeded(result.clone(), vec![asset.clone()]),
        ToolTerminalOutcome::SucceededWithAuthorizedContent {
            result,
            authorized_content: vec![asset],
        },
    );
}

#[test]
fn atomic_commit_rejects_a_dispatch_result_that_differs_from_the_journal() {
    let recorded = committed_result(b"recorded");
    let returned = committed_result(b"different");
    let outcome = ToolTerminalOutcome::Succeeded(recorded);

    assert_eq!(
        validate_committed_success(&outcome, &returned),
        Err(port(ErrorCode::ContentIntegrityFailed, false))
    );
}

#[test]
fn durable_success_keeps_materialization_failure_observable() {
    assert_eq!(
        tool_telemetry_observation(ToolObservationReceipt::Durable {
            receipt: DurableToolReceipt::Succeeded,
            materialization_error: Some(ErrorCode::InternalInvariantViolated),
        }),
        ToolTelemetryObservation {
            outcome: TelemetryOutcome::Success,
            error: None,
            materialization_error: Some(ErrorCode::InternalInvariantViolated),
        }
    );
}

fn committed_result(bytes: &[u8]) -> CapabilityResult {
    let result_hash = Sha256::digest(bytes);
    CapabilityResult {
        result_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("JSON media type"),
            byte_length: u64::try_from(bytes.len()).expect("bounded test content"),
            sha256: result_hash,
        },
        result_schema: musteragent_content_ref::SchemaRef {
            schema_key: musteragent_identity::VersionedKey::new("test.result@1")
                .expect("schema key"),
            schema_hash: Sha256::digest(b"test schema"),
        },
        result_hash,
        provider_request_ref: None,
    }
}

#[test]
fn question_preflight_uses_the_same_exact_contract_as_execution() {
    for (key, value, valid) in [
        (
            "interaction.question@1",
            serde_json::json!({"kind":"text_input","question":"Missing context?"}),
            true,
        ),
        (
            "interaction.question@1",
            serde_json::json!({"kind":"confirm","question":"Ready?"}),
            false,
        ),
        (
            "interaction.question@1",
            serde_json::json!({"kind":"single_select","question":"?","choices":["x","x"]}),
            false,
        ),
        (
            "interaction.question@2",
            serde_json::json!({"kind":"text_input","question":"When does the event start?"}),
            true,
        ),
        (
            "interaction.question@2",
            serde_json::json!({"kind":"single_select","question":"Which topic?","choices":[{"label":"Partners","description":"Compare persistent partners with temporary workers."},{"label":"Reliability"}]}),
            true,
        ),
        (
            "interaction.question@2",
            serde_json::json!({"kind":"single_select","question":"Which topic?","choices":["Partners","Reliability"]}),
            false,
        ),
        (
            "interaction.question@2",
            serde_json::json!({"kind":"confirm","question":"Ready?"}),
            false,
        ),
        (
            "interaction.question@3",
            serde_json::json!({"kind":"text_input","question":"Future contract"}),
            false,
        ),
        (
            "interaction.request@99",
            serde_json::json!({"kind":"text_input","title":"legacy"}),
            false,
        ),
        (
            "interaction.request@1",
            serde_json::json!({"kind":"confirm","title":"Ready?"}),
            true,
        ),
        (
            "interaction.request@2",
            serde_json::json!({"kind":"confirm","question":"Ready?"}),
            true,
        ),
    ] {
        let key = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
        assert_eq!(
            interaction_arguments_valid(&key, &value),
            valid,
            "{}",
            key.as_str()
        );
        assert_eq!(
            interaction_arguments_valid(&key, &value),
            StructuredInteractionRequest::parse_tool_arguments(
                key.as_str(),
                &serde_json::to_vec(&value).unwrap()
            )
            .is_ok()
        );
    }
}
