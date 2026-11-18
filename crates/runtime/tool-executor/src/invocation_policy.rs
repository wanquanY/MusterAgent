use musteragent_content_ref::ContentRef;
use musteragent_content_ref::Sha256;
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_runtime_capability::InvocationPolicyBasis;
use musteragent_tool::ToolManifest;

const EXPERIENCE_PROPOSE_TOOL_KEY: &str = "experience.propose";
const INVOCATION_POLICY_REF: &str = "musteragent.invocation-policy.conditional-approval@9";
const LEGACY_INVOCATION_POLICY_REF: &str =
    "musteragent.legacy.invocation-policy.conditional-approval@1";
const INVOCATION_POLICY_DOCUMENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../contracts/invocation-policy-v9.json"
));
const LEGACY_INVOCATION_POLICY_DOCUMENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../contracts/invocation-policy-v1.json"
));

/// Immutable identity of the argument-level policy implementation used for
/// this Runtime release. It is deliberately separate from the Agent safety
/// policy pinned into `RuntimeSnapshot`.
#[must_use]
pub fn invocation_policy_ref() -> &'static str {
    INVOCATION_POLICY_REF
}

#[must_use]
pub fn invocation_policy_hash() -> Sha256 {
    Sha256::digest(INVOCATION_POLICY_DOCUMENT)
}

fn uses_kernel_policy(input: &InvocationPolicyInput<'_>) -> bool {
    input.policy_basis.is_some_and(|basis| {
        let policy = basis.agent_policy_ref.as_str();
        policy.starts_with("musteragent.") && !policy.starts_with("musteragent.legacy.")
    })
}

pub(crate) fn invocation_policy_identity_for(
    input: &InvocationPolicyInput<'_>,
) -> (&'static str, Sha256) {
    if input.tool.key.as_str() == crate::TERMINAL_HOST_ACCESS_TOOL {
        (
            crate::terminal_host_access::POLICY_REF,
            Sha256::digest(crate::terminal_host_access::POLICY_DOCUMENT),
        )
    } else if uses_kernel_policy(input) {
        (invocation_policy_ref(), invocation_policy_hash())
    } else {
        (
            LEGACY_INVOCATION_POLICY_REF,
            Sha256::digest(LEGACY_INVOCATION_POLICY_DOCUMENT),
        )
    }
}

/// Complete, immutable input available to an argument-aware invocation policy.
///
/// Capability grants and manifest floors remain owned by Runtime authorization;
/// this input only refines the `ByPolicy` branch for the exact validated call.
pub struct InvocationPolicyInput<'a> {
    /// Original execution scope, never a model-supplied path or workspace.
    pub workspace_id: Option<musteragent_identity::WorkspaceId>,
    pub tool: &'a ToolManifest,
    pub arguments: &'a serde_json::Value,
    pub policy_basis: Option<&'a InvocationPolicyBasis>,
    pub authorized_content: &'a [ContentRef],
}

/// Product policy result for the `ByPolicy` portion of one exact invocation.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InvocationPolicyDisposition {
    /// Preserve the permission-mode decision from the immutable Run Snapshot.
    Inherit,
    /// Suppress only a `ByPolicy` approval for this exact invocation.
    Allow,
    /// Require an ordinary permission-mode approval for this exact
    /// invocation. Request-approval asks the human, approve-for-me delegates
    /// to the isolated reviewer, full-access suppresses this ordinary boundary,
    /// and unattended deny mode fails closed.
    RequirePolicyApproval(&'static str),
    /// Require approval for this exact invocation, including in trusted mode.
    RequireApproval(&'static str),
    /// Reject the invocation before any physical effect.
    Deny(&'static str),
}

/// Fallible parameter-policy verdict. The durable executor records this before
/// returning a failed tool outcome; failures never become implicit allowance.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InvocationPolicyEvaluationError {
    /// Stable evaluator identity.
    pub evaluator: String,
    /// Stable reason code, including explicit panic classification.
    pub code: String,
    /// Safe explanation (no credentials or raw panic payload).
    pub reason: String,
}

pub trait InvocationPolicyEvaluator: Send + Sync {
    /// Fallible extension seam. Existing infallible policies retain their behavior.
    fn evaluate_checked(
        &self,
        input: &InvocationPolicyInput<'_>,
    ) -> Result<InvocationPolicyDisposition, InvocationPolicyEvaluationError> {
        Ok(self.evaluate(input))
    }

    fn evaluate(&self, input: &InvocationPolicyInput<'_>) -> InvocationPolicyDisposition;
}

/// Runtime-wide, versioned parameter policy.
///
/// The resolver uses exact Tool identities and validated typed parameters, not
/// localized keywords from user text. This first slice has two deliberately
/// separate responsibilities: it applies narrow, policy-owned safe harbors for
/// bounded consequences, and it preserves canonical current-task provenance
/// for the future typed outcome resolver. Provenance does not claim that a
/// user message semantically authorized an arbitrary Tool.
#[derive(Debug, Default)]
pub struct RuntimeInvocationPolicyEvaluator;

impl InvocationPolicyEvaluator for RuntimeInvocationPolicyEvaluator {
    fn evaluate(&self, input: &InvocationPolicyInput<'_>) -> InvocationPolicyDisposition {
        if input.tool.key.as_str() == "device.host.desktop.list@1" {
            return desktop_metadata_policy(input);
        }
        if crate::terminal_host_access_requested(input.tool.key.as_str(), input.arguments) {
            return InvocationPolicyDisposition::RequireApproval(
                crate::TERMINAL_HOST_ACCESS_REASON,
            );
        }
        match input.tool.key.logical_key() {
            EXPERIENCE_PROPOSE_TOOL_KEY => task_bounded_safe_harbor(
                input,
                "The experience proposal is not bound to canonical current-task provenance",
            ),
            _ => InvocationPolicyDisposition::Inherit,
        }
    }
}

fn task_bounded_safe_harbor(
    input: &InvocationPolicyInput<'_>,
    missing_reason: &'static str,
) -> InvocationPolicyDisposition {
    if input
        .policy_basis
        .is_some_and(|basis| basis.has_current_task())
    {
        InvocationPolicyDisposition::Allow
    } else {
        InvocationPolicyDisposition::RequirePolicyApproval(missing_reason)
    }
}

/// Stable identity of the user-visible consequence authorized or presented for
/// approval. Attempt identities and provider polling cursors are intentionally
/// absent; request, task provenance, content, provider and policy drift all
/// change the fingerprint.
#[must_use]
pub fn consequence_fingerprint(input: &InvocationPolicyInput<'_>) -> Sha256 {
    invocation_fingerprint(input, true)
}

/// Stable session-grant identity for the same validated operation.
///
/// Unlike the audit consequence fingerprint, this intentionally excludes the
/// current task record identities so an identical command in a later turn of
/// the same Conversation can reuse the user's session grant. Tool, arguments,
/// policy, permission mode, schemas, scopes, and authorized content remain in
/// the fingerprint, so materially different operations never collide.
#[must_use]
pub fn session_approval_fingerprint(input: &InvocationPolicyInput<'_>) -> Sha256 {
    invocation_fingerprint(input, false)
}

fn invocation_fingerprint(input: &InvocationPolicyInput<'_>, include_task_records: bool) -> Sha256 {
    let (policy_ref, policy_hash) = invocation_policy_identity_for(input);
    let mut bytes = if uses_kernel_policy(input) {
        b"musteragent.invocation-consequence.v2\0".to_vec()
    } else {
        b"musteragent.legacy.invocation-consequence.v1\0".to_vec()
    };
    if matches!(
        policy_ref,
        INVOCATION_POLICY_REF | crate::terminal_host_access::POLICY_REF
    ) {
        append(
            &mut bytes,
            input
                .workspace_id
                .map(|id| id.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
    }
    append(&mut bytes, input.tool.key.as_str().as_bytes());
    append(&mut bytes, input.tool.wire_name.as_str().as_bytes());
    append(&mut bytes, input.tool.capability_key.as_str().as_bytes());
    append(
        &mut bytes,
        input.tool.capability_provider_id.to_string().as_bytes(),
    );
    bytes.extend_from_slice(&input.tool.input_schema.schema_hash.into_bytes());
    bytes.extend_from_slice(&input.tool.output_schema.schema_hash.into_bytes());
    bytes.push(effect_code(input.tool.effect));
    bytes.push(approval_code(input.tool.approval));
    bytes.push(data_classification_code(input.tool.data_classification));
    bytes.push(egress_policy_code(input.tool.egress_policy));
    match &input.tool.execution_policy {
        musteragent_tool::ToolExecutionPolicy::LegacyFixed { timeout } => {
            append(&mut bytes, timeout.as_millis().to_string().as_bytes())
        }
        musteragent_tool::ToolExecutionPolicy::ActivityV1(policy) => {
            append(&mut bytes, &policy.canonical_bytes())
        }
    }
    append(
        &mut bytes,
        input.tool.max_attempts.get().to_string().as_bytes(),
    );
    for scope in &input.tool.required_scopes {
        append(&mut bytes, scope.as_str().as_bytes());
    }
    append(&mut bytes, &canonical_json_bytes(input.arguments));
    append(&mut bytes, policy_ref.as_bytes());
    append(&mut bytes, &policy_hash.into_bytes());
    if let Some(basis) = input.policy_basis {
        append(&mut bytes, basis.agent_policy_ref.as_str().as_bytes());
        append(&mut bytes, &basis.agent_policy_hash.into_bytes());
        bytes.push(basis.permission_mode.canonical_code());
        if include_task_records {
            for record in &basis.source_records {
                bytes.push(record.source_kind.canonical_code());
                append(&mut bytes, record.record_id.to_string().as_bytes());
                append(&mut bytes, &record.content_ref.sha256.into_bytes());
            }
        }
    } else {
        append(&mut bytes, b"no-task-provenance");
    }
    for content in input.authorized_content {
        append(&mut bytes, content.content_id.to_string().as_bytes());
        append(&mut bytes, &content.sha256.into_bytes());
    }
    Sha256::digest(bytes)
}

fn canonical_json_bytes(value: &serde_json::Value) -> Vec<u8> {
    let mut output = Vec::new();
    write_canonical_json(value, &mut output);
    output
}

fn write_canonical_json(value: &serde_json::Value, output: &mut Vec<u8>) {
    match value {
        serde_json::Value::Object(values) => {
            output.push(b'{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .expect("JSON object key serialization is infallible")
                        .as_bytes(),
                );
                output.push(b':');
                write_canonical_json(value, output);
            }
            output.push(b'}');
        }
        serde_json::Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output);
            }
            output.push(b']');
        }
        _ => output.extend_from_slice(
            serde_json::to_vec(value)
                .expect("validated JSON serialization is infallible")
                .as_slice(),
        ),
    }
}

fn append(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

const fn effect_code(value: Effect) -> u8 {
    match value {
        Effect::Read => 1,
        Effect::WriteReversible => 2,
        Effect::WriteExternal => 3,
        Effect::Destructive => 4,
    }
}

const fn approval_code(value: ApprovalRequirement) -> u8 {
    match value {
        ApprovalRequirement::Never => 1,
        ApprovalRequirement::ByPolicy => 2,
        ApprovalRequirement::Always => 3,
    }
}

const fn data_classification_code(value: musteragent_policy::DataClassification) -> u8 {
    match value {
        musteragent_policy::DataClassification::Public => 1,
        musteragent_policy::DataClassification::Internal => 2,
        musteragent_policy::DataClassification::Confidential => 3,
        musteragent_policy::DataClassification::Restricted => 4,
    }
}

const fn egress_policy_code(value: musteragent_policy::EgressPolicy) -> u8 {
    match value {
        musteragent_policy::EgressPolicy::None => 1,
        musteragent_policy::EgressPolicy::TenantOnly => 2,
        musteragent_policy::EgressPolicy::ProviderBound => 3,
        musteragent_policy::EgressPolicy::Internet => 4,
    }
}

/// Merges the Runtime-wide resolver with a home-specific parameter policy.
/// A stricter verdict always wins; an explicit bounded Allow can refine an
/// otherwise inherited `ByPolicy` decision.
pub fn combine_dispositions(
    runtime: InvocationPolicyDisposition,
    home: InvocationPolicyDisposition,
) -> InvocationPolicyDisposition {
    use InvocationPolicyDisposition::{
        Allow, Deny, Inherit, RequireApproval, RequirePolicyApproval,
    };
    match (runtime, home) {
        (Deny(reason), _) | (_, Deny(reason)) => Deny(reason),
        (RequireApproval(reason), _) | (_, RequireApproval(reason)) => RequireApproval(reason),
        (RequirePolicyApproval(reason), _) | (_, RequirePolicyApproval(reason)) => {
            RequirePolicyApproval(reason)
        }
        (Allow, _) | (_, Allow) => Allow,
        (Inherit, Inherit) => Inherit,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;
    use std::time::Duration;

    use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
    use musteragent_identity::{ContentId, LedgerRecordId, VersionedKey};
    use musteragent_policy::{
        ApprovalRequirement, DataClassification, Effect, EgressPolicy, PermissionMode,
    };
    use musteragent_runtime_capability::{
        InvocationPolicyBasis, TaskProvenanceRecord, TaskProvenanceSourceKind,
    };
    use musteragent_tool::{ToolKey, ToolRetryContract, WireName};

    use super::*;

    #[test]
    fn requested_terminal_host_access_is_a_mandatory_exact_invocation_boundary() {
        let mut tool = candidate_tool();
        tool.key = ToolKey::from_versioned_key(
            VersionedKey::new(crate::TERMINAL_HOST_ACCESS_TOOL).unwrap(),
        );
        let mut basis = task_basis();
        let requested = serde_json::json!({"command": "printf result > /outside/report.txt", "request_host_access": true});
        for mode in [
            PermissionMode::Trusted,
            PermissionMode::Supervised,
            PermissionMode::FullAccess,
        ] {
            basis.permission_mode = mode;
            let input = InvocationPolicyInput {
                workspace_id: None,
                tool: &tool,
                arguments: &requested,
                policy_basis: Some(&basis),
                authorized_content: &[],
            };
            assert_eq!(
                RuntimeInvocationPolicyEvaluator.evaluate(&input),
                InvocationPolicyDisposition::RequireApproval(crate::TERMINAL_HOST_ACCESS_REASON)
            );
            assert_eq!(
                invocation_policy_identity_for(&input).0,
                crate::terminal_host_access::POLICY_REF
            );
        }
        for arguments in [
            serde_json::json!({"command":"pwd"}),
            serde_json::json!({"command":"pwd","request_host_access":false}),
        ] {
            let input = InvocationPolicyInput {
                workspace_id: None,
                tool: &tool,
                arguments: &arguments,
                policy_basis: Some(&basis),
                authorized_content: &[],
            };
            assert_eq!(
                RuntimeInvocationPolicyEvaluator.evaluate(&input),
                InvocationPolicyDisposition::Inherit
            );
        }
    }

    #[test]
    fn by_policy_is_not_unconditional_approval() {
        let evaluator = RuntimeInvocationPolicyEvaluator;
        let tool = candidate_tool();
        let basis = task_basis();
        let mut empty_basis = basis.clone();
        empty_basis.source_records.clear();

        assert_eq!(
            evaluator.evaluate(&InvocationPolicyInput {
                workspace_id: None,
                tool: &tool,
                arguments: &serde_json::json!({"prompt":"a private illustration"}),
                policy_basis: Some(&basis),
                authorized_content: &[],
            }),
            InvocationPolicyDisposition::Allow
        );
        assert!(matches!(
            evaluator.evaluate(&InvocationPolicyInput {
                workspace_id: None,
                tool: &tool,
                arguments: &serde_json::json!({"title":"candidate without task authority"}),
                policy_basis: Some(&empty_basis),
                authorized_content: &[],
            }),
            InvocationPolicyDisposition::RequirePolicyApproval(_)
        ));
        assert!(matches!(
            evaluator.evaluate(&InvocationPolicyInput {
                workspace_id: None,
                tool: &tool,
                arguments: &serde_json::json!({"prompt":"unbound"}),
                policy_basis: None,
                authorized_content: &[],
            }),
            InvocationPolicyDisposition::RequirePolicyApproval(_)
        ));
    }

    #[test]
    fn renamed_legacy_policy_does_not_collide_with_current_kernel_namespace() {
        let tool = candidate_tool();
        let mut basis = task_basis();
        let args = serde_json::json!({});
        let current = InvocationPolicyInput {
            workspace_id: None,
            tool: &tool,
            arguments: &args,
            policy_basis: Some(&basis),
            authorized_content: &[],
        };
        assert_eq!(
            invocation_policy_identity_for(&current).0,
            INVOCATION_POLICY_REF
        );
        let current_fingerprint = consequence_fingerprint(&current);
        basis.agent_policy_ref =
            VersionedKey::new("musteragent.legacy.runtime-policy@1").expect("legacy key");
        let legacy = InvocationPolicyInput {
            workspace_id: None,
            tool: &tool,
            arguments: &args,
            policy_basis: Some(&basis),
            authorized_content: &[],
        };
        assert_eq!(
            invocation_policy_identity_for(&legacy).0,
            LEGACY_INVOCATION_POLICY_REF
        );
        assert_ne!(consequence_fingerprint(&legacy), current_fingerprint);
    }

    #[test]
    fn invocation_policy_identity_is_stable_and_distinct_from_agent_policy() {
        let document: serde_json::Value = serde_json::from_slice(INVOCATION_POLICY_DOCUMENT)
            .expect("invocation policy document must be valid JSON");
        assert_eq!(
            invocation_policy_ref(),
            "musteragent.invocation-policy.conditional-approval@9"
        );
        assert_eq!(
            document
                .get("policy_ref")
                .and_then(serde_json::Value::as_str),
            Some(invocation_policy_ref()),
            "the compiled policy identity must match its versioned contract document"
        );
        assert_eq!(
            invocation_policy_hash(),
            Sha256::digest(INVOCATION_POLICY_DOCUMENT)
        );
        assert_ne!(
            invocation_policy_hash(),
            task_basis().agent_policy_hash,
            "argument-level and Agent safety policy identities must not collapse"
        );
    }

    #[test]
    fn experience_candidate_writes_are_task_bounded() {
        let evaluator = RuntimeInvocationPolicyEvaluator;
        let basis = task_basis();
        {
            let key = EXPERIENCE_PROPOSE_TOOL_KEY;
            let mut tool = candidate_tool();
            tool.key = ToolKey::from_versioned_key(
                VersionedKey::new(format!("{key}@1")).expect("tool key"),
            );
            assert_eq!(
                evaluator.evaluate(&InvocationPolicyInput {
                    workspace_id: None,
                    tool: &tool,
                    arguments: &serde_json::json!({"title":"bounded result"}),
                    policy_basis: Some(&basis),
                    authorized_content: &[],
                }),
                InvocationPolicyDisposition::Allow,
                "{key}"
            );
            assert!(matches!(
                evaluator.evaluate(&InvocationPolicyInput {
                    workspace_id: None,
                    tool: &tool,
                    arguments: &serde_json::json!({"title":"unbound result"}),
                    policy_basis: None,
                    authorized_content: &[],
                }),
                InvocationPolicyDisposition::RequirePolicyApproval(_)
            ));
        }
    }

    #[test]
    fn session_fingerprint_reuses_exact_operation_across_task_turns() {
        let tool = candidate_tool();
        let first_basis = task_basis();
        let second_basis = task_basis();
        let arguments = serde_json::json!({"prompt":"the same exact operation"});
        let first = InvocationPolicyInput {
            workspace_id: None,
            tool: &tool,
            arguments: &arguments,
            policy_basis: Some(&first_basis),
            authorized_content: &[],
        };
        let second = InvocationPolicyInput {
            workspace_id: None,
            tool: &tool,
            arguments: &arguments,
            policy_basis: Some(&second_basis),
            authorized_content: &[],
        };

        assert_ne!(
            consequence_fingerprint(&first),
            consequence_fingerprint(&second)
        );
        assert_eq!(
            session_approval_fingerprint(&first),
            session_approval_fingerprint(&second)
        );

        let changed_arguments = serde_json::json!({"prompt":"a different operation"});
        let changed = InvocationPolicyInput {
            workspace_id: None,
            tool: &tool,
            arguments: &changed_arguments,
            policy_basis: Some(&second_basis),
            authorized_content: &[],
        };
        assert_ne!(
            session_approval_fingerprint(&first),
            session_approval_fingerprint(&changed)
        );
    }

    #[test]
    fn exact_approval_is_bound_to_the_authoritative_workspace() {
        let tool = candidate_tool();
        let basis = task_basis();
        let arguments = serde_json::json!({"prompt":"same invocation in another workspace"});
        let first = InvocationPolicyInput {
            workspace_id: Some(musteragent_identity::WorkspaceId::generate()),
            tool: &tool,
            arguments: &arguments,
            policy_basis: Some(&basis),
            authorized_content: &[],
        };
        let second = InvocationPolicyInput {
            workspace_id: Some(musteragent_identity::WorkspaceId::generate()),
            ..first
        };
        assert_ne!(
            session_approval_fingerprint(&first),
            session_approval_fingerprint(&second)
        );
    }

    fn task_basis() -> InvocationPolicyBasis {
        let bytes = b"generate a private image";
        InvocationPolicyBasis {
            source_records: vec![TaskProvenanceRecord {
                record_id: LedgerRecordId::generate(),
                source_kind: TaskProvenanceSourceKind::UserMessage,
                content_ref: musteragent_content_ref::ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
                    byte_length: bytes.len() as u64,
                    sha256: Sha256::digest(bytes),
                },
            }],
            agent_policy_ref: VersionedKey::new("musteragent.runtime-policy@1").expect("policy"),
            agent_policy_hash: Sha256::digest(b"policy"),
            permission_mode: PermissionMode::Supervised,
        }
    }

    fn candidate_tool() -> ToolManifest {
        let schema = |key: &str| SchemaRef {
            schema_key: VersionedKey::new(key).expect("schema key"),
            schema_hash: Sha256::digest(key.as_bytes()),
        };
        ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(
                VersionedKey::new(format!("{EXPERIENCE_PROPOSE_TOOL_KEY}@1")).expect("tool key"),
            ),
            wire_name: WireName::new("image_generation_tool").expect("wire name"),
            input_schema: schema("artifact.image.create.input@1"),
            output_schema: schema("artifact.image.create.output@1"),
            capability_key: VersionedKey::new("media.image.create@1").expect("capability key"),
            capability_provider_id: "01982f80-0000-7000-8000-000000000101"
                .parse()
                .expect("provider"),
            effect: Effect::WriteExternal,
            approval: ApprovalRequirement::ByPolicy,
            execution_policy: musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: Duration::from_secs(30),
            },
            max_attempts: NonZeroU32::new(1).expect("attempts"),
            retry_contract: ToolRetryContract::QueryBeforeRetry,
            required_scopes: BTreeSet::new(),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::ProviderBound,
        }
    }
}

fn desktop_metadata_policy(input: &InvocationPolicyInput<'_>) -> InvocationPolicyDisposition {
    let mode = input
        .policy_basis
        .filter(|basis| basis.has_current_task())
        .map(|basis| basis.permission_mode);
    desktop_metadata_decision(mode)
}
fn desktop_metadata_decision(
    mode: Option<musteragent_policy::PermissionMode>,
) -> InvocationPolicyDisposition {
    use musteragent_policy::PermissionMode;
    match mode {
        Some(PermissionMode::FullAccess) => InvocationPolicyDisposition::Allow,
        Some(PermissionMode::Supervised | PermissionMode::Trusted) => {
            InvocationPolicyDisposition::RequireApproval(
                "Listing Desktop metadata requires approval for this exact request",
            )
        }
        _ => InvocationPolicyDisposition::Deny(
            "Desktop metadata requires an authorized current task",
        ),
    }
}

#[cfg(test)]
mod desktop_metadata_tests {
    use super::*;
    use musteragent_policy::PermissionMode::*;
    #[test]
    fn frozen_mode_does_not_authorize_unattended_or_reusable_host_access() {
        assert_eq!(
            desktop_metadata_decision(Some(FullAccess)),
            InvocationPolicyDisposition::Allow
        );
        for mode in [Supervised, Trusted] {
            assert!(matches!(
                desktop_metadata_decision(Some(mode)),
                InvocationPolicyDisposition::RequireApproval(_)
            ));
        }
        for mode in [None, Some(Deny), Some(ServiceManaged)] {
            assert!(matches!(
                desktop_metadata_decision(mode),
                InvocationPolicyDisposition::Deny(_)
            ));
        }
    }
}
