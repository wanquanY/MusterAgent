//! Provider-neutral, durable Tool execution shared by Cloud and Device homes.

#![forbid(unsafe_code)]

mod atomic_authorization;
pub use atomic_authorization::dialogue_workspace_matches;
mod atomic_tool_receipt;
mod local_execution;
pub use local_execution::{
    local_atomic_execution, opaque_atomic_execution, opaque_dispatcher_execution,
};
mod delegation_history;
pub use delegation_history::HistoryCapabilityHandler;
mod approval_reviewer;
mod artifact;
mod browser_environment;
pub use browser_environment::BrowserEnvironmentCapabilityHandler;
mod browser_execution;
mod browser_python;
pub use browser_python::BrowserPythonCapabilityHandler;
mod browser_workspace;
mod browser_workspace_feedback;
mod browser_workspace_inputs;
mod reconciliation;
pub use browser_workspace::BrowserWorkspaceCapabilityHandler;
mod diagnostic_result;
mod producer_content;
pub use diagnostic_result::persist as persist_tool_diagnostic;
mod discovery;
mod semantic_tool_search;
pub use semantic_tool_search::SemanticToolSearch;
mod discovery_catalog;
mod dispatcher;
mod product_dispatcher;
pub use product_dispatcher::{ProductCapabilityDispatcher, PublishedCapabilityFactory};
mod executor;
mod file_artifact;
mod file_edit;
mod file_query;
pub use file_query::{FileQueryBackendOutcome, FileQueryCapabilityHandler, FileQueryToolBackend};
mod interaction;
mod invocation_policy;
mod terminal_host_access;
pub use terminal_host_access::{
    TERMINAL_HOST_ACCESS_REASON, TERMINAL_HOST_ACCESS_TOOL, exact_invocation_approved,
    terminal_host_access_approved, terminal_host_access_requested,
};
mod manifest;
mod media_artifact;
mod memory;
pub use media_artifact::MediaArtifactCapabilityHandler;
mod default_work_item;
mod executor_environment;
mod work_item;
pub use default_work_item::DefaultWorkItemTools;
pub use executor_environment::ExecutorEnvironmentCapabilityHandler;
pub use work_item::WorkItemCapabilityHandler;
mod dialogue_publication;
pub use dialogue_publication::DialoguePublicationCapabilityHandler;
mod plan;
mod resource;
mod schema_validation;
mod skill;
mod skill_preview;
pub use skill_preview::SkillPreviewCapabilityHandler;
mod standing_goal;
mod vision;

pub use approval_reviewer::{
    ApprovalAutoReviewDecision, ApprovalAutoReviewInput, approval_auto_review_policy_hash,
    approval_auto_review_policy_ref, review_approval,
};
pub use discovery::{DiscoveryCapabilityHandler, validate_tool_exposure_commit};
pub use discovery_catalog::{DiscoveryExecutionCatalogError, discovery_execution_definitions};
pub use dispatcher::{
    ApprovalPromptOverride, AtomicCapabilityHandler, CapabilityDispatcher,
    CompositeCapabilityDispatcher, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    RegistryCapabilityDispatcher, RetainedToolResult, ToolPhysicalExecution,
};
pub use executor::{
    DurableToolExecutor, InheritInvocationPolicyEvaluator, ToolDispatchAuthorizer,
    WorkerTraceContextSource, interaction_id, metering_outbox_id, nested_invocation_id,
    tool_invocation_id, tool_invocation_idempotency_key, tool_request_content_id,
};
pub use file_artifact::FileArtifactCapabilityHandler;
pub use file_edit::FileEditCapabilityHandler;
pub use interaction::InteractionCapabilityHandler;
pub use invocation_policy::{
    InvocationPolicyDisposition, InvocationPolicyEvaluationError, InvocationPolicyEvaluator,
    InvocationPolicyInput, RuntimeInvocationPolicyEvaluator, combine_dispositions,
    consequence_fingerprint, invocation_policy_hash, invocation_policy_ref,
    session_approval_fingerprint,
};
pub use manifest::{builtin_capability_id, builtin_capability_manifest_at_location};
pub use memory::MemoryCapabilityHandler;
pub use plan::ExecutionPlanCapabilityHandler;
pub use resource::ResourceCapabilityHandler;
pub use skill::{SkillDiscoveryCapabilityHandler, SkillExecutionResources};
pub use standing_goal::{STANDING_GOAL_TOOL_KEYS, StandingGoalCapabilityHandler};
pub use vision::{VisionCapabilityHandler, vision_model_context};

mod account;
pub use account::AccountCapabilityHandler;

mod sandbox_routing;
