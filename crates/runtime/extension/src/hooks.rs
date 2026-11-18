//! Hook contracts. Observations borrow immutable inputs; transformations return values.
use crate::RuntimeApi;
use musteragent_identity::{InvocationId, RunId};
use musteragent_loop::{AgentLoopExit, PlannedToolCall, ToolExecutionOutcome};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{
    CanonicalMessage, CanonicalModelRequest, JsonValue, ModelStreamEvent, ModelText,
};
use musteragent_tool_executor::{InvocationPolicyDisposition, InvocationPolicyInput};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};

/// Host-verified execution identity. Never populated from model arguments.
#[derive(Debug, Clone)]
pub struct ExtensionContext {
    /// Admitted run authority.
    pub run: RunContext,
    /// Current fenced attempt.
    pub attempt: AttemptContext,
    /// Host-selected workspace directory.
    pub workspace_dir: PathBuf,
    /// Opaque host session key.
    pub session_key: String,
    /// Host channel identifier, if there is one.
    pub channel: Option<String>,
    /// Whether the tool runs in a sandbox.
    pub sandboxed: bool,
    /// Authoritative newly admitted input; absent for recovery/background work.
    /// Historical context is never searched for executable commands.
    pub ingress: Option<CommandInput>,
}

/// Host-admitted input identity, used for command idempotency across retries.
#[derive(Debug, Clone)]
pub struct CommandInput {
    /// Stable ingress identity supplied by the host, never by the model.
    pub id: String,
    /// Original user input, before context selection or compaction.
    pub text: ModelText,
}

/// Stable hook identity used in audit records and errors.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hook {
    /// Freeze registration identity before execution.
    Registration,
    /// Prompt contribution.
    PromptBuild,
    /// Read-only invocation policy.
    InvocationPolicy,
    /// Tool admission / argument transformation.
    BeforeToolCall,
    /// Result observer.
    AfterToolCall,
    /// Result context view.
    ToolResultPersist,
    /// Model context view.
    ContextProjection,
    /// Outbound annotations.
    BeforeProviderRequest,
    /// Input observer.
    LlmInput,
    /// Output event observer (including usage and terminal events).
    LlmOutput,
    /// Before entering the agent loop.
    AgentStart,
    /// After every loop exit, including failure/cancellation/wait.
    AgentEnd,
    /// Host session has ended normally.
    SessionEnd,
    /// Host session is shutting down.
    SessionShutdown,
    /// Delegation port accepted a child run.
    SubagentSpawned,
    /// Host observed child completion.
    SubagentEnded,
    /// Background service start.
    ServiceStart,
    /// Background service stop.
    ServiceStop,
    /// Slash command dispatch.
    Command,
    /// Host method invocation.
    GatewayMethod,
    /// Registered tool execution.
    Tool,
    /// Memory port or middleware.
    Memory,
    /// Committed outbox observer.
    Outbox,
}

/// A hook's explicit failure, safe to persist. Do not put credentials in messages.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct HookError {
    /// Stable extension-specific code.
    pub code: String,
    /// Safe explanation.
    pub message: String,
}
impl HookError {
    /// Construct a failure.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Failure classification independent of error message text.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookFailureKind {
    /// Extension returned an explicit error.
    Error(HookError),
    /// Extension unwound. Panic payloads may contain secrets and are not recorded.
    Panic,
}

/// Typed, recorded hook failure.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("extension {extension} failed in {hook:?}: {kind:?}")]
pub struct HookFailure {
    /// Extension/service identity.
    pub extension: String,
    /// Failed hook.
    pub hook: Hook,
    /// Error versus panic.
    pub kind: HookFailureKind,
}

/// Composition/port failures. Audit failures never silently permit dispatch.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ExtensionError {
    /// Registration name is not a safe single component.
    #[error("invalid registration identifier: {0}")]
    InvalidIdentifier(String),
    /// Registry entry or exclusive slot already occupied.
    #[error("registration already exists: {0}")]
    Duplicate(String),
    /// Required host port or registration is unavailable.
    #[error("unavailable extension capability: {0}")]
    Unavailable(String),
    /// Typed configuration could not be decoded.
    #[error("invalid extension configuration: {0}")]
    Config(String),
    /// Host/run identities disagree.
    #[error("extension context identity mismatch")]
    IdentityMismatch,
    /// Persistent append/read failed.
    #[error("extension audit failed: {0}")]
    Audit(String),
    /// Hook denied or failed before dispatch.
    #[error(transparent)]
    Hook(#[from] HookFailure),
    /// Invalid loop-detection policy.
    #[error("invalid loop detector configuration: {0}")]
    LoopConfig(String),
    /// State directory could not be resolved.
    #[error("extension state directory: {0}")]
    StateDirectory(String),
}

/// Ordered additions to a fresh system context. Prepends from A/B appear A/B,
/// followed by the base context, followed by appends and named sections A/B.
#[derive(Debug, Default, Clone)]
pub struct PromptContribution {
    /// Insert before the base system instructions.
    pub prepend: Vec<ModelText>,
    /// Insert after the base system instructions.
    pub append: Vec<ModelText>,
    /// Explicitly named sections, rendered after this extension's appends.
    pub sections: Vec<(String, ModelText)>,
}

/// Only model-visible context is mutable. Route, credentials, authority, tools,
/// invocation IDs and durable request material are outside this view.
#[derive(Debug, Clone)]
pub struct ContextView {
    /// System instructions.
    pub instructions: Vec<ModelText>,
    /// Conversation messages projected for this physical model call.
    pub input: Vec<CanonicalMessage>,
}

/// Before-tool decision. A deny cannot be undone by a later extension.
#[derive(Debug, Clone)]
pub enum ToolDecision {
    /// Preserve the current arguments and existing authorization.
    Allow,
    /// Replace arguments; the before/after values are appended before dispatch.
    Rewrite(JsonValue),
    /// Reject the call with a stable typed reason.
    Deny(HookError),
}

/// Lifecycle data; variants correspond to explicit host and loop boundaries.
#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    /// Attempt is about to enter the loop.
    AgentStart,
    /// Exact loop exit; waiting is distinct from a successful completion.
    AgentEnd(AgentLoopExit),
    /// Session ended normally.
    SessionEnd,
    /// Session shutdown, including process shutdown.
    SessionShutdown,
    /// Child accepted by delegation port.
    SubagentSpawned(RunId),
    /// Child terminal report supplied by its owner.
    SubagentEnded {
        /// Child run identity.
        run_id: RunId,
        /// Terminal report supplied by the child run owner.
        report: serde_json::Value,
    },
}
impl LifecycleEvent {
    /// Audit hook identity.
    pub fn hook(&self) -> Hook {
        match self {
            Self::AgentStart => Hook::AgentStart,
            Self::AgentEnd(_) => Hook::AgentEnd,
            Self::SessionEnd => Hook::SessionEnd,
            Self::SessionShutdown => Hook::SessionShutdown,
            Self::SubagentSpawned(_) => Hook::SubagentSpawned,
            Self::SubagentEnded { .. } => Hook::SubagentEnded,
        }
    }
}

/// An ordered extension. Default hooks are explicitly no-op optional contributions.
/// Registration is performed by the host through `ExtensionRegistry`.
pub trait KernelExtension: Send + Sync {
    /// Unique stable identifier, validated when registered.
    fn id(&self) -> &str;
    /// Build fresh prompt contributions before each physical model call.
    fn prompt_build(
        &self,
        _context: &ExtensionContext,
        _request: &CanonicalModelRequest,
    ) -> Result<PromptContribution, HookError> {
        Ok(PromptContribution::default())
    }
    /// Transform only the context view.
    fn context_projection(
        &self,
        _context: &ExtensionContext,
        view: ContextView,
    ) -> Result<ContextView, HookError> {
        Ok(view)
    }
    /// Parameter policy composed with the runtime evaluator; strictest wins.
    fn invocation_policy(
        &self,
        _input: &InvocationPolicyInput<'_>,
    ) -> Result<InvocationPolicyDisposition, HookError> {
        Ok(InvocationPolicyDisposition::Inherit)
    }
    /// Deny or rewrite before the real tool executor validates/authorizes arguments.
    fn before_tool_call(
        &self,
        _context: &ExtensionContext,
        _call: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        Ok(ToolDecision::Allow)
    }
    /// Observe the immutable original outcome.
    fn after_tool_call(
        &self,
        _context: &ExtensionContext,
        _call: &PlannedToolCall,
        _outcome: &ToolExecutionOutcome,
    ) -> Result<(), HookError> {
        Ok(())
    }
    /// Rewrite a result's context view. Called on fresh copies, never ledger records.
    fn tool_result_persist(
        &self,
        _context: &ExtensionContext,
        result: CanonicalMessage,
    ) -> Result<CanonicalMessage, HookError> {
        Ok(result)
    }
    /// Add validated transport annotations. These are not model content.
    fn before_provider_request(
        &self,
        _context: &ExtensionContext,
        _request: &CanonicalModelRequest,
        _annotations: &mut musteragent_runtime_model::ModelRequestAnnotations,
    ) -> Result<(), HookError> {
        Ok(())
    }
    /// Observe the exact outgoing view; cannot mutate it.
    fn llm_input(
        &self,
        _context: &ExtensionContext,
        _request: &CanonicalModelRequest,
    ) -> Result<(), HookError> {
        Ok(())
    }
    /// Observe each output event, including usage on incomplete/failed responses.
    fn llm_output(
        &self,
        _context: &ExtensionContext,
        _invocation: InvocationId,
        _event: &ModelStreamEvent,
    ) -> Result<(), HookError> {
        Ok(())
    }
    /// Ordered lifecycle hook. Start failures abort; terminal observers continue.
    fn lifecycle<'a>(
        &'a self,
        _context: &'a ExtensionContext,
        _event: &'a LifecycleEvent,
        _api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async { Ok(()) })
    }
    /// Observe a committed outbox item. Host owns acknowledgement/retry cursor.
    fn outbox(
        &self,
        _context: &ExtensionContext,
        _message: &musteragent_ledger::CommittedOutbox,
    ) -> Result<(), HookError> {
        Ok(())
    }
}

pub(crate) fn guard<T>(
    id: &str,
    hook: Hook,
    f: impl FnOnce() -> Result<T, HookError>,
) -> Result<T, HookFailure> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(HookFailure {
            extension: id.into(),
            hook,
            kind: HookFailureKind::Error(error),
        }),
        Err(_) => Err(HookFailure {
            extension: id.into(),
            hook,
            kind: HookFailureKind::Panic,
        }),
    }
}

pub(crate) async fn guard_async<'a, T>(
    id: &str,
    hook: Hook,
    f: impl FnOnce() -> ApplicationFuture<'a, Result<T, HookError>>,
) -> Result<T, HookFailure> {
    use futures_util::FutureExt;
    let future = guard(id, hook, || Ok(f()))?;
    match std::panic::AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(HookFailure {
            extension: id.into(),
            hook,
            kind: HookFailureKind::Error(error),
        }),
        Err(_) => Err(HookFailure {
            extension: id.into(),
            hook,
            kind: HookFailureKind::Panic,
        }),
    }
}

pub(crate) fn valid_id(id: &str) -> Result<(), ExtensionError> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        || id == "."
        || id == ".."
    {
        return Err(ExtensionError::InvalidIdentifier(id.into()));
    }
    Ok(())
}

/// Shared extension instance; registration order is preserved.
pub type Extension = Arc<dyn KernelExtension>;
