use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::num::NonZeroU64;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_identity::{
    ActivityId, DelegationDeliveryId, DelegationId, InvocationId, OutputId, RunId, ToolCallId,
};
use musteragent_model::{ApiFormat, CredentialGrantRef, ModelKey, ModelRoute, ProviderStatePolicy};
use musteragent_run::InvocationContext;
use musteragent_tool::{ToolKey, WireName};
use thiserror::Error;

use crate::{ContinuationRef, ModelRouteRef, ModelUsage, ProviderRequestRef};

const MAX_MODEL_TEXT_BYTES: usize = 1_048_576;
const MAX_MODEL_REPLAY_TEXT_BYTES: usize = 64 * 1_048_576;
const MAX_JSON_DEPTH: usize = 64;

/// Validated immutable text. Context projections share its allocation instead
/// of copying the full conversation at each route/exposure/budget stage.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ModelText(Arc<str>);

impl ModelText {
    pub fn new(value: impl Into<String>) -> Result<Self, CanonicalModelError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CanonicalModelError::EmptyText);
        }
        if value.len() > MAX_MODEL_TEXT_BYTES {
            return Err(CanonicalModelError::TextTooLong);
        }
        if value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(CanonicalModelError::TextControlCharacter);
        }
        Ok(Self(value.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Splits one logical text value into bounded, adjacent canonical parts.
    ///
    /// `ModelText` bounds each independently validated transport part, not the
    /// size of a user Message. Context materializers use this constructor so a
    /// large UTF-8 Message remains one canonical Message without weakening the
    /// per-part validation contract. Provider adapters must preserve the exact
    /// concatenation of adjacent parts when projecting their wire protocol.
    pub fn chunked(value: impl Into<String>) -> Result<Vec<Self>, CanonicalModelError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CanonicalModelError::EmptyText);
        }
        let mut chunks = Vec::with_capacity(value.len().div_ceil(MAX_MODEL_TEXT_BYTES));
        let mut start = 0;
        while start < value.len() {
            let mut end = start.saturating_add(MAX_MODEL_TEXT_BYTES).min(value.len());
            while !value.is_char_boundary(end) {
                end = end.saturating_sub(1);
            }
            debug_assert!(
                end > start,
                "one UTF-8 scalar always fits in the part bound"
            );
            chunks.push(Self::new(value[start..end].to_owned())?);
            start = end;
        }
        Ok(chunks)
    }
}

/// Provider-private replay material accumulated across multiple bounded
/// stream events. Public text and individual event values retain the tighter
/// `ModelText` contract.
/// Clones share the immutable payload, including large private replay blocks.
#[derive(Clone, Eq, PartialEq)]
pub struct ModelReplayText(Arc<str>);

impl ModelReplayText {
    pub fn new(value: impl Into<String>) -> Result<Self, CanonicalModelError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CanonicalModelError::EmptyText);
        }
        if value.len() > MAX_MODEL_REPLAY_TEXT_BYTES {
            return Err(CanonicalModelError::TextTooLong);
        }
        if value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(CanonicalModelError::TextControlCharacter);
        }
        Ok(Self(value.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for ModelReplayText {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ModelReplayText([REDACTED])")
    }
}

/// Provider-neutral JSON value. Objects are ordered to keep request fixtures
/// and signing/hash inputs deterministic.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Integer(i64),
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

impl JsonValue {
    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        self.validate_at_depth(0)
    }

    fn validate_at_depth(&self, depth: usize) -> Result<(), CanonicalModelError> {
        if depth > MAX_JSON_DEPTH {
            return Err(CanonicalModelError::JsonTooDeep);
        }
        match self {
            Self::Number(value) if !value.is_finite() => Err(CanonicalModelError::NonFiniteNumber),
            Self::String(value) if value.len() > MAX_MODEL_TEXT_BYTES => {
                Err(CanonicalModelError::TextTooLong)
            }
            Self::Array(values) => {
                for value in values {
                    value.validate_at_depth(depth + 1)?;
                }
                Ok(())
            }
            Self::Object(values) => {
                for (key, value) in values {
                    if key.is_empty() || key.len() > 256 {
                        return Err(CanonicalModelError::InvalidJsonObjectKey);
                    }
                    value.validate_at_depth(depth + 1)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn as_object(&self) -> Result<&BTreeMap<String, JsonValue>, CanonicalModelError> {
        match self {
            Self::Object(value) => Ok(value),
            _ => Err(CanonicalModelError::JsonObjectRequired),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CanonicalRole {
    User,
    Assistant,
    Tool,
}

/// Private Runtime-owned input that wakes an existing Run without pretending
/// to be a conversation participant or changing the Run's policy authority.
///
/// Provider protocols do not share a native Runtime-event role. Adapters
/// project these events into the protocol's legal input form only at the wire
/// boundary; the canonical request retains the exact source semantics and
/// provenance independently of that transport representation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CanonicalRuntimeEvent {
    ToolApprovalResolved(crate::ToolApprovalObservation),
    /// A rejection committed by the original Dialogue delivery Owner. This
    /// carries authority bindings only; the complete candidate stays private
    /// Assistant data in the same authorized request.
    PublicDeliveryRepair {
        source_invocation_id: InvocationId,
        binding: musteragent_execution_context::PublicDeliveryRepairBinding,
    },
    AgentDialogueHistory {
        frozen_cut: musteragent_execution_context::DialogueContextCut,
        from_seq: u64,
        through_seq: u64,
        source_page_ref: musteragent_content_ref::ContentRef,
        materialized_context: ModelText,
        input_attachments: Vec<crate::DialogueInputObservation>,
    },
    /// A durable failed primary invocation followed by an authorized same-Run
    /// retry. This is not inferred from partial Assistant prose.
    ModelRecovery {
        source_invocation_id: InvocationId,
    },
    /// A completed private response did not satisfy the sealed NativeToolsV1
    /// publication contract. Context proves the original current-Run source.
    NativePublicationRequired {
        source_invocation_id: InvocationId,
    },
    /// The current Run's latest physical response ended at a generation token limit.
    /// This is reconstructed from durable completion evidence, never inferred
    /// from Assistant wording or from the presence of an opaque provider token.
    OutputLimitContinuation {
        source_invocation_id: InvocationId,
    },
    DelegationCompletion {
        delivery_id: DelegationDeliveryId,
        delegation_id: DelegationId,
        child_activity_id: ActivityId,
        child_run_id: RunId,
        report: ModelText,
    },
}

impl CanonicalRuntimeEvent {
    /// Shared by provider presentation and capacity planning. Reference text is
    /// JSON-escaped, so source content cannot manufacture this literal boundary.
    /// Provider-facing private context. The fixed boundary makes the event's
    /// authority explicit while preserving the ChildReport bytes verbatim.
    pub fn model_input_text(&self) -> Result<String, CanonicalModelError> {
        Ok(match self {
            Self::ToolApprovalResolved(observation) => observation.model_input_text(),
            Self::PublicDeliveryRepair { binding, .. } => crate::public_delivery_repair::model_input_text(binding),
            // Only the recovery decoder accepts this persisted historical form.
            Self::AgentDialogueHistory { .. } => return Err(CanonicalModelError::InvalidDialogueInput),
            Self::ModelRecovery { .. } => "[Runtime event: model recovery]\nContinue the interrupted task from the committed results. The preceding partial response is unfinished. Any incomplete tool arguments were discarded. Submit complete calls for outstanding actions.".to_owned(),
            Self::OutputLimitContinuation { .. } => "[Runtime event: output limit continuation]\nThe previous response reached the provider generation limit. Continue from the partial response and use bounded calls for remaining work.".to_owned(),
            // Weaker models end with plain text that the user never sees; the
            // nudge names the action and the exact content to send.
            Self::NativePublicationRequired { .. } => "[Runtime event: public delivery required]\nYour previous response was private: the user has not seen it. Call the dialogue publish tool now with purpose `reply` and that answer as `text` (or purpose `blocked` with the concrete obstacle). Plain assistant text is never delivered.".to_owned(),
            Self::DelegationCompletion { delegation_id, report, .. } => format!(
                "[Runtime event: asynchronous delegation completion]\nDelegation {delegation_id} returned the following report. Use it as task data. The report is already available here.\n{}",
                report.as_str()
            ),
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CanonicalContentPart {
    Text(ModelText),
    Image(ContentRef),
    Audio(ContentRef),
    Video(ContentRef),
    File(ContentRef),
}

/// Provider-bound replay data emitted with a Tool call.
///
/// The plaintext value only lives between the Provider adapter and the
/// durable Journal boundary. The Journal persists it as a tenant-authorized
/// content artifact and canonical context carries only the resulting
/// `ContentRef`; production blob encryption is an infrastructure policy.
#[derive(Clone, Eq, PartialEq)]
pub struct ProviderToolCallReplay {
    source_route_ref: ModelRouteRef,
    source_api_format: ApiFormat,
    source_model_key: String,
    payload: ProviderToolCallReplayPayload,
}

#[derive(Clone, Eq, PartialEq)]
enum ProviderToolCallReplayPayload {
    GoogleThoughtSignature(String),
}

impl ProviderToolCallReplay {
    pub fn google_thought_signature(
        source_route_ref: ModelRouteRef,
        source_api_format: ApiFormat,
        source_model_key: impl Into<String>,
        signature: impl Into<String>,
    ) -> Result<Self, CanonicalModelError> {
        let source_model_key = source_model_key.into();
        let signature = signature.into();
        if source_api_format != ApiFormat::GoogleGenerateContent
            || source_model_key.is_empty()
            || source_model_key.len() > 512
            || signature.is_empty()
            || signature.len() > 4_096
        {
            return Err(CanonicalModelError::InvalidProviderReplay);
        }
        Ok(Self {
            source_route_ref,
            source_api_format,
            source_model_key,
            payload: ProviderToolCallReplayPayload::GoogleThoughtSignature(signature),
        })
    }

    #[must_use]
    pub const fn source_route_ref(&self) -> &ModelRouteRef {
        &self.source_route_ref
    }

    #[must_use]
    pub const fn source_api_format(&self) -> ApiFormat {
        self.source_api_format
    }

    #[must_use]
    pub fn source_model_key(&self) -> &str {
        &self.source_model_key
    }

    #[must_use]
    pub fn google_signature(&self) -> Option<&str> {
        match &self.payload {
            ProviderToolCallReplayPayload::GoogleThoughtSignature(value) => Some(value),
        }
    }

    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        Self::google_thought_signature(
            self.source_route_ref.clone(),
            self.source_api_format,
            self.source_model_key.clone(),
            self.google_signature()
                .ok_or(CanonicalModelError::InvalidProviderReplay)?
                .to_owned(),
        )
        .map(|_| ())
    }
}

impl Debug for ProviderToolCallReplay {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderToolCallReplay")
            .field("source_route_ref", &self.source_route_ref)
            .field("source_api_format", &self.source_api_format)
            .field("source_model_key", &self.source_model_key)
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalToolCall {
    pub call_id: ToolCallId,
    pub name: WireName,
    pub arguments: JsonValue,
    pub provider_item_ref: Option<ProviderRequestRef>,
    pub provider_replay_ref: Option<ContentRef>,
}

impl CanonicalToolCall {
    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        self.arguments.as_object()?;
        self.arguments.validate()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReasoningReplay {
    pub content_ref: ContentRef,
    pub source_route_ref: ModelRouteRef,
    pub continuation_ref: Option<ContinuationRef>,
}

/// One provider-native reasoning item that may be replayed only to the exact
/// source protocol and model. It is carried through the in-process stream but
/// never emitted as a public output Frame.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderReasoningReplayItem {
    pub source_api_format: ApiFormat,
    pub source_model_key: String,
    pub payload: JsonValue,
}

impl ProviderReasoningReplayItem {
    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        if self.source_model_key.trim().is_empty() || self.source_model_key.len() > 512 {
            return Err(CanonicalModelError::InvalidProviderReplay);
        }
        self.payload
            .as_object()
            .map_err(|_| CanonicalModelError::InvalidProviderReplay)?;
        self.payload.validate()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalMessage {
    pub role: CanonicalRole,
    pub content: Vec<CanonicalContentPart>,
    pub tool_calls: Vec<CanonicalToolCall>,
    pub tool_call_id: Option<ToolCallId>,
    pub tool_name: Option<WireName>,
    pub reasoning: Option<ReasoningReplay>,
}

impl CanonicalMessage {
    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        if self.content.is_empty()
            && self.tool_calls.is_empty()
            && !(self.role == CanonicalRole::Assistant && self.reasoning.is_some())
        {
            return Err(CanonicalModelError::EmptyMessage);
        }
        for tool_call in &self.tool_calls {
            tool_call.validate()?;
        }
        for part in &self.content {
            let valid = match part {
                CanonicalContentPart::Text(_) | CanonicalContentPart::File(_) => true,
                CanonicalContentPart::Image(reference) => {
                    reference.media_type.as_str().starts_with("image/")
                }
                CanonicalContentPart::Audio(reference) => {
                    reference.media_type.as_str().starts_with("audio/")
                }
                CanonicalContentPart::Video(reference) => {
                    reference.media_type.as_str().starts_with("video/")
                }
            };
            if !valid {
                return Err(CanonicalModelError::InvalidContentModality);
            }
        }
        match self.role {
            CanonicalRole::User => {
                if !self.tool_calls.is_empty()
                    || self.tool_call_id.is_some()
                    || self.tool_name.is_some()
                    || self.reasoning.is_some()
                {
                    return Err(CanonicalModelError::InvalidUserMessage);
                }
            }
            CanonicalRole::Assistant => {
                if self.tool_call_id.is_some() || self.tool_name.is_some() {
                    return Err(CanonicalModelError::InvalidAssistantMessage);
                }
            }
            CanonicalRole::Tool => {
                if self.tool_call_id.is_none()
                    || self.tool_name.is_none()
                    || !self.tool_calls.is_empty()
                    || self.reasoning.is_some()
                {
                    return Err(CanonicalModelError::InvalidToolMessage);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalToolDefinition {
    pub key: ToolKey,
    /// Exact name presented to the model, allocated from the complete pinned catalog.
    pub model_name: WireName,
    pub name: WireName,
    pub description: ModelText,
    pub input_schema_ref: SchemaRef,
    pub input_schema: JsonValue,
    pub strict: bool,
}

impl CanonicalToolDefinition {
    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        if self.model_name.as_str().len() > 64 {
            return Err(CanonicalModelError::InvalidToolInterface);
        }
        let object = self.input_schema.as_object()?;
        self.input_schema.validate()?;
        if !matches!(object.get("type"), Some(JsonValue::String(value)) if value == "object") {
            return Err(CanonicalModelError::ToolSchemaObjectRequired);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ReasoningPolicy {
    Disabled,
    Summary,
    Full,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CachePolicy {
    ProviderDefault,
    ExplicitEphemeralPrefix,
    Disabled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalModelRequest {
    pub invocation: InvocationContext,
    pub route: ModelRoute,
    /// Explicit signed route namespace. Never derive this from invocation
    /// purpose, model capabilities, or a Provider API format.
    pub route_role: crate::ModelInvocationRouteRole,
    pub credential_grant_ref: CredentialGrantRef,
    /// Provider-bound server-side state from the previous successful physical
    /// invocation. It is opaque to Runtime Core and is only legal for a route
    /// that explicitly opted into provider continuation.
    pub continuation_ref: Option<ContinuationRef>,
    pub instructions: Vec<ModelText>,
    pub input: Vec<CanonicalMessage>,
    /// Runtime-owned wake-up events appended after durable conversation
    /// history. They are deliberately separate from both participant Messages
    /// and policy instructions.
    pub runtime_events: Vec<CanonicalRuntimeEvent>,
    pub tools: Vec<CanonicalToolDefinition>,
    pub response_schema: Option<(SchemaRef, JsonValue)>,
    pub reasoning_policy: ReasoningPolicy,
    pub cache_policy: CachePolicy,
    /// Admission-pinned bounds enforced independently by the Provider adapter,
    /// canonical normalizer and Agent Loop.
    pub stream_budget: musteragent_runtime_snapshot::ModelStreamBudgetSnapshot,
    pub max_output_tokens: NonZeroU64,
    pub temperature: Option<f64>,
}

impl CanonicalModelRequest {
    pub fn validate(&self) -> Result<(), CanonicalModelError> {
        if self.route.api_format.is_decision() {
            return Err(CanonicalModelError::InvalidRoute);
        }
        if self.invocation.attempt_context.run_id != self.invocation.run_context.run_id {
            return Err(CanonicalModelError::InvocationIdentityMismatch);
        }
        self.route
            .validate()
            .map_err(|_| CanonicalModelError::InvalidRoute)?;
        if self.continuation_ref.is_some()
            && self.route.provider_state_policy != ProviderStatePolicy::OpaqueContinuation
        {
            return Err(CanonicalModelError::ContinuationOnStatelessRoute);
        }
        if self.max_output_tokens.get() > self.route.max_output_tokens.get() {
            return Err(CanonicalModelError::OutputBudgetExceedsRoute);
        }
        self.stream_budget
            .validate()
            .map_err(|_| CanonicalModelError::InvalidStreamBudget)?;
        if self.instructions.is_empty() || self.input.is_empty() {
            return Err(CanonicalModelError::MissingPromptOrInput);
        }
        if !self.runtime_events.is_empty()
            && !matches!(self.route_role, crate::ModelInvocationRouteRole::Chat)
        {
            return Err(CanonicalModelError::RuntimeEventOnAuxiliaryRoute);
        }
        crate::public_delivery_repair::validate_request(self)?;
        crate::dialogue_input::validate_request(self)?;
        crate::native_publication::validate_request(self)?;
        // Evaluation is an internally driven Agent loop: its policy requests
        // another turn until a decision tool commits, including after text-only
        // feedback. Interactive chat still requires a user/tool/runtime stimulus.
        if matches!(self.route_role, crate::ModelInvocationRouteRole::Chat)
            && self.invocation.run_context.activity_kind
                != musteragent_activity::ActivityKind::Evaluation
            && self
                .input
                .last()
                .is_some_and(|message| message.role == CanonicalRole::Assistant)
            && self.runtime_events.is_empty()
        {
            return Err(CanonicalModelError::MissingTurnStimulus);
        }
        for message in &self.input {
            message.validate()?;
        }
        validate_tool_exchanges(
            &self.input,
            self.continuation_ref.is_some()
                && self.route.provider_state_policy == ProviderStatePolicy::OpaqueContinuation,
        )?;
        let mut model_names = BTreeSet::new();
        let mut canonical_names = BTreeSet::new();
        let mut tool_keys = BTreeSet::new();
        for tool in &self.tools {
            tool.validate()?;
            if !model_names.insert(&tool.model_name)
                || !canonical_names.insert(&tool.name)
                || !tool_keys.insert(&tool.key)
            {
                return Err(CanonicalModelError::InvalidToolInterface);
            }
        }
        if let Some((_, schema)) = &self.response_schema {
            schema.as_object()?;
            schema.validate()?;
        }
        if self
            .temperature
            .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
        {
            return Err(CanonicalModelError::InvalidTemperature);
        }
        Ok(())
    }
}

/// Validate closed, ordered historical exchanges. Only a provider-native continuation
/// may begin with results whose calls are held by its authenticated cursor.
pub fn validate_tool_exchanges(
    messages: &[CanonicalMessage],
    continuation_delta: bool,
) -> Result<(), CanonicalModelError> {
    let mut pending = BTreeMap::<ToolCallId, &musteragent_tool::WireName>::new();
    let mut continuation_results = BTreeSet::<ToolCallId>::new();
    let mut saw_non_tool = false;
    for message in messages {
        match message.role {
            CanonicalRole::Assistant => {
                if !pending.is_empty() {
                    return Err(CanonicalModelError::IncompleteToolExchange);
                }
                saw_non_tool = true;
                for call in &message.tool_calls {
                    if pending.insert(call.call_id.clone(), &call.name).is_some() {
                        return Err(CanonicalModelError::DuplicateToolCallId);
                    }
                }
            }
            CanonicalRole::Tool => {
                let call_id = message
                    .tool_call_id
                    .as_ref()
                    .ok_or(CanonicalModelError::InvalidToolMessage)?;
                let name = message
                    .tool_name
                    .as_ref()
                    .ok_or(CanonicalModelError::InvalidToolMessage)?;
                if let Some(expected_name) = pending.remove(call_id) {
                    if expected_name != name {
                        return Err(CanonicalModelError::ToolIdentityMismatch);
                    }
                } else {
                    if !continuation_delta || saw_non_tool {
                        return Err(CanonicalModelError::OrphanToolResult);
                    }
                    if !continuation_results.insert(call_id.clone()) {
                        return Err(CanonicalModelError::DuplicateToolCallId);
                    }
                }
            }
            CanonicalRole::User => {
                if !pending.is_empty() {
                    return Err(CanonicalModelError::IncompleteToolExchange);
                }
                saw_non_tool = true;
            }
        }
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(CanonicalModelError::IncompleteToolExchange)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StopReason {
    Stop,
    ToolCalls,
    MaxOutputTokens,
    /// The provider reports a generation token boundary without distinguishing
    /// the requested output ceiling from the model's context window.
    TokenLimit,
    ContentFilter,
    Cancelled,
    /// The provider explicitly reports exhausted context capacity, including
    /// during generation. Replan through capacity recovery before continuing.
    ContextWindowExceeded,
}

/// Evidence carried by the provider's termination protocol, not inferred from
/// JSON EOF, missing usage, local token estimates, or assistant prose.
#[derive(Debug, Copy, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationLimit {
    OutputTokens,
    OutputOrContextWindow,
}

impl StopReason {
    #[must_use]
    pub const fn generation_limit(self) -> Option<GenerationLimit> {
        match self {
            Self::MaxOutputTokens => Some(GenerationLimit::OutputTokens),
            Self::TokenLimit => Some(GenerationLimit::OutputOrContextWindow),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_token_limited(self) -> bool {
        self.generation_limit().is_some()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProviderErrorKind {
    Authentication,
    Authorization,
    RateLimited,
    Quota,
    InvalidRequest,
    Server,
    Protocol,
    Timeout,
    Transport,
    Cancelled,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProviderErrorCode {
    AuthenticationFailed,
    AuthorizationFailed,
    RateLimited,
    QuotaExceeded,
    CreditsExhausted,
    ModelNotEntitled,
    PaymentRequired,
    /// The product gateway failed before confirming an upstream response.
    GatewayUnavailable,

    InvalidRequest,
    ProviderServer,
    ProtocolViolation,
    DeadlineExceeded,
    TransportFailed,
    Cancelled,
    CredentialResolutionFailed,
    CredentialExpired,
    CredentialStoreUnavailable,
    ContentUnavailable,
    ContentNotFound,
    ContentCapacityExceeded,
    DependencyStoreUnavailable,
    DependencyStateInvalid,
    ContentResolutionFailed,
    RouteConfigurationInvalid,
    /// The Provider explicitly rejected the request because its input exceeded
    /// the model context window (structured HTTP error, streamed error event,
    /// or an equivalent verified terminal signal). Generic 400/413/429,
    /// authentication, output-limit and unknown failures must never use it.
    ContextWindowExceeded,
    /// Accepted inference exists but no host resolver is installed.
    ExistingInvocationResolverUnavailable,
    /// The original inference stayed pending beyond the bounded lookup budget.
    ExistingInvocationTimeout,
    /// The gateway verified a terminal failure of the original inference.
    ExistingInvocationFailed,
    /// Lookup failed without authorizing another inference dispatch.
    ExistingInvocationLookupFailed,
}

impl ProviderErrorCode {
    /// Closed diagnostic vocabulary; never contains Provider response bodies.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthenticationFailed => "authentication_failed",
            Self::AuthorizationFailed => "authorization_failed",
            Self::RateLimited => "rate_limited",
            Self::QuotaExceeded => "quota_exceeded",
            Self::CreditsExhausted => "credits_exhausted",
            Self::ModelNotEntitled => "model_not_entitled",
            Self::PaymentRequired => "payment_required",
            Self::GatewayUnavailable => "gateway_unavailable",

            Self::InvalidRequest => "invalid_request",
            Self::ProviderServer => "provider_server",
            Self::ProtocolViolation => "protocol_violation",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::TransportFailed => "transport_failed",
            Self::Cancelled => "cancelled",
            Self::CredentialResolutionFailed => "credential_resolution_failed",
            Self::CredentialExpired => "credential_expired",
            Self::CredentialStoreUnavailable => "credential_store_unavailable",
            Self::ContentUnavailable => "content_unavailable",
            Self::ContentNotFound => "content_not_found",
            Self::ContentCapacityExceeded => "content_capacity_exceeded",
            Self::DependencyStoreUnavailable => "dependency_store_unavailable",
            Self::DependencyStateInvalid => "dependency_state_invalid",
            Self::ContentResolutionFailed => "content_resolution_failed",
            Self::RouteConfigurationInvalid => "route_configuration_invalid",
            Self::ContextWindowExceeded => "context_window_exceeded",
            Self::ExistingInvocationResolverUnavailable => {
                "existing_invocation_resolver_unavailable"
            }
            Self::ExistingInvocationTimeout => "existing_invocation_timeout",
            Self::ExistingInvocationFailed => "existing_invocation_failed",
            Self::ExistingInvocationLookupFailed => "existing_invocation_lookup_failed",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub safe_code: ProviderErrorCode,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub diagnostic_ref: Option<ProviderRequestRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModelStreamEvent {
    ResponseStarted {
        response_ref: ProviderRequestRef,
        response_model_key: ModelKey,
    },
    TextDelta {
        output_index: u32,
        delta: ModelText,
    },
    ReasoningDelta {
        output_index: u32,
        delta: ModelText,
    },
    ReasoningSummaryDelta {
        output_index: u32,
        delta: ModelText,
    },
    ProviderReasoningReplay {
        output_index: u32,
        item: ProviderReasoningReplayItem,
    },
    ToolCallStarted {
        output_index: u32,
        call_id: ToolCallId,
        name: WireName,
        provider_item_ref: Option<ProviderRequestRef>,
        provider_replay: Option<ProviderToolCallReplay>,
    },
    ToolCallArgumentsDelta {
        output_index: u32,
        call_id: ToolCallId,
        delta: String,
    },
    /// Runtime-derived, schema-whitelisted Artifact content. Providers never
    /// construct this event: the Agent Loop emits it only after decoding a
    /// Tool field declared safe for live presentation.
    ArtifactContentFrame {
        output_id: OutputId,
        frame_index: u64,
        byte_offset_start: u64,
        content: String,
        final_for_output: bool,
    },
    ToolCallCompleted {
        output_index: u32,
        call_id: ToolCallId,
        arguments: JsonValue,
    },
    /// The model used a storage identity instead of a callable exposed name.
    /// Private terminal action evidence, never authority to invoke that tool.
    ToolCallUnexposed {
        name: WireName,
    },
    /// A completed, identified model call whose argument payload cannot be
    /// decoded. The Loop persists a non-executable planning failure and waits
    /// for response completion before the Tool host returns normal feedback.
    /// Corrupted call IDs remain fatal.
    ToolCallRejected {
        output_index: u32,
        call_id: ToolCallId,
        kind: musteragent_runtime_contract::ToolArgumentValidationIssueKind,
        diagnostic: Option<crate::ArgumentDiagnostic>,
    },
    Usage(ModelUsage),
    NativeCompactionItem {
        content_ref: ContentRef,
    },
    ResponseCompleted {
        stop_reason: StopReason,
    },
    ResponseError(ProviderError),
}

impl ModelStreamEvent {
    /// Reasoning summaries can be sealed independently of a delivered answer.
    /// Replaying any answer, admitted Tool lifecycle or Artifact preview would
    /// cross a public delivery boundary and still requires explicit recovery.
    #[must_use]
    pub fn blocks_automatic_replay(&self) -> bool {
        self.is_public_output() && !matches!(self, Self::ReasoningSummaryDelta { .. })
    }

    /// Whether this canonical event contributes user-visible model output.
    ///
    /// This is the single visibility classifier shared by orchestration and
    /// persistence. Provider-private reasoning, identity, usage and terminal
    /// control facts are deliberately excluded. Tool lifecycle events are
    /// public once the Application coalescer admits the complete validated
    /// lifecycle; an incomplete Provider-side call remains private.
    #[must_use]
    pub fn is_public_output(&self) -> bool {
        match self {
            Self::TextDelta { delta, .. } | Self::ReasoningSummaryDelta { delta, .. } => {
                !delta.as_str().trim().is_empty()
            }
            Self::ToolCallStarted { .. }
            | Self::ToolCallArgumentsDelta { .. }
            | Self::ToolCallCompleted { .. } => true,
            Self::ArtifactContentFrame {
                content,
                final_for_output,
                ..
            } => !content.is_empty() || *final_for_output,
            Self::ResponseStarted { .. }
            | Self::ReasoningDelta { .. }
            | Self::ProviderReasoningReplay { .. }
            | Self::ToolCallRejected { .. }
            | Self::ToolCallUnexposed { .. }
            | Self::Usage(_)
            | Self::NativeCompactionItem { .. }
            | Self::ResponseCompleted { .. }
            | Self::ResponseError(_) => false,
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum CanonicalModelError {
    #[error("dialogue input observation has invalid provenance or materialization")]
    InvalidDialogueInput,
    #[error("model tool interface is ambiguous or exceeds the protocol limit")]
    InvalidToolInterface,
    #[error("model text cannot be empty")]
    EmptyText,
    #[error("model text exceeds one MiB")]
    TextTooLong,
    #[error("model text contains an unsupported control character")]
    TextControlCharacter,
    #[error("canonical JSON exceeds the depth limit")]
    JsonTooDeep,
    #[error("canonical JSON number must be finite")]
    NonFiniteNumber,
    #[error("canonical JSON object key is empty or too long")]
    InvalidJsonObjectKey,
    #[error("canonical JSON object is required")]
    JsonObjectRequired,
    #[error("canonical Message cannot be empty")]
    EmptyMessage,
    #[error("canonical content kind does not match its immutable MIME type")]
    InvalidContentModality,
    #[error("User Message contains Assistant or Tool-only fields")]
    InvalidUserMessage,
    #[error("Assistant Message contains Tool-result fields")]
    InvalidAssistantMessage,
    #[error("Tool Message requires text result and exact Tool identity")]
    InvalidToolMessage,
    #[error("Tool input schema root must declare type=object")]
    ToolSchemaObjectRequired,
    #[error("Provider Tool-call replay metadata is invalid")]
    InvalidProviderReplay,
    #[error("Tool call identity is duplicated within one model request")]
    DuplicateToolCallId,
    #[error("Tool result has no matching assistant Tool call")]
    OrphanToolResult,
    #[error("Tool result identity does not match the assistant Tool call")]
    ToolIdentityMismatch,
    #[error("assistant Tool calls and Tool results must remain one complete exchange")]
    IncompleteToolExchange,
    #[error("model request requires instructions and at least one input Message")]
    MissingPromptOrInput,
    #[error("a Chat request ending in Assistant history requires a new Runtime event")]
    MissingTurnStimulus,
    #[error("Runtime events are only legal for Chat model requests")]
    RuntimeEventOnAuxiliaryRoute,
    #[error("private Dialogue delivery repair stimulus is inconsistent with its sealed request")]
    InvalidPublicDeliveryRepair,
    #[error("Native publication stimulus is inconsistent with its sealed request")]
    InvalidNativePublicationRequired,
    #[error("Invocation Attempt and Run identities disagree")]
    InvocationIdentityMismatch,
    #[error("temperature must be finite and between zero and two")]
    InvalidTemperature,
    #[error("pinned Model Route is invalid")]
    InvalidRoute,
    #[error("request output budget exceeds the pinned Model Route")]
    OutputBudgetExceedsRoute,
    #[error("model stream budget is invalid")]
    InvalidStreamBudget,
    #[error("provider continuation is forbidden on a stateless Model Route")]
    ContinuationOnStatelessRoute,
}

impl CanonicalModelError {
    /// Stable, non-sensitive diagnostic key for typed telemetry.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidDialogueInput => "invalid_dialogue_input",
            Self::InvalidToolInterface => "invalid_tool_interface",
            Self::EmptyText => "empty_text",
            Self::TextTooLong => "text_too_long",
            Self::TextControlCharacter => "text_control_character",
            Self::JsonTooDeep => "json_too_deep",
            Self::NonFiniteNumber => "non_finite_number",
            Self::InvalidJsonObjectKey => "invalid_json_object_key",
            Self::JsonObjectRequired => "json_object_required",
            Self::EmptyMessage => "empty_message",
            Self::InvalidContentModality => "invalid_content_modality",
            Self::InvalidUserMessage => "invalid_user_message",
            Self::InvalidAssistantMessage => "invalid_assistant_message",
            Self::InvalidToolMessage => "invalid_tool_message",
            Self::ToolSchemaObjectRequired => "tool_schema_object_required",
            Self::InvalidProviderReplay => "invalid_provider_replay",
            Self::DuplicateToolCallId => "duplicate_tool_call_id",
            Self::OrphanToolResult => "orphan_tool_result",
            Self::ToolIdentityMismatch => "tool_identity_mismatch",
            Self::IncompleteToolExchange => "incomplete_tool_exchange",
            Self::MissingPromptOrInput => "missing_prompt_or_input",
            Self::MissingTurnStimulus => "missing_turn_stimulus",
            Self::RuntimeEventOnAuxiliaryRoute => "runtime_event_on_auxiliary_route",
            Self::InvalidPublicDeliveryRepair => "invalid_public_delivery_repair",
            Self::InvalidNativePublicationRequired => "invalid_native_publication_required",
            Self::InvocationIdentityMismatch => "invocation_identity_mismatch",
            Self::InvalidTemperature => "invalid_temperature",
            Self::InvalidRoute => "invalid_route",
            Self::OutputBudgetExceedsRoute => "output_budget_exceeds_route",
            Self::InvalidStreamBudget => "invalid_stream_budget",
            Self::ContinuationOnStatelessRoute => "continuation_on_stateless_route",
        }
    }
}
