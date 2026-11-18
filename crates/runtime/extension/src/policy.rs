//! LoopPolicy decorator: owns ordered intervention facts, views and loop detection.
use crate::*;
use musteragent_context::{ContextPlanError, ContextPlanInput, ContextPlanProfile, ModelIteration};
use musteragent_identity::{InteractionId, ToolCallId, VersionedKey};
use musteragent_loop::*;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_model::JsonValue;
use musteragent_telemetry::AttemptTelemetryContext;
use musteragent_tool::WireName;
use std::sync::Arc;

/// Decorates an existing policy; never replaces its grants, gate, execution mode,
/// cancellation ownership or recovery policy. Bind one instance per attempt.
pub struct ExtensionLoopPolicy {
    inner: Arc<dyn LoopPolicy>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
}
impl ExtensionLoopPolicy {
    /// Bind extension composition to the same attempt as the durable loop.
    pub fn new(
        inner: Arc<dyn LoopPolicy>,
        host: Arc<ExtensionHost>,
        context: ExtensionContext,
    ) -> Result<Self, ExtensionError> {
        host.validate_context(&context)?;
        Ok(Self {
            inner,
            host,
            context,
        })
    }
    fn check(&self, attempt: &AttemptContext) -> Result<(), LoopPortError> {
        if attempt != &self.context.attempt {
            Err(port_error(ExtensionError::IdentityMismatch))
        } else {
            Ok(())
        }
    }
    async fn call_facts(
        &self,
        call: &PlannedToolCall,
    ) -> Result<Vec<ExtensionFactRecord>, ExtensionError> {
        self.host
            .journal
            .select(&self.context, FactQuery::Call(call.call_id.as_str().into()))
            .await
    }
    async fn replay(
        &self,
        call: &mut PlannedToolCall,
        facts: &[ExtensionFactRecord],
    ) -> Result<(), ExtensionError> {
        for record in facts {
            if let ExtensionFact::ArgumentsRewritten {
                before,
                after,
                content,
                ..
            } = &record.fact
            {
                if PayloadDigest::of(&canonical_arguments(&call.arguments))? != *before {
                    return Err(ExtensionError::IdentityMismatch);
                }
                call.arguments = json_value(
                    &content
                        .read::<serde_json::Value>(
                            self.host.ports.content.as_ref(),
                            &self.context,
                            after,
                        )
                        .await?,
                )?;
            }
        }
        Ok(())
    }
    async fn stopped(&self) -> Result<bool, ExtensionError> {
        Ok(self.host.journal.select(&self.context, FactQuery::LoopState).await?.iter().any(|f| matches!(&f.fact, ExtensionFact::LoopDetected { detection } if detection.level != LoopLevel::Warning)))
    }
    async fn prepare(&self, mut call: PlannedToolCall) -> Result<PlannedToolCall, ExtensionError> {
        let facts = self.call_facts(&call).await?;
        if facts
            .iter()
            .any(|f| matches!(&f.fact, ExtensionFact::ToolDenied { .. }))
        {
            return Err(ExtensionError::Unavailable("tool_denied".into()));
        }
        if self.stopped().await? {
            return Err(ExtensionError::Unavailable("tool_loop_stopped".into()));
        }
        if self
            .host
            .ports
            .history
            .requested(&self.context, call.call_id.as_str())
            .await?
        {
            self.replay(&mut call, &facts).await?;
            return Ok(call);
        }
        let mut denied = false;
        for extension in &self.host.registry.extensions {
            let key = format!("tool:{}:{}", call.call_id.as_str(), extension.id());
            if let Some(record) = facts
                .iter()
                .find(|record| record.key == format!("{key}:rewrite"))
            {
                self.replay(&mut call, std::slice::from_ref(record)).await?;
                continue;
            }
            if let Some(ExtensionFactRecord {
                fact: ExtensionFact::HookFailed { failure, .. },
                ..
            }) = self
                .host
                .journal
                .select(&self.context, FactQuery::Key(format!("{key}:failed")))
                .await?
                .first()
            {
                return Err(failure.clone().into());
            }
            let decision = guard(extension.id(), Hook::BeforeToolCall, || {
                let decision = extension.before_tool_call(&self.context, &call)?;
                if let ToolDecision::Rewrite(arguments) = &decision {
                    arguments.validate().map_err(|e| {
                        HookError::new("invalid_rewritten_arguments", e.to_string())
                    })?;
                }
                Ok(decision)
            });
            match self
                .host
                .required(&self.context, &format!("{key}:failed"), decision)
                .await?
            {
                ToolDecision::Allow => {}
                ToolDecision::Deny(reason) => {
                    denied = true;
                    self.host
                        .journal
                        .append(
                            &self.context,
                            &format!("{key}:denied"),
                            ExtensionFact::ToolDenied {
                                extension: extension.id().into(),
                                call_id: call.call_id.as_str().into(),
                                reason,
                            },
                        )
                        .await?;
                }
                ToolDecision::Rewrite(arguments) => {
                    if arguments != call.arguments {
                        let before = PayloadDigest::of(&canonical_arguments(&call.arguments))?;
                        let (after, content) = RewriteContent::store(
                            self.host.ports.content.as_ref(),
                            &self.context,
                            &key,
                            &canonical_arguments(&arguments),
                        )
                        .await?;
                        self.host
                            .journal
                            .append(
                                &self.context,
                                &format!("{key}:rewrite"),
                                ExtensionFact::ArgumentsRewritten {
                                    extension: extension.id().into(),
                                    call_id: call.call_id.as_str().into(),
                                    before,
                                    after,
                                    content,
                                },
                            )
                            .await?;
                    }
                    call.arguments = arguments;
                }
            }
        }
        if denied {
            return Err(ExtensionError::Unavailable("tool_denied".into()));
        }
        self.detect(Some(&call), true).await?;
        Ok(call)
    }
    async fn detect(
        &self,
        current: Option<&PlannedToolCall>,
        enforce: bool,
    ) -> Result<(), ExtensionError> {
        let mut history = self
            .host
            .ports
            .history
            .recent(
                &self.context,
                current.map(|c| c.call_id.as_str()),
                self.host.detection.history,
                &self.host.detection.poll_tools,
            )
            .await?;
        let facts = self
            .host
            .journal
            .select(
                &self.context,
                FactQuery::Calls(
                    history
                        .iter()
                        .map(|entry| entry.call.call_id.clone())
                        .collect(),
                ),
            )
            .await?;
        // Only the final replacement is needed for each historical invocation.
        let mut replacements = std::collections::BTreeMap::new();
        for fact in facts {
            if let ExtensionFact::ArgumentsRewritten {
                call_id,
                after,
                content,
                ..
            } = fact.fact
            {
                replacements.insert(call_id, (after, content));
            }
        }
        for entry in &mut history {
            if let Some((after, content)) = replacements.get(&entry.call.call_id) {
                entry.call.arguments = content
                    .read(self.host.ports.content.as_ref(), &self.context, after)
                    .await?;
            }
        }
        if let Some(call) = current {
            if let Some(entry) = history
                .iter_mut()
                .find(|entry| entry.call.call_id == call.call_id.as_str())
            {
                entry.call.arguments = canonical_arguments(&call.arguments);
            } else {
                history.push(ToolHistoryEntry {
                    call: ToolSample {
                        call_id: call.call_id.as_str().into(),
                        tool: call.name.as_str().into(),
                        arguments: canonical_arguments(&call.arguments),
                    },
                    result: None,
                });
            }
        }
        let mut stopped = false;
        for detection in self.host.detection.detect(&history) {
            stopped |= detection.level != LoopLevel::Warning;
            let key = format!(
                "loop:{}:{:?}:{:?}:{}",
                detection.call_id, detection.pattern, detection.level, detection.count
            );
            self.host
                .journal
                .append(
                    &self.context,
                    &key,
                    ExtensionFact::LoopDetected { detection },
                )
                .await?;
        }
        if enforce && stopped {
            Err(ExtensionError::Unavailable("tool_loop_stopped".into()))
        } else {
            Ok(())
        }
    }
    async fn observed(
        &self,
        call: &PlannedToolCall,
        outcome: &ToolExecutionOutcome,
    ) -> Result<(), ExtensionError> {
        let mut effective_call = call.clone();
        self.replay(&mut effective_call, &self.call_facts(call).await?)
            .await?;
        for extension in &self.host.registry.extensions {
            self.host
                .observe(
                    &self.context,
                    &format!("after:{}:{}", call.call_id.as_str(), extension.id()),
                    guard(extension.id(), Hook::AfterToolCall, || {
                        extension.after_tool_call(&self.context, &effective_call, outcome)
                    }),
                )
                .await?;
        }
        // Only committed kernel results participate in no-progress detection.
        self.detect(None, false).await
    }
}
macro_rules! forward {
    ($name:ident($($arg:ident:$ty:ty),*)->$result:ty)=>{
        fn $name(&self,$($arg:$ty),*)->ApplicationFuture<'_,$result>{self.inner.$name($($arg),*)}
    }
}
impl LoopPolicy for ExtensionLoopPolicy {
    fn load_execution_policy(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.check(&attempt)?;
            let policy = self.inner.load_execution_policy(&attempt).await?;
            self.host
                .lifecycle(&self.context, LifecycleEvent::AgentStart)
                .await
                .map_err(port_error)?;
            if let Some(input) = &self.context.ingress {
                self.host
                    .command(&self.context, input)
                    .await
                    .map_err(port_error)?;
            }
            Ok(policy)
        })
    }
    fn gate(
        &self,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<GateVerdict, LoopPortError>> {
        let context = context.clone();
        Box::pin(async move {
            self.check(&context.attempt)?;
            let base = self.inner.gate(&context).await?;
            if self.stopped().await.map_err(port_error)? {
                return Ok(GateVerdict::TerminalFailure {
                    code: ErrorCode::PolicyDenied,
                });
            }
            Ok(base)
        })
    }
    fn transform_context(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        self.transform_context_with_projection(
            input,
            None,
            Arc::new(ExtensionProjection {
                host: self.host.clone(),
                context: self.context.clone(),
            }),
        )
    }
    fn transform_context_with_profile(
        &self,
        input: ContextPlanInput,
        profile: ContextPlanProfile,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        self.transform_context_with_projection(
            input,
            Some(profile),
            Arc::new(ExtensionProjection {
                host: self.host.clone(),
                context: self.context.clone(),
            }),
        )
    }
    fn transform_context_with_projection(
        &self,
        input: ContextPlanInput,
        profile: Option<ContextPlanProfile>,
        projection: Arc<dyn musteragent_context::ContextViewProjection>,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        Box::pin(async move {
            self.check(&input.attempt)
                .map_err(|_| ContextPlanError::invariant())?;
            match &self.host.registry.context {
                Some(engine) => {
                    let id = format!("engine:{}:{}", input.next_iteration, engine.owner);
                    let result = guard_async(&engine.owner, Hook::ContextProjection, || {
                        Box::pin(async {
                            engine
                                .handler
                                .prepare_model_iteration_with_projection(input, profile, projection)
                                .await
                                .map_err(|error| {
                                    HookError::new(error.code.as_str(), error.to_string())
                                })
                        })
                    })
                    .await;
                    self.host
                        .required(&self.context, &id, result)
                        .await
                        .map_err(context_error)
                }
                None => {
                    self.inner
                        .transform_context_with_projection(input, profile, projection)
                        .await
                }
            }
        })
    }
    fn execute_tool(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let attempt = attempt.clone();
        let telemetry = telemetry.clone();
        Box::pin(async move {
            self.check(&attempt)?;
            let lane = self
                .host
                .calls
                .lane(&self.context, call.call_id.as_str())
                .map_err(port_error)?;
            // Keep duplicate callers behind the original executor's admission.
            // A passive hook has no extension receipt to close this window.
            let _guard = lane.lock().await;
            let call = self.prepare(call).await.map_err(port_error)?;
            self.inner.execute_tool(&attempt, &telemetry, call).await
        })
    }
    fn execute_activity_tool<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        telemetry: &'a AttemptTelemetryContext,
        supervisor: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'a, Result<Result<ToolExecutionOutcome, LoopPortError>, InvocationAborted>>
    {
        Box::pin(async move {
            if let Err(error) = self.check(attempt) {
                return Ok(Err(error));
            }
            let lane = match self.host.calls.lane(&self.context, call.call_id.as_str()) {
                Ok(lane) => lane,
                Err(error) => return Ok(Err(port_error(error))),
            };
            let _guard = lane.lock().await;
            let call = match self.prepare(call).await {
                Ok(call) => call,
                Err(error) => return Ok(Err(port_error(error))),
            };
            self.inner
                .execute_activity_tool(attempt, telemetry, supervisor, call)
                .await
        })
    }
    fn after_tool(
        &self,
        call: &PlannedToolCall,
        outcome: ToolExecutionOutcome,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let call = call.clone();
        let context = context.clone();
        Box::pin(async move {
            if !matches!(outcome, ToolExecutionOutcome::Completed { .. }) {
                self.observed(&call, &outcome).await.map_err(port_error)?;
            }
            self.inner.after_tool(&call, outcome, &context).await
        })
    }
    fn tool_result_committed(
        &self,
        call: &PlannedToolCall,
        outcome: &ToolExecutionOutcome,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        let call = call.clone();
        let outcome = outcome.clone();
        let context = context.clone();
        Box::pin(async move {
            self.inner
                .tool_result_committed(&call, &outcome, &context)
                .await?;
            self.observed(&call, &outcome).await.map_err(port_error)?;
            if self.stopped().await.map_err(port_error)? {
                Err(port_error(ExtensionError::Unavailable(
                    "tool_loop_stopped".into(),
                )))
            } else {
                Ok(())
            }
        })
    }
    forward!(context_compaction_policy_ref(attempt:&AttemptContext)->Result<Option<VersionedKey>,LoopPortError>);
    forward!(before_tool(call:&PlannedToolCall,context:&StepContext)->Result<ToolGate,LoopPortError>);
    forward!(request_tool_approval(attempt:&AttemptContext,request:musteragent_approval::ApprovalRequest)->Result<InteractionId,LoopPortError>);
    forward!(should_stop(turn:TurnSummary,context:&StepContext)->Result<bool,LoopPortError>);
    forward!(prepare_next_turn(turn:TurnSummary,context:&StepContext)->Result<NextTurnDecision,LoopPortError>);
    forward!(fallback_after_upstream_failure(context:&StepContext)->Result<FallbackDecision,LoopPortError>);
    forward!(fallback_after_model_failure(failure:ModelFailure,context:&StepContext)->Result<FallbackDecision,LoopPortError>);
    forward!(drain_steering(attempt:&AttemptContext)->Result<u32,LoopPortError>);
    forward!(drain_follow_up(attempt:&AttemptContext)->Result<u32,LoopPortError>);
    fn preview_tool_arguments(
        &self,
        attempt: &AttemptContext,
        call: &ToolCallId,
        name: &WireName,
        json: &str,
    ) -> Option<ToolDocumentPreview> {
        self.inner.preview_tool_arguments(attempt, call, name, json)
    }
}
/// Map composition failures to the existing durable failure vocabulary. The full
/// typed reason remains in extension facts, never hidden in an opaque message.
pub(crate) fn port_error(error: ExtensionError) -> LoopPortError {
    LoopPortError {
        code: match error {
            ExtensionError::Audit(_) => ErrorCode::DependencyUnavailable,
            ExtensionError::IdentityMismatch => ErrorCode::RuntimeSnapshotInvalid,
            _ => ErrorCode::PolicyDenied,
        },
        retryable: false,
    }
}
fn context_error(error: ExtensionError) -> ContextPlanError {
    ContextPlanError {
        code: port_error(error).code,
        retryable: false,
        early_provider_cause: None,
        retry_after_ms: None,
    }
}
pub(crate) fn json_value(value: &serde_json::Value) -> Result<JsonValue, ExtensionError> {
    Ok(match value {
        serde_json::Value::Null => JsonValue::Null,
        serde_json::Value::Bool(v) => JsonValue::Bool(*v),
        serde_json::Value::Number(v) => {
            if let Some(n) = v.as_i64() {
                JsonValue::Integer(n)
            } else {
                JsonValue::Number(
                    v.as_f64()
                        .ok_or_else(|| ExtensionError::Config("non-finite number".into()))?,
                )
            }
        }
        serde_json::Value::String(v) => JsonValue::String(v.clone()),
        serde_json::Value::Array(v) => {
            JsonValue::Array(v.iter().map(json_value).collect::<Result<_, _>>()?)
        }
        serde_json::Value::Object(v) => JsonValue::Object(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), json_value(v)?)))
                .collect::<Result<_, ExtensionError>>()?,
        ),
    })
}

struct ExtensionProjection {
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
}
impl musteragent_context::ContextViewProjection for ExtensionProjection {
    fn project<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: &'a musteragent_runtime_model::CanonicalModelRequest,
        view: musteragent_context::ModelContextView,
    ) -> ApplicationFuture<'a, Result<musteragent_context::ModelContextView, ContextPlanError>>
    {
        Box::pin(async move {
            if attempt != &self.context.attempt {
                return Err(ContextPlanError::invariant());
            }
            let mut request = request.clone();
            request.instructions = view.instructions;
            request.input = view.input;
            self.host
                .project(&self.context, &mut request)
                .await
                .map_err(context_error)?;
            Ok(musteragent_context::ModelContextView {
                instructions: request.instructions,
                input: request.input,
            })
        })
    }
}
