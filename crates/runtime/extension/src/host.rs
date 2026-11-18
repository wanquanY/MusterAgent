//! Composition root and lifecycle/host entry points.
use crate::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::TenantId;
use musteragent_loop::{AgentLoopExit, DurableAgentLoop, InvocationSupervisor};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalModelRequest, CanonicalRole, ModelText,
};
use musteragent_telemetry::AttemptTelemetryContext;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;

/// Frozen composition. Use one instance for a host's registered extension set.
pub struct ExtensionHost {
    pub(crate) registry: ExtensionRegistry,
    pub(crate) journal: Arc<dyn ExtensionJournal>,
    pub(crate) ports: RuntimePorts,
    pub(crate) detection: LoopDetectionConfig,
    pub(crate) calls: crate::history::CallLanes,
    services: Mutex<BTreeMap<(TenantId, String), BTreeSet<String>>>,
}
impl ExtensionHost {
    /// Whether this composition has lifecycle subscribers.
    pub fn has_extensions(&self) -> bool {
        !self.registry.extensions.is_empty()
    }

    /// Freeze registration and validate loop thresholds before any execution.
    pub fn new(
        registry: ExtensionRegistry,
        journal: Arc<dyn ExtensionJournal>,
        ports: RuntimePorts,
        detection: LoopDetectionConfig,
    ) -> Result<Arc<Self>, ExtensionError> {
        detection.validate()?;
        Ok(Arc::new(Self {
            registry,
            journal,
            ports,
            detection,
            calls: Default::default(),
            services: Mutex::new(BTreeMap::new()),
        }))
    }
    /// Assemble both decorators around the original kernel ports. This ensures
    /// lifecycle, context projection and provider observation use the same scope.
    pub fn compose_loop(
        self: &Arc<Self>,
        context: ExtensionContext,
        ports: ExtensionLoopPorts,
    ) -> Result<ExtendedAgentLoop, ExtensionError> {
        self.validate_context(&context)?;
        let policy = Arc::new(ExtensionLoopPolicy::new(
            ports.policy,
            self.clone(),
            context.clone(),
        )?);
        let model = Arc::new(ExtendedModelProvider::new(
            ports.model,
            self.clone(),
            context.clone(),
        )?);
        let core = DurableAgentLoop::new(ports.clock, policy, model, ports.journal)
            .with_execution_supervision(ports.supervision);
        Ok(ExtendedAgentLoop {
            core,
            host: self.clone(),
            context,
        })
    }
    /// Runtime API bound to a registered extension and a host-verified context.
    pub fn api(
        self: &Arc<Self>,
        context: &ExtensionContext,
        extension: &str,
    ) -> Result<RuntimeApi, ExtensionError> {
        self.validate_context(context)?;
        if !self.registry.configs.contains_key(extension) {
            return Err(ExtensionError::Unavailable(format!(
                "extension:{extension}"
            )));
        }
        Ok(RuntimeApi {
            host: self.clone(),
            context: context.clone(),
            extension: extension.into(),
        })
    }
    pub(crate) fn internal_api(
        self: &Arc<Self>,
        context: &ExtensionContext,
        extension: &str,
    ) -> RuntimeApi {
        RuntimeApi {
            host: self.clone(),
            context: context.clone(),
            extension: extension.into(),
        }
    }
    pub(crate) fn validate_context(
        &self,
        context: &ExtensionContext,
    ) -> Result<(), ExtensionError> {
        if context.run.run_id != context.attempt.run_id
            || context.session_key.is_empty()
            || !context.workspace_dir.is_absolute()
        {
            return Err(ExtensionError::IdentityMismatch);
        }
        Ok(())
    }
    pub(crate) async fn required<T>(
        &self,
        context: &ExtensionContext,
        key: &str,
        result: Result<T, HookFailure>,
    ) -> Result<T, ExtensionError> {
        match result {
            Ok(value) => Ok(value),
            Err(failure) => {
                self.journal
                    .append(
                        context,
                        key,
                        ExtensionFact::HookFailed {
                            failure: failure.clone(),
                            continues: false,
                        },
                    )
                    .await?;
                Err(failure.into())
            }
        }
    }
    pub(crate) async fn observe(
        &self,
        context: &ExtensionContext,
        key: &str,
        result: Result<(), HookFailure>,
    ) -> Result<Option<HookFailure>, ExtensionError> {
        match result {
            Ok(()) => Ok(None),
            Err(failure) => {
                self.journal
                    .append(
                        context,
                        key,
                        ExtensionFact::HookFailed {
                            failure: failure.clone(),
                            continues: true,
                        },
                    )
                    .await?;
                Ok(Some(failure))
            }
        }
    }
    /// Notify lifecycle hooks in registration order. Start is fail-fast;
    /// terminal/child observers all run, with typed failures returned and recorded.
    pub async fn lifecycle(
        self: &Arc<Self>,
        context: &ExtensionContext,
        event: LifecycleEvent,
    ) -> Result<Vec<HookFailure>, ExtensionError> {
        self.validate_context(context)?;
        let hook = event.hook();
        let child = match &event {
            LifecycleEvent::SubagentSpawned(id)
            | LifecycleEvent::SubagentEnded { run_id: id, .. } => Some(*id),
            _ => None,
        };
        let event_identity = match &event {
            LifecycleEvent::AgentEnd(exit) => format!("{exit:?}"),
            LifecycleEvent::SubagentEnded { report, .. } => report.to_string(),
            _ => String::new(),
        };
        let event_digest = Sha256::digest(event_identity).to_string();
        let mut failures = Vec::new();
        for extension in &self.registry.extensions {
            let key = format!(
                "lifecycle:{hook:?}:{}:{child:?}:{event_digest}",
                extension.id()
            );
            let api = self.internal_api(context, extension.id());
            let result = guard_async(extension.id(), hook, || {
                extension.lifecycle(context, &event, &api)
            })
            .await;
            if hook == Hook::AgentStart {
                self.required(context, &format!("{key}:failed"), result)
                    .await?;
            } else if let Some(failure) = self
                .observe(context, &format!("{key}:failed"), result)
                .await?
            {
                failures.push(failure);
                continue;
            }
            self.journal
                .append(
                    context,
                    &key,
                    ExtensionFact::Lifecycle {
                        extension: extension.id().into(),
                        hook,
                        child_run_id: child,
                    },
                )
                .await?;
        }
        Ok(failures)
    }
    /// Start services in registration order, once per tenant/session. If a start
    /// fails, stop all previously started services before surfacing the failure.
    pub async fn start_services(
        self: &Arc<Self>,
        context: &ExtensionContext,
    ) -> Result<(), ExtensionError> {
        self.validate_context(context)?;
        let mut states = self.services.lock().await;
        let started = states
            .entry((context.run.tenant_id, context.session_key.clone()))
            .or_default();
        for (name, service) in &self.registry.services {
            if started.contains(name) {
                continue;
            }
            let api = self.internal_api(context, &service.owner);
            let result = guard_async(name, Hook::ServiceStart, || {
                service.handler.start(context, &api)
            })
            .await;
            let result = match self
                .required(context, &format!("service:start:{name}:failed"), result)
                .await
            {
                Ok(()) => {
                    started.insert(name.clone());
                    self.journal
                        .append(
                            context,
                            &format!("service:start:{name}"),
                            ExtensionFact::Service {
                                service: name.clone(),
                                hook: Hook::ServiceStart,
                            },
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                // Cleanup must continue even when the audit port is unavailable.
                let cleanup = self.stop_started(context, started, "rollback").await;
                return Err(cleanup.err().unwrap_or(error));
            }
        }
        Ok(())
    }
    /// Stop and join services in registration order. Failed stops remain owned
    /// and can be retried; all failures are returned and persisted.
    pub async fn stop_services(
        self: &Arc<Self>,
        context: &ExtensionContext,
    ) -> Result<Vec<HookFailure>, ExtensionError> {
        self.validate_context(context)?;
        let mut states = self.services.lock().await;
        let started = states
            .entry((context.run.tenant_id, context.session_key.clone()))
            .or_default();
        self.stop_started(context, started, "stop").await
    }
    async fn stop_started(
        self: &Arc<Self>,
        context: &ExtensionContext,
        started: &mut BTreeSet<String>,
        phase: &str,
    ) -> Result<Vec<HookFailure>, ExtensionError> {
        let mut failures = Vec::new();
        let mut audit_failure = None;
        for (name, service) in &self.registry.services {
            if !started.contains(name) {
                continue;
            }
            let api = self.internal_api(context, &service.owner);
            let result = guard_async(name, Hook::ServiceStop, || {
                service.handler.stop(context, &api)
            })
            .await;
            let stopped = result.is_ok();
            match self
                .observe(context, &format!("service:{phase}:{name}:failed"), result)
                .await
            {
                Ok(Some(failure)) => failures.push(failure),
                Ok(None) => {}
                Err(error) => {
                    audit_failure.get_or_insert(error);
                }
            }
            if stopped {
                started.remove(name);
                if let Err(error) = self
                    .journal
                    .append(
                        context,
                        &format!("service:{phase}:{name}"),
                        ExtensionFact::Service {
                            service: name.clone(),
                            hook: Hook::ServiceStop,
                        },
                    )
                    .await
                {
                    audit_failure.get_or_insert(error);
                }
            }
        }
        match audit_failure {
            Some(error) => Err(error),
            None => Ok(failures),
        }
    }
    /// Dispatch a typed host method through its serialized boundary.
    pub async fn gateway(
        self: &Arc<Self>,
        context: &ExtensionContext,
        request: GatewayRequest,
    ) -> Result<GatewayResponse, ExtensionError> {
        self.validate_context(context)?;
        let method = self
            .registry
            .methods
            .get(&request.method)
            .ok_or_else(|| ExtensionError::Unavailable(format!("method:{}", request.method)))?;
        let api = self.internal_api(context, &method.owner);
        let key = format!(
            "gateway:{}:{}",
            request.method,
            Sha256::digest(request.payload.to_string())
        );
        let result = guard_async(&request.method, Hook::GatewayMethod, || {
            method.handler.call(context, request.payload, &api)
        })
        .await;
        self.required(context, &key, result)
            .await
            .map(|payload| GatewayResponse { payload })
    }
    /// Observe one committed outbox item. The caller owns the durable subscription
    /// cursor and acknowledges only after success. Extension consumers must be
    /// idempotent by message ID, since delivery is at least once.
    pub async fn consume_outbox(
        &self,
        context: &ExtensionContext,
        message: &musteragent_ledger::CommittedOutbox,
    ) -> Result<Vec<HookFailure>, ExtensionError> {
        self.validate_context(context)?;
        if message.tenant_id != context.run.tenant_id
            || message.conversation_id != context.run.conversation_id
        {
            return Err(ExtensionError::IdentityMismatch);
        }
        // Avoid an observer-failure -> extension fact -> observer-failure cycle.
        if message.message.topic.as_str() == EXTENSION_OUTBOX_TOPIC {
            return Ok(Vec::new());
        }
        let mut failures = Vec::new();
        for extension in &self.registry.extensions {
            let key = format!("outbox:{}:{}", message.message.message_id, extension.id());
            if let Some(failure) = self
                .observe(
                    context,
                    &key,
                    guard(extension.id(), Hook::Outbox, || {
                        extension.outbox(context, message)
                    }),
                )
                .await?
            {
                failures.push(failure);
            }
        }
        Ok(failures)
    }
    /// Parse and execute a leading registered slash command. Unknown commands
    /// fail explicitly. `None` means ordinary user input.
    pub async fn command(
        self: &Arc<Self>,
        context: &ExtensionContext,
        input: &CommandInput,
    ) -> Result<Option<CommandResponse>, ExtensionError> {
        self.validate_context(context)?;
        valid_id(&input.id)?;
        let text = input.text.as_str();
        let Some(rest) = text.trim_start().strip_prefix('/') else {
            return Ok(None);
        };
        let (name, args) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
        let command = self
            .registry
            .commands
            .get(name)
            .ok_or_else(|| ExtensionError::Unavailable(format!("command:{name}")))?;
        let key = format!("command:{}", input.id);
        for record in self
            .journal
            .select(context, FactQuery::Key(key.clone()))
            .await?
        {
            if record.key == key
                && let ExtensionFact::CommandHandled {
                    response,
                    input_digest,
                    ..
                } = record.fact
            {
                if input_digest != Sha256::digest(text).to_string() {
                    return Err(ExtensionError::IdentityMismatch);
                }
                return Ok(Some(CommandResponse { message: response }));
            }
        }
        let api = self.internal_api(context, &command.owner);
        let result = guard_async(&command.owner, Hook::Command, || {
            command.handler.execute(
                context,
                CommandRequest {
                    input_id: input.id.clone(),
                    arguments: args.trim().into(),
                },
                &api,
            )
        })
        .await;
        let response = self
            .required(context, &format!("{key}:failed"), result)
            .await?;
        self.journal
            .append(
                context,
                &key,
                ExtensionFact::CommandHandled {
                    command: name.into(),
                    input_digest: Sha256::digest(text).to_string(),
                    response: response.message.clone(),
                },
            )
            .await?;
        Ok(Some(response))
    }
    pub(crate) async fn project(
        self: &Arc<Self>,
        context: &ExtensionContext,
        request: &mut CanonicalModelRequest,
    ) -> Result<(), ExtensionError> {
        let invocation = request.invocation.invocation_id.to_string();
        let original_view = (request.instructions.clone(), request.input.clone());
        // Only the host-admitted current input may execute a command. Historical
        // slash text remains ordinary context, even when it names a registration.
        if let Some(input) = &context.ingress {
            let receipt = self
                .journal
                .select(context, FactQuery::Key(format!("command:{}", input.id)))
                .await?
                .into_iter()
                .find_map(|record| {
                    if record.key == format!("command:{}", input.id)
                        && let ExtensionFact::CommandHandled { response, .. } = record.fact
                    {
                        return Some(response);
                    }
                    None
                });
            if let Some(response) = receipt
                && let Some(message) = request.input.iter_mut().rev().find(|message| {
                    message.role == CanonicalRole::User
                        && message
                            .content
                            .iter()
                            .map(|p| match p {
                                CanonicalContentPart::Text(t) => Some(t.as_str()),
                                _ => None,
                            })
                            .collect::<Option<Vec<_>>>()
                            .is_some_and(|parts| parts.join("") == input.text.as_str())
                })
            {
                message.content = vec![CanonicalContentPart::Text(
                    ModelText::new(response).map_err(|e| ExtensionError::Config(e.to_string()))?,
                )];
            }
        }
        // Result transformations always start from original planner copies and
        // compose in order. The durable ToolResultCommit remains unchanged.
        for (index, message) in request.input.iter_mut().enumerate() {
            if message.role != CanonicalRole::Tool {
                continue;
            }
            let identity = (
                message.role,
                message.tool_call_id.clone(),
                message.tool_name.clone(),
            );
            for extension in &self.registry.extensions {
                let key = format!("view:{invocation}:tool:{index}:{}", extension.id());
                let result = guard(extension.id(), Hook::ToolResultPersist, || {
                    let projected = extension.tool_result_persist(context, message.clone())?;
                    if (
                        projected.role,
                        projected.tool_call_id.clone(),
                        projected.tool_name.clone(),
                    ) != identity
                        || !projected.tool_calls.is_empty()
                    {
                        return Err(HookError::new(
                            "invalid_result_identity",
                            "a result view cannot change tool identity",
                        ));
                    }
                    Ok(projected)
                });
                let projected = self.required(context, &key, result).await?;
                *message = projected;
            }
        }
        let mut view = ContextView {
            instructions: request.instructions.clone(),
            input: request.input.clone(),
        };
        for extension in &self.registry.extensions {
            let result = guard(extension.id(), Hook::ContextProjection, || {
                let view = extension.context_projection(context, view)?;
                let mut candidate = request.clone();
                candidate.instructions = view.instructions.clone();
                candidate.input = view.input.clone();
                candidate
                    .validate()
                    .map_err(|e| HookError::new("invalid_context_projection", e.to_string()))?;
                Ok(view)
            });
            view = self
                .required(
                    context,
                    &format!("view:{invocation}:{}", extension.id()),
                    result,
                )
                .await?;
        }
        request.instructions = view.instructions;
        request.input = view.input;
        let mut prepend = Vec::new();
        let mut append = Vec::new();
        for extension in &self.registry.extensions {
            let result = guard(extension.id(), Hook::PromptBuild, || {
                let contribution = extension.prompt_build(context, request)?;
                for (name, _) in &contribution.sections {
                    valid_id(name)
                        .map_err(|e| HookError::new("invalid_prompt_section", e.to_string()))?;
                }
                Ok(contribution)
            });
            let contribution = self
                .required(
                    context,
                    &format!("prompt:{invocation}:{}", extension.id()),
                    result,
                )
                .await?;
            prepend.extend(contribution.prepend);
            append.extend(contribution.append);
            for (name, text) in contribution.sections {
                append.push(
                    ModelText::new(format!("[{name}]\n{}", text.as_str()))
                        .map_err(|e| ExtensionError::Config(e.to_string()))?,
                );
            }
        }
        prepend.append(&mut request.instructions);
        prepend.append(&mut append);
        request.instructions = prepend;
        let facts = self.journal.select(context, FactQuery::LoopState).await?;
        for record in facts.iter().rev().filter(|f|matches!(&f.fact,ExtensionFact::LoopDetected { detection } if detection.level==LoopLevel::Warning)).take(3).collect::<Vec<_>>().into_iter().rev() {
            if let ExtensionFact::LoopDetected { detection }=&record.fact {
                request.instructions.push(ModelText::new(format!("Tool loop warning: {:?} reached {} calls. Change strategy or explain why progress is blocked.",detection.pattern,detection.count)).map_err(|e|ExtensionError::Config(e.to_string()))?);
            }
        }
        // Opaque provider state may contain the unprojected view. A fresh request
        // must carry the complete projected input instead of reusing that state.
        request.continuation_ref = None;
        request
            .validate()
            .map_err(|e| ExtensionError::Config(e.to_string()))?;
        if original_view.0 == request.instructions && original_view.1 == request.input {
            return Ok(());
        }
        self.journal.append(context,&format!("context:{invocation}"),ExtensionFact::ContextProjected {
            invocation_id:invocation,
            digest:Sha256::digest(serde_json::json!({"instructions":request.instructions.iter().map(ModelText::as_str).collect::<Vec<_>>(),"messages":request.input.iter().map(message_value).collect::<Vec<_>>()}).to_string()).to_string(),
            instruction_count:request.instructions.len(),message_count:request.input.len()
        }).await
    }
}
/// Result preserves the original durable exit and separately surfaces observer
/// failures. A billing/extraction error cannot rewrite a completed run as failed.
#[derive(Debug)]
pub struct ExtensionRunReport {
    /// Original kernel exit.
    pub exit: AgentLoopExit,
    /// Continuing observer errors/panics, also in the append-only journal.
    pub observer_failures: Vec<HookFailure>,
}
/// Original kernel ports to compose. Persistence, authority and supervision
/// remain supplied by their existing owners.
pub struct ExtensionLoopPorts {
    /// Wall clock.
    pub clock: Arc<dyn musteragent_loop::AgentLoopClock>,
    /// Existing host policy (normally DefaultLoopPolicy).
    pub policy: Arc<dyn musteragent_loop::LoopPolicy>,
    /// Original model provider.
    pub model: Arc<dyn musteragent_runtime_model::ModelProvider>,
    /// Original fenced loop journal.
    pub journal: Arc<dyn musteragent_loop::AgentLoopJournal>,
    /// Original physical execution supervisor.
    pub supervision: musteragent_loop::ExecutionSupervision,
}
/// Entrypoint that guarantees agent_end after every ordinary loop exit.
/// The host must keep this future polled through cancellation, as with the kernel.
pub struct ExtendedAgentLoop {
    core: DurableAgentLoop,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
}
impl ExtendedAgentLoop {
    /// Bound physical tool concurrency without weakening a sequential policy.
    pub fn with_tool_concurrency_limit(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.core = self.core.with_tool_concurrency_limit(limit);
        self
    }
    /// Execute the real durable loop, then notify terminal observers.
    pub async fn execute(
        &self,
        telemetry: AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> Result<ExtensionRunReport, ExtensionError> {
        let exit = self
            .core
            .execute(self.context.attempt.clone(), telemetry, supervisor)
            .await;
        self.host
            .lifecycle(&self.context, LifecycleEvent::AgentEnd(exit.clone()))
            .await?;
        let observer_failures = self
            .host
            .journal
            .read(&self.context)
            .await?
            .into_iter()
            .filter_map(|record| {
                if let ExtensionFact::HookFailed {
                    failure,
                    continues: true,
                } = record.fact
                {
                    Some(failure)
                } else {
                    None
                }
            })
            .collect();
        Ok(ExtensionRunReport {
            exit,
            observer_failures,
        })
    }
}
