//! Small real-use extensions. They are host-neutral examples, not built-in policy.
use crate::*;
use musteragent_identity::{InvocationId, TenantId};
use musteragent_loop::AgentLoopExit;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{CanonicalModelRequest, ModelStreamEvent, ModelText, ModelUsage};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

/// Plan mode paired with `/plan on|off`; state is restored from its scoped directory.
#[derive(Default)]
pub struct PlanMode {
    enabled: Mutex<BTreeSet<(TenantId, String)>>,
}
impl KernelExtension for PlanMode {
    fn id(&self) -> &str {
        "plan"
    }
    fn lifecycle<'a>(
        &'a self,
        context: &'a ExtensionContext,
        event: &'a LifecycleEvent,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            if matches!(event, LifecycleEvent::AgentStart) {
                let path = api
                    .state_dir()
                    .map_err(|e| HookError::new("plan_state", e.to_string()))?
                    .join("enabled");
                let value = match std::fs::read_to_string(path) {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => "off".into(),
                    Err(error) => return Err(HookError::new("plan_state", error.to_string())),
                };
                let mut enabled = self
                    .enabled
                    .lock()
                    .map_err(|_| HookError::new("state_poisoned", "plan mode state unavailable"))?;
                let key = (context.run.tenant_id, context.session_key.clone());
                match value.as_str() {
                    "on" => {
                        enabled.insert(key);
                    }
                    "off" => {
                        enabled.remove(&key);
                    }
                    _ => return Err(HookError::new("plan_state", "invalid persisted mode")),
                }
            }
            Ok(())
        })
    }
    fn prompt_build(
        &self,
        context: &ExtensionContext,
        _: &CanonicalModelRequest,
    ) -> Result<PromptContribution, HookError> {
        let enabled = self
            .enabled
            .lock()
            .map_err(|_| HookError::new("state_poisoned", "plan mode state unavailable"))?;
        if enabled.contains(&(context.run.tenant_id, context.session_key.clone())) {
            Ok(PromptContribution {
                append: vec![
                    ModelText::new(
                        "Plan mode: explain the implementation plan before executing changes.",
                    )
                    .map_err(|e| HookError::new("prompt", e.to_string()))?,
                ],
                ..Default::default()
            })
        } else {
            Ok(Default::default())
        }
    }
}
impl ExtensionCommand for PlanMode {
    fn execute<'a>(
        &'a self,
        context: &'a ExtensionContext,
        request: CommandRequest,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<CommandResponse, HookError>> {
        Box::pin(async move {
            let mut enabled = self
                .enabled
                .lock()
                .map_err(|_| HookError::new("state_poisoned", "plan mode state unavailable"))?;
            let key = (context.run.tenant_id, context.session_key.clone());
            if !matches!(request.arguments.as_str(), "on" | "off") {
                return Err(HookError::new("invalid_plan_mode", "expected on or off"));
            }
            let directory = api
                .state_dir()
                .map_err(|e| HookError::new("plan_state", e.to_string()))?;
            let temporary = directory.join(format!("enabled-{}.tmp", context.attempt.attempt_id));
            std::fs::write(&temporary, &request.arguments)
                .and_then(|_| std::fs::rename(&temporary, directory.join("enabled")))
                .map_err(|e| HookError::new("plan_state", e.to_string()))?;
            if request.arguments == "on" {
                enabled.insert(key);
            } else {
                enabled.remove(&key);
            }
            Ok(CommandResponse {
                message: format!("Plan mode is {}.", request.arguments),
            })
        })
    }
}
/// User profile ingress contributes a distinct system section.
pub struct UserProfileIngress {
    /// Trusted host profile text.
    pub profile: String,
}
impl KernelExtension for UserProfileIngress {
    fn id(&self) -> &str {
        "user-profile"
    }
    fn prompt_build(
        &self,
        _: &ExtensionContext,
        _: &CanonicalModelRequest,
    ) -> Result<PromptContribution, HookError> {
        Ok(PromptContribution {
            sections: vec![(
                "user-profile".into(),
                ModelText::new(self.profile.clone())
                    .map_err(|e| HookError::new("profile", e.to_string()))?,
            )],
            ..Default::default()
        })
    }
}
/// Billing observation example: retain latest physical usage per invocation.
/// Settlement/pricing belongs to the downstream host, not the kernel.
#[derive(Default)]
pub struct RuntimeBilling {
    usage: Mutex<BTreeMap<InvocationId, ModelUsage>>,
}
impl RuntimeBilling {
    /// Latest observed usage keyed by stable physical invocation identity.
    pub fn usage(&self) -> Result<BTreeMap<InvocationId, ModelUsage>, HookError> {
        self.usage
            .lock()
            .map(|v| v.clone())
            .map_err(|_| HookError::new("billing_state", "usage state unavailable"))
    }
}
impl KernelExtension for RuntimeBilling {
    fn id(&self) -> &str {
        "runtime-billing"
    }
    fn llm_output(
        &self,
        _: &ExtensionContext,
        id: InvocationId,
        event: &ModelStreamEvent,
    ) -> Result<(), HookError> {
        if let ModelStreamEvent::Usage(usage) = event {
            self.usage
                .lock()
                .map_err(|_| HookError::new("billing_state", "usage state unavailable"))?
                .insert(id, *usage);
        }
        Ok(())
    }
}
/// On successful agent completion, ask the registered memory owner to extract
/// durable memories. The owner resolves the run's original records itself.
pub struct MemoryExtraction;
impl KernelExtension for MemoryExtraction {
    fn id(&self) -> &str {
        "memory-extraction"
    }
    fn lifecycle<'a>(
        &'a self,
        context: &'a ExtensionContext,
        event: &'a LifecycleEvent,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            if matches!(
                event,
                LifecycleEvent::AgentEnd(AgentLoopExit::Completed { .. })
            ) {
                api.memory(MemoryRequest {
                    operation: "extract".into(),
                    arguments: serde_json::json!({"run_id":context.run.run_id.to_string()}),
                })
                .await
                .map_err(|e| HookError::new("memory_extraction", e.to_string()))?;
            }
            Ok(())
        })
    }
}
