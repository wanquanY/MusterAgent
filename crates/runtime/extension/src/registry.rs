//! Explicit registration. Duplicate names and exclusive slots fail without mutation.
use crate::*;
use musteragent_context::ContextPlanner;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::CanonicalToolDefinition;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, sync::Arc};

/// Slash command arguments (the leading slash/name have already been parsed).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandRequest {
    /// Stable host ingress identity; handlers must be idempotent by this identity.
    pub input_id: String,
    /// Remaining command text.
    pub arguments: String,
}
/// A consumed command never forwards its raw input to the model.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandResponse {
    /// Host/model-visible receipt.
    pub message: String,
}
/// A registered command, invoked before context projection and prompt hooks.
pub trait ExtensionCommand: Send + Sync {
    /// Execute in the verified runtime scope.
    fn execute<'a>(
        &'a self,
        context: &'a ExtensionContext,
        request: CommandRequest,
        api: &'a RuntimeApi,
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<CommandResponse, HookError>>;
}
/// Typed host method. This crate owns no network listener or transport.
pub trait GatewayMethod: Send + Sync + 'static {
    /// Host request schema.
    type Request: DeserializeOwned + Send;
    /// Host response schema.
    type Response: Serialize + Send;
    /// Invoke through an authorized host context.
    fn call<'a>(
        &'a self,
        context: &'a ExtensionContext,
        request: Self::Request,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<Self::Response, HookError>>;
}
/// Serialized boundary of the typed method registry.
#[derive(Debug, Clone)]
pub struct GatewayRequest {
    /// Registered method name.
    pub method: String,
    /// Typed handler deserializes this payload.
    pub payload: serde_json::Value,
}
/// Host-exposed method result; transport status mapping remains host-owned.
#[derive(Debug, Clone, PartialEq)]
pub struct GatewayResponse {
    /// Handler's serialized response.
    pub payload: serde_json::Value,
}

pub(crate) trait ErasedMethod: Send + Sync {
    fn call<'a>(
        &'a self,
        context: &'a ExtensionContext,
        request: serde_json::Value,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<serde_json::Value, HookError>>;
}
struct TypedMethod<M>(M);
impl<M: GatewayMethod> ErasedMethod for TypedMethod<M> {
    fn call<'a>(
        &'a self,
        context: &'a ExtensionContext,
        request: serde_json::Value,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<serde_json::Value, HookError>> {
        Box::pin(async move {
            let request = serde_json::from_value(request)
                .map_err(|e| HookError::new("invalid_method_request", e.to_string()))?;
            let result = self.0.call(context, request, api).await?;
            serde_json::to_value(result)
                .map_err(|e| HookError::new("invalid_method_response", e.to_string()))
        })
    }
}
/// Background service: the host owns the lifetime and must await stop.
pub trait ExtensionService: Send + Sync {
    /// Start resource ownership; errors must clean up partially allocated resources.
    fn start<'a>(
        &'a self,
        context: &'a ExtensionContext,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>>;
    /// Stop and join all resources created by start. Called in registration order.
    fn stop<'a>(
        &'a self,
        context: &'a ExtensionContext,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>>;
}
/// Frozen tool metadata and implementation. The host admits these definitions
/// into its snapshot; registration does not grant permission or tool exposure.
#[derive(Clone)]
pub struct RegisteredTool {
    /// Registered extension that owns this tool and its runtime API/config.
    pub owner: String,
    /// Canonical tool schema and model name.
    pub definition: CanonicalToolDefinition,
    /// Physical implementation with explicit supervisor ownership.
    pub implementation: Arc<dyn ExtensionTool>,
}
pub(crate) struct RegisteredExtension {
    id: String,
    implementation: Extension,
}
impl RegisteredExtension {
    pub fn id(&self) -> &str {
        &self.id
    }
}
impl std::ops::Deref for RegisteredExtension {
    type Target = dyn KernelExtension;
    fn deref(&self) -> &Self::Target {
        self.implementation.as_ref()
    }
}
pub(crate) struct Owned<T: ?Sized> {
    pub owner: String,
    pub handler: Arc<T>,
}
/// Builder; freeze into `ExtensionHost` before serving runs.
#[derive(Default)]
pub struct ExtensionRegistry {
    pub(crate) extensions: Vec<RegisteredExtension>,
    pub(crate) tools: BTreeMap<String, RegisteredTool>,
    pub(crate) commands: BTreeMap<String, Owned<dyn ExtensionCommand>>,
    pub(crate) methods: BTreeMap<String, Owned<dyn ErasedMethod>>,
    pub(crate) services: Vec<(String, Owned<dyn ExtensionService>)>,
    pub(crate) memory: Option<Arc<dyn MemoryProvider>>,
    pub(crate) memory_wrappers: Vec<Arc<dyn MemoryToolWrapper>>,
    pub(crate) context: Option<Owned<dyn ContextPlanner>>,
    pub(crate) configs: BTreeMap<String, serde_json::Value>,
}
impl ExtensionRegistry {
    fn require_owner(&self, owner: &str) -> Result<(), ExtensionError> {
        if self.configs.contains_key(owner) {
            Ok(())
        } else {
            Err(ExtensionError::Unavailable(format!("extension:{owner}")))
        }
    }

    /// Append an extension in deterministic registration order.
    pub fn register_extension(
        &mut self,
        extension: Extension,
        config: serde_json::Value,
    ) -> Result<(), ExtensionError> {
        let id = guard("registration", Hook::Registration, || {
            Ok(extension.id().to_owned())
        })?;
        valid_id(&id)?;
        if self.extensions.iter().any(|e| e.id() == id) {
            return Err(ExtensionError::Duplicate(id));
        }
        self.configs.insert(id.clone(), config);
        self.extensions.push(RegisteredExtension {
            id,
            implementation: extension,
        });
        Ok(())
    }
    /// Register a tool without replacing any existing wire-name owner.
    pub fn register_tool(&mut self, tool: RegisteredTool) -> Result<(), ExtensionError> {
        if !self.configs.contains_key(&tool.owner) {
            return Err(ExtensionError::Unavailable(format!(
                "extension:{}",
                tool.owner
            )));
        }
        tool.definition
            .validate()
            .map_err(|e| ExtensionError::Config(e.to_string()))?;
        let name = tool.definition.name.as_str().to_owned();
        if self.tools.contains_key(&name)
            || self.tools.values().any(|v| {
                v.definition.key == tool.definition.key
                    || v.definition.model_name == tool.definition.model_name
            })
        {
            return Err(ExtensionError::Duplicate(name));
        }
        self.tools.insert(name, tool);
        Ok(())
    }
    /// Register a service. Its start/stop order is the registration order.
    pub fn register_service(
        &mut self,
        owner: &str,
        name: &str,
        service: Arc<dyn ExtensionService>,
    ) -> Result<(), ExtensionError> {
        self.require_owner(owner)?;
        valid_id(name)?;
        if self.services.iter().any(|(n, _)| n == name) {
            return Err(ExtensionError::Duplicate(name.into()));
        }
        self.services.push((
            name.into(),
            Owned {
                owner: owner.into(),
                handler: service,
            },
        ));
        Ok(())
    }
    /// Single-occupancy context engine slot.
    pub fn register_context_engine(
        &mut self,
        owner: &str,
        engine: Arc<dyn ContextPlanner>,
    ) -> Result<(), ExtensionError> {
        self.require_owner(owner)?;
        if self.context.is_some() {
            return Err(ExtensionError::Duplicate("context_engine".into()));
        }
        self.context = Some(Owned {
            owner: owner.into(),
            handler: engine,
        });
        Ok(())
    }
    /// Single-occupancy memory provider slot. Middleware does not occupy a slot.
    pub fn register_memory(
        &mut self,
        memory: Arc<dyn MemoryProvider>,
    ) -> Result<(), ExtensionError> {
        if self.memory.is_some() {
            return Err(ExtensionError::Duplicate("memory".into()));
        }
        self.memory = Some(memory);
        Ok(())
    }
    /// Wrap memory calls in registration order, without replacing the memory owner.
    pub fn wrap_memory_tools(&mut self, wrapper: Arc<dyn MemoryToolWrapper>) {
        self.memory_wrappers.push(wrapper);
    }
    /// Register a slash command without its leading slash.
    pub fn register_command(
        &mut self,
        owner: &str,
        name: &str,
        command: Arc<dyn ExtensionCommand>,
    ) -> Result<(), ExtensionError> {
        self.require_owner(owner)?;
        valid_id(name)?;
        if self.commands.contains_key(name) {
            return Err(ExtensionError::Duplicate(name.into()));
        }
        self.commands.insert(
            name.into(),
            Owned {
                owner: owner.into(),
                handler: command,
            },
        );
        Ok(())
    }
    /// Register a typed host method. No network transport is started.
    pub fn register_gateway_method<M: GatewayMethod>(
        &mut self,
        owner: &str,
        name: &str,
        method: M,
    ) -> Result<(), ExtensionError> {
        self.require_owner(owner)?;
        valid_id(name)?;
        if self.methods.contains_key(name) {
            return Err(ExtensionError::Duplicate(name.into()));
        }
        self.methods.insert(
            name.into(),
            Owned {
                owner: owner.into(),
                handler: Arc::new(TypedMethod(method)),
            },
        );
        Ok(())
    }
    /// Definitions to include at host admission, preserving stable wire-name order.
    pub fn tool_definitions(&self) -> Vec<CanonicalToolDefinition> {
        self.tools.values().map(|t| t.definition.clone()).collect()
    }
}
