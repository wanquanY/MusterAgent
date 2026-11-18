//! Host capability ports made available to each extension.
use crate::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::RunId;
use musteragent_runtime_contract::ApplicationFuture;
use serde::de::DeserializeOwned;
use std::{path::PathBuf, sync::Arc};

/// Child run request; the real delegation adapter is supplied by the host.
#[derive(Debug, Clone)]
pub struct SubagentRequest {
    /// Requested task.
    pub task: String,
    /// Host-owned agent selection key.
    pub agent: String,
    /// Idempotency key for the child admission.
    pub idempotency_key: String,
}
/// Successful child admission, not completion.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubagentReceipt {
    /// Accepted child run.
    pub run_id: RunId,
}
/// Delegation port implemented by the cloud orchestration owner.
pub trait SubagentSpawner: Send + Sync {
    /// True when the durable orchestration outbox owns lifecycle delivery.
    fn owns_lifecycle(&self) -> bool {
        false
    }

    /// Must enforce parent authority and idempotent child admission.
    fn spawn<'a>(
        &'a self,
        parent: &'a ExtensionContext,
        request: SubagentRequest,
    ) -> ApplicationFuture<'a, Result<SubagentReceipt, HookError>>;
}
/// Memory operation shared by tools and lifecycle extraction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MemoryRequest {
    /// Stable memory operation name.
    pub operation: String,
    /// Operation arguments.
    pub arguments: serde_json::Value,
}
/// Single memory slot; storage/authorization stays with the provider.
pub trait MemoryProvider: Send + Sync {
    /// Execute the real memory operation.
    fn call<'a>(
        &'a self,
        context: &'a ExtensionContext,
        request: MemoryRequest,
    ) -> ApplicationFuture<'a, Result<serde_json::Value, HookError>>;
}
/// Ordered memory middleware. Only the primary memory provider occupies the slot.
pub trait MemoryToolWrapper: Send + Sync {
    /// Rewrite/deny a memory request; errors are surfaced and audited.
    fn before(&self, request: MemoryRequest) -> Result<MemoryRequest, HookError> {
        Ok(request)
    }
    /// Read-only observation, in registration order.
    fn after(
        &self,
        _request: &MemoryRequest,
        _result: &serde_json::Value,
    ) -> Result<(), HookError> {
        Ok(())
    }
}
/// Configuration of the runtime API. No port silently falls back to a stub.
pub struct RuntimePorts {
    /// Existing tenant content owner for immutable rewrite replay.
    pub content: Arc<dyn musteragent_runtime_content::ContentStore>,
    /// Read-only bounded view of the kernel's original tool records.
    pub history: Arc<dyn ToolHistory>,
    /// Root for extension-specific state, outside the model-controlled workspace.
    pub state_root: PathBuf,
    /// Optional until a host supports delegation. Absence is a typed error.
    pub subagents: Option<Arc<dyn SubagentSpawner>>,
}
/// Scope-limited runtime API supplied to hooks, commands and registered tools.
#[derive(Clone)]
pub struct RuntimeApi {
    pub(crate) host: Arc<ExtensionHost>,
    pub(crate) context: ExtensionContext,
    pub(crate) extension: String,
}
impl RuntimeApi {
    /// Invoke a registered host method in this extension's verified scope.
    pub async fn gateway(
        &self,
        request: GatewayRequest,
    ) -> Result<GatewayResponse, ExtensionError> {
        self.host.gateway(&self.context, request).await
    }

    /// Read configuration as the extension's own typed schema.
    pub fn config<T: DeserializeOwned>(&self) -> Result<T, ExtensionError> {
        let value = self
            .host
            .registry
            .configs
            .get(&self.extension)
            .ok_or_else(|| ExtensionError::Unavailable(format!("config:{}", self.extension)))?;
        serde_json::from_value(value.clone()).map_err(|e| ExtensionError::Config(e.to_string()))
    }
    /// Resolve/create this extension's state directory. Tenant + session hashes
    /// prevent a session key from becoming a path. Symlink components are rejected.
    pub fn state_dir(&self) -> Result<PathBuf, ExtensionError> {
        valid_id(&self.extension)?;
        let root = &self.host.ports.state_root;
        if !root.is_absolute() {
            return Err(ExtensionError::StateDirectory(
                "state_root must be absolute".into(),
            ));
        }
        let session = Sha256::digest(self.context.session_key.as_bytes()).to_string();
        let path = root
            .join(self.context.run.tenant_id.to_string())
            .join(session)
            .join(&self.extension);
        let mut current = PathBuf::new();
        for part in path.components() {
            if matches!(part, std::path::Component::ParentDir) {
                return Err(ExtensionError::StateDirectory("parent component".into()));
            }
            current.push(part.as_os_str());
            match std::fs::symlink_metadata(&current) {
                Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
                    return Err(ExtensionError::StateDirectory(
                        "non-directory or symlink component".into(),
                    ));
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    match std::fs::create_dir(&current) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            let meta = std::fs::symlink_metadata(&current)
                                .map_err(|e| ExtensionError::StateDirectory(e.to_string()))?;
                            if meta.file_type().is_symlink() || !meta.is_dir() {
                                return Err(ExtensionError::StateDirectory(
                                    "non-directory or symlink component".into(),
                                ));
                            }
                        }
                        Err(error) => {
                            return Err(ExtensionError::StateDirectory(error.to_string()));
                        }
                    }
                }
                Err(e) => return Err(ExtensionError::StateDirectory(e.to_string())),
            }
        }
        Ok(path)
    }
    /// Admit a child via the supplied port, then notify extensions in order.
    pub async fn spawn_subagent(
        &self,
        mut request: SubagentRequest,
    ) -> Result<SubagentReceipt, ExtensionError> {
        let port = self
            .host
            .ports
            .subagents
            .as_ref()
            .ok_or_else(|| ExtensionError::Unavailable("subagent_spawner".into()))?;
        request.idempotency_key = format!("{}:{}", self.extension, request.idempotency_key);
        let key = format!("spawn:{}", request.idempotency_key);
        let result = guard_async(&self.extension, Hook::SubagentSpawned, || {
            port.spawn(&self.context, request)
        })
        .await;
        let receipt = self.host.required(&self.context, &key, result).await?;
        if !port.owns_lifecycle() {
            self.host
                .lifecycle(
                    &self.context,
                    LifecycleEvent::SubagentSpawned(receipt.run_id),
                )
                .await?;
        }
        Ok(receipt)
    }
    /// Call memory through all registered wrappers, followed by the one memory owner.
    pub async fn memory(
        &self,
        mut request: MemoryRequest,
    ) -> Result<serde_json::Value, ExtensionError> {
        let memory = self
            .host
            .registry
            .memory
            .as_ref()
            .ok_or_else(|| ExtensionError::Unavailable("memory".into()))?;
        let key = format!(
            "memory:{}:{}",
            self.extension,
            Sha256::digest(format!("{request:?}"))
        );
        for (i, wrapper) in self.host.registry.memory_wrappers.iter().enumerate() {
            let rewrite_key = format!("{key}:rewrite:{i}");
            if let Some(ExtensionFactRecord {
                fact:
                    ExtensionFact::MemoryRewritten {
                        before,
                        after,
                        content,
                        ..
                    },
                ..
            }) = self
                .host
                .journal
                .select(&self.context, FactQuery::Key(rewrite_key.clone()))
                .await?
                .first()
            {
                if PayloadDigest::of(&request)? != *before {
                    return Err(ExtensionError::IdentityMismatch);
                }
                request = content
                    .read(self.host.ports.content.as_ref(), &self.context, after)
                    .await?;
                continue;
            }
            let before = request.clone();
            let result = guard(&self.extension, Hook::Memory, || wrapper.before(request));
            request = self
                .host
                .required(&self.context, &format!("{key}:before:{i}"), result)
                .await?;
            if request != before {
                let (after, content) = RewriteContent::store(
                    self.host.ports.content.as_ref(),
                    &self.context,
                    &rewrite_key,
                    &request,
                )
                .await?;
                self.host
                    .journal
                    .append(
                        &self.context,
                        &format!("{key}:rewrite:{i}"),
                        ExtensionFact::MemoryRewritten {
                            extension: self.extension.clone(),
                            wrapper_index: i,
                            before: PayloadDigest::of(&before)?,
                            after,
                            content,
                        },
                    )
                    .await?;
            }
        }
        let result = guard_async(&self.extension, Hook::Memory, || {
            memory.call(&self.context, request.clone())
        })
        .await;
        let result = self.host.required(&self.context, &key, result).await?;
        for (i, wrapper) in self.host.registry.memory_wrappers.iter().enumerate() {
            self.host
                .required(
                    &self.context,
                    &format!("{key}:after:{i}"),
                    guard(&self.extension, Hook::Memory, || {
                        wrapper.after(&request, &result)
                    }),
                )
                .await?;
        }
        Ok(result)
    }
}
