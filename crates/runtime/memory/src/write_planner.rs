//! Host port for finite, independently released Memory business computations.
//! An implementation identity binds recovery; it never conveys authorization.
use crate::MemoryToolError;
use musteragent_content_ref::Sha256;
use musteragent_memory_business::preparation::WritePreparation;
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryProgramIdentity(String);

impl MemoryProgramIdentity {
    /// Called by the original publication adapter with admission-frozen hashes.
    /// No current/latest release lookup belongs in preparation or recovery.
    pub fn published(
        source: Sha256,
        component_release: uuid::Uuid,
        bundle: Sha256,
        archive: Sha256,
    ) -> Result<Self, MemoryToolError> {
        if component_release.is_nil() {
            return Err(MemoryToolError::InvalidResult);
        }
        Ok(Self(format!(
            "memory-program.v1:{source}:{component_release}:{bundle}:{archive}"
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryWriteOperation {
    Remember,
    ProposeV2,
    ProposeV3,
    Publish,
}

impl MemoryWriteOperation {
    /// MCP structured JSON has no object field-order guarantee. Decode the
    /// closed ABI before canonicalizing; never pass through an untyped Value
    /// that would silently erase duplicate authority fields.
    pub fn canonical_response(
        self,
        request: &[u8],
        response: &[u8],
    ) -> Result<Vec<u8>, MemoryToolError> {
        if response.len() > musteragent_memory_business::preparation::MAX_PREPARATION_BYTES {
            return Err(MemoryToolError::InvalidResult);
        }
        let plan: WritePreparation =
            serde_json::from_slice(response).map_err(|_| MemoryToolError::InvalidResult)?;
        let bytes = plan
            .canonical_bytes()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        WritePreparation::decode_for_request(self.tool_key(), request, &bytes)
            .map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(bytes)
    }
    pub fn from_tool_key(key: &str) -> Result<Self, MemoryToolError> {
        match key {
            "user_memory.remember@2" => Ok(Self::Remember),
            "experience.propose@2" => Ok(Self::ProposeV2),
            "experience.propose@3" => Ok(Self::ProposeV3),
            "experience.publish@1" => Ok(Self::Publish),
            _ => Err(MemoryToolError::UnsupportedTool),
        }
    }

    pub fn tool_key(self) -> &'static str {
        match self {
            Self::Remember => "user_memory.remember@2",
            Self::ProposeV2 => "experience.propose@2",
            Self::ProposeV3 => "experience.propose@3",
            Self::Publish => "experience.publish@1",
        }
    }
}

pub trait MemoryWritePlanner: Send + Sync {
    fn identity(&self) -> MemoryProgramIdentity;

    /// Return a plan in the fixed private ABI, never a Memory/Tool receipt.
    /// Implementations must bound and await their physical computation lifetime.
    /// Host cancellation is checked before and after this future, not by
    /// dropping it while a process could still use captured inputs.
    fn prepare<'a>(
        &'a self,
        operation: MemoryWriteOperation,
        request: &'a [u8],
    ) -> ApplicationFuture<'a, Result<Vec<u8>, MemoryToolError>>;
}

pub trait MemoryWritePlannerFactory: Send + Sync {
    fn bind(
        &self,
        material: &musteragent_runtime_capability::ToolExecutionMaterial,
        invocation: &musteragent_runtime_capability::CapabilityInvocation,
    ) -> Result<std::sync::Arc<dyn MemoryWritePlanner>, MemoryToolError>;
}

pub(crate) struct CompiledMemoryWritePlanner;

impl MemoryWritePlanner for CompiledMemoryWritePlanner {
    fn identity(&self) -> MemoryProgramIdentity {
        MemoryProgramIdentity(musteragent_memory_business::COMPILED_IMPLEMENTATION.into())
    }

    fn prepare<'a>(
        &'a self,
        operation: MemoryWriteOperation,
        request: &'a [u8],
    ) -> ApplicationFuture<'a, Result<Vec<u8>, MemoryToolError>> {
        Box::pin(async move {
            WritePreparation::prepare(operation.tool_key(), request)
                .and_then(|value| value.canonical_bytes())
                .map_err(|_| MemoryToolError::InvalidInput)
        })
    }
}

pub(crate) async fn prepare(
    planner: &dyn MemoryWritePlanner,
    key: &str,
    request: &[u8],
    execution: Option<&musteragent_runtime_capability::OperationContext>,
) -> Result<WritePreparation, MemoryToolError> {
    if execution.is_some_and(|context| context.stopping()) {
        return Err(MemoryToolError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ));
    }
    let bytes = planner
        .prepare(MemoryWriteOperation::from_tool_key(key)?, request)
        .await?;
    if execution.is_some_and(|context| context.stopping()) {
        return Err(MemoryToolError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ));
    }
    WritePreparation::decode_for_request(key, request, &bytes)
        .map_err(|_| MemoryToolError::InvalidResult)
}
