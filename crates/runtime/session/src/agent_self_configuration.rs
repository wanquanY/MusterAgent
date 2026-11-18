//! Current-Agent identity and instructions remain account configuration, not memory.
//! Tool callers cannot select another Agent or change permissions/installed capabilities.
use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_identity::{Actor, AgentId, InvocationId, ToolCallId, UtcTimestamp};
use musteragent_run::{AttemptContext, RunContext};

use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentSelfChange {
    Name {
        expected_revision: u64,
        display_name: String,
    },
    Instructions {
        expected_revision: u64,
        text: String,
    },
}
impl AgentSelfChange {
    pub fn parse(bytes: &[u8]) -> Result<Self, AgentSelfConfigurationError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| AgentSelfConfigurationError::InvalidArguments)?;
        let (revision, valid) = match &value {
            Self::Name {
                expected_revision,
                display_name,
            } => (
                *expected_revision,
                !display_name.trim().is_empty()
                    && display_name.len() <= 120
                    && !display_name.chars().any(char::is_control),
            ),
            Self::Instructions {
                expected_revision,
                text,
            } => (
                *expected_revision,
                text.len() <= 64 * 1024 && !text.contains('\0'),
            ),
        };
        if revision == 0 || revision >= i64::MAX as u64 || !valid {
            return Err(AgentSelfConfigurationError::InvalidArguments);
        }
        Ok(value)
    }
    pub fn expected_revision(&self) -> u64 {
        match self {
            Self::Name {
                expected_revision, ..
            }
            | Self::Instructions {
                expected_revision, ..
            } => *expected_revision,
        }
    }
    pub fn result_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"status":"saved", "kind": match self { Self::Name {..} => "name", Self::Instructions {..} => "instructions" }, "revision":self.expected_revision()+1})).expect("bounded configuration result")
    }
}
#[derive(Debug, Clone)]
pub struct AgentSelfConfigurationResult {
    pub result_ref: ContentRef,
    pub result_schema: SchemaRef,
}
#[derive(Clone)]
pub struct AgentSelfToolInvocation {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub run_context: RunContext,
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub tool_key: String,
    pub request_ref: ContentRef,
    pub result_schema: SchemaRef,
    pub requested_at: UtcTimestamp,
}
#[derive(Debug, thiserror::Error)]
pub enum AgentSelfConfigurationError {
    #[error("current Agent configuration requires a direct user request")]
    Forbidden,
    #[error("invalid Agent configuration")]
    InvalidArguments,
    #[error("Agent configuration changed; inspect again before updating")]
    Conflict,
    #[error("execution fence is stale")]
    StaleFence,
    #[error("configuration service unavailable")]
    Infrastructure,
    #[error("configuration custody inconsistent")]
    Corrupt,
}
pub trait AgentSelfConfigurationTools: Send + Sync {
    fn inspect<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: AgentSelfToolInvocation,
    ) -> ApplicationFuture<'a, Result<AgentSelfConfigurationResult, AgentSelfConfigurationError>>;
    /// Account mutation and original Tool success share the execution owner's commit boundary.
    fn configure<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: AgentSelfToolInvocation,
    ) -> ApplicationFuture<'a, Result<AgentSelfConfigurationResult, AgentSelfConfigurationError>>;
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_is_bounded_and_cannot_choose_identity_or_authority() {
        assert!(
            AgentSelfChange::parse(
                br#"{"kind":"name","expected_revision":1,"display_name":"Remy"}"#
            )
            .is_ok()
        );
        for input in [
            r#"{"kind":"name","expected_revision":0,"display_name":"Remy"}"#,
            r#"{"kind":"name","expected_revision":1,"display_name":"  "}"#,
            r#"{"kind":"permission","expected_revision":1,"mode":"full_access"}"#,
            r#"{"kind":"instructions","expected_revision":1,"text":"brief","agent_id":"other"}"#,
        ] {
            assert!(AgentSelfChange::parse(input.as_bytes()).is_err());
        }
    }
}

/// Authenticated Native transport facts, reconstructed from the original Tool
/// journal. The model never supplies this admission, invocation or lease proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSelfDeviceRequest {
    pub admission: crate::DeviceDeliveryBinding,
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub tool_key: String,
    pub request_ref: ContentRef,
    pub request_bytes: Vec<u8>,
    pub request_schema: SchemaRef,
    pub result_schema: SchemaRef,
    pub attempt: AttemptContext,
}
impl AgentSelfDeviceRequest {
    pub fn validate(&self) -> Result<(), AgentSelfConfigurationError> {
        self.admission
            .validate_subject()
            .map_err(|_| AgentSelfConfigurationError::Forbidden)?;
        if self.admission.work_item.is_some()
            || self.attempt.run_id != self.admission.run_id
            || self.attempt.lease_expires_at.unix_millis() <= 0
            || self.request_bytes.len() > 70 * 1024
            || self.request_ref.media_type.as_str() != "application/json"
            || self.request_ref.byte_length != self.request_bytes.len() as u64
            || self.request_ref.sha256
                != musteragent_content_ref::Sha256::digest(&self.request_bytes)
        {
            return Err(AgentSelfConfigurationError::InvalidArguments);
        }
        match self.tool_key.as_str() {
            "agents.self.inspect@1"
                if serde_json::from_slice::<serde_json::Value>(&self.request_bytes).ok()
                    == Some(serde_json::json!({})) =>
            {
                Ok(())
            }
            "agents.self.configure@1" | "agents.self.configure@2" => {
                AgentSelfChange::parse(&self.request_bytes).map(|_| ())
            }
            _ => Err(AgentSelfConfigurationError::InvalidArguments),
        }
    }
}
#[derive(Debug, Clone)]
pub struct AgentSelfDeviceReceipt {
    pub invocation_id: InvocationId,
    pub request_hash: musteragent_content_ref::Sha256,
    pub result_ref: ContentRef,
    pub result_bytes: Vec<u8>,
}
impl AgentSelfDeviceReceipt {
    pub fn validate(
        &self,
        request: &AgentSelfDeviceRequest,
    ) -> Result<(), AgentSelfConfigurationError> {
        if self.invocation_id != request.invocation_id
            || self.request_hash != request.request_ref.sha256
            || self.result_bytes.len() > 128 * 1024
            || self.result_ref.media_type.as_str() != "application/json"
            || self.result_ref.byte_length != self.result_bytes.len() as u64
            || self.result_ref.sha256 != musteragent_content_ref::Sha256::digest(&self.result_bytes)
        {
            return Err(AgentSelfConfigurationError::Corrupt);
        }
        Ok(())
    }
}
/// Confirmed owner rejection is distinct from an unknown transport outcome.
#[derive(Debug, Clone)]
pub enum AgentSelfDeviceResponse {
    Receipt(Option<AgentSelfDeviceReceipt>),
    RevisionConflict,
}
pub trait AgentSelfDeviceTransport: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: &'a AgentSelfDeviceRequest,
        lookup: bool,
    ) -> ApplicationFuture<'a, Result<AgentSelfDeviceResponse, AgentSelfConfigurationError>>;
}
pub trait AgentSelfDeviceStore: Send + Sync {
    fn execute<'a>(
        &'a self,
        actor: Actor,
        target: &'a crate::OriginDeviceExecutionTarget,
        request: &'a AgentSelfDeviceRequest,
        lookup: bool,
    ) -> ApplicationFuture<'a, Result<Option<AgentSelfDeviceReceipt>, AgentSelfConfigurationError>>;
}
