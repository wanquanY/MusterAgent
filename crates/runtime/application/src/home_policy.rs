use musteragent_conversation::{ConversationCommandError, CreateConversation};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{DeviceId, DottedKey, TenantId};
use thiserror::Error;

const CLOUD_RUNTIME_AUDIENCE: &str = "musteragent.runtime.cloud";

/// Composition-pinned Runtime ownership for every command handled by one
/// Runtime Application instance.
///
/// The policy is deliberately immutable: an adapter cannot select a home per
/// request, and a Device composition cannot drift between different devices.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeHomePolicy {
    control_home: RuntimeHome,
    execution_home: RuntimeHome,
    runtime_audience: DottedKey,
}

impl RuntimeHomePolicy {
    /// Canonical Cloud composition retained for existing production callers.
    #[must_use]
    pub fn cloud() -> Self {
        Self {
            control_home: RuntimeHome::Cloud,
            execution_home: RuntimeHome::Cloud,
            runtime_audience: DottedKey::new(CLOUD_RUNTIME_AUDIENCE)
                .unwrap_or_else(|_| unreachable!("built-in Runtime audience is valid")),
        }
    }

    /// Canonical single-device composition.
    ///
    /// The audience is supplied by the grant-authority composition rather than
    /// invented by the Application or a storage adapter.
    #[must_use]
    pub const fn device(device_id: DeviceId, runtime_audience: DottedKey) -> Self {
        let home = RuntimeHome::Device { device_id };
        Self {
            control_home: home,
            execution_home: home,
            runtime_audience,
        }
    }

    /// Immutable owner of command authority.
    #[must_use]
    pub const fn control_home(&self) -> RuntimeHome {
        self.control_home
    }

    /// Immutable home where admitted Runs execute.
    #[must_use]
    pub const fn execution_home(&self) -> RuntimeHome {
        self.execution_home
    }

    /// Audience required on every authenticated execution grant.
    #[must_use]
    pub const fn runtime_audience(&self) -> &DottedKey {
        &self.runtime_audience
    }

    /// Reject conversation creation for another composition.
    pub fn validate_create(
        &self,
        command: &CreateConversation,
    ) -> Result<(), RuntimeHomePolicyError> {
        command
            .validate()
            .map_err(RuntimeHomePolicyError::InvalidConversation)?;
        if command.control_home != self.control_home {
            return Err(RuntimeHomePolicyError::ControlHomeMismatch);
        }
        Ok(())
    }

    /// Reject scopes outside the pinned home and tenant.
    pub fn validate_execution_scope(
        &self,
        tenant_id: TenantId,
        scope: ExecutionScope,
    ) -> Result<(), RuntimeHomePolicyError> {
        let valid = match (self.control_home, self.execution_home, scope) {
            (
                RuntimeHome::Cloud,
                RuntimeHome::Cloud,
                ExecutionScope::Tenant { tenant_id: scope },
            ) => scope == tenant_id,
            (RuntimeHome::Cloud, RuntimeHome::Cloud, ExecutionScope::Workspace { .. }) => true,
            (
                RuntimeHome::Device {
                    device_id: control_device,
                },
                RuntimeHome::Device {
                    device_id: execution_device,
                },
                ExecutionScope::DevicePrivate {
                    device_id: scope_device,
                    ..
                },
            ) => control_device == execution_device && control_device == scope_device,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(RuntimeHomePolicyError::ExecutionScopeMismatch)
        }
    }
}

impl Default for RuntimeHomePolicy {
    fn default() -> Self {
        Self::cloud()
    }
}

/// A command attempted to cross the fixed application ownership boundary.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum RuntimeHomePolicyError {
    #[error("Conversation command is invalid: {0}")]
    InvalidConversation(ConversationCommandError),
    #[error("Conversation control home does not match the Runtime composition")]
    ControlHomeMismatch,
    #[error("execution scope does not match the Runtime composition")]
    ExecutionScopeMismatch,
}
