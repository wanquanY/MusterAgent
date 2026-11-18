//! Activating an account instance references an already qualified immutable
//! definition. Content preparation and remote installation happen beforehand.
use std::num::NonZeroU64;

use musteragent_content_ref::Sha256;
use musteragent_definition::owned_agent::{
    OwnedAgent, OwnedAgentConfiguration, OwnedAgentError, OwnedAgentStatus, OwnerSpace,
};
use musteragent_definition::{AgentDefinition, AgentVersion};
use musteragent_identity::{Actor, AgentId, CommandId, OwnerSpaceId, PrincipalKind};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

use crate::DialogueStoreError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgentActivation {
    pub actor: Actor,
    pub command_id: CommandId,
    pub space_id: OwnerSpaceId,
    pub agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
    pub definition: AgentDefinition,
    pub version: AgentVersion,
    pub published_pointer_version: NonZeroU64,
    pub make_default: bool,
}

impl OwnedAgentActivation {
    pub fn validate(&self) -> Result<(), OwnedAgentApplicationError> {
        if self.actor.principal_kind != PrincipalKind::User {
            return Err(OwnedAgentApplicationError::Forbidden);
        }
        if self.agent.space_id != self.space_id
            || self.configuration.agent_id != self.agent.agent_id
            || self.agent.status != OwnedAgentStatus::Active
            || self.agent.revision != NonZeroU64::MIN
            || self.configuration.manifest_revision != NonZeroU64::MIN
            || self.configuration.profile.revision != NonZeroU64::MIN
            || self.configuration.instructions_revision != NonZeroU64::MIN
            || self.configuration.permission_revision != NonZeroU64::MIN
            // Retained pre-upgrade preparations keep their captured default.
            // New preparations use Trusted; neither may begin with FullAccess.
            || !matches!(
                self.configuration.permission_mode,
                musteragent_policy::PermissionMode::Supervised
                    | musteragent_policy::PermissionMode::Trusted
            )
        {
            return Err(OwnedAgentError::IdentityMismatch.into());
        }
        self.agent
            .validate_definition(&self.definition, &self.version)?;
        self.configuration.validate()?;
        Ok(())
    }

    pub fn fingerprint(&self) -> Sha256 {
        let mut material = b"musteragent.owned-agent-activation.v1\0".to_vec();
        // Generated Dialogue/Space IDs and server timestamps do not change the
        // semantic operation. Definition/version identity is immutable authority.
        for field in [
            self.actor.tenant_id.to_string(),
            self.actor.principal_id.to_string(),
            self.agent.agent_id.to_string(),
            self.agent.published_version_id.to_string(),
            self.published_pointer_version.to_string(),
            self.make_default.to_string(),
            self.configuration.profile.display_name.clone(),
            self.configuration.profile.description.clone(),
        ] {
            append_field(&mut material, field.as_bytes());
        }
        for content in [
            &self.configuration.profile.avatar_ref,
            &self.configuration.instructions_ref,
        ] {
            match content {
                Some(value) => {
                    material.push(1);
                    for field in [
                        value.content_id.to_string(),
                        value.media_type.as_str().to_owned(),
                        value.byte_length.to_string(),
                        value.sha256.to_string(),
                    ] {
                        append_field(&mut material, field.as_bytes());
                    }
                }
                None => material.push(0),
            }
        }
        append_field(
            &mut material,
            &(self.configuration.permission_mode as u8).to_be_bytes(),
        );
        Sha256::digest(material)
    }
}

fn append_field(bytes: &mut Vec<u8>, field: &[u8]) {
    bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
    bytes.extend_from_slice(field);
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OwnedAgentActivationDisposition {
    Created,
    ExistingDefault,
    Replayed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgentActivationReceipt {
    pub space: OwnerSpace,
    pub agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
    pub disposition: OwnedAgentActivationDisposition,
}

/// Activation's transaction owns the instance, unique public dialogue, initial
/// configuration/inbox, account default pointer, receipt and public outbox.
/// It checks the immutable Definition and Published Pointer under the same lock.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgentSnapshot {
    pub primary_agent_id: AgentId,
    pub activity: Option<crate::OwnedAgentActivity>,
    pub agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerSpaceSnapshot {
    pub space: OwnerSpace,
    pub agents: Vec<OwnedAgentSnapshot>,
    pub sync_epoch: u64,
    pub head_event_seq: u64,
}

pub trait OwnedAgentStore: Send + Sync {
    fn delete_agent(
        &self,
        command: crate::DeleteOwnedAgent,
    ) -> ApplicationFuture<'_, Result<OwnedAgent, OwnedAgentApplicationError>>;
    fn configure(
        &self,
        command: crate::ConfigureOwnedAgent,
    ) -> ApplicationFuture<'_, Result<OwnedAgentConfiguration, DialogueStoreError>>;
    fn home_snapshot(
        &self,
        actor: Actor,
    ) -> ApplicationFuture<'_, Result<Option<OwnerSpaceSnapshot>, DialogueStoreError>>;
    fn activate(
        &self,
        command: OwnedAgentActivation,
    ) -> ApplicationFuture<'_, Result<OwnedAgentActivationReceipt, DialogueStoreError>>;
    fn load_space(
        &self,
        actor: Actor,
    ) -> ApplicationFuture<'_, Result<Option<OwnerSpace>, DialogueStoreError>>;
    fn list(
        &self,
        actor: Actor,
    ) -> ApplicationFuture<'_, Result<Vec<OwnedAgent>, DialogueStoreError>>;
    fn configuration(
        &self,
        actor: Actor,
        agent_id: AgentId,
    ) -> ApplicationFuture<'_, Result<Option<OwnedAgentConfiguration>, DialogueStoreError>>;
}

#[derive(Debug, Error)]
pub enum OwnedAgentApplicationError {
    #[error(transparent)]
    Domain(#[from] OwnedAgentError),
    #[error(transparent)]
    Store(#[from] DialogueStoreError),
    #[error("account-owned Agent management requires an authenticated user")]
    Forbidden,
    #[error("Agent still has unfinished work")]
    Busy,
}
