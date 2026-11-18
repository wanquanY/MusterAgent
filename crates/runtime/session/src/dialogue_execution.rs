//! Owner product ownership admission. Physical ExecutionScope and signed
//! execution grants remain owned by the existing Runtime; tenant ownership
//! never implies local filesystem or desktop authorization.

use musteragent_content_ref::Sha256;
use musteragent_definition::owned_agent::{OwnedAgent, OwnedAgentConfiguration, OwnedAgentStatus};
use musteragent_execution_context::{DialogueCursor, UserIntent};
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, CommandId, ConversationId, MessageId, ParticipantId,
    PrincipalId, PrincipalKind, TenantId, UserIntentId, WorkspaceId,
};
use musteragent_ledger::AdmissionReceipt;
use musteragent_runtime_contract::ApplicationFuture;

use crate::{AdmissionCommit, DialogueStoreError};
mod device_memory;

pub use device_memory::*;
mod device_material;
pub use device_material::*;
mod device_progress;
pub use device_progress::*;
mod device_handoff;
pub use device_handoff::*;
mod device_grant;
pub use device_grant::{validate_device_grant, validate_device_reauthorization};

/// Canonical account/Agent product ownership, not a physical execution scope.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueExecutionScope {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    pub context_watermark: DialogueCursor,
}

/// One stable private ContextBranch per accepted intent. Public command IDs
/// remain in the dialogue owner; execution IDs have an independent namespace.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueExecutionIdentities {
    pub command_id: CommandId,
    pub conversation_id: ConversationId,
    pub requested_by: ParticipantId,
    /// Actual account owner for later user decisions; never the service requester.
    pub user: ParticipantId,
    pub executor: ParticipantId,
    pub message_id: MessageId,
}
impl DialogueExecutionIdentities {
    pub fn runtime_snapshot_id(
        self,
    ) -> Result<musteragent_identity::RuntimeSnapshotId, DialogueStoreError> {
        musteragent_runtime_admission::AdmissionIdentities::derive(self.command_id)
            .map(|ids| ids.runtime_snapshot_id)
            .ok_or(DialogueStoreError::Corrupt)
    }
    pub fn run_id(self) -> Result<musteragent_identity::RunId, DialogueStoreError> {
        musteragent_runtime_admission::AdmissionIdentities::derive(self.command_id)
            .map(|ids| ids.run_id)
            .ok_or(DialogueStoreError::Corrupt)
    }
    pub fn derive(scope: DialogueExecutionScope) -> Result<Self, DialogueStoreError> {
        fn derive(scope: DialogueExecutionScope, key: &[u8]) -> uuid::Uuid {
            let mut seed = b"musteragent.intent-execution.v1\0".to_vec();
            for value in [
                scope.tenant_id.into_uuid(),
                scope.owner_id.into_uuid(),
                scope.agent_id.into_uuid(),
                scope.source_intent_id.into_uuid(),
            ] {
                seed.extend_from_slice(value.as_bytes());
            }
            seed.extend_from_slice(key);
            let hash = Sha256::digest(seed).into_bytes();
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&hash[..16]);
            bytes[6] = (bytes[6] & 0x0f) | 0x50;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            uuid::Uuid::from_bytes(bytes)
        }
        Ok(Self {
            command_id: CommandId::from_uuid(derive(scope, b"command"))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            conversation_id: ConversationId::from_uuid(derive(scope, b"conversation"))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            requested_by: ParticipantId::from_uuid(derive(scope, b"requester"))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            user: ParticipantId::from_uuid(derive(scope, b"user"))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            executor: ParticipantId::from_uuid(derive(scope, b"executor"))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            message_id: MessageId::from_uuid(derive(scope, b"message"))
                .map_err(|_| DialogueStoreError::Corrupt)?,
        })
    }
}

/// Obtained from canonical dialogue rows. Admission revalidates every pin in
/// the same transaction as the Run and publication binding.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueExecutionPreparation {
    /// Exact publication frozen by the originating schedule. Never an entitlement.
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    /// Installation signing key selected at acceptance for a cloud-coordinated foreground turn.
    pub origin_tool_device_id: Option<musteragent_identity::DeviceId>,
    /// Physical Native placement; None preserves legacy custody byte-for-byte.
    /// This never changes the Dialogue data namespace into a Project.
    pub device_workspace_id: Option<WorkspaceId>,
    pub scope: DialogueExecutionScope,
    /// Canonical account directory pin, revalidated by admission.
    pub default_agent_id: AgentId,
    pub identities: DialogueExecutionIdentities,
    pub intent: UserIntent,
    pub agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
    pub assistant_identity: crate::AgentAdmissionIdentity,
    pub context_cut: musteragent_execution_context::DialogueContextCut,
    /// Original source owner observation, recomputed under admission CAS.
    pub context_has_input_attachments: bool,
    /// Preparation pin; source hash and commit revalidation include this exact contract.
    pub delivery_contract: musteragent_execution_context::DialogueOutputContract,
}
impl DialogueExecutionPreparation {
    pub fn default_agent_direct_intent(&self) -> bool {
        self.default_agent_id == self.scope.agent_id
    }

    pub fn actor(&self) -> Actor {
        Actor {
            tenant_id: self.scope.tenant_id,
            principal_id: self.scope.owner_id,
            principal_kind: PrincipalKind::User,
        }
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if let Some(reference) = &self.product_release {
            if self.intent.origin.surface
                != musteragent_execution_context::DialogueInputSurface::Schedule
            {
                return Err(DialogueStoreError::Corrupt);
            }
            if reference.digest == [0; 32] {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        musteragent_execution_context::validate_dialogue_input_attachments(
            &self.intent.input_attachments,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?;
        if self.origin_tool_device_id.is_some()
            && (self.intent.origin.surface
                != musteragent_execution_context::DialogueInputSurface::Desktop
                || self.intent.origin.installation_id.is_none())
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let scope = self.scope;
        if self.device_workspace_id.is_some()
            && (!self.delivery_contract.is_native()
                || !matches!(
                    self.intent.origin.surface,
                    musteragent_execution_context::DialogueInputSurface::Desktop
                        | musteragent_execution_context::DialogueInputSurface::Mobile
                        | musteragent_execution_context::DialogueInputSurface::Capability
                ))
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if scope.context_watermark != self.intent.accepted_cursor
            || scope.context_watermark.seq == 0
            || scope.tenant_id != self.intent.tenant_id
            || scope.owner_id != self.intent.owner_id
            || scope.source_intent_id != self.intent.intent_id
            || scope.dialogue_id != self.intent.dialogue_id
            || scope.dialogue_id != self.agent.dialogue_id
            || scope.agent_id != self.agent.agent_id
            || scope.agent_id != self.configuration.agent_id
            || self.agent.status != OwnedAgentStatus::Active
            || self.identities != DialogueExecutionIdentities::derive(scope)?
        {
            return Err(DialogueStoreError::Corrupt);
        }
        self.agent
            .validate_definition(
                &self.assistant_identity.definition,
                &self.assistant_identity.version,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.configuration
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.context_cut
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if self.context_cut.tenant_id != scope.tenant_id
            || self.context_cut.owner_id != scope.owner_id
            || self.context_cut.agent_id != scope.agent_id
            || self.context_cut.dialogue_id != scope.dialogue_id
            || self.context_cut.through.epoch != scope.context_watermark.epoch
            || self.context_cut.through.seq != scope.context_watermark.seq - 1
        {
            return Err(DialogueStoreError::Corrupt);
        }
        scope
            .context_watermark
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DialogueExecutionAdmissionCommit {
    pub preparation: DialogueExecutionPreparation,
    pub admission: AdmissionCommit,
}
impl DialogueExecutionAdmissionCommit {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.preparation.validate()?;
        let p = &self.preparation;
        let batch = &self.admission.batch;
        batch.validate().map_err(|_| DialogueStoreError::Corrupt)?;
        let ids =
            musteragent_runtime_admission::AdmissionIdentities::derive(p.identities.command_id)
                .ok_or(DialogueStoreError::Corrupt)?;
        if batch.actor != p.actor()
            || batch.run.context.conversation_id != p.identities.conversation_id
            || batch.run.context.executor_participant_id != p.identities.executor
            || batch.run.context.requested_by_participant_id != p.identities.requested_by
            || batch.run.context.run_id != ids.run_id
            || batch.runtime_snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || batch.runtime_snapshot.memory_agent_id != p.scope.agent_id
            || batch.message.message_id != p.identities.message_id
            || batch.message.content != p.intent.content_ref
            || batch.conversation_title.is_some()
            || batch.append.expected_offset != musteragent_identity::LedgerOffset::ZERO
            || batch.run.context.home_epoch.get() != 1
            || self.admission.content_stage.content != p.intent.content_ref
            || self.admission.content_stage.command_fingerprint != batch.append.command_fingerprint
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

/// A prepared DevicePrivate batch, not a durable receipt or delegated Executor
/// assignment. The SQLite Dialogue owner must atomically validate source custody
/// and commit this batch before the ordinary scheduler can execute it.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceAdmissionCommit {
    pub device_id: musteragent_identity::DeviceId,
    pub execution: DialogueExecutionAdmissionCommit,
}
impl DeviceAdmissionCommit {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
        self.execution.validate()?;
        let p = &self.execution.preparation;
        let batch = &self.execution.admission.batch;
        let home = RuntimeHome::Device {
            device_id: self.device_id,
        };
        if batch.runtime_snapshot.execution_grant.nonce.as_str()
            != p.local_execution_binding(self.device_id)?.grant_nonce()
            || batch.run.context.control_home != home
            || batch.run.context.execution_home != home
            || batch.run.context.execution_scope
                != (ExecutionScope::DevicePrivate {
                    device_id: self.device_id,
                    workspace_id: p.device_workspace_id,
                })
            || batch.runtime_snapshot.execution_grant.permission_mode
                != Some(p.configuration.permission_mode)
            || !matches!(
                p.intent.origin.surface,
                musteragent_execution_context::DialogueInputSurface::Desktop
                    | musteragent_execution_context::DialogueInputSurface::Mobile
            )
            || p.intent.origin.validate().is_err()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueExecutionReceipt {
    pub conversation_id: ConversationId,
    pub run_id: musteragent_identity::RunId,
}

pub trait DialogueExecutionStore: Send + Sync {
    fn lookup(
        &self,
        actor: Actor,
        intent_id: UserIntentId,
    ) -> ApplicationFuture<'_, Result<Option<DialogueExecutionReceipt>, DialogueStoreError>>;
    fn prepare(
        &self,
        actor: Actor,
        intent_id: UserIntentId,
    ) -> ApplicationFuture<'_, Result<DialogueExecutionPreparation, DialogueStoreError>>;
    fn admit(
        &self,
        commit: DialogueExecutionAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, DialogueStoreError>>;
}
impl DialogueExecutionPreparation {
    pub fn local_execution_binding(
        &self,
        device_id: musteragent_identity::DeviceId,
    ) -> Result<
        musteragent_execution_context::local_execution::LocalExecutionBinding,
        DialogueStoreError,
    > {
        let installation = self
            .intent
            .origin
            .installation_id
            .ok_or(DialogueStoreError::Unavailable)?;
        musteragent_execution_context::local_execution::LocalExecutionBinding::new_in_workspace(
            self.grant_fact_binding()?,
            self.intent.origin.clone(),
            device_id,
            installation,
            self.device_workspace_id,
        )
        .map_err(|_| DialogueStoreError::Unavailable)
    }
    pub fn grant_fact_binding(
        &self,
    ) -> Result<musteragent_execution_context::DialogueExecutionFactBinding, DialogueStoreError>
    {
        self.validate()?;
        Ok(
            musteragent_execution_context::DialogueExecutionFactBinding {
                product_release: self.product_release.clone(),
                device_workspace_id: self.device_workspace_id,
                actor: self.actor(),
                intent_id: self.scope.source_intent_id,
                agent_id: self.scope.agent_id,
                agent_version_id: self.agent.published_version_id,
                dialogue_id: self.scope.dialogue_id,
                context_conversation_id: self.identities.conversation_id,
                command_id: self.identities.command_id,
                run_id: self.identities.run_id()?,
                context_watermark: self.scope.context_watermark,
                manifest_revision: self.configuration.manifest_revision.get(),
                instructions_revision: self.configuration.instructions_revision.get(),
                permission_revision: self.configuration.permission_revision.get(),
                permission_mode: self.configuration.permission_mode as i32,
                default_agent_direct_intent: self.default_agent_direct_intent(),
                source_sha256: crate::dialogue_execution_source_hash(self)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            },
        )
    }
}
