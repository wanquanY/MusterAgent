//! Exact local admission source commitment. This is data, never a grant or an
//! Executor assignment. The canonical Dialogue owner chooses one Home before
//! the existing DevicePrivate grant signer authorizes it.
use crate::{DialogueExecutionFactBinding, DialogueInputOrigin, DialogueInputSurface};
use musteragent_content_ref::Sha256;
use musteragent_identity::{DeviceId, DeviceInstallationId, PrincipalKind, WorkspaceId};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalExecutionBinding {
    source: DialogueExecutionFactBinding,
    origin: DialogueInputOrigin,
    device_id: DeviceId,
    installation_id: DeviceInstallationId,
    device_workspace_id: Option<WorkspaceId>,
    digest: Sha256,
}
impl LocalExecutionBinding {
    pub fn new(
        source: DialogueExecutionFactBinding,
        origin: DialogueInputOrigin,
        device_id: DeviceId,
        installation_id: DeviceInstallationId,
    ) -> Result<Self, LocalExecutionError> {
        Self::new_in_workspace(source, origin, device_id, installation_id, None)
    }
    /// None is the exact historical source, never an instruction to choose a root.
    pub fn new_in_workspace(
        source: DialogueExecutionFactBinding,
        origin: DialogueInputOrigin,
        device_id: DeviceId,
        installation_id: DeviceInstallationId,
        device_workspace_id: Option<WorkspaceId>,
    ) -> Result<Self, LocalExecutionError> {
        if source.device_workspace_id != device_workspace_id
            || source.actor.principal_kind != PrincipalKind::User
            || !matches!(source.permission_mode, 1 | 2 | 4)
            || source.context_watermark.validate().is_err()
            || source.context_watermark.seq == 0
            || source.context_watermark.seq > i64::MAX as u64
            || source.context_watermark.epoch > i64::MAX as u64
            || [
                source.manifest_revision,
                source.instructions_revision,
                source.permission_revision,
            ]
            .iter()
            .any(|v| !(1..=i64::MAX as u64).contains(v))
            || source.source_sha256.into_bytes() == [0; 32]
        {
            return Err(LocalExecutionError::Source);
        }
        if origin.validate().is_err()
            || !matches!(
                origin.surface,
                DialogueInputSurface::Desktop | DialogueInputSurface::Mobile
            )
            || origin.installation_id != Some(installation_id)
        {
            return Err(LocalExecutionError::Origin);
        }
        let digest = Sha256::digest(canonical_bytes(
            &source,
            &origin,
            device_id,
            installation_id,
            device_workspace_id,
        ));
        Ok(Self {
            source,
            origin,
            device_id,
            installation_id,
            device_workspace_id,
            digest,
        })
    }
    pub fn source(&self) -> &DialogueExecutionFactBinding {
        &self.source
    }
    pub fn origin(&self) -> &DialogueInputOrigin {
        &self.origin
    }
    pub const fn device_id(&self) -> DeviceId {
        self.device_id
    }
    pub const fn installation_id(&self) -> DeviceInstallationId {
        self.installation_id
    }
    pub const fn device_workspace_id(&self) -> Option<WorkspaceId> {
        self.device_workspace_id
    }
    pub const fn digest(&self) -> Sha256 {
        self.digest
    }
    /// The original signed ExecutionGrant nonce commits to these exact facts.
    /// Merely constructing this string conveys no authority.
    pub fn grant_nonce(&self) -> String {
        Self::grant_nonce_for_digest(self.digest)
    }
    /// Authenticate an already retained source using its original signed nonce.
    pub fn grant_nonce_for_digest(digest: Sha256) -> String {
        format!("device_source_{digest}")
    }
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_bytes(
            &self.source,
            &self.origin,
            self.device_id,
            self.installation_id,
            self.device_workspace_id,
        )
    }
}
fn canonical_bytes(
    s: &DialogueExecutionFactBinding,
    origin: &DialogueInputOrigin,
    device: DeviceId,
    installation: DeviceInstallationId,
    workspace: Option<WorkspaceId>,
) -> Vec<u8> {
    let mut bytes = if workspace.is_some() {
        b"musteragent.local-execution-source.v2\0".to_vec()
    } else {
        b"musteragent.local-execution-source.v1\0".to_vec()
    };
    for id in [
        s.actor.tenant_id.into_uuid(),
        s.actor.principal_id.into_uuid(),
        s.intent_id.into_uuid(),
        s.agent_id.into_uuid(),
        s.agent_version_id.into_uuid(),
        s.dialogue_id.into_uuid(),
        s.context_conversation_id.into_uuid(),
        s.command_id.into_uuid(),
        s.run_id.into_uuid(),
        device.into_uuid(),
        installation.into_uuid(),
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    for n in [
        s.context_watermark.epoch,
        s.context_watermark.seq,
        s.manifest_revision,
        s.instructions_revision,
        s.permission_revision,
    ] {
        bytes.extend_from_slice(&n.to_be_bytes());
    }
    bytes.extend_from_slice(&s.permission_mode.to_be_bytes());
    bytes.push(u8::from(s.default_agent_direct_intent));
    bytes.extend_from_slice(&s.source_sha256.into_bytes());
    // Only the two native variants pass new(); explicit codes, never enum order.
    bytes.push(match origin.surface {
        DialogueInputSurface::Desktop => 1,
        DialogueInputSurface::Mobile => 2,
        _ => unreachable!("qualified native origin"),
    });
    if let Some(workspace) = workspace {
        bytes.extend_from_slice(workspace.into_uuid().as_bytes());
    }
    bytes
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum LocalExecutionError {
    #[error("local execution source is invalid")]
    Source,
    #[error("local execution must belong to the accepted native input installation")]
    Origin,
}
