//! Account stop obligations for the exact original Native Intent custody.
//! The original local RunCancel owner executes admitted targets; only the local
//! admission writer may prove NotAdmitted. Missing ACK never implies no Run.
use crate::{DeviceDeliveryBinding, DialogueStoreError, OriginDeviceExecutionTarget};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, CommandId, ConversationId, DeviceId, DeviceInstallationId,
    PrincipalKind, RunId, UserIntentId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;

pub const DEVICE_CONTROL_PAGE_SIZE: usize = 32;
pub const MAX_DEVICE_CONTROL_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueExecutionControlTarget {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub intent_id: UserIntentId,
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub device_id: DeviceId,
    pub installation_id: DeviceInstallationId,
    pub source_digest: Sha256,
    /// Original immutable grant history cutoff, not a copy of the history.
    /// Zero/None means custody preceded the first grant. A real local admission
    /// must authenticate its own persisted proof; Cloud verifies that exact
    /// receipt against its indexed history at or before this cut.
    pub grant_revision: u64,
    pub grant_proof_hash: Option<Sha256>,
}
impl NativeDialogueExecutionControlTarget {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User
            || self.source_digest.into_bytes() == [0; 32]
            || self.grant_revision > i64::MAX as u64
            || (self.grant_revision == 0) != self.grant_proof_hash.is_none()
            || self
                .grant_proof_hash
                .is_some_and(|h| h.into_bytes() == [0; 32])
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    /// Scope comparison only. The original local admission owner additionally
    /// authenticates its retained proof; this is not a history membership test.
    pub fn matches_admission(&self, admission: &DeviceDeliveryBinding) -> bool {
        admission.work_item.is_none()
            && admission.intent_id == self.intent_id
            && admission.run_id == self.run_id
            && admission.device_id == self.device_id
            && admission.installation_id == self.installation_id
            && admission.source_digest == self.source_digest
            && self.grant_revision > 0
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let mut b = b"musteragent.device-control-target.v1\0".to_vec();
        for id in [
            self.actor.tenant_id.into_uuid(),
            self.actor.principal_id.into_uuid(),
            self.agent_id.into_uuid(),
            self.dialogue_id.into_uuid(),
            self.intent_id.into_uuid(),
            self.run_id.into_uuid(),
            self.conversation_id.into_uuid(),
            self.device_id.into_uuid(),
            self.installation_id.into_uuid(),
        ] {
            b.extend_from_slice(id.as_bytes());
        }
        b.extend_from_slice(&self.source_digest.into_bytes());
        b.extend_from_slice(&self.grant_revision.to_be_bytes());
        if let Some(hash) = self.grant_proof_hash {
            b.extend_from_slice(&hash.into_bytes());
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let mut r = Reader::new(bytes, b"musteragent.device-control-target.v1\0")?;
        let actor = Actor {
            tenant_id: r.id()?,
            principal_id: r.id()?,
            principal_kind: PrincipalKind::User,
        };
        let mut value = Self {
            actor,
            agent_id: r.id()?,
            dialogue_id: r.id()?,
            intent_id: r.id()?,
            run_id: r.id()?,
            conversation_id: r.id()?,
            device_id: r.id()?,
            installation_id: r.id()?,
            source_digest: r.hash()?,
            grant_revision: u64::from_be_bytes(r.take()?),
            grant_proof_hash: None,
        };
        if value.grant_revision > 0 {
            value.grant_proof_hash = Some(r.hash()?);
        }
        if !r.0.is_empty() || value.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueExecutionControl {
    pub cancellation_id: CommandId,
    pub command_id: CommandId,
    pub observed_activity_revision: u64,
    pub created_at: UtcTimestamp,
    pub target: NativeDialogueExecutionControlTarget,
}
impl NativeDialogueExecutionControl {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.target.validate()?;
        if self.observed_activity_revision == 0
            || self.observed_activity_revision > i64::MAX as u64
            || self.created_at.unix_millis() <= 0
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let mut b = b"musteragent.device-control.v1\0".to_vec();
        b.extend_from_slice(self.cancellation_id.into_uuid().as_bytes());
        b.extend_from_slice(self.command_id.into_uuid().as_bytes());
        b.extend_from_slice(&self.observed_activity_revision.to_be_bytes());
        b.extend_from_slice(&self.created_at.unix_millis().to_be_bytes());
        b.extend_from_slice(&self.target.canonical_bytes()?);
        Ok(b)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let mut r = Reader::new(bytes, b"musteragent.device-control.v1\0")?;
        let value = Self {
            cancellation_id: r.id()?,
            command_id: r.id()?,
            observed_activity_revision: u64::from_be_bytes(r.take()?),
            created_at: UtcTimestamp::from_unix_millis(i64::from_be_bytes(r.take()?)),
            target: NativeDialogueExecutionControlTarget::decode(r.0)?,
        };
        if value.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueExecutionNotAdmittedReceipt {
    pub control: NativeDialogueExecutionControl,
}
impl NativeDialogueExecutionNotAdmittedReceipt {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        let mut b = b"musteragent.device-not-admitted.v1\0".to_vec();
        b.extend_from_slice(&self.control.canonical_bytes()?);
        Ok(b)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let r = Reader::new(bytes, b"musteragent.device-not-admitted.v1\0")?;
        let v = Self {
            control: NativeDialogueExecutionControl::decode(r.0)?,
        };
        if v.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(v)
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct NativeDialogueExecutionControlCursor {
    pub created_at: UtcTimestamp,
    pub command_id: CommandId,
}
impl NativeDialogueExecutionControlCursor {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.created_at.unix_millis() <= 0 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueExecutionControlPage {
    pub controls: Vec<NativeDialogueExecutionControl>,
    pub next_cursor: Option<NativeDialogueExecutionControlCursor>,
    pub scan_exhausted: bool,
}
impl NativeDialogueExecutionControlPage {
    pub fn validate_after(
        &self,
        after: Option<NativeDialogueExecutionControlCursor>,
    ) -> Result<(), DialogueStoreError> {
        if let Some(a) = after {
            a.validate()?;
        }
        if self.controls.len() > DEVICE_CONTROL_PAGE_SIZE
            || self.scan_exhausted == self.next_cursor.is_some()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if let Some(n) = self.next_cursor {
            n.validate()?;
            if after.is_some_and(|a| n <= a) {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        let mut previous = after;
        for c in &self.controls {
            c.validate()?;
            let cursor = NativeDialogueExecutionControlCursor {
                created_at: c.created_at,
                command_id: c.command_id,
            };
            if previous.is_some_and(|p| cursor <= p) {
                return Err(DialogueStoreError::Corrupt);
            }
            previous = Some(cursor);
        }
        if let (Some(last), Some(next)) = (previous, self.next_cursor)
            && last > next
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
pub trait DeviceExecutionControlStore: Send + Sync {
    fn list_controls<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        after: Option<NativeDialogueExecutionControlCursor>,
    ) -> ApplicationFuture<'a, Result<NativeDialogueExecutionControlPage, DialogueStoreError>>;
    fn record_not_admitted<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        receipt: NativeDialogueExecutionNotAdmittedReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>>;
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], prefix: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > MAX_DEVICE_CONTROL_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(Self(
            bytes
                .strip_prefix(prefix)
                .ok_or(DialogueStoreError::Corrupt)?,
        ))
    }
    fn take<const N: usize>(&mut self) -> Result<[u8; N], DialogueStoreError> {
        let (a, b) = self
            .0
            .split_at_checked(N)
            .ok_or(DialogueStoreError::Corrupt)?;
        self.0 = b;
        a.try_into().map_err(|_| DialogueStoreError::Corrupt)
    }
    fn id<T: std::str::FromStr>(&mut self) -> Result<T, DialogueStoreError> {
        uuid::Uuid::from_bytes(self.take()?)
            .to_string()
            .parse()
            .map_err(|_| DialogueStoreError::Corrupt)
    }
    fn hash(&mut self) -> Result<Sha256, DialogueStoreError> {
        Sha256::from_bytes(self.take::<32>()?).map_err(|_| DialogueStoreError::Corrupt)
    }
}

#[cfg(test)]
mod tests;
