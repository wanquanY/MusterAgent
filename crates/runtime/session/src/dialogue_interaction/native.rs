//! A signed observation and command inbox for the original Device Interaction.
//! These values never transition a Cloud Interaction or authorize a Tool effect.
use super::{
    DialogueInteractionAction as Action, DialogueInteractionResolution as Resolution,
    DialogueInteractionTarget, DialogueInteractionView,
};
use crate::{DeviceDeliveryBinding, OriginDeviceExecutionTarget};
use musteragent_approval::{InteractionKind, InteractionStatus, InteractionView};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, AgentDialogueId, CapabilityProviderId, CommandId, ConversationId, CorrelationId,
    DialogueEntryId, InteractionId, InvocationId, RunSeq, UtcTimestamp,
};
use musteragent_runtime_contract::{
    ApplicationFuture, CommandAck, CommandMetadata, ErrorCode, RuntimeError,
};

pub const NATIVE_INTERACTION_MAX_PROMPT_BYTES: usize = 1024 * 1024;
pub const NATIVE_INTERACTION_MAX_COMMANDS: u32 = 32;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueInteractionApprovalPolicy {
    /// Exact host policy identity. Recognition and current consent are checked by
    /// NativeDialogueInteractionControlAuthorizer; structural validation is not authority.
    pub policy_sha256: Sha256,
    pub tool_key: String,
    pub capability_key: String,
    pub provider_id: CapabilityProviderId,
    pub effect: musteragent_policy::Effect,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueInteractionProjection {
    pub admission: DeviceDeliveryBinding,
    pub conversation_id: ConversationId,
    pub invocation_id: InvocationId,
    pub interaction: InteractionView,
    pub prompt_bytes: Vec<u8>,
    pub observed_run_sequence: RunSeq,
    /// Derived from the original Run wait/cancellation state, never the client UI.
    pub control_open: bool,
    pub approval_policy: Option<NativeDialogueInteractionApprovalPolicy>,
    pub resolution: Option<Resolution>,
}
impl NativeDialogueInteractionProjection {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        use InteractionKind as Kind;
        use InteractionStatus as Status;
        self.admission
            .validate_subject()
            .map_err(|_| ErrorCode::InvalidCommand)?;
        let i = &self.interaction;
        if i.run_id != self.admission.run_id
            || [
                self.admission.source_digest,
                self.admission.snapshot_hash,
                self.admission.grant_proof_hash,
            ]
            .iter()
            .any(|h| h.into_bytes() == [0; 32])
            || self.prompt_bytes.len() > NATIVE_INTERACTION_MAX_PROMPT_BYTES
            || !content_matches(&i.prompt_ref, &self.prompt_bytes)
            || (self.control_open && i.status != Status::Pending)
            || (self.approval_policy.is_some() && i.kind != Kind::Approval)
            || i.expires_at.is_some_and(|t| t <= i.requested_at)
            || i.responded_at.is_some_and(|t| t < i.requested_at)
        {
            return Err(ErrorCode::InvalidCommand);
        }
        match (i.status, i.kind, &self.resolution, i.responded_at) {
            (Status::Pending | Status::Expired | Status::Cancelled, _, None, None) => {}
            (
                Status::Responded,
                Kind::Clarification | Kind::Selection,
                Some(Resolution::Answer { reference, bytes }),
                Some(_),
            ) if !bytes.is_empty()
                && bytes.len() <= musteragent_runtime_interaction::MAX_INTERACTION_INPUT_BYTES
                && content_matches(reference, bytes) => {}
            (
                Status::Responded,
                Kind::Approval | Kind::Confirmation,
                Some(Resolution::Approved | Resolution::Denied),
                Some(_),
            ) => {}
            // Local session approvals are resolved observations without a remote
            // ApproveOnce decision. Secure material is never projected.
            (Status::Responded, Kind::Approval | Kind::Credential | Kind::OAuth, None, Some(_)) => {
            }
            _ => return Err(ErrorCode::InvalidCommand),
        }
        if let Some(policy) = &self.approval_policy
            && (policy.policy_sha256.into_bytes() == [0; 32]
                || policy.tool_key.len() > 256
                || policy.capability_key.len() > 256
                || policy.tool_key.is_empty()
                || policy.capability_key.is_empty())
        {
            return Err(ErrorCode::PolicyDenied);
        }
        Ok(())
    }
    /// Stable immutable request identity, independent of subsequent Run progress.
    pub fn request_fingerprint(&self) -> Sha256 {
        let mut b = b"musteragent.native-interaction-request.v1\0".to_vec();
        let a = &self.admission;
        for id in [
            a.intent_id.into_uuid(),
            a.run_id.into_uuid(),
            a.device_id.into_uuid(),
            a.installation_id.into_uuid(),
            self.conversation_id.into_uuid(),
            self.invocation_id.into_uuid(),
            self.interaction.interaction_id.into_uuid(),
        ] {
            b.extend_from_slice(id.as_bytes());
        }
        b.push(u8::from(a.work_item.is_some()));
        if let Some(t) = a.work_item {
            b.extend_from_slice(t.work_item_id.into_uuid().as_bytes());
            for n in [t.goal_revision, t.control_revision, t.assignment_epoch] {
                b.extend_from_slice(&n.to_be_bytes());
            }
        }
        for h in [a.source_digest, a.snapshot_hash, a.grant_proof_hash] {
            b.extend_from_slice(&h.into_bytes());
        }
        b.push(kind_tag(self.interaction.kind));
        reference_bytes(&mut b, &self.interaction.prompt_ref);
        bytes(
            &mut b,
            self.interaction
                .response_schema
                .schema_key
                .as_str()
                .as_bytes(),
        );
        b.extend_from_slice(&self.interaction.response_schema.schema_hash.into_bytes());
        b.extend_from_slice(&self.interaction.requested_at.unix_millis().to_be_bytes());
        time(&mut b, self.interaction.expires_at);
        b.push(u8::from(self.approval_policy.is_some()));
        if let Some(p) = &self.approval_policy {
            b.extend_from_slice(&p.policy_sha256.into_bytes());
            bytes(&mut b, p.tool_key.as_bytes());
            bytes(&mut b, p.capability_key.as_bytes());
            b.extend_from_slice(p.provider_id.into_uuid().as_bytes());
            b.push(effect_tag(p.effect));
        }
        Sha256::digest(b)
    }
    pub fn state_fingerprint(&self) -> Sha256 {
        let mut b = b"musteragent.native-interaction-state.v1\0".to_vec();
        b.extend_from_slice(&self.request_fingerprint().into_bytes());
        b.push(status_tag(self.interaction.status));
        b.push(u8::from(self.control_open));
        time(&mut b, self.interaction.responded_at);
        match &self.resolution {
            None => b.push(0),
            Some(Resolution::Answer { reference, .. }) => {
                b.push(1);
                reference_bytes(&mut b, reference);
            }
            Some(Resolution::Approved) => b.push(2),
            Some(Resolution::Denied) => b.push(3),
        }
        Sha256::digest(b)
    }
    pub fn fingerprint(&self) -> Sha256 {
        let mut b = b"musteragent.native-interaction-observation.v1\0".to_vec();
        b.extend_from_slice(&self.state_fingerprint().into_bytes());
        b.extend_from_slice(&self.observed_run_sequence.get().to_be_bytes());
        Sha256::digest(b)
    }
    /// The execution Owner supplies Tool eligibility; current executor consent
    /// is a separate authority fact and must be revalidated by the caller.
    pub fn actions(&self, now: UtcTimestamp, remote_tool_approval: bool) -> Vec<Action> {
        if !self.control_open
            || self.interaction.status != InteractionStatus::Pending
            || self.interaction.expires_at.is_some_and(|t| t <= now)
        {
            return vec![];
        }
        match self.interaction.kind {
            InteractionKind::Clarification | InteractionKind::Selection => {
                vec![Action::Answer, Action::Cancel]
            }
            InteractionKind::Confirmation => vec![Action::ApproveOnce, Action::Deny],
            InteractionKind::Approval if remote_tool_approval && self.approval_policy.is_some() => {
                vec![Action::ApproveOnce, Action::Deny]
            }
            InteractionKind::OAuth
                if self.interaction.response_schema.schema_key.as_str()
                    == musteragent_runtime_interaction::CAPABILITY_RESPONSE_SCHEMA =>
            {
                vec![Action::OpenSecureFlow, Action::Cancel]
            }
            InteractionKind::Credential | InteractionKind::OAuth => vec![Action::Cancel],
            _ => vec![],
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueInteractionControlResponse {
    Answer(Vec<u8>),
    ApproveOnce,
    Deny,
    Cancel,
    CompleteCapability(Vec<u8>),
}
impl DialogueInteractionControlResponse {
    pub fn action(&self) -> Action {
        match self {
            Self::Answer(_) => Action::Answer,
            Self::ApproveOnce => Action::ApproveOnce,
            Self::Deny => Action::Deny,
            Self::Cancel => Action::Cancel,
            Self::CompleteCapability(_) => Action::OpenSecureFlow,
        }
    }
    pub fn validate(&self) -> Result<(), ErrorCode> {
        if let Self::CompleteCapability(v) = self
            && (v.len() > 4096
                || serde_json::from_slice::<musteragent_runtime_interaction::CapabilityDescriptor>(
                    v,
                )
                .ok()
                .is_none_or(|d| d.validate().is_err()))
        {
            return Err(ErrorCode::InvalidCommand);
        }
        if let Self::Answer(v) = self
            && (v.is_empty()
                || v.len() > musteragent_runtime_interaction::MAX_INTERACTION_INPUT_BYTES)
        {
            return Err(ErrorCode::InvalidCommand);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueInteractionCommand {
    pub command_id: CommandId,
    pub interaction_id: InteractionId,
    pub target: DialogueInteractionTarget,
    pub admission: DeviceDeliveryBinding,
    pub response: DialogueInteractionControlResponse,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueInteractionPublicationReceipt {
    pub interaction_id: InteractionId,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub projection_sha256: Sha256,
    pub control_revision: Sha256,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueInteractionControlDelivery {
    Queued,
    Applied(CommandAck),
    Rejected(ErrorCode),
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueInteractionControlReceipt {
    pub command_id: CommandId,
    pub delivery: DialogueInteractionControlDelivery,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueInteractionAcknowledgement {
    pub publication: NativeDialogueInteractionPublicationReceipt,
    pub control: DialogueInteractionControlReceipt,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NativeDialogueInteractionInstallationNamespace {
    DialogueOrigin,
    NativeClient,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NativeDialogueInteractionCommandCursor {
    pub queued_at: UtcTimestamp,
    pub command_id: CommandId,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeDialogueInteractionCommandPage {
    pub commands: Vec<NativeDialogueInteractionCommand>,
    pub next_cursor: Option<NativeDialogueInteractionCommandCursor>,
    pub scan_exhausted: bool,
}
/// Original Publication transport plus a durable command inbox. Applied receipts
/// are accepted only from the authenticated Device which owns the admission.
pub trait NativeDialogueInteractionStore: Send + Sync {
    fn read_control(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<DialogueInteractionControlReceipt, RuntimeError>>;
    fn publish<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        projection: &'a NativeDialogueInteractionProjection,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionPublicationReceipt, RuntimeError>>;
    fn commands<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        namespace: NativeDialogueInteractionInstallationNamespace,
        after: Option<NativeDialogueInteractionCommandCursor>,
        limit: u32,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionCommandPage, RuntimeError>>;
    fn acknowledge<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        command: &'a NativeDialogueInteractionCommand,
        projection: &'a NativeDialogueInteractionProjection,
        outcome: Result<CommandAck, ErrorCode>,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionAcknowledgement, RuntimeError>>;
    fn read(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<DialogueInteractionView>, RuntimeError>>;
    fn submit(
        &self,
        metadata: CommandMetadata,
        target: DialogueInteractionTarget,
        response: DialogueInteractionControlResponse,
    ) -> ApplicationFuture<'_, Result<Option<DialogueInteractionControlReceipt>, RuntimeError>>;
}
fn content_matches(r: &ContentRef, b: &[u8]) -> bool {
    r.byte_length == b.len() as u64 && r.sha256 == Sha256::digest(b)
}
fn bytes(out: &mut Vec<u8>, v: &[u8]) {
    out.extend_from_slice(&(v.len() as u64).to_be_bytes());
    out.extend_from_slice(v);
}
fn time(out: &mut Vec<u8>, v: Option<UtcTimestamp>) {
    out.push(u8::from(v.is_some()));
    if let Some(t) = v {
        out.extend_from_slice(&t.unix_millis().to_be_bytes());
    }
}
fn reference_bytes(out: &mut Vec<u8>, r: &ContentRef) {
    out.extend_from_slice(r.content_id.into_uuid().as_bytes());
    bytes(out, r.media_type.as_str().as_bytes());
    out.extend_from_slice(&r.byte_length.to_be_bytes());
    out.extend_from_slice(&r.sha256.into_bytes());
}
fn kind_tag(k: InteractionKind) -> u8 {
    match k {
        InteractionKind::Approval => 1,
        InteractionKind::Clarification => 2,
        InteractionKind::Selection => 3,
        InteractionKind::Credential => 4,
        InteractionKind::OAuth => 5,
        InteractionKind::Confirmation => 6,
    }
}
fn status_tag(s: InteractionStatus) -> u8 {
    match s {
        InteractionStatus::Pending => 1,
        InteractionStatus::Responded => 2,
        InteractionStatus::Expired => 3,
        InteractionStatus::Cancelled => 4,
    }
}
fn effect_tag(e: musteragent_policy::Effect) -> u8 {
    use musteragent_policy::Effect;
    match e {
        Effect::Read => 1,
        Effect::WriteReversible => 2,
        Effect::WriteExternal => 3,
        Effect::Destructive => 4,
    }
}

/// Current Backend membership and immutable executor consent are resolved by
/// their original authority, not inferred from a signed historical observation.
#[derive(Debug, Clone)]
pub struct NativeDialogueInteractionControlAuthority {
    pub target: OriginDeviceExecutionTarget,
    pub remote_tool_approval: bool,
}
pub trait NativeDialogueInteractionControlAuthorizer: Send + Sync {
    fn qualify<'a>(
        &'a self,
        actor: Actor,
        agent_id: musteragent_identity::AgentId,
        admission: &'a DeviceDeliveryBinding,
        approval_policy: Option<Sha256>,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionControlAuthority, RuntimeError>>;
}
