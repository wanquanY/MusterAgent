//! Read-only execution context for an already admitted Native Dialogue Run.
//! A receipt identifies immutable facts; it is never authority. Every read is
//! independently qualified by the original custody, permission and erasure owners.
use crate::{
    DeviceDeliveryBinding, DialogueExecutionPreparation, DialogueStoreError,
    OriginDeviceExecutionTarget, WorkItemDeviceExecutionPreparation,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_execution_context::{
    DialogueContextCheckpoint, DialogueContextCut, DialogueContextSourcePage,
};
use musteragent_identity::{Actor, VersionedKey};
use musteragent_runtime_contract::ApplicationFuture;
use std::num::NonZeroU64;
mod contents;
pub use contents::*;

pub const DEVICE_CONTEXT_REQUEST_LIMIT: usize = 8 * 1024;
pub const DEVICE_CONTEXT_RPC_REQUEST_LIMIT: usize = 40 * 1024;
// Original 5 MiB source/context plus 7 MiB deduplicated immutable preferences.
pub const DEVICE_CONTEXT_STATE_LIMIT: usize = 12 * 1024 * 1024;
pub const DEVICE_CONTEXT_PAGE_LIMIT: usize = 272 * 1024;
pub const DEVICE_CONTEXT_CHUNK_BYTES: usize = 1024 * 1024;
pub const DEVICE_CONTEXT_CHUNK_LIMIT: usize = DEVICE_CONTEXT_CHUNK_BYTES + 8192;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeviceContextSource {
    Intent(Box<DialogueExecutionPreparation>),
    WorkItem(Box<WorkItemDeviceExecutionPreparation>),
}
impl DeviceContextSource {
    pub fn cut(&self) -> &DialogueContextCut {
        match self {
            Self::Intent(source) => &source.context_cut,
            Self::WorkItem(source) => &source.preparation().context_cut,
        }
    }
    pub fn validate_for(&self, binding: &DeviceDeliveryBinding) -> Result<(), DialogueStoreError> {
        validate_binding(binding)?;
        let bad = DialogueStoreError::Corrupt;
        match self {
            Self::Intent(source) => {
                source.validate()?;
                let local = source.local_execution_binding(binding.device_id)?;
                if binding.work_item.is_some()
                    || source.scope.source_intent_id != binding.intent_id
                    || source.identities.run_id()? != binding.run_id
                    || local.installation_id() != binding.installation_id
                    || local.digest() != binding.source_digest
                {
                    return Err(bad);
                }
            }
            Self::WorkItem(source) => {
                source.validate()?;
                let p = source.preparation();
                let subject = binding.work_item.ok_or(bad)?;
                if source.target().device_id != binding.device_id
                    || source.target().installation_id != binding.installation_id
                    || source.source_sha256()? != binding.source_digest
                    || p.work_item.source_intent_id != binding.intent_id
                    || p.key.identities().run_id()? != binding.run_id
                    || p.key.work_item_id != subject.work_item_id
                    || p.key.goal_revision.get() != subject.goal_revision
                    || p.key.control_revision.get() != subject.control_revision
                    || p.key.assignment_epoch.get() != subject.assignment_epoch
                {
                    return Err(bad);
                }
            }
        }
        Ok(())
    }
}

/// Current instructions are separate from immutable admission configuration.
/// A missing reference means the original current owner has no instructions.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextInstructions {
    pub revision: NonZeroU64,
    pub content_ref: Option<ContentRef>,
}

/// The original sealed Context policy is a checkpoint selector, not authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextStateRequest {
    pub include_scene_members: bool,
    /// Explicit capability negotiation; false preserves the original reader.
    pub include_approval_context: bool,
    /// Explicit optional execution-observation capability, independent of source authority.
    pub include_execution_context: bool,
    pub binding: DeviceDeliveryBinding,
    pub compaction_policy_ref: VersionedKey,
}
impl DeviceContextStateRequest {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        validate_binding(&self.binding)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextState {
    pub scene_members: Vec<musteragent_execution_context::AgentSceneMember>,
    pub binding: DeviceDeliveryBinding,
    pub compaction_policy_ref: VersionedKey,
    pub source: DeviceContextSource,
    /// Same-batch read consistency only. Never a grant, lease or durable authority.
    /// Includes original current permission/instructions/erasure observations.
    pub fingerprint: Sha256,
    /// Original fixed-cut source/erasure observation, excluding instructions and
    /// checkpoint selection. A change invalidates previously imported history;
    /// future admissions may observe already-erased entries normally.
    pub source_observation_sha256: Sha256,
    pub instructions: DeviceContextInstructions,
    pub checkpoint: Option<DialogueContextCheckpoint>,
    pub approval_context: Option<musteragent_execution_context::DialogueApprovalContext>,
    pub execution_context: Option<musteragent_execution_context::DialogueExecutionContext>,
}
impl DeviceContextState {
    /// Dynamic execution observations are not source/authorization identity.
    /// Keep the original full comparison for every other field.
    pub fn same_source_observation(&self, other: &Self) -> bool {
        self.binding == other.binding
            && self.compaction_policy_ref == other.compaction_policy_ref
            && self.source == other.source
            && self.fingerprint == other.fingerprint
            && self.source_observation_sha256 == other.source_observation_sha256
            && self.instructions == other.instructions
            && self.checkpoint == other.checkpoint
            && self.approval_context == other.approval_context
    }
    pub fn validate_for(
        &self,
        request: &DeviceContextStateRequest,
    ) -> Result<(), DialogueStoreError> {
        let bad = DialogueStoreError::Corrupt;
        request.validate()?;
        self.source.validate_for(&request.binding)?;
        nonzero(self.fingerprint)?;
        nonzero(self.source_observation_sha256)?;
        if self.binding != request.binding
            || self.compaction_policy_ref != request.compaction_policy_ref
            || self.instructions.revision.get() > i64::MAX as u64
        {
            return Err(bad);
        }
        if (!request.include_scene_members && !self.scene_members.is_empty())
            || !musteragent_execution_context::valid_scene_members(
                self.source.cut().agent_id,
                &self.scene_members,
            )
        {
            return Err(bad);
        }
        if request.include_execution_context != self.execution_context.is_some() {
            return Err(bad);
        }
        if let Some(context) = &self.execution_context {
            context.validate().map_err(|_| bad)?;
            let reply = match &self.source {
                DeviceContextSource::Intent(p) => p.intent.reply_binding.as_ref(),
                DeviceContextSource::WorkItem(_) => None,
            };
            if context.reply_binding.as_ref() != reply {
                return Err(bad);
            }
            if context.frozen_cut != *self.source.cut()
                || context.current.run_id != self.binding.run_id
                || context.current.source.intent_id() != Some(self.binding.intent_id)
                || context.current.source.task_id()
                    != self.binding.work_item.map(|w| w.work_item_id)
            {
                return Err(bad);
            }
        }
        if request.include_approval_context != self.approval_context.is_some() {
            return Err(bad);
        }
        if let Some(context) = &self.approval_context {
            context.validate().map_err(|_| bad)?;
            if context.frozen_cut != *self.source.cut() {
                return Err(bad);
            }
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate().map_err(|_| bad)?;
            let cut = self.source.cut();
            if checkpoint.policy_ref != self.compaction_policy_ref
                || !cut.same_dialogue(&checkpoint.coverage)
                || checkpoint.coverage.through.seq > cut.through.seq
                || (checkpoint.coverage.through.seq == cut.through.seq
                    && checkpoint.coverage.source_chain_sha256 != cut.source_chain_sha256)
            {
                return Err(bad);
            }
        }
        Ok(())
    }
}

/// Locator is a position in the admitted source, not a caller-supplied ContentRef.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeviceContextContentLocator {
    EntryBody { seq: u64 },
    EntryAttachment { seq: u64, ordinal: u32 },
    AcceptedInputAttachment { ordinal: u32 },
    AcceptedReplyContent { ordinal: u32 },
    CheckpointSummary,
    CheckpointManifest,
    CurrentInstructions,
}
impl DeviceContextContentLocator {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if matches!(self, Self::EntryBody { seq } | Self::EntryAttachment { seq, .. }
            if *seq == 0 || *seq > i64::MAX as u64)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if matches!(self, Self::AcceptedReplyContent { ordinal } if *ordinal >= 2) {
            return Err(DialogueStoreError::Corrupt);
        }
        // The source owner additionally requires the exact ordinal to exist.
        if matches!(self, Self::EntryAttachment { ordinal, .. } | Self::AcceptedInputAttachment { ordinal }
            if *ordinal >= 64)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextPageRequest {
    pub original_entries: bool,
    /// Explicit capability negotiation; false preserves the original reader.
    pub include_approval_context: bool,
    /// Explicit optional execution-observation capability, independent of source authority.
    pub include_execution_context: bool,
    pub binding: DeviceDeliveryBinding,
    pub compaction_policy_ref: VersionedKey,
    pub fingerprint: Sha256,
    pub from_seq: u64,
}
impl DeviceContextPageRequest {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        validate_binding(&self.binding)?;
        nonzero(self.fingerprint)?;
        if self.from_seq == 0 || self.from_seq > i64::MAX as u64 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextPage {
    pub binding: DeviceDeliveryBinding,
    pub compaction_policy_ref: VersionedKey,
    pub fingerprint: Sha256,
    pub page: DialogueContextSourcePage,
}
impl DeviceContextPage {
    pub fn validate_for(
        &self,
        request: &DeviceContextPageRequest,
    ) -> Result<(), DialogueStoreError> {
        request.validate()?;
        self.page
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if self.binding != request.binding
            || self.compaction_policy_ref != request.compaction_policy_ref
            || self.fingerprint != request.fingerprint
            || self.page.from_seq != request.from_seq
            || self
                .page
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.policy_ref != self.compaction_policy_ref)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextContentRequest {
    /// Explicit capability negotiation; false preserves the original reader.
    pub include_approval_context: bool,
    /// Explicit optional execution-observation capability, independent of source authority.
    pub include_execution_context: bool,
    pub binding: DeviceDeliveryBinding,
    pub compaction_policy_ref: VersionedKey,
    pub fingerprint: Sha256,
    pub locator: DeviceContextContentLocator,
    pub offset: u64,
}
impl DeviceContextContentRequest {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        validate_binding(&self.binding)?;
        nonzero(self.fingerprint)?;
        self.locator.validate()?;
        if self.offset > i64::MAX as u64 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextChunk {
    pub binding: DeviceDeliveryBinding,
    pub compaction_policy_ref: VersionedKey,
    pub fingerprint: Sha256,
    pub locator: DeviceContextContentLocator,
    pub content_ref: ContentRef,
    pub offset: u64,
    pub total: u64,
    pub bytes: Vec<u8>,
    pub eof: bool,
}
impl DeviceContextChunk {
    pub fn validate_for(
        &self,
        request: &DeviceContextContentRequest,
    ) -> Result<(), DialogueStoreError> {
        request.validate()?;
        let bad = DialogueStoreError::Corrupt;
        let end = self
            .offset
            .checked_add(self.bytes.len() as u64)
            .ok_or(bad)?;
        if self.binding != request.binding
            || self.compaction_policy_ref != request.compaction_policy_ref
            || self.fingerprint != request.fingerprint
            || self.locator != request.locator
            || self.offset != request.offset
            || self.total != self.content_ref.byte_length
            || self.total > i64::MAX as u64
            || end > self.total
            || self.eof != (end == self.total)
            || self.bytes.len()
                != (self.total - self.offset).min(DEVICE_CONTEXT_CHUNK_BYTES as u64) as usize
            || (self.offset == 0
                && self.eof
                && Sha256::digest(&self.bytes) != self.content_ref.sha256)
        {
            return Err(bad);
        }
        Ok(())
    }
}

/// Cloud original source owner. Revalidate after all asynchronous Content reads.
/// Expired grants authenticate retained custody only; current authority and
/// erasure remain mandatory. The local Run need not exist in the Cloud ledger.
pub trait DeviceContextStore: Send + Sync {
    fn read_contents<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextContentsRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextContents, DialogueStoreError>> {
        Box::pin(async move {
            request.validate()?;
            let mut chunks = Vec::new();
            for locator in &request.locators {
                let chunk = self
                    .read_content(actor, target, &request.content_request(*locator))
                    .await?;
                contents::append_complete(&mut chunks, request, chunk)?;
            }
            let contents = DeviceContextContents { chunks };
            contents.validate_for(request)?;
            Ok(contents)
        })
    }
    fn get_state<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextStateRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextState, DialogueStoreError>>;
    fn read_page<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextPageRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextPage, DialogueStoreError>>;
    fn read_content<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextContentRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextChunk, DialogueStoreError>>;
}

/// Native's current authenticated account session. No caller-selected Actor or
/// target is accepted. Losing that session fences every read and model dispatch.
pub trait DeviceContextTransport: Send + Sync {
    fn read_contents<'a>(
        &'a self,
        request: &'a DeviceContextContentsRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextContents, DialogueStoreError>> {
        Box::pin(async move {
            request.validate()?;
            let mut chunks = Vec::new();
            for locator in &request.locators {
                let chunk = self
                    .read_content(&request.content_request(*locator))
                    .await?;
                contents::append_complete(&mut chunks, request, chunk)?;
            }
            let contents = DeviceContextContents { chunks };
            contents.validate_for(request)?;
            Ok(contents)
        })
    }
    fn get_state<'a>(
        &'a self,
        request: &'a DeviceContextStateRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextState, DialogueStoreError>>;
    fn read_page<'a>(
        &'a self,
        request: &'a DeviceContextPageRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextPage, DialogueStoreError>>;
    fn read_content<'a>(
        &'a self,
        request: &'a DeviceContextContentRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextChunk, DialogueStoreError>>;
}

fn nonzero(hash: Sha256) -> Result<(), DialogueStoreError> {
    if hash.into_bytes() == [0; 32] {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}
fn validate_binding(binding: &DeviceDeliveryBinding) -> Result<(), DialogueStoreError> {
    binding
        .validate_subject()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    for hash in [
        binding.source_digest,
        binding.snapshot_hash,
        binding.grant_proof_hash,
    ] {
        nonzero(hash)?;
    }
    Ok(())
}
