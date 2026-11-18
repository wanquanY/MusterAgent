//! Original-owner work committed atomically with message retraction. These
//! receipts describe effects to reconcile; they are not a second file catalog.
use crate::{RetractMessageAuthority, RetractMessageCommit};
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_ledger::{MAX_APPEND_OUTBOX_MESSAGES, ProposedOutbox};
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const NATIVE_FILE_RETRACTION_TOPIC: &str = "workspace.native_file.retract";

pub trait RetractionFileEffectsPlanner: Send + Sync {
    fn plan<'a>(
        &'a self,
        actor: Actor,
        command: CommandId,
        authority: &'a RetractMessageAuthority,
    ) -> ApplicationFuture<'a, Result<RetractionFileEffects, RuntimeError>>;
}

/// Planned source facts, fenced by the same original Conversation commit.
/// Native effects use the durable physical-owner queue. Managed resources are
/// reconciled by their original Resource transaction before the commit returns.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetractionFileEffects {
    pub native_outbox: Vec<ProposedOutbox>,
    pub managed_resources: Option<ManagedResourceRetractionPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedResourceRetractionPlan {
    pub invocation_ids: BTreeSet<InvocationId>,
    /// Exact objects which survive in the effective projection after retraction.
    pub retained_artifact_ids: BTreeSet<ArtifactId>,
}
impl ManagedResourceRetractionPlan {
    pub const MAX_ITEMS: usize = 4096;
    pub fn validate(&self) -> bool {
        self.invocation_ids.len() <= Self::MAX_ITEMS
            && self.retained_artifact_ids.len() <= Self::MAX_ITEMS
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFileRetraction {
    pub schema: String,
    pub tenant_id: String,
    pub principal_id: String,
    pub device_id: String,
    pub conversation_id: String,
    pub home_epoch: String,
    pub baseline_offset: String,
    pub trigger_message_id: String,
    pub source_run_id: String,
    pub workspace_id: String,
    pub invocation_id: String,
    pub path: String,
    pub sha256: String,
}

impl NativeFileRetraction {
    pub fn validate(&self) -> bool {
        self.schema == "musteragent.native-file-retraction.v1"
            && self.tenant_id.parse::<TenantId>().is_ok()
            && self.principal_id.parse::<PrincipalId>().is_ok()
            && self.device_id.parse::<DeviceId>().is_ok()
            && self.conversation_id.parse::<ConversationId>().is_ok()
            && self.trigger_message_id.parse::<MessageId>().is_ok()
            && self.source_run_id.parse::<RunId>().is_ok()
            && self.workspace_id.parse::<WorkspaceId>().is_ok()
            && self.invocation_id.parse::<InvocationId>().is_ok()
            && self
                .home_epoch
                .parse::<u64>()
                .is_ok_and(|v| v > 0 && v.to_string() == self.home_epoch)
            && self
                .baseline_offset
                .parse::<u64>()
                .is_ok_and(|v| v.to_string() == self.baseline_offset)
            && self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && !self.path.is_empty()
            && self.path.len() <= 4096
            && !self.path.contains('\\')
            && !self.path.chars().any(char::is_control)
            && self
                .path
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..")
            && self.path.split('/').next() != Some(".musteragent")
    }
}

/// Bounded immutable batches keep the Ledger transaction independent of the
/// number of physical operations. Each effect is settled separately by index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFileRetractionBatch {
    schema: String,
    pub effects: Vec<NativeFileRetraction>,
}
impl NativeFileRetractionBatch {
    pub const MAX_EFFECTS: usize = 128;
    pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

    pub fn decode(outbox: &ProposedOutbox) -> Option<Self> {
        if outbox.topic.as_str() != NATIVE_FILE_RETRACTION_TOPIC
            || outbox.payload.len() > Self::MAX_PAYLOAD_BYTES
            || Sha256::digest(&outbox.payload) != outbox.payload_hash
        {
            return None;
        }
        let value: Self = serde_json::from_slice(&outbox.payload).ok()?;
        let mut unique = BTreeSet::new();
        (value.schema == "musteragent.native-file-retraction-batch.v1"
            && !value.effects.is_empty()
            && value.effects.len() <= Self::MAX_EFFECTS
            && value.effects.iter().all(|effect| {
                effect.validate() && unique.insert((&effect.invocation_id, &effect.path))
            }))
        .then_some(value)
    }

    pub fn outboxes(
        command: CommandId,
        mut effects: Vec<NativeFileRetraction>,
    ) -> Option<Vec<ProposedOutbox>> {
        // Canonical order fixes durable effect indexes across retries.
        effects.sort_by(|a, b| (&a.invocation_id, &a.path).cmp(&(&b.invocation_id, &b.path)));
        let mut unique = BTreeSet::new();
        if effects.len() > Self::MAX_EFFECTS * MAX_APPEND_OUTBOX_MESSAGES
            || !effects.iter().all(|effect| {
                effect.validate() && unique.insert((&effect.invocation_id, &effect.path))
            })
        {
            return None;
        }
        effects
            .chunks(Self::MAX_EFFECTS)
            .enumerate()
            .map(|(index, chunk)| {
                let batch = Self {
                    schema: "musteragent.native-file-retraction-batch.v1".into(),
                    effects: chunk.to_vec(),
                };
                let payload = serde_json::to_vec(&batch).ok()?;
                if payload.len() > Self::MAX_PAYLOAD_BYTES {
                    return None;
                }
                let key = format!("native-file-retraction:{command}:{index}");
                Some(ProposedOutbox {
                    message_id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes())
                        .to_string()
                        .parse()
                        .ok()?,
                    topic: DottedKey::new(NATIVE_FILE_RETRACTION_TOPIC).ok()?,
                    payload_hash: Sha256::digest(&payload),
                    payload,
                })
            })
            .collect()
    }
}

/// Stores validate the effect envelope in the same transaction as the source
/// ledger facts. No Product request accepts paths or physical proofs here.
pub fn validate_file_retraction_outbox(commit: &RetractMessageCommit) -> bool {
    let mut unique = BTreeSet::new();
    commit.append.validate_bounds().is_ok()
        && commit.append.outbox.iter().all(|outbox| {
            NativeFileRetractionBatch::decode(outbox).is_some_and(|batch| {
                batch.effects.iter().all(|effect| {
                    unique.insert((effect.invocation_id.clone(), effect.path.clone()))
                        && effect.tenant_id == commit.actor.tenant_id.to_string()
                        && effect.principal_id == commit.actor.principal_id.to_string()
                        && effect.conversation_id == commit.append.conversation_id.to_string()
                        && effect.home_epoch == commit.append.expected_home_epoch.get().to_string()
                        && effect.baseline_offset == commit.append.expected_offset.get().to_string()
                        && commit.targets.iter().any(|target| {
                            effect.trigger_message_id == target.message_id.to_string()
                        })
                })
            })
        })
}

#[cfg(test)]
mod tests;
