use std::collections::BTreeSet;

use musteragent_activity::{Activity, ActivityKind, ActivityStatus};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_conversation::{
    AdmissionMessage, ConversationTitleOrigin, MessageRole, Participant, ParticipantKind,
};
use musteragent_event_envelope::{ExecutionScope, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_execution_grant::{
    EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1, EncodedExecutionGrantProof, ExecutionGrant,
};
use musteragent_identity::{
    Actor, AgentId, ConversationSeq, DelegationId, DottedKey, DreamJobId, GoalId, GoalPursuitId,
    InterSessionMessageId, InvocationId, LedgerRecordId, PrincipalId, PrincipalKind, RunSeq,
    RuntimeSnapshotId, UtcTimestamp, VersionedKey,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{AuxiliaryModelRole, SignedModelRouteHash};
use musteragent_run::{Run, RunStatus};
use musteragent_runtime_contract::{ApplicationFuture, DisplayText, TraceContext};
use musteragent_runtime_memory::MaintenanceRuntimeSnapshot;
use thiserror::Error;

use crate::{AppendReceipt, AppendRequest, LedgerError};

const ADMISSION_RECORD_COUNT: usize = 3;
const DELEGATION_ADMISSION_RECORD_COUNT: usize = 4;
const COORDINATION_ADMISSION_RECORD_COUNT: usize = 3;
const MAINTENANCE_ADMISSION_RECORD_COUNT: usize = 3;
const STANDING_GOAL_ADMISSION_RECORD_COUNT: usize = 3;
const RETRY_ADMISSION_RECORD_COUNT: usize = 2;

/// Optional canonical title fact committed with the first user-message
/// admission. Keeping it inside the admission durability unit prevents a
/// message/title split if either process or transport fails.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionConversationTitle {
    pub record_id: LedgerRecordId,
    pub title: DisplayText,
    pub origin: ConversationTitleOrigin,
    pub set_by: PrincipalId,
}

/// Deterministically encoded, immutable Runtime configuration prepared by the
/// admission resolver. The typed snapshot contract is introduced in P4; this
/// persistence envelope already enforces immutable identity and content hash.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSnapshotBlob {
    pub runtime_snapshot_id: RuntimeSnapshotId,
    pub schema_version: DottedKey,
    pub encoded_snapshot: Vec<u8>,
    pub content_hash: Sha256,
    pub created_at: UtcTimestamp,
    /// Transport copy of the exact selection inside `encoded_snapshot`, used
    /// only to insert auditable Memory refs in the same admission transaction.
    pub memory_selection: MemorySelectionSnapshot,
    /// Transport copy of exact governed Instinct revisions inside the sealed
    /// Snapshot, used only to insert causal usage refs atomically.
    pub instinct_selections: Vec<musteragent_runtime_snapshot::InstinctSelectionSnapshot>,
    pub memory_agent_id: AgentId,
    /// Signature-verified claims consumed atomically with this Run.
    pub execution_grant: ExecutionGrant,
    /// Exact signed control-plane envelope retained for a Device Run that may
    /// call Backend-owned Capabilities. Cloud persistence may leave this
    /// absent because its trusted Runtime-to-Backend channel is server-side.
    pub execution_grant_proof: Option<EncodedExecutionGrantProof>,
}

impl RuntimeSnapshotBlob {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        if self.encoded_snapshot.is_empty() {
            return Err(AdmissionBatchError::EmptySnapshot);
        }
        if Sha256::digest(&self.encoded_snapshot) != self.content_hash {
            return Err(AdmissionBatchError::SnapshotHashMismatch);
        }
        self.execution_grant
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidExecutionGrant)?;
        if let Some(proof) = &self.execution_grant_proof {
            proof
                .validate_for(&self.execution_grant)
                .map_err(|_| AdmissionBatchError::InvalidExecutionGrantProof)?;
        }
        self.memory_selection
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidMemorySelection)?;
        if self.instinct_selections.len() > 100
            || self
                .instinct_selections
                .windows(2)
                .any(|pair| pair[0].instinct_id >= pair[1].instinct_id)
            || self
                .instinct_selections
                .iter()
                .any(|instinct| instinct.validate().is_err())
        {
            return Err(AdmissionBatchError::InvalidInstinctSelection);
        }
        Ok(())
    }
}

/// Credential-bound persistence envelope for a non-interactive Maintenance
/// Snapshot. Its typed, sealed content has no Memory, Skill, Tool, Capability,
/// prompt or primary-model fields by construction.
#[derive(Debug, Clone, PartialEq)]
pub struct MaintenanceRuntimeSnapshotBlob {
    pub runtime_snapshot_id: RuntimeSnapshotId,
    pub snapshot: MaintenanceRuntimeSnapshot,
    pub memory_agent_id: AgentId,
    pub execution_grant: ExecutionGrant,
    pub execution_grant_proof: Option<EncodedExecutionGrantProof>,
}

impl MaintenanceRuntimeSnapshotBlob {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.snapshot
            .content()
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidMaintenanceSnapshot)?;
        self.execution_grant
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidExecutionGrant)?;
        if let Some(proof) = &self.execution_grant_proof {
            proof
                .validate_for(&self.execution_grant)
                .map_err(|_| AdmissionBatchError::InvalidExecutionGrantProof)?;
        }
        if self.memory_agent_id != self.snapshot.content().target_agent_id {
            return Err(AdmissionBatchError::InvalidMaintenanceSnapshot);
        }
        Ok(())
    }
}

/// One all-or-nothing message-backed Run admission durability unit.
///
/// Product Application Services own which closed [`musteragent_activity::ActivityKind`] may be
/// created by a given entrypoint. This persistence contract only requires the
/// Activity and RunContext to agree; it must not collapse Coordination, Automation or
/// Delegation into Chat.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionBatch {
    pub actor: Actor,
    /// Original ingress trace persisted with the Run and reused by recovered
    /// Attempts and outbound Capability calls.
    pub trace_context: TraceContext,
    pub append: AppendRequest,
    pub message_record_id: LedgerRecordId,
    pub conversation_title: Option<AdmissionConversationTitle>,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub message: AdmissionMessage,
    pub activity: Activity,
    pub run: Run,
    pub runtime_snapshot: RuntimeSnapshotBlob,
}

impl AdmissionBatch {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.append
            .validate()
            .map_err(AdmissionBatchError::Ledger)?;
        self.runtime_snapshot.validate()?;
        self.run
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidRun)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != PrincipalKind::User
        {
            return Err(AdmissionBatchError::InvalidActor);
        }

        let expected_record_count =
            ADMISSION_RECORD_COUNT + usize::from(self.conversation_title.is_some());
        if self.append.records.len() != expected_record_count || self.append.outbox.len() != 1 {
            return Err(AdmissionBatchError::InvalidBatchShape);
        }
        let mut record_ids = vec![
            self.message_record_id,
            self.activity_record_id,
            self.run_record_id,
        ];
        if let Some(title) = &self.conversation_title {
            record_ids.push(title.record_id);
        }
        if record_ids.iter().copied().collect::<BTreeSet<_>>().len() != record_ids.len() {
            return Err(AdmissionBatchError::DuplicateSemanticRecord);
        }
        let title_offset = usize::from(self.conversation_title.is_some());
        if self.append.records[0].record_id != self.message_record_id
            || self
                .conversation_title
                .as_ref()
                .is_some_and(|title| self.append.records[1].record_id != title.record_id)
            || self.append.records[1 + title_offset].record_id != self.activity_record_id
            || self.append.records[2 + title_offset].record_id != self.run_record_id
        {
            return Err(AdmissionBatchError::InvalidRecordOrder);
        }
        let message_record = self.record(self.message_record_id)?;
        let activity_record = self.record(self.activity_record_id)?;
        let run_record = self.record(self.run_record_id)?;
        if self.append.records.iter().any(|record| {
            record.record_kind != LedgerRecordKind::DomainEvent
                || record.producer != Producer::RuntimeApi
        }) {
            return Err(AdmissionBatchError::InvalidRecordClass);
        }
        if message_record.participant_id != Some(self.message.participant_id)
            || message_record.activity_id != Some(self.activity.activity_id)
            || message_record.run_id.is_some()
        {
            return Err(AdmissionBatchError::MessageRecordMismatch);
        }
        if let Some(title) = &self.conversation_title {
            let title_record = self.record(title.record_id)?;
            if title_record.participant_id != Some(self.message.participant_id)
                || title_record.activity_id.is_some()
                || title_record.run_id.is_some()
                || title.set_by != self.actor.principal_id
                || title.origin != ConversationTitleOrigin::FirstUserMessage
            {
                return Err(AdmissionBatchError::ConversationTitleRecordMismatch);
            }
        }
        if activity_record.activity_id != Some(self.activity.activity_id)
            || activity_record.run_id.is_some()
        {
            return Err(AdmissionBatchError::ActivityRecordMismatch);
        }
        if run_record.activity_id != Some(self.activity.activity_id)
            || run_record.run_id != Some(self.run.context.run_id)
            || run_record.run_seq.map(RunSeq::get) != Some(1)
            || run_record.participant_id != Some(self.run.context.executor_participant_id)
        {
            return Err(AdmissionBatchError::RunRecordMismatch);
        }

        let context = self.run.context;
        if context.tenant_id != self.append.tenant_id
            || context.conversation_id != self.append.conversation_id
            || context.activity_id != self.activity.activity_id
            || context.activity_kind != self.activity.kind
            || context.requested_by_participant_id != self.message.participant_id
            || context.run_id != self.message.source_run_id
            || context.home_epoch != self.append.expected_home_epoch
            || context.runtime_snapshot_id != self.runtime_snapshot.runtime_snapshot_id
            || !interactive_home_matches_grant(&context, &self.runtime_snapshot.execution_grant)
            || self.runtime_snapshot.execution_grant.tenant_id != context.tenant_id
            || self.runtime_snapshot.execution_grant.run_id != context.run_id
            || self.run.admitted_at < self.runtime_snapshot.execution_grant.issued_at
            || self.run.admitted_at >= self.runtime_snapshot.execution_grant.expires_at
            || (matches!(context.execution_home, RuntimeHome::Device { .. })
                && self.runtime_snapshot.execution_grant_proof.is_none())
        {
            return Err(AdmissionBatchError::IdentityMismatch);
        }
        if self.activity.conversation_id != self.append.conversation_id
            || self.activity.requested_by_participant_id != self.message.participant_id
            || self.activity.root_activity_id != self.activity.activity_id
            || self.activity.parent_activity_id.is_some()
            || self.activity.status != ActivityStatus::Active
            || self.activity.completed_at.is_some()
            || self.run.state.status() != RunStatus::Queued
        {
            return Err(AdmissionBatchError::InvalidInitialState);
        }
        if self.message.role != MessageRole::User {
            return Err(AdmissionBatchError::InvalidMessage);
        }
        let topic = run_dispatch_topic(&self.runtime_snapshot.execution_grant);
        if self.append.outbox[0].topic.as_str() != topic {
            return Err(AdmissionBatchError::InvalidDispatch);
        }
        Ok(())
    }

    fn record(
        &self,
        record_id: LedgerRecordId,
    ) -> Result<&crate::ProposedRecord, AdmissionBatchError> {
        self.append
            .records
            .iter()
            .find(|record| record.record_id == record_id)
            .ok_or(AdmissionBatchError::MissingSemanticRecord)
    }
}

/// One all-or-nothing delegated child Activity/Run admission unit.
///
/// The objective is an Activity-owned context projection rather than a
/// Conversation message. That keeps the visible transcript unchanged and
/// gives context planners an explicit boundary at which parent context stops.
#[derive(Debug, Clone, PartialEq)]
pub struct DelegationAdmissionBatch {
    pub actor: Actor,
    pub trace_context: TraceContext,
    pub append: AppendRequest,
    pub participant_record_id: LedgerRecordId,
    pub objective_record_id: LedgerRecordId,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub delegation_id: DelegationId,
    pub objective_ref: ContentRef,
    pub executor_participant: Participant,
    pub activity: Activity,
    pub run: Run,
    pub runtime_snapshot: RuntimeSnapshotBlob,
}

impl DelegationAdmissionBatch {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.append
            .validate()
            .map_err(AdmissionBatchError::Ledger)?;
        self.runtime_snapshot.validate()?;
        self.run
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidRun)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != PrincipalKind::User
        {
            return Err(AdmissionBatchError::InvalidActor);
        }
        if self.append.records.len() != DELEGATION_ADMISSION_RECORD_COUNT
            || self.append.outbox.len() != 1
        {
            return Err(AdmissionBatchError::InvalidDelegationBatchShape);
        }
        let record_ids = [
            self.participant_record_id,
            self.objective_record_id,
            self.activity_record_id,
            self.run_record_id,
        ];
        if record_ids.iter().copied().collect::<BTreeSet<_>>().len() != record_ids.len() {
            return Err(AdmissionBatchError::DuplicateSemanticRecord);
        }
        if self.append.records[0].record_id != self.participant_record_id
            || self.append.records[1].record_id != self.objective_record_id
            || self.append.records[2].record_id != self.activity_record_id
            || self.append.records[3].record_id != self.run_record_id
        {
            return Err(AdmissionBatchError::InvalidDelegationRecordOrder);
        }
        if self.append.records.iter().any(|record| {
            record.record_kind != LedgerRecordKind::DomainEvent
                || record.producer != Producer::RuntimeApi
        }) {
            return Err(AdmissionBatchError::InvalidRecordClass);
        }
        let participant_record = self.record(self.participant_record_id)?;
        let objective_record = self.record(self.objective_record_id)?;
        let activity_record = self.record(self.activity_record_id)?;
        let run_record = self.record(self.run_record_id)?;
        if participant_record.activity_id.is_some()
            || participant_record.run_id.is_some()
            || participant_record.participant_id != Some(self.executor_participant.participant_id)
        {
            return Err(AdmissionBatchError::ParticipantRecordMismatch);
        }
        if objective_record.activity_id != Some(self.activity.activity_id)
            || objective_record.run_id.is_some()
            || objective_record.participant_id != Some(self.activity.requested_by_participant_id)
        {
            return Err(AdmissionBatchError::DelegationObjectiveRecordMismatch);
        }
        if activity_record.activity_id != Some(self.activity.activity_id)
            || activity_record.run_id.is_some()
        {
            return Err(AdmissionBatchError::ActivityRecordMismatch);
        }
        if run_record.activity_id != Some(self.activity.activity_id)
            || run_record.run_id != Some(self.run.context.run_id)
            || run_record.run_seq.map(RunSeq::get) != Some(1)
        {
            return Err(AdmissionBatchError::RunRecordMismatch);
        }

        let context = self.run.context;
        if self.executor_participant.participant_id != context.executor_participant_id
            || self.executor_participant.conversation_id != self.append.conversation_id
            || self.executor_participant.kind != ParticipantKind::Agent
            || self.executor_participant.principal_ref.is_some()
            || self.executor_participant.agent_ref != Some(self.runtime_snapshot.memory_agent_id)
            || self.executor_participant.service_ref.is_some()
            || self.executor_participant.joined_at != self.run.admitted_at
            || self.executor_participant.left_at.is_some()
            || context.tenant_id != self.append.tenant_id
            || context.conversation_id != self.append.conversation_id
            || context.activity_id != self.activity.activity_id
            || context.activity_kind != ActivityKind::Delegation
            || context.parent_run_id.is_none()
            || context.delegation_id != Some(self.delegation_id)
            || context.home_epoch != self.append.expected_home_epoch
            || context.runtime_snapshot_id != self.runtime_snapshot.runtime_snapshot_id
            || !home_policy_matches_execution_scope(&context)
            || self.runtime_snapshot.execution_grant.tenant_id != context.tenant_id
            || self.runtime_snapshot.execution_grant.run_id != context.run_id
            || self.run.admitted_at < self.runtime_snapshot.execution_grant.issued_at
            || self.run.admitted_at >= self.runtime_snapshot.execution_grant.expires_at
            || (matches!(context.execution_home, RuntimeHome::Device { .. })
                && self.runtime_snapshot.execution_grant_proof.is_none())
        {
            return Err(AdmissionBatchError::IdentityMismatch);
        }
        if self.activity.conversation_id != self.append.conversation_id
            || self.activity.kind != ActivityKind::Delegation
            || self.activity.requested_by_participant_id != context.requested_by_participant_id
            || self.activity.parent_activity_id.is_none()
            || self.activity.root_activity_id == self.activity.activity_id
            || self.activity.status != ActivityStatus::Active
            || self.activity.completed_at.is_some()
            || self.run.state.status() != RunStatus::Queued
        {
            return Err(AdmissionBatchError::InvalidInitialState);
        }
        if self.append.outbox[0].topic.as_str() != "run.dispatch" {
            return Err(AdmissionBatchError::InvalidDispatch);
        }
        Ok(())
    }

    fn record(
        &self,
        record_id: LedgerRecordId,
    ) -> Result<&crate::ProposedRecord, AdmissionBatchError> {
        self.append
            .records
            .iter()
            .find(|record| record.record_id == record_id)
            .ok_or(AdmissionBatchError::MissingSemanticRecord)
    }
}

/// One all-or-nothing cross-Conversation coordination admission unit.
///
/// A durable mailbox payload is projected into the target Activity's private
/// model context. It is deliberately not a Conversation `UserMessage` and
/// must never impersonate the target user. Product Projection may expose the
/// same immutable payload as a typed Coordination ingress.
#[derive(Debug, Clone, PartialEq)]
pub struct CoordinationAdmissionBatch {
    pub actor: Actor,
    pub trace_context: TraceContext,
    pub append: AppendRequest,
    pub context_record_id: LedgerRecordId,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub mailbox_message_id: InterSessionMessageId,
    pub source_conversation_id: musteragent_identity::ConversationId,
    pub payload_ref: ContentRef,
    pub activity: Activity,
    pub run: Run,
    pub runtime_snapshot: RuntimeSnapshotBlob,
}

impl CoordinationAdmissionBatch {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.append
            .validate()
            .map_err(AdmissionBatchError::Ledger)?;
        self.runtime_snapshot.validate()?;
        self.run
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidRun)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != PrincipalKind::User
        {
            return Err(AdmissionBatchError::InvalidActor);
        }
        if self.append.records.len() != COORDINATION_ADMISSION_RECORD_COUNT
            || self.append.outbox.len() != 1
        {
            return Err(AdmissionBatchError::InvalidCoordinationBatchShape);
        }
        let record_ids = [
            self.context_record_id,
            self.activity_record_id,
            self.run_record_id,
        ];
        if record_ids.iter().copied().collect::<BTreeSet<_>>().len() != record_ids.len() {
            return Err(AdmissionBatchError::DuplicateSemanticRecord);
        }
        if self.append.records[0].record_id != self.context_record_id
            || self.append.records[1].record_id != self.activity_record_id
            || self.append.records[2].record_id != self.run_record_id
        {
            return Err(AdmissionBatchError::InvalidCoordinationRecordOrder);
        }
        if self.append.records.iter().any(|record| {
            record.record_kind != LedgerRecordKind::DomainEvent
                || record.producer != Producer::RuntimeApi
        }) {
            return Err(AdmissionBatchError::InvalidRecordClass);
        }
        let context_record = self.record(self.context_record_id)?;
        let activity_record = self.record(self.activity_record_id)?;
        let run_record = self.record(self.run_record_id)?;
        if context_record.activity_id != Some(self.activity.activity_id)
            || context_record.run_id.is_some()
            || context_record.participant_id != Some(self.activity.requested_by_participant_id)
        {
            return Err(AdmissionBatchError::CoordinationContextRecordMismatch);
        }
        if activity_record.activity_id != Some(self.activity.activity_id)
            || activity_record.run_id.is_some()
        {
            return Err(AdmissionBatchError::ActivityRecordMismatch);
        }
        if run_record.activity_id != Some(self.activity.activity_id)
            || run_record.run_id != Some(self.run.context.run_id)
            || run_record.run_seq.map(RunSeq::get) != Some(1)
        {
            return Err(AdmissionBatchError::RunRecordMismatch);
        }

        let context = self.run.context;
        if self.source_conversation_id == self.append.conversation_id
            || context.tenant_id != self.append.tenant_id
            || context.conversation_id != self.append.conversation_id
            || context.activity_id != self.activity.activity_id
            || context.activity_kind != ActivityKind::Coordination
            || context.parent_run_id.is_some()
            || context.delegation_id.is_some()
            || context.home_epoch != self.append.expected_home_epoch
            || context.runtime_snapshot_id != self.runtime_snapshot.runtime_snapshot_id
            || !home_policy_matches_execution_scope(&context)
            || self.runtime_snapshot.execution_grant.tenant_id != context.tenant_id
            || self.runtime_snapshot.execution_grant.run_id != context.run_id
            || self.run.admitted_at < self.runtime_snapshot.execution_grant.issued_at
            || self.run.admitted_at >= self.runtime_snapshot.execution_grant.expires_at
            || (matches!(context.execution_home, RuntimeHome::Device { .. })
                && self.runtime_snapshot.execution_grant_proof.is_none())
        {
            return Err(AdmissionBatchError::IdentityMismatch);
        }
        if self.activity.conversation_id != self.append.conversation_id
            || self.activity.kind != ActivityKind::Coordination
            || self.activity.requested_by_participant_id != context.requested_by_participant_id
            || self.activity.parent_activity_id.is_some()
            || self.activity.root_activity_id != self.activity.activity_id
            || self.activity.status != ActivityStatus::Active
            || self.activity.completed_at.is_some()
            || self.run.state.status() != RunStatus::Queued
        {
            return Err(AdmissionBatchError::InvalidInitialState);
        }
        if self.append.outbox[0].topic.as_str() != "run.dispatch" {
            return Err(AdmissionBatchError::InvalidDispatch);
        }
        Ok(())
    }

    fn record(
        &self,
        record_id: LedgerRecordId,
    ) -> Result<&crate::ProposedRecord, AdmissionBatchError> {
        self.append
            .records
            .iter()
            .find(|record| record.record_id == record_id)
            .ok_or(AdmissionBatchError::MissingSemanticRecord)
    }
}

/// One all-or-nothing user-requested retry admission.
///
/// The source user Message is immutable and remains the only transcript turn.
/// This batch commits only a fresh Chat Activity, a causally linked Run, its
/// sealed Snapshot/Grant and dispatch. Context branches are outside this
/// transaction by design.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryAdmissionBatch {
    pub actor: Actor,
    pub trace_context: TraceContext,
    pub append: AppendRequest,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub source_run_id: musteragent_identity::RunId,
    pub source_message_id: musteragent_identity::MessageId,
    pub source_attempt_number: musteragent_identity::AttemptNumber,
    pub activity: Activity,
    pub run: Run,
    pub runtime_snapshot: RuntimeSnapshotBlob,
    /// Trusted host routing context for the retry Run. `None` inherits the
    /// source Run's context, when it has one, in the same transaction.
    pub sandbox_routing: Option<musteragent_sandbox::SandboxRoutingContext>,
}

/// Shared execution dispatch is consumed by the assigned Executor path.
pub fn run_dispatch_topic(grant: &musteragent_execution_grant::ExecutionGrant) -> &'static str {
    if grant.identity_binding.is_some_and(|binding| {
        matches!(
            binding.authority_scope,
            musteragent_execution_grant::ExecutionGrantAuthorityScope::HostedProjectExecution { .. }
        )
    }) {
        "run.execution.dispatch"
    } else {
        "run.dispatch"
    }
}

impl RetryAdmissionBatch {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.append
            .validate()
            .map_err(AdmissionBatchError::Ledger)?;
        self.runtime_snapshot.validate()?;
        self.run
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidRun)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != PrincipalKind::User
            || self.append.records.len() != RETRY_ADMISSION_RECORD_COUNT
            || self.append.outbox.len() != 1
        {
            return Err(AdmissionBatchError::InvalidBatchShape);
        }
        if self.activity_record_id == self.run_record_id
            || self.append.records[0].record_id != self.activity_record_id
            || self.append.records[1].record_id != self.run_record_id
            || self.append.records.iter().any(|record| {
                record.record_kind != LedgerRecordKind::DomainEvent
                    || record.producer != Producer::RuntimeApi
            })
        {
            return Err(AdmissionBatchError::InvalidRecordOrder);
        }
        let activity_record = self.record(self.activity_record_id)?;
        let run_record = self.record(self.run_record_id)?;
        let context = self.run.context;
        if activity_record.activity_id != Some(self.activity.activity_id)
            || activity_record.run_id.is_some()
            || run_record.activity_id != Some(self.activity.activity_id)
            || run_record.run_id != Some(context.run_id)
            || run_record.run_seq.map(RunSeq::get) != Some(1)
        {
            return Err(AdmissionBatchError::ActivityRecordMismatch);
        }
        if context.tenant_id != self.append.tenant_id
            || context.conversation_id != self.append.conversation_id
            || context.activity_id != self.activity.activity_id
            || context.activity_kind != ActivityKind::Chat
            || context.parent_run_id != Some(self.source_run_id)
            || context.run_id == self.source_run_id
            || context.delegation_id.is_some()
            || context.home_epoch != self.append.expected_home_epoch
            || context.runtime_snapshot_id != self.runtime_snapshot.runtime_snapshot_id
            || !interactive_home_matches_grant(&context, &self.runtime_snapshot.execution_grant)
            || self.runtime_snapshot.execution_grant.tenant_id != context.tenant_id
            || self.runtime_snapshot.execution_grant.run_id != context.run_id
            || self.run.admitted_at < self.runtime_snapshot.execution_grant.issued_at
            || self.run.admitted_at >= self.runtime_snapshot.execution_grant.expires_at
            || (matches!(context.execution_home, RuntimeHome::Device { .. })
                && self.runtime_snapshot.execution_grant_proof.is_none())
            || self.activity.conversation_id != self.append.conversation_id
            || self.activity.kind != ActivityKind::Chat
            || self.activity.requested_by_participant_id != context.requested_by_participant_id
            || self.activity.parent_activity_id.is_some()
            || self.activity.root_activity_id != self.activity.activity_id
            || self.activity.status != ActivityStatus::Active
            || self.activity.completed_at.is_some()
            || self.run.state.status() != RunStatus::Queued
            || self.append.outbox[0].topic.as_str()
                != run_dispatch_topic(&self.runtime_snapshot.execution_grant)
        {
            return Err(AdmissionBatchError::IdentityMismatch);
        }
        Ok(())
    }

    fn record(
        &self,
        record_id: LedgerRecordId,
    ) -> Result<&crate::ProposedRecord, AdmissionBatchError> {
        self.append
            .records
            .iter()
            .find(|record| record.record_id == record_id)
            .ok_or(AdmissionBatchError::MissingSemanticRecord)
    }
}

/// One all-or-nothing message-free continuation admission for a standing Goal.
///
/// The Goal objective and prior Evaluation are immutable Activity-private
/// context. No Conversation Message is created, so an autonomous continuation
/// can never impersonate the user or create a fake transcript turn.
#[derive(Debug, Clone, PartialEq)]
pub struct StandingGoalAdmissionBatch {
    pub actor: Actor,
    pub trace_context: TraceContext,
    pub append: AppendRequest,
    pub context_record_id: LedgerRecordId,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub source: crate::StandingGoalAdmissionSource,
    pub fence: crate::StandingGoalAdmissionFence,
    /// Backend-signed opaque authority is persisted in this same transaction.
    pub continuation_authority: Option<Vec<u8>>,
    pub context: StandingGoalAdmissionContext,
    pub activity: Activity,
    pub run: Run,
    pub runtime_snapshot: RuntimeSnapshotBlob,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StandingGoalAdmissionContext {
    Continuation {
        objective_ref: ContentRef,
        previous_evaluation_summary_ref: Option<ContentRef>,
        input_refs: Vec<ContentRef>,
    },
    Judge {
        judge_input_ref: ContentRef,
    },
}

impl StandingGoalAdmissionBatch {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        if let crate::StandingGoalAdmissionSource::OwnerGoal {
            binding_revision,
            previous_run_id,
            ..
        } = self.source
            && (binding_revision == 0
                || binding_revision > i64::MAX as u64
                || self.continuation_authority.is_none()
                || self.runtime_snapshot.execution_grant_proof.is_none()
                || self.runtime_snapshot.schema_version.as_str()
                    != musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION
                || matches!(&self.context, StandingGoalAdmissionContext::Continuation { input_refs, .. } if input_refs.is_empty())
                || (matches!(&self.context, StandingGoalAdmissionContext::Judge { .. })
                    && previous_run_id.is_none()))
        {
            return Err(AdmissionBatchError::InvalidBatchShape);
        }
        if matches!(&self.context, StandingGoalAdmissionContext::Continuation { input_refs, .. } if input_refs.len() > 100)
            || !self.fence.is_valid()
            || self
                .continuation_authority
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 512 * 1024)
        {
            return Err(AdmissionBatchError::InvalidBatchShape);
        }
        self.append
            .validate()
            .map_err(AdmissionBatchError::Ledger)?;
        self.runtime_snapshot.validate()?;
        self.run
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidRun)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != PrincipalKind::User
            || self.append.records.len() != STANDING_GOAL_ADMISSION_RECORD_COUNT
            || self.append.outbox.len() != 1
        {
            return Err(AdmissionBatchError::InvalidBatchShape);
        }
        let record_ids = [
            self.context_record_id,
            self.activity_record_id,
            self.run_record_id,
        ];
        if record_ids.iter().copied().collect::<BTreeSet<_>>().len() != record_ids.len()
            || self.append.records[0].record_id != self.context_record_id
            || self.append.records[1].record_id != self.activity_record_id
            || self.append.records[2].record_id != self.run_record_id
            || self.append.records.iter().any(|record| {
                record.record_kind != LedgerRecordKind::DomainEvent
                    || record.producer != Producer::RuntimeApi
            })
        {
            return Err(AdmissionBatchError::InvalidRecordOrder);
        }
        let context_record = self.record(self.context_record_id)?;
        let activity_record = self.record(self.activity_record_id)?;
        let run_record = self.record(self.run_record_id)?;
        if context_record.activity_id != Some(self.activity.activity_id)
            || context_record.run_id.is_some()
            || context_record.participant_id != Some(self.activity.requested_by_participant_id)
            || activity_record.activity_id != Some(self.activity.activity_id)
            || activity_record.run_id.is_some()
            || run_record.activity_id != Some(self.activity.activity_id)
            || run_record.run_id != Some(self.run.context.run_id)
            || run_record.run_seq.map(RunSeq::get) != Some(1)
        {
            return Err(AdmissionBatchError::ActivityRecordMismatch);
        }
        let context = self.run.context;
        if context.tenant_id != self.append.tenant_id
            || context.conversation_id != self.append.conversation_id
            || context.activity_id != self.activity.activity_id
            || !matches!(
                (&self.context, context.activity_kind),
                (
                    StandingGoalAdmissionContext::Continuation { .. },
                    ActivityKind::Automation
                ) | (
                    StandingGoalAdmissionContext::Judge { .. },
                    ActivityKind::Evaluation
                )
            )
            || context.parent_run_id != self.source.parent_run_id()
            || context.delegation_id.is_some()
            || context.home_epoch != self.append.expected_home_epoch
            || context.runtime_snapshot_id != self.runtime_snapshot.runtime_snapshot_id
            || !home_policy_matches_execution_scope(&context)
            || self.runtime_snapshot.execution_grant.tenant_id != context.tenant_id
            || self.runtime_snapshot.execution_grant.run_id != context.run_id
            || self.run.admitted_at < self.runtime_snapshot.execution_grant.issued_at
            || self.run.admitted_at >= self.runtime_snapshot.execution_grant.expires_at
            || (matches!(context.execution_home, RuntimeHome::Device { .. })
                && self.runtime_snapshot.execution_grant_proof.is_none())
            || self.activity.conversation_id != self.append.conversation_id
            || self.activity.kind != context.activity_kind
            || self.activity.requested_by_participant_id != context.requested_by_participant_id
            || self.activity.parent_activity_id.is_some()
            || self.activity.root_activity_id != self.activity.activity_id
            || self.activity.status != ActivityStatus::Active
            || self.activity.completed_at.is_some()
            || self.run.state.status() != RunStatus::Queued
            || self.append.outbox[0].topic.as_str() != "run.dispatch"
        {
            return Err(AdmissionBatchError::IdentityMismatch);
        }
        Ok(())
    }

    fn record(
        &self,
        record_id: LedgerRecordId,
    ) -> Result<&crate::ProposedRecord, AdmissionBatchError> {
        self.append
            .records
            .iter()
            .find(|record| record.record_id == record_id)
            .ok_or(AdmissionBatchError::MissingSemanticRecord)
    }
}

/// One all-or-nothing autonomous learning maintenance admission unit.
///
/// The input projection is Activity-private and never creates a visible
/// Conversation Message. Only a System principal may admit it, and the
/// immutable Snapshot must contain the separately authorized Evolution route.
#[derive(Debug, Clone, PartialEq)]
pub struct MaintenanceAdmissionBatch {
    pub actor: Actor,
    pub trace_context: TraceContext,
    pub append: AppendRequest,
    pub context_record_id: LedgerRecordId,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub dream_job_id: DreamJobId,
    pub invocation_id: InvocationId,
    pub input_manifest_ref: ContentRef,
    pub model_input_ref: ContentRef,
    pub model_route_ref: VersionedKey,
    pub model_route_hash: Sha256,
    pub activity: Activity,
    pub run: Run,
    pub runtime_snapshot: MaintenanceRuntimeSnapshotBlob,
}

impl MaintenanceAdmissionBatch {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.append
            .validate()
            .map_err(AdmissionBatchError::Ledger)?;
        self.runtime_snapshot.validate()?;
        self.run
            .validate()
            .map_err(|_| AdmissionBatchError::InvalidRun)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != PrincipalKind::System
            || self
                .runtime_snapshot
                .execution_grant
                .identity_binding
                .is_none_or(|binding| binding.principal_id != self.actor.principal_id)
        {
            return Err(AdmissionBatchError::InvalidMaintenanceActor);
        }
        if self.append.records.len() != MAINTENANCE_ADMISSION_RECORD_COUNT
            || self.append.outbox.len() != 1
        {
            return Err(AdmissionBatchError::InvalidMaintenanceBatchShape);
        }
        let record_ids = [
            self.context_record_id,
            self.activity_record_id,
            self.run_record_id,
        ];
        if record_ids.iter().copied().collect::<BTreeSet<_>>().len() != record_ids.len() {
            return Err(AdmissionBatchError::DuplicateSemanticRecord);
        }
        if self.append.records[0].record_id != self.context_record_id
            || self.append.records[1].record_id != self.activity_record_id
            || self.append.records[2].record_id != self.run_record_id
        {
            return Err(AdmissionBatchError::InvalidMaintenanceRecordOrder);
        }
        if self.append.records.iter().any(|record| {
            record.record_kind != LedgerRecordKind::DomainEvent
                || record.producer != Producer::RuntimeApi
        }) {
            return Err(AdmissionBatchError::InvalidRecordClass);
        }
        let context_record = self.record(self.context_record_id)?;
        let activity_record = self.record(self.activity_record_id)?;
        let run_record = self.record(self.run_record_id)?;
        if context_record.activity_id != Some(self.activity.activity_id)
            || context_record.run_id.is_some()
            || context_record.participant_id != Some(self.activity.requested_by_participant_id)
        {
            return Err(AdmissionBatchError::MaintenanceContextRecordMismatch);
        }
        if activity_record.activity_id != Some(self.activity.activity_id)
            || activity_record.run_id.is_some()
        {
            return Err(AdmissionBatchError::ActivityRecordMismatch);
        }
        if run_record.activity_id != Some(self.activity.activity_id)
            || run_record.run_id != Some(self.run.context.run_id)
            || run_record.run_seq.map(RunSeq::get) != Some(1)
        {
            return Err(AdmissionBatchError::RunRecordMismatch);
        }
        let context = self.run.context;
        let snapshot = self.runtime_snapshot.snapshot.content();
        let grant = &self.runtime_snapshot.execution_grant;
        let evolution_route =
            if grant.schema_version.as_str() == EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1 {
                Some((&grant.model_route, &grant.grant_ref))
            } else {
                grant
                    .auxiliary_model_routes
                    .iter()
                    .find(|route| route.role == AuxiliaryModelRole::Evolution)
                    .map(|route| (&route.route, &route.credential_grant_ref))
            };
        let snapshot_route_hash = SignedModelRouteHash::digest(&snapshot.evolution_route)
            .map_err(|_| AdmissionBatchError::InvalidMaintenanceSnapshot)?;
        if context.tenant_id != self.append.tenant_id
            || context.conversation_id != self.append.conversation_id
            || context.activity_id != self.activity.activity_id
            || context.activity_kind != ActivityKind::Maintenance
            || context.parent_run_id.is_some()
            || context.delegation_id.is_some()
            || context.home_epoch != self.append.expected_home_epoch
            || context.runtime_snapshot_id != self.runtime_snapshot.runtime_snapshot_id
            || !home_policy_matches_execution_scope(&context)
            || self.runtime_snapshot.execution_grant.tenant_id != context.tenant_id
            || self.runtime_snapshot.execution_grant.run_id != context.run_id
            || self
                .runtime_snapshot
                .execution_grant
                .agent_contract
                .as_ref()
                != Some(&snapshot.agent_contract)
            || evolution_route.is_none_or(|(route, credential)| {
                route != &snapshot.evolution_route
                    || credential != &snapshot.credential_grant_ref
                    || route.route_ref != self.model_route_ref
            })
            || snapshot_route_hash.into_bytes() != self.model_route_hash.into_bytes()
            || snapshot.tenant_id != context.tenant_id
            || snapshot.target_agent_id != self.runtime_snapshot.memory_agent_id
            || snapshot.dream_job_id != self.dream_job_id
            || snapshot.invocation_id != self.invocation_id
            || snapshot.input_manifest_ref != self.input_manifest_ref
            || snapshot.model_input_ref != self.model_input_ref
            || snapshot.policy_version_ref != self.activity.policy_snapshot_ref
            || self.runtime_snapshot.snapshot.created_at() != self.run.admitted_at
            || self.run.admitted_at < self.runtime_snapshot.execution_grant.issued_at
            || self.run.admitted_at >= self.runtime_snapshot.execution_grant.expires_at
            || (matches!(context.execution_home, RuntimeHome::Device { .. })
                && self.runtime_snapshot.execution_grant_proof.is_none())
        {
            return Err(AdmissionBatchError::IdentityMismatch);
        }
        if self.activity.conversation_id != self.append.conversation_id
            || self.activity.kind != ActivityKind::Maintenance
            || self.activity.requested_by_participant_id != context.requested_by_participant_id
            || context.requested_by_participant_id == context.executor_participant_id
            || self.activity.parent_activity_id.is_some()
            || self.activity.root_activity_id != self.activity.activity_id
            || self.activity.status != ActivityStatus::Active
            || self.activity.completed_at.is_some()
            || self.run.state.status() != RunStatus::Queued
            || self.input_manifest_ref == self.model_input_ref
            || self.input_manifest_ref.byte_length == 0
            || self.model_input_ref.byte_length == 0
        {
            return Err(AdmissionBatchError::InvalidInitialState);
        }
        if self.append.outbox[0].topic.as_str() != "run.dispatch" {
            return Err(AdmissionBatchError::InvalidDispatch);
        }
        Ok(())
    }

    fn record(
        &self,
        record_id: LedgerRecordId,
    ) -> Result<&crate::ProposedRecord, AdmissionBatchError> {
        self.append
            .records
            .iter()
            .find(|record| record.record_id == record_id)
            .ok_or(AdmissionBatchError::MissingSemanticRecord)
    }
}

// Explicit message and terminal-retry admission share the same typed binding.
// Delegation/maintenance remain on their original Home contract.
fn interactive_home_matches_grant(
    context: &musteragent_run::RunContext,
    grant: &ExecutionGrant,
) -> bool {
    use musteragent_execution_grant::ExecutionGrantAuthorityScope;
    if !grant.identity_binding.is_some_and(|binding| {
        matches!(
            binding.authority_scope,
            ExecutionGrantAuthorityScope::HostedProjectExecution { .. }
        )
    }) {
        return home_policy_matches_execution_scope(context);
    }
    let ExecutionScope::HostedProject { scope } = context.execution_scope else {
        return false;
    };
    let Some(binding) = grant.identity_binding else {
        return false;
    };
    let ExecutionGrantAuthorityScope::HostedProjectExecution {
        scope: granted_scope,
        ..
    } = binding.authority_scope
    else {
        return false;
    };
    granted_scope == scope
        && context.control_home
            == RuntimeHome::Device {
                device_id: scope.home.device_id,
            }
        && Some(context.execution_home)
            == binding
                .device_id
                .map(|device_id| RuntimeHome::Device { device_id })
        && context.home_epoch == scope.home.home_epoch
        && context.tenant_id == scope.resource_tenant_id
}

pub(crate) fn home_policy_matches_execution_scope(context: &musteragent_run::RunContext) -> bool {
    match (
        context.control_home,
        context.execution_home,
        context.execution_scope,
    ) {
        (RuntimeHome::Cloud, RuntimeHome::Cloud, ExecutionScope::Tenant { tenant_id }) => {
            tenant_id == context.tenant_id
        }
        (RuntimeHome::Cloud, RuntimeHome::Cloud, ExecutionScope::Workspace { .. }) => true,
        (
            RuntimeHome::Device {
                device_id: control_device_id,
            },
            RuntimeHome::Device {
                device_id: execution_device_id,
            },
            ExecutionScope::DevicePrivate {
                device_id: scope_device_id,
                ..
            },
        ) => control_device_id == execution_device_id && control_device_id == scope_device_id,
        (
            RuntimeHome::Device {
                device_id: control_device_id,
            },
            RuntimeHome::Device {
                device_id: execution_device_id,
            },
            ExecutionScope::HostedProject { scope },
        ) => {
            control_device_id == scope.home.device_id
                && execution_device_id == scope.home.device_id
                && context.home_epoch == scope.home.home_epoch
                && context.tenant_id == scope.resource_tenant_id
        }
        _ => false,
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AdmissionReceipt {
    pub ledger: AppendReceipt,
    pub message_sequence: ConversationSeq,
}

/// Persistence Port for the single SubmitMessage transaction boundary.
pub trait AdmissionStore: Send + Sync {
    type Error: Send;

    fn admit(
        &self,
        batch: AdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, Self::Error>>;

    fn admit_retry(
        &self,
        batch: RetryAdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>>;
}

/// Persistence port for a first-class autonomous Maintenance Run admission.
///
/// Unlike [`AdmissionStore`], this boundary never accepts or creates visible
/// message content. Implementations must verify that both immutable Dream
/// inputs already exist, create/validate the private System conversation lane,
/// consume the signed grant and append the context/activity/run records in one
/// transaction.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MaintenanceAdmissionFailureClass {
    /// The durable dependency could not complete the transaction. Retrying may
    /// succeed without changing the command.
    Unavailable,
    /// The current durable authority does not permit this transaction.
    Denied,
    /// The transaction or persisted state violates the closed admission
    /// contract. Replaying the same command cannot repair it.
    Invalid,
}

pub trait MaintenanceAdmissionStore: Send + Sync {
    type Error: Send;

    fn admit_maintenance(
        &self,
        batch: MaintenanceAdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>>;

    /// Classifies the concrete adapter error without erasing whether retrying
    /// is safe. Persistence adapters own this mapping because only they know
    /// which failures are transient transport errors versus durable contract
    /// or authority failures.
    fn classify_maintenance_error(error: &Self::Error) -> MaintenanceAdmissionFailureClass;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum AdmissionBatchError {
    #[error(transparent)]
    Ledger(LedgerError),
    #[error("Runtime Snapshot bytes cannot be empty")]
    EmptySnapshot,
    #[error("Runtime Snapshot content hash mismatch")]
    SnapshotHashMismatch,
    #[error("execution grant does not authorize this admission")]
    InvalidExecutionGrant,
    #[error("Device admission requires an exact signed execution grant proof")]
    InvalidExecutionGrantProof,
    #[error("Runtime Snapshot Memory selection is not canonical")]
    InvalidMemorySelection,
    #[error("Runtime Snapshot Instinct selection is not canonical")]
    InvalidInstinctSelection,
    #[error("Maintenance Runtime Snapshot violates the closed autonomous execution contract")]
    InvalidMaintenanceSnapshot,
    #[error("admission Message is invalid")]
    InvalidMessage,
    #[error("admission Run is invalid")]
    InvalidRun,
    #[error("message-backed Run admission requires a tenant-matching user actor")]
    InvalidActor,
    #[error(
        "message-backed Run admission requires three records, an optional first-message title, and one dispatch"
    )]
    InvalidBatchShape,
    #[error(
        "delegated child admission requires participant, objective, activity, run, and one dispatch"
    )]
    InvalidDelegationBatchShape,
    #[error("coordination admission requires context, activity, run, and one dispatch")]
    InvalidCoordinationBatchShape,
    #[error("maintenance admission requires a tenant-matching System actor and signed binding")]
    InvalidMaintenanceActor,
    #[error("maintenance admission requires context, activity, run, and one dispatch")]
    InvalidMaintenanceBatchShape,
    #[error("semantic admission records must be distinct")]
    DuplicateSemanticRecord,
    #[error("admission records must be ordered message, activity, run")]
    InvalidRecordOrder,
    #[error("delegation admission records must be ordered participant, objective, activity, run")]
    InvalidDelegationRecordOrder,
    #[error("coordination admission records must be ordered context, activity, run")]
    InvalidCoordinationRecordOrder,
    #[error("maintenance admission records must be ordered context, activity, run")]
    InvalidMaintenanceRecordOrder,
    #[error("a semantic admission record is missing")]
    MissingSemanticRecord,
    #[error("admission records must be Runtime API domain events")]
    InvalidRecordClass,
    #[error("message.accepted record identity does not match the batch")]
    MessageRecordMismatch,
    #[error("participant.joined record identity does not match the batch")]
    ParticipantRecordMismatch,
    #[error("delegation context projection record identity does not match the batch")]
    DelegationObjectiveRecordMismatch,
    #[error("coordination context projection record identity does not match the batch")]
    CoordinationContextRecordMismatch,
    #[error("maintenance context projection record identity does not match the batch")]
    MaintenanceContextRecordMismatch,
    #[error("conversation.title_changed record identity does not match the admission batch")]
    ConversationTitleRecordMismatch,
    #[error("activity.started record identity does not match the batch")]
    ActivityRecordMismatch,
    #[error("run.admitted record identity does not match the batch")]
    RunRecordMismatch,
    #[error("admission identities or Cloud home do not align")]
    IdentityMismatch,
    #[error("admission aggregates are not in their required initial states")]
    InvalidInitialState,
    #[error("admission Outbox must contain exactly one run.dispatch")]
    InvalidDispatch,
}
