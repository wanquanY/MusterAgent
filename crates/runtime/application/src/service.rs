mod adapters;

use std::sync::Arc;

use musteragent_activity::{Activity, ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_approval::{
    GetInteraction, InteractionApplication, InteractionEvent, InteractionRead, RespondInteraction,
};
use musteragent_content_ref::Sha256;
use musteragent_conversation::{
    ActivateConversationContextBranch, AdmissionMessage, ArchiveConversation, Conversation,
    ConversationApplication, ConversationEvent, ConversationLifecycle, ConversationTitleOrigin,
    EditAndResubmitMessage, ForkConversationContext, GetConversation, MessageRole, Participant,
    ParticipantDisplaySnapshot, ParticipantIdentity, ParticipantKind, RetractMessage,
    SetConversationTitle, SubmitMessage, UnarchiveConversation, first_user_message_title,
};
use musteragent_execution_grant::{
    EncodedExecutionGrantProof, ExecutionGrant, ExecutionGrantProofEncoder,
    GrantAuthorizationRequest, SignedExecutionGrant,
};
use musteragent_identity::{
    AttemptNumber, ConversationSeq, DottedKey, HomeEpoch, LedgerOffset, RunSeq, UtcTimestamp,
};
use musteragent_ledger::{
    AdmissionBatch, AdmissionConversationTitle, AppendDisposition, AppendRequest, CancelMode,
    DomainEvent, LedgerPayloadEncoder, ProposedOutbox, RetryAdmissionBatch, RunControlBatch,
    RuntimeSnapshotBlob,
};
use musteragent_run::{
    EnqueueRunControl, RetryRun, Run, RunApplication, RunContext, RunEvent, RunState, RunStatus,
};
use musteragent_runtime_content::AdmissionContentSlot;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::{
    ActiveRunArchivePolicy, ApplicationFuture, CancelReason, CommandAck, CommandDisposition,
    CommandEnvelope, ErrorCode, ErrorDetails, ResourceRef, RuntimeError,
};
use musteragent_scheduler::{
    AdmissionApplication, AdmissionSelection, ExecutionGrantAuthenticator,
};
use musteragent_tool::ToolEvent;

use crate::grant_binding::execution_grant_identity_binding;
use crate::identity::{
    AdmissionIdentities, admission_identities_for_command, conversation_participant_id,
    create_command_outbox_id, create_command_record_id,
};
use crate::record::{RecordIdentity, domain_record};
use crate::runtime_error;
use crate::{
    ApplicationClock, RuntimeHomePolicy, RuntimeSnapshotResolver, SnapshotResolutionRequest,
};
use crate::{CommandReplayQuery, ConversationCreationRecoveryQuery};
use musteragent_runtime_session::{
    ActivateContextBranchCommit, AdmissionAuditContext, AdmissionAuthorizationRequest,
    AdmissionCommit, AdmissionDenialCommit, AdmissionDenialQuery, ArchiveConversationCommit,
    CreateConversationCommit, EditAndResubmitCommit, ForkContextCommit, RetractMessageCommit,
    RunControlCommit, RuntimeApplicationStore, RuntimeInteractionApplicationStore,
    SetConversationTitleCommit, UnarchiveConversationCommit,
};

mod admission;

mod context_branch;

mod conversation_control;
mod conversation_creation;

mod error_mapping;
mod execution_grant_proof;
mod fingerprint;

mod recovery;
mod retraction;
mod retry_run;
mod run_context;
mod run_control;

mod submission;
mod submission_lookup;

use error_mapping::*;
use fingerprint::*;
use run_context::resolved_request_run_context;

struct AdmissionPreparation {
    ids: AdmissionIdentities,
    now: UtcTimestamp,
    resolved: crate::RuntimeSnapshotResolution,
    execution_grant: ExecutionGrant,
    execution_grant_proof: Option<EncodedExecutionGrantProof>,
}

/// Infrastructure and host policy required to compose the application service.
/// The store implements transactional commits; no transport performs them itself.
pub struct RuntimeApplicationPorts {
    /// Durable authorization, replay and atomic ledger commits.
    pub store: Arc<dyn RuntimeApplicationStore>,
    /// Resolves and seals host-authorized immutable execution material.
    pub snapshots: Arc<dyn RuntimeSnapshotResolver>,
    /// Tenant-scoped immutable content and admission staging.
    pub content: Arc<dyn ContentStore>,
    /// Canonical domain-event encoding.
    pub encoder: Arc<dyn LedgerPayloadEncoder>,
    /// Application wall clock used for deadlines and durable timestamps.
    pub clock: Arc<dyn ApplicationClock>,
    /// Verifies the authority and integrity of submitted grants.
    pub grant_authenticator: Arc<dyn ExecutionGrantAuthenticator>,
    /// Transactional interaction response and cancellation owner.
    pub interactions: Arc<dyn RuntimeInteractionApplicationStore>,
}

/// Host-facing durable application service. Stores own atomic commits; the host
/// supplies an explicit Agent key, authenticated grants and snapshot material.
pub struct RuntimeApplication {
    store: Arc<dyn RuntimeApplicationStore>,
    snapshots: Arc<dyn RuntimeSnapshotResolver>,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    clock: Arc<dyn ApplicationClock>,
    grant_authenticator: Arc<dyn ExecutionGrantAuthenticator>,
    execution_grant_proof_encoder: Option<Arc<dyn ExecutionGrantProofEncoder>>,
    interactions: Arc<dyn RuntimeInteractionApplicationStore>,
    controls: musteragent_runtime_session::InteractionResponseApplication,
    agent_key: musteragent_identity::AgentKey,
    home_policy: RuntimeHomePolicy,
    file_retractions: Option<Arc<dyn crate::file_retraction::RetractionFileEffectsPlanner>>,
}

impl RuntimeApplication {
    /// Attach the file-effect owner required for retracting file-producing messages.
    #[must_use]
    pub fn with_file_retractions(
        mut self,
        planner: Arc<dyn crate::file_retraction::RetractionFileEffectsPlanner>,
    ) -> Self {
        self.file_retractions = Some(planner);
        self
    }

    async fn load_admission_denial(
        &self,
        actor: musteragent_identity::Actor,
        command_id: musteragent_identity::CommandId,
        conversation_id: musteragent_identity::ConversationId,
        command_fingerprint: Sha256,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> Result<Option<RuntimeError>, RuntimeError> {
        self.store
            .load_admission_denial(
                AdmissionDenialQuery {
                    tenant_id: actor.tenant_id,
                    command_id,
                    conversation_id,
                    command_fingerprint,
                },
                correlation_id,
            )
            .await
            .map(|receipt| receipt.map(|value| canonical_denial_error(value, correlation_id)))
    }

    async fn record_admission_denial(
        &self,
        metadata: &musteragent_runtime_contract::CommandMetadata,
        conversation_id: musteragent_identity::ConversationId,
        command_fingerprint: Sha256,
        occurred_at: UtcTimestamp,
        error: RuntimeError,
    ) -> RuntimeError {
        if !is_audited_admission_denial(error.code) {
            return error;
        }
        let diagnostic_ref = match error.details.as_ref() {
            ErrorDetails::Diagnostic { reference } => Some(reference.clone()),
            _ => None,
        };
        let audit_result = self
            .store
            .record_admission_denial(AdmissionDenialCommit {
                actor: metadata.actor,
                command_id: metadata.command_id,
                conversation_id,
                correlation_id: metadata.correlation_id,
                trace_context: metadata.trace_context.clone(),
                command_fingerprint,
                error_code: error.code,
                retryable: error.retryable,
                diagnostic_ref: diagnostic_ref.clone(),
                occurred_at,
            })
            .await;
        finalize_admission_denial(
            error,
            diagnostic_ref.as_ref(),
            audit_result,
            metadata.correlation_id,
        )
    }

    /// Compose the durable service with host-owned ports and an explicit Agent.
    #[must_use]
    pub fn new(agent_key: musteragent_identity::AgentKey, ports: RuntimeApplicationPorts) -> Self {
        let RuntimeApplicationPorts {
            store,
            snapshots,
            content,
            encoder,
            clock,
            grant_authenticator,
            interactions,
        } = ports;
        let controls = musteragent_runtime_session::InteractionResponseApplication::new(
            Arc::clone(&store),
            Arc::clone(&content),
            Arc::clone(&encoder),
            Arc::clone(&clock),
            Arc::clone(&interactions),
        );
        Self {
            store,
            snapshots,
            content,
            encoder,
            clock,
            grant_authenticator,
            execution_grant_proof_encoder: None,
            interactions,
            controls,
            agent_key,
            home_policy: RuntimeHomePolicy::cloud(),
            file_retractions: None,
        }
    }

    /// Pins this Application instance to one closed Runtime home before it is
    /// shared with any transport or worker composition.
    #[must_use]
    pub fn with_home_policy(mut self, home_policy: RuntimeHomePolicy) -> Self {
        self.home_policy = home_policy;
        self
    }

    async fn load_guarded_replay<T>(
        &self,
        command: &CommandEnvelope<T>,
        command_fingerprint: Sha256,
    ) -> Result<Option<musteragent_ledger::AppendReceipt>, RuntimeError> {
        load_guarded_replay(self.store.as_ref(), command, command_fingerprint).await
    }
}
async fn load_guarded_replay<T>(
    store: &dyn RuntimeApplicationStore,
    command: &CommandEnvelope<T>,
    command_fingerprint: Sha256,
) -> Result<Option<musteragent_ledger::AppendReceipt>, RuntimeError> {
    let guard = command
        .metadata
        .conversation_guard
        .ok_or_else(|| invariant(command))?;
    store
        .load_command_replay(
            CommandReplayQuery {
                tenant_id: command.metadata.actor.tenant_id,
                conversation_id: guard.conversation_id,
                home_epoch: guard.expected_home_epoch,
                idempotency_key: command.metadata.idempotency_key.clone(),
                command_fingerprint,
            },
            command.metadata.correlation_id,
        )
        .await
}

mod coordination;
pub use coordination::{
    CoordinationAdmissionReceipt, CoordinationPreparation, coordination_target_grant_nonce,
};

mod dialogue_execution;

impl RuntimeApplication {
    async fn load_bound_assistant(
        &self,
        actor: musteragent_identity::Actor,
        conversation: musteragent_identity::ConversationId,
        correlation: musteragent_identity::CorrelationId,
    ) -> Result<crate::AgentAdmissionIdentity, RuntimeError> {
        let identity = self
            .store
            .load_assistant_identity(self.agent_key.clone(), correlation)
            .await?;
        let view = self.store.get_conversation(actor, conversation).await?;
        if !view.participants.iter().any(|participant| {
            participant.participant_id == view.conversation.execution_owner_participant_id
                && participant.agent_ref == Some(identity.definition.agent_id)
                && participant.left_at.is_none()
        }) {
            return Err(runtime_error(
                ErrorCode::PolicyDenied,
                correlation,
                false,
                "Configured Agent does not own this conversation",
            ));
        }
        Ok(identity)
    }
}
