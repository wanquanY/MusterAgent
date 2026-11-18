use std::num::NonZeroU32;
use std::str::FromStr;

use crate::wire;
use musteragent_activity::{ActivityEvent, ActivityKind};
use musteragent_approval::{
    ApprovalDecision, InteractionEvent, InteractionEventResponse, InteractionKind,
};
use musteragent_artifact::{ArtifactEvent, ArtifactKind};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_context::{ContextEvent, ContextSnapshot};
use musteragent_conversation::{
    ContextBranchOrigin, ConversationEvent, ConversationKind, ConversationTitleOrigin, MessageRole,
    PartialMessageReason, ParticipantDisplaySnapshot, ParticipantKind, ServiceRef,
};
use musteragent_discovery::{DiscoveryEvent, SkillDisclosed, ToolExposureAdded, ToolExposureItem};
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{
    AttemptNumber, ConversationSeq, FencingToken, ToolCallId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_ledger::{DomainEvent, LedgerPayload, LedgerPayloadEncoder};
use musteragent_policy::Effect;
use musteragent_projection::{LedgerPayloadDecoder, SubscriptionError};
use musteragent_run::{AttemptTrigger, RunBudgetResource, RunControlKind, RunEvent, RunWaitReason};
use musteragent_runtime_contract::{
    AttachmentRef, CancelReason, DisplayText, ErrorCode, ErrorInfo, FrameContent, InlineFrame,
    InterruptionReason, OpaqueDiagnosticRef, OpaqueExternalRef, OutputChannel, OutputFrame,
    ResourceRef, ResumeReason, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_model::{
    ContinuationRef, LegacyUsageRecordedV1, ModelEvent, ModelInvocationPurpose, ModelRouteRef,
    ModelUsage,
};
use musteragent_tool::{ToolEvent, ToolKey};
use prost::Message;

/// Complete canonical Ledger payload codec used by persistence and gRPC.
#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufLedgerPayloadCodec;

impl ProtobufLedgerPayloadCodec {
    /// Encode an already validated domain fact without changing ledger history.
    pub fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        match payload {
            LedgerPayload::DomainEvent(event) => {
                crate::outbound::domain_event(event).encode_to_vec()
            }
            LedgerPayload::OutputFrame(frame) => {
                crate::outbound::output_frame(frame).encode_to_vec()
            }
        }
    }
}

impl LedgerPayloadEncoder for ProtobufLedgerPayloadCodec {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        Self::encode(self, payload)
    }
}

impl LedgerPayloadDecoder for ProtobufLedgerPayloadCodec {
    fn decode(
        &self,
        record_kind: LedgerRecordKind,
        payload: &[u8],
    ) -> Result<LedgerPayload, SubscriptionError> {
        match record_kind {
            LedgerRecordKind::DomainEvent => {
                let wire = wire::DomainEvent::decode(payload).map_err(|_| corrupt())?;
                let event = decode_domain_event(wire)?;
                Ok(LedgerPayload::DomainEvent(event))
            }
            LedgerRecordKind::OutputFrame => wire::OutputFrame::decode(payload)
                .map_err(|_| corrupt())
                .and_then(decode_output_frame)
                .map(LedgerPayload::OutputFrame),
        }
    }
}

mod activity;
mod artifact;
mod context;
mod conversation;
mod discovery;
mod interaction;
mod model;
mod run;
mod tool;

fn decode_domain_event(value: wire::DomainEvent) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value.event.ok_or_else(corrupt)? {
        event @ (Event::ConversationCreated(_)
        | Event::ConversationWorkspaceAssigned(_)
        | Event::ConversationArchived(_)
        | Event::ConversationTitleChanged(_)
        | Event::ParticipantJoined(_)
        | Event::ParticipantLeft(_)
        | Event::MessageAccepted(_)
        | Event::MessageCommitted(_)
        | Event::MessagePartiallyCommitted(_)
        | Event::MessageImported(_)
        | Event::MessageRetracted(_)) => conversation::decode(event),
        event @ (Event::ActivityStarted(_)
        | Event::DelegationContextProjected(_)
        | Event::CoordinationContextProjected(_)
        | Event::MaintenanceContextProjected(_)
        | Event::StandingGoalContextProjected(_)
        | Event::StandingGoalJudgeContextProjected(_)
        | Event::ActivityCompleted(_)
        | Event::ActivityFailed(_)
        | Event::ActivityCancelled(_)) => activity::decode(event),
        event @ (Event::RunAdmitted(_)
        | Event::RunStarted(_)
        | Event::RunWaiting(_)
        | Event::RunBudgetExceeded(_)
        | Event::RunBudgetAmended(_)
        | Event::RunControlQueued(_)
        | Event::ExecutionLivenessRecorded(_)
        | Event::RunControlConsumed(_)
        | Event::RunRecoveryRequested(_)
        | Event::RunResumed(_)
        | Event::RunCancelRequested(_)
        | Event::RunRetryScheduled(_)
        | Event::RunCompleted(_)
        | Event::RunFailed(_)
        | Event::RunCancelled(_)
        | Event::RunInterrupted(_)
        | Event::RunExecutionPlanUpdated(_)
        | Event::AttemptClaimed(_)
        | Event::AttemptSucceeded(_)
        | Event::AttemptFailed(_)
        | Event::AttemptAbandoned(_)
        | Event::AttemptSuspended(_)) => run::decode(event),
        event @ (Event::ModelInvocationStarted(_)
        | Event::ModelInvocationCompleted(_)
        | Event::ModelInvocationFailed(_)
        | Event::ModelInvocationCancelled(_)
        | Event::UsageRecorded(_)) => model::decode(event),
        event @ (Event::ToolSandboxRouted(_)
        | Event::ToolInvocationRequested(_)
        | Event::ToolInvocationWaitingInteraction(_)
        | Event::ToolInvocationStarted(_)
        | Event::ToolInvocationSucceeded(_)
        | Event::ToolInvocationFailed(_)
        | Event::ToolInvocationIndeterminate(_)
        | Event::ToolInvocationCancelled(_)) => tool::decode(event),
        event @ (Event::ToolExposureAdded(_) | Event::SkillDisclosed(_)) => {
            discovery::decode(event)
        }
        event @ (Event::InteractionRequested(_)
        | Event::InteractionResponded(_)
        | Event::InteractionExpired(_)
        | Event::InteractionCancelled(_)) => interaction::decode(event),
        event @ (Event::ContextCompactionStarted(_)
        | Event::ContextBranchCreated(_)
        | Event::ContextBranchActivated(_)
        | Event::DialogueContextBound(_)
        | Event::ContextDerived(_)
        | Event::ContextSnapshotCreated(_)
        | Event::ContextCompactionFailed(_)
        | Event::ContextCompactionResolved(_)) => context::decode(event),
        event @ (Event::ArtifactCreated(_)
        | Event::ArtifactVersionCommitted(_)
        | Event::NoteAuthoringStarted(_)
        | Event::NoteSegmentCommitted(_)
        | Event::NoteVersionFinalized(_)
        | Event::ArtifactFailed(_)
        | Event::ArtifactEditProposed(_)
        | Event::ArtifactEditApplied(_)
        | Event::ArtifactEditRejected(_)
        | Event::ArtifactEditExpired(_)) => artifact::decode(event),
        _ => Err(corrupt()),
    }
}

fn context_compaction_outcome(
    value: i32,
) -> Result<musteragent_context::ContextCompactionOutcome, SubscriptionError> {
    use musteragent_context::ContextCompactionOutcome as Value;
    match wire::ContextCompactionOutcome::try_from(value) {
        Ok(wire::ContextCompactionOutcome::RetryScheduled) => Ok(Value::RetryScheduled),
        Ok(wire::ContextCompactionOutcome::Succeeded) => Ok(Value::Succeeded),
        Ok(wire::ContextCompactionOutcome::FailedContinuedUncompressed) => {
            Ok(Value::FailedContinuedUncompressed)
        }
        Ok(wire::ContextCompactionOutcome::FailedStopped) => Ok(Value::FailedStopped),
        Ok(wire::ContextCompactionOutcome::IndeterminateStopped) => Ok(Value::IndeterminateStopped),
        _ => Err(corrupt()),
    }
}

fn decode_output_frame(value: wire::OutputFrame) -> Result<OutputFrame, SubscriptionError> {
    let final_for_output = value.final_for_output;
    let content = match value.content.ok_or_else(corrupt)? {
        wire::output_frame::Content::InlineUtf8(value) => {
            FrameContent::InlineUtf8(if value.is_empty() && final_for_output {
                InlineFrame::final_marker()
            } else {
                InlineFrame::new(value).map_err(|_| corrupt())?
            })
        }
        wire::output_frame::Content::ContentRef(value) => {
            FrameContent::ContentRef(content_ref(value)?)
        }
    };
    let frame = OutputFrame {
        frame_id: required_id(value.frame_id.map(|value| value.value))?,
        output_id: required_id(value.output_id.map(|value| value.value))?,
        message_id: optional_id(value.message_id.map(|value| value.value))?,
        channel: output_channel(value.channel)?,
        frame_index: value.frame_index,
        byte_offset_start: value.byte_offset_start,
        byte_offset_end: value.byte_offset_end,
        content,
        content_hash: sha256(value.content_hash)?,
        final_for_output,
    };
    frame.validate().map_err(|_| corrupt())?;
    Ok(frame)
}

fn context_snapshot(
    value: wire::ContextSnapshotCreated,
) -> Result<ContextSnapshot, SubscriptionError> {
    let snapshot = ContextSnapshot {
        plan_lineage: value
            .plan_lineage
            .map(crate::context_plan_lifecycle_codec::context_plan_lineage_from_message)
            .transpose()
            .map_err(|_| corrupt())?,
        delivery_repair: value
            .delivery_repair
            .map(crate::public_delivery_repair_codec::public_delivery_repair_binding_from_message)
            .transpose()
            .map_err(|_| corrupt())?,
        snapshot_id: required_id(value.snapshot_id.map(|value| value.value))?,
        run_id: required_id(value.run_id.map(|value| value.value))?,
        iteration: value.iteration,
        parent_snapshot_id: optional_id(value.parent_snapshot_id.map(|value| value.value))?,
        source_seq_from: ConversationSeq::new(value.source_seq_from).map_err(|_| corrupt())?,
        source_seq_to: ConversationSeq::new(value.source_seq_to).map_err(|_| corrupt())?,
        retained_record_ids: value
            .retained_record_ids
            .into_iter()
            .map(|value| required_id(Some(value.value)))
            .collect::<Result<_, _>>()?,
        structured_summary_ref: content_ref(value.structured_summary_ref.ok_or_else(corrupt)?)?,
        model_request_ref: content_ref(value.model_request_ref.ok_or_else(corrupt)?)?,
        input_tokens_before: value.input_tokens_before,
        input_tokens_after: value.input_tokens_after,
        model_route_ref: model_route_ref(value.model_route_ref)?,
        checksum: sha256(value.checksum)?,
    };
    snapshot.validate().map_err(|_| corrupt())?;
    Ok(snapshot)
}

fn content_ref(value: wire::ContentRef) -> Result<ContentRef, SubscriptionError> {
    Ok(ContentRef {
        content_id: required_id(value.content_id.map(|value| value.value))?,
        media_type: MediaType::new(value.media_type).map_err(|_| corrupt())?,
        byte_length: value.byte_length,
        sha256: sha256(value.sha256)?,
    })
}

fn schema_ref(value: Option<wire::SchemaRef>) -> Result<SchemaRef, SubscriptionError> {
    let value = value.ok_or_else(corrupt)?;
    Ok(SchemaRef {
        schema_key: VersionedKey::new(value.schema_key).map_err(|_| corrupt())?,
        schema_hash: sha256(value.schema_hash)?,
    })
}

fn sha256(value: Option<wire::Sha256>) -> Result<Sha256, SubscriptionError> {
    Sha256::from_bytes(value.ok_or_else(corrupt)?.value).map_err(|_| corrupt())
}

fn timestamp(value: prost_types::Timestamp) -> Result<UtcTimestamp, SubscriptionError> {
    if !(0..1_000_000_000).contains(&value.nanos) {
        return Err(corrupt());
    }
    value
        .seconds
        .checked_mul(1_000)
        .and_then(|seconds| seconds.checked_add(i64::from(value.nanos / 1_000_000)))
        .map(UtcTimestamp::from_unix_millis)
        .ok_or_else(corrupt)
}

fn required_id<T>(value: Option<String>) -> Result<T, SubscriptionError>
where
    T: FromStr<Err = musteragent_identity::IdentityError>,
{
    value.ok_or_else(corrupt)?.parse().map_err(|_| corrupt())
}

fn optional_id<T>(value: Option<String>) -> Result<Option<T>, SubscriptionError>
where
    T: FromStr<Err = musteragent_identity::IdentityError>,
{
    value
        .map(|value| value.parse().map_err(|_| corrupt()))
        .transpose()
}

fn optional_text(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn runtime_home(
    value: Option<wire::RuntimeHome>,
) -> Result<musteragent_event_envelope::RuntimeHome, SubscriptionError> {
    match value.ok_or_else(corrupt)?.home.ok_or_else(corrupt)? {
        wire::runtime_home::Home::Cloud(_) => Ok(musteragent_event_envelope::RuntimeHome::Cloud),
        wire::runtime_home::Home::Device(value) => {
            Ok(musteragent_event_envelope::RuntimeHome::Device {
                device_id: required_id(value.device_id.map(|value| value.value))?,
            })
        }
    }
}

fn resource_ref(value: wire::ResourceRef) -> Result<ResourceRef, SubscriptionError> {
    use wire::resource_ref::Resource;
    match value.resource.ok_or_else(corrupt)? {
        Resource::ConversationId(value) => {
            required_id(Some(value.value)).map(ResourceRef::Conversation)
        }
        Resource::MessageId(value) => required_id(Some(value.value)).map(ResourceRef::Message),
        Resource::ActivityId(value) => required_id(Some(value.value)).map(ResourceRef::Activity),
        Resource::RunId(value) => required_id(Some(value.value)).map(ResourceRef::Run),
        Resource::InteractionId(value) => {
            required_id(Some(value.value)).map(ResourceRef::Interaction)
        }
        Resource::ArtifactId(value) => required_id(Some(value.value)).map(ResourceRef::Artifact),
        Resource::ContextSnapshotId(value) => {
            required_id(Some(value.value)).map(ResourceRef::ContextSnapshot)
        }
        Resource::ErasureJobId(value) => {
            required_id(Some(value.value)).map(ResourceRef::ErasureJob)
        }
        Resource::ErasureReceiptId(value) => {
            required_id(Some(value.value)).map(ResourceRef::ErasureReceipt)
        }
        Resource::Content(value) => content_ref(value).map(ResourceRef::Content),
    }
}

fn error_info(value: Option<wire::ErrorInfo>) -> Result<ErrorInfo, SubscriptionError> {
    let value = value.ok_or_else(corrupt)?;
    let code = error_code(value.code)?;
    let error = ErrorInfo {
        code,
        category: code.category(),
        safe_message: SafeDisplayText::new(value.safe_message).map_err(|_| corrupt())?,
        retryable: value.retryable,
        dependency_ref: value
            .dependency_ref
            .map(OpaqueDiagnosticRef::new)
            .transpose()
            .map_err(|_| corrupt())?,
        diagnostic_ref: value
            .diagnostic_ref
            .map(OpaqueDiagnosticRef::new)
            .transpose()
            .map_err(|_| corrupt())?,
    };
    if error_category(value.category)? != error.category {
        return Err(corrupt());
    }
    error.validate().map_err(|_| corrupt())?;
    Ok(error)
}

macro_rules! closed_enum {
    ($function:ident, $wire:ty, $domain_type:ty, { $($wire_variant:ident => $domain:expr),+ $(,)? }) => {
        fn $function(value: i32) -> Result<$domain_type, SubscriptionError> {
            match <$wire>::try_from(value) {
                $(Ok(<$wire>::$wire_variant) => Ok($domain),)+
                _ => Err(corrupt()),
            }
        }
    };
}

closed_enum!(conversation_kind, wire::ConversationKind, ConversationKind, {
    Direct => ConversationKind::Direct,
    Team => ConversationKind::Team,
    Automation => ConversationKind::Automation,
    System => ConversationKind::System,
});
closed_enum!(participant_kind, wire::ParticipantKind, ParticipantKind, {
    User => ParticipantKind::User,
    Agent => ParticipantKind::Agent,
    Service => ParticipantKind::Service,
});
closed_enum!(message_role, wire::MessageRole, MessageRole, {
    User => MessageRole::User,
    Assistant => MessageRole::Assistant,
    SystemNotice => MessageRole::SystemNotice,
});
closed_enum!(partial_message_reason, wire::PartialMessageReason, PartialMessageReason, {
    Cancelled => PartialMessageReason::Cancelled,
    Interrupted => PartialMessageReason::Interrupted,
    ProviderFailure => PartialMessageReason::ProviderFailure,
    ToolFailure => PartialMessageReason::ToolFailure,
});
fn context_branch_origin(value: i32) -> Result<ContextBranchOrigin, SubscriptionError> {
    match wire::ContextBranchOrigin::try_from(value) {
        // Origin metadata was added after ContextBranchCreated first shipped.
        // Proto3 decodes the absent field as zero, so retaining this explicit
        // replay-only value is required for historical Ledger compatibility.
        Ok(wire::ContextBranchOrigin::Unspecified) => Ok(ContextBranchOrigin::LegacyUnclassified),
        Ok(wire::ContextBranchOrigin::Fork) => Ok(ContextBranchOrigin::Fork),
        Ok(wire::ContextBranchOrigin::EditPreserved) => Ok(ContextBranchOrigin::EditPreserved),
        Ok(wire::ContextBranchOrigin::EditReplacement) => Ok(ContextBranchOrigin::EditReplacement),
        Err(_) => Err(corrupt()),
    }
}
closed_enum!(activity_kind, wire::ActivityKind, ActivityKind, {
    Chat => ActivityKind::Chat,
        Coordination => ActivityKind::Coordination,
    Automation => ActivityKind::Automation,
    Delegation => ActivityKind::Delegation,
    Maintenance => ActivityKind::Maintenance,
    Evaluation => ActivityKind::Evaluation,
});
closed_enum!(cancel_reason, wire::CancelReason, CancelReason, {
    UserRequested => CancelReason::UserRequested,
    ConversationArchived => CancelReason::ConversationArchived,
    ParentCancelled => CancelReason::ParentCancelled,
    PolicyRevoked => CancelReason::PolicyRevoked,
    SystemShutdown => CancelReason::SystemShutdown,
    CoordinationInterrupted => CancelReason::CoordinationInterrupted,
});
closed_enum!(resume_reason, wire::ResumeReason, ResumeReason, {
    InteractionResponded => ResumeReason::InteractionResponded,
    RetryBackoffElapsed => ResumeReason::RetryBackoffElapsed,
    DeviceAvailable => ResumeReason::DeviceAvailable,
    Recovery => ResumeReason::Recovery,
    BudgetAmended => ResumeReason::BudgetAmended,
    DelegationCompleted => ResumeReason::DelegationCompleted,
    ReconciliationDue => ResumeReason::ReconciliationDue,
});
closed_enum!(interruption_reason, wire::InterruptionReason, InterruptionReason, {
    LeaseLost => InterruptionReason::LeaseLost,
    WorkerLost => InterruptionReason::WorkerLost,
    ControlHomeMoved => InterruptionReason::ControlHomeMoved,
    SystemShutdown => InterruptionReason::SystemShutdown,
    ExternalOutcomeUnknown => InterruptionReason::ExternalOutcomeUnknown,
    CoordinationRequested => InterruptionReason::CoordinationRequested,
    CoordinationForcedAbort => InterruptionReason::CoordinationForcedAbort,
});
closed_enum!(attempt_trigger, wire::AttemptTrigger, AttemptTrigger, {
    Initial => AttemptTrigger::Initial,
    Retry => AttemptTrigger::Retry,
    Recovery => AttemptTrigger::Recovery,
    InteractionResumed => AttemptTrigger::InteractionResumed,
    BudgetAmended => AttemptTrigger::BudgetAmended,
    DelegationCompleted => AttemptTrigger::DelegationCompleted,
    ReconciliationDue => AttemptTrigger::ReconciliationDue,
});
closed_enum!(run_control_kind, wire::RunControlKind, RunControlKind, {
    Steering => RunControlKind::Steering,
    FollowUp => RunControlKind::FollowUp,
});
closed_enum!(retry_disposition, wire::RetryDisposition, RetryDisposition, {
    DoNotRetry => RetryDisposition::DoNotRetry,
    RetryNewAttempt => RetryDisposition::RetryNewAttempt,
    ReconcileBeforeRetry => RetryDisposition::ReconcileBeforeRetry,
    RetryFallbackRoute => RetryDisposition::RetryFallbackRoute,
});
closed_enum!(model_purpose, wire::ModelInvocationPurpose, ModelInvocationPurpose, {
    Primary => ModelInvocationPurpose::Primary,
    ContextCompaction => ModelInvocationPurpose::ContextCompaction,
    Finalization => ModelInvocationPurpose::Finalization,
    Verification => ModelInvocationPurpose::Verification,
    EvolutionEvaluation => ModelInvocationPurpose::EvolutionEvaluation,
    EvolutionInduction => ModelInvocationPurpose::EvolutionInduction,
});
closed_enum!(effect, wire::ToolEffect, Effect, {
    Read => Effect::Read,
    WriteReversible => Effect::WriteReversible,
    WriteExternal => Effect::WriteExternal,
    Destructive => Effect::Destructive,
});
closed_enum!(interaction_kind, wire::InteractionKind, InteractionKind, {
    Approval => InteractionKind::Approval,
    Clarification => InteractionKind::Clarification,
    Confirmation => InteractionKind::Confirmation,
    Selection => InteractionKind::Selection,
    Credential => InteractionKind::Credential,
    Oauth => InteractionKind::OAuth,
});
closed_enum!(artifact_kind, wire::ArtifactKind, ArtifactKind, {
    Presentation => ArtifactKind::Presentation,
    Note => ArtifactKind::Note,
    LegacyDiagram => ArtifactKind::LegacyDiagram,
    Image => ArtifactKind::Image,
    Video => ArtifactKind::Video,
    File => ArtifactKind::File,
    Application => ArtifactKind::Application,
});
closed_enum!(output_channel, wire::OutputChannel, OutputChannel, {
    AssistantText => OutputChannel::AssistantText,
    ReasoningSummary => OutputChannel::ReasoningSummary,
    ToolArguments => OutputChannel::ToolArguments,
    ToolProgress => OutputChannel::ToolProgress,
    ArtifactContent => OutputChannel::ArtifactContent,
});
closed_enum!(error_category, wire::ErrorCategory, musteragent_runtime_contract::ErrorCategory, {
    Validation => musteragent_runtime_contract::ErrorCategory::Validation,
    Authentication => musteragent_runtime_contract::ErrorCategory::Authentication,
    Authorization => musteragent_runtime_contract::ErrorCategory::Authorization,
    Conflict => musteragent_runtime_contract::ErrorCategory::Conflict,
    NotFound => musteragent_runtime_contract::ErrorCategory::NotFound,
    Policy => musteragent_runtime_contract::ErrorCategory::Policy,
    Quota => musteragent_runtime_contract::ErrorCategory::Quota,
    Dependency => musteragent_runtime_contract::ErrorCategory::Dependency,
    Timeout => musteragent_runtime_contract::ErrorCategory::Timeout,
    Internal => musteragent_runtime_contract::ErrorCategory::Internal,
});
closed_enum!(error_code, wire::ErrorCode, ErrorCode, {
    InvalidCommand => ErrorCode::InvalidCommand,
    Unauthenticated => ErrorCode::Unauthenticated,
    UnsupportedSchemaVersion => ErrorCode::UnsupportedSchemaVersion,
    IdempotencyConflict => ErrorCode::IdempotencyConflict,
    ConversationNotFound => ErrorCode::ConversationNotFound,
    ConversationArchived => ErrorCode::ConversationArchived,
    ConversationEpochStale => ErrorCode::ConversationEpochStale,
    ConversationSeqConflict => ErrorCode::ConversationSeqConflict,
    ParticipantNotAuthorized => ErrorCode::ParticipantNotAuthorized,
    AgentNotAvailable => ErrorCode::AgentNotAvailable,
    RuntimeSnapshotInvalid => ErrorCode::RuntimeSnapshotInvalid,
    ModelRouteUnavailable => ErrorCode::ModelRouteUnavailable,
    ModelResponseInvalid => ErrorCode::ModelResponseInvalid,
    PublicDeliveryMissing => ErrorCode::PublicDeliveryMissing,
    ModelRequestRejected => ErrorCode::ModelRequestRejected,
    CapabilityUnavailable => ErrorCode::CapabilityUnavailable,
    RunNotFound => ErrorCode::RunNotFound,
    RunTerminal => ErrorCode::RunTerminal,
    RunTransitionInvalid => ErrorCode::RunTransitionInvalid,
    AttemptLeaseStale => ErrorCode::AttemptLeaseStale,
    InteractionNotFound => ErrorCode::InteractionNotFound,
    ArtifactNotFound => ErrorCode::ArtifactNotFound,
    InteractionNotPending => ErrorCode::InteractionNotPending,
    InteractionExpired => ErrorCode::InteractionExpired,
    InteractionResponseInvalid => ErrorCode::InteractionResponseInvalid,
    ContentIntegrityFailed => ErrorCode::ContentIntegrityFailed,
    CredentialRefreshRequired => ErrorCode::CredentialRefreshRequired,
    ContentUnavailable => ErrorCode::ContentUnavailable,
    CursorEpochStale => ErrorCode::CursorEpochStale,
    CursorCompacted => ErrorCode::CursorCompacted,
    PolicyDenied => ErrorCode::PolicyDenied,
    NativePermissionReduced => ErrorCode::NativePermissionReduced,
    QuotaExceeded => ErrorCode::QuotaExceeded,
    CreditsExhausted => ErrorCode::CreditsExhausted,
    ModelNotEntitled => ErrorCode::ModelNotEntitled,
    ModelPaymentRequired => ErrorCode::ModelPaymentRequired,
    MemoryNotFound => ErrorCode::MemoryNotFound,
    MemoryConflict => ErrorCode::MemoryConflict,
    MemoryEvaluationPending => ErrorCode::MemoryEvaluationPending,
    MemoryNotPublishable => ErrorCode::MemoryNotPublishable,

    ModelContextExceeded => ErrorCode::ModelContextExceeded,
    ToolNotExposed => ErrorCode::ToolNotExposed,
    DependencyUnavailable => ErrorCode::DependencyUnavailable,
    DependencyTimeout => ErrorCode::DependencyTimeout,
    InternalInvariantViolated => ErrorCode::InternalInvariantViolated,
    ResourceRevisionConflict => ErrorCode::ResourceRevisionConflict,
    ResourceNameConflict => ErrorCode::ResourceNameConflict,
    ResourceMoveCycle => ErrorCode::ResourceMoveCycle,
    ResourceNodeNotFound => ErrorCode::ResourceNodeNotFound,
    ResourceParentNotFound => ErrorCode::ResourceParentNotFound,
    ResourceFileUnavailable => ErrorCode::ResourceFileUnavailable,
    CatalogCursorInvalidated => ErrorCode::CatalogCursorInvalidated,
});

fn model_route_ref(value: Option<wire::ModelRouteRef>) -> Result<ModelRouteRef, SubscriptionError> {
    Ok(ModelRouteRef::from_key(
        VersionedKey::new(value.ok_or_else(corrupt)?.route_key).map_err(|_| corrupt())?,
    ))
}

fn attempt_number(value: u32) -> Result<AttemptNumber, SubscriptionError> {
    AttemptNumber::new(value).map_err(|_| corrupt())
}

fn run_wait_reason(
    reason: i32,
    interaction_id: Option<String>,
    invocation_id: Option<String>,
    child_activity_id: Option<String>,
    delegation_group_id: Option<String>,
    recovery_error_code: i32,
) -> Result<RunWaitReason, SubscriptionError> {
    if reason != wire::RunWaitReason::RecoveryRequired as i32 && recovery_error_code != 0 {
        return Err(corrupt());
    }
    match wire::RunWaitReason::try_from(reason) {
        Ok(wire::RunWaitReason::Approval) => Ok(RunWaitReason::Approval {
            interaction_id: required_id(interaction_id)?,
        }),
        Ok(wire::RunWaitReason::Input) => Ok(RunWaitReason::Input {
            interaction_id: required_id(interaction_id)?,
        }),
        Ok(wire::RunWaitReason::Device) => Ok(RunWaitReason::Device {
            invocation_id: required_id(invocation_id)?,
        }),
        Ok(wire::RunWaitReason::RetryBackoff)
            if interaction_id.is_none() && invocation_id.is_none() =>
        {
            Ok(RunWaitReason::RetryBackoff)
        }
        Ok(wire::RunWaitReason::RecoveryRequired)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && child_activity_id.is_none()
                && delegation_group_id.is_none() =>
        {
            Ok(RunWaitReason::RecoveryRequired {
                code: error_code(recovery_error_code)?,
            })
        }
        Ok(wire::RunWaitReason::Budget) if interaction_id.is_none() && invocation_id.is_none() => {
            Ok(RunWaitReason::Budget)
        }
        Ok(wire::RunWaitReason::Delegation)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && delegation_group_id.is_none() =>
        {
            Ok(RunWaitReason::Delegation {
                child_activity_id: required_id(child_activity_id)?,
            })
        }
        Ok(wire::RunWaitReason::DelegationGroup)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && child_activity_id.is_none() =>
        {
            Ok(RunWaitReason::DelegationGroup {
                group_id: required_id(delegation_group_id)?,
            })
        }
        Ok(wire::RunWaitReason::DelegationCompletions)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && child_activity_id.is_none()
                && delegation_group_id.is_none() =>
        {
            Ok(RunWaitReason::DelegationCompletions)
        }
        Ok(wire::RunWaitReason::ExecutionRecovery)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && child_activity_id.is_none()
                && delegation_group_id.is_none() =>
        {
            Ok(RunWaitReason::ExecutionRecovery)
        }
        Ok(wire::RunWaitReason::ExternalOperation)
            if interaction_id.is_none()
                && invocation_id.is_some()
                && child_activity_id.is_none() =>
        {
            Ok(RunWaitReason::ExternalOperation {
                invocation_id: required_id(invocation_id)?,
            })
        }
        _ => Err(corrupt()),
    }
}

fn run_budget_resource(value: i32) -> Result<RunBudgetResource, SubscriptionError> {
    match wire::RunBudgetResource::try_from(value) {
        Ok(wire::RunBudgetResource::Steps) => Ok(RunBudgetResource::Steps),
        Ok(wire::RunBudgetResource::ToolCalls) => Ok(RunBudgetResource::ToolCalls),
        Ok(wire::RunBudgetResource::Tokens) => Ok(RunBudgetResource::Tokens),
        Ok(wire::RunBudgetResource::Cost) => Ok(RunBudgetResource::Cost),
        _ => Err(corrupt()),
    }
}

fn interaction_response(
    value: Option<wire::InteractionEventResponse>,
) -> Result<InteractionEventResponse, SubscriptionError> {
    match value.ok_or_else(corrupt)?.response.ok_or_else(corrupt)? {
        wire::interaction_event_response::Response::Approval(value) => {
            match wire::ApprovalDecision::try_from(value) {
                Ok(wire::ApprovalDecision::Approve) => Ok(InteractionEventResponse::Approval(
                    ApprovalDecision::Approve,
                )),
                Ok(wire::ApprovalDecision::ApproveSession) => Ok(
                    InteractionEventResponse::Approval(ApprovalDecision::ApproveSession),
                ),
                Ok(wire::ApprovalDecision::Deny) => {
                    Ok(InteractionEventResponse::Approval(ApprovalDecision::Deny))
                }
                _ => Err(corrupt()),
            }
        }
        wire::interaction_event_response::Response::Content(value) => {
            content_ref(value).map(InteractionEventResponse::Content)
        }
        wire::interaction_event_response::Response::SecureAccepted(_) => {
            Ok(InteractionEventResponse::SecureAccepted)
        }
    }
}

const fn corrupt() -> SubscriptionError {
    SubscriptionError::CorruptRecord
}

#[cfg(test)]
mod tests;
