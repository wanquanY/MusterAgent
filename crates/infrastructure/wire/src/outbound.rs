mod context;
mod enum_codes;
mod event;
use crate::wire;
use context::context_event;
use enum_codes::{attempt_trigger, interruption_reason, resume_reason};
pub(crate) use event::domain_event;
use event::{context_snapshot, model_route_ref};
use musteragent_activity::{ActivityEvent, ActivityKind};
use musteragent_approval::{
    ApprovalDecision, InteractionEvent, InteractionEventResponse, InteractionKind,
};
use musteragent_artifact::{ArtifactEvent, ArtifactKind};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_context::ContextSnapshot;
use musteragent_conversation::{
    ContextBranchOrigin, ConversationEvent, ConversationKind, ConversationTitleOrigin, MessageRole,
    PartialMessageReason, ParticipantKind,
};
use musteragent_discovery::{DiscoveryEvent, SkillDisclosed, ToolExposureAdded};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::*;
use musteragent_ledger::DomainEvent;
use musteragent_run::{RunBudgetResource, RunControlKind, RunEvent, RunWaitReason};
use musteragent_runtime_contract::{
    CancelReason, ErrorCategory, ErrorCode, ErrorInfo, FrameContent, OutputChannel, OutputFrame,
    ResourceRef, RetryDisposition,
};
use musteragent_runtime_model::{ModelEvent, ModelInvocationPurpose, ModelRouteRef};
use musteragent_tool::{ToolEvent, ToolKey};
use prost_types::Timestamp;
const MILLIS_PER_SECOND: i64 = 1000;
const NANOS_PER_MILLISECOND: i32 = 1_000_000;
macro_rules! wire_id {
    ($function:ident, $domain:ty, $wire:ty) => {
        fn $function(value: $domain) -> $wire {
            let mut output = <$wire>::default();
            output.value = value.to_string();
            output
        }
    };
}

wire_id!(principal_id, PrincipalId, wire::PrincipalId);
wire_id!(conversation_id, ConversationId, wire::ConversationId);
wire_id!(participant_id, ParticipantId, wire::ParticipantId);
wire_id!(message_id, MessageId, wire::MessageId);
wire_id!(
    inter_session_message_id,
    InterSessionMessageId,
    wire::InterSessionMessageId
);
wire_id!(workspace_id, WorkspaceId, wire::WorkspaceId);
wire_id!(activity_id, ActivityId, wire::ActivityId);
wire_id!(run_id, RunId, wire::RunId);
wire_id!(delegation_id, DelegationId, wire::DelegationId);
wire_id!(dream_job_id, DreamJobId, wire::DreamJobId);
wire_id!(goal_id, GoalId, wire::GoalId);
wire_id!(goal_pursuit_id, GoalPursuitId, wire::GoalPursuitId);
wire_id!(attempt_id, AttemptId, wire::AttemptId);
wire_id!(interaction_id, InteractionId, wire::InteractionId);
wire_id!(invocation_id, InvocationId, wire::InvocationId);
wire_id!(ledger_record_id, LedgerRecordId, wire::LedgerRecordId);
wire_id!(frame_id, FrameId, wire::FrameId);
wire_id!(output_id, OutputId, wire::OutputId);
wire_id!(
    runtime_snapshot_id,
    RuntimeSnapshotId,
    wire::RuntimeSnapshotId
);
wire_id!(
    context_snapshot_id,
    ContextSnapshotId,
    wire::ContextSnapshotId
);
wire_id!(context_branch_id, ContextBranchId, wire::ContextBranchId);
wire_id!(agent_id, AgentId, wire::AgentId);
wire_id!(artifact_id, ArtifactId, wire::ArtifactId);
wire_id!(
    artifact_proposal_id,
    ArtifactProposalId,
    wire::ArtifactProposalId
);
wire_id!(command_id, CommandId, wire::CommandId);
wire_id!(correlation_id, CorrelationId, wire::CorrelationId);
wire_id!(device_id, DeviceId, wire::DeviceId);
wire_id!(lease_id, LeaseId, wire::LeaseId);

pub(crate) fn timestamp(value: UtcTimestamp) -> Timestamp {
    let millis = value.unix_millis();
    Timestamp {
        seconds: millis.div_euclid(MILLIS_PER_SECOND),
        nanos: i32::try_from(millis.rem_euclid(MILLIS_PER_SECOND))
            .expect("millisecond remainder fits i32")
            * NANOS_PER_MILLISECOND,
    }
}

fn sha256(value: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: value.into_bytes().to_vec(),
    }
}

pub(crate) fn content_ref(value: ContentRef) -> wire::ContentRef {
    wire::ContentRef {
        content_id: Some(wire::ContentId {
            value: value.content_id.to_string(),
        }),
        media_type: value.media_type.as_str().to_owned(),
        byte_length: value.byte_length,
        sha256: Some(sha256(value.sha256)),
    }
}

fn schema_ref(value: SchemaRef) -> wire::SchemaRef {
    wire::SchemaRef {
        schema_key: value.schema_key.as_str().to_owned(),
        schema_hash: Some(sha256(value.schema_hash)),
    }
}

fn runtime_home(value: RuntimeHome) -> wire::RuntimeHome {
    let home = match value {
        RuntimeHome::Cloud => wire::runtime_home::Home::Cloud(wire::CloudHome {}),
        RuntimeHome::Device { device_id: value } => {
            wire::runtime_home::Home::Device(wire::DeviceHome {
                device_id: Some(device_id(value)),
            })
        }
    };
    wire::RuntimeHome { home: Some(home) }
}

fn error_category(value: ErrorCategory) -> i32 {
    (match value {
        ErrorCategory::Validation => wire::ErrorCategory::Validation,
        ErrorCategory::Authentication => wire::ErrorCategory::Authentication,
        ErrorCategory::Authorization => wire::ErrorCategory::Authorization,
        ErrorCategory::Conflict => wire::ErrorCategory::Conflict,
        ErrorCategory::NotFound => wire::ErrorCategory::NotFound,
        ErrorCategory::Policy => wire::ErrorCategory::Policy,
        ErrorCategory::Quota => wire::ErrorCategory::Quota,
        ErrorCategory::Dependency => wire::ErrorCategory::Dependency,
        ErrorCategory::Timeout => wire::ErrorCategory::Timeout,
        ErrorCategory::Internal => wire::ErrorCategory::Internal,
    }) as i32
}

fn error_code(value: ErrorCode) -> i32 {
    (match value {
        ErrorCode::InvalidCommand => wire::ErrorCode::InvalidCommand,
        ErrorCode::Unauthenticated => wire::ErrorCode::Unauthenticated,
        ErrorCode::UnsupportedSchemaVersion => wire::ErrorCode::UnsupportedSchemaVersion,
        ErrorCode::IdempotencyConflict => wire::ErrorCode::IdempotencyConflict,
        ErrorCode::ConversationNotFound => wire::ErrorCode::ConversationNotFound,
        ErrorCode::ConversationArchived => wire::ErrorCode::ConversationArchived,
        ErrorCode::ConversationEpochStale => wire::ErrorCode::ConversationEpochStale,
        ErrorCode::ConversationSeqConflict => wire::ErrorCode::ConversationSeqConflict,
        ErrorCode::ParticipantNotAuthorized => wire::ErrorCode::ParticipantNotAuthorized,
        ErrorCode::AgentNotAvailable => wire::ErrorCode::AgentNotAvailable,
        ErrorCode::RuntimeSnapshotInvalid => wire::ErrorCode::RuntimeSnapshotInvalid,
        ErrorCode::ModelRouteUnavailable => wire::ErrorCode::ModelRouteUnavailable,
        ErrorCode::ModelResponseInvalid => wire::ErrorCode::ModelResponseInvalid,
        ErrorCode::PublicDeliveryMissing => wire::ErrorCode::PublicDeliveryMissing,
        ErrorCode::ModelRequestRejected => wire::ErrorCode::ModelRequestRejected,
        ErrorCode::CapabilityUnavailable => wire::ErrorCode::CapabilityUnavailable,
        ErrorCode::RunNotFound => wire::ErrorCode::RunNotFound,
        ErrorCode::RunTerminal => wire::ErrorCode::RunTerminal,
        ErrorCode::RunTransitionInvalid => wire::ErrorCode::RunTransitionInvalid,
        ErrorCode::AttemptLeaseStale => wire::ErrorCode::AttemptLeaseStale,
        ErrorCode::InteractionNotFound => wire::ErrorCode::InteractionNotFound,
        ErrorCode::ArtifactNotFound => wire::ErrorCode::ArtifactNotFound,
        ErrorCode::InteractionNotPending => wire::ErrorCode::InteractionNotPending,
        ErrorCode::InteractionExpired => wire::ErrorCode::InteractionExpired,
        ErrorCode::InteractionResponseInvalid => wire::ErrorCode::InteractionResponseInvalid,
        ErrorCode::ContentIntegrityFailed => wire::ErrorCode::ContentIntegrityFailed,
        ErrorCode::CredentialRefreshRequired => wire::ErrorCode::CredentialRefreshRequired,
        ErrorCode::ContentUnavailable => wire::ErrorCode::ContentUnavailable,
        ErrorCode::CursorEpochStale => wire::ErrorCode::CursorEpochStale,
        ErrorCode::CursorCompacted => wire::ErrorCode::CursorCompacted,
        ErrorCode::PolicyDenied => wire::ErrorCode::PolicyDenied,
        ErrorCode::NativePermissionReduced => wire::ErrorCode::NativePermissionReduced,
        ErrorCode::QuotaExceeded => wire::ErrorCode::QuotaExceeded,
        ErrorCode::CreditsExhausted => wire::ErrorCode::CreditsExhausted,
        ErrorCode::ModelNotEntitled => wire::ErrorCode::ModelNotEntitled,
        ErrorCode::ModelPaymentRequired => wire::ErrorCode::ModelPaymentRequired,
        ErrorCode::MemoryNotFound => wire::ErrorCode::MemoryNotFound,
        ErrorCode::MemoryConflict => wire::ErrorCode::MemoryConflict,
        ErrorCode::MemoryEvaluationPending => wire::ErrorCode::MemoryEvaluationPending,
        ErrorCode::MemoryNotPublishable => wire::ErrorCode::MemoryNotPublishable,
        ErrorCode::ModelContextExceeded => wire::ErrorCode::ModelContextExceeded,
        ErrorCode::ToolNotExposed => wire::ErrorCode::ToolNotExposed,
        ErrorCode::DependencyUnavailable => wire::ErrorCode::DependencyUnavailable,
        ErrorCode::DependencyTimeout => wire::ErrorCode::DependencyTimeout,
        ErrorCode::InternalInvariantViolated => wire::ErrorCode::InternalInvariantViolated,
        ErrorCode::ResourceRevisionConflict => wire::ErrorCode::ResourceRevisionConflict,
        ErrorCode::ResourceNameConflict => wire::ErrorCode::ResourceNameConflict,
        ErrorCode::ResourceMoveCycle => wire::ErrorCode::ResourceMoveCycle,
        ErrorCode::ResourceNodeNotFound => wire::ErrorCode::ResourceNodeNotFound,
        ErrorCode::ResourceParentNotFound => wire::ErrorCode::ResourceParentNotFound,
        ErrorCode::ResourceFileUnavailable => wire::ErrorCode::ResourceFileUnavailable,
        ErrorCode::CatalogCursorInvalidated => wire::ErrorCode::CatalogCursorInvalidated,
    }) as i32
}

fn error_info(value: ErrorInfo) -> wire::ErrorInfo {
    wire::ErrorInfo {
        code: error_code(value.code),
        category: error_category(value.category),
        safe_message: value.safe_message.as_str().to_owned(),
        retryable: value.retryable,
        dependency_ref: value
            .dependency_ref
            .map(|reference| reference.as_str().to_owned()),
        diagnostic_ref: value
            .diagnostic_ref
            .map(|reference| reference.as_str().to_owned()),
    }
}

fn resource_ref(value: ResourceRef) -> wire::ResourceRef {
    let resource = match value {
        ResourceRef::Conversation(value) => {
            wire::resource_ref::Resource::ConversationId(conversation_id(value))
        }
        ResourceRef::Message(value) => wire::resource_ref::Resource::MessageId(message_id(value)),
        ResourceRef::Activity(value) => {
            wire::resource_ref::Resource::ActivityId(activity_id(value))
        }
        ResourceRef::Run(value) => wire::resource_ref::Resource::RunId(run_id(value)),
        ResourceRef::Interaction(value) => {
            wire::resource_ref::Resource::InteractionId(interaction_id(value))
        }
        ResourceRef::Artifact(value) => {
            wire::resource_ref::Resource::ArtifactId(artifact_id(value))
        }
        ResourceRef::Content(value) => wire::resource_ref::Resource::Content(content_ref(value)),
        ResourceRef::ContextSnapshot(value) => {
            wire::resource_ref::Resource::ContextSnapshotId(context_snapshot_id(value))
        }
        ResourceRef::ErasureJob(value) => {
            wire::resource_ref::Resource::ErasureJobId(wire::ErasureJobId {
                value: value.to_string(),
            })
        }
        ResourceRef::ErasureReceipt(value) => {
            wire::resource_ref::Resource::ErasureReceiptId(wire::ErasureReceiptId {
                value: value.to_string(),
            })
        }
    };
    wire::ResourceRef {
        resource: Some(resource),
    }
}

fn conversation_kind(value: ConversationKind) -> i32 {
    (match value {
        ConversationKind::Direct => wire::ConversationKind::Direct,
        ConversationKind::Team => wire::ConversationKind::Team,
        ConversationKind::Automation => wire::ConversationKind::Automation,
        ConversationKind::System => wire::ConversationKind::System,
    }) as i32
}

fn context_branch_origin(value: ContextBranchOrigin) -> i32 {
    (match value {
        ContextBranchOrigin::LegacyUnclassified => wire::ContextBranchOrigin::Unspecified,
        ContextBranchOrigin::Fork => wire::ContextBranchOrigin::Fork,
        ContextBranchOrigin::EditPreserved => wire::ContextBranchOrigin::EditPreserved,
        ContextBranchOrigin::EditReplacement => wire::ContextBranchOrigin::EditReplacement,
    }) as i32
}

fn participant_kind(value: ParticipantKind) -> i32 {
    (match value {
        ParticipantKind::User => wire::ParticipantKind::User,
        ParticipantKind::Agent => wire::ParticipantKind::Agent,
        ParticipantKind::Service => wire::ParticipantKind::Service,
    }) as i32
}

fn activity_kind(value: ActivityKind) -> i32 {
    (match value {
        ActivityKind::Chat => wire::ActivityKind::Chat,
        ActivityKind::Coordination => wire::ActivityKind::Coordination,
        ActivityKind::Automation => wire::ActivityKind::Automation,
        ActivityKind::Delegation => wire::ActivityKind::Delegation,
        ActivityKind::Maintenance => wire::ActivityKind::Maintenance,
        ActivityKind::Evaluation => wire::ActivityKind::Evaluation,
    }) as i32
}

fn interaction_kind(value: InteractionKind) -> i32 {
    (match value {
        InteractionKind::Approval => wire::InteractionKind::Approval,
        InteractionKind::Clarification => wire::InteractionKind::Clarification,
        InteractionKind::Confirmation => wire::InteractionKind::Confirmation,
        InteractionKind::Selection => wire::InteractionKind::Selection,
        InteractionKind::Credential => wire::InteractionKind::Credential,
        InteractionKind::OAuth => wire::InteractionKind::Oauth,
    }) as i32
}

fn output_channel(value: OutputChannel) -> i32 {
    (match value {
        OutputChannel::AssistantText => wire::OutputChannel::AssistantText,
        OutputChannel::ReasoningSummary => wire::OutputChannel::ReasoningSummary,
        OutputChannel::ToolArguments => wire::OutputChannel::ToolArguments,
        OutputChannel::ToolProgress => wire::OutputChannel::ToolProgress,
        OutputChannel::ArtifactContent => wire::OutputChannel::ArtifactContent,
    }) as i32
}

pub(crate) fn output_frame(value: OutputFrame) -> wire::OutputFrame {
    let content = match value.content {
        FrameContent::InlineUtf8(value) => {
            wire::output_frame::Content::InlineUtf8(value.as_str().to_owned())
        }
        FrameContent::ContentRef(value) => {
            wire::output_frame::Content::ContentRef(content_ref(value))
        }
    };
    wire::OutputFrame {
        frame_id: Some(frame_id(value.frame_id)),
        output_id: Some(output_id(value.output_id)),
        message_id: value.message_id.map(message_id),
        channel: output_channel(value.channel),
        frame_index: value.frame_index,
        byte_offset_start: value.byte_offset_start,
        byte_offset_end: value.byte_offset_end,
        content: Some(content),
        content_hash: Some(sha256(value.content_hash)),
        final_for_output: value.final_for_output,
    }
}

wire_id!(
    note_authoring_session_id,
    NoteAuthoringSessionId,
    wire::NoteAuthoringSessionId
);
wire_id!(note_segment_id, NoteSegmentId, wire::NoteSegmentId);
fn recovery_error_code(reason: Option<RunWaitReason>) -> i32 {
    match reason {
        Some(RunWaitReason::RecoveryRequired { code }) => error_code(code),
        _ => wire::ErrorCode::Unspecified as i32,
    }
}

fn run_wait(
    value: Option<RunWaitReason>,
) -> (
    i32,
    Option<wire::InteractionId>,
    Option<wire::InvocationId>,
    Option<wire::ActivityId>,
    Option<wire::DelegationGroupId>,
) {
    match value {
        None => (
            wire::RunWaitReason::Unspecified as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Approval {
            interaction_id: value,
        }) => (
            wire::RunWaitReason::Approval as i32,
            Some(interaction_id(value)),
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Input {
            interaction_id: value,
        }) => (
            wire::RunWaitReason::Input as i32,
            Some(interaction_id(value)),
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Device {
            invocation_id: value,
        }) => (
            wire::RunWaitReason::Device as i32,
            None,
            Some(invocation_id(value)),
            None,
            None,
        ),
        Some(RunWaitReason::RetryBackoff) => (
            wire::RunWaitReason::RetryBackoff as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::RecoveryRequired { .. }) => (
            wire::RunWaitReason::RecoveryRequired as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Budget) => (wire::RunWaitReason::Budget as i32, None, None, None, None),
        Some(RunWaitReason::ExecutionRecovery) => (
            wire::RunWaitReason::ExecutionRecovery as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Delegation { child_activity_id }) => (
            wire::RunWaitReason::Delegation as i32,
            None,
            None,
            Some(activity_id(child_activity_id)),
            None,
        ),
        Some(RunWaitReason::DelegationGroup { group_id: value }) => (
            wire::RunWaitReason::DelegationGroup as i32,
            None,
            None,
            None,
            Some(wire::DelegationGroupId {
                value: value.to_string(),
            }),
        ),
        Some(RunWaitReason::DelegationCompletions) => (
            wire::RunWaitReason::DelegationCompletions as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::ExternalOperation {
            invocation_id: value,
        }) => (
            wire::RunWaitReason::ExternalOperation as i32,
            None,
            Some(invocation_id(value)),
            None,
            None,
        ),
    }
}
