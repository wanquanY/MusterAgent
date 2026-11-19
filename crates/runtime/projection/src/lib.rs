//! Cursor-safe Ledger subscription and idempotent Outbox projection runtime.

#![forbid(unsafe_code)]

mod conversation_catalog;
mod conversation_catalog_api;
mod conversation_catalog_application;
mod conversation_catalog_projector;
mod failure_backfill;
mod outbox;
mod product_reducer;
mod product_snapshot;
mod read_head;
mod retraction;
mod subscription;

pub use conversation_catalog::{
    AttentionRevision, CatalogActionIdentity, CatalogActionKind, CatalogActivityPosition,
    CatalogAdapterError, CatalogAttentionAudience, CatalogAttentionKind, CatalogAttentionRecord,
    CatalogEventContext, CatalogExecutionCounts, CatalogExecutionIdentity, CatalogExecutionStatus,
    CatalogFact, CatalogFactDraft, CatalogFactKind, CatalogGoalPursuitStatus,
    CatalogProjectionApplyDisposition, CatalogProjectionError, CatalogRecoverability,
    CatalogRunProjectionContext, CatalogRunWaitReason, CatalogSourceClaim, CatalogSourceIdentity,
    CatalogSourceKey, CatalogSourceKind, CatalogStandingGoalStatus, ConversationCatalogProjection,
    ConversationReadReceipt, ExecutionOutcome, ExecutionOutcomeKind, adapt_activity_event,
    adapt_artifact_event, adapt_conversation_event, adapt_goal_evaluation, adapt_goal_pursuit,
    adapt_interaction_event, adapt_run_event, adapt_standing_goal,
};
pub use conversation_catalog_api::{
    AcknowledgeConversationSeenThrough, CatalogWorkspaceFilter, ConversationActionSummaryV2,
    ConversationCatalogChangeV2, ConversationCatalogCursorV2,
    ConversationCatalogInvalidationBatchV2, ConversationCatalogInvalidationV2,
    ConversationCatalogPageV2, ConversationCatalogQueryError, ConversationCatalogSourceError,
    ConversationCatalogSummaryV2, ConversationCatalogV2Application, ConversationCatalogV2Source,
    ConversationExecutionSummaryV2, ConversationOwnedOutcomeSummaryV2, ConversationReadReceiptAck,
    ConversationUnreadSummaryV2, ListConversationCatalogV2, MAX_CONVERSATION_CATALOG_PAGE_SIZE,
    MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS, SubscribeConversationCatalogV2,
};
pub use conversation_catalog_application::{
    ConversationCatalogInvalidationFeed, ConversationCatalogInvalidationSubscription,
    ConversationCatalogSourceErrorMapper, ConversationCatalogV2Policy,
    ConversationCatalogV2PolicyError, SharedConversationCatalogV2Application,
};
pub use conversation_catalog_projector::{
    CatalogGoalEvaluationProjectionSnapshot, CatalogGoalProjectionSnapshot,
    CatalogProjectionAuthority, CatalogProjectionDeferral, CatalogProjectionRecord,
    CatalogProjectionRecordPage, CatalogRunAuthority, CatalogRunCancellationAuthority,
    ConversationCatalogLedgerProjector, ConversationCatalogProjectionPass,
    ConversationCatalogProjectionPort, ConversationCatalogProjectorError,
    ConversationCatalogRecordSource,
};
pub use failure_backfill::{
    PROJECTION_FAILURE_BACKFILL_PAGE_SIZE, ProjectionRunFailureQuery, ProjectionRunFailureSource,
    RetainedProjectionRunEvent, backfill_projection_run_failures,
};
pub use outbox::{
    DurableProjector, LocalProjectionSignal, OutboxLease, ProjectionDisposition, ProjectionError,
    ProjectionHandler, ProjectionPolicy, ProjectionPolicyError, ProjectionSignal, ProjectionStore,
    ProjectionSubscription, ProjectionWork,
};
pub use product_reducer::{ProductProjectionReducer, ProjectionContentStore};
pub use product_snapshot::{
    CURRENT_PROJECTION_SNAPSHOT_GENERATION, ConversationProjectionApplication,
    ConversationProjectionSnapshot, ConversationProjectionSnapshotCodec,
    ConversationProjectionWindow, ConversationProjectionWindowCursor,
    CursorRecoveryAuthoritySource, DEFAULT_PROJECTION_WINDOW_MESSAGES, GetConversationProjection,
    GetConversationProjectionWindow, LEGACY_PROJECTION_SNAPSHOT_BRANCH_KIND_PREFIX,
    LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND, MAX_PROJECTION_WINDOW_MESSAGES,
    MAX_PUBLIC_PROJECTION_TAIL_RECORDS, PROJECTION_SNAPSHOT_BRANCH_KIND_PREFIX,
    PROJECTION_SNAPSHOT_ROOT_KIND, PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedActivity,
    ProjectedArtifact, ProjectedContextCompaction, ProjectedContextCompactionStatus,
    ProjectedCoordinationOrigin, ProjectedDelegationOrigin, ProjectedInteraction, ProjectedMessage,
    ProjectedMessageStatus, ProjectedOutputCheckpoint, ProjectedReasoningSummary, ProjectedRun,
    ProjectedRunFailure, ProjectedStreamOutput, ProjectedToolItem, ProjectedToolStatus,
    ProjectionManifestError, ProjectionSnapshotCodecError, ProjectionWindowError,
    SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS, canonical_content_manifest,
    ledger_record_reaches_product_content, projection_snapshot_kind,
    projection_window_from_snapshot, supported_projection_snapshot_kinds,
};
pub use read_head::{ConversationReadHeadError, validate_conversation_read_head};
pub use retraction::ProjectionRetraction;
pub use subscription::{
    CatalogChangeSubscription, ChangeFeed, ChangeSubscription,
    ConversationCatalogSubscriptionPolicy, ConversationCatalogSubscriptionService,
    LedgerPayloadDecoder, LedgerRecordSource, LedgerSubscriptionService, SubscriptionError,
    SubscriptionErrorMapper, SubscriptionPolicy, SubscriptionPolicyError,
};

mod workspace_catalog_activity;
pub use workspace_catalog_activity::{
    ListWorkspaceCatalogActivity, MAX_WORKSPACE_ACTIVITY_BATCH, WorkspaceCatalogActivity,
    WorkspaceCatalogActivityBatch, WorkspaceCatalogActivitySource,
};
