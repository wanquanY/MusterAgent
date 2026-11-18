//! Transport-independent V2 Conversation Catalog query contract.

use musteragent_conversation::{ConversationKind, ConversationLifecycle};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    Actor, ConversationId, HomeEpoch, PrincipalId, TenantId, UtcTimestamp, WorkspaceId,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ApplicationStream, DisplayText, RuntimeError,
};

use crate::{
    AttentionRevision, CatalogActionKind, CatalogActivityPosition, CatalogAttentionKind,
    CatalogExecutionCounts, CatalogRecoverability, CatalogRunWaitReason, CatalogStandingGoalStatus,
    ExecutionOutcomeKind,
};

pub const MAX_CONVERSATION_CATALOG_PAGE_SIZE: u16 = 200;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ConversationCatalogCursorV2 {
    /// Change-feed high-water mark observed when this page was read. This is
    /// reconciliation metadata, not a snapshot lock: later pages may advance
    /// it while the keyset fields below retain pagination continuity.
    pub snapshot_position: CatalogActivityPosition,
    pub after_activity_at: UtcTimestamp,
    pub after_conversation_id: ConversationId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ListConversationCatalogV2 {
    pub actor: Actor,
    pub conversation_id: Option<ConversationId>,
    pub workspace_id: Option<WorkspaceId>,
    pub lifecycle: ConversationLifecycle,
    pub page_size: u16,
    pub after: Option<ConversationCatalogCursorV2>,
}

impl ListConversationCatalogV2 {
    pub fn validate(self) -> Result<Self, ConversationCatalogQueryError> {
        if self.page_size == 0 || self.page_size > MAX_CONVERSATION_CATALOG_PAGE_SIZE {
            return Err(ConversationCatalogQueryError::InvalidPageSize);
        }
        Ok(self)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConversationCatalogQueryError {
    InvalidPageSize,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ConversationExecutionSummaryV2 {
    pub root: CatalogExecutionCounts,
    pub child: CatalogExecutionCounts,
    pub pursuit: CatalogExecutionCounts,
    pub wait_reasons: Vec<CatalogRunWaitReason>,
}

impl ConversationExecutionSummaryV2 {
    #[must_use]
    pub const fn has_active_execution(&self) -> bool {
        self.root.total() > 0 || self.child.total() > 0 || self.pursuit.total() > 0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ConversationActionSummaryV2 {
    pub count: u32,
    pub highest_priority: Option<CatalogActionKind>,
    pub kinds: Vec<CatalogActionKind>,
}

/// Owned wire-safe outcome. `error_class` remains a bounded stable key, never
/// a raw provider message or stack trace.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationOwnedOutcomeSummaryV2 {
    pub kind: ExecutionOutcomeKind,
    pub error_class: Option<String>,
    pub recoverability: Option<CatalogRecoverability>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationUnreadSummaryV2 {
    pub count: u32,
    pub seen_through: AttentionRevision,
    pub latest_visible_revision: AttentionRevision,
    pub latest_unread_kind: Option<CatalogAttentionKind>,
    pub latest_unread_outcome: Option<ConversationOwnedOutcomeSummaryV2>,
}

impl ConversationUnreadSummaryV2 {
    #[must_use]
    pub const fn has_unread(&self) -> bool {
        self.count > 0
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationCatalogSummaryV2 {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub workspace_id: Option<WorkspaceId>,
    pub kind: ConversationKind,
    pub title: Option<DisplayText>,
    pub lifecycle: ConversationLifecycle,
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub catalog_revision: u64,
    pub last_activity_position: CatalogActivityPosition,
    pub last_user_visible_activity_at: UtcTimestamp,
    pub metadata_updated_at: UtcTimestamp,
    pub execution: ConversationExecutionSummaryV2,
    pub action_required: ConversationActionSummaryV2,
    pub standing_goal_status: Option<CatalogStandingGoalStatus>,
    pub pursuit_status: Option<crate::CatalogGoalPursuitStatus>,
    pub latest_outcome: Option<ConversationOwnedOutcomeSummaryV2>,
    pub unread: ConversationUnreadSummaryV2,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationCatalogPageV2 {
    /// Change-feed high-water mark for this internally consistent page.
    pub snapshot_position: CatalogActivityPosition,
    pub conversations: Vec<ConversationCatalogSummaryV2>,
    pub next_cursor: Option<ConversationCatalogCursorV2>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AcknowledgeConversationSeenThrough {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub expected_home_epoch: HomeEpoch,
    pub seen_through: AttentionRevision,
    pub rendered_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ConversationReadReceiptAck {
    pub principal_id: PrincipalId,
    pub conversation_id: ConversationId,
    pub seen_through: AttentionRevision,
    pub changed: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ConversationCatalogInvalidationV2 {
    pub position: CatalogActivityPosition,
    pub conversation_id: ConversationId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationCatalogInvalidationBatchV2 {
    pub invalidations: Vec<ConversationCatalogInvalidationV2>,
    pub latest_position: CatalogActivityPosition,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConversationCatalogSourceError {
    InvalidQuery,
    Unauthorized,
    NotFound,
    Corrupt,
    Unavailable,
}

/// Persistence Port shared by Device SQLite and Cloud PostgreSQL. Implementors
/// own different data; only behavior and DTOs are shared.
pub trait ConversationCatalogV2Source: crate::WorkspaceCatalogActivitySource {
    fn list(
        &self,
        query: ListConversationCatalogV2,
    ) -> ApplicationFuture<'_, Result<ConversationCatalogPageV2, ConversationCatalogSourceError>>;

    fn acknowledge(
        &self,
        command: AcknowledgeConversationSeenThrough,
    ) -> ApplicationFuture<'_, Result<ConversationReadReceiptAck, ConversationCatalogSourceError>>;

    fn read_invalidations(
        &self,
        actor: Actor,
        after: CatalogActivityPosition,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<ConversationCatalogInvalidationBatchV2, ConversationCatalogSourceError>,
    >;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct SubscribeConversationCatalogV2 {
    pub actor: Actor,
    pub lifecycle: ConversationLifecycle,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ConversationCatalogChangeV2 {
    Upsert {
        position: CatalogActivityPosition,
        summary: Box<ConversationCatalogSummaryV2>,
    },
    Remove {
        position: CatalogActivityPosition,
        conversation_id: ConversationId,
    },
    ResyncRequired {
        latest_position: CatalogActivityPosition,
    },
}

pub trait ConversationCatalogV2Application: Send + Sync {
    fn workspace_activity(
        &self,
        query: crate::ListWorkspaceCatalogActivity,
    ) -> ApplicationFuture<'_, Result<crate::WorkspaceCatalogActivityBatch, RuntimeError>>;

    fn list_conversation_catalog_v2(
        &self,
        query: ListConversationCatalogV2,
    ) -> ApplicationFuture<'_, Result<ConversationCatalogPageV2, RuntimeError>>;

    fn acknowledge_conversation_seen_through(
        &self,
        command: AcknowledgeConversationSeenThrough,
    ) -> ApplicationFuture<'_, Result<ConversationReadReceiptAck, RuntimeError>>;

    fn subscribe_conversation_catalog_v2(
        &self,
        query: SubscribeConversationCatalogV2,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<ConversationCatalogChangeV2, RuntimeError>>, RuntimeError>,
    >;
}
