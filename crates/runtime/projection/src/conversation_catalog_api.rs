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
pub const MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS: usize = 100;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Default)]
pub enum CatalogWorkspaceFilter {
    #[default]
    All,
    Unassigned,
    Assigned(WorkspaceId),
}

impl CatalogWorkspaceFilter {
    #[must_use]
    pub const fn from_workspace_id(workspace_id: Option<WorkspaceId>) -> Self {
        match workspace_id {
            None => Self::All,
            Some(workspace_id) => Self::Assigned(workspace_id),
        }
    }
}

impl From<Option<WorkspaceId>> for CatalogWorkspaceFilter {
    fn from(value: Option<WorkspaceId>) -> Self {
        Self::from_workspace_id(value)
    }
}

impl From<WorkspaceId> for CatalogWorkspaceFilter {
    fn from(value: WorkspaceId) -> Self {
        Self::Assigned(value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ConversationCatalogCursorV2 {
    /// Change-feed high-water mark observed when this page was read. This is
    /// reconciliation metadata, not a snapshot lock: later pages may advance
    /// it while the keyset fields below retain pagination continuity.
    pub snapshot_position: CatalogActivityPosition,
    pub after_activity_at: UtcTimestamp,
    pub after_conversation_id: ConversationId,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListConversationCatalogV2 {
    pub actor: Actor,
    pub conversation_id: Option<ConversationId>,
    pub workspace_filter: CatalogWorkspaceFilter,
    pub title_query: Option<String>,
    pub lifecycle: ConversationLifecycle,
    pub page_size: u16,
    pub after: Option<ConversationCatalogCursorV2>,
}

impl ListConversationCatalogV2 {
    #[must_use]
    pub const fn new(actor: Actor, lifecycle: ConversationLifecycle, page_size: u16) -> Self {
        Self {
            actor,
            conversation_id: None,
            workspace_filter: CatalogWorkspaceFilter::All,
            title_query: None,
            lifecycle,
            page_size,
            after: None,
        }
    }

    #[must_use]
    pub const fn from_workspace_id(
        actor: Actor,
        conversation_id: Option<ConversationId>,
        workspace_id: Option<WorkspaceId>,
        lifecycle: ConversationLifecycle,
        page_size: u16,
        after: Option<ConversationCatalogCursorV2>,
    ) -> Self {
        Self {
            actor,
            conversation_id,
            workspace_filter: CatalogWorkspaceFilter::from_workspace_id(workspace_id),
            title_query: None,
            lifecycle,
            page_size,
            after,
        }
    }

    #[must_use]
    pub const fn with_conversation_id(mut self, conversation_id: Option<ConversationId>) -> Self {
        self.conversation_id = conversation_id;
        self
    }

    #[must_use]
    pub const fn with_workspace_filter(mut self, workspace_filter: CatalogWorkspaceFilter) -> Self {
        self.workspace_filter = workspace_filter;
        self
    }

    #[must_use]
    pub const fn with_workspace_id(mut self, workspace_id: Option<WorkspaceId>) -> Self {
        self.workspace_filter = CatalogWorkspaceFilter::from_workspace_id(workspace_id);
        self
    }

    #[must_use]
    pub fn with_title_query(mut self, title_query: Option<impl Into<String>>) -> Self {
        self.title_query = title_query.map(Into::into);
        self
    }

    #[must_use]
    pub const fn with_after(mut self, after: Option<ConversationCatalogCursorV2>) -> Self {
        self.after = after;
        self
    }

    pub fn validate(mut self) -> Result<Self, ConversationCatalogQueryError> {
        if self.page_size == 0 || self.page_size > MAX_CONVERSATION_CATALOG_PAGE_SIZE {
            return Err(ConversationCatalogQueryError::InvalidPageSize);
        }
        if let Some(raw) = self.title_query {
            let trimmed = raw.trim();
            let char_count = trimmed.chars().count();
            if trimmed.is_empty()
                || char_count > MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS
                || trimmed.chars().any(char::is_control)
            {
                return Err(ConversationCatalogQueryError::InvalidTitleQuery);
            }
            self.title_query = Some(trimmed.to_owned());
        }
        Ok(self)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConversationCatalogQueryError {
    InvalidPageSize,
    InvalidTitleQuery,
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

#[cfg(test)]
mod tests {
    use musteragent_identity::PrincipalKind;

    use super::*;

    fn fixture_actor() -> Actor {
        Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        }
    }

    #[test]
    fn workspace_filter_constructors_preserve_option_workspace_id_semantics() {
        let actor = fixture_actor();
        let workspace_id = WorkspaceId::generate();

        assert_eq!(
            CatalogWorkspaceFilter::default(),
            CatalogWorkspaceFilter::All
        );
        assert_eq!(
            CatalogWorkspaceFilter::from(None),
            CatalogWorkspaceFilter::All
        );
        assert_eq!(
            CatalogWorkspaceFilter::from(Some(workspace_id)),
            CatalogWorkspaceFilter::Assigned(workspace_id)
        );
        assert_eq!(
            CatalogWorkspaceFilter::from(workspace_id),
            CatalogWorkspaceFilter::Assigned(workspace_id)
        );

        let all_query = ListConversationCatalogV2::from_workspace_id(
            actor,
            None,
            None,
            ConversationLifecycle::Active,
            20,
            None,
        );
        assert_eq!(all_query.workspace_filter, CatalogWorkspaceFilter::All);
        assert_eq!(all_query.title_query, None);

        let assigned_query = ListConversationCatalogV2::from_workspace_id(
            actor,
            None,
            Some(workspace_id),
            ConversationLifecycle::Active,
            20,
            None,
        );
        assert_eq!(
            assigned_query.workspace_filter,
            CatalogWorkspaceFilter::Assigned(workspace_id)
        );

        let unassigned_query =
            ListConversationCatalogV2::new(actor, ConversationLifecycle::Active, 25)
                .with_workspace_filter(CatalogWorkspaceFilter::Unassigned)
                .with_title_query(Some("  quarterly  "));
        assert_eq!(
            unassigned_query.workspace_filter,
            CatalogWorkspaceFilter::Unassigned
        );
        assert_eq!(
            unassigned_query
                .validate()
                .expect("valid query")
                .title_query,
            Some("quarterly".to_owned())
        );
    }

    #[test]
    fn title_query_validation_trims_and_enforces_character_bounds() {
        let actor = fixture_actor();
        let base = ListConversationCatalogV2::new(actor, ConversationLifecycle::Active, 20);

        assert_eq!(
            base.clone()
                .with_title_query(Some("  Release 100%_\\ Plan  "))
                .validate()
                .expect("trimmed query")
                .title_query,
            Some("Release 100%_\\ Plan".to_owned())
        );

        let exact_100_unicode = format!(
            "  {}  ",
            "中".repeat(MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS)
        );
        let validated = base
            .clone()
            .with_title_query(Some(exact_100_unicode))
            .validate()
            .expect("100 Unicode characters are permitted");
        assert_eq!(
            validated.title_query.as_deref().map(|s| s.chars().count()),
            Some(MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS)
        );

        for invalid in [
            String::new(),
            "   \t\n  ".to_owned(),
            "x".repeat(MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS + 1),
            "中".repeat(MAX_CONVERSATION_CATALOG_TITLE_QUERY_CHARS + 1),
            "has\0nul".to_owned(),
        ] {
            assert_eq!(
                base.clone().with_title_query(Some(invalid)).validate(),
                Err(ConversationCatalogQueryError::InvalidTitleQuery)
            );
        }

        for page_size in [0_u16, MAX_CONVERSATION_CATALOG_PAGE_SIZE + 1] {
            assert_eq!(
                ListConversationCatalogV2::new(actor, ConversationLifecycle::Active, page_size)
                    .validate(),
                Err(ConversationCatalogQueryError::InvalidPageSize)
            );
        }
    }
}
