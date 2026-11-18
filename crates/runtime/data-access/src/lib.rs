//! Conversation-scoped authorization and query orchestration for public
//! Runtime data.
//!
//! Authentication token parsing is transport infrastructure. This crate owns
//! the transport-independent claims, scope checks, Snapshot/Tail queries and
//! ContentRef reachability proof.

#![forbid(unsafe_code)]

pub mod delegation_history;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use musteragent_content_ref::ContentRef;
use musteragent_conversation::{ConversationView, GetConversation};
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{
    Actor, ArtifactId, ArtifactVersionId, ConversationId, ConversationSeq, DottedKey, HomeEpoch,
    LedgerOffset, LedgerRecordId, ProjectionSnapshotId, RuntimeDataTicketId, TenantId,
    UtcTimestamp, WorkspaceId,
};
use musteragent_ledger::{
    CanonicalLedgerRecord, LedgerApplication, ListConversationRecords, RecordPage,
    SubscribeConversationRecords,
};
use musteragent_projection::{
    ConversationProjectionApplication, ConversationProjectionSnapshot,
    ConversationProjectionSnapshotCodec, ConversationProjectionWindow,
    ConversationProjectionWindowCursor, GetConversationProjection, GetConversationProjectionWindow,
    ledger_record_reaches_product_content,
};
pub use musteragent_runtime_content::ArtifactContentAuthorization;
use musteragent_runtime_content::{ContentStore, ContentStoreError};
use musteragent_runtime_contract::{ApplicationFuture, ApplicationStream, RuntimeError};
use thiserror::Error;

pub const WORKSPACE_FILE_TICKET_SCHEMA_VERSION: &str = "musteragent.workspace-file-ticket.v1";

pub const RUNTIME_DATA_TICKET_SCHEMA_VERSION: &str = "musteragent.runtime-data-ticket.v2";
pub const RUNTIME_DATA_TICKET_AUDIENCE: &str = "musteragent.runtime.data-edge";
pub const LEGACY_RUNTIME_DATA_TICKET_SCHEMA_VERSION: &str =
    "musteragent.legacy.runtime-data-ticket.v1";
pub const LEGACY_RUNTIME_DATA_TICKET_AUDIENCE: &str = "musteragent.legacy.runtime.data-edge";
pub const RUNTIME_DATA_CONTRACT_VERSION: &str = "musteragent.product.agent.v1";
pub const LEGACY_RUNTIME_DATA_CONTRACT_VERSION: &str = "musteragent.legacy.product.agent.v1";
pub const MAX_RUNTIME_DATA_TICKET_LIFETIME_MILLIS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RuntimeDataScope {
    ProjectionRead,
    RecordList,
    RecordSubscribe,
    ContentRead,
}

/// Immutable authority supplied by a Product content hydration request.
///
/// Snapshot authority covers cold/history projection hydration. Ledger-record
/// authority covers a ContentRef exposed by one just-delivered live or Tail
/// record without consulting a moving projection head. Artifact-version
/// authority covers one exact immutable historical Artifact revision.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ContentReadAuthority {
    BrowserJob(musteragent_identity::BrowserJobId),
    ProjectionSnapshot(ProjectionSnapshotId),
    LedgerRecord {
        record_id: LedgerRecordId,
        conversation_seq: ConversationSeq,
        home_epoch: HomeEpoch,
    },
    ArtifactVersion {
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationDataGrant {
    pub conversation_id: ConversationId,
    pub allowed_home_epoch: Option<HomeEpoch>,
    pub scopes: BTreeSet<RuntimeDataScope>,
}

/// Read authority for one immutable version of a currently accessible File.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkspaceFileDataGrant {
    pub workspace_id: WorkspaceId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeDataAccessTicketClaims {
    pub schema_version: DottedKey,
    pub issuer: DottedKey,
    pub audience: DottedKey,
    pub ticket_id: RuntimeDataTicketId,
    pub actor: Actor,
    pub conversation_grants: Vec<ConversationDataGrant>,
    pub workspace_file_grants: Vec<WorkspaceFileDataGrant>,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
    pub contract_version: DottedKey,
}

impl RuntimeDataAccessTicketClaims {
    pub fn validate(&self, now: UtcTimestamp) -> Result<(), RuntimeDataTicketContractError> {
        let expected_contract = match self.schema_version.as_str() {
            RUNTIME_DATA_TICKET_SCHEMA_VERSION | WORKSPACE_FILE_TICKET_SCHEMA_VERSION
                if self.audience.as_str() == RUNTIME_DATA_TICKET_AUDIENCE =>
            {
                RUNTIME_DATA_CONTRACT_VERSION
            }
            LEGACY_RUNTIME_DATA_TICKET_SCHEMA_VERSION
                if self.audience.as_str() == LEGACY_RUNTIME_DATA_TICKET_AUDIENCE =>
            {
                LEGACY_RUNTIME_DATA_CONTRACT_VERSION
            }
            RUNTIME_DATA_TICKET_SCHEMA_VERSION
            | WORKSPACE_FILE_TICKET_SCHEMA_VERSION
            | LEGACY_RUNTIME_DATA_TICKET_SCHEMA_VERSION => {
                return Err(RuntimeDataTicketContractError::InvalidAudience);
            }
            _ => return Err(RuntimeDataTicketContractError::UnsupportedSchema),
        };
        if self.contract_version.as_str() != expected_contract {
            return Err(RuntimeDataTicketContractError::UnsupportedContract);
        }
        let lifetime = self
            .expires_at
            .unix_millis()
            .checked_sub(self.issued_at.unix_millis())
            .ok_or(RuntimeDataTicketContractError::InvalidLifetime)?;
        if lifetime <= 0 || lifetime > MAX_RUNTIME_DATA_TICKET_LIFETIME_MILLIS {
            return Err(RuntimeDataTicketContractError::InvalidLifetime);
        }
        if self.issued_at.unix_millis() > now.unix_millis() + 30_000 {
            return Err(RuntimeDataTicketContractError::IssuedInFuture);
        }
        if self.expires_at.unix_millis() <= now.unix_millis() {
            return Err(RuntimeDataTicketContractError::Expired);
        }
        if self.schema_version.as_str() == WORKSPACE_FILE_TICKET_SCHEMA_VERSION {
            if !self.conversation_grants.is_empty()
                || self.actor.principal_kind != musteragent_identity::PrincipalKind::User
            {
                return Err(RuntimeDataTicketContractError::InvalidGrant);
            }
            if self.workspace_file_grants.is_empty() || self.workspace_file_grants.len() > 16 {
                return Err(RuntimeDataTicketContractError::InvalidGrantCount);
            }
            let unique = self.workspace_file_grants.iter().collect::<BTreeSet<_>>();
            return if unique.len() == self.workspace_file_grants.len() {
                Ok(())
            } else {
                Err(RuntimeDataTicketContractError::InvalidGrant)
            };
        }
        if !self.workspace_file_grants.is_empty() {
            return Err(RuntimeDataTicketContractError::InvalidGrant);
        }
        if self.conversation_grants.is_empty() || self.conversation_grants.len() > 16 {
            return Err(RuntimeDataTicketContractError::InvalidGrantCount);
        }
        let mut conversations = BTreeSet::new();
        for grant in &self.conversation_grants {
            if grant.scopes.is_empty() || !conversations.insert(grant.conversation_id) {
                return Err(RuntimeDataTicketContractError::InvalidGrant);
            }
        }
        Ok(())
    }

    pub fn authorize_workspace_file(
        &self,
        grant: WorkspaceFileDataGrant,
    ) -> Result<(), RuntimeDataAccessError> {
        if self.schema_version.as_str() != WORKSPACE_FILE_TICKET_SCHEMA_VERSION
            || !self.conversation_grants.is_empty()
            || !self.workspace_file_grants.contains(&grant)
        {
            return Err(RuntimeDataAccessError::NotAuthorized);
        }
        Ok(())
    }

    pub fn authorize(
        &self,
        conversation_id: ConversationId,
        scope: RuntimeDataScope,
    ) -> Result<&ConversationDataGrant, RuntimeDataAccessError> {
        self.conversation_grants
            .iter()
            .find(|grant| grant.conversation_id == conversation_id && grant.scopes.contains(&scope))
            .ok_or(RuntimeDataAccessError::NotAuthorized)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RuntimeDataTicketContractError {
    #[error("Runtime data ticket schema is unsupported")]
    UnsupportedSchema,
    #[error("Runtime data ticket audience is invalid")]
    InvalidAudience,
    #[error("Runtime data contract is unsupported")]
    UnsupportedContract,
    #[error("Runtime data ticket lifetime is invalid")]
    InvalidLifetime,
    #[error("Runtime data ticket was issued in the future")]
    IssuedInFuture,
    #[error("Runtime data ticket expired")]
    Expired,
    #[error("Runtime data ticket grant count is invalid")]
    InvalidGrantCount,
    #[error("Runtime data ticket grant is invalid")]
    InvalidGrant,
}

#[derive(Debug, Error)]
pub enum RuntimeDataAccessError {
    #[error("Runtime data scope is not authorized")]
    NotAuthorized,
    #[error("Runtime data grant belongs to another tenant")]
    TenantMismatch,
    #[error("Runtime data grant belongs to another home epoch")]
    HomeEpochMismatch,
    #[error("ContentRef is not reachable from the authorized Conversation")]
    ContentNotReachable,
    #[error("Projection query failed")]
    Projection(RuntimeError),
    #[error("Conversation authority query failed")]
    Conversation(RuntimeError),
    #[error("Ledger query failed")]
    Ledger(RuntimeError),
    #[error("Artifact content authorization failed")]
    ArtifactAuthorization(RuntimeError),
    #[error("Artifact content authorization returned an invalid batch contract")]
    ArtifactAuthorizationContractInvalid,
    #[error("Content read failed")]
    Content(#[from] ContentStoreError),
    #[error("Content Store returned an invalid batch contract")]
    ContentBatchContractInvalid,
    #[error("Projection Snapshot contract is invalid")]
    ProjectionContractInvalid,
    #[error("Conversation read bootstrap authority is unavailable")]
    BootstrapAuthorityUnavailable,
    #[error("Conversation read bootstrap observed a moving read skew")]
    BootstrapReadSkew,
    #[error("Conversation read bootstrap contract is invalid")]
    BootstrapContractInvalid,
}

/// Minimal control-plane read authority required by Runtime Data. Keeping this
/// Port smaller than `ConversationApplication` lets Data Edge own read
/// orchestration without depending on command admission capabilities.
pub trait ConversationReadAuthority: Send + Sync {
    fn get_conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>>;
}

impl<T> ConversationReadAuthority for T
where
    T: musteragent_conversation::ConversationApplication + ?Sized,
{
    fn get_conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        musteragent_conversation::ConversationApplication::get_conversation(self, query)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationReadBootstrap {
    pub conversation: ConversationView,
    pub projection: ConversationProjectionSnapshot,
    pub authority_snapshot_id: ProjectionSnapshotId,
    pub head_cursor: LedgerCursor,
    pub older_cursor: Option<ConversationProjectionWindowCursor>,
    pub has_older: bool,
    /// Conversation head observed only after the Projection authority was
    /// captured. It is the proof that the returned control view covers the
    /// Snapshot without requiring a second client request.
    pub observed_head_cursor: LedgerCursor,
    /// Complete presentation-critical Content payloads for the returned
    /// Projection window. `None` is an explicit bounded fallback for an
    /// unusually large window; clients then use the ordinary batch endpoint.
    pub eager_content: Option<Vec<(ContentRef, Vec<u8>)>>,
}

const MAX_BOOTSTRAP_CONTENT_ITEMS: usize = 512;
const MAX_BOOTSTRAP_CONTENT_BYTES: u64 = 32 * 1024 * 1024;

pub struct RuntimeDataApplication {
    browser_jobs: Option<Arc<dyn musteragent_runtime_capability::BrowserGatewayStore>>,
    conversations: Option<Arc<dyn ConversationReadAuthority>>,
    projections: Arc<dyn ConversationProjectionApplication>,
    ledger: Arc<dyn LedgerApplication>,
    content: Arc<dyn ContentStore>,
    artifact_content: Arc<dyn ArtifactContentAuthorization>,
    projection_codec: Arc<dyn ConversationProjectionSnapshotCodec>,
}

impl RuntimeDataApplication {
    #[must_use]
    pub fn new(
        projections: Arc<dyn ConversationProjectionApplication>,
        ledger: Arc<dyn LedgerApplication>,
        content: Arc<dyn ContentStore>,
        artifact_content: Arc<dyn ArtifactContentAuthorization>,
        projection_codec: Arc<dyn ConversationProjectionSnapshotCodec>,
    ) -> Self {
        Self {
            conversations: None,
            browser_jobs: None,
            projections,
            ledger,
            content,
            artifact_content,
            projection_codec,
        }
    }

    #[must_use]
    pub fn with_browser_jobs(
        mut self,
        jobs: Arc<dyn musteragent_runtime_capability::BrowserGatewayStore>,
    ) -> Self {
        self.browser_jobs = Some(jobs);
        self
    }

    #[must_use]
    pub fn with_conversation_authority(
        mut self,
        conversations: Arc<dyn ConversationReadAuthority>,
    ) -> Self {
        self.conversations = Some(conversations);
        self
    }

    /// Captures one coherent Product read authority.
    ///
    /// Projection is deliberately captured before Conversation. On the
    /// authoritative Runtime database the Ledger head is monotonic, so the
    /// later Conversation read must cover the Snapshot. The relation is still
    /// validated here so a future replica or routing regression becomes a
    /// retryable server-side read skew rather than a terminal client-side
    /// ownership failure.
    pub async fn get_conversation_read_bootstrap(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        limit: usize,
        preferred_snapshot_id: Option<ProjectionSnapshotId>,
        include_eager_content: bool,
    ) -> Result<ConversationReadBootstrap, RuntimeDataAccessError> {
        let grant = claims.authorize(conversation_id, RuntimeDataScope::ProjectionRead)?;
        let window = if let Some(snapshot_id) = preferred_snapshot_id {
            let projection = self
                .get_projection(claims, conversation_id, Some(snapshot_id))
                .await?;
            ConversationProjectionWindow {
                authority_snapshot_id: projection.snapshot_id,
                head_cursor: projection.cursor(),
                projection,
                older_cursor: None,
                has_older: false,
            }
        } else {
            self.get_projection_window(claims, conversation_id, limit, None)
                .await?
        };
        let conversations = self
            .conversations
            .as_ref()
            .ok_or(RuntimeDataAccessError::BootstrapAuthorityUnavailable)?;
        let conversation = conversations
            .get_conversation(GetConversation {
                actor: claims.actor,
                conversation_id,
            })
            .await
            .map_err(RuntimeDataAccessError::Conversation)?;
        let observed_head_cursor = conversation.latest_cursor;
        musteragent_projection::validate_conversation_read_head(
            claims.actor,
            conversation_id,
            &window,
            &conversation,
        )
        .map_err(|error| match error {
            musteragent_projection::ConversationReadHeadError::AuthorityMismatch
            | musteragent_projection::ConversationReadHeadError::EpochChanged => {
                RuntimeDataAccessError::BootstrapContractInvalid
            }
            musteragent_projection::ConversationReadHeadError::ReadSkew => {
                RuntimeDataAccessError::BootstrapReadSkew
            }
        })?;
        if grant
            .allowed_home_epoch
            .is_some_and(|epoch| epoch != conversation.conversation.home_epoch)
        {
            return Err(RuntimeDataAccessError::HomeEpochMismatch);
        }
        let eager_content = if include_eager_content {
            self.read_bootstrap_content(claims, conversation_id, &window.projection)
                .await?
        } else {
            None
        };
        Ok(ConversationReadBootstrap {
            conversation,
            projection: window.projection,
            authority_snapshot_id: window.authority_snapshot_id,
            head_cursor: window.head_cursor,
            older_cursor: window.older_cursor,
            has_older: window.has_older,
            observed_head_cursor,
            eager_content,
        })
    }

    async fn read_bootstrap_content(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        projection: &ConversationProjectionSnapshot,
    ) -> Result<Option<Vec<(ContentRef, Vec<u8>)>>, RuntimeDataAccessError> {
        claims.authorize(conversation_id, RuntimeDataScope::ContentRead)?;
        let references = bootstrap_content_references(projection);
        let byte_length = references.iter().try_fold(0_u64, |total, reference| {
            total.checked_add(reference.byte_length)
        });
        if references.len() > MAX_BOOTSTRAP_CONTENT_ITEMS
            || byte_length.is_none_or(|length| length > MAX_BOOTSTRAP_CONTENT_BYTES)
        {
            return Ok(None);
        }
        let payloads = self
            .content
            .read_batch(claims.actor.tenant_id, &references)
            .await
            .map_err(RuntimeDataAccessError::Content)?;
        if payloads.len() != references.len() {
            return Err(RuntimeDataAccessError::ContentBatchContractInvalid);
        }
        Ok(Some(references.into_iter().zip(payloads).collect()))
    }

    pub async fn get_projection(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        preferred_snapshot_id: Option<musteragent_identity::ProjectionSnapshotId>,
    ) -> Result<ConversationProjectionSnapshot, RuntimeDataAccessError> {
        let grant = claims.authorize(conversation_id, RuntimeDataScope::ProjectionRead)?;
        let query = GetConversationProjection {
            actor: claims.actor,
            conversation_id,
            preferred_snapshot_id,
        };
        // A default Product read must be self-contained at one captured Ledger
        // head. Returning an older reusable Snapshot would require the client to
        // hydrate ContentRefs that may already have been removed by its Tail;
        // ContentRead correctly authorizes only the current visible manifest.
        // Exact Snapshot reads remain available for an explicit cursor-recovery
        // flow, whose client already owns that Snapshot.
        let snapshot = self
            .projections
            .get_product_read_projection(query)
            .await
            .map_err(RuntimeDataAccessError::Projection)?;
        validate_snapshot(
            claims.actor.tenant_id,
            grant,
            &snapshot,
            self.projection_codec.as_ref(),
        )?;
        Ok(snapshot)
    }

    pub async fn get_projection_window(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        limit: usize,
        before: Option<ConversationProjectionWindowCursor>,
    ) -> Result<ConversationProjectionWindow, RuntimeDataAccessError> {
        let grant = claims.authorize(conversation_id, RuntimeDataScope::ProjectionRead)?;
        let window = self
            .projections
            .get_product_read_projection_window(GetConversationProjectionWindow {
                actor: claims.actor,
                conversation_id,
                limit,
                before,
            })
            .await
            .map_err(RuntimeDataAccessError::Projection)?;
        validate_snapshot(
            claims.actor.tenant_id,
            grant,
            &window.projection,
            self.projection_codec.as_ref(),
        )?;
        if window.head_cursor != window.projection.cursor()
            || window.authority_snapshot_id != window.projection.snapshot_id
            || window.has_older != window.older_cursor.is_some()
        {
            return Err(RuntimeDataAccessError::ProjectionContractInvalid);
        }
        Ok(window)
    }

    pub async fn list_records(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        cursor: LedgerCursor,
        limit: usize,
    ) -> Result<RecordPage, RuntimeDataAccessError> {
        let grant = claims.authorize(conversation_id, RuntimeDataScope::RecordList)?;
        validate_cursor(grant, conversation_id, cursor)?;
        self.ledger
            .list_conversation_records(ListConversationRecords {
                actor: claims.actor,
                conversation_id,
                cursor,
                limit,
            })
            .await
            .map_err(RuntimeDataAccessError::Ledger)
    }

    pub async fn subscribe_records(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        cursor: LedgerCursor,
    ) -> Result<
        ApplicationStream<Result<CanonicalLedgerRecord, RuntimeError>>,
        RuntimeDataAccessError,
    > {
        let grant = claims.authorize(conversation_id, RuntimeDataScope::RecordSubscribe)?;
        validate_cursor(grant, conversation_id, cursor)?;
        self.ledger
            .subscribe_conversation_records(SubscribeConversationRecords {
                actor: claims.actor,
                conversation_id,
                cursor,
            })
            .await
            .map_err(RuntimeDataAccessError::Ledger)
    }

    pub async fn read_workspace_file_content(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        grant: WorkspaceFileDataGrant,
        reference: &ContentRef,
    ) -> Result<Vec<u8>, RuntimeDataAccessError> {
        claims.authorize_workspace_file(grant)?;
        let reachable = self
            .artifact_content
            .workspace_version_content_reachability(
                claims.actor,
                grant.workspace_id,
                grant.artifact_id,
                grant.artifact_version_id,
                std::slice::from_ref(reference),
            )
            .await
            .map_err(RuntimeDataAccessError::ArtifactAuthorization)?;
        if reachable.len() != 1 {
            return Err(RuntimeDataAccessError::ArtifactAuthorizationContractInvalid);
        }
        if !reachable[0] {
            return Err(RuntimeDataAccessError::ContentNotReachable);
        }
        self.content
            .read(claims.actor.tenant_id, reference)
            .await
            .map_err(RuntimeDataAccessError::Content)
    }

    pub async fn read_content(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        reference: &ContentRef,
        authority: Option<ContentReadAuthority>,
    ) -> Result<Vec<u8>, RuntimeDataAccessError> {
        let mut payloads = self
            .read_content_batch(
                claims,
                conversation_id,
                std::slice::from_ref(reference),
                authority,
            )
            .await?;
        payloads
            .pop()
            .ok_or(RuntimeDataAccessError::ContentBatchContractInvalid)
    }

    /// Reads one ordered ContentRef set under a single authorization proof.
    ///
    /// Current Artifact reachability remains an independent authorization
    /// source, but it is evaluated once for the whole set. Any unresolved
    /// references are then proven by one exact Snapshot, Ledger record, or
    /// bounded head read before the Content Store receives one batch request.
    pub async fn read_content_batch(
        &self,
        claims: &RuntimeDataAccessTicketClaims,
        conversation_id: ConversationId,
        references: &[ContentRef],
        authority: Option<ContentReadAuthority>,
    ) -> Result<Vec<Vec<u8>>, RuntimeDataAccessError> {
        if references.is_empty() {
            return Ok(Vec::new());
        }
        let grant = claims.authorize(conversation_id, RuntimeDataScope::ContentRead)?;
        self.read_authorized_content_batch(claims.actor, grant, references, authority)
            .await
    }

    /// Native authenticated callers own a control-plane Actor, not a Data Edge
    /// ticket. Reauthorize membership before and after physical IO, then use
    /// the same Snapshot/Ledger/Artifact reachability rules as ticketed callers.
    pub async fn read_member_content_batch(
        &self,
        query: GetConversation,
        references: &[ContentRef],
        authority: Option<ContentReadAuthority>,
    ) -> Result<Vec<Vec<u8>>, RuntimeDataAccessError> {
        let conversations = self
            .conversations
            .as_ref()
            .ok_or(RuntimeDataAccessError::BootstrapAuthorityUnavailable)?;
        let before = conversations
            .get_conversation(GetConversation {
                actor: query.actor,
                conversation_id: query.conversation_id,
            })
            .await
            .map_err(RuntimeDataAccessError::Conversation)?;
        if before.conversation.tenant_id != query.actor.tenant_id
            || before.conversation.conversation_id != query.conversation_id
        {
            return Err(RuntimeDataAccessError::BootstrapContractInvalid);
        }
        // Current membership is conversation-scoped, not an old epoch-bound
        // transport ticket. Explicit historical Snapshot authority stays legal.
        let grant = ConversationDataGrant {
            conversation_id: query.conversation_id,
            allowed_home_epoch: None,
            scopes: BTreeSet::from([RuntimeDataScope::ContentRead]),
        };
        let payloads = self
            .read_authorized_content_batch(query.actor, &grant, references, authority)
            .await?;
        let after = conversations
            .get_conversation(query)
            .await
            .map_err(RuntimeDataAccessError::Conversation)?;
        if before.conversation.tenant_id != after.conversation.tenant_id
            || before.conversation.conversation_id != after.conversation.conversation_id
            || before.conversation.workspace_id != after.conversation.workspace_id
            || before.conversation.control_home != after.conversation.control_home
            || before.conversation.home_epoch != after.conversation.home_epoch
        {
            return Err(RuntimeDataAccessError::BootstrapContractInvalid);
        }
        Ok(payloads)
    }

    async fn read_authorized_content_batch(
        &self,
        actor: Actor,
        grant: &ConversationDataGrant,
        references: &[ContentRef],
        authority: Option<ContentReadAuthority>,
    ) -> Result<Vec<Vec<u8>>, RuntimeDataAccessError> {
        if references.is_empty() {
            return Ok(Vec::new());
        }
        let conversation_id = grant.conversation_id;
        if let Some(ContentReadAuthority::BrowserJob(job_id)) = authority {
            let jobs = self
                .browser_jobs
                .as_ref()
                .ok_or(RuntimeDataAccessError::ContentNotReachable)?;
            let job = jobs
                .load_job(actor.tenant_id, job_id)
                .await
                .map_err(|_| RuntimeDataAccessError::Content(ContentStoreError::Unavailable))?
                .ok_or(RuntimeDataAccessError::ContentNotReachable)?;
            if job.delivery.lease.principal_id != actor.principal_id
                || job.delivery.request.conversation_id != conversation_id
                || !references.iter().all(|reference| {
                    job.delivery
                        .request
                        .inputs
                        .iter()
                        .any(|input| &input.content_ref == reference)
                })
            {
                return Err(RuntimeDataAccessError::ContentNotReachable);
            }
            let values = self
                .content
                .read_batch(actor.tenant_id, references)
                .await
                .map_err(RuntimeDataAccessError::Content)?;
            if values.len() != references.len() {
                return Err(RuntimeDataAccessError::ContentBatchContractInvalid);
            }
            for (reference, bytes) in references.iter().zip(&values) {
                musteragent_runtime_content::verify_content(reference, bytes)
                    .map_err(|_| RuntimeDataAccessError::ContentBatchContractInvalid)?;
            }
            return Ok(values);
        }

        let artifact_reachability = self
            .artifact_content
            .current_content_reachability(actor, conversation_id, references)
            .await
            .map_err(RuntimeDataAccessError::ArtifactAuthorization)?;
        if artifact_reachability.len() != references.len() {
            return Err(RuntimeDataAccessError::ArtifactAuthorizationContractInvalid);
        }

        let unresolved = references
            .iter()
            .zip(&artifact_reachability)
            .filter(|(_, reachable)| !**reachable)
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        if !unresolved.is_empty() {
            match authority {
                Some(ContentReadAuthority::ArtifactVersion {
                    artifact_id,
                    artifact_version_id,
                }) => {
                    let reachable = self
                        .artifact_content
                        .version_content_reachability(
                            actor,
                            conversation_id,
                            artifact_id,
                            artifact_version_id,
                            &unresolved,
                        )
                        .await
                        .map_err(RuntimeDataAccessError::ArtifactAuthorization)?;
                    if reachable.len() != unresolved.len() {
                        return Err(RuntimeDataAccessError::ArtifactAuthorizationContractInvalid);
                    }
                    if reachable.iter().any(|reachable| !reachable) {
                        return Err(RuntimeDataAccessError::ContentNotReachable);
                    }
                }
                Some(ContentReadAuthority::LedgerRecord {
                    record_id,
                    conversation_seq,
                    home_epoch,
                }) => {
                    if grant
                        .allowed_home_epoch
                        .is_some_and(|allowed| allowed != home_epoch)
                    {
                        return Err(RuntimeDataAccessError::HomeEpochMismatch);
                    }
                    let cursor = LedgerCursor {
                        conversation_id,
                        home_epoch,
                        after_conversation_seq: LedgerOffset::new(conversation_seq.get() - 1),
                    };
                    let page = self
                        .ledger
                        .list_conversation_records(ListConversationRecords {
                            actor,
                            conversation_id,
                            cursor,
                            limit: 1,
                        })
                        .await
                        .map_err(RuntimeDataAccessError::Ledger)?;
                    let authorized = page.records.first().is_some_and(|record| {
                        record.metadata.tenant_id == actor.tenant_id
                            && record.metadata.conversation_id == conversation_id
                            && record.metadata.home_epoch == home_epoch
                            && record.metadata.conversation_seq == conversation_seq
                            && record.metadata.record_id == record_id
                            && unresolved.iter().all(|reference| {
                                ledger_record_reaches_product_content(record, reference)
                            })
                    });
                    if !authorized {
                        return Err(RuntimeDataAccessError::ContentNotReachable);
                    }
                }
                snapshot_authority => {
                    let authority_snapshot_id =
                        snapshot_authority.map(|authority| match authority {
                            ContentReadAuthority::ProjectionSnapshot(snapshot_id) => snapshot_id,
                            ContentReadAuthority::BrowserJob(_)
                            | ContentReadAuthority::LedgerRecord { .. }
                            | ContentReadAuthority::ArtifactVersion { .. } => unreachable!(),
                        });
                    let query = GetConversationProjection {
                        actor,
                        conversation_id,
                        preferred_snapshot_id: authority_snapshot_id,
                    };
                    let snapshot = if authority_snapshot_id.is_some() {
                        // The Product received this immutable Snapshot before it
                        // started hydrating its manifest. The exact Snapshot is
                        // loaded and validated once for the complete batch.
                        self.projections.get_product_read_projection(query).await
                    } else {
                        self.projections.get_bounded_head_projection(query).await
                    }
                    .map_err(RuntimeDataAccessError::Projection)?;
                    validate_snapshot(
                        actor.tenant_id,
                        grant,
                        &snapshot,
                        self.projection_codec.as_ref(),
                    )?;
                    let manifest = snapshot.content_manifest.iter().collect::<HashSet<_>>();
                    if !unresolved
                        .iter()
                        .all(|reference| manifest.contains(reference))
                    {
                        return Err(RuntimeDataAccessError::ContentNotReachable);
                    }
                }
            }
        }

        let payloads = self
            .content
            .read_batch(actor.tenant_id, references)
            .await
            .map_err(RuntimeDataAccessError::Content)?;
        if payloads.len() != references.len() {
            return Err(RuntimeDataAccessError::ContentBatchContractInvalid);
        }
        Ok(payloads)
    }
}

/// Ordered content needed to install the Product read model. Artifact bodies
/// and message attachments remain lazy because timeline cards render entirely
/// from their immutable references.
fn bootstrap_content_references(snapshot: &ConversationProjectionSnapshot) -> Vec<ContentRef> {
    let mut references = Vec::new();
    for message in &snapshot.messages {
        references.push(message.content_ref.clone());
        for summary in &message.reasoning_summaries {
            references.push(summary.content_ref.clone());
        }
        if let Some(reference) = &message.imported_projection_ref {
            references.push(reference.clone());
        }
    }
    for activity in &snapshot.activities {
        if let Some(reference) = activity
            .delegation_origin
            .as_ref()
            .and_then(|origin| origin.objective_ref.as_ref())
        {
            references.push(reference.clone());
        }
        if let Some(reference) = activity
            .coordination_origin
            .as_ref()
            .and_then(|origin| origin.payload_ref.as_ref())
        {
            references.push(reference.clone());
        }
    }
    for tool in &snapshot.tool_timeline {
        references.push(tool.request_ref.clone());
        if let Some(reference) = &tool.result_ref {
            references.push(reference.clone());
        }
    }
    for interaction in &snapshot.pending_interactions {
        references.push(interaction.prompt_ref.clone());
    }
    for output in &snapshot.stream_outputs {
        references.push(output.content_ref.clone());
    }
    references
}

fn validate_snapshot(
    tenant_id: TenantId,
    grant: &ConversationDataGrant,
    snapshot: &ConversationProjectionSnapshot,
    codec: &dyn ConversationProjectionSnapshotCodec,
) -> Result<(), RuntimeDataAccessError> {
    if snapshot.tenant_id != tenant_id {
        return Err(RuntimeDataAccessError::TenantMismatch);
    }
    if snapshot.conversation_id != grant.conversation_id {
        return Err(RuntimeDataAccessError::NotAuthorized);
    }
    if grant
        .allowed_home_epoch
        .is_some_and(|epoch| epoch != snapshot.home_epoch)
    {
        return Err(RuntimeDataAccessError::HomeEpochMismatch);
    }
    codec
        .encode(snapshot)
        .map_err(|_| RuntimeDataAccessError::ProjectionContractInvalid)?;
    Ok(())
}

fn validate_cursor(
    grant: &ConversationDataGrant,
    conversation_id: ConversationId,
    cursor: LedgerCursor,
) -> Result<(), RuntimeDataAccessError> {
    if cursor.conversation_id != conversation_id || grant.conversation_id != conversation_id {
        return Err(RuntimeDataAccessError::NotAuthorized);
    }
    if grant
        .allowed_home_epoch
        .is_some_and(|epoch| epoch != cursor.home_epoch)
    {
        return Err(RuntimeDataAccessError::HomeEpochMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
